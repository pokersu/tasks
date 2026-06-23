# OCR 任务调度系统技术规格说明书

## 1. 项目背景与目标

**背景**：系统接收 OCR 识别请求，将其持久化为任务表（已有表结构），需要一种多实例（Pod）环境下的调度机制，确保任务被有序、高效、不重复地执行。

**目标**：

- 防止任务重复执行（多实例竞争安全）。
- 保护下游 OCR 服务（限流，负载感知）。
- 多租户公平（不同 project_id 的任务均能及时处理）。
- 容错（实例崩溃后任务可被其他实例接管）。
- 极简（无外部依赖，仅依赖数据库，代码量小）。

## 2. 整体架构设计

### 2.1 核心组件

- **TaskScheduler**：一个 Spring `@Scheduled` 定时器，同时负责"任务抢占"和"提交执行"。
- **ThreadPoolExecutor**：固定大小、无缓冲队列（`SynchronousQueue`）的线程池，作为限流和执行载体。
- **PostgreSQL**：存储任务状态，利用行锁和原子 UPDATE 实现分布式抢占（利用 `RETURNING` 子句一次完成抢锁 + 返回数据）。

### 2.2 调度流程（单次触发）

1. **超时重置**：将卡在 PROCESSING 状态超过阈值（如 10 分钟）的任务重置为 PENDING。
2. **负载检查**：计算当前线程池活跃线程数，若负载 ≥ 75%（可配置），则跳过本次调度（不查询数据库）。
3. **获取可抢槽位数**：`slots = ceil(coreSize * 0.75) - activeCount`，即本次最多可抢的任务数。
4. **查询活跃租户**：`SELECT DISTINCT project_id` 从 PENDING 且无主的任务中获取有积压的租户列表（限制最多 10 个）。
5. **轮询租户抢占任务**：对每个租户，执行原子 `UPDATE ... RETURNING *` 抢 1 个任务（PENDING → PROCESSING，设置 worker_id 和 start_time），一次 SQL 同时完成抢占和数据获取，无需二次查询。
6. **立即提交线程池**：抢锁成功后，立即 `executor.execute(taskRunnable)`。由于 `SynchronousQueue`，若无空闲线程则立即抛出 `RejectedExecutionException`，捕获后执行回滚（UPDATE 回 PENDING）。
7. **任务执行**：执行 OCR 调用，成功后更新 SUCCESS，失败则更新 FAILED（不自动重试）。

## 3. 数据库表结构（已有，仅做说明）

### 3.1 核心字段（假设）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | bigint / serial | 主键 |
| project_id | varchar(32) | 租户ID |
| status | smallint / int | 状态枚举（见下方约定） |
| worker_id | varchar(64) | 执行实例标识（Pod名/IP），NULL 表示无主 |
| start_time | timestamp | 开始处理时间（抢占时设为 NOW()） |
| retry_count | int | 已重试次数（超时重置时累加） |
| ... (其他业务字段) | - | 如 image_url, callback_url 等 |

### 3.2 状态枚举约定

| 值 | 状态 | 说明 |
|----|------|------|
| 0 | PENDING | 待处理 |
| 1 | PROCESSING | 处理中 |
| 2 | SUCCESS | 成功 |
| 3 | FAILED | 失败 |

### 3.3 索引要求

```sql
CREATE INDEX idx_dispatch ON ocr_task (status, worker_id, project_id, start_time);
```

## 4. 核心 SQL 语句（PostgreSQL 语法）

### 4.1 超时重置（resetTimeoutTasks）

```sql
UPDATE ocr_task
SET status = 0, 
    worker_id = NULL, 
    retry_count = retry_count + 1
WHERE status = 1
  AND start_time < NOW() - INTERVAL '#{timeoutMinutes} minutes';
```

参数 `timeoutMinutes` 为配置项（建议 10）。

### 4.2 查询活跃租户（selectActiveTenants）

```sql
SELECT DISTINCT project_id
FROM ocr_task
WHERE status = 0 AND worker_id IS NULL
ORDER BY project_id ASC
LIMIT #{limit};
```

`limit` 建议 10。

### 4.3 按租户抢占一个任务（lockOneTaskByTenant）—— 核心，利用 RETURNING

```sql
UPDATE ocr_task
SET status = 1, 
    worker_id = #{workerId}, 
    start_time = NOW()
WHERE id = (
    SELECT id
    FROM ocr_task
    WHERE project_id = #{projectId}
      AND status = 0
      AND worker_id IS NULL
    ORDER BY start_time ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED   -- 防止被其他实例同时抢占
)
RETURNING *;   -- 一次返回完整任务数据，无需二次查询
```

**说明**：

- `FOR UPDATE SKIP LOCKED` 是 PostgreSQL 的利器，跳过已被其他事务锁定的行，避免等待。
- `RETURNING *` 直接返回被更新的完整行，Java 端可直接映射为 `OcrTask` 对象。

### 4.4 释放锁（unlockTask）

```sql
UPDATE ocr_task
SET status = 0, worker_id = NULL
WHERE id = #{taskId}
  AND worker_id = #{workerId}
  AND status = 1;
```

### 4.5 更新成功（updateSuccess）

```sql
UPDATE ocr_task
SET status = 2, worker_id = NULL
WHERE id = #{taskId}
  AND status = 1
  AND worker_id = #{workerId};
```

### 4.6 更新失败（updateFailed）

```sql
UPDATE ocr_task
SET status = 3, worker_id = NULL
WHERE id = #{taskId}
  AND status = 1
  AND worker_id = #{workerId};
```

## 5. Java 核心逻辑实现

### 5.1 组件配置

- **线程池 Bean（ThreadPoolExecutor）**：
  - `corePoolSize = maxPoolSize` = 可配置（如 20）
  - `keepAliveTime = 60s`
  - `BlockingQueue` = `SynchronousQueue`（无缓冲）
  - `RejectedExecutionHandler` = `AbortPolicy`
- **调度器 Bean**：使用 `@Scheduled(fixedDelay = 3000)`（可配置）。

### 5.2 调度方法签名

```java
@Scheduled(fixedDelayString = "${ocr.scheduler.fixed-delay:3000}")
public void dispatchAndExecute()
```

**核心逻辑步骤**：

```java
public void dispatchAndExecute() {
    // 1. 超时重置
    int resetCount = taskMapper.resetTimeoutTasks(timeoutMinutes);
    if (resetCount > 0) {
        log.warn("超时重置了 {} 个任务", resetCount);
    }

    // 2. 负载检查
    int active = executor.getActiveCount();
    int coreSize = executor.getCorePoolSize();
    double load = (double) active / coreSize;
    if (load >= loadThreshold) {
        log.debug("负载 {}/{} = {}% >= {}%，跳过本次调度", active, coreSize, (int)(load*100), (int)(loadThreshold*100));
        return;
    }

    // 3. 计算最大可抢槽位
    int maxSlots = (int) Math.ceil(coreSize * loadThreshold) - active;
    if (maxSlots <= 0) return;

    // 4. 查询活跃租户
    List<String> projectIds = taskMapper.selectActiveTenants(maxTenants);
    if (projectIds.isEmpty()) return;

    // 5. 轮询租户，逐个抢占
    int dispatched = 0;
    Iterator<String> iterator = projectIds.iterator();
    while (iterator.hasNext() && dispatched < maxSlots) {
        String projectId = iterator.next();
        
        // ★ 核心：一次SQL完成抢锁 + 返回完整任务
        OcrTask task = taskMapper.lockOneTaskByTenant(projectId, workerId);
        if (task == null) {
            iterator.remove(); // 该租户无可用任务
            continue;
        }

        // 抢锁成功，尝试提交线程池
        try {
            executor.execute(() -> executeTask(task));
            dispatched++;
        } catch (RejectedExecutionException e) {
            // 线程池瞬时满载（极低概率），回滚该任务
            taskMapper.unlockTask(task.getId(), workerId);
            log.warn("线程池满载，回滚任务 taskId={}", task.getId());
            // 不移除租户，留给下次调度
        }
    }
    
    log.debug("本轮调度完成，分发 {} 个任务，当前负载 {}/{}", dispatched, executor.getActiveCount(), coreSize);
}
```

### 5.3 任务执行逻辑

```java
private void executeTask(OcrTask task) {
    try {
        // 调用OCR服务（需设置超时，如120秒）
        String result = ocrClient.recognize(task);
        
        // 更新成功（带worker_id校验）
        int rows = taskMapper.updateSuccess(task.getId(), workerId);
        if (rows == 0) {
            log.warn("任务可能已被重置或抢走，更新成功无效 taskId={}", task.getId());
        }
    } catch (Exception e) {
        log.error("OCR执行失败 taskId={}", task.getId(), e);
        taskMapper.updateFailed(task.getId(), workerId);
    }
}
```

## 6. 配置参数（外部化）

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| ocr.thread-pool.core-size | int | 20 | 线程池固定大小 |
| ocr.scheduler.fixed-delay | long | 3000 | 定时器间隔（毫秒） |
| ocr.scheduler.load-threshold | double | 0.75 | 负载阈值（0~1） |
| ocr.scheduler.timeout-minutes | int | 10 | 超时重置分钟数（建议 > 单次OCR最大耗时×3） |
| ocr.scheduler.max-tenants | int | 10 | 每轮最多处理的租户数 |
| ocr.client.timeout-seconds | int | 120 | OCR调用超时（秒） |

## 7. MyBatis Mapper 接口定义

```java
@Mapper
public interface OcrTaskMapper {
    
    int resetTimeoutTasks(@Param("timeoutMinutes") int timeoutMinutes);
    
    List<String> selectActiveTenants(@Param("limit") int limit);
    
    // ★ 返回完整任务对象
    OcrTask lockOneTaskByTenant(@Param("projectId") String projectId, 
                                 @Param("workerId") String workerId);
    
    int unlockTask(@Param("taskId") Long taskId, 
                   @Param("workerId") String workerId);
    
    int updateSuccess(@Param("taskId") Long taskId, 
                      @Param("workerId") String workerId);
    
    int updateFailed(@Param("taskId") Long taskId, 
                     @Param("workerId") String workerId);
}
```

**对应 XML 映射（lockOneTaskByTenant 关键示例）**：

```xml
<select id="lockOneTaskByTenant" resultType="com.xxx.OcrTask">
    UPDATE ocr_task
    SET status = 1, 
        worker_id = #{workerId}, 
        start_time = NOW()
    WHERE id = (
        SELECT id
        FROM ocr_task
        WHERE project_id = #{projectId}
          AND status = 0
          AND worker_id IS NULL
        ORDER BY start_time ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
</select>
```

> **注意**：MyBatis 对 `RETURNING *` 的支持需使用 `@Select` 或 `<select>` 标签，并设置 `resultType` 映射。若版本不支持，可用 `@Update` 配合 `@Options(statementType = StatementType.STATEMENT)` 并手动解析结果集，但建议升级 MyBatis 或使用 JdbcTemplate 原生支持。

## 8. worker_id 获取工具类

```java
public class WorkerIdUtils {
    public static String getWorkerId() {
        // K8s 环境优先
        String hostname = System.getenv("HOSTNAME");
        if (StringUtils.hasText(hostname)) return hostname;
        // 降级为主机名
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return UUID.randomUUID().toString();
        }
    }
}
```

## 9. 异常处理与容错

| 场景 | 处理方式 |
|------|----------|
| 数据库连接失败 | 定时器抛异常，Spring 会暂停下一次执行，需外部告警 |
| OCR 调用失败 | 任务置为 FAILED，需人工介入（可后续扩展重试策略） |
| 线程池拒绝 | 任务回滚至 PENDING，不丢失 |
| 实例崩溃 | 该实例正在执行的任务卡在 PROCESSING，超时重置后会被其他实例接管 |
| 集群时钟不同步 | 使用数据库 NOW() 统一时间，已规避此问题 |

## 10. 监控指标（建议暴露 Actuator 端点）

| 指标 | 说明 |
|------|------|
| `ocr.executor.activeCount` | 当前活跃线程数 |
| `ocr.executor.queueSize` | 队列大小（应为 0） |
| `ocr.tasks.pending` | 待处理任务数（`SELECT COUNT(*) FROM ocr_task WHERE status=0`） |
| `ocr.tasks.processing` | 处理中任务数（`SELECT COUNT(*) FROM ocr_task WHERE status=1`） |
| `ocr.tasks.timeout-reset-count` | 超时重置累计次数（内存计数） |

## 11. 测试要点

| 场景 | 验证内容 |
|------|----------|
| 单实例 | 任务正常流转 |
| 多实例并发抢占 | 启动 2 个实例，插入 10 条 PENDING，验证每个任务只被执行一次 |
| 负载阈值 | 线程池满载时定时器跳过调度 |
| 超时重置 | 手动将任务置为 PROCESSING 且 start_time 为 5 分钟前，验证重置 |
| 租户公平 | projectA 100 条，projectB 1 条，验证 B 不饿死 |
| 实例崩溃 | kill Pod，观察超时后任务被其他 Pod 接管 |

## 12. 补充说明

### 关于 `FOR UPDATE SKIP LOCKED` 的注意点

- 仅在 PostgreSQL 9.5+ 支持，你的版本应满足。
- 结合 `RETURNING *`，一条 SQL 完成"抢占 + 返回数据"，是此方案效率最高的写法。

### 关于 `start_time` 字段

- 原字段名 `start_time` 在抢占时被更新为 `NOW()`，用作超时判断的基准。
- 任务被重置（PENDING）时，`start_time` 保持不变（保留原值用于排查），但超时判断仅针对 PROCESSING 状态，故不影响逻辑。

### 关于 `status` 枚举

- 请确认你系统中 status 的枚举值与本文档一致（0=PENDING，1=PROCESSING，2=SUCCESS，3=FAILED）。如不一致，请替换相应数字。
- 此 Spec 已完全适配你现有的表结构（PostgreSQL，start_time，status），可直接交付给 AI Agent 实现。 🚀
