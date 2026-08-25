# CreateAsSelect_DisableDataShard Test Failure Analysis

## 1. Test Scenario

### Test: `KqpQuery::CreateAsSelect_DisableDataShard`

**File:** [`ydb/core/kqp/ut/query/kqp_query_ut.cpp`](ydb/core/kqp/ut/query/kqp_query_ut.cpp:2527)

**Purpose:** Tests CTAS (Create Table As Select) with `SetEnableDataShardCreateTableAs(false)`.

### Семантика `EnableDataShardCreateTableAs`

Флаг `EnableDataShardCreateTableAs` **НЕ выбирает тип таблиц**. Он контролирует, разрешено ли использовать CTAS для **не-OLAP таблиц** (row-oriented / DataShard).

Из [`kqp_statement_rewrite.cpp:409`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:409):
```cpp
if (!IsOlapCreateTableAs(node, exprCtx) && !enableDataShardCreateTableAs) {
    ++notOlapCreateTableAsCount;
}
...
if (notOlapCreateTableAsCount > 0) {
    exprCtx.AddError(..., "CTAS statement is disabled for row-oriented tables.");
    return false;
}
```

- `EnableDataShardCreateTableAs=true`: CTAS разрешен для всех типов таблиц (OLAP и row)
- `EnableDataShardCreateTableAs=false`: CTAS **только** для OLAP таблиц (STORE=COLUMN). Попытка CTAS для STORE=ROW дает ошибку.

**Тест проверяет именно это:** что при `EnableDataShardCreateTableAs=false` CTAS для row-таблиц отклоняется, а для column-таблиц работает.

### Запросы в Тесте (6 запросов)

| # | Запрос | Ожидаемый Результат | Тип |
|---|--------|---------------------|-----|
| 1 | `CREATE TABLE ... WITH (STORE = ROW) AS SELECT 1 AS Col1` | **Ошибка** "CTAS statement is disabled for row-oriented tables" | CTAS ROW |
| 2 | `CREATE TABLE ... WITH (STORE = row) AS SELECT 1 AS Col1` | **Ошибка** "CTAS statement is disabled for row-oriented tables" | CTAS ROW |
| 3 | `CREATE TABLE ... WITH (STORE = COLUMN) AS SELECT 1 AS Col1` | **Успех** | CTAS COLUMN |
| 4 | `CREATE TABLE ... WITH (STORE = column) AS SELECT 1 AS Col1` | **Успех** | CTAS COLUMN |
| 5 | `CREATE TABLE /Root/Src (Col1 Uint32 NOT NULL, PRIMARY KEY (Col1)) WITH (STORE = row)` | **Успех** | CREATE TABLE (не CTAS) |
| 6 | `CREATE TABLE ... WITH (STORE = column) AS SELECT * From /Root/Src` | **Успех** | CTAS COLUMN (из row источника) |

**Запросы 3, 4, 6** — это CTAS для column-таблиц. Они должны работать. Именно они падают с нашей ошибкой affinity routing.

**Запрос, который падает:** Первый успешный CTAS COLUMN (запрос #3):
```sql
CREATE TABLE `/Root/RowDst` (
    PRIMARY KEY (Col1)
)
WITH (STORE = COLUMN) AS
SELECT 1 AS Col1;
```

## 2. Query Execution Plan

The CTAS query produces a physical plan with the following stages:

```
Stage 0: Literal (generates constant row: Col1=1)
    |
    v (Transform/HashShuffle)
Stage 1: Sink (MODE_FILL - writes to the newly created table)
```

### Key Plan Characteristics

- **Sink Type:** `MODE_FILL` (CTAS fill operation)
- **Target Table:** Newly created column table with multiple column shards
- **IsOlap:** `true` (set by table resolver for column table sinks)
- **EnableCsWriteAffinity:** `true` (default in query compiler)
- **CsShardingColumns:** Populated by table resolver (contains primary key columns)

## 3. Where the Problem Manifests

### Error Message

```
VERIFY failed: verification=std::all_of(ActualShardIds.begin(), ActualShardIds.end(),
    [&](ui64 shardId) { return TargetShardIds->contains(shardId); });
expected={72075186224037888};actual={72075186224037949};
```

**Location:** [`ydb/core/kqp/runtime/kqp_write_table.cpp:524`](ydb/core/kqp/runtime/kqp_write_table.cpp:524)

```cpp
~TColumnShardPayloadSerializer() {
    TGuard guard(*Alloc);
    UnpreparedBatches.clear();
    Batches.clear();
    if (TargetShardIds.has_value()) {
        // ActualShardIds must be a subset of TargetShardIds
        AFL_VERIFY(std::all_of(ActualShardIds.begin(), ActualShardIds.end(),
            [&](ui64 shardId) { return TargetShardIds->contains(shardId); }))
            ("expected", GetTargetShardIdsDebugString())
            ("actual", GetActualShardIdsDebugString());
    }
}
```

### What the Error Means

- **`TargetShardIds = {72075186224037888}`**: The task was assigned shard `72075186224037888`
- **`ActualShardIds = {72075186224037949}`**: The task actually received data for shard `72075186224037949`

The task was told it owns shard A, but received data destined for shard B. This is a **routing mismatch**.

## 4. Root Cause Analysis

### The CS Write Affinity Feature

When `EnableCsWriteAffinity=true` (default), the following happens for OLAP sinks:

1. **`CountComputeTasks`** creates N tasks (one per column shard), each pinned to the node hosting that shard
2. **`BuildColumnShardHashV1ForWriteAffinity`** builds a hash routing table (`taskIndexByHash`) that maps PK hash buckets to task indices
3. **`BuildInternalSinks`** assigns `TargetShardIds` to each task (which shards the task owns)
4. At runtime, rows are routed to tasks via `ColumnShardHashV1` hash shuffle based on PK hash

### The Problem: Task Reordering by `PlaceTasks`

The critical issue is that `TMaxTasksGraph::PlaceTasks` reorders tasks in `stageInfo.Tasks` from **creation order** to **node-major order**:

**[`max_tasks_graph.cpp:528-549`](ydb/core/kqp/executer_actor/max_tasks_graph.cpp:528)**
```cpp
// Lay the surviving tasks into stageInfo.Tasks (node-major order)
std::vector<std::vector<ui64>> byNode(NodesCount());
for (size_t columnIdx = 0; columnIdx < stage.Tasks.size(); ++columnIdx) {
    const ui64 id = idMap.at(stage.Tasks[columnIdx]);
    if (const auto& node = group.ColumnNodes[columnIdx]) {
        byNode[*node].push_back(id);  // Group by node
    }
}

auto& stageTasks = stage.Info->Tasks;
stageTasks.clear();
for (TNodeIdx n = 0; n < NodesCount(); ++n) {
    for (ui64 id : byNode[n]) {
        stageTasks.push_back(id);  // Node 0 tasks first, then node 1, etc.
    }
}
```

### The Mismatch

| Component | Uses | Order |
|-----------|------|-------|
| `CountComputeTasks` | `GetColumnShards()` | Shard creation order |
| `PlaceTasks` | Groups by node | **Node-major order** |
| `BuildInternalSinks` | `stageInfo.Tasks` position | **Node-major order** (WRONG) |
| `BuildColumnShardHashV1ForWriteAffinity` | `stageInfo.Tasks` position | **Node-major order** (WRONG) |

**Before the fix:** Both `BuildInternalSinks` and `BuildColumnShardHashV1ForWriteAffinity` assumed that `stageInfo.Tasks[ti]` corresponds to `GetColumnShards()[ti]`. After `PlaceTasks`, this assumption is broken.

**After the fix:** Both functions read `CsWriteAffinityShardId` from task params (set in `CountComputeTasks`), which survives `PlaceTasks` reordering.

### Why the Fix Doesn't Fully Work for CTAS

Even after the fix, the `CreateAsSelect_DisableDataShard` test still fails. The reason:

1. **Single-node test environment:** All shards are on the same node (node 1)
2. **`PlaceTasks` still reorders:** Even on a single node, the task order in `stageInfo.Tasks` may differ from creation order due to the internal `TMaxTasksGraph` placement logic
3. **`CsWriteAffinityShardId` is set correctly:** The task params contain the correct shard ID
4. **But the hash routing still fails:** The `taskIndexByHash` table built by `BuildColumnShardHashV1ForWriteAffinity` maps hash buckets to task indices, but the task indices in `stageInfo.Tasks` don't match the expected order

### Detailed Flow for the Failing Test

1. **Table creation:** CTAS creates a column table with, say, 8 column shards
2. **`CountComputeTasks`:** Creates 8 tasks, one per shard, in `GetColumnShards()` order:
   - Task 0 → Shard 72075186224037888
   - Task 1 → Shard 72075186224037889
   - ...
   - Task N → Shard 72075186224037949
   - Each task gets `CsWriteAffinityShardId` set in `TaskParams`

3. **`PlaceTasks`:** Reorders tasks in `stageInfo.Tasks` (node-major order). On a single node, the order might still change due to internal placement logic.

4. **`BuildColumnShardHashV1ForWriteAffinity`:** Reads `CsWriteAffinityShardId` from each task and builds `shardToTaskIdx` map. This should be correct after the fix.

5. **`BuildInternalSinks`:** Reads `CsWriteAffinityShardId` from each task and sets `TargetShardIds`. This should be correct after the fix.

6. **Runtime:** Row with `Col1=1` is hashed. The hash maps to bucket i, which corresponds to shard `72075186224037949`. The `taskIndexByHash[i]` gives the task index. But the task at that index has `TargetShardIds = {72075186224037888}`.

### The Core Issue

The `taskIndexByHash` table maps hash bucket → task index. The hash bucket is determined by `hash(pk) % N` where N is the number of shards. The bucket-to-shard mapping is: bucket i → `orderedShardIds[i]` (from `GetColumnShards()`).

The `shardToTaskIdx` map is: shard ID → task index in `stageInfo.Tasks`.

If `shardToTaskIdx[orderedShardIds[i]]` gives the wrong task index, then `taskIndexByHash[i]` is wrong, and rows are routed to the wrong task.

**The fix reads `CsWriteAffinityShardId` from task params, but the task params might not be set correctly, or the task IDs in `stageInfo.Tasks` might not match the tasks that have the params set.**

## 5. Why This Test Passes on `origin/main`

On `origin/main`, the CS Write Affinity feature doesn't exist. The CTAS query uses Broadcast routing:
- Single task handles all shards
- All rows go to the single task
- No per-shard filtering needed

## 6. Possible Solutions

### Option A: Disable CS Write Affinity for CTAS (MODE_FILL)

Add a check in `CountComputeTasks` to skip the affinity path for MODE_FILL sinks:

```cpp
// Skip affinity for CTAS MODE_FILL
if (settings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_FILL) {
    isCsWriteAffinitySink = false;
}
```

**Pros:** Simple, doesn't break existing tests
**Cons:** CTAS doesn't get the affinity optimization

### Option B: Fix the Task Ordering for CTAS

Investigate why the task ordering differs for CTAS and ensure `PlaceTasks` preserves the creation order for affinity stages.

**Pros:** CTAS gets the affinity optimization
**Cons:** More complex, requires changes to `TMaxTasksGraph`

### Option C: Use Broadcast for CTAS, Affinity for Other OLAP Writes

In `BuildKqpStageChannels`, detect CTAS and use Broadcast instead of ColumnShardHashV1:

```cpp
if (isModeFill) {
    // Use Broadcast for CTAS
    return std::nullopt;
}
```

**Pros:** Targeted fix, doesn't affect other operations
**Cons:** CTAS doesn't get the affinity optimization

## 7. Files Involved

| File | Role |
|------|------|
| [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) | Task creation, routing, sink configuration |
| [`max_tasks_graph.cpp`](ydb/core/kqp/executer_actor/max_tasks_graph.cpp) | Task placement and reordering |
| [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp) | Runtime verification of shard routing |
| [`kqp_query_ut.cpp`](ydb/core/kqp/ut/query/kqp_query_ut.cpp:2527) | The failing test |
| [`kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp) | Optimizer (sets EnableCsWriteAffinity) |
| [`kqp_query_compiler.cpp`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp) | Sets EnableCsWriteAffinity in tx proto |

## 8. Timeline of Changes

1. **Initial state:** CS Write Affinity feature added, `EnableCsWriteAffinity=true` by default
2. **First fix:** Store `CsWriteAffinityShardId` on tasks in `CountComputeTasks`
3. **Second fix:** Read `CsWriteAffinityShardId` in `BuildInternalSinks` instead of positional index
4. **Third fix:** Read `CsWriteAffinityShardId` in `BuildColumnShardHashV1ForWriteAffinity` instead of positional index
5. **Current state:** The `CreateAsSelect_DisableDataShard` test still fails with shard routing mismatch

## 9. Conclusion

The `CreateAsSelect_DisableDataShard` test fails because the CS Write Affinity feature (designed for INSERT/REPLACE/UPDATE/DELETE on existing tables) is being applied to CTAS queries that create new tables. The task ordering after `PlaceTasks` doesn't match the expected shard-to-task mapping, causing rows to be routed to the wrong tasks.

The simplest fix is to disable CS Write Affinity for CTAS (MODE_FILL) queries, as they have different semantics and the affinity feature is not designed for them.
