# CTAS Per-Shard Routing: переиспользование `ColumnShardHashV1`

## Что такое `ColumnShardHashV1`

`ColumnShardHashV1` — существующий механизм **shuffle elimination** для ColumnShard-сканирования.
Его изначальная задача: если Scan Stage уже читает данные по нодам (один таск = один шард), то дальнейший HashShuffle к агрегирующей стадии можно заменить прямой маршрутизацией по хэшу PK в тот таск, который уже читает нужный шард.

```
┌───────────────────────────────────────────────────────────────┐
│  ColumnShardHashV1 (исходное назначение: shuffle elimination)  │
│                                                               │
│  Scan Stage (N tasks)      Agg Stage (N tasks)               │
│  ┌──────────────────┐      ┌──────────────────┐              │
│  │ Task 0 → Shard 0 │─────→│ Task 0           │              │
│  │ Task 1 → Shard 1 │─────→│ Task 1           │              │
│  │ Task N → Shard N │─────→│ Task N           │              │
│  └──────────────────┘      └──────────────────┘              │
│       hash(PK) → bucket i → TaskIndexByHash[i] = taskIdx     │
└───────────────────────────────────────────────────────────────┘
```

Параметры (`TColumnShardHashV1Params`):
| Поле | Смысл |
|------|-------|
| `SourceShardCount` | N = общее число шардов таблицы |
| `TaskIndexByHash[i]` | индекс таска для шард-бакета `i` |
| `SourceTableKeyColumnTypes` | типы колонок шардирующего ключа |

---

## Проблема: `TDqCnBroadcast` vs per-shard routing

Компилятор CTAS всегда создаёт соединение Transform → Sink как **`TDqCnBroadcast`**:

```
Transform Stage (1 task)
    │
    │  TDqCnBroadcast ← каждая строка идёт во ВСЕ N тасков
    │
    ├──→ Sink Task 0
    ├──→ Sink Task 1
    │    ...
    └──→ Sink Task N-1
```

При N тасков это создаёт **N-кратный трафик**: каждая строка копируется N раз, хотя каждый Sink Task нужна только 1/N строк.

`ColumnShardHashV1` позволяет заменить это точечной маршрутизацией.

---

## Решение: переиспользование `ColumnShardHashV1` для Sink-маршрутизации

### Общая схема

```
Transform Stage (1 task, Node D)
    │
    │  ColumnShardHashV1 HashShuffle
    │  hash(pk_cols) → bucket i → TaskIndexByHash[i] → taskIdx
    │
    ├──[ch 0]──→ Sink Task 0 (Node A)  TargetShards={CS[0]} → CS[0]
    ├──[ch 1]──→ Sink Task 1 (Node A)  TargetShards={CS[3]} → CS[3]
    ├──[ch 2]──→ Sink Task 2 (Node B)  TargetShards={CS[1]} → CS[1]
    ├──[ch 3]──→ Sink Task 3 (Node B)  TargetShards={CS[4]} → CS[4]
    ├──[ch 4]──→ Sink Task 4 (Node C)  TargetShards={CS[2]} → CS[2]
    └──[ch 5]──→ Sink Task 5 (Node C)  TargetShards={CS[5]} → CS[5]
```

Каждая строка идёт **только в один** Sink Task — тот, который владеет нужным шардом.

---

## Пошаговая реализация

### Шаг 1 — Table Resolver заполняет `CtasShardingColumns`

**Файл:** [`kqp_table_resolver.cpp:268`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:268)

Резолвер читает `ColumnTableInfo` целевой таблицы CTAS и сохраняет имена колонок шардирующего ключа в `stageInfo.Meta.CtasShardingColumns`. Это «триггер» для последующей логики.

```
┌─────────────────────────────────────────┐
│  Table Resolver                          │
│                                         │
│  entry.ColumnTableInfo                  │
│      ↓ read sharding key columns        │
│  stageInfo.Meta.CtasShardingColumns     │
│      = ["Col1", "Col2", ...]            │
└─────────────────────────────────────────┘
```

### Шаг 2 — `CountComputeTasks` создаёт N тасков

**Файл:** [`kqp_tasks_graph.cpp:4276`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4276)

Для Sink Stage создаётся **один таск на каждый шард** из `ShardIdToNodeId` (шарды, чей nodeId известен), в порядке `GetPartitions()`.

```cpp
// Один таск на шард, пиннинг к ноде шарда
for (auto& [shardId, nodeId] : shardNodes) {
    auto& task = AddTask(stageInfo);
    task.Meta.ExpectedNodeId = nodeId;  // pin to shard's node
}
```

```
┌──────────────────────────────────────────────────────────────────┐
│  Sink Stage после CountComputeTasks                               │
│                                                                  │
│  Task 0: ExpectedNodeId=A  (для Shard CS[0] на Node A)          │
│  Task 1: ExpectedNodeId=A  (для Shard CS[3] на Node A)          │
│  Task 2: ExpectedNodeId=B  (для Shard CS[1] на Node B)          │
│  Task 3: ExpectedNodeId=B  (для Shard CS[4] на Node B)          │
│  Task 4: ExpectedNodeId=C  (для Shard CS[2] на Node C)          │
│  Task 5: ExpectedNodeId=C  (для Shard CS[5] на Node C)          │
└──────────────────────────────────────────────────────────────────┘
```

### Шаг 3 — `BuildKqpStageChannels` перехватывает `kBroadcast`

**Файл:** [`kqp_tasks_graph.cpp:1447`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1447)

В `case kBroadcast:` добавлена проверка: если у Sink Stage заполнены `CtasShardingColumns` и >1 тасков → вместо `BuildBroadcastChannels` выполняется построение `ColumnShardHashV1`.

#### 3а — Построение `shardToTaskIdx`

Resolved shards в порядке `GetPartitions()` → их task-индексы:

```cpp
TVector<ui64> resolvedShardIds;
for (const auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
    if (GetMeta().ShardIdToNodeId.contains(partition.ShardId))
        resolvedShardIds.push_back(partition.ShardId);
}

THashMap<ui64, ui32> shardToTaskIdx;
for (ui32 ti = 0; ti < stageInfo.Tasks.size(); ++ti)
    shardToTaskIdx[resolvedShardIds[ti]] = ti;
```

```
Partitions order: [CS0, CS1, CS2, CS3, CS4, CS5]
resolvedShardIds: [CS0, CS1, CS2, CS3, CS4, CS5]  (все resolved)

shardToTaskIdx:
  CS0 → Task 0
  CS3 → Task 1  (порядок задан GetPartitions, не nodeId)
  CS1 → Task 2
  CS4 → Task 3
  CS2 → Task 4
  CS5 → Task 5
```

#### 3б — Построение `TaskIndexByHash[0..N-1]`

Бакет `i` соответствует шарду на позиции `i` в `GetPartitions()`:

```cpp
auto taskIndexByHash = make_shared<TVector<ui64>>(N, 0);
for (ui32 i = 0; i < N; ++i) {
    ui64 shardId = partitions[i].ShardId;
    (*taskIndexByHash)[i] = shardToTaskIdx[shardId];
}
```

```
GetPartitions() order:  [CS0, CS1, CS2, CS3, CS4, CS5]
                bucket:  [ 0,   1,   2,   3,   4,   5]
TaskIndexByHash:        [ 0,   2,   4,   1,   3,   5]

hash(pk) → bucket i → TaskIndexByHash[i] = taskIdx
```

#### 3в — Параметры записываются в Transform Stage

```cpp
// inputStageInfo = Transform Stage (upstream)
auto& transformParams = inputStageInfo.Meta.ColumnShardHashV1Params;
transformParams.SourceShardCount = N;
transformParams.TaskIndexByHash = taskIndexByHash;
transformParams.SourceTableKeyColumnTypes = keyTypes;

// Строить каналы как HashShuffle (не Broadcast)
BuildHashShuffleChannels(..., EHashShuffleFuncType::ColumnShardHashV1);
```

```
┌──────────────────────────────────────────────────────────────────┐
│  Transform Stage Meta после BuildKqpStageChannels                 │
│                                                                  │
│  ColumnShardHashV1Params:                                        │
│    SourceShardCount = 6                                          │
│    TaskIndexByHash  = [0, 2, 4, 1, 3, 5]                        │
│    KeyColumnTypes   = [Uint64]  (типы sharding key)             │
└──────────────────────────────────────────────────────────────────┘
```

### Шаг 4 — Сериализация в proto

**Файл:** [`kqp_tasks_graph.cpp:1664`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1664)

При `FillOutputDesc` параметры сериализуются в `NDqProto::TDqTask` Transform-таска:

```cpp
auto& columnShardHashV1 = *hashPartitionDesc.MutableColumnShardHashV1();
columnShardHashV1.SetShardCount(N);
for (ui64 taskID : *TaskIndexByHash)
    columnShardHashV1.MutableTaskIndexByHash()->Add(taskID);
// + key column types
```

### Шаг 5 — Runtime маршрутизация

Во время выполнения Transform-таск для каждой строки:
1. Вычисляет `ColumnShardHashV1(pk_columns)` → номер бакета `i`
2. Смотрит `TaskIndexByHash[i]` → `taskIdx`
3. Отправляет строку только в канал к Sink Task `taskIdx`

Sink Task получает строки только для своего шарда. `TargetShardIds={shardId}` в `TShardedWriteController` служит дополнительным guard — отбрасывает строки для чужих шардов (в норме не нужен, т.к. маршрутизация точная).

---

## Сравнение: до и после

### До (Broadcast, N=6)

```
Transform Task
    │
    ├──→ Sink Task 0  (получает ВСЕ строки, пишет только в CS0)
    ├──→ Sink Task 1  (получает ВСЕ строки, пишет только в CS3)
    ├──→ Sink Task 2  (получает ВСЕ строки, пишет только в CS1)
    ├──→ Sink Task 3  (получает ВСЕ строки, пишет только в CS4)
    ├──→ Sink Task 4  (получает ВСЕ строки, пишет только в CS2)
    └──→ Sink Task 5  (получает ВСЕ строки, пишет только в CS5)

Трафик: 6× размер данных
```

### После (ColumnShardHashV1 HashShuffle, N=6)

```
Transform Task
    │  hash(pk) → bucket → TaskIndexByHash[bucket] = taskIdx
    │
    ├──→ Sink Task 0  (получает строки ТОЛЬКО для CS0)
    ├──→ Sink Task 1  (получает строки ТОЛЬКО для CS3)
    ├──→ Sink Task 2  (получает строки ТОЛЬКО для CS1)
    ├──→ Sink Task 3  (получает строки ТОЛЬКО для CS4)
    ├──→ Sink Task 4  (получает строки ТОЛЬКО для CS2)
    └──→ Sink Task 5  (получает строки ТОЛЬКО для CS5)

Трафик: 1× размер данных (каждая строка идёт в один таск)
```

---

## Ключевое переиспользование

| Аспект | Исходное назначение | CTAS per-shard |
|--------|-------------------|----------------|
| **Направление** | Scan → Compute (чтение) | Transform → Sink (запись) |
| **`TaskIndexByHash[i]`** | индекс Compute-таска, читающего шард `i` | индекс Sink-таска, пишущего в шард `i` |
| **`SourceShardCount`** | число шардов источника | число шардов целевой таблицы |
| **Кто заполняет параметры** | `BuildScanTasksFromShards` | `BuildKqpStageChannels` (case kBroadcast) |
| **Где хранятся параметры** | `inputStageInfo.Meta.HashParamsByOutput` | `inputStageInfo.Meta.ColumnShardHashV1Params` |

Механизм полностью переиспользован без изменений в runtime — только точка заполнения параметров перенесена из scan-пути в sink-путь.
