# CTAS Data Routing to Shards — Detailed Design

## Проблема

При разбиении на per-shard/per-node WriteActor'ы нужно обеспечить что каждая строка попадёт в правильный шард. Column Shards партиционируются по primary key с использованием hash-функции (обычно `HASH_FUNCTION_CONSISTENCY_64`).

## Текущая архитектура маршрутизации

В текущей архитектуре **один WriteActor** содержит `TShardedWriteController` который:
1. Получает все строки от ComputeActor
2. Для каждой строки вычисляет hash от PK
3. Определяет target shard через `Partitioning->GetPartitionByHash(hash)`
4. Кладёт строку в per-shard буфер
5. Периодически отправляет буферы на соответствующие Column Shards

```
ComputeActor
    ↓ (все строки)
TKqpDirectWriteActor
    ↓
TShardedWriteController
    ├─ Hash(PK) → Shard[0] → Buffer[0] → Send to CS[0]
    ├─ Hash(PK) → Shard[1] → Buffer[1] → Send to CS[1]
    └─ Hash(PK) → Shard[N] → Buffer[N] → Send to CS[N]
```

Файл: [`ydb/core/kqp/runtime/kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:485)

## Новая архитектура: Маршрутизация на уровне DQ

### Вариант A: TDqCnPartitionByKey (per-shard)

```
Previous Stage (Compute)
    ↓
TDqCnPartitionByKey
    Key = PK целевой таблицы
    PartitionCount = количество шардов
    ↓          ↓          ↓
  Stage[0]  Stage[1]  Stage[N]
    ↓          ↓          ↓
  Sink[0]   Sink[1]   Sink[N]
    ↓          ↓          ↓
  CS[0]      CS[1]      CS[N]
```

#### Шаг 1: Определение ключа партиционирования

В [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162):

```cpp
// Получить PK колонки из rowType
TVector<TString> pkColumns;
for (const auto* item : rowType->GetItems()) {
    if (primariKeyColumns.contains(item->GetName())) {
        pkColumns.push_back(item->GetName());
    }
}

// Создать key selector lambda
auto keySelector = BuildKeySelectorLambda(pkColumns, pos, ctx);

// Создать PartitionByKey
auto partitionByKey = Build<TCoPartitionByKey>(ctx, pos)
    .Input(previousStageOutput)
    .KeySelectorLambda(keySelector)
    .Done();
```

#### Шаг 2: Runtime резолюция

На этапе компиляции partitioning целевой таблицы **неизвестен** (таблица ещё не создана). Решение:

**Виртуальное партиционирование**:
1. На этапе компиляции создать `TCoPartitionByKey` с неизвестным `PartitionCount`
2. На этапе выполнения (после CREATE TABLE) получить partitioning
3. `TDqCnPartitionByKey` использует runtime partitioning для маршрутизации

```cpp
// В TDqCnPartitionByKey runtime:
ui64 ComputePartition(const TCellVec& key, const TKeyDesc& partitioning) {
    ui64 hash = ConsistencyHash64(key);
    return partitioning.GetPartitionByHash(hash);
}
```

#### Шаг 3: Маршрутизация через Channel Service

DQ Channel Service ([`dq_channel_service_impl.h:269`](ydb/library/yql/dq/runtime/dq_channel_service_impl.h:269)) обеспечивает маршрутизацию:

```cpp
class TOutputDescriptor {
    void PushDataChunk(TDataChunk&& data, TNodeState* nodeState, 
                       std::shared_ptr<TOutputDescriptor> self);
    // Данные маршрутизируются по partition key
};
```

Процесс:
1. `TDqCnPartitionByKey` вычисляет hash от PK
2. Определяется target partition index
3. Данные отправляются в соответствующий input stage через channel
4. Каждый sink stage получает только данные для своего шарда

### Вариант B: Per-Node группировка (рекомендуется)

```
Previous Stage (Compute)
    ↓
TDqCnPartitionByNode
    Key = NodeId(hash(PK) → ShardId → NodeId)
    ↓          ↓          ↓
  NodeActor  NodeActor  NodeActor
   (Node A)   (Node B)   (Node C)
    ↓          ↓          ↓
  ┌──────┐  ┌──────┐  ┌──────┐
  │CS[0] │  │CS[1] │  │CS[N] │
  │CS[2] │  │CS[3] │  │CS[N+1]│
  └──────┘  └──────┘  └──────┘
  (local)    (local)    (local)
```

#### Преимущества per-node

1. **Меньше акторов**: Количество нод (обычно 10-100) << количество шардов (может быть 1000+)
2. **Локальность данных**: Внутри ноды данные не передаются по сети
3. **Проще реализовать**: Использует существующий `ShardIdToNodeId` маппинг
4. **Меньше overhead**: Нет необходимости в сложном partitioning на уровне DQ

#### Реализация per-node

**Шаг 1: Группировка шардов по нодам**

В [`TKqpTasksGraph::BuildSinks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3355):

```cpp
// Получить partitioning целевой таблицы
auto partitioning = GetTargetTablePartitioning();

// Сгруппировать шарды по нодам
THashMap<ui64, TVector<ui64>> nodeToShards;
for (const auto& partition : *partitioning) {
    ui64 shardId = partition.ShardId;
    ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
    nodeToShards[nodeId].push_back(shardId);
}

// Создать по одному sink task на каждую ноду
for (const auto& [nodeId, shards] : nodeToShards) {
    auto& task = CreateNewTask(nodeId);
    
    // Настройки sink'а для этой ноды
    NKikimrKqp::TKqpTableSinkSettings settings;
    settings.SetType(MODE_FILL);
    settings.SetInconsistentTx(true);
    settings.SetEnableStreamWrite(true);
    
    // Добавить список шардов для этой ноды
    for (ui64 shardId : shards) {
        settings.AddTargetShardIds(shardId);
    }
    
    // Установить affinity к ноде
    task.Meta.ExpectedNodeId = nodeId;
}
```

**Шаг 2: Маршрутизация данных**

В ComputeActor данные маршрутизируются по ноде:

```cpp
// В TDqCnPartitionByNode:
ui64 ComputeTargetNode(const TCellVec& pk, 
                       const TKeyDesc& partitioning,
                       const TShardIdToNodeIdMap& shardToNode) {
    ui64 hash = ConsistencyHash64(pk);
    ui64 shardId = partitioning.GetPartitionByHash(hash);
    return shardToNode.at(shardId);
}
```

**Шаг 3: WriteActor на ноде**

`TKqpDirectWriteActor` на ноде пишет во все шарды на этой ноде:

```cpp
class TKqpDirectWriteActor {
    // Список шардов на этой ноде
    TVector<ui64> TargetShardIds;
    
    void Write(IDataBatchPtr data) {
        for (auto& row : data->GetRows()) {
            // Определить целевой шард (уже на этой ноде)
            ui64 shardId = ComputeTargetShard(row.GetPK());
            
            // Проверить что шард принадлежит этой ноде
            YQL_ENSURE(TargetShardIds.contains(shardId));
            
            // Записать локально (без сетевой передачи)
            SendToLocalShard(shardId, row);
        }
    }
};
```

## Сравнение вариантов

| Критерий | Per-Shard | Per-Node |
|----------|-----------|----------|
| Количество акторов | = количеству шардов | = количеству нод |
| Сетевые передачи | Все по сети | Только между нодами |
| Локальность | Нет | Да (внутри ноды) |
| Сложность | Высокая | Средняя |
| Балансировка | Равномерная | Зависит от распределения шардов |
| Overhead | Высокий | Низкий |

## Ключевые файлы

| Файл | Роль |
|------|------|
| [`dq_opt_phy.cpp:1514`](ydb/library/yql/dq/opt/dq_opt_phy.cpp:1514) | `DqBuildPartitionStage()` — создание partition stage |
| [`kqp_opt_phy.cpp:75`](ydb/core/kqp/opt/physical/kqp_opt_phy.cpp:75) | `BuildPartitionStage` — KQP обёртка |
| [`kqp_opt_effects.cpp:162`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) | `BuildFillTableEffect()` — создать PartitionByKey + sink stages |
| [`kqp_write_actor.cpp:785`](ydb/core/kqp/runtime/kqp_write_actor.cpp:785) | `ResolveShards()` — получить partitioning |
| [`kqp_tasks_graph.cpp:3383`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3383) | `ResolveShards()` — ShardIdToNodeId маппинг |
| [`dq_channel_service_impl.h:269`](ydb/library/yql/dq/runtime/dq_channel_service_impl.h:269) | `TOutputDescriptor` — маршрутизация данных |
| [`kqp_planner.cpp:346`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346) | `AssignTasksToNodes()` — планирование задач |

## Рекомендация

Начать с **per-node** варианта:
1. Меньше рисков (меньше изменений)
2. Уже есть `ShardIdToNodeId` маппинг
3. Локальность данных внутри ноды
4. Проще тестировать
