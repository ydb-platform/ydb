# CTAS Generalized Shard Assignment Design

## Текущая реализация

### Архитектура акторов и стейджей

В текущей реализации CTAS создаётся **один Sink Stage** с **одним WriteActor**, который пишет во все шарды целевой таблицы.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Cluster Nodes                                   │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Node A     │    │   Node B     │    │   Node C     │              │
│  │              │    │              │    │              │              │
│  │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │              │
│  │  │ CS[0]  │  │    │  │ CS[1]  │  │    │  │ CS[2]  │  │              │
│  │  │ CS[3]  │  │    │  │ CS[4]  │  │    │  │ CS[5]  │  │              │
│  │  └────────┘  │    │  └────────┘  │    │  └────────┘  │              │
│  │       ↑      │    │       ↑      │    │       ↑      │              │
│  └───────┼──────┘    └───────┼──────┘    └───────┼──────┘              │
│          │ network           │ network           │ network              │
│          │                   │                   │                      │
│  ┌───────┴───────────────────┴───────────────────┴──────┐              │
│  │              Node D (arbitrary)                       │              │
│  │                                                      │              │
│  │  ┌────────────────────────────────────────────────┐  │              │
│  │  │  ComputeActor (Stage N: Transform)             │  │              │
│  │  │  ┌──────────────────────────────────────────┐  │  │              │
│  │  │  │  Program: CoFlatMap / CoMap / ...        │  │  │              │
│  │  │  └──────────────────────────────────────────┘  │  │              │
│  │  └──────────────────────┬─────────────────────────┘  │              │
│  │                         │ all rows                    │              │
│  │  ┌──────────────────────┴─────────────────────────┐  │              │
│  │  │  ComputeActor (Stage N+1: Sink)                │  │              │
│  │  │  ┌──────────────────────────────────────────┐  │  │              │
│  │  │  │  TKqpDirectWriteActor                    │  │  │              │
│  │  │  │  ┌────────────────────────────────────┐  │  │  │              │
│  │  │  │  │  TShardedWriteController           │  │  │  │              │
│  │  │  │  │  ┌──────────────────────────────┐  │  │  │  │              │
│  │  │  │  │  │ Per-Shard Buffers            │  │  │  │  │              │
│  │  │  │  │  │  ├─ Shard[0] → Buffer[0]    │  │  │  │  │              │
│  │  │  │  │  │  ├─ Shard[1] → Buffer[1]    │  │  │  │  │              │
│  │  │  │  │  │  ├─ Shard[2] → Buffer[2]    │  │  │  │  │              │
│  │  │  │  │  │  ├─ ...                      │  │  │  │  │              │
│  │  │  │  │  │  └─ Shard[N] → Buffer[N]    │  │  │  │  │              │
│  │  │  │  │  └──────────────────────────────┘  │  │  │  │              │
│  │  │  │  │         ↓ route by hash(PK)        │  │  │  │              │
│  │  │  │  └────────────────────────────────────┘  │  │  │              │
│  │  │  └──────────────────────────────────────────┘  │  │              │
│  │  └─────────────────────────────────────────────────┘  │              │
│  └────────────────────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Ключевые характеристики текущей реализации

| Параметр | Значение |
|----------|----------|
| **Стейджей** | 1 Sink Stage |
| **WriteActor'ов** | 1 TKqpDirectWriteActor |
| **TShardedWriteController** | 1 (хранит все per-shard буферы) |
| **Расположение** | Произвольная нода (planner решает) |
| **Маршрутизация** | Внутри WriteActor: hash(PK) → ShardId → Buffer |
| **Сетевые передачи** | Все данные идут по сети к шардам |

### Проблемы текущей реализации

1. **Нет локальности**: WriteActor может быть на любой ноде, данные всегда идут по сети
2. **Все буферы в одном месте**: Память всех per-shard буферов в одном акторе
3. **Один актор = bottleneck**: Один WriteActor обрабатывает все записи
4. **Нет affinity**: Planner не учитывает расположение шардов при планировании

### Код текущей реализации

**Создание Sink Stage**: [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162)
```cpp
// Создаёт один stage с одним sink
auto stageInput = Build<TDqStage>(ctx, node.Pos())
    .Inputs().Add(mapCn).Build()
    .Program()
        .Args({rowArgument})
        .Body<TCoToFlow>().Input(rowArgument).Build()
        .Build()
    .Outputs().Add(sink).Build()  // Один sink
    .Done();
```

**Создание WriteActor**: [`RegisterKqpWriteActor()`](ydb/core/kqp/runtime/kqp_write_actor.cpp:6684)
```cpp
// Создаёт один WriteActor
auto* actor = new TKqpDirectWriteActor(std::move(settings), std::move(args), counters);
```

**Маршрутизация внутри WriteActor**: [`TShardedWriteController`](ydb/core/kqp/runtime/kqp_write_actor.cpp:485)
```cpp
// Все буферы в одном контроллере
ShardedWriteController = CreateShardedWriteController(...);
// Маршрутизация по hash(PK)
ShardedWriteController->Write(token, std::move(data));
```

---

## Обобщённая модель

### Базовая абстракция

**Shard Assignment** — это отображение от пишущего стейджа к предопределённому набору шардов:

```
ShardAssignment: WriteStage → Set<ShardId>
```

Каждый пишущий стейдж (WriteStage) имеет:
- **TargetShards**: Множество шардов, в которые этот стейдж пишет
- **ExpectedNodeId**: Нода, на которой должен выполняться этот стейдж (опционально)

### Общая архитектура (предлагаемая)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Cluster Nodes                                   │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Node A     │    │   Node B     │    │   Node C     │              │
│  │              │    │              │    │              │              │
│  │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │              │
│  │  │ CS[0]  │  │    │  │ CS[1]  │  │    │  │ CS[2]  │  │              │
│  │  │ CS[3]  │  │    │  │ CS[4]  │  │    │  │ CS[5]  │  │              │
│  │  └────────┘  │    │  └────────┘  │    │  └────────┘  │              │
│  │       ↑ local│    │       ↑ local│    │       ↑ local│              │
│  │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │              │
│  │  │WriteActor│    │  │WriteActor│    │  │WriteActor│  │              │
│  │  │Stage[0] │    │  │Stage[1] │    │  │Stage[2] │  │              │
│  │  │Sink[0]  │    │  │Sink[1]  │    │  │Sink[2]  │  │              │
│  │  └────────┘  │    │  └────────┘  │    │  └────────┘  │              │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘              │
│         │                   │                   │                       │
│         └───────────────────┼───────────────────┘                       │
│                     TDqCnPartitionByAssignment                          │
│                     Route: hash(PK) → ShardId → StageIdx               │
│                            ↓                                            │
│              ┌──────────────────────────────┐                          │
│              │  ComputeActor (Transform)    │                          │
│              │  Stage N                     │                          │
│              └──────────────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────┘

Легенда:
  ────  Локальная передача (внутри ноды, без сети)
  ────  Передача по сети (между нодами)
```

### Сравнение с текущей архитектурой

| Аспект | Текущая | Предлагаемая |
|--------|---------|-------------|
| **Стейджей** | 1 Sink Stage | K Sink Stages |
| **WriteActor'ов** | 1 | K |
| **Расположение** | Произвольная нода | На ноде шардов (affinity) |
| **Маршрутизация** | Внутри WriteActor | На уровне DQ (TDqCnPartition) |
| **Сетевые передачи** | Все по сети | Локальные внутри ноды |
| **Буферы** | Все в одном акторе | Распределены по акторам |

Где:
- `Shards[i] ∩ Shards[j] = ∅` для `i ≠ j` (дисъюнктные множества)
- `∪ Shards[i] = AllShards` (покрытие всех шардов)
- `K` — количество пишущих стейджей (параметр конфигурации)

### Формальное определение

```
Given:
  S = {s₁, s₂, ..., sₙ} — множество всех шардов целевой таблицы
  P: S → NodeId — маппинг шарда на ноду (ShardIdToNodeId)
  A: S → {0, 1, ..., K-1} — assignment функция

Define:
  StageShards[i] = {s ∈ S | A(s) = i} — шарды i-го стейджа
  StageNode[i] = f({P(s) | s ∈ StageShards[i]}) — нода i-го стейджа

Constraints:
  1. ∀s ∈ S: ∃!i such that s ∈ StageShards[i] (каждый шард точно в одном стейдже)
  2. StageNode[i] выбирается так, чтобы максимизировать локальность
```

### Функция маршрутизации строк

```cpp
// Для каждой строки определить целевой стейдж:
ui32 RouteRowToStage(const TCellVec& pk,
                     const TKeyDesc& partitioning,
                     const TShardAssignment& assignment) {
    ui64 hash = ConsistencyHash64(pk);
    ui64 shardId = partitioning.GetPartitionByHash(hash);
    return assignment.GetStageForShard(shardId);
}
```

---

## Вариант A: Per-Shard (K = N, каждый стейдж = один шард)

### Определение assignment

```
A(s) = s  (или индекс шарда)
K = N (количество шардов)
StageShards[i] = {sᵢ} (один шард)
StageNode[i] = P(sᵢ) (нода шарда)
```

### Архитектура

```
Previous Stage
    ↓
TDqCnPartitionByKey
    Key = PK
    PartitionCount = N (шардов)
    ↓      ↓      ↓      ↓
  Stage[0] Stage[1] ... Stage[N-1]
    ↓      ↓            ↓
  Sink[0] Sink[1]   Sink[N-1]
    ↓      ↓            ↓
   CS[0]   CS[1]      CS[N-1]
   Node A  Node B      Node C
```

### Параметры

| Параметр | Значение |
|----------|----------|
| K (стейджей) | N (количество шардов) |
| StageShards[i] | {sᵢ} |
| StageNode[i] | P(sᵢ) |
| Локальность | 100% (один шард на ноде стейджа) |
| Overhead | Высокий (N акторов) |

### Плюсы и минусы

**Плюсы:**
- Максимальная гранулярность
- Каждый стейдж на ноде своего шарда
- Простая семантика

**Минусы:**
- Много акторов (N может быть 1000+)
- Высокий overhead на создание/управление акторами
- Сложная балансировка

---

## Вариант B: Per-Node (K = M, каждый стейдж = все шарды на ноде)

### Определение assignment

```
A(s) = P(s)  (нода шарда)
K = M (количество уникальных нод)
StageShards[i] = {s ∈ S | P(s) = Nodeᵢ} (все шарды на ноде i)
StageNode[i] = Nodeᵢ
```

### Архитектура

```
Previous Stage
    ↓
TDqCnPartitionByNode
    Key = NodeId(ShardId(Hash(PK)))
    PartitionCount = M (нод)
    ↓          ↓          ↓
  Stage[0]  Stage[1]  Stage[M-1]
    ↓          ↓          ↓
  Sink[0]   Sink[1]   Sink[M-1]
    ↓          ↓          ↓
  ┌──────┐  ┌──────┐  ┌──────┐
  │CS[0] │  │CS[1] │  │CS[N-1]│
  │CS[2] │  │CS[3] │  │       │
  └──────┘  └──────┘  └──────┘
  Node A    Node B    Node C
  (local)   (local)   (local)
```

### Параметры

| Параметр | Значение |
|----------|----------|
| K (стейджей) | M (количество нод) |
| StageShards[i] | {s ∈ S \| P(s) = Nodeᵢ} |
| StageNode[i] | Nodeᵢ |
| Локальность | 100% (все шарды на ноде стейджа) |
| Overhead | Низкий (M обычно 10-100) |

### Плюсы и минусы

**Плюсы:**
- Мало акторов (M << N)
- Локальность данных внутри ноды
- Простая реализация через ShardIdToNodeId
- Низкий overhead

**Минусы:**
- Внутри стейджа нужна маршрутизация по шардам (но локальная)
- Зависит от распределения шардов по нодам

---

## Сравнение через обобщённую модель

| Критерий | Per-Shard (A) | Per-Node (B) |
|----------|---------------|--------------|
| **Assignment** | `A(s) = shard_index(s)` | `A(s) = P(s)` |
| **K** | N (шардов) | M (нод) |
| **StageShards[i]** | `{sᵢ}` | `{s \| P(s) = Nodeᵢ}` |
| **StageNode[i]** | `P(sᵢ)` | `Nodeᵢ` |
| **Локальность** | 100% | 100% |
| **Акторов** | N | M |
| **Сложность** | Высокая | Средняя |

---

## Реализация обобщённой модели

### 1. Прототип настройки

```protobuf
message TKqpTableSinkSettings {
    // ... существующие поля ...

    // Shard assignment
    repeated uint64 target_shard_ids = 100;  // Шарды этого стейджа
    optional uint64 expected_node_id = 101;   // Нода для этого стейджа
}
```

### 2. Создание стейджей (общий алгоритм)

В [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162):

```cpp
// Общий алгоритм создания пишущих стейджей:
// 1. Получить partitioning целевой таблицы
auto partitioning = GetTargetTablePartitioning();

// 2. Вычислить assignment
auto assignment = ComputeShardAssignment(partitioning, strategy);
// strategy = PER_SHARD | PER_NODE

// 3. Для каждого стейджа создать TDqStage с TDqSink
for (ui32 stageIdx = 0; stageIdx < assignment.GetStageCount(); ++stageIdx) {
    auto stageShards = assignment.GetStageShards(stageIdx);
    auto stageNode = assignment.GetStageNode(stageIdx);

    auto sink = Build<TDqSink>(ctx, pos)
        .Settings<TKqpTableSinkSettings>()
            .Table(table)
            .Mode(ctx.NewAtom(pos, "fill_table"))
            .InconsistentWrite(ctx.NewAtom(pos, "true"))
            .StreamWrite(ctx.NewAtom(pos, "true"))
            // Shard assignment:
            .TargetShardIds(stageShards)
            .ExpectedNodeId(stageNode)
            .Build()
        .Done();

    auto stage = Build<TDqStage>(ctx, pos)
        .Inputs().Add(partitionConnection).Build()
        .Program()
            .Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build()
            .Build()
        .Outputs().Add(sink).Build()
        .Done();

    sinkStages.push_back(stage.Ptr());
}
```

### 3. Функция assignment

```cpp
class TShardAssignment {
public:
    virtual ~TShardAssignment() = default;

    virtual ui32 GetStageCount() const = 0;
    virtual TVector<ui64> GetStageShards(ui32 stageIdx) const = 0;
    virtual ui64 GetStageNode(ui32 stageIdx) const = 0;
    virtual ui32 GetStageForShard(ui64 shardId) const = 0;
};

// Per-Shard implementation
class TPerShardAssignment : public TShardAssignment {
    const TKeyDesc& Partitioning;
    const TShardIdToNodeIdMap& ShardToNode;

public:
    ui32 GetStageCount() const override {
        return Partitioning.Size();
    }

    TVector<ui64> GetStageShards(ui32 stageIdx) const override {
        return {Partitioning.GetShardId(stageIdx)};
    }

    ui64 GetStageNode(ui32 stageIdx) const override {
        return ShardToNode.at(Partitioning.GetShardId(stageIdx));
    }

    ui32 GetStageForShard(ui64 shardId) const override {
        return Partitioning.GetShardIndex(shardId);
    }
};

// Per-Node implementation
class TPerNodeAssignment : public TShardAssignment {
    const TKeyDesc& Partitioning;
    const TShardIdToNodeIdMap& ShardToNode;
    THashMap<ui64, TVector<ui64>> NodeToShards;
    THashMap<ui64, ui32> NodeToStageIdx;

public:
    TPerNodeAssignment(const TKeyDesc& p, const TShardIdToNodeIdMap& s)
        : Partitioning(p), ShardToNode(s) {
        // Build node→shards mapping
        for (ui32 i = 0; i < p.Size(); ++i) {
            ui64 shardId = p.GetShardId(i);
            ui64 nodeId = s.at(shardId);
            NodeToShards[nodeId].push_back(shardId);
        }

        // Assign stage indices to nodes
        ui32 idx = 0;
        for (auto& [nodeId, _] : NodeToShards) {
            NodeToStageIdx[nodeId] = idx++;
        }
    }

    ui32 GetStageCount() const override {
        return NodeToShards.size();
    }

    TVector<ui64> GetStageShards(ui32 stageIdx) const override {
        // Find node with this stage index
        for (auto& [nodeId, idx] : NodeToStageIdx) {
            if (idx == stageIdx) {
                return NodeToShards.at(nodeId);
            }
        }
        return {};
    }

    ui64 GetStageNode(ui32 stageIdx) const override {
        for (auto& [nodeId, idx] : NodeToStageIdx) {
            if (idx == stageIdx) {
                return nodeId;
            }
        }
        return 0;
    }

    ui32 GetStageForShard(ui64 shardId) const override {
        ui64 nodeId = ShardToNode.at(shardId);
        return NodeToStageIdx.at(nodeId);
    }
};
```

### 4. Планирование задач

В [`TKqpTasksGraph::BuildSinks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3355):

```cpp
void TKqpTasksGraph::BuildSinks(...) const {
    // Для FillTable:
    if (settings.GetType() == MODE_FILL) {
        // Получить assignment из settings
        const auto& targetShards = settings.GetTargetShardIds();
        const ui64 expectedNode = settings.GetExpectedNodeId();

        if (expectedNode) {
            newTask.Meta.ExpectedNodeId = expectedNode;
        }

        // Сохранить target shards для runtime
        newTask.TargetShards = targetShards;
    }
}
```

### 5. Планировщик

В [`TKqpPlanner::AssignTasksToNodes()`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346):

```cpp
std::unique_ptr<IEventHandle> TKqpPlanner::AssignTasksToNodes() {
    for (const auto& task : TasksGraph.GetTasks()) {
        if (task.Meta.ExpectedNodeId) {
            // Shard-affinity task — назначаем на конкретную ноду
            TasksPerNode[*task.Meta.ExpectedNodeId].emplace_back(task.Id);
        } else {
            UnassignedTasks.emplace_back(task.Id);
        }
    }
    // ...
}
```

---

## Ключевые файлы

| Файл | Роль |
|------|------|
| [`kqp_opt_effects.cpp:162`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) | `BuildFillTableEffect()` — создать стейджи по assignment |
| [`kqp_tasks_graph.cpp:3355`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3355) | `BuildSinks()` — создать задачи с ExpectedNodeId |
| [`kqp_tasks_graph.cpp:3383`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3383) | `ResolveShards()` — ShardIdToNodeId маппинг |
| [`kqp_write_actor.cpp:785`](ydb/core/kqp/runtime/kqp_write_actor.cpp:785) | `ResolveShards()` — получить partitioning |
| [`kqp_planner.cpp:346`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346) | `AssignTasksToNodes()` — планирование с affinity |
| [`dq_opt_phy.cpp:1514`](ydb/library/yql/dq/opt/dq_opt_phy.cpp:1514) | `DqBuildPartitionStage()` — partition stage |
| [`dq_channel_service_impl.h:269`](ydb/library/yql/dq/runtime/dq_channel_service_impl.h:269) | `TOutputDescriptor` — маршрутизация |

## Рекомендация

Начать с **Per-Node** варианта как частного случая обобщённой модели:
1. `A(s) = P(s)` — простая assignment функция
2. `K = M` — мало стейджей
3. 100% локальность внутри ноды
4. Минимальные изменения в существующем коде
