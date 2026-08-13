# CTAS WriteActor Per-Shard Architecture

## Проблема

При CTAS (Create Table As Select) в колонную таблицу:
- Создаётся **один** WriteActor на весь Sink Stage
- WriteActor пишет во **все** Column Shard'ы целевой таблицы
- WriteActor может быть расписан на **любую** ноду кластера
- Данные передаются по сети от WriteActor к каждому Column Shard'у

Это приводит к:
- Лишним сетевым передачам
- Неравномерной нагрузке на ноды
- Потенциальным проблемам с памятью (все буферы в одном акторе)

## Решение: Один WriteActor на каждый Column Shard

### Архитектура

```
До (текущая архитектура):

  ComputeActor (Stage N)
        ↓
  ┌─────────────────────┐
  │  TKqpDirectWriteActor│  ← Один актор на все шарды
  │  TShardedWriteCtrl  │
  │  ┌───────────────┐  │
  │  │ Per-Shard Buf │  │  ← Все буферы в одном месте
  │  └───────────────┘  │
  └─────────────────────┘
        ↓ (по сети)
  ┌──────┬──────┬──────┐
  │CS[0] │CS[1] │CS[N] │  ← Column Shards на разных нодах
  └──────┴──────┴──────┘

После (новая архитектура):

  ComputeActor (Stage N)
        ↓
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │WriteActor│    │WriteActor│    │WriteActor│  ← По одному на шард
  │  Shard 0 │    │  Shard 1 │    │  Shard N │
  └────┬─────┘    └────┬─────┘    └────┬─────┘
       ↓ (local)        ↓ (по сети)      ↓ (по сети)
  ┌──────┐         ┌──────┐         ┌──────┐
  │CS[0] │         │CS[1] │         │CS[N] │
  └──────┘         └──────┘         └──────┘
   Node A           Node B           Node C
```

### Ключевые изменения

#### 1. Разбиение Sink Stage на подзадачи

Вместо одного `TDqStage` с одним `TDqSink`, создаём:
- **Один `TDqStage` на каждый Column Shard** целевой таблицы
- Каждый stage имеет свой `TDqSink` с настройками для конкретного шарда

Файл: [`ydb/core/kqp/opt/kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162)

```cpp
// До:
bool BuildFillTableEffect(const TKqlFillTable& node, TExprContext& ctx,
    TMaybeNode<TExprBase>& effect, const i64 order)
{
    // Создаёт один stage с одним sink
    auto stageInput = Build<TDqStage>(...)
        .Outputs().Add<TDqSink>(...).Build()
        ...;
}

// После:
// Нужно создать N stages (по количеству шардов), каждый со своим sink.
// Данные из предыдущего stage распределяются через TDqCnPartitionByKey
// или аналогичный механизм.
```

#### 2. Роутинг данных по шардам

Данные из предыдущего stage нужно распределить по шардам целевой таблицы.

Варианты:

**2a. TDqCnPartitionByKey (рекомендуется)**

Использовать существующий механизм партиционирования:
```
Previous Stage
    ↓
TDqCnPartitionByKey (по PK целевой таблицы)
    ↓          ↓          ↓
  Sink[0]   Sink[1]   Sink[N]
    ↓          ↓          ↓
  CS[0]      CS[1]      CS[N]
```

**2b. Broadcast + Filter**

Каждый sink получает все строки и фильтрует по своему диапазону ключей:
```
Previous Stage
    ↓ (broadcast)
  Sink[0]   Sink[1]   Sink[N]  ← Каждый фильтрует по своему range
    ↓          ↓          ↓
  CS[0]      CS[1]      CS[N]
```

Вариант 2a предпочтительнее — данные идут только к нужному шарду.

#### 3. Настройки sink'а для конкретного шарда

В `TKqpTableSinkSettings` добавить информацию о целевом шарде:

Файл: protobuf definition (NKikimrKqp.TKqpTableSinkSettings)

```protobuf
message TKqpTableSinkSettings {
    // Существующие поля...
    
    // Новые поля для per-shard write:
    optional uint64 target_shard_id = XX;     // Конкретный shard ID
    optional bytes target_key_range_from = XX; // Диапазон ключей для этого шарда
    optional bytes target_key_range_to = XX;
}
```

#### 4. Изменения в WriteActor

Файл: [`ydb/core/kqp/runtime/kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:475)

`TKqpDirectWriteActor` становится **per-shard**:
- Не нужен `TShardedWriteController` (один шард = один контроллер)
- Пишет только в свой шард
- Не нужно маршрутизировать данные

```cpp
class TKqpDirectWriteActor {
    // До:
    IShardedWriteControllerPtr ShardedWriteController;  // Много шардов
    
    // После:
    ui64 TargetShardId;                                  // Один шард
    TActorId TargetShardActorId;                         // Прямая ссылка на шард
};
```

#### 5. Планирование задач

Файл: [`ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Каждая задача Sink Stage получает `ExpectedNodeId` на основе `TargetShardId`:

```cpp
void TKqpTasksGraph::BuildSinks(...) {
    // Для FillTable с per-shard настройками:
    if (settings.GetType() == MODE_FILL && settings.HasTargetShardId()) {
        ui64 shardId = settings.GetTargetShardId();
        ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
        newTask.Meta.ExpectedNodeId = nodeId;  // Шедулим на ноду шарда
    }
}
```

Файл: [`ydb/core/kqp/executer_actor/kqp_planner.cpp`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346)

```cpp
std::unique_ptr<IEventHandle> TKqpPlanner::AssignTasksToNodes() {
    for (const auto& task : TasksGraph.GetTasks()) {
        if (task.Meta.ExpectedNodeId) {
            // Задача с affinity — назначаем на конкретную ноду
            TasksPerNode[*task.Meta.ExpectedNodeId].emplace_back(task.Id);
        } else {
            UnassignedTasks.emplace_back(task.Id);
        }
    }
    // ...
}
```

#### 6. Создание задач для Sink Stage

Файл: [`ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:2242)

```cpp
void TKqpTasksGraph::BuildSinks(const NKqpProto::TKqpPhyStage& stage, 
                                  const TStageInfo& stageInfo, 
                                  TTaskType& task) const {
    // Для FillTable:
    // Вместо одного sink, создать по одному на каждый шард
    // Каждый sink получает настройки для своего шарда
}
```

### Пошаговый план реализации

#### Шаг 1: Добавить per-shard настройки в protobuf

Файл: `ydb/public/proto/kqp.proto` (или аналогичный)

```protobuf
message TKqpTableSinkSettings {
    // ... существующие поля ...
    
    // Per-shard write settings
    optional uint64 target_shard_id = 100;
    optional bool per_shard_write = 101;  // Флаг для включения режима
}
```

#### Шаг 2: Изменить BuildFillTableEffect()

Файл: [`ydb/core/kqp/opt/kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162)

Создать граф стейджей:
1. Получить partitioning целевой таблицы (на этапе компиляции или отложить до runtime)
2. Для каждого шарда создать отдельный `TDqStage` с `TDqSink`
3. Связать стейджи через `TDqCnPartitionByKey`

Проблема: на этапе компиляции partitioning может быть неизвестен (таблица ещё не создана).
Решение: использовать `TDqCnPartitionByKey` с runtime резолюцией.

#### Шаг 3: Изменить TKqpTasksGraph

Файл: [`ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

- [`BuildSinks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3355): создать по одной задаче на каждый шард
- Установить `ExpectedNodeId` для каждой задачи

#### Шаг 4: Изменить TKqpDirectWriteActor

Файл: [`ydb/core/kqp/runtime/kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:475)

- Убрать `TShardedWriteController`
- Добавить прямую запись в один шард
- Упростить логику (нет маршрутизации)

#### Шаг 5: Обновить RegisterKqpWriteActor

Файл: [`ydb/core/kqp/runtime/kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:6684)

```cpp
void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, 
                            TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, 
                    NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            if (settings.GetPerShardWrite()) {
                // Новый per-shard актор
                auto* actor = new TKqpPerShardWriteActor(std::move(settings), 
                                                         std::move(args), counters);
                return {actor, actor};
            } else {
                // Старый актор (для обратной совместимости)
                auto* actor = new TKqpDirectWriteActor(std::move(settings), 
                                                       std::move(args), counters);
                return {actor, actor};
            }
        });
}
```

### Альтернативный подход: PartitionByKey на уровне DQ

Вместо изменения оптимизатора, использовать существующий механизм `TDqCnPartitionByKey`:

```
Previous Stage
    ↓
TDqCnPartitionByKey (partition by target table PK)
    ↓          ↓          ↓
  Stage[0]  Stage[1]  Stage[N]  ← Каждый stage = один шард
    ↓          ↓          ↓
  Sink[0]   Sink[1]   Sink[N]
    ↓          ↓          ↓
  CS[0]      CS[1]      CS[N]
```

Этот подход:
- Не требует изменений в `BuildFillTableEffect()`
- Использует существующий механизм партиционирования
- Но требует чтобы partitioning был известен на этапе компиляции

### Риски и соображения

1. **Количество акторов**: Если целевая таблица имеет 100+ шардов, создастся 100+ WriteActor'ов
   - Митигация: группировка шардов по нодам (один актор на ноду, пишет в несколько шардов на этой ноде)

2. **Балансировка нагрузки**: Разные шарды могут получать разное количество данных
   - Митигация: мониторинг и динамическое перераспределение

3. **Обратная совместимость**: Флаг `per_shard_write` для включения нового поведения

4. **CTAS специфика**: Partitioning целевой таблицы неизвестен на этапе компиляции
   - Решение: отложить создание sink stages до runtime (после CREATE TABLE)
   - Или использовать виртуальное партиционирование

### Рекомендуемая реализация (упрощённая)

Вместо полного разбиения на per-shard акторы, сделать **per-node** акторы:

```
Previous Stage
    ↓
TDqCnPartitionByNode (по ноде, где находятся шарды)
    ↓          ↓          ↓
  NodeActor  NodeActor  NodeActor  ← По одному актору на ноду
   (Node A)   (Node B)   (Node C)
    ↓          ↓          ↓
  CS[0],     CS[1],     CS[N],    ← Несколько шардов на одной ноде
  CS[2]      CS[3]      CS[N+1]
```

Преимущества:
- Меньше акторов (количество нод << количество шардов)
- Данные локальны для ноды
- Проще реализовать
- `ShardIdToNodeId` уже доступен в [`TKqpTasksGraph`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3385)

### Файлы для изменений

| Файл | Изменение |
|------|----------|
| [`kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) | `BuildFillTableEffect()` — создать несколько sink stages |
| [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3355) | `BuildSinks()` — создать задачи per-node/per-shard |
| [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3383) | `ResolveShards()` — использовать для affinity |
| [`kqp_planner.cpp`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346) | `AssignTasksToNodes()` — учесть ExpectedNodeId |
| [`kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:475) | `TKqpDirectWriteActor` — упростить для одного шарда |
| [`kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp:6684) | `RegisterKqpWriteActor()` — добавить per-shard режим |
| `kqp.proto` | Добавить `target_shard_id` и `per_shard_write` |
