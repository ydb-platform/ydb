# CTAS Per-Node Shard Affinity: Implementation Plan

**Цель**: Реализовать Per-Node shard affinity для CTAS (FillTable), где каждый WriteActor выполняется на ноде своих column shards.

**Механизм включения**: `PRAGMA ydb.EnableCsWriteAffinity = "true";`
- Без прагмы — текущее поведение (один WriteActor, произвольная нода)
- С прагмой — множественные WriteActors с node affinity

**Принцип**: После каждого этапа:
1. При **выключенной** прагме все существующие тесты проходят
2. При **включенной** прагме новые тесты подтверждают ожидаемое поведение

---

## Этап 1: PRAGMA + protobuf поле

**Цель**: Добавить PRAGMA `ydb.EnableCtasWriteAffinity` и передать значение в план запроса.

**Файлы**:
- `ydb/core/kqp/pragma/kqp_pragma.h` — зарегистрировать новую прагму
- `ydb/core/protos/kqp.proto` — добавить поле в `TKqpPhyTx`
- `ydb/core/kqp/query_compiler/kqp_query_compiler.cpp` — передать прагму в proto

**Изменения**:

### 1.1 Зарегистрировать PRAGMA

Аналогично `OptShuffleElimination`:
```cpp
// kqp_pragma.h
MakeBoolPrag("ydb.EnableCsWriteAffinity", &TKqpConfig::EnableCsWriteAffinity)
```

### 1.2 Добавить поле в TKqpPhyTx

```protobuf
// kqp.proto
message TKqpPhyTx {
    // ...
    optional bool enable_cs_write_affinity = XXX;
}
```

### 1.3 Передать в план

В [`kqp_query_compiler.cpp:1208`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1208):
```cpp
txProto.SetEnableCsWriteAffinity(Config->EnableCsWriteAffinity.Get().GetOrElse(false));
```

**Гарантия при выключенной прагме**: По умолчанию `false`, поведение не меняется. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_PragmaPropagates) {
    // Проверить, что прагма попадает в TKqpPhyTx
    // SELECT ... CREATE TABLE ... AS SELECT ...
    // PRAGMA ydb.EnableCsWriteAffinity = "true";
}
```

---

## Этап 2: Выделение WriteActor в отдельный stage

**Цель**: Выделить sink с WriteActor в отдельный TDqStage, чтобы он мог быть независимо распараллелен и назначен на разные ноды.

**Текущая ситуация**: В [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) sink добавляется в тот же stage, что и программа трансформации:
```cpp
auto stageInput = Build<TDqStage>(ctx, node.Pos())
    .Inputs().Add(mapCn).Build()
    .Program()
        .Args({rowArgument})
        .Body<TCoToFlow>().Input(rowArgument).Build()
        .Build()
    .Outputs().Add(sink).Build()  // Sink в том же stage, что и transform
    .Done();
```

**Целевая ситуация**: Sink вынесен в отдельный stage:
```
ComputeActor (Transform Stage)
    ↓ TDqCnMap
ComputeActor (Sink Stage) → TKqpDirectWriteActor
```

**Файлы**:
- `ydb/core/kqp/opt/kqp_opt_effects.cpp` — `BuildFillTableEffect()`

**Изменения**:

```cpp
bool BuildFillTableEffect(...) {
    // ... существующий код до создания stage ...

    // Stage 1: Transform (без sink)
    auto transformStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(mapCn).Build()
        .Program()
            .Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build()
            .Build()
        .Outputs().Add<TDqOutput>().Build()  // Просто output
        .Done();

    // Stage 2: Sink (отдельный stage)
    auto sinkInput = Build<TDqCnMap>(ctx, node.Pos())
        .Output(transformStage.Output(0))
        .Done();
    auto sinkStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(sinkInput).Build()
        .Program()
            .Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build()
            .Build()
        .Outputs().Add(sink).Build()
        .Done();

    effect = Build<TKqpSinkEffect>(ctx, node.Pos())
        .Stage(sinkStage.Ptr())  // Ссылка на sink stage
        .SinkIndex().Build("0")
        .Done();

    return true;
}
```

**Почему безопасно**:
- Функциональность не меняется — данные идут через TDqCnMap channel
- DQ framework поддерживает множественные stages
- TKqpSinkEffect ссылается на правильный stage
- Это изменение применяется **всегда** (не зависит от прагмы), но является подготовительным

**Гарантия при выключенной прагме**: Структура stages изменилась, но поведение то же. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_SeparateSinkStage) {
    // Проверить, что sink находится в отдельном stage
    // Проверить, что TDqCnMap соединяет transform и sink stages
}
```

---

## Этап 3: Прототип — поля в TKqpTableSinkSettings

**Цель**: Добавить опциональные поля `target_shard_ids` и `expected_node_id` в `TKqpTableSinkSettings`.

**Файлы**:
- `ydb/core/protos/kqp.proto`

**Изменения**:
```protobuf
message TKqpTableSinkSettings {
    // ...
    repeated uint64 target_shard_ids = 30;
    optional uint64 expected_node_id = 31;
}
```

**Гарантия при выключенной прагме**: Поля не заполняются, существующий код их не читает. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_SinkSettingsFields) {
    // Проверить, что поля target_shard_ids и expected_node_id
    // появляются в TKqpTableSinkSettings при включенной прагме
}
```

---

## Этап 4: BuildFillTableEffect — заполнить sink settings при прагме

**Цель**: В `BuildFillTableEffect()` при включенной прагме заполнить `target_shard_ids` и `expected_node_id` в sink settings.

**Файлы**:
- `ydb/core/kqp/opt/kqp_opt_effects.cpp` — `BuildFillTableEffect()`

**Изменения**:

На этапе оптимизации информация о ShardIdToNodeId **недоступна**. Поэтому на этом этапе мы:
1. Помечаем sink как "needs affinity" через заполнение `target_shard_ids` всеми шардами (или специальный маркер)
2. Оставляем `expected_node_id` пустым — он будет установлен в TasksGraph

```cpp
bool BuildFillTableEffect(...) {
    // ... существующий код ...

    if (config->EnableCsWriteAffinity) {
        // Пометить sink для дальнейшей обработки в TasksGraph
        // На этом этапе мы не знаем ShardIdToNodeId
        sinkSettings->AddTargetShardIds(SPECIAL_AFFINITY_MARKER);
    }

    // ... существующий код ...
}
```

**Гарантия при выключенной прагме**: Код не выполняется. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_SinkMarkedForAffinity) {
    // Проверить, что sink помечен для affinity обработки
}
```

---

## Этап 5: TasksGraph — создать множественные задачи для sink stage

**Цель**: В `TKqpTasksGraph` при обнаружении помеченного sink создать множественные задачи (по одной на ноду) с правильным `ExpectedNodeId`.

**Файлы**:
- `ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp` — `CountComputeTasks()`, `BuildComputeTasks()`
- `ydb/core/kqp/executer_actor/kqp_tasks_graph.h` — `TStageInfo`

**Изменения**:

### 4.1 Обнаружить sink с affinity

В `FillStages()` или `BuildSinks()`:
```cpp
bool IsCtasWriteAffinityEnabled(const TStageInfo& stageInfo) {
    // Проверить txProto.EnableCsWriteAffinity
    // и наличие маркера в sink settings
}
```

### 4.2 Разрешить шарды и ноды

Используем существующий `ResolveShards()` для получения ShardIdToNodeId:
```cpp
// После ResolveShards() имеем:
// GetMeta().ShardIdToNodeId — маппинг shard → node
```

### 4.3 Сгруппировать шарды по нодам

```cpp
THashMap<ui64, TVector<ui64>> NodeToShards;
for (const auto& [shardId, nodeId] : GetMeta().ShardIdToNodeId) {
    NodeToShards[nodeId].push_back(shardId);
}
```

### 4.4 Создать множественные задачи

В `CountComputeTasks()` / `BuildComputeTasks()` для sink stage:
```cpp
if (IsCtasWriteAffinityEnabled(stageInfo)) {
    // Создать по одной задаче на каждую ноду с шардами
    for (const auto& [nodeId, shards] : NodeToShards) {
        auto& task = AddTask(stageInfo);
        task.Meta.ExpectedNodeId = nodeId;
        task.Meta.TaskParams["target_shard_ids"] = SerializeShards(shards);
    }
} else {
    // Существующее поведение — одна задача
    CountComputeTasks(stageInfo, nodesCount);
}
```

### 4.5 Установить ExpectedNodeId

Через существующий механизм [`TMaxTasksGraph::PlaceTasks()`](ydb/core/kqp/executer_actor/max_tasks_graph.h:57), который stamp-ит `Meta.ExpectedNodeId`.

**Гарантия при выключенной прагме**: `IsCtasWriteAffinityEnabled() = false`, существующий путь. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_MultipleTasksCreated) {
    // Проверить, что создано M задач (где M = количество нод с шардами)
    // Каждая задача имеет ExpectedNodeId = nodeId соответствующих шардов
}

UNIT_TEST(CTAS_WriteAffinity_TaskShardAssignment) {
    // Проверить, что target_shard_ids в каждой задаче
    // соответствуют шардам на ExpectedNodeId
}
```

---

## Этап 6: Планировщик — affinity уже поддерживается

**Цель**: Убедиться, что `TKqpPlanner::AssignTasksToNodes()` корректно обрабатывает задачи с `ExpectedNodeId`.

**Файлы**: Проверка, изменения не нужны

**Проверка**: В существующем коде ([`kqp_planner.cpp:346`](ydb/core/kqp/executer_actor/kqp_planner.cpp:346)):
```cpp
for (const auto& task : TasksGraph.GetTasks()) {
    if (task.Meta.ExpectedNodeId) {
        TasksPerNode[*task.Meta.ExpectedNodeId].emplace_back(task.Id);
    } else {
        UnassignedTasks.emplace_back(task.Id);
    }
}
```

**Вывод**: Планировщик уже поддерживает affinity. Изменения не нужны.

**Гарантия при выключенной прагме**: Без изменений. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_TasksAssignedToCorrectNodes) {
    // Проверить, что sink задачи назначены на ноды своих шардов
}
```

---

## Этап 7: WriteActor — фильтровать шарды по target_shard_ids

**Цель**: В `TKqpDirectWriteActor` при наличии `target_shard_ids` писать только в указанные шарды.

**Файлы**:
- `ydb/core/kqp/runtime/kqp_write_actor.cpp`

**Изменения**:

### 6.1 Прочитать target_shard_ids

При инициализации WriteActor:
```cpp
// Из task params или sink settings
if (!TargetShardIds.empty()) {
    // Фильтровать resolved shards
    for (auto& shard : ResolvedShards) {
        if (!Contains(TargetShardIds, shard.ShardId)) {
            continue; // Пропустить шард не в нашем списке
        }
    }
}
```

### 6.2 Валидация

```cpp
for (const auto& shard : ResolvedShards) {
    YQL_ENSURE(Contains(TargetShardIds, shard.ShardId),
        "Shard " << shard.ShardId << " not in target_shard_ids");
}
```

**Гарантия при выключенной прагме**: `TargetShardIds` пустой, поведение не меняется. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_WriteActorFiltersShards) {
    // Проверить, что WriteActor пишет только в свои шарды
}

UNIT_TEST(CTAS_WriteAffinity_DataIntegrity) {
    // CTAS с прагмой → проверить, что все данные записаны корректно
    // Ни одна строка не потеряна, ни одна не дублирована
}
```

---

## Этап 7: Роутинг данных — TDqCnPartition

**Цель**: Убедиться, что данные из предыдущего stage корректно маршрутизируются к правильным sink задачам.

**Файлы**:
- `ydb/core/kqp/opt/kqp_opt_effects.cpp` — `BuildFillTableEffect()`
- `ydb/library/yql/dq/opt/dq_opt_phy.cpp` — `DqBuildPartitionStage()`

**Изменения**:

При включенной прагме нужно обеспечить, чтобы строка с определённым PK попала в ту задачу, где находится соответствующий шард.

**Ключевой инсайт**: В текущей модели DQ framework автоматически маршрутизирует данные через каналы между задачами. Если sink stage имеет M задач, то предыдущий stage тоже должен иметь M задач, и канал между ними должен использовать partition routing.

**Подвариант A**: Использовать существующий `TDqCnPartitionByKey` с key = hash(PK) → nodeId
**Подвариант B**: Использовать `TDqCnMap` (1:1 mapping) если предыдущий stage уже имеет правильное количество задач

**Рекомендуемый подход**: Подвариант B — если sink stage имеет M задач, то DQ framework автоматически создаст M задач для предыдущего stage через COPY механизм в `TMaxTasksGraph`.

**Гарантия при выключенной прагме**: Один sink task, один предыдущий task. Все тесты проходят.

**Новые тесты при включенной прагме**:
```cpp
UNIT_TEST(CTAS_WriteAffinity_DataRouting) {
    // Вставить данные с известными PK
    // Проверить, что каждая строка записана в правильный шард
}

UNIT_TEST(CTAS_WriteAffinity_FullPipeline) {
    // Полный тест CTAS с прагмой:
    // 1. Создать таблицу с несколькими шардами на разных нодах
    // 2. Выполнить CTAS с прагмой
    // 3. Проверить целостность данных
    // 4. Проверить, что WriteActors были на правильных нодах
}
```

---

## Этап 9: Комплексное тестирование

**Цель**: Полная валидация функциональности.

**Тесты**:

### 9.1 Регрессия без прагмы
```bash
./ya make --build relwithdebinfo ydb/core/kqp/ut -tA -F *OlapCreateAsSelect*
```
Все существующие тесты проходят.

### 9.2 Новые тесты с прагмой
```cpp
UNIT_TEST(CTAS_WriteAffinity_SingleNode) {
    // Таблица с 1 шардом → 1 WriteActor
}

UNIT_TEST(CTAS_WriteAffinity_MultiNode) {
    // Таблица с K шардами на M нодах → M WriteActors
}

UNIT_TEST(CTAS_WriteAffinity_LargeData) {
    // Большой объём данных → проверить производительность и целостность
}

UNIT_TEST(CTAS_WriteAffinity_Rebalance) {
    // Перераспределение шардов → проверить корректность маршрутизации
}
```

### 9.3 Интеграционные тесты
- Реальный кластер с несколькими нодами
- CTAS с разными размерами таблиц
- Проверка логов на корректное расположение WriteActors

---

## Порядок выполнения и зависимости

```
Этап 1 (PRAGMA + proto поле в TKqpPhyTx)
    ↓  прагма доступна, по умолчанию false
Этап 2 (Выделение WriteActor в отдельный stage)
    ↓  sink в отдельном stage, поведение не меняется
Этап 3 (Поля в TKqpTableSinkSettings)
    ↓  поля доступны, по умолчанию пустые
Этап 4 (BuildFillTableEffect — пометить sink)
    ↓  sink помечен только при прагме
Этап 5 (TasksGraph — множественные задачи)
    ↓  множественные задачи только при прагме
Этап 6 (Планировщик — проверка)
    ↓  affinity уже работает
Этап 7 (WriteActor — фильтровать шарды)
    ↓  фильтрация только при target_shard_ids
Этап 8 (Роутинг данных)
    ↓  корректная маршрутизация
Этап 9 (Комплексное тестирование)
```

---

## Гарантии после каждого этапа

| Этап | Прагма выключена | Прагма включена |
|------|-----------------|-----------------|
| 1 | Все тесты проходят | PRAGMA попадает в план |
| 2 | Все тесты проходят | Sink в отдельном stage |
| 3 | Все тесты проходят | Поля доступны в proto |
| 4 | Все тесты проходят | Sink помечен для affinity |
| 5 | Все тесты проходят | M задач с ExpectedNodeId |
| 6 | Все тесты проходят | Задачи на правильных нодах |
| 7 | Все тесты проходят | WriteActor пишет в свои шарды |
| 8 | Все тесты проходят | Данные маршрутизируются корректно |
| 9 | Все тесты проходят | Полный цикл работает |

---

## Риски и митигация

| Риск | Митигация |
|------|-----------|
| ShardIdToNodeId недоступен на этапе оптимизации | Создаём задачи в TasksGraph, где информация доступна |
| Несоответствие роутинга и assignment | DQ framework автоматически синхронизирует задачи через COPY |
| Перегрузка при большом количестве нод | Per-Node: K = M (нод), обычно 10-100 |
| Регрессия существующей функциональности | PRAGMA по умолчанию false |
| Потеря данных | Валидация в WriteActor, тесты на целостность |

---

## Критерии завершения

1. Все существующие тесты проходят без прагмы
2. Все существующие тесты проходят с прагмой
3. Новые тесты покрывают все этапы с прагмой
4. Данные корректно записываются в целевую таблицу
5. WriteActor'ы выполняются на нодах своих шардов
6. Нет деградации производительности без прагмы
7. Улучшение производительности с прагмой (из-за локальности)
