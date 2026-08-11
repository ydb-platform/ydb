# CTAS Per-Node Shard Affinity: Implementation Plan

**Цель**: Реализовать Per-Node shard affinity для CTAS (FillTable), где каждый WriteActor выполняется на ноде своих column shards.

**Механизм включения**: `PRAGMA ydb.EnableCsWriteAffinity` (по умолчанию **включено**)
- По умолчанию режим **включён** (`GetOrElse(true)` в обоих местах чтения).
- Явно выключить: `PRAGMA ydb.EnableCsWriteAffinity = "false";`
- ⚠️ На данный момент включён только **Этап 2** (разделение sink в отдельный stage).
  Полноценный per-node affinity (несколько WriteActors на нодах своих шардов) появится
  после Этапов 3–8; до этого «включённый» режим лишь добавляет отдельный sink-stage.

**Принцип**: После каждого этапа:
1. При **выключенной** прагме все существующие тесты проходят
2. При **включенной** прагме новые тесты подтверждают ожидаемое поведение

---

## Текущий статус

| Этап | Название | Статус |
|------|----------|--------|
| 1 | PRAGMA + proto-поле `TKqpPhyTx.EnableCsWriteAffinity` | ✅ Выполнено |
| 2 | Выделение WriteActor в отдельный stage (по прагме) | ✅ Выполнено |
| 3 | Поля `TargetShardIds` / `ExpectedNodeId` в `TKqpTableSinkSettings` | ⬜ Не начато |
| 4 | Пометка sink для affinity в `BuildFillTableEffect()` | ⬜ Не начато |
| 5 | Множественные задачи sink stage в TasksGraph | ⬜ Не начато |
| 6 | Планировщик (проверка, изменений не требуется) | ⬜ Не начато |
| 7 | Фильтрация шардов в WriteActor | ⬜ Не начато |
| 8 | Роутинг данных | ⬜ Не начато |
| 9 | Комплексное тестирование | ⬜ Не начато |

**Реализовано на данный момент**:
- Прагма `ydb.EnableCsWriteAffinity` (по умолчанию `false`), проброшена в `TKqpPhyTx`.
- При включённой прагме sink с WriteActor выносится в отдельный `TDqStage`
  (4 stage в плане против 3 без прагмы).
- TWIN-тест `CTAS_WriteAffinity_Twin` проверяет обе ветки: число stage в плане и
  идентичность данных.
- Регрессия `*CreateAsSelect*`: 17/17 GOOD.

---

## Этап 1: PRAGMA + protobuf поле ✅ ВЫПОЛНЕНО

**Цель**: Добавить PRAGMA `ydb.EnableCsWriteAffinity` и передать значение в план запроса.

**Файлы (фактические)**:
- [`ydb/core/kqp/provider/yql_kikimr_settings.h`](ydb/core/kqp/provider/yql_kikimr_settings.h:127) — объявление настройки
- [`ydb/core/kqp/provider/yql_kikimr_settings.cpp`](ydb/core/kqp/provider/yql_kikimr_settings.cpp:173) — регистрация настройки
- [`ydb/core/protos/kqp_physical.proto`](ydb/core/protos/kqp_physical.proto:740) — поле в `TKqpPhyTx`
- [`ydb/core/kqp/query_compiler/kqp_query_compiler.cpp`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1211) — передача в proto

> **Примечание об архитектуре.** Изначально план предполагал регистрацию через
> `kqp_pragma.h`/`MakeBoolPrag`. Фактически используется механизм `TKikimrConfiguration`
> (`NCommon::TConfSetting` + `REGISTER_SETTING`), поэтому прагма читается двумя способами:
> - **на этапе оптимизации** — из `kqpCtx.Config->EnableCsWriteAffinity` (Этап 2);
> - **в TasksGraph/runtime** — из proto-поля `TKqpPhyTx.EnableCsWriteAffinity` (Этапы 4–5).

**Изменения (фактические)**:

### 1.1 Объявить и зарегистрировать настройку

```cpp
// yql_kikimr_settings.h
NCommon::TConfSetting<bool, Static> EnableCsWriteAffinity;

// yql_kikimr_settings.cpp
REGISTER_SETTING(*this, EnableCsWriteAffinity);
```

### 1.2 Добавить поле в TKqpPhyTx

```protobuf
// kqp_physical.proto
message TKqpPhyTx {
    // ...
    bool EnableCsWriteAffinity = 12;
}
```

### 1.3 Передать в план

В [`kqp_query_compiler.cpp:1211`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1211):
```cpp
txProto.SetEnableCsWriteAffinity(Config->EnableCsWriteAffinity.Get().GetOrElse(true));
```

> **Дефолт режима — `true` (включено).** Значение по умолчанию читается в двух местах и
> должно совпадать:
> - [`kqp_opt_effects.cpp:238`](ydb/core/kqp/opt/kqp_opt_effects.cpp:238) — управляет разделением stage;
> - [`kqp_query_compiler.cpp:1211`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1211) — проброс в `TKqpPhyTx`.
>
> Чтобы вернуть выключенное поведение, замените оба `GetOrElse(true)` на `GetOrElse(false)`.

**Отключение**: `PRAGMA ydb.EnableCsWriteAffinity = "false";` — план возвращается к 3 stage.
Все тесты проходят в обоих режимах.

**Тесты**: покрыто TWIN-тестом `CTAS_WriteAffinity_Twin` (см. Этап 2) — он гоняет обе
ветки прагмы и проверяет как план, так и данные.

---

## Этап 2: Выделение WriteActor в отдельный stage ✅ ВЫПОЛНЕНО

**Статус**: Реализовано. Разделение на два stage выполняется **только при включённой прагме**
`ydb.EnableCsWriteAffinity`, чтобы гарантировать неизменность поведения по умолчанию.

**Ключевые детали реализации**:
- Transform stage строится без явного `.Outputs()`; sink stage ссылается на его выход
  через `TDqCnMap.Output<TDqOutput>().Stage(transformStage).Index("0")` — тот же паттерн,
  что используется в физическом оптимизаторе.
- Для лямбды sink stage используется **отдельный** `TCoArgument` (`sinkRow`); переиспользование
  одного узла-аргумента в двух лямбдах приводит к падению на `CheckArguments()`.
- TWIN-тест `CTAS_WriteAffinity_Twin` проверяет план через Explain: **3 stage** без прагмы,
  **4 stage** с прагмой, а также идентичность записанных данных в обоих режимах.

**Цель**: Выделить sink с WriteActor в отдельный TDqStage, чтобы он мог быть независимо распараллелен и назначен на разные ноды.

**Исходная ситуация**: В [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) sink добавляется в тот же stage, что и программа трансформации (ветка `else`, сохранена как поведение по умолчанию):
```cpp
auto stageInput = Build<TDqStage>(ctx, node.Pos())
    .Inputs().Add(mapCn).Build()
    .Program()
        .Args({rowArgument})
        .Body<TCoToFlow>().Input(rowArgument).Build()
        .Build()
    .Outputs<TDqStageOutputsList>().Add(sink).Build()
    .Done();
```

**Целевая ситуация** (только при включённой прагме): Sink вынесен в отдельный stage:
```
ComputeActor (Transform Stage)
    ↓ TDqCnMap
ComputeActor (Sink Stage) → TKqpDirectWriteActor
```

**Файлы**:
- [`ydb/core/kqp/opt/kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:240) — `BuildFillTableEffect()`

**Фактические изменения** (важные отличия от исходного черновика плана отмечены ⚠️):

```cpp
const bool enableCsWriteAffinity =
    kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(false);

if (enableCsWriteAffinity) {
    // Transform stage: без явного .Outputs()
    auto transformStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(mapCn).Build()
        .Program()
            .Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build()
            .Build()
        .Settings().Build()
        .Done();

    // Соединение ссылается на выход transform stage напрямую
    auto sinkInput = Build<TDqCnMap>(ctx, node.Pos())
        .Output<TDqOutput>()
            .Stage(transformStage)
            .Index().Build("0")
            .Build()
        .Done();

    // ⚠️ Отдельный аргумент для лямбды sink stage
    const auto sinkRowArgument = Build<TCoArgument>(ctx, node.Pos())
        .Name("sinkRow").Done();

    auto sinkStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(sinkInput).Build()
        .Program()
            .Args({sinkRowArgument})
            .Body<TCoToFlow>().Input(sinkRowArgument).Build()
            .Build()
        .Outputs<TDqStageOutputsList>().Add(sink).Build()
        .Settings().Build()
        .Done();

    effect = Build<TKqpSinkEffect>(ctx, node.Pos())
        .Stage(sinkStage.Ptr())
        .SinkIndex().Build("0")
        .Done();
} else {
    // ... прежний одиночный stage (см. выше) ...
}
```

**⚠️ Уроки, не отражённые в исходном черновике плана**:
1. **Разделение условное, не безусловное.** Черновик предлагал делить stage «всегда».
   На практике разделение включается **только по прагме** — так проще гарантировать
   неизменность плана по умолчанию и не трогать многочисленные canondata-эталоны планов.
2. **Нет метода `transformStage.Output(0)`.** Соединение строится как
   `TDqCnMap.Output<TDqOutput>().Stage(transformStage).Index("0")` — стандартный паттерн
   соединения stage из физического оптимизатора. Transform stage при этом объявляется
   **без** `.Outputs()`.
3. **Нельзя переиспользовать один `TCoArgument` в двух лямбдах.** Это приводит к аборту
   на проверке `CheckArguments()` (`Fatal: ... code: 1060`). Для sink stage создаётся
   отдельный аргумент `sinkRow`.
4. **`.Add<TDqOutput>()` в списке `.Outputs()` не работает** — билдер требует ссылку на
   stage (`TDqOutput builder: Stage not defined`). Именно поэтому transform stage выхода
   в списке не объявляет.

**Гарантия при выключенной прагме**: Выполняется ветка `else` — план идентичен прежнему.
Все существующие тесты проходят.

**Реализованные тесты**:
```cpp
Y_UNIT_TEST_TWIN(CTAS_WriteAffinity_Twin, EnableCsWriteAffinity) {
    // Обе ветки прагмы:
    //  - Explain-план: 3 stage без прагмы / 4 stage с прагмой (FindPlanStages)
    //  - данные в целевой таблице идентичны в обоих режимах
}
```
Регрессия: `*CreateAsSelect*` в `ydb/core/kqp/ut/query` — 17/17 GOOD.

---

## Этап 3: Прототип — поля в TKqpTableSinkSettings

**Цель**: Добавить опциональные поля `TargetShardIds` и `ExpectedNodeId` в `TKqpTableSinkSettings`.

**Файлы**:
- [`ydb/core/protos/kqp.proto`](ydb/core/protos/kqp.proto:889) — сообщение `TKqpTableSinkSettings`
  (последнее занятое поле — `InputRowFormat = 29`, поэтому берём номера 30/31).

**Изменения**:
```protobuf
message TKqpTableSinkSettings {
    // ... поля 3..29 ...
    repeated uint64 TargetShardIds = 30;
    optional uint64 ExpectedNodeId = 31;
}
```
> Именование в стиле `PascalCase` — под остальные поля этого сообщения.

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

На этапе оптимизации информация о ShardIdToNodeId **недоступна**. Поэтому на этом этапе мы
лишь **помечаем** sink как требующий affinity; конкретные шарды и `ExpectedNodeId`
проставляются позже, в TasksGraph (Этап 5).

> **Уточнение по механизму передачи флага (учтено после Этапа 1–2).** Прагма уже доступна
> в двух местах, поэтому явный «маркер» в `TargetShardIds` **не требуется**:
> - в `BuildFillTableEffect()` — через `kqpCtx.Config->EnableCsWriteAffinity` (используется
>   уже на Этапе 2 для разделения stage);
> - в TasksGraph — через proto-поле `TKqpPhyTx.EnableCsWriteAffinity`.
>
> Рекомендуемый вариант: **не** класть спец-значение в `TargetShardIds`, а определять
> «нужен ли affinity» по `EnableCsWriteAffinity` + признаку `fill_table`-режима sink.
> Поле `TargetShardIds` заполняется реальными шардами только на Этапе 5.

```cpp
bool BuildFillTableEffect(...) {
    const bool enableCsWriteAffinity =
        kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(false);

    if (enableCsWriteAffinity) {
        // Разделение на два stage уже сделано на Этапе 2.
        // Здесь на Этапе 4 при необходимости можно добавить маркерные
        // sink settings, но предпочтительно опираться на EnableCsWriteAffinity
        // из TKqpPhyTx в TasksGraph (Этап 5).
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

### 5.1 Обнаружить sink с affinity

В `FillStages()` или `BuildSinks()`:
```cpp
bool IsCtasWriteAffinityEnabled(const TStageInfo& stageInfo) {
    // Проверить txProto.EnableCsWriteAffinity
    // и наличие маркера в sink settings
}
```

### 5.2 Разрешить шарды и ноды

Используем существующий `ResolveShards()` для получения ShardIdToNodeId:
```cpp
// После ResolveShards() имеем:
// GetMeta().ShardIdToNodeId — маппинг shard → node
```

### 5.3 Сгруппировать шарды по нодам

```cpp
THashMap<ui64, TVector<ui64>> NodeToShards;
for (const auto& [shardId, nodeId] : GetMeta().ShardIdToNodeId) {
    NodeToShards[nodeId].push_back(shardId);
}
```

### 5.4 Создать множественные задачи

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

### 5.5 Установить ExpectedNodeId

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

### 7.1 Прочитать target_shard_ids

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

### 7.2 Валидация

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

## Этап 8: Роутинг данных — TDqCnPartition

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

| Этап | Статус | Прагма выключена | Прагма включена |
|------|--------|-----------------|-----------------|
| 1 | ✅ | Все тесты проходят | PRAGMA попадает в план |
| 2 | ✅ | Все тесты проходят (17/17 CreateAsSelect) | Sink в отдельном stage (4 stage vs 3) |
| 3 | ⬜ | Все тесты проходят | Поля доступны в proto |
| 4 | ⬜ | Все тесты проходят | Sink помечен для affinity |
| 5 | ⬜ | Все тесты проходят | M задач с ExpectedNodeId |
| 6 | ⬜ | Все тесты проходят | Задачи на правильных нодах |
| 7 | ⬜ | Все тесты проходят | WriteActor пишет в свои шарды |
| 8 | ⬜ | Все тесты проходят | Данные маршрутизируются корректно |
| 9 | ⬜ | Все тесты проходят | Полный цикл работает |

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
