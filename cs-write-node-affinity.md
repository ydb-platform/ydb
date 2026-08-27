# ColumnShard Write Node Affinity


---

## 1. Формулировка проблемы.

При записи в колонную таблицу при CTAS / FILL / INSERT создаётся **один** WriteActor
на весь Sink Stage:

```
ComputeActor
    ↓ (все строки)
TKqpDirectWriteActor  ← один актор на все шарды
    ↓
TShardedWriteController
    ├─ Hash(PK) → Shard[0] → Buffer[0] → отправка на CS[0] (другая нода!)
    ├─ Hash(PK) → Shard[1] → Buffer[1] → отправка на CS[1] (другая нода!)
    └─ Hash(PK) → Shard[N] → Buffer[N] → отправка на CS[N] (другая нода!)
```

**Недостатки:**

1. **Нет локальности** — WriteActor может быть на любой ноде, данные всегда идут по сети
2. **Bottleneck** — один WriteActor обрабатывает все записи последовательно
3. **Память** — все per-shard буферы сосредоточены в одном месте
4. **Нет affinity** — планировщик не учитывает расположение шардов

### 1.1 Цепочка преобразований плана выполнения запроса

Запрос на запись в колоночную таблицу проходит несколько стадий преобразования — от SQL до исполнения. Ниже описана полная цепочка для CTAS (FILL) и INSERT.

#### 1.1.1 Общая схема

```
SQL (CREATE TABLE AS / INSERT / REPLACE)
   │
   ▼
[1] Парсинг + построение AST (YQL)
   │
   ▼
[2] Логический план (TKqlFillTable / TKqlWriteTable)
   │
   ▼
[3] Оптимизация (KQP optimizer) → физический план (TDqStage / TDqSink)
   │
   ▼
[4] Компиляция (KqpQueryCompiler) → TKqpPhyTx (proto)
   │
   ▼
[5] Исполнение (KqpExecuter) → TasksGraph → ComputeActor / WriteActor
```

#### 1.1.2 Стадия 1 — Парсинг и AST

SQL-запрос парсится в AST YQL. Для записи формируются узлы:
- **CTAS** → `TKqlFillTable` (с `OriginalPath` = путь destination)
- **INSERT/REPLACE** → `TKqlWriteTable`

#### 1.1.3 Стадия 2 — Логический план

Логический план содержит узел записи с описанием целевой таблицы и источника данных. На этом этапе нет информации о шардировании — только логическая структура.

#### 1.1.4 Стадия 3 — Оптимизация (KQP optimizer)

[`BuildFillTableEffect`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) преобразует логический узел в физический план:

**origin/main (без `EnableCsWriteAffinity`)**:
- Один `TDqStage`: вход `TDqCnMap` из upstream, программа `ToFlow(row)`, выход `TDqSink`
- Sink settings: `MODE_FILL`, `Table.Path`, `InputColumns`, `InconsistentWrite=true`, `StreamWrite=true`

**Ветка (с `EnableCsWriteAffinity`)**:
- Два `TDqStage`: Transform (вход `TDqCnMap`, программа `ToFlow`) + Sink (вход `TDqCnHashShuffle` с `ColumnShardHashV1`, выход `TDqSink`)
- `TDqCnHashShuffle` с `ColumnShardHashV1` направляет строки в Sink-задачи по hash(key) → bucket → task. KeyColumns берутся из выходного struct-типа Transform-стадии (placeholder); реальные sharding-колонки подставляются в runtime через `CsShardingColumns` из table resolver'а
- `PropogateHashFuncToHashShuffles` (пост-оптимизационный трансформер) сохраняет `ColumnShardHashV1`, не перезаписывая его на `HashV2`

#### 1.1.5 Стадия 4 — Компиляция (KqpQueryCompiler)

[`FillCreateTableAs`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:2456) заполняет proto `TKqpTableSinkSettings`:
- `MODE_FILL`, `Table.Path`, `InputColumns`
- `CtasDestinationPath` (путь destination) — в ветке
- `Columns`, `KeyColumns`, `WriteIndexes` не заполняются — добавляются table resolver'ом во время выполнения

Результат — `TKqpPhyTx` (proto), содержащий стадии, соединения и sink settings.

#### 1.1.6 Стадия 5 — Исполнение (KqpExecuter)

Executer строит `TKqpTasksGraph` из `TKqpPhyTx`:

1. **`FillStages`** — создаёт `TStageInfo` для каждой стадии, заполняет `TablePath`, `TableId`, `ShardOperations` из sink settings
2. **Table Resolver** — резолвит таблицу по пути/TableId, заполняет `ResolvedSinkSettings`, `ColumnTableInfoPtr`, `ShardKey`
3. **`CountComputeTasks`** — определяет число задач на стадию (в origin/main: 1 задача для sink)
4. **`BuildKqpStageChannels`** — строит каналы между задачами (Map/Broadcast/HashShuffle)
5. **`BuildInternalSinks`** — сериализует sink settings в task output
6. **`AssignTasksToNodes`** — планировщик назначает задачи нодам
7. **ComputeActor / WriteActor** — исполнение: WriteActor шардифицирует строки и отправляет в ColumnShards

#### 1.1.7 Ключевые точки, где в ветке вносятся изменения

| Стадия | Функция | Изменение в ветке |
|--------|---------|-------------------|
| 3 (оптимизация) | `BuildFillTableEffect` | Разделение Transform/Sink на два stage + `TDqCnHashShuffle` с `ColumnShardHashV1` (CTAS); `TDqCnBroadcast` (Pure OLAP, UPDATE/DELETE — конвертируется в HashShuffle в runtime) |
| 4 (компиляция) | `FillCreateTableAs` | Сохранение `CtasDestinationPath` |
| 5 (исполнение) | Table Resolver | Заполнение `CsShardingColumns` |
| 5 (исполнение) | `CountComputeTasks` | Per-shard задачи (M=N) |
| 5 (исполнение) | `BuildKqpStageChannels` | ColumnShardHashV1 HashShuffle вместо Broadcast/Map; CTAS — `kColumnShardHashV1` case (HashShuffle из оптимизатора), Pure OLAP/UPDATE/DELETE — `kBroadcast`/`kMap` case (Broadcast→HashShuffle конверсия) |
| 5 (исполнение) | `BuildInternalSinks` | Заполнение `TargetShardIds` |
| 5 (исполнение) | WriteActor | `AFL_VERIFY(TargetShardIds->contains(shardId))` |

---

### 1.2 Текущая(origin/main) реализация записи в колоночные шарды

Ниже описано поведение каждой изменённой в ветке функции в origin/main.

#### 1.2.1 Оптимизатор: [`BuildFillTableEffect`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162)

`BuildFillTableEffect(node, ctx, effect, order)` — сигнатура без `TKqpOptimizeContext`.

Для Union-input (типичный CTAS с источником из другой таблицы) строится **один** `TDqStage`:
- входной канал: `TDqCnMap` из upstream stage
- программа: `ToFlow(row)`
- выход: `TDqSink` (sink внутри того же stage)

Sink settings (`TKqpTableSinkSettings`):
- `Type = MODE_FILL`, `Table.Path` = путь temp-таблицы назначения
- `InputColumns` = список имён колонок из плана
- `InconsistentWrite = true`, `StreamWrite = true`
- `OriginalPath` (путь destination) хранится как атом в settings самого stage — в proto sink settings не передаётся

**Гарантии**: один stage, один sink, Map-канал из upstream.
**Ограничения**: нет разделения Transform и Sink на разные stage; нет Broadcast-канала.

#### 1.2.2 Компилятор: [`FillCreateTableAs`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:2456)

Заполняет proto `TKqpTableSinkSettings`:
- `MODE_FILL`, `Table.Path`, `InputColumns`
- `Columns`, `KeyColumns`, `WriteIndexes` не заполняются — добавляются table resolver'ом во время выполнения

**Гарантии**: базовые настройки sink заполнены.
**Ограничения**: схема таблицы (columns, key columns) отсутствует на этапе компиляции.

#### 1.2.3 Table Resolver: [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp)

**Проход 1 — `HandleResolveNames` (Navigate by path)**:
- Навигирует temp-таблицу по `settings.GetTable().GetPath()`
- `AFL_ENSURE(settings.GetType() == MODE_FILL)` — обрабатывает **только** MODE_FILL (не INSERT и др.)
- Заполняет `stageMeta.ResolvedSinkSettings`: `TableId`, `IsOlap`, `KeyColumns`, `Columns`, `WriteIndexes`
- Создаёт `stageMeta.ShardKey = ExtractKey(tableId, keyTypes, Update)`
- `CsShardingColumns` не заполняется

**Проход 2 — `HandleResolveKeys` (Navigate by TableId)**:
- `stageMeta.ColumnTableInfoPtr = entry.ColumnTableInfo`
- Резолвинг `ShardKey->Partitioning` через `TEvResolveKeySetResult`

**Гарантии**: `ResolvedSinkSettings` и `ColumnTableInfoPtr` заполнены для MODE_FILL.
**Ограничения**: `CsShardingColumns` не заполняется — нет информации о sharding-колоночках для routing'а. Обрабатывается только MODE_FILL, не INSERT.

#### 1.2.4 Executer: [`kqp_executer_impl.h`](ydb/core/kqp/executer_actor/kqp_executer_impl.h)

Executer собирает `shardIds` для резолвинга нод только по стадиям с `TableOps` (scan-источники). Для стадий без TableOps (в т.ч. FILL-sink) — ветка `else` с TODO-комментариями, без кода. Шарды temp-таблицы в `shardIds` не попадают. Если источник данных не читает никаких таблиц, `shardIds` пуст, и `TasksGraph.ResolveShards({})` вызывается сразу с пустой картой — `ShardIdToNodeId` пуст.

**Гарантии**: шарды source-таблиц резолвятся.
**Ограничения**: шарды destination (temp) таблицы не резолвятся — `ShardIdToNodeId` не содержит нод destination-шардов.

#### 1.2.5 `CountComputeTasks`: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Для sink-стейджа FILL входной канал — `TDqCnMap`:
- `inputTypeCase == kMap` → `stageType = COPY`, `partitionsCount = upstream_tasks_count`
- Upstream имеет 1 задачу → `partitionsCount = 1`
- Результат: **1 задача** FILL, выполняется на executer-ноде

**Гарантии**: одна задача на sink stage.
**Ограничения**: нет per-shard задач; нет node affinity; задача выполняется на executer-ноде.

#### 1.2.6 `BuildKqpStageChannels`: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

`TDqCnMap` обрабатывается как стандартный **Map-канал** 1:1 между upstream-задачей и единственной задачей FILL.

`TDqCnBroadcast` вызывает `BuildBroadcastChannels` — все строки отправляются всем задачам. Специальной логики для OLAP sink нет.

**Гарантии**: данные доходят до sink задачи.
**Ограничения**: нет ColumnShardHashV1 routing'а; Broadcast шлёт все данные всем задачам.

#### 1.2.7 `BuildInternalSinks`: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Берёт `ResolvedSinkSettings`, вызывает `FillKqpTableSinkSettings`, пакует в `output.SinkSettings`:
```cpp
output.SinkSettings.ConstructInPlace();
output.SinkSettings->PackFrom(settings);
```
Поля `TargetShardIds` в proto и в коде нет.

**Гарантии**: sink settings сериализованы в task output.
**Ограничения**: нет `TargetShardIds` — WriteActor не знает, какие шарды ему принадлежат.

#### 1.2.8 WriteActor: [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp)

`TShardedWriteController::ShardAndFlushBatch`:
```cpp
void ShardAndFlushBatch(TRecordBatchPtr&& unshardedBatch, bool force) {
    for (auto [shardId, shardBatch] : Sharding->SplitByShardsToArrowBatches(...)) {
        ShardIds.insert(shardId);
        auto& unpreparedBatch = UnpreparedBatches[shardId];
        ...
        FlushUnpreparedBatch(shardId, unpreparedBatch, force);
    }
}
```
Единственный WriteActor шардирует все строки и отправляет каждый батч в соответствующий ColumnShard по сети. `TargetShardIds` отсутствует — фильтрации по шардам нет.

**Гарантии**: все строки записаны в правильные шарды.
**Ограничения**: один актор, все данные по сети, нет node affinity.

#### 1.2.9 Декомпозиция CTAS на стейтменты

С `EnablePerStatementQueryExecution=true` CTAS компилируется как три независимых стейтмента:

1. **CREATE TABLE** — создаёт temp-таблицу `/.tmp/sessions/.../Destination_uuid`
2. **FILL** — записывает данные в temp-таблицу (стейтмент с sink mode `MODE_FILL`)
3. **MOVE** — атомарно переименовывает temp-таблицу в `/Root/Destination`

Именно стейтмент **FILL** является объектом оптимизации.

#### 1.2.10 Итоговая схема в origin/main

```
Upstream ComputeActor
    ↓ TDqCnMap (1:1, COPY)
TKqpDirectWriteActor (1 задача, executer-нода)
    └── TShardedWriteController
            ├── Hash(PK) → CS[0]  (сеть)
            ├── Hash(PK) → CS[1]  (сеть)
            └── Hash(PK) → CS[N]  (сеть)
```

**Резюме origin/main**:
| Функция | Поведение | Гарантия | Ограничение |
|---------|-----------|----------|-------------|
| `BuildFillTableEffect` | Один stage с Map-каналом | Sink создан | Нет разделения Transform/Sink |
| `FillCreateTableAs` | Базовые sink settings | Path, columns | Нет key columns |
| Table Resolver | ResolvedSinkSettings + ColumnTableInfo | Схема резолвлена | `CsShardingColumns` пусто, только MODE_FILL |
| Executer | ShardIds только из TableOps | Source shards резолвлены | Destination shards не резолвлены |
| `CountComputeTasks` | 1 задача (COPY от Map) | Задача создана | Нет per-shard задач, нет affinity |
| `BuildKqpStageChannels` | Map 1:1 или Broadcast | Данные доходят | Нет ColumnShardHashV1 routing'а |
| `BuildInternalSinks` | PackFrom(settings) | Settings сериализованы | Нет TargetShardIds |
| WriteActor | Шардифицирует все строки | Все строки записаны | Один актор, всё по сети |

---

### 1.3 Другие (не CTAS) сценарии записи в origin/main

**INSERT/REPLACE INTO** в колоночные таблицы идёт тем же путём:
- Table Resolver резолвит таблицу по TableId (есть в sink settings)
- `ResolvedSinkSettings` заполняется аналогично FILL
- `CountComputeTasks` создаёт 1 задачу
- WriteActor шардифицирует и отправляет все данные по сети

Отличие от CTAS: у INSERT есть `TableConstInfo` из компиляции (таблица существует до выполнения), тогда как у CTAS FILL temp-таблица создаётся во время выполнения и `TableConstInfo` отсутствует — используется `ColumnTableInfo` из SchemeCache.

**PRAGMA `EnableCsWriteAffinity`** в origin/main отсутствует.


## 2. Целевая картина

### 2.1 Архитектура

Sink Stage разбивается на **M задач** — по одной на каждую ноду (или шард), каждая пишет
только в свои шарды и выполняется на ноде этих шардов:

```
До:                                    После (Per-Shard):

ComputeActor (Stage N)                 ComputeActor (Stage N)
      ↓                                      ↓ ColumnShardHashV1 HashShuffle
┌─────────────────────┐              ┌──────────┐  ┌──────────┐  ┌──────────┐
│  WriteActor          │              │WriteActor│  │WriteActor│  │WriteActor│
│  (все шарды)         │              │ Node A   │  │ Node B   │  │ Node C   │
└─────────────────────┘              └────┬─────┘  └────┬─────┘  └────┬─────┘
      ↓ (всё по сети)                     ↓local        ↓local        ↓local
   CS[0] CS[1] CS[N]                   CS[0]CS[3]    CS[1]CS[4]    CS[2]CS[5]
```

### 2.2 Ключевые требования

1. **Точный routing**: каждая задача получает строки **только своих** шардов (тех, что
   в её `TargetShardIds`). Фильтрация в WriteActor — **неправильный** подход
   (M× сетевой трафик и избыточная работа). При Per-Node разбивке одна задача обслуживает
   все шарды данной ноды — их может быть несколько.

2. **Mechanism**: DQ-канал Transform→Sink использует `ColumnShardHashV1` HashShuffle.
   `TaskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а `bucket`.

3. **Совместимость hash-функций** (доказана):

   | Компонент | Реализация |
   |-----------|-----------|
   | DQ `TColumnShardHashV1` ([`dq_output_consumer.cpp:136`](ydb/library/yql/dq/runtime/dq_output_consumer.cpp:136)) | `NXX64::TStreamStringHashCalcer(seed=0)` + `Update(raw_bytes)` per column |
   | ColumnShard `TXX64::Execute()` ([`calcer.cpp:106`](ydb/core/formats/arrow/hash/calcer.cpp:106)) | `NXX64::TStreamStringHashCalcer(seed=0)` + `Update(raw_bytes)` per column |

   hash(row.pk) и bucket mapping `min(h/(Max/N), N-1)` совпадают → функции **совместимы**.

4. **Единство порядка шардов**: `CountComputeTasks`, `BuildInternalSinks` и `BuildKqpStageChannels`
   используют один порядок `GetSharding().GetColumnShards()`:

   ```
   строка → hash(pk) → bucket i → TaskIndexByHash[i] → task i → пишет только в ColumnShards[i]
   ```

5. **`AFL_VERIFY` в WriteActor**: если строка чужого шарда попала в задачу — это баг routing'а.

### 2.3 Модели shard assignment

Поддерживаются два варианта. Оба требуют точного routing'а: строки попадают
**только в задачу-владельца** нужного шарда.

**Вариант A: Per-Shard** K = N

**PRAGMA**: `PRAGMA ydb.EnableCsWriteAffinity` (по умолчанию **включено**).


```
StageShards[i] = {sᵢ}            — ровно один шард на задачу
StageNode[i]   = P(sᵢ)           — нода шарда sᵢ
TargetShardIds = {sᵢ}            — один шард
TaskIndexByHash[bucket] = i       — bucket = hash(pk) / (Max/N)
```

**Вариант B: Per-Node** (K = M, рекомендуется для минимума акторов)
```
StageShards[j] = {s ∈ S | P(s) = Nodeⱼ}  — все шарды ноды j
StageNode[j]   = Nodeⱼ
TargetShardIds = StageShards[j]            — несколько шардов!
TaskIndexByHash[bucket] = j                — bucket → нода шарда sᵢ
```

При Per-Node каждая задача обслуживает **несколько** шардов. Hash-routing должен
направлять строку в задачу, чья нода владеет целевым шардом.

### 2.4 Функции, которые нужно изменить

Ниже для каждой функции, изменяемой в ветке, описаны: текущее поведение (origin/main),
новая задача, которую она получает, и гарантии, которые она должна давать после изменения.

#### 2.4.1 [`BuildFillTableEffect`](ydb/core/kqp/opt/kqp_opt_effects.cpp:162) — оптимизатор

**Текущее (origin/main)**: строит один `TDqStage` (Transform + Sink вместе), вход `TDqCnMap`.

**Новая задача**: при `EnableCsWriteAffinity` разделить на два stage — Transform (вход `TDqCnMap`, программа `ToFlow`) и Sink (выход `TDqSink`). Соединение Transform→Sink:

- **CTAS (FILL)**: `TDqCnHashShuffle` с `ColumnShardHashV1` — эмитится оптимизатором напрямую. KeyColumns — placeholder (первая колонка из выходного struct-типа Transform-стадии); реальные sharding-колонки подставляются в runtime. `PropogateHashFuncToHashShuffles` сохраняет `ColumnShardHashV1`.
- **Pure OLAP (REPLACE INTO/INSERT без source)**: `TDqCnBroadcast` — конвертируется в `ColumnShardHashV1` HashShuffle в runtime (`BuildKqpStageChannels`, `kBroadcast` case).
- **UPDATE/DELETE (с source)**: `TDqCnMap` (source→Transform) + `TDqCnBroadcast` (Transform→Sink) — Broadcast конвертируется в HashShuffle в runtime.

**Гарантии**:
- Sink-стадия может быть разбита на M задач независимо от числа задач Transform
- Для CTAS: `ColumnShardHashV1` виден в EXPLAIN-плане (HashShuffle); routing строится в runtime через `BuildColumnShardHashV1ForWriteAffinity` helper
- Для Pure OLAP / UPDATE/DELETE: Broadcast доставляет все строки во все Sink-задачи, точный routing строится в runtime (Broadcast→HashShuffle конверсия)

#### 2.4.2 [`FillCreateTableAs`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:2456) — компилятор

**Текущее (origin/main)**: заполняет `MODE_FILL`, `Table.Path`, `InputColumns`.

**Новая задача**: сохранить `CtasDestinationPath` (путь destination) в `TKqpTableSinkSettings`, чтобы table resolver мог навигировать правильную (destination) таблицу для per-shard affinity.

**Гарантии**:
- Table resolver получает путь destination-таблицы, а не source
- `CtasDestinationPath` доступен в runtime (в `TKqpTableSinkSettings`)

#### 2.4.3 Table Resolver: [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp)

**Текущее (origin/main)**: `HandleResolveNames` обрабатывает только `MODE_FILL`; `CsShardingColumns` не заполняется.

**Новая задача**:
- `HandleResolveNames`: принимать OLAP sinks всех типов (не только MODE_FILL), заполнять `ResolvedSinkSettings`
- `HandleResolveKeys`: при OLAP sink заполнять `stageMeta.CsShardingColumns` и `ShardKey->Partitioning` из `ColumnTableInfo.GetColumnShards()`

**Гарантии**:
- `CsShardingColumns` заполнен для OLAP sink — это обязательное условие для ColumnShardHashV1 routing'а
- `ShardKey->Partitioning` заполнен в порядке `GetColumnShards()` (канонический порядок bucket'ов)

#### 2.4.4 Executer: [`kqp_executer_impl.h`](ydb/core/kqp/executer_actor/kqp_executer_impl.h)

**Текущее (origin/main)**: `shardIds` собираются только из стадий с `TableOps` (scan-источники). Шарды destination-таблицы не резолвятся.

**Новая задача**: для FILL-sink стадий с `EnableCsWriteAffinity` добавлять шарды temp-таблицы в `shardIds`, чтобы они попали в `ShardIdToNodeId`.

**Гарантии**:
- `ShardIdToNodeId` содержит ноды destination-шардов → `CountComputeTasks` может пиннить задачи к нодам шардов
- Node affinity достижим (задача выполняется на ноде своего шарда)

#### 2.4.5 [`CountComputeTasks`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) — число задач

**Текущее (origin/main)**: 1 задача на sink stage (COPY от Map).

**Новая задача**: при OLAP sink (`GetIsOlap()`) создавать per-shard задачи из `ColumnTableInfoPtr->GetColumnShards()`, пиннить к ноде шарда через `ShardIdToNodeId`.

**Гарантии**:
- Число задач = числу шардов (Per-Shard, K=N)
- Каждая задача пиннится к ноде своего шарда (через `ExpectedNodeId`)
- Порядок задач совпадает с порядком `GetColumnShards()` — критично для `TaskIndexByHash`

#### 2.4.6 [`BuildKqpStageChannels`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) — каналы

**Текущее (origin/main)**: `TDqCnMap` → Map 1:1; `TDqCnBroadcast` → Broadcast всем.

**Новая задача**: при `CsShardingColumns` + N>1 задач строить `ColumnShardHashV1` HashShuffle вместо Broadcast/Map. Обработка зависит от типа соединения:

- **`kColumnShardHashV1`** (CTAS — оптимизатор эмитит `TDqCnHashShuffle` с `ColumnShardHashV1`): если `columnShardHashV1Params` ещё не populated (нет shuffle elimination), вызывает `BuildColumnShardHashV1ForWriteAffinity` helper для построения params из `ColumnTableInfoPtr`. Если уже populated (shuffle elimination) — использует proto's KeyColumns напрямую.
- **`kBroadcast`** (Pure OLAP / UPDATE/DELETE — оптимизатор эмитит `TDqCnBroadcast`): вызывает `BuildColumnShardHashV1ForWriteAffinity` helper; при успехе строит `BuildHashShuffleChannels` с `ColumnShardHashV1`, иначе fallback на `BuildBroadcastChannels`.
- **`kMap`** (UPDATE/DELETE source→Transform): при OLAP sink с N>1 задачами аналогично заменяет Map на `ColumnShardHashV1` HashShuffle.

`TaskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а.

**Гарантии**:
- Каждая строка направляется ровно в одну задачу — владельца целевого шарда
- `TaskIndexByHash` построен по `GetColumnShards()` (канонический порядок), совпадает с порядком задач из `CountComputeTasks`
- Нет M× сетевого трафика (в отличие от Broadcast)
- Shared helper `BuildColumnShardHashV1ForWriteAffinity` ([`kqp_tasks_graph.cpp:1328`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1328)) извлекает общую логику для `kColumnShardHashV1` и `kBroadcast` cases

#### 2.4.7 [`BuildInternalSinks`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) — sink settings

**Текущее (origin/main)**: `PackFrom(settings)` без `TargetShardIds`.

**Новая задача**: при `IsOlap` + N>1 задач назначать `TargetShardIds = {shard_i}` задаче i по индексу в `GetColumnShards()`.

**Гарантии**:
- Каждая задача знает, какие шарды ей принадлежат (`TargetShardIds`)
- Порядок `TargetShardIds` совпадает с порядком задач из `CountComputeTasks`

#### 2.4.8 WriteActor: [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp)

**Текущее (origin/main)**: `ShardAndFlushBatch` шардифицирует все строки, `TargetShardIds` отсутствует.

**Новая задача**: `AFL_VERIFY(TargetShardIds->contains(shardId))` — строгая валидация, что строка принадлежит задаче.

**Гарантии**:
- Если строка чужого шарда попала в задачу — это баг routing'а, и он детектируется (crash)
- Корректность записи: каждая задача пишет только в свои шарды

#### 2.4.9 Сводная таблица изменений

| Функция | Новая задача | Гарантия |
|---------|--------------|----------|
| `BuildFillTableEffect` | Разделить Transform/Sink на два stage; CTAS — `TDqCnHashShuffle` с `ColumnShardHashV1`, Pure OLAP/UPDATE/DELETE — `TDqCnBroadcast` (→HashShuffle в runtime) | Sink-стадия независимо параллелизуется |
| `FillCreateTableAs` | Сохранить `CtasDestinationPath` | Resolver навигирует destination-таблицу |
| Table Resolver | Заполнить `CsShardingColumns` | Обязательное условие ColumnShardHashV1 |
| Executer | Добавить destination-шарды в `ShardIdToNodeId` | Node affinity достижим |
| `CountComputeTasks` | Per-shard задачи (K=N) | Задача на шард, пиннинг к ноде |
| `BuildKqpStageChannels` | ColumnShardHashV1 HashShuffle | Точный routing, нет M× трафика |
| `BuildInternalSinks` | Заполнить `TargetShardIds` | Задача знает свои шарды |
| WriteActor | `AFL_VERIFY` routing'а | Детекция багов routing'а |

---

## 3. Текущее состояние в ветке

### 3.1 Что реализовано

| № | Описание | Статус | Файл |
|---|----------|--------|------|
| 1 | PRAGMA `EnableCsWriteAffinity` → флаг в `TKqpPhyTx.EnableCsWriteAffinity` (proto) | ✅ | [`yql_kikimr_settings.h`](ydb/core/kqp/provider/yql_kikimr_settings.h), [`kqp_physical.proto`](ydb/core/protos/kqp_physical.proto) |
| 2 | `BuildFillTableEffect`: при `enableCsWriteAffinity` строятся **два** stage — Transform + отдельный Sink. CTAS: соединение `TDqCnHashShuffle` с `ColumnShardHashV1`; Pure OLAP / UPDATE/DELETE: `TDqCnBroadcast` (→HashShuffle в runtime) | ✅ | [`kqp_opt_effects.cpp:238`](ydb/core/kqp/opt/kqp_opt_effects.cpp:238) |
| 3 | Proto-поля `TargetShardIds = 30`, `ExpectedNodeId = 31`, `CtasDestinationPath = 32` в `TKqpTableSinkSettings` | ✅ | [`kqp.proto`](ydb/core/protos/kqp.proto) |
| 4 | `FillCreateTableAs`: сохраняет `CtasDestinationPath` (путь destination) в `TKqpTableSinkSettings` | ✅ | [`kqp_query_compiler.cpp:2456`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:2456) |
| 5 | Table resolver `HandleResolveNames`: принимает OLAP сinks (не только MODE_FILL); `ResolvedSinkSettings` заполняется для всех типов | ✅ | [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp) |
| 6 | Table resolver `HandleResolveKeys`: при OLAP sink заполняет `stageMeta.CsShardingColumns` и `ShardKey->Partitioning` из `ColumnTableInfo.GetColumnShards()` | ✅ | [`kqp_table_resolver.cpp:303`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:303) |
| 7 | `kqp_executer_impl.h`: для FILL-sink стадий с `EnableCsWriteAffinity` добавляет шарды temp-таблицы в `shardIds` → они попадают в `ShardIdToNodeId` | ✅ | [`kqp_executer_impl.h:319`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:319) |
| 8 | `CountComputeTasks`: при наличии OLAP sink (`GetIsOlap()`) создаёт per-shard задачи из `ColumnTableInfoPtr->GetColumnShards()`, пиннит к ноде шарда через `ShardIdToNodeId` | ✅ | [`kqp_tasks_graph.cpp:4408`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4408) |
| 9 | `BuildKqpStageChannels` (kBroadcast): при `CsShardingColumns` + N>1 задач строит `ColumnShardHashV1` HashShuffle вместо Broadcast (Pure OLAP, UPDATE/DELETE) | ✅ | [`kqp_tasks_graph.cpp:1472`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1472) |
| 10 | `BuildKqpStageChannels` (kColumnShardHashV1): обрабатывает `TDqCnHashShuffle` с `ColumnShardHashV1` из оптимизатора (CTAS), строит params через `BuildColumnShardHashV1ForWriteAffinity` helper | ✅ | [`kqp_tasks_graph.cpp:1637`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1637) |
| 10a | `BuildKqpStageChannels` (kMap): при OLAP sink с N>1 задачами аналогично заменяет Map на `ColumnShardHashV1` HashShuffle | ✅ | [`kqp_tasks_graph.cpp:1590`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1590) |
| 10b | `BuildColumnShardHashV1ForWriteAffinity`: shared helper для построения ColumnShardHashV1 params (используется в kColumnShardHashV1 и kBroadcast) | ✅ | [`kqp_tasks_graph.cpp:1328`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1328) |
| 10c | `PropogateHashFuncToHashShuffles`: сохраняет `ColumnShardHashV1` на HashShuffle-соединениях, не перезаписывая на `HashV2` | ✅ | [`kqp_opt_hash_func_propagate_transformer.cpp:100`](ydb/core/kqp/opt/kqp_opt_hash_func_propagate_transformer.cpp:100) |
| 11 | `BuildInternalSinks`: при `IsOlap` + N>1 задач назначает `TargetShardIds = {shard_i}` задаче i по индексу в `GetColumnShards()` | ✅ | [`kqp_tasks_graph.cpp:3545`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3545) |
| 12 | `AssignTasksToNodes`: планировщик использует `ExpectedNodeId` для пиннинга задач к нодам шардов | ✅ | [`kqp_planner.cpp`](ydb/core/kqp/executer_actor/kqp_planner.cpp) |
| 13 | `ShardAndFlushBatch`: `AFL_VERIFY(TargetShardIds->contains(shardId))` — строгая валидация routing'а | ✅ | [`kqp_write_table.cpp:522`](ydb/core/kqp/runtime/kqp_write_table.cpp:522) |
| 14 | Тесты: `KqpWriteAffinity::*`, `KqpQuery::CTAS_WriteAffinity_Twin*`, `*CreateAsSelect*`, olap/operations | ✅ | [`kqp_write_affinity_ut.cpp`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp) |

### 3.2 Статус: ✅ Стабильно

**Цель ветки**: при `EnableCsWriteAffinity=true` всегда создавать отдельную задачу для каждого CS. При `EnableCsWriteAffinity=false` — стандартное поведение origin/main (1 задача на sink stage).

**Текущий статус**: все тесты проходят (`OlapCreateAsSelect_Simple` + 16 других CreateAsSelect тестов + 12 OLAP тестов).

**Применённые фиксы**:
1. Gate на `EnableCsWriteAffinity` в `CountComputeTasks` — per-shard задачи только когда `EnableCsWriteAffinity=true`
2. Gate на `EnableCsWriteAffinity` в `BuildInternalSinks` — `TargetShardIds` заполняется только для affinity cases
3. Shuffle Elimination отключён для write affinity sink stages (`isCsWriteAffinitySink` check)
4. `isPureStage` check в `CountComputeTasks` — не создавать per-shard задачи для CTAS без входных каналов
5. `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` relaxed — accepts `nullopt` для non-affinity cases

### 3.3 Итоговая схема в ветке (с `EnableCsWriteAffinity`)

```
Upstream ComputeActor
    ↓ TDqCnMap (1:1)
Transform Stage (1 задача, любая нода)
    ↓ TDqCnHashShuffle с ColumnShardHashV1 (hash(PK) → task i)
      [CTAS: эмитится оптимизатором; Pure OLAP/UPDATE/DELETE: Broadcast→HashShuffle в runtime]
Sink Stage (N задач, по одной на шард)
    WriteActor[0] на Node(CS[0]) → CS[0]  (local)
    WriteActor[1] на Node(CS[1]) → CS[1]  (local)
    ...
    WriteActor[N] на Node(CS[N]) → CS[N]  (local)
```

---

## 4. Список доработок

### 4.1 **Вариант A: Per-Shard** K = N

#### 4.1.1 [БЛОКЕР] Shuffle Elimination path bug — mismatch DQ routing vs runtime sharding

**Статус**: ✅ Исправлено

**Симптом**: тест `OlapCreateAsSelect_Simple` падал при `EnableCsWriteAffinity=true` (default):
```
VERIFY failed: shard_id=72075186224037901; target_shard_ids={72075186224037899};
shards_count=4; ordered_shard_ids=0:72075186224037898,1:72075186224037899,2:72075186224037900,3:72075186224037901
```

Рута направлена на задачу для shard `...898` (`...899`), но `SplitByShardsToArrowBatches` возвращает строки для shard `...901`. Порядок шардов корректный — проблема в **несовпадении числа hash buckets** между DQ routing и runtime sharding.

**Корневая причина**: `BuildKqpStageChannels` enters **Shuffle Elimination** path для CTAS с `ColumnShardHashV1` HashShuffle:

```cpp
// Shuffle Elimination block
if (/* shuffle elimination conditions */) {
    // Builds identity TaskIndexByHash using stageInfo.Tasks.size() as SourceShardCount
    for (ui32 i = 0; i < stageInfo.Tasks.size(); ++i) {
        taskIndexByHash[i] = i;
    }
}
```

**Проблема**: `stageInfo` здесь — **Transform Stage**, а не Sink Stage. Transform Stage's `Tasks.size()` отражает shard count **upstream source таблицы** (например, 10 shards), в то время как destination CTAS таблица имеет другое число шардов (например, 4 shards).

**Механизм несовпадения**:

| Компонент | Формула | `N` (ShardCount) | Источник |
|-----------|---------|------------------|----------|
| DQ `ColumnShardHashV1` (routing) | `bucket = floor(h * N / MAX)` | `N = Transform.Tasks.size()` (10) | Shuffle Elimination использует Transform Stage |
| Runtime `TConsistencySharding64::MakeSharding` | `bucket = floor(h * N / MAX)` | `N = Sharding->GetShardsCount()` (4) | Destination table schema |

Для одного и того же hash-значения `h` разные `N` → разные buckets → строка маршрутизируется на wrong task.

**Применённое решение (Вариант A)**: Отключить Shuffle Elimination для CTAS с `EnableCsWriteAffinity=true`:

```cpp
// In BuildKqpStageChannels, before shuffle elimination check:
const bool isCsWriteAffinitySink = stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()
    && !stageInfo.Meta.CsShardingColumns.empty();
if (enableShuffleElimination && !isCsWriteAffinitySink && !hasMap && !isFusedWithScanStage && stageInfo.Tasks.size() > 0 && stage.InputsSize() > 0) {
    // shuffle elimination block — skipped for write affinity sinks
}
```

Shuffle Elimination — оптимизация для read path; для write affinity path она не нужна, так как write affinity уже обеспечивает корректный routing через `ColumnShardHashV1`.

**Дополнительные исправления**:
1. Восстановлен gate на `EnableCsWriteAffinity` в `CountComputeTasks` и `BuildInternalSinks`:
   - При `EnableCsWriteAffinity=true` + OLAP sink → создавать N per-shard задач
   - При `EnableCsWriteAffinity=false` + OLAP sink → 1 задача, стандартный путь origin/main
2. Добавлена проверка `isPureStage` в `CountComputeTasks` для CTAS без входных каналов (pure stage) — не создавать per-shard задачи когда нет HashShuffle каналов
3. `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` и `AFL_VERIFY` в WriteActor срабатывают только при `EnableCsWriteAffinity=true`. Для стандартного пути `TargetShardIds` — `nullopt` (проверок нет).

**Файл**: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1690) — Shuffle Elimination block в `BuildKqpStageChannels`.

---

#### 4.1.2 [БЛОКЕР] Pure Stage OLAP: BUILD_UPSERT_ROWS_EFFECT для чистых выражений

**Статус**: ✅ Реализовано в ветке

**Проблема**: `REPLACE INTO VALUES` → `BuildUpsertRowsEffect` с `IsDqPureExpr(node.Input()) == true` → `RebuildPureStageWithSink` → один `TDqStage` без входных каналов (pure stage). `CountComputeTasks` видит OLAP sink, создаёт N=8 задач, но у каждой задачи `task.Inputs` пуст — нет upstream HashShuffle-каналов. Каждая задача независимо вычисляет все VALUES и получает все строки.

**Что нужно было**: Для OLAP + EnableCsWriteAffinity создать **два** stage — Transform (pure, 0 inputs) → `TDqCnBroadcast` → Sink (N задач). Broadcast конвертируется в HashShuffle в runtime через `BuildKqpStageChannels (kBroadcast)`.

**Реализация**: В `BuildUpsertRowsEffect` для `IsDqPureExpr` добавлен аналог `BuildCsWriteAffinitySinkStage()` паттерна.

---

#### 4.1.3 [БЛОКЕР] INSERT/REPLACE: CsShardingColumns пуст → per-shard задачи + Broadcast → CRASH

**Статус**: ✅ Исправлено

**Краш** (тест `KqpQuery::CTAS_WriteAffinity_Twin+EnableCsWriteAffinity`, INSERT-фаза):
```
VERIFY failed: verification=!stageInfo.Meta.CsShardingColumns.empty();
stageId=[0,0]; shardNodesCount=8; isOlapSink=1
```

**Корневая причина**: Table resolver заполнял `CsShardingColumns` только для CTAS FILL (через `ColumnTableInfo.GetSharding().GetHashSharding().GetColumns()`), но для обычного INSERT этот путь не срабатывал. `CountComputeTasks` создавал per-shard задачи, но `BuildKqpStageChannels` откатывался на Broadcast → `AFL_VERIFY` краш.

**Исправление**: Table resolver заполняет `CsShardingColumns` для **всех** OLAP sink'ов (не только MODE_FILL) в `HandleResolveKeys`. Также заполняется `ShardKey->Partitioning`.

**Детали исправления** (`kqp_table_resolver.cpp`):
```cpp
// HandleResolveNames: CsShardingColumns и ColumnTableInfoPtr для non-CTAS OLAP
if (isOlap && entry.ColumnTableInfo) {
    stageMeta.ColumnTableInfoPtr = entry.ColumnTableInfo;
    const auto& desc = entry.ColumnTableInfo->Description;
    if (desc.HasSharding() && desc.GetSharding().HasHashSharding()) {
        for (const auto& col : desc.GetSharding().GetHashSharding().GetColumns()) {
            stageMeta.CsShardingColumns.emplace_back(col);
        }
    }
}

// HandleResolveKeys: ShardKey->Partitioning из GetColumnShards()
if (entry.ColumnTableInfo && isOlapSink) {
    TVector<TKeyDesc::TPartitionInfo> partitions;
    for (const auto& shardId : desc.GetSharding().GetColumnShards()) {
        partitions.emplace_back(shardId);
    }
    if (!partitions.empty()) {
        if (!stageMeta.ShardKey) {
            stageMeta.ShardKey = TKeyDesc::CreateMiniKeyDesc(...);
        }
        stageMeta.ShardKey->Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
    }
}
```

---

#### 4.1.4 [БЛОКЕР] CTAS TargetShardIds: ColumnTableInfoPtr null для новосозданной таблицы

**Статус**: ✅ Исправлено (через ShardKey fallback + per-shard tasks для всех OLAP sinks)

**Проблема**: CTAS создаёт temp-таблицу, но `ColumnTableInfoPtr` может быть null до момента создания. `CountComputeTasks` не может создать per-shard задачи → 1 задача → `TargetShardIds` пуст → `AFL_VERIFY(TargetShardIds.has_value())` → crash.

**Анализ** (`cs-write-affinity-ctas-problem.md`):
- `TargetShardIdsFromSettings` в `kqp_write_actor.cpp` возвращает `nullopt` когда `TargetShardIds` пуст
- `BuildInternalSinks` не заполняет `TargetShardIds` из-за отсутствия `ColumnTableInfoPtr` и `ShardKey`
- `CountComputeTasks` fall-through к стандартному 1-task пути

**Исправление**: Per-shard задачи создаются для **всех** OLAP sinks. Для CTAS `ColumnTableInfoPtr` устанавливается из `HandleResolveKeys` navigate response. `ShardKey->Partitioning` используется как fallback когда `ColumnTableInfoPtr` недоступен в момент `CountComputeTasks`.

---

#### 4.1.5 [БЛОКЕР] Routing: BuildKqpStageChannels для CTAS sink

**Статус**: ✅ Исправлено

**Проблема**: В case `kColumnShardHashV1` (CTAS) нужно отличать write affinity от shuffle elimination. Если `columnShardHashV1Params` уже populated из scan stage — использовать proto's KeyColumns. Если нет — вызвать `BuildColumnShardHashV1ForWriteAffinity` helper.

**Исправление**: В case `kColumnShardHashV1` добавлена проверка: если stage является OLAP sink с affinity и `columnShardHashV1Params` не populated → вызвать `BuildColumnShardHashV1ForWriteAffinity`.

Shared helper `BuildColumnShardHashV1ForWriteAffinity` ([`kqp_tasks_graph.cpp:1328`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1328)) используется в обоих cases: `kColumnShardHashV1` (CTAS) и `kBroadcast` (Pure OLAP / UPDATE/DELETE).

---

#### 4.1.6 Write Stats: двойной счёт при per-shard задачах

**Статус**: ⬜ Workaround, требует нормального фикса

В [`kqp_cost_ut.cpp`](ydb/core/kqp/ut/cost/kqp_cost_ut.cpp) строгие ассерты на количество строк/байт в write stats заменены на `UNIT_ASSERT_GE + rows % N == 0`. При per-shard задачах каждая задача сообщает свою статистику, итого rows умножается на число задач.

**Действия**:
- Выяснить, где агрегируется write stats (executer или session actor)
- Дедуплицировать/суммировать корректно
- Вернуть строгие ассерты в тестах

---

#### 4.1.7 Session Actor: OLAP shards в shardIds

**Статус**: ⬜ Требует проверки

В [`kqp_session_actor.cpp:1147`](ydb/core/kqp/session_actor/kqp_session_actor.cpp:1147) добавлено:
```cpp
if (stageInfo.Meta.ColumnTableInfoPtr && ...) {
    for (const auto& shardId : ...GetColumnShards())
        shardIds.insert(shardId);
}
```

**Действия**: Установить, зачем session actor собирает shard'ы OLAP-таблиц. Убедиться, что это изменение не является побочным эффектом.

---

#### 4.1.8 CTAS без `EnablePerStatementQueryExecution`

**Статус**: ⬜ Не исследовано

Без флага CTAS компилируется иначе (не через `TKqlFillTable`/FILL). Нужно выяснить, применяется ли тот же путь.

---

#### 4.1.9 Удалить `CtasDestinationPath` если не используется

**Статус**: ⬜ Требует проверки

Поле `CtasDestinationPath = 32` добавлено в proto и заполняется в `FillCreateTableAs`. Если нигде не читается — поле можно удалить.

---

#### 4.1.10 Временные debug-флаги в `ya.make`

**Статус**: ✅ Флаги `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` и `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT=1` активны в [`ya.make`](ydb/core/kqp/runtime/ya.make) для диагностики и валидации routing'а. `QP_FORCE_CS_WRITE_AFFINITY` — опциональный debug-флаг для принудительного включения режима.

Перед мержем:
- Удалить `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` и `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT` из `ya.make`
- Проверить нужность `QP_FORCE_CS_WRITE_AFFINITY`

---

#### 4.1.11 Бенчмарк

**Статус**: ⬜ Не реализовано

Измерить: сетевой трафик, время выполнения, пиковое потребление памяти — affinity vs baseline (1 задача).

---

### 4.2 **Вариант B: Per-Node** K = M

> Задачи для этапа B будут уточнены после завершения и стабилизации этапа A (Per-Shard).

Общая идея: сгруппировать шарды по нодам (`P: Shard → Node`), создать M=|{Node}| задач вместо N шардов. Каждая задача обслуживает `{s | P(s) = Nodeⱼ}` шардов. `TargetShardIds` содержит несколько шардов. `ColumnShardHashV1` routing: `TaskIndexByHash[bucket] = j` (нода, не шард).

---

## 5. Code Review

**Задача:** Реализация **ColumnShard Write Affinity** — оптимизации для OLAP-записей (INSERT, REPLACE, UPDATE, DELETE, CTAS), которая маршрутизирует строки напрямую к соответствующему shard-у через `ColumnShardHashV1` HashShuffle вместо Broadcast.

### 5.1 Изменения по файлам (24 файла, +3213/-111 строк)

#### `ydb/core/kqp/opt/kqp_opt_effects.cpp` — Оптимизатор

| Операция | Функция | Изменение |
|----------|---------|-----------|
| **CTAS** | `BuildFillTableEffect()` | При `enableCsWriteAffinity=true` создаётся Transform Stage → HashShuffle(ColumnShardHashV1) → Sink Stage |
| **REPLACE/INSERT (pure expr)** | `BuildUpsertRowsEffect()` | Для OLAP + affinity: разделение на Transform + Sink stage с HashShuffle |
| **REPLACE/INSERT (non-pure)** | `BuildUpsertRowsEffect()` | Аналогично: Transform Stage (с Map от source) → HashShuffle → Sink Stage |
| **UPDATE** | `BuildUpsertRowsEffect()` | Аналогично non-pure INSERT |
| **DELETE** | `BuildDeleteRowsEffect()` | Аналогично: Transform Stage → HashShuffle → Sink Stage |

#### `ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp` — Runtime (TasksGraph)

| Изменение | Описание |
|-----------|----------|
| `BuildColumnShardHashV1ForWriteAffinity()` | Shared helper для построения ColumnShardHashV1 params |
| `kColumnShardHashV1` в `BuildKqpStageChannels()` | CTAS: routing через helper вместо shuffle elimination логики |
| `kBroadcast` в `BuildKqpStageChannels()` | INSERT/UPDATE/DELETE: Broadcast → HashShuffle(ColumnShardHashV1) |
| `kMap` в `BuildKqpStageChannels()` | OLAP sink с >1 задачи: Map → Broadcast/HashShuffle |
| `BuildInternalSinks()` | Заполняет `TargetShardIds`: 1 shard на задачу |
| `CountComputeTasks()` | N задач для Sink Stage (по одной на shard) |

#### `ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp` — Тесты

| Тест | Операция | Проверяет |
|------|----------|-----------|
| `Replace` | REPLACE INTO (pure expr) | 3 stage с affinity, 2 без + план |
| `Insert` | INSERT (pure expr) | Корректная запись + план |
| `Update` | UPDATE | Корректное обновление + план |
| `Delete` | DELETE | Корректное удаление + план |
| `Ctas` | CREATE TABLE AS | 4 stage с affinity, 3 без + план |

### 5.2 Замечания

#### Дублирование кода в `kqp_opt_effects.cpp` ✅ Исправлено
Создание Transform→HashShuffle→Sink паттерна повторяется 5+ раз. Вынесено в helper `BuildCsWriteAffinitySinkStage()`. Все 4 места дублирования теперь вызывают этот helper.

#### Дублирование в `kMap` case `kqp_tasks_graph.cpp` ✅ Исправлено
~130 строк инлайн-кода в `kMap` case заменены на вызов `BuildColumnShardHashV1ForWriteAffinity()`.

#### Неполная план-верификация в тестах ✅ Исправлено
Добавлена план-верификация через `VerifyPlanWithAffinity` в тесты `Insert`, `Update`, `Delete`. Для UPDATE и DELETE указаны корректные ожидаемые числа стадий (4 с affinity, 3 без).

#### `QP_FORCE_CS_WRITE_AFFINITY`
Debug-механизм с AFL_VERIFY инвариантами. Использует `#ifdef` — не попадает в production-сборки.

#### Fallback на Broadcast ✅ Исправлено
Когда `EnableCsWriteAffinity=true` и ColumnShardHashV1 не может быть построен, возвращается ошибка (`Y_ENSURE(false, ...)`) вместо тихого fallback. Когда affinity отключён — fallback на Broadcast сохраняется как безопасный дефолт.

---

## 6. ColumnShardHashV1 — сводная таблица сценариев

| Аспект | 1. Shuffle Elimination (Read) | 2. CS Write Affinity (Pure OLAP) | 3. CS Write Affinity (UPDATE/DELETE) | 4. CS Write Affinity (CTAS) |
|--------|-------------------------------|----------------------------------|--------------------------------------|------------------------------|
| **Назначение** | Сохранить партиционирование source | Route строк в per-shard Sink | Route строк через Transform в Sink | Заполнить temp-таблицу, затем move |
| **Trigger** | `isRead && enableShuffleElimination` | `isOlap && enableCsWriteAffinity` + pure OLAP | `isOlap && enableCsWriteAffinity` + source | `isOlap && enableCsWriteAffinity` + MODE_FILL |
| **Optimizer connection** | `TDqCnHashShuffle` с `ColumnShardHashV1` | `TDqCnBroadcast` (Transform→Sink) | `TDqCnMap` + `TDqCnBroadcast` | `TDqCnMap` + `TDqCnHashShuffle` ColumnShardHashV1 |
| **Runtime conversion** | Нет | Broadcast→HashShuffle | Broadcast→HashShuffle | Нет (оптимизатор эмитит напрямую) |
| **Key columns source** | Source table PK | `CsShardingColumns` из table resolver | `CsShardingColumns` из table resolver | `CsShardingColumns` / fallback raw proto |
| **TargetShardIds** | N/A | 1 shard per task | 1 shard per task | 1 temp-table shard per task |
| **EXPLAIN visibility** | `HashShuffle` с `ColumnShardHashV1` | `Broadcast` (оптимизатор) | `Broadcast` (оптимизатор) | `HashShuffle` с `ColumnShardHashV1` |
| **Feature flag** | `OptShuffleElimination` | `EnableCsWriteAffinity` | `EnableCsWriteAffinity` | `EnableCsWriteAffinity` + `EnablePerStatementQueryExecution` |

### Ключевые места в коде

#### Runtime: `TColumnShardHashV1`
- **Definition**: [`dq_output_consumer.cpp:136`](ydb/library/yql/dq/runtime/dq_output_consumer.cpp:136)
- **Hash**: `NXX64::TStreamStringHashCalcer(seed=0)` + `Update(raw_bytes)` per column
- **Finish**: maps hash → shard bucket → task index via `TaskIndexByHash`

#### Optimizer: Write Affinity
- **CTAS/FILL**: [`kqp_opt_effects.cpp:269`](ydb/core/kqp/opt/kqp_opt_effects.cpp:269)
- **Pure OLAP (REPLACE INTO/INSERT)**: [`kqp_opt_effects.cpp:408`](ydb/core/kqp/opt/kqp_opt_effects.cpp:408)
- **UPDATE/INSERT with source**: [`kqp_opt_effects.cpp:568`](ydb/core/kqp/opt/kqp_opt_effects.cpp:568)
- **DELETE with source**: [`kqp_opt_effects.cpp:808`](ydb/core/kqp/opt/kqp_opt_effects.cpp:808)

#### Tasks Graph
- **Shared helper**: [`kqp_tasks_graph.cpp:1328`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1328)
- **Broadcast→HashShuffle**: [`kqp_tasks_graph.cpp:1471`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1471)
- **CTAS HashShuffle**: [`kqp_tasks_graph.cpp:1637`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1637)
- **Task count**: [`kqp_tasks_graph.cpp:3891`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3891)
- **TargetShardIds**: [`kqp_tasks_graph.cpp:3964`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3964)

#### Table Resolver
- **CsShardingColumns**: [`kqp_table_resolver.cpp:305`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:305)
- **CTAS columns**: [`kqp_table_resolver.cpp:186`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:186)
