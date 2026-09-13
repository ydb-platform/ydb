# CTAS Write Affinity: Описание решения

## 1. Введение

Оптимизация **ColumnShard Write Node Affinity** для операции CTAS (CREATE TABLE AS SELECT) маршрутизирует строки напрямую к соответствующему ColumnShard через `ColumnShardHashV1` HashShuffle.

[1] Решение касается **только CTAS**. Не-CTAS операции (INSERT/REPLACE/UPDATE/DELETE) используют отдельный путь через Table Resolver и не требуют изменений.


---

## 2. Реализация

Оптимизация **ColumnShard Write Node Affinity** для CTAS управляется флагом `EnableCsWriteAffinity`. Ниже описаны три ключевых этапа обработки запроса: **переписывание** (Rewrite), **оптимизация** (Optimization) и **исполнение** (Runtime).

### 2.1 Переписывание запроса CTAS

**Вход:** SQL-запрос CTAS:

```sql
CREATE TABLE Destination
    PARTITION BY HASH(Col1)
    PRIMARY KEY (Col1, Col2)
    AS SELECT Col1, Col2, Col3 FROM Source WHERE Col1 > 10;
```

Плюс `TKikimrConfiguration` с флагом `EnableCsWriteAffinity`


#### 2.1.1 Rewrite: извлечение sharding columns из CREATE TABLE

[`RewriteCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:313) получает `TWriteTableSettings settings` с полями:
- `settings.PartitionBy` — колонки из `PARTITION BY HASH(...)`
- `primariKeyColumns` — колонки из `PRIMARY KEY (...)`
- `settings.TableSettings` — настройки таблицы (включая `storeType = column`)

[14] Для ColumnShard таблиц ([`IsOlapCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:96) проверяет `storeType = column`) извлекаются sharding columns с приоритетом:
1. `PartitionBy` columns (явный ключ шардирования)
2. PRIMARY KEY columns (fallback)
3. Ошибка, если оба отсутствуют

[15] Sharding columns добавляются в `insertSettings` только при включённом флаге `EnableCsWriteAffinity`:
```cpp
// kqp_statement_rewrite.cpp:314-339
const bool enableCsWriteAffinity = sessionCtx->ConfigPtr()->GetEnableCsWriteAffinity();
if (IsOlapCreateTableAs(root, exprCtx) && enableCsWriteAffinity) {
    NYql::TExprNode::TListType partitionColumnsList;
    if (settings.PartitionBy.IsValid()) {
        for (const auto& col : settings.PartitionBy.Cast()) {
            partitionColumnsList.push_back(exprCtx.NewAtom(pos, col.Value()));
        }
    } else if (!primariKeyColumns.empty()) {
        for (const auto& col : primariKeyColumns) {
            partitionColumnsList.push_back(exprCtx.NewAtom(pos, TString(col)));
        }
    }

    if (!partitionColumnsList.empty()) {
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "CtasShardingColumns"),
                exprCtx.NewList(pos, std::move(partitionColumnsList)),
            }));
    } else {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(pos),
            "CTAS to ColumnShard table requires partition key"));
        return std::nullopt;
    }
}
```

[16] Итоговый `Write!`-callable имеет 5 аргументов:
```cpp
// kqp_statement_rewrite.cpp:338-354
const auto insert = exprCtx.NewCallable(pos, "Write!", {
    topLevelRead == nullptr ? exprCtx.NewWorld(pos) : exprCtx.NewCallable(pos, "Left!", {topLevelRead.Get()}),
    exprCtx.NewCallable(pos, "DataSink", {
        exprCtx.NewAtom(pos, "kikimr"),
        exprCtx.NewAtom(pos, "db"),
    }),
    exprCtx.NewCallable(pos, "Key", {
        exprCtx.NewList(pos, {
            exprCtx.NewAtom(pos, "table"),
            exprCtx.NewCallable(pos, "String", {
                exprCtx.NewAtom(pos, createTableName),
            }),
        }),
    }),
    insertDataCopy,
    exprCtx.NewList(pos, std::move(insertSettings)),  // ← CtasShardingColumns здесь
});
```

**Примеры**:
- `PARTITION BY HASH(Col2)` → `CtasShardingColumns = ["Col2"]`
- без PARTITION BY, `PRIMARY KEY (Col1)` → `CtasShardingColumns = ["Col1"]`

**Было**: `RewriteCreateTableAs` разбивает CTAS на 3 стейтмента (CREATE temp, FILL, MOVE). В `Write!` callable sharding columns не передаются — оптимизатор не знает, по каким колонкам шардируется целевая таблица.

**Стало (без аффинити)**: `RewriteCreateTableAs` ([`kqp_statement_rewrite.cpp:314`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:314)) проверяет флаг `EnableCsWriteAffinity`. При `false` — sharding columns не извлекаются, `CtasShardingColumns` не добавляется в `Write!` (логика не изменилась).

**Стало (с аффинити)**: `RewriteCreateTableAs` при `EnableCsWriteAffinity=true` извлекает sharding columns из `PARTITION BY` (или из `PRIMARY KEY` как fallback) и записывает их в `CtasShardingColumns` узла `Write!`. Разница проявляется на этапе Optimization (см. 2.2.2), где `CtasShardingColumns` используется для построения HashShuffle.

### 2.2 Оптимизация

**Вход:** `Write!`-callable из Rewrite (раздел 2.1.1) + `TKikimrConfiguration` с флагом `EnableCsWriteAffinity`

#### 2.2.1 Преобразование Write! → TKqlFillTable

Между Rewrite (раздел 2.1) и оптимизатором (раздел 2.2.2) AST-узел `Write!` превращается в типизированный узел `TKqlFillTable`.

**Вход:** `Write!`-callable из Rewrite (раздел 2.1.1):
```
Write! (Callable)
├── Child[0]: Input (TExprBase) — SELECT-данные
├── Child[1]: DataSink (Callable) — "kikimr/db"
├── Child[2]: Key (Callable) — { table: "Destination_uuid" }
├── Child[3]: Data (TExprBase) — данные для записи
└── Child[4]: Settings (List) — [
    ["mode", "fill_table"],
    ["OriginalPath", "/tmp/sessions/.../Destination_uuid"],
    ["AllowInconsistentWrites"],
    ["CtasShardingColumns", [Col1, Col2]]   ← из Rewrite
  ]
```

Преобразование `Write!` → `TKiWriteTable` (стандартный `RewriteIO()` в `yql_kikimr_datasink.cpp`) и типизация `TKiWriteTable` (`HandleWriteTable()` в `yql_kikimr_type_ann.cpp`) **не изменяются в этой ветке**: настройка `CtasShardingColumns` проходит сквозь них транзитно через `settings.Other`, а `HandleWriteTable()` для `mode="fill_table"` лишь устанавливает тип узла как тип `World` (валидация `CtasShardingColumns` происходит позже в `AnnotateFillTable()`, раздел 2.2.1.2).

##### 2.2.1.1 BuildFillTable: TKiWriteTable → TKqlFillTable

Оптимизатор вызывает `HandleWriteTable()`, который при `mode="fill_table"` вызывает [`BuildFillTable()`](ydb/core/kqp/opt/kqp_opt_kql.cpp:485):

```cpp
// kqp_opt_kql.cpp:1463-1464
if (GetTableOp(write) == TYdbOperation::FillTable) {
    return BuildFillTable(write, ctx).Ptr();
}
```

`BuildFillTable()` извлекает компоненты из `TKiWriteTable` и создаёт `TKqlFillTable` через builder:

```cpp
// kqp_opt_kql.cpp:485-501
TExprBase BuildFillTable(const TKiWriteTable& write, TExprContext& ctx) {
    auto originalPathNode = GetSetting(write.Settings().Ref(), "OriginalPath");
    AFL_ENSURE(originalPathNode);
    auto ctasShardingColumnsNode = GetSetting(write.Settings().Ref(), "CtasShardingColumns");
    
    auto builder = Build<TKqlFillTable>(ctx, write.Pos())
        .Input(write.Input())                    // Child[0]: TExprBase
        .Table(write.Table())                    // Child[1]: TCoAtom
        .Cluster(write.DataSink().Cluster())     // Child[2]: TCoAtom
        .OriginalPath(TCoNameValueTuple(originalPathNode).Value().Cast<TCoAtom>());  // Child[3]: TCoAtom

    if (ctasShardingColumnsNode) {
        const auto ctasShardingColumns = TCoNameValueTuple(ctasShardingColumnsNode).Value().Cast<TCoAtomList>();
        if (ctasShardingColumns.Ref().ChildrenSize() > 0) {
            builder.CtasShardingColumns(ctasShardingColumns);  // Child[4]: TCoAtomList (опционально)
        }
    }
    return builder.Done();
}
```

**Было:** `BuildFillTable()` создаёт `TKqlFillTable` с 4 аргументами (без `CtasShardingColumns`).

**Стало (без аффинити):** `BuildFillTable()` создаёт `TKqlFillTable` с 4 аргументами (без `CtasShardingColumns`, логика не изменилась).

**Стало (с аффинити):** `BuildFillTable()` создаёт `TKqlFillTable` с 5 аргументами (извлекает `CtasShardingColumns` из settings и передаёт в `TKqlFillTable`).

##### 2.2.1.2 Типизация TKqlFillTable

[`AnnotateFillTable()`](ydb/core/kqp/opt/kqp_type_ann.cpp:833) валидирует `TKqlFillTable`:
1. `EnsureMinMaxArgsCount(*node, 4, 5, ctx)` — проверяет, что аргументов 4 или 5
2. Если 5-й аргумент (`CtasShardingColumns`) присутствует — `EnsureTupleOfAtoms()` проверяет, что это список строк
3. Устанавливает тип узла: `TListExprType` или `TStreamExprType` с `KqpEffectType`

**Было:** `AnnotateFillTable()` проверяет 4 аргумента, не проверяет `CtasShardingColumns`.

**Стало (без аффинити):** `AnnotateFillTable()` проверяет 4 аргумента, не проверяет `CtasShardingColumns` (логика не изменилась).

**Стало (с аффинити):** `AnnotateFillTable()` проверяет 4-5 аргументов, проверяет тип `CtasShardingColumns` (список строк) при наличии.

##### 2.2.1.3 Выход: TKqlFillTable

Результат — типизированный узел `TKqlFillTable`:
```
TKqlFillTable (Callable "KqlFillTable")
├── Child[0] (Input): TExprBase — SELECT-данные
├── Child[1] (Table): TCoAtom — "/tmp/sessions/.../Destination_uuid"
├── Child[2] (Cluster): TCoAtom — "kikimr/db"
├── Child[3] (OriginalPath): TCoAtom — "/Tables/Destination"
└── Child[4] (CtasShardingColumns): TCoAtomList (опционально)
    ├── Child[0]: TCoAtom("Col1")
    └── Child[1]: TCoAtom("Col2")
```

**Схема узла** ([`kqp_expr_nodes.json`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:292)):
```json
{
    "Name": "TKqlFillTable",
    "Base": "TExprBase",
    "Match": {"Type": "Callable", "Name": "KqlFillTable"},
    "Children": [
        {"Index": 0, "Name": "Input", "Type": "TExprBase"},
        {"Index": 1, "Name": "Table", "Type": "TCoAtom"},
        {"Index": 2, "Name": "Cluster", "Type": "TCoAtom"},
        {"Index": 3, "Name": "OriginalPath", "Type": "TCoAtom"},
        {"Index": 4, "Name": "CtasShardingColumns", "Type": "TCoAtomList", "Optional": true}
    ]
}
```

Поле `CtasShardingColumns` опционально (`"Optional": true`), так как для не-CTAS случаев (INSERT/REPLACE) sharding columns не передаются — они берутся из метаданных таблицы при runtime.

**Было:** `TKqlFillTable` имеет 4 аргумента (без `CtasShardingColumns`).

**Стало (без аффинити):** `TKqlFillTable` имеет 4 аргумента (без `CtasShardingColumns`, логика не изменилась).

**Стало (с аффинити):** `TKqlFillTable` имеет 5 аргументов (добавлен `CtasShardingColumns`).

#### 2.2.2 BuildFillTableEffect: построение физического плана

[`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:220) строит физический план (DQ-граф стадий) для `TKqlFillTable`. Результат — узел `TKqpSinkEffect`, который сериализуется в `TKqpPhyTx` proto (раздел 2.2.4).

**Вход:** Типизированный узел `TKqlFillTable` (раздел 2.2.1.3):
```
TKqlFillTable (Callable "KqlFillTable")
├── Child[0] (Input): TExprBase — SELECT-данные
├── Child[1] (Table): TCoAtom — "/tmp/sessions/.../Destination_uuid"
├── Child[2] (Cluster): TCoAtom — "kikimr/db"
├── Child[3] (OriginalPath): TCoAtom — "/Tables/Destination"
└── Child[4] (CtasShardingColumns): TCoAtomList (опционально)
    ├── Child[0]: TCoAtom("Col1")
    └── Child[1]: TCoAtom("Col2")
```

Построение `TKqpTable` (метаданные целевой таблицы: путь, пустые PathId/SysView/Version), `settings` (`OriginalPath` — путь к исходной таблице до CTAS-декомпозиции) и определение типа Input **не изменяются в этой ветке**: при `IsDqPureExpr(node.Input())` выбирается путь A (раздел 2.2.2.2), иначе ожидается `TDqCnUnionAll` (`EnsureDqUnion`) — путь B (раздел 2.2.2.3). Изменение добавляет ветку `csWriteAffinity` в каждый из двух путей.

##### 2.2.2.1 Определение режима: `csWriteAffinity`

```cpp
// kqp_opt_effects.cpp:225
const bool csWriteAffinity = node.CtasShardingColumns().IsValid();
```

`csWriteAffinity` — индикатор, что sharding columns установлены. Определяет, какой план строить: стандартный (Map) или affinity (HashShuffle).

**Было:** `csWriteAffinity` всегда `false` (поле `CtasShardingColumns` не существовало в `TKqlFillTable`).

**Стало (без аффинити):** `csWriteAffinity` всегда `false` (поле `CtasShardingColumns` не заполняется при `EnableCsWriteAffinity=false`, раздел 2.1.1).

**Стало (с аффинити):** `csWriteAffinity` = `true` (поле `CtasShardingColumns` заполнено при `EnableCsWriteAffinity=true`).

##### 2.2.2.2 Путь A: pure expression (нет входных данных)

Если `IsDqPureExpr(node.Input())` — `true` (CTAS без чтения из таблиц, например `CREATE TABLE t AS SELECT 1 AS x`):

```cpp
// kqp_opt_effects.cpp:241-279
if (IsDqPureExpr(node.Input())) {
    if (csWriteAffinity) {
        auto sink = BuildTableSink(ctx, node.Pos(), table, ...);
        auto transformStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs().Build()  // No inputs — pure stage
            .Program().Args({}).Body<TCoToFlow>().Input(node.Input()).Build().Build()
            .Settings().Build()
            .Done();
        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), node.CtasShardingColumns(), sink.Ptr()))
            .SinkIndex().Build("0")
            .Done();
        return true;
    } else {
        auto stageInput = RebuildPureStageWithSink(
            node.Input(), table, ..., settings, priority, ctx);
        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput.Ptr())
            .SinkIndex().Build("0")
            .Done();
    }
    return true;
}
```

**Без аффинити:** [`RebuildPureStageWithSink()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:78) создаёт один stage с inlined sink (стандартный путь).

**С аффинити:** Создаётся `transformStage` (pure stage без входов, `ToFlow(input)`) и `sink` ([`BuildTableSink()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:45)), затем [`BuildCsWriteAffinitySinkStage()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:171) (раздел 2.2.3) оборачивает их в HashShuffle-схему: Transform → HashShuffle → Sink.

**Было:** Строится план: Pure Stage с inlined sink (1 задача).

**Стало (без аффинити):** Строится план: Pure Stage с inlined sink (1 задача). Логика не изменилась.

**Стало (с аффинити):** Строится план: Pure Stage → HashShuffle(`ColumnShardHashV1`) → Sink Stage (N per-shard задач).

##### 2.2.2.3 Путь B: DQ-union (есть входные данные)

Если `node.Input()` — `TDqCnUnionAll` (CTAS с чтением из таблиц, стандартный случай):

```cpp
// kqp_opt_effects.cpp:281-347
if (!EnsureDqUnion(node.Input(), ctx)) {
    return false;
}

auto settingsNode = Build<TCoNameValueTupleList>(ctx, node.Pos())
    .Add(settings).Done();

auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
auto stage = dqUnion.Output().Stage();
auto program = stage.Program();
auto input = program.Body();

auto sink = BuildTableSink(ctx, node.Pos(), table, ...);

const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
    .Name("row").Done();

auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
    .Output(dqUnion.Output()).Done();

if (csWriteAffinity) {
    auto stageInput = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(mapCn).Build()
        .Program().Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build().Build()
        .Settings().Build()
        .Done();

    effect = Build<TKqpSinkEffect>(ctx, node.Pos())
        .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), stageInput.Ptr(), node.CtasShardingColumns(), sink.Ptr()))
        .SinkIndex().Build("0")
        .Done();
} else {
    auto stageInput = Build<TDqStage>(ctx, node.Pos())
        .Inputs().Add(mapCn).Build()
        .Program().Args({rowArgument})
            .Body<TCoToFlow>().Input(rowArgument).Build().Build()
        .Outputs<TDqStageOutputsList>().Add(sink).Build()
        .Settings().Build()
        .Done();

    effect = Build<TKqpSinkEffect>(ctx, node.Pos())
        .Stage(stageInput.Ptr())
        .SinkIndex().Build("0")
        .Done();
}
```

Последовательность:
1. `EnsureDqUnion()` — валидация, что Input — `TDqCnUnionAll`
2. [`BuildTableSink()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:45) — создаёт `TDqSink` (настройки записи: `mode="fill_table"`, `InconsistentWrite=true`, `StreamWrite=true`)
3. `TDqCnMap` — 1:1 соединение от DQ-union к следующему stage
4. **Без аффинити:** `TDqStage` с `Map`-входом и inlined `sink` в `Outputs` (стандартный путь)
5. **С аффинити:** `TDqStage` (Transform) с `Map`-входом и `ToFlow(row)`, затем `BuildCsWriteAffinitySinkStage()` (раздел 2.2.3) добавляет HashShuffle и Sink Stage

**Было:** Строится план: Transform Stage → `Map` (1:1 COPY) → Sink Stage (1 задача, inlined sink).

**Стало (без аффинити):** Строится план: Transform Stage → `Map` (1:1 COPY) → Sink Stage (1 задача, inlined sink). Логика не изменилась.

**Стало (с аффинити):** Строится план: Transform Stage → `Map` → HashShuffle(`ColumnShardHashV1`) → Sink Stage (N per-shard задач).

##### 2.2.2.4 Выход: `TKqpSinkEffect`

Результат — узел `TKqpSinkEffect` с физическим планом (DQ-граф стадий):

**Без аффинити (путь B, стандартный CTAS):**
```
TKqpSinkEffect
└── Stage: TDqStage (Sink Stage)
    ├── Inputs: [TDqCnMap → DQ-union (Transform Stage)]
    ├── Program: ToFlow(row)
    └── Outputs: [TDqSink (mode="fill_table")]
```

**С аффинити (путь B):**
```
TKqpSinkEffect
└── Stage: TDqStage (Sink Stage)
    ├── Inputs: [TDqCnHashShuffle]
    │   ├── Output: TDqOutput → Transform Stage (через Map)
    │   ├── KeyColumns: [Col1, Col2]
    │   ├── HashFunc: "ColumnShardHashV1"
    │   └── UseSpilling: false
    ├── Program: ToFlow(sinkRow)
    └── Outputs: [TDqSink (mode="fill_table")]
```

**С аффинити (путь A, pure expression):**
```
TKqpSinkEffect
└── Stage: TDqStage (Sink Stage)
    ├── Inputs: [TDqCnHashShuffle]
    │   ├── Output: TDqOutput → Pure Stage (без входов)
    │   ├── KeyColumns: [Col1, Col2]
    │   ├── HashFunc: "ColumnShardHashV1"
    │   └── UseSpilling: false
    ├── Program: ToFlow(sinkRow)
    └── Outputs: [TDqSink (mode="fill_table")]
```

**Было:** `TKqpSinkEffect` с планом Transform → Map → Sink (inlined).

**Стало (без аффинити):** `TKqpSinkEffect` с планом Transform → Map → Sink (inlined). Логика не изменилась.

**Стало (с аффинити):** `TKqpSinkEffect` с планом Transform → Map → HashShuffle → Sink.

#### 2.2.3 BuildCsWriteAffinitySinkStage: Transform → HashShuffle → Sink

[`BuildCsWriteAffinitySinkStage()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:171) инкапсулирует паттерн Transform → HashShuffle → Sink. Вызывается из `BuildFillTableEffect()` (раздел 2.2.2) при `csWriteAffinity=true`.

**Вход:**
```
transformStage: TDqStage — уже построенный Transform Stage (источник строк)
shardingColumns: TMaybeNode<TCoAtomList> — CtasShardingColumns из TKqlFillTable
sinkNode: TDqSink — узел записи в таблицу (BuildTableSink, раздел 2.2.2)
```

##### 2.2.3.1 Построение `keyColumnAtoms` из `CtasShardingColumns`

```cpp
// kqp_opt_effects.cpp:178-183
TVector<TCoAtom> keyColumnAtoms;
if (shardingColumns.IsValid()) {
    for (const auto& col : shardingColumns.Cast()) {
        keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, pos).Value(col.Value()).Done());
    }
}
```

Каждое имя колонки из `CtasShardingColumns` копируется в новый `TCoAtom`. Результат — список атомов, используемых как `KeyColumns` в HashShuffle.

**Было:** Функция `BuildCsWriteAffinitySinkStage()` не существовала.

**Стало (без аффинити):** Функция не вызывается (`csWriteAffinity=false`).

**Стало (с аффинити):** Функция вызывается, `keyColumnAtoms` заполняется из `CtasShardingColumns`.

##### 2.2.3.2 Построение `TDqCnHashShuffle`

```cpp
// kqp_opt_effects.cpp:185-195
auto sinkInput = Build<TDqCnHashShuffle>(ctx, pos)
    .Output<TDqOutput>()
        .Stage(transformStage)     // Источник данных — Transform Stage
        .Index().Build("0")        // Индекс выхода из Transform Stage
        .Build()
    .KeyColumns()
        .Add(keyColumnAtoms)       // Ключи шардирования из CTAS
    .Build()
    .UseSpilling().Build(false)    // Spilling отключён
    .HashFunc().Build("ColumnShardHashV1")  // Функция хеширования
    .Done();
```

| Поле | Значение | Назначение |
|------|----------|------------|
| `Output.Stage` | `transformStage` | Источник строк — Transform Stage |
| `Output.Index` | `"0"` | Индекс выхода из Transform Stage |
| `KeyColumns` | `keyColumnAtoms` | Колонки для вычисления хэша (из `CtasShardingColumns`) |
| `HashFunc` | `"ColumnShardHashV1"` | Та же hash-функция, что использует ColumnShard для маршрутизации строк |
| `UseSpilling` | `false` | Spilling на диск отключён |

`TDqCnHashShuffle` — соединение между Transform Stage и Sink Stage. При исполнении каждая строка маршрутизируется: `hash(KeyColumns) → bucket → task i`.

**Было:** Соединение Transform→Sink — `TDqCnMap` (1:1 COPY, без маршрутизации).

**Стало (без аффинити):** Соединение Transform→Sink — `TDqCnMap` (1:1 COPY, логика не изменилась).

**Стало (с аффинити):** Соединение Transform→Sink — `TDqCnHashShuffle` с `ColumnShardHashV1`.

##### 2.2.3.3 Построение `TDqStage` (Sink Stage)

```cpp
// kqp_opt_effects.cpp:197-215
const auto sinkRowArgument = Build<TCoArgument>(ctx, pos)
    .Name("sinkRow").Done();

auto sinkStage = Build<TDqStage>(ctx, pos)
    .Inputs()
        .Add(sinkInput)            // Вход — HashShuffle
        .Build()
    .Program()
        .Args({sinkRowArgument})
        .Body<TCoToFlow>()
            .Input(sinkRowArgument) // ToFlow(sinkRow)
            .Build()
        .Build()
    .Outputs<TDqStageOutputsList>()
        .Add(sinkNode)             // Выход — TDqSink
        .Build()
    .Settings().Build()
    .Done();
```

| Поле | Значение | Назначение |
|------|----------|------------|
| `Inputs` | `[sinkInput]` (HashShuffle) | Маршрутизация строк от Transform Stage |
| `Program` | `ToFlow(sinkRow)` | Преобразование строки в поток (pass-through) |
| `Outputs` | `[sinkNode]` (TDqSink) | Запись в таблицу (`mode="fill_table"`) |

Sink Stage — конечная стадия: принимает строки через HashShuffle и записывает их в `TDqSink`.

**Было:** Sink Stage с `Map`-входом и inlined sink (1 задача).

**Стало (без аффинити):** Sink Stage с `Map`-входом и inlined sink (1 задача, логика не изменилась).

**Стало (с аффинити):** Sink Stage с `HashShuffle`-входом и `TDqSink` в `Outputs` (N per-shard задач).

##### 2.2.3.4 Выход: Sink Stage

Функция возвращает `sinkStage.Ptr()` — узел `TDqStage`, который становится `Stage` в `TKqpSinkEffect` (раздел 2.2.2.4).

**Итоговая топология:**
```
Transform Stage (1 задача)
    │
    ▼
TDqCnHashShuffle(ColumnShardHashV1, KeyColumns=CtasShardingColumns)
    │ hash(sharding_key) → bucket → task i
    ▼
Sink Stage (N задач, по одной на шард)
    │ ToFlow(sinkRow)
    ▼
TDqSink → WriteActor → ColumnShard[i] (локально)
```

**Было:** Transform → Map → Sink (1 задача, запись через сеть во все шарды).

**Стало (без аффинити):** Transform → Map → Sink (1 задача, логика не изменилась).

**Стало (с аффинити):** Transform → HashShuffle → Sink (N задач, локальная запись в свой шард).

[`PropogateHashFuncToHashShuffles`](ydb/core/kqp/opt/kqp_opt_hash_func_propagate_transformer.cpp:53) сохраняет `ColumnShardHashV1`, не перезаписывая на `HashV2`.

#### 2.2.4 Code Generation и Proto finalization

Компилятор сериализует физический план (DQ-граф из разделов 2.2.2–2.2.3) в `TKqpPhyTx` proto — единственный канал передачи данных от компилятора к исполнителю. Механизм сериализации (`kqp_query_compiler.cpp`) **не изменяется в этой ветке**: стандартный `CompileStage()` преобразует `TDqCnHashShuffle` в `TKqpPhyCnHashShuffle` с использованием существующих proto-полей.

Сводная таблица ключевых данных в proto:

| Поле proto | Было | Стало (без аффинити) | Стало (с аффинити) | Источник |
|------------|------|----------------------|---------------------|----------|
| `TKqpPhyCnHashShuffle.ColumnShardHashV1` | нет HashShuffle | нет HashShuffle (`Map`) | `oneof HashKind = ColumnShardHashV1` | Оптимизатор (раздел 2.2.3) |
| `TKqpPhyCnHashShuffle.KeyColumns` | не заполняется | не заполняется | `["Col1", ...]` | `CtasShardingColumns` из Rewrite-фазы (раздел 2.1.1) |
| `TKqpPhyCnHashShuffle.ColumnShardHashV1.KeyColumnTypes` | не заполняется | не заполняется | `[typeId(Col1), ...]` | Query compiler: типы резолвятся из struct-типа соединения (типы колонок SELECT известны на этапе компиляции) |

**Типы ключевых колонок.** Имена sharding-колонок (`KeyColumns`) и их типы (`KeyColumnTypes`) доносятся до физического плана **одновременно и тем же механизмом**: query compiler в [`FillConnection()`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:2689) для каждого `ColumnShardHashV1`-shuffle резолвит тип каждой key-колонки из struct-типа соединения (типы колонок SELECT известны на этапе компиляции) и заполняет [`TColumnShardHashV1.KeyColumnTypes`](ydb/core/protos/kqp_physical.proto:260). Исполнитель читает типы из proto — runtime-вывод типов из `ResolvedSinkSettings` не требуется (раздел 2.3.5).


### 2.3 Executor

**Вход:** `TKqpPhyTx` proto — физический план, сериализованный компилятором (описание полей — раздел 2.2.4).

**Различия между CS Write Affinity и Shuffle Elimination в Executor:**

| Аспект | CS Write Affinity | Shuffle Elimination |
|--------|-------------------|---------------------|
| **Цель** | Маршрутизация строк в целевую таблицу (запись) | Сохранение партиционирования при чтении из источника |
| **Таблица** | Целевая (куда пишем) | Источниковая (откуда читаем) |
| **`orderedShardIds`** | `GetCsWriteAffinityShardIds()` → `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` | `GetCsWriteAffinityShardIds()` → `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` |
| **`shardToTaskIdx`** | Из `task.Meta.Writes` (1 task = 1 shard) | Из `task.Meta.Reads` (1 task может читать K shards) |
| **`SourceShardCount`** | N (число shards целевой таблицы) | N (число shards источниковой таблицы) или `stageInfo.Tasks.size()` (identity) |
| **`TaskIndexByHash`** | `BuildTaskIndexByHash(orderedShardIds, shardToTaskIdx)` | `BuildTaskIndexByHash(orderedShardIds, shardToTaskIdx)` или identity |
| **Когда вызывается** | `BuildKqpStageChannels` (обработка HashShuffle) | `BuildScanTasksFromShards` (построение scan tasks) или `BuildKqpStageChannels` (identity) |
| **Общие helpers** | `BuildColumnShardHashV1TaskIndexByHash`, `BuildTaskIndexByHash`, `ReadColumnShardHashV1KeyColumnTypes` | `BuildColumnShardHashV1TaskIndexByHash`, `BuildTaskIndexByHash`, `ReadColumnShardHashV1KeyColumnTypes` |

Ключевые поля, используемые исполнителем:
- `Stages[].Inputs[].HashShuffle` — наличие `HashShuffle` с `ColumnShardHashV1` определяет, что план построен с affinity (раздел 2.2.3)
- `Stages[].Inputs[].HashShuffle.KeyColumns` — sharding columns (заполняются `FillStages()`)
- `Stages[].Inputs[].HashShuffle.ColumnShardHashV1.KeyColumnTypes` — типы sharding-колонок (заполняются query compiler'ом, раздел 2.2.4)
- `Stages[].Sinks[].InternalSink` — настройки sink (заполняются Table Resolver'ом)

**Детекция affinity:** выполняется **по стадиям** и **один раз** — в `FillStages()` (раздел 2.3.1). Статическая функция [`IsCsWriteAffinitySinkStage(stage)`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:624) проверяет форму стадии по raw proto — ровно один вход `HashShuffle(ColumnShardHashV1)` + ровно один internal sink `TKqpTableSinkSettings(MODE_FILL)`. Такая стадия однозначно является sink-стадией affinity-плана (раздел 2.2.3): не-affinity CTAS использует Map-вход, а shuffle-eliminated планы чтения/записи не имеют MODE_FILL sink'а. При прохождении проверки `FillStages()` устанавливает `meta.IsCsWriteAffinity = true` — **этот bool и есть маркер affinity**, который все четыре точки читают через [`TStageInfoMeta::IsCsWriteAffinitySink()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.h): сбор шардов в `HandleResolve` (раздел 2.3.2.1), per-shard задачи в `CountComputeTasks` (раздел 2.3.3), `TargetShardIds` в `BuildInternalSinks` (раздел 2.3.4) и skip shuffle elimination в `BuildKqpStageChannels` (раздел 2.3.5) — поэтому сбор шардов и активация affinity-пути всегда согласованы. Запрос-level флага нет: в одном запросе могут соседствовать affinity-стадии (CTAS sink) и обычные стадии.

KqpExecuter превращает физический план (`TKqpPhyTx`) в исполняемые задачи и маршрутизирует данные к целевым ColumnShard'ам. Для CTAS с write affinity ключевая задача — создать **N per-shard задач** (по одной на шард), пиннить каждую к ноде своего шарда и настроить HashShuffle-маршрутизацию, чтобы каждая строка попала в задачу, владеющую её шардом.

Исполнитель обрабатывает `TKqpPhyTx` proto последовательно, строя граф задач. В этой ветке изменены 5 этапов (разделы 2.3.1–2.3.5); остальные этапы (`TKqpShardsResolver`, `ResolveShards`, `PlaceTasks`, `BuildComputeTasks`) не изменяются и упоминаются только там, где это необходимо для понимания изменений. Далее — этап runtime-исполнения (раздел 2.4). Ниже для каждого этапа описано состояние **до оптимизации** (старый код), **после оптимизации без аффинити** (план без HashShuffle) и **после оптимизации с аффинити** (план с HashShuffle).

**Было** — логический план (3 стадии):
```
Stage 0: Scan/Compute (чтение из таблицы или вычисление)
    ↓ Map (1:1, COPY)
Stage 1: Transform (compute-логика: SELECT, фильтры, агрегации)
    ↓ Map (1:1, COPY)
Stage 2: Sink (write-логика, 1 задача на executer-ноде)
    └── TShardedWriteController
            ├── Hash(PK) → CS[0]  (сеть)
            ├── Hash(PK) → CS[1]  (сеть)
            └── Hash(PK) → CS[N]  (сеть)
```

Физическое исполнение (3 задачи): Scan/Compute → Transform → Sink (1 WriteActor, маршрутизация по сети). Нет node affinity — WriteActor на executer-ноде, все per-shard буферы в одном месте.

**Стало (без аффинити)** — логический план (3 стадии): тот же, что и "Было". Оптимизатор не строит HashShuffle, соединение Transform→Sink остаётся `Map`. Исполнитель создаёт 1 задачу в Sink Stage. Новые поля (`IsCsWriteAffinity`, `TargetShardIds`) существуют в коде, но не заполняются (false/пустые).

**Стало (с аффинити)** — логический план (4 стадии):
```
Stage 0: Scan/Compute (чтение из таблицы или вычисление)
    ↓ Map (1:1, COPY)
Stage 1: Transform (compute-логика: SELECT, фильтры, агрегации)
    ↓ HashShuffle (ColumnShardHashV1, hash(sharding_key) → task i)
Stage 2: Sink (write-логика, N задач — по одной на шард)
    WriteActor[0] на Node(CS[0]) → CS[0]  (local)
    WriteActor[1] на Node(CS[1]) → CS[1]  (local)
    ...
    WriteActor[N] на Node(CS[N]) → CS[N]  (local)
Stage 3: (дополнительная стадия для table creation в CTAS)
```

Физическое исполнение (N+3 задачи): Scan/Compute → Transform → N Sink-задач (каждая на ноде своего шарда) → Table creation. N задач = N шардов (Per-Shard модель), запись локальная, per-shard буферы распределены по нодам.

#### 2.3.1 `FillStages()` — заполнение метаданных стадий

[`FillStages()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:619) обходит все стадии `TKqpPhyTx` и заполняет `TStageInfoMeta` для каждой.

**Вход:** `TKqpPhyTx` proto — физический план (раздел 2.2.4). Для каждой стадии: `Stages[].Inputs[]` (соединения), `Stages[].Sinks[]` (настройки sink), `Stages[].Sources[]` (источники чтения).

**Было:**
- Поля `IsCsWriteAffinity` не существует в `TStageInfoMeta` — оно добавлено как часть работы CS Write Affinity
- `ShardOperations` = `{Update}` (MODE_FILL → Update)
- `TableId`, `TablePath` не заполняются для MODE_FILL (таблица ещё не существует)
- `TableConstInfo` = `nullptr` для MODE_FILL
- `ColumnTableInfoPtr` = `nullptr` (заполняется Table Resolver'ом для OLAP-таблиц, включая CTAS sink-стадии)
- `ShardKey` = `nullptr`
- `ResolvedSinkSettings` = `nullopt`

**Стало (без аффинити):**
- `IsCsWriteAffinity` существует в `TStageInfoMeta`, но остаётся `false` — нет HashShuffle-входа с `kColumnShardHashV1` (оптимизатор построил `Map`, а не `HashShuffle`)
- Остальные поля — как в "Было": `ColumnTableInfoPtr=nullptr`, `ShardKey=nullptr`, `ResolvedSinkSettings=nullopt`

**Стало (с аффинити):**
- `IsCsWriteAffinity` устанавливается в `true` **только** если стадия проходит проверку affinity-формы `IsCsWriteAffinitySinkStage(stage)` ([`kqp_tasks_graph.cpp:624`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:624)):
  ```cpp
  meta.IsCsWriteAffinity = IsCsWriteAffinitySinkStage(stage);
  ```
  `IsCsWriteAffinity = true` — маркер affinity: `Meta.IsCsWriteAffinitySink()` возвращает `IsCsWriteAffinity`, и все последующие этапы (2.3.2.1–2.3.5) читают этот helper. Для не-affinity стадий с `ColumnShardHashV1`-входом (shuffle elimination) `IsCsWriteAffinity` остаётся `false`.
- Остальные поля (`TableId`, `TableConstInfo`, `ColumnTableInfoPtr`, `ShardKey`, `ResolvedSinkSettings`) по-прежнему пусты — они заполняются позже Table Resolver'ом.

#### 2.3.2 `ResolveShards()` — разрешение шардов на ноды

Цель: построить маппинг `ShardIdToNodeId` — для каждого шарда определить, на какой ноде он размещён. Этот маппинг используется в `BuildAllTasks()` (раздел 2.3.3) для пиннинга per-shard задач к нодам.

**Вход:**
- `TasksGraph` (заполнен в разделе 2.3.1): `ShardKey` (ключ + партиции), `Sinks` (настройки sink)
- `TxManager` — управляет партиционированием
- `PartitionPruner` — вычисляет нужные партиции для чтения
- `ColumnShardHashV1` в DQ-графе — наличие определяет affinity-путь

Процесс состоит из трёх асинхронных шагов:

```
Executer → TableResolver → (схемы) → TEvTableResolveStatus
    → HandleResolve: сбор shardIds
    → TKqpShardsResolver → (pipe cache) → TEvShardsResolveStatus
    → HandleResolve: TasksGraph.ResolveShards()
```

##### 2.3.2.1 Сбор `shardIds`

**Инициация.** При получении запроса executer создаёт `TKqpTableResolver` ([`kqp_executer_impl.h:1259`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:1259)) и переходит в состояние `WaitResolveState`. TableResolver навигирует по схемам, получает `TableId`, `ShardKey` и `ColumnTableInfoPtr` (для OLAP-таблиц) для каждой таблицы, затем отправляет `TEvTableResolveStatus` обратно.

**Сбор.** [`HandleResolve(TEvTableResolveStatus)`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:251) вызывается по этому событию. На этом моменте `ShardKey` и `ColumnTableInfoPtr` (для OLAP) уже заполнены. Для каждой стадии в `TasksGraph.GetStagesInfo()`:

1. Устанавливает партиционирование в `TxManager` (`SetPartitioning`), пропускает sysview-таблицы.
2. Определяет тип стадии и собирает `shardIds`:

   | Тип стадии | Условие | Источник shardIds |
   |-----------|---------|-------------------|
   | Read source | `Sources(0).Type == kReadRangesSource` | `PartitionPruner->Prune()` → `PrunedPartitions` → `shardId` |
   | Scan / OLAP | `IsScan() \|\| IsOlap()` | `PartitionPruner->Prune()` для каждого `TableOp` → `shardId` |

   **Важно:** CTAS sink-стадия классифицируется Table Resolver'ом как `IsOlap()` (`TableKind=Olap`, [`kqp_table_resolver.cpp:108`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:108)) и проходит через ветку Scan/OLAP, которая для неё ничего не собирает (у sink-стадии нет `TableOps`). Поэтому сбор шардов целевой таблицы выполнен отдельным блоком **на уровне цикла по стадиям** (после всей цепочки веток, [`kqp_executer_impl.h:336`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:336)) — он выполняется для каждой стадии и читает маркер affinity, вычисленный один раз в `FillStages()` (раздел 2.3.1):
   - Условие: `stageInfo.Meta.IsCsWriteAffinitySink()` (`IsCsWriteAffinity == true`) — тот же маркер, что читают `CountComputeTasks`, `BuildInternalSinks` и `BuildKqpStageChannels`
   - Шарды: [`GetCsWriteAffinityShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1397) — `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` (заполняется Table Resolver'ом)

3. Если `shardIds` не пуст → создаёт `TKqpShardsResolver` (шаг 2.3.2.2). Если пуст → сразу вызывает `TasksGraph.ResolveShards({})` и продолжает.

##### 2.3.2.2 Разрешение shard → node и сохранение результата (не изменяется в этой ветке)

`TKqpShardsResolver` для каждого `tabletId` из `shardIds` определяет через pipe cache (`TEvGetTabletNode`), на какой ноде размещён шард, и возвращает `TEvShardsResolveStatus`. `HandleResolve(TEvShardsResolveStatus)` добавляет ноды в `TxManager` и вызывает `TasksGraph.ResolveShards()`, который заполняет:

- `ShardIdToNodeId`: `TMap<ui64 shardId, ui64 nodeId>` — авторитетная карта размещения (используется в разделе 2.3.3 для пиннинга per-shard задач к нодам)
- `ShardsOnNode`: `TMap<ui64 nodeId, TVector<ui64 shardId>>` — обратный индекс (используется в `InvalidateNode()`)

**Было:**
- Sink-стадии (CTAS): `else`-ветка содержит только `TODO`-комментарий, shardIds не добавляются
- `ShardIdToNodeId`: только шарды read-таблиц
- Node affinity: недостижим

**Стало (без аффинити):**
- Sink-стадии (CTAS): то же — нет `ColumnShardHashV1`-входа, условие не срабатывает
- `ShardIdToNodeId`: только шарды read-таблиц
- Node affinity: недостижим

**Стало (с аффинити):**
- Sink-стадии (CTAS): `GetCsWriteAffinityShardIds()` → `shardIds` (условие: `IsCsWriteAffinitySink()`)
- `ShardIdToNodeId`: шарды read-таблиц **+ шарды целевой таблицы**
- Node affinity: достижим (задача пиннится к ноде своего шарда)

#### 2.3.3 `BuildAllTasks()` — подсчёт задач (`CountTasks`)

[`BuildAllTasks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4187) вызывает `CountComputeTasks` для COMPUTE_TASKS стадий.

**Вход:** `TasksGraph` с заполненными `ShardIdToNodeId` (раздел 2.3.2) и `IsCsWriteAffinity` (раздел 2.3.1). Для affinity-стадий: `ColumnTableInfoPtr` (заполняется Table Resolver'ом).

**Было:**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (т.к. `ShardOperations` не пуст и есть sink)
- `CountComputeTasks` создаёт 1 задачу (стандартный путь): `partitionsCount` определяется по upstream-стадии (Map-соединение → COPY)
- Задача размещается на executer-ноде (`StageNeedsLocalPlacement`)
- `TasksType` = `COMPUTE_TASKS`
- `task.Meta.Writes` — не заполнен

**Стало (без аффинити):**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (то же условие)
- Нет affinity-плана (оптимизатор построил `Map`) → `IsCsWriteAffinity=false` → `Meta.IsCsWriteAffinitySink()=false` → условие affinity-блока не выполняется
- Создаётся 1 задача (стандартный путь), как в "Было"
- `task.Meta.Writes` — не заполнен

**Стало (с аффинити):**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (то же условие)
- Условие affinity-блока: `stageInfo.Meta.IsCsWriteAffinitySink()` → true (маркер подразумевает ровно один вход, т.е. стадия не pure)
- Создаются **per-shard задачи** ([`kqp_tasks_graph.cpp:4417`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4417)):
  ```cpp
  MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);
  for (const auto& [shardId, nodeId] : shardNodes) {
      auto& task = AddTask(stageInfo, TTask::UNKNOWN);
      task.Meta.Writes.ConstructInPlace();
      task.Meta.Writes->emplace_back(TTaskMeta::TShardInfo{.ShardId = shardId});
      MaxTasksGraph->AddTask(task, nodeId);
  }
  ```
- Источник `shardNodes` — [`GetCsWriteAffinityShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1397): `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` (заполняется Table Resolver'ом). Тот же helper используется при сборе shardIds в `HandleResolve` (раздел 2.3.2.1), в `BuildInternalSinks` (раздел 2.3.4) и `BuildColumnShardHashV1ForWriteAffinity` (раздел 2.3.5) — все этапы видят одинаковый набор и порядок шардов
- Каждая задача получает `TShardInfo{.ShardId = shardId}` в `task.Meta.Writes`
- `stageType` = `FIXED` (количество задач = количество шардов, не зависит от upstream)
- **Инварианты** (нарушение → `YQL_ENSURE` с ошибкой):
  - Каждый shardId должен присутствовать в `ShardIdToNodeId` (заполняется `ResolveShards`)
  - `ColumnTableInfoPtr` должна быть заполнена (Table Resolver)
- После создания per-shard задач — **early return** (стандартный путь не выполняется)
- Последующий `PlaceTasks()` (`max_tasks_graph.cpp`, не изменяется в этой ветке) переупорядочивает задачи по нодам — позиционный индекс больше не соответствует шарду, поэтому связь задачи с шардом хранится в `task.Meta.Writes` и используется в `BuildInternalSinks` (раздел 2.3.4) и `BuildColumnShardHashV1ForWriteAffinity` (раздел 2.3.5). `BuildComputeTasks()` (не изменяется) далее вызывает `BuildSinks` для каждой задачи.

Sharding columns для affinity-стадий берутся из `KeyColumns` HashShuffle proto (раздел 2.3.1).

**Почему для CTAS используется HashShuffle proto?** Компилятор знает sharding columns из CREATE TABLE (PARTITION BY или PRIMARY KEY) и записывает их в `KeyColumns` proto. `FillStages()` проверяет форму стадии через `IsCsWriteAffinitySinkStage()` и устанавливает `IsCsWriteAffinity = true`.

**Шарды**: `GetCsWriteAffinityShardIds()` использует `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` (заполняется Table Resolver'ом). Если `ColumnTableInfoPtr` пуст — `YQL_ENSURE` с ошибкой. Порядок задач совпадает с порядком `GetColumnShards()` — критично для `TaskIndexByHash`.

#### 2.3.4 `BuildSinks()` → `BuildInternalSinks()` — заполнение TargetShardIds

[`BuildInternalSinks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4022) заполняет `TargetShardIds` в sink settings.

**Вход:** `stageInfo.Tasks` с `Writes` (раздел 2.3.3). `stageInfo.Meta.IsCsWriteAffinitySink()` — признак affinity (вычислен один раз в `FillStages()`, раздел 2.3.1). `ColumnTableInfoPtr` — источник `resolvedShardIds`.

**Было:**
- `TargetShardIds` остаётся пустым
- WriteActor обрабатывает все шарды (без фильтрации)
- 1 задача пишет все шарды

**Стало (без аффинити):**
- `TargetShardIds` остаётся пустым — `Meta.IsCsWriteAffinitySink()` false (нет affinity-плана: оптимизатор построил `Map`, а не `HashShuffle(ColumnShardHashV1)`, `IsCsWriteAffinity` не установлен)
- WriteActor обрабатывает все шарды (без фильтрации) — как в "Было"
- 1 задача пишет все шарды

**Стало (с аффинити):**
- При `stageInfo.Meta.IsCsWriteAffinitySink()`:
  - `resolvedShardIds` заполняется из `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()`
  - Каждая задача получает ровно 1 шард (из `task.Meta.Writes`):
    ```cpp
    ui64 shardId = task.Meta.Writes->front().ShardId;
    YQL_ENSURE(std::find(resolvedShardIds.begin(), resolvedShardIds.end(), shardId) != resolvedShardIds.end(), ...);
    settings.AddTargetShardIds(shardId);
    ```
- WriteActor фильтрует строки по `TargetShardIds` (отбрасывает строки для чужих шардов)

`TargetShardIds = {shard_i}` для задачи i. `resolvedShardIds` получается через [`GetCsWriteAffinityShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1356) (тот же источник и порядок, что при сборе shardIds в `HandleResolve` и в `CountComputeTasks`).

#### 2.3.5 `BuildKqpStageChannels()` → `BuildColumnShardHashV1ForWriteAffinity()` — построение hash routing

[`BuildKqpStageChannels()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1522) строит каналы между стадиями.
[`BuildColumnShardHashV1ForWriteAffinity()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1412) настраивает ColumnShardHashV1 routing, используя общий helper [`BuildColumnShardHashV1TaskIndexByHash()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1404) (тот же, что и shuffle elimination).

**Вход:** `stageInfo` с заполненными задачами (раздел 2.3.3) и `TargetShardIds` (раздел 2.3.4). `task.Meta.Writes` — маппинг задач на шарды. `KeyColumns` и `KeyColumnTypes` — из proto (заполняются query compiler'ом, раздел 2.2.4).

**Было:**
- Соединение между Transform и Sink — `Map` (не HashShuffle)
- `BuildColumnShardHashV1ForWriteAffinity` не вызывается (нет `kColumnShardHashV1`)
- Канал: Map → все строки идут в 1 задачу

**Стало (без аффинити):**
- Соединение между Transform и Sink — `Map` (как в "Было", оптимизатор не построил HashShuffle)
- `BuildColumnShardHashV1ForWriteAffinity` не вызывается (нет `kColumnShardHashV1`)
- Канал: Map → все строки идут в 1 задачу

**Стало (с аффинити):**
- Соединение — `HashShuffle` с `kColumnShardHashV1`
- `BuildColumnShardHashV1ForWriteAffinity` вызывается и:
  1. Строит `shardToTaskIdx` из `task.Meta.Writes` (1 task = 1 shard)
  2. Вызывает [`BuildColumnShardHashV1TaskIndexByHash()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1404) (общий helper, используемый также shuffle elimination), который строит `orderedShardIds` через `GetCsWriteAffinityShardIds()` и `TaskIndexByHash` через `BuildTaskIndexByHash()`
  3. Заполняет `ColumnShardHashV1Params` на Transform-стадии:
     - `SourceShardCount` = N (число shards)
     - `TaskIndexByHash` = mapping
     - `SourceTableKeyColumnTypes` = типы ключевых колонок (из proto, через `ReadColumnShardHashV1KeyColumnTypes()`)
- Канал: HashShuffle → строки маршрутизируются по hash(sharding_key) → bucket → task

В case `kColumnShardHashV1` вызывается [`BuildColumnShardHashV1ForWriteAffinity`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1412). `TaskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а.

**Shuffle Elimination skip:** Для CTAS sink с `ColumnShardHashV1`-входом shuffle elimination отключён. Причина: shuffle elimination использует `stageInfo.Tasks.size()` (количество задач Transform-стадии) как `SourceShardCount`, что отражает количество шардов *источника*, а не *целевой* таблицы. Это вызывает несоответствие hash bucket'ов между DQ `ColumnShardHashV1` routing и runtime `TConsistencySharding64`. В коде это реализовано добавлением условия `!isCsWriteAffinitySink` в два места [`BuildKqpStageChannels`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1922) (строки 1923 и 1948):
```cpp
const bool isCsWriteAffinitySink = stageInfo.Meta.IsCsWriteAffinitySink();
if (enableShuffleElimination && !isCsWriteAffinitySink && !isFusedWithScanStage) { ... }
if (enableShuffleElimination && !isCsWriteAffinitySink && !hasMap && !isFusedWithScanStage && ...) { ... }
```

**Key columns**: `BuildHashShuffleChannels()` получает `KeyColumns` **напрямую из proto** (`input.GetHashShuffle().GetKeyColumns()`) — идентично shuffle elimination, без какой-либо конвертации. Это работает, потому что канал Transform→Sink для CTAS — узкий (`Struct`), и runtime [`FindColumnInfo()`](ydb/library/yql/dq/runtime/dq_columns_resolve.cpp:9) резолвит колонки **по имени** (`Struct.FindMemberIndex`). Для широких (`Multi`) каналов runtime ожидает числовые индексы, но CTAS sink всегда получает узкий канал, поэтому имена из proto (из `CtasShardingColumns`) используются как есть. Оба пути (write affinity и shuffle elimination) вызывают `BuildHashShuffleChannels` одинаково.

**Общий helper `BuildColumnShardHashV1TaskIndexByHash`:**

Оба пути (write affinity и shuffle elimination) используют одну и ту же функцию [`BuildColumnShardHashV1TaskIndexByHash()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1404), которая строит `TaskIndexByHash` из `orderedShardIds` + `shardToTaskIdx`:

| | Write Affinity | Shuffle Elimination (scan) | Shuffle Elimination (non-scan) |
|---|---|---|---|
| **`shardToTaskIdx`** | из `task.Meta.Writes` (1 task = 1 shard) | из `task.Meta.Reads` (1 task может читать K shards) | не нужен (identity) |
| **`orderedShardIds`** | `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` | `ColumnTableInfoPtr->Description.GetSharding().GetColumnShards()` | не нужен |
| **`SourceShardCount`** | N (число shards) | N (число shards) | `stageInfo.Tasks.size()` (число tasks) |
| **`TaskIndexByHash`** | `BuildColumnShardHashV1TaskIndexByHash(stageInfo, shardToTaskIdx)` | `BuildColumnShardHashV1TaskIndexByHash(stageInfo, shardToTaskIdx)` | identity: `TaskIndexByHash[i] = i` |
| **Когда вызывается** | `BuildColumnShardHashV1ForWriteAffinity` (из `BuildKqpStageChannels`) | `BuildScanTasksFromShards` | `BuildKqpStageChannels` (identity mapping) |

`BuildColumnShardHashV1ForWriteAffinity` — тонкая обёртка: строит `shardToTaskIdx` из `Writes`, вызывает `BuildColumnShardHashV1TaskIndexByHash`, заполняет params. Shuffle elimination вызывает `BuildColumnShardHashV1TaskIndexByHash` напрямую.

### 2.4 Runtime

#### 2.4.1 Выполнение задач и запись

**Вход:** Граф задач, полностью построенный в разделах 2.3.1–2.3.5. Для affinity: N задач с `TargetShardIds` (раздел 2.3.4) и `ColumnShardHashV1` routing (раздел 2.3.5). Каждая задача размещена на ноде своего шарда (раздел 2.3.3).

**Было:**
- 1 задача вычисляет все строки и пишет во все шарды
- WriteActor: `TColumnShardPayloadSerializer` обрабатывает все шарды
- `ShardAndFlushBatch()`: нет проверки `TargetShardIds`
- Node affinity: нет (всё на executer-ноде)

**Стало (без аффинити):**
- 1 задача вычисляет все строки и пишет во все шарды (как в "Было")
- WriteActor: `TColumnShardPayloadSerializer` обрабатывает все шарды
- `TargetShardIds` = `nullopt` → `SplitByShards()` всегда выполняет реальное hash-разделение (как в "Было")
- Node affinity: нет (всё на executer-ноде)

**Стало (с аффинити):**
- N задач (по одной на шард), каждая на ноде своего шарда
- Transform-стадия: `ColumnShardHashV1` hash-функция маршрутизирует строки по bucket'ам
- Каждая sink-задача получает только строки для своего шарда (через HashShuffle routing)
- WriteActor: `TargetShardIds` содержит ровно 1 шард
- `SplitByShards()`: fast path — при `TargetShardIds->size() == 1` весь batch передаётся этому шарду без hash-разделения (routing уже доставил строки в правильную задачу)
- Node affinity: каждая задача на ноде своего шарда → данные не пересекают ноды

Изменения в [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp) (`TColumnShardPayloadSerializer`):
- Конструктор ([`446`](ydb/core/kqp/runtime/kqp_write_table.cpp:446)): новый параметр `targetShardIds` → член `TargetShardIds` (`std::optional<THashSet<ui64>>`)
- [`SplitByShards()`](ydb/core/kqp/runtime/kqp_write_table.cpp:525): single-shard fast path (см. выше)
- [`ShardAndFlushBatch()`](ydb/core/kqp/runtime/kqp_write_table.cpp:541): не изменяется — вызывает `SplitByShards()` и накапливает батчи per-shard

Отладочная валидация корректности routing'а — compile-time флаг `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` (включается `./ya make -D KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK=yes`, см. [`runtime/ya.make`](ydb/core/kqp/runtime/ya.make)):
- Конструктор: `AFL_VERIFY(TargetShardIds.has_value())`; при `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT` — `AFL_VERIFY(TargetShardIds->size() == 1)`
- `SplitByShards()`: вместо fast path — проверяет, что реальное hash-разделение даёт ровно 1 шард
- Деструктор ([`484`](ydb/core/kqp/runtime/kqp_write_table.cpp:484)): `AFL_VERIFY(ShardIds ⊆ TargetShardIds)` — сериализатор не писал в чужие шарды
- WriteActor ([`kqp_write_actor.cpp:919`](ydb/core/kqp/runtime/kqp_write_actor.cpp:919)): `AFL_VERIFY(ev->Sender.NodeId() == SelfId().NodeId())` — ответ на запись пришёл от локального ColumnShard

#### 2.4.2 Proto поля: `TargetShardIds`, `ExpectedNodeId`

Поля в `TKqpTableSinkSettings` ([`kqp.proto:934-939`](ydb/core/protos/kqp.proto:934)):
```protobuf
message TKqpTableSinkSettings {
    // Target shard IDs for per-shard write affinity.
    // When set, the WriteActor only writes to the specified shards.
    repeated uint64 TargetShardIds = 31;
    // Expected node ID for task scheduling affinity.
    // When set, the task will be scheduled on the specified node.
    optional uint64 ExpectedNodeId = 32;
}
```

**`TargetShardIds` (repeated uint64, field #31)**

Назначение: список shard ID, которые эта задача WriteActor должна записывать. В multi-task случае каждая задача владеет ровно одним шардом; в single-task случае единственная задача получает все шарды (раздел 2.3.4).

Где задаётся: [`kqp_tasks_graph.cpp:4022`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4022) в `BuildInternalSinks()`:
```cpp
settings.AddTargetShardIds(shardId);
```

Как используется:
- [`kqp_write_actor.cpp:2948`](ydb/core/kqp/runtime/kqp_write_actor.cpp:2948) — `TargetShardIdsFromSettings()` читает proto, создаёт `THashSet<ui64>`; передаётся через `TShardedWriteControllerSettings.TargetShardIds` ([`kqp_write_table.h:266`](ydb/core/kqp/runtime/kqp_write_table.h:266)) в `TColumnShardPayloadSerializer`
- [`kqp_write_table.cpp:525`](ydb/core/kqp/runtime/kqp_write_table.cpp:525) — `SplitByShards()`: single-shard fast path
- Под флагом `KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` — валидация в конструкторе/деструкторе и в WriteActor (раздел 2.4.1)

**`ExpectedNodeId` (optional uint64, field #32)**

Поле объявлено в proto, но **в текущей ветке не используется** (нигде не заполняется и не читается) — зарезервировано. Фактический пиннинг задач к нодам выполняется существующим in-memory механизмом (не изменяется в этой ветке): `CountComputeTasks` вызывает `MaxTasksGraph->AddTask(task, nodeId)` (раздел 2.3.3) → `PlaceTasks()` проставляет `task.Meta.ExpectedNodeId` (`TTaskMeta`, [`max_tasks_graph.cpp:543`](ydb/core/kqp/executer_actor/max_tasks_graph.cpp:543)) → `TKqpPlanner` размещает задачи с установленным `ExpectedNodeId` напрямую на этой ноде ([`kqp_planner.cpp:349`](ydb/core/kqp/executer_actor/kqp_planner.cpp:349)).

**Было (origin/main):**
- Поле `TargetShardIds` отсутствует в `TKqpTableSinkSettings`
- WriteActor обрабатывает все шарды без фильтрации
- Single-shard fast path в `SplitByShards()` отсутствует

**Стало (без аффинити):**
- Поле `TargetShardIds` существует в proto, но не заполняется → `TargetShardIdsFromSettings()` возвращает `nullopt`
- WriteActor обрабатывает все шарды без фильтрации (как в "Было")

**Стало (с аффинити):**
- `TargetShardIds` заполняется в `BuildInternalSinks()`: каждая задача получает свой шард
- WriteActor: single-shard fast path в `SplitByShards()`; под отладочным флагом — валидация, что запись не ушла в чужие шарды и что шард локален
- Node affinity: задача выполняется на ноде своего ColumnShard (через `TTaskMeta::ExpectedNodeId`) → запись локально


