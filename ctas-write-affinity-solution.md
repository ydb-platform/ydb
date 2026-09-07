# CTAS Write Affinity: Описание решения

## 1. Введение

Оптимизация **ColumnShard Write Node Affinity** для операции CTAS (CREATE TABLE AS SELECT) маршрутизирует строки напрямую к соответствующему ColumnShard через `ColumnShardHashV1` HashShuffle.

[1] Решение касается **только CTAS**. Не-CTAS операции (INSERT/REPLACE/UPDATE/DELETE) используют отдельный путь через Table Resolver и не требуют изменений.

**Статус**: ✅ Реализовано и протестировано. Все 12 TWIN тестов (24 запуска) проходят успешно.

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

Плюс `TKikimrConfiguration` с флагом `EnableCsWriteAffinity` (раздел 2.4.1).


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

**Вход:** `Write!`-callable из Rewrite (раздел 2.1.1) + `TKikimrConfiguration` с флагом `EnableCsWriteAffinity` (раздел 2.4.1).

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

##### 2.2.1.1 IO Rewrite: Write! → TKiWriteTable

[`RewriteIO()`](ydb/core/kqp/provider/yql_kikimr_datasink.cpp:1117) преобразует `Write!` в `TKiWriteTable`:

```cpp
// yql_kikimr_datasink.cpp:1293-1301
return Build<TKiWriteTable>(ctx, node->Pos())
    .World(node->Child(0))
    .DataSink(node->Child(1))
    .Table().Build(key.GetTablePath())
    .Input(node->Child(3))
    .Mode(mode)
    .Settings(settings.Other)
    .ReturningColumns(returningColumns)
    .Done()
    .Ptr();
```

**Было:** `RewriteIO()` преобразует `Write!` в `TKiWriteTable` без `CtasShardingColumns` в settings.

**Стало (без аффинити):** `RewriteIO()` преобразует `Write!` в `TKiWriteTable` без `CtasShardingColumns` в settings (логика не изменилась).

**Стало (с аффинити):** `RewriteIO()` преобразует `Write!` в `TKiWriteTable` с `CtasShardingColumns` в settings (из Rewrite, раздел 2.1.1).

##### 2.2.1.2 Типизация TKiWriteTable

[`HandleWriteTable()`](ydb/core/kqp/provider/yql_kikimr_type_ann.cpp:931) в type annotation валидирует `TKiWriteTable`:
- Проверяет тип `World`
- Проверяет тип `DataSink` (должен быть `kikimr`)
- Для `mode="fill_table"` — проверяет `CtasShardingColumns` (если присутствует), устанавливает тип узла как тип `World` и завершает работу
- Для других режимов — проверяет тип `Input` (должен быть списком или stream структур), соответствие схеме таблицы, наличие ключевых колонок

**Было:** `HandleWriteTable()` для `mode="fill_table"` устанавливает тип узла как тип `World` и завершает работу. `CtasShardingColumns` не проверяется.

**Стало (без аффинити):** `HandleWriteTable()` для `mode="fill_table"` устанавливает тип узла как тип `World` и завершает работу. `CtasShardingColumns` не проверяется (логика не изменилась).

**Стало (с аффинити):** `HandleWriteTable()` для `mode="fill_table"` проверяет `CtasShardingColumns` (если присутствует) через `EnsureTupleOfAtoms()`, устанавливает тип узла как тип `World` и завершает работу.

##### 2.2.1.3 BuildFillTable: TKiWriteTable → TKqlFillTable

Оптимизатор вызывает `HandleWriteTable()`, который при `mode="fill_table"` вызывает [`BuildFillTable()`](ydb/core/kqp/opt/kqp_opt_kql.cpp:478):

```cpp
// kqp_opt_kql.cpp:1456-1457
if (GetTableOp(write) == TYdbOperation::FillTable) {
    return BuildFillTable(write, ctx).Ptr();
}
```

`BuildFillTable()` извлекает компоненты из `TKiWriteTable` и создаёт `TKqlFillTable` через builder:

```cpp
// kqp_opt_kql.cpp:478-494
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

##### 2.2.1.4 Типизация TKqlFillTable

[`AnnotateFillTable()`](ydb/core/kqp/opt/kqp_type_ann.cpp:833) валидирует `TKqlFillTable`:
1. `EnsureMinMaxArgsCount(*node, 4, 5, ctx)` — проверяет, что аргументов 4 или 5
2. Если 5-й аргумент (`CtasShardingColumns`) присутствует — `EnsureTupleOfAtoms()` проверяет, что это список строк
3. Устанавливает тип узла: `TListExprType` или `TStreamExprType` с `KqpEffectType`

**Было:** `AnnotateFillTable()` проверяет 4 аргумента, не проверяет `CtasShardingColumns`.

**Стало (без аффинити):** `AnnotateFillTable()` проверяет 4 аргумента, не проверяет `CtasShardingColumns` (логика не изменилась).

**Стало (с аффинити):** `AnnotateFillTable()` проверяет 4-5 аргументов, проверяет тип `CtasShardingColumns` (список строк) при наличии.

##### 2.2.1.5 Выход: TKqlFillTable

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

[`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:221) строит физический план (DQ-граф стадий) для `TKqlFillTable`. Результат — узел `TKqpSinkEffect`, который сериализуется в `TKqpPhyTx` proto (раздел 2.2.4).

**Вход:** Типизированный узел `TKqlFillTable` (раздел 2.2.1.5):
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

##### 2.2.2.1 Определение режима: `csWriteAffinity`

```cpp
// kqp_opt_effects.cpp:225
const bool csWriteAffinity = node.CtasShardingColumns().IsValid();
```

`csWriteAffinity` — индикатор, что sharding columns установлены. Определяет, какой план строить: стандартный (Map) или affinity (HashShuffle).

**Было:** `csWriteAffinity` всегда `false` (поле `CtasShardingColumns` не существовало в `TKqlFillTable`).

**Стало (без аффинити):** `csWriteAffinity` всегда `false` (поле `CtasShardingColumns` не заполняется при `EnableCsWriteAffinity=false`, раздел 2.1.1).

**Стало (с аффинити):** `csWriteAffinity` = `true` (поле `CtasShardingColumns` заполнено при `EnableCsWriteAffinity=true`).

##### 2.2.2.2 Построение `TKqpTable` и `settings`

```cpp
// kqp_opt_effects.cpp:227-239
const TKqpTable table = Build<TKqpTable>(ctx, node.Pos())
    .Path(node.Table())
    .PathId(ctx.NewAtom(node.Pos(), ""))
    .SysView(ctx.NewAtom(node.Pos(), ""))
    .Version(ctx.NewAtom(node.Pos(), ""))
    .Done();

TVector<TCoNameValueTuple> settings;
settings.emplace_back(
    Build<TCoNameValueTuple>(ctx, node.Pos())
        .Name().Build("OriginalPath")
        .Value<TCoAtom>().Build(node.OriginalPath())
        .Done());
```

Создаётся `TKqpTable` (метаданные целевой таблицы: путь, пустые PathId/SysView/Version) и `settings` (содержит `OriginalPath` — путь к исходной таблице до CTAS-декомпозиции).

**Было:** Создаётся `TKqpTable` и `settings` с `OriginalPath`.

**Стало (без аффинити):** Создаётся `TKqpTable` и `settings` с `OriginalPath` (логика не изменилась).

**Стало (с аффинити):** Создаётся `TKqpTable` и `settings` с `OriginalPath` (логика не изменилась).

##### 2.2.2.3 Определение типа Input: pure expression или DQ-union

```cpp
// kqp_opt_effects.cpp:241
if (IsDqPureExpr(node.Input())) {
    // ... путь 2.2.2.4
}
// kqp_opt_effects.cpp:281
if (!EnsureDqUnion(node.Input(), ctx)) {
    return false;
}
// ... путь 2.2.2.5
```

`node.Input()` — это либо чистое выражение (pure expression, нет входных данных из таблиц), либо `TDqCnUnionAll` (результат DQ-оптимизации SELECT с чтением из таблиц). Код проверяет `IsDqPureExpr()` первым; если `false` — ожидает `TDqCnUnionAll`.

**Было:** Определение типа Input (логика не изменилась).

**Стало (без аффинити):** Определение типа Input (логика не изменилась).

**Стало (с аффинити):** Определение типа Input (логика не изменилась).

##### 2.2.2.4 Путь A: pure expression (нет входных данных)

Если `IsDqPureExpr(node.Input())` — `true` (CTAS без чтения из таблиц, например `CREATE TABLE t AS SELECT 1 AS x`):

```cpp
// kqp_opt_effects.cpp:241-278
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

**С аффинити:** Создаётся `transformStage` (pure stage без входов, `ToFlow(input)`) и `sink` ([`BuildTableSink()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:45)), затем [`BuildCsWriteAffinitySinkStage()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:172) (раздел 2.2.3) оборачивает их в HashShuffle-схему: Transform → HashShuffle → Sink.

**Было:** Строится план: Pure Stage с inlined sink (1 задача).

**Стало (без аффинити):** Строится план: Pure Stage с inlined sink (1 задача). Логика не изменилась.

**Стало (с аффинити):** Строится план: Pure Stage → HashShuffle(`ColumnShardHashV1`) → Sink Stage (N per-shard задач).

##### 2.2.2.5 Путь B: DQ-union (есть входные данные)

Если `node.Input()` — `TDqCnUnionAll` (CTAS с чтением из таблиц, стандартный случай):

```cpp
// kqp_opt_effects.cpp:281-345
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

##### 2.2.2.6 Выход: `TKqpSinkEffect`

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

[`BuildCsWriteAffinitySinkStage()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:172) инкапсулирует паттерн Transform → HashShuffle → Sink. Вызывается из `BuildFillTableEffect()` (раздел 2.2.2) при `csWriteAffinity=true`.

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
| `HashFunc` | `"ColumnShardHashV1"` | Та же hash-функция, что использует ColumnShard (раздел 3.2) |
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

Функция возвращает `sinkStage.Ptr()` — узел `TDqStage`, который становится `Stage` в `TKqpSinkEffect` (раздел 2.2.2.6).

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

Компилятор сериализует физический план (DQ-граф из разделов 2.2.2–2.2.3) в `TKqpPhyTx` proto — единственный канал передачи данных от компилятора к исполнителю.

**Вход:** Физический план (DQ-граф стадий) из `BuildFillTableEffect()` (раздел 2.2.2) + `TKikimrConfiguration` (раздел 2.4.1).

##### 2.2.4.1 Сериализация физического плана

Компилятор обходит все стадии DQ-графа и сериализует каждую в `TKqpPhyTx.Stages[]`:

```cpp
// kqp_query_compiler.cpp:1202-1206
for (const auto& stage : tx.Stages()) {
    physicalStageByID[stage.Ref().UniqueId()] = txProto.AddStages();
    CompileStage(stage, *physicalStageByID[stage.Ref().UniqueId()], ctx, ...);
    stagesMap[stage.Ref().UniqueId()] = txProto.StagesSize() - 1;
}
// ... (hasEffectStage, hasPqSources checks)
```

Каждая стадия (`TDqStage`) сериализуется в `TKqpPhyStage`: Sources, Program, Connections (включая `TDqCnHashShuffle` → `TKqpPhyCnHashShuffle`), Sinks.

**Было:** Сериализация плана Transform → Map → Sink. Поле `EnableCsWriteAffinity` отсутствует в proto.

**Стало (без аффинити):** Сериализация плана Transform → Map → Sink (логика не изменилась). Поле `EnableCsWriteAffinity` записывается как `false`.

**Стало (с аффинити):** Сериализация плана Transform → HashShuffle(`ColumnShardHashV1`) → Sink. Поле `EnableCsWriteAffinity` записывается как `true`.

##### 2.2.4.2 Запись `EnableCsWriteAffinity` в proto

```cpp
// kqp_query_compiler.cpp:1223
txProto.SetEnableCsWriteAffinity(Config->GetEnableCsWriteAffinity());
```

Config Setting `TKikimrConfiguration::EnableCsWriteAffinity` (раздел 2.4.1) копируется в proto `TKqpPhyTx.EnableCsWriteAffinity`. Поле записывается для информационных целей, но **не используется исполнителем как gate** — исполнитель детектит affinity по наличию `ColumnShardHashV1` в DQ-графе (раздел 2.3).

Определение поля ([`kqp_physical.proto:752`](ydb/core/protos/kqp_physical.proto:752)):
```protobuf
bool EnableCsWriteAffinity = 13;
```

**Было:** Поле `EnableCsWriteAffinity` отсутствует в `TKqpPhyTx` proto.

**Стало (без аффинити):** `EnableCsWriteAffinity=false` записывается в proto. План не содержит `ColumnShardHashV1` — исполнитель не активирует affinity-путь.

**Стало (с аффинити):** `EnableCsWriteAffinity=true` записывается в proto. План содержит `ColumnShardHashV1` — исполнитель активирует affinity-путь (раздел 2.3).

##### 2.2.4.3 Выход: `TKqpPhyTx` proto

Сводная таблица ключевых данных в proto:

| Поле proto | Было | Стало (без аффинити) | Стало (с аффинити) | Источник |
|------------|------|----------------------|---------------------|----------|
| `TKqpPhyTx.EnableCsWriteAffinity` | поле отсутствует | `false` | `true` | `TKikimrConfiguration::GetEnableCsWriteAffinity()` |
| `TKqpPhyCnHashShuffle.ColumnShardHashV1` | нет HashShuffle | нет HashShuffle (`Map`) | `oneof HashKind = ColumnShardHashV1` | Оптимизатор (раздел 2.2.3) |
| `TKqpPhyCnHashShuffle.KeyColumns` | не заполняется | не заполняется | `["Col1", ...]` | `CtasShardingColumns` из Rewrite-фазы (раздел 2.1.1) |

> **Примечание:** Поля `TargetShardIds` и `ExpectedNodeId` в `TKqpTableSinkSettings` заполняются на стороне исполнителя (runtime) — см. раздел 2.3.10.

### 2.3 Runtime

**Вход:** `TKqpPhyTx` proto — физический план, сериализованный компилятором (описание полей — раздел 2.2.4).

Ключевые поля, используемые исполнителем:
- `Stages[].Inputs[].HashShuffle` — наличие `HashShuffle` с `ColumnShardHashV1` определяет, что план построен с affinity (раздел 2.2.3)
- `Stages[].Inputs[].HashShuffle.KeyColumns` — sharding columns (заполняются `FillStages()`)
- `Stages[].Sinks[].InternalSink` — настройки sink (заполняются Table Resolver'ом)

**Детекция affinity:** Исполнитель определяет, что запрос скомпилирован с аффинити, по наличию `ColumnShardHashV1`-входа в DQ-графе. Если оптимизатор построил план с `ColumnShardHashV1` (раздел 2.2.3), исполнитель активирует affinity-путь. Флаг `EnableCsWriteAffinity` в proto (раздел 2.2.4) записывается компилятором, но не используется исполнителем как gate — если запрос был скомпилирован с аффинити, он выполняется с аффинити.

KqpExecuter превращает физический план (`TKqpPhyTx`) в исполняемые задачи и маршрутизирует данные к целевым ColumnShard'ам. Для CTAS с write affinity ключевая задача — создать **N per-shard задач** (по одной на шард), пиннить каждую к ноде своего шарда и настроить HashShuffle-маршрутизацию, чтобы каждая строка попала в задачу, владеющую её шардом.

Исполнитель обрабатывает `TKqpPhyTx` proto последовательно, проходя 8 этапов (разделы 2.3.2–2.3.9). Ниже для каждого этапа описано состояние **до оптимизации** (старый код), **после оптимизации без аффинити** (план без HashShuffle) и **после оптимизации с аффинити** (план с HashShuffle).

#### 2.3.1 Обзор: Было / Стало (без аффинити) / Стало (с аффинити)

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

**Стало (без аффинити)** — логический план (3 стадии): тот же, что и "Было". `EnableCsWriteAffinity=false` → оптимизатор не строит HashShuffle, соединение Transform→Sink остаётся `Map`. Исполнитель создаёт 1 задачу в Sink Stage. Новые поля (`CsShardingColumns`, `TargetShardIds`) существуют в коде, но не заполняются (пустые).

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

#### 2.3.2 `FillStages()` — заполнение метаданных стадий

[`FillStages()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:619) обходит все стадии `TKqpPhyTx` и заполняет `TStageInfoMeta` для каждой.

**Вход:** `TKqpPhyTx` proto — физический план (раздел 2.2.4). Для каждой стадии: `Stages[].Inputs[]` (соединения), `Stages[].Sinks[]` (настройки sink), `Stages[].Sources[]` (источники чтения).

**Было:**
- Поля `CsShardingColumns` не существует в `TStageInfoMeta` — оно добавлено как часть работы CS Write Affinity
- `ShardOperations` = `{Update}` (MODE_FILL → Update)
- `TableId`, `TablePath` не заполняются для MODE_FILL (таблица ещё не существует)
- `TableConstInfo` = `nullptr` для MODE_FILL
- `ColumnTableInfoPtr` = `nullptr`
- `ShardKey` = `nullptr`
- `ResolvedSinkSettings` = `nullopt`

**Стало (без аффинити):**
- `CsShardingColumns` существует в `TStageInfoMeta`, но остаётся пустым — нет HashShuffle-входа с `kColumnShardHashV1` (оптимизатор построил `Map`, а не `HashShuffle`)
- Остальные поля — как в "Было": `ColumnTableInfoPtr=nullptr`, `ShardKey=nullptr`, `ResolvedSinkSettings=nullopt`

**Стало (с аффинити):**
- `CsShardingColumns` заполняется из `KeyColumns` proto HashShuffle-входа ([`kqp_tasks_graph.cpp:733`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:733)):
  ```cpp
  if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kHashShuffle
          && input.GetHashShuffle().GetHashKindCase() == NKqpProto::TKqpPhyCnHashShuffle::kColumnShardHashV1) {
      for (const auto& col : input.GetHashShuffle().GetKeyColumns()) {
          meta.CsShardingColumns.push_back(col);
      }
  }
  ```
- Остальные поля (`TableId`, `TableConstInfo`, `ColumnTableInfoPtr`, `ShardKey`, `ResolvedSinkSettings`) по-прежнему пусты — они заполняются позже Table Resolver'ом.

#### 2.3.3 `ResolveShards()` — разрешение шардов на ноды

**Вход:** `TasksGraph` (заполнен в разделе 2.3.2) — для каждой стадии: `ShardKey` (ключ + партиции), `PrunedPartitions` (пусто — заполняется здесь), `Sinks` (настройки sink). `TxManager` (управляет партиционированием), `PartitionPruner` (вычисляет нужные партиции для чтения). `ColumnShardHashV1` в DQ-графе (`Stages[].Inputs[]`) — наличие определяет affinity-путь.

Выполняется в два асинхронных этапа: сбор `shardIds` и разрешение через `TKqpShardsResolver`.

##### 2.3.3.1 Сбор `shardIds` — [`HandleResolve(TEvTableResolveStatus)`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:248)

**Инициация:** при получении запроса executer создаёт `TKqpTableResolver` ([`kqp_executer_impl.h:1259`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:1259)) и переходит в состояние `WaitResolveState`. `TKqpTableResolver` навигирует по схемам, получает `TableId` и `ShardKey` для каждой таблицы, затем отправляет `TEvTableResolveStatus` обратно в executer. `HandleResolve(TEvTableResolveStatus)` вызывается по этому событию — на этом моменте `ShardKey` уже заполнен.

1. **Для каждой стадии** в `TasksGraph.GetStagesInfo()`:
   - Устанавливает партиционирование в `TxManager` (`SetPartitioning`)
   - Пропускает sysview-таблицы

2. **Определяет тип стадии** и собирает `shardIds`:

   | Тип стадии | Условие | Откуда берутся shardIds |
   |-----------|---------|------------------------|
   | Read source | `stage.Sources(0).Type == kReadRangesSource` | `PartitionPruner->Prune(source, ...)` → `PrunedPartitions` → ключи `shardId` |
   | Scan / OLAP | `IsScan \|\| IsOlap()` | `PartitionPruner->Prune(op, ...)` для каждого `TableOp` → `PrunedPartitions` → ключи `shardId` |
   | **Sink (CTAS)** | `else` (нет sources, не scan) | **Только при наличии `ColumnShardHashV1`-входа**: `ShardKey->GetPartitions()` → `partition.ShardId` |

   Для sink-стадий (CTAS) код проверяет:
   - `sink.HasInternalSink()` и настройки — `TKqpTableSinkSettings`
   - `sinkSettings.GetType() == MODE_FILL`
   - `stageInfo.Meta.ShardKey` не пуст

3. **Если `shardIds` не пуст** → создаёт `TKqpShardsResolver` и переходит в состояние ожидания (`WAIT_SHARDS`).
   **Если пуст** → сразу вызывает `TasksGraph.ResolveShards({})` (пустое отображение) и продолжает.

##### 2.3.3.2 Разрешение — [`TKqpShardsResolver`](ydb/core/kqp/executer_actor/shards_resolver/kqp_shards_resolver.cpp:31)

1. **`Bootstrap()`**: для каждого `tabletId` из `ShardIds` отправляет `TEvGetTabletNode` в pipe cache (`MakePipePerNodeCacheID(useFollowers)`)
2. **`HandleResolve(TEvGetTabletNodeResult)`**: для каждого ответа:
   - `NodeId != 0` → записывает `Result[tabletId] = nodeId`
   - `NodeId == 0` → ретраит (до `MAX_RETRIES_COUNT`), затем ошибка
   - Когда `Result.size() == ShardIds.size()` → `ReplyAndDie()`
3. **`ReplyAndDie()`**: отправляет `TEvShardsResolveStatus { ShardsToNodes = Result }` обратно в executer

##### 2.3.3.3 Сохранение результата — [`HandleResolve(TEvShardsResolveStatus)`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:389)

1. Проверяет статус (ошибка → `ReplyErrorAndDie`)
2. Для каждого `nodeId` в `ShardsToNodes` → `TxManager->AddParticipantNode(nodeId)`
3. Вызывает **`TasksGraph.ResolveShards(std::move(reply.ShardsToNodes))`**

**`ResolveShards()`** — [`kqp_tasks_graph.cpp:4050`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4050):

```cpp
void TKqpTasksGraph::ResolveShards(TShardToNodeMap&& shardsToNodes) {
    GetMeta().ShardsResolved = true;
    GetMeta().ShardIdToNodeId = std::move(shardsToNodes);
    for (const auto& [shardId, nodeId] : GetMeta().ShardIdToNodeId) {
        GetMeta().ShardsOnNode[nodeId].push_back(shardId);
    }
}
```

Результат:
- `ShardIdToNodeId`: `TMap<ui64 shardId, ui64 nodeId>` — авторитетная карта размещения
- `ShardsOnNode`: `TMap<ui64 nodeId, TVector<ui64 shardId>>` — обратный индекс (используется в `InvalidateNode()` для принудительного переподключения pipes)

**Было:**
- Sink-стадии (CTAS): `else`-ветка содержит только `TODO`-комментарий, shardIds не добавляются
- `ShardIdToNodeId`: только шарды read-таблиц
- Node affinity: недостижим

**Стало (без аффинити):**
- Sink-стадии (CTAS): то же — нет `ColumnShardHashV1`-входа, условие не срабатывает
- `ShardIdToNodeId`: только шарды read-таблиц
- Node affinity: недостижим

**Стало (с аффинити):**
- Sink-стадии (CTAS): `ShardKey->GetPartitions()` → `shardIds` (условие: есть `ColumnShardHashV1`-вход)
- `ShardIdToNodeId`: шарды read-таблиц **+ шарды целевой таблицы**
- Node affinity: достижим (задача пиннится к ноде своего шарда)

#### 2.3.4 `BuildAllTasks()` — подсчёт задач (`CountTasks`)

[`BuildAllTasks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4058) вызывает `CountComputeTasks` для COMPUTE_TASKS стадий.

**Вход:** `TasksGraph` с заполненными `ShardIdToNodeId` (раздел 2.3.3) и `CsShardingColumns` (раздел 2.3.2). Для affinity-стадий: `ColumnTableInfoPtr` (если доступен) или `ShardKey` (fallback для CTAS).

**Было:**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (т.к. `ShardOperations` не пуст и есть sink)
- `CountComputeTasks` создаёт 1 задачу (стандартный путь): `partitionsCount` определяется по upstream-стадии (Map-соединение → COPY)
- Задача размещается на executer-ноде (`StageNeedsLocalPlacement`)
- `TasksType` = `COMPUTE_TASKS`
- `task.Meta.TaskParams` — пустой (нет `CsWriteAffinityShardId`)

**Стало (без аффинити):**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (то же условие)
- Нет `ColumnShardHashV1`-входа (оптимизатор построил `Map`) → `HasColumnShardHashV1Input=false` → условие affinity-блока не выполняется
- Создаётся 1 задача (стандартный путь), как в "Было"
- `task.Meta.TaskParams` — пустой

**Стало (с аффинити):**
- CTAS sink-стадия классифицируется как `COMPUTE_TASKS` (то же условие)
- Условие affinity-блока: `!isPureStage && HasColumnShardHashV1Input(stageInfo)` → true
- Создаются **per-shard задачи** ([`kqp_tasks_graph.cpp:4882`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4882)):
  ```cpp
  MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);
  for (const auto& [shardId, nodeId] : shardNodes) {
      auto& task = AddTask(stageInfo, TTask::UNKNOWN);
      task.Meta.TaskParams["CsWriteAffinityShardId"] = ToString(shardId);
      MaxTasksGraph->AddTask(task, nodeId);
  }
  ```
- Источник `shardNodes`:
  - `ColumnTableInfoPtr->Description.GetSharding()` → `GetCsShardingOrderedShardIds()` (если доступен)
  - `ShardKey->GetPartitions()` (fallback для CTAS, заполняется Table Resolver'ом)
- Каждая задача получает `CsWriteAffinityShardId` в `TaskParams`
- `stageType` = `FIXED` (количество задач = количество шардов, не зависит от upstream)
- **Инварианты** (нарушение → `YQL_ENSURE` с ошибкой):
  - Каждый shardId должен присутствовать в `ShardIdToNodeId` (заполняется `ResolveShards`)
  - Хотя бы один источник шардов должен быть доступен (`ColumnTableInfoPtr` или `ShardKey`)
- После создания per-shard задач — **early return** (стандартный путь не выполняется)

Логика выбора источника sharding columns инкапсулирована в [`GetEffectiveShardingColumns()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1384), который вызывается в `CountComputeTasks`, `BuildInternalSinks` и `BuildColumnShardHashV1ForWriteAffinity`:
```cpp
if (!stageInfo.Meta.CsShardingColumns.empty()) {
    return stageInfo.Meta.CsShardingColumns;
}
fallbackBuffer = ExtractShardingColumnsFromHashShuffle(stageInfo);
return fallbackBuffer;
```

**Почему для CTAS используется HashShuffle proto?** CTAS создаёт новую таблицу — Table Resolver не может заполнить `ColumnTableInfoPtr`. Компилятор знает sharding columns из CREATE TABLE (PARTITION BY или PRIMARY KEY) и записывает их в `KeyColumns`. `FillStages()` извлекает эти `KeyColumns` и заполняет `CsShardingColumns`, поэтому `GetEffectiveShardingColumns` находит их в `CsShardingColumns` и не требует fallback.

**CTAS fallback**: когда `ColumnTableInfoPtr == nullptr` (CTAS без PARTITION BY), `CountComputeTasks` использует `ShardKey->GetPartitions()` (заполняется Table Resolver'ом из `GetColumnShards()`). Если и `ShardKey` пуст — `YQL_ENSURE(false)` с ошибкой (оба источника недоступны — это баг). Порядок задач совпадает с порядком [`GetCsShardingOrderedShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1399) — критично для `TaskIndexByHash`.

#### 2.3.5 `PlaceTasks()` — размещение задач по нодам

[`PlaceTasks()`](ydb/core/kqp/executer_actor/max_tasks_graph.cpp:497) переупорядочивает задачи по нодам.

**Вход:** `stageInfo.Tasks` — список задач, созданных в разделе 2.3.4. `ShardIdToNodeId` — маппинг шардов на ноды (раздел 2.3.3).

**Было:**
- 1 задача → размещается на executer-ноде
- `stageInfo.Tasks` содержит 1 задачу

**Стало (без аффинити):**
- 1 задача → размещается на executer-ноде (как в "Было")
- `stageInfo.Tasks` содержит 1 задачу

**Стало (с аффинити):**
- N задач (по одной на шард) → размещаются на нодах, соответствующих шардам
- `PlaceTasks` переупорядочивает задачи по нодам → позиционный индекс больше не соответствует шарду
- Поэтому `CsWriteAffinityShardId` хранится в `TaskParams` (не зависит от порядка)
- `stageInfo.Tasks` содержит N задач в порядке нод

#### 2.3.6 `BuildComputeTasks()` — построение задач

[`BuildComputeTasks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:2910) заполняет метаданные задач.

**Вход:** `stageInfo.Tasks` — задачи, размещённые по нодам в разделе 2.3.5. `TaskParams` — параметры задач (включая `CsWriteAffinityShardId` для affinity-задач).

**Было:**
- 1 задача с пустыми `TaskParams`
- `BuildSinks` вызывается для 1 задачи

**Стало (без аффинити):**
- 1 задача с пустыми `TaskParams` (как в "Было")
- `BuildSinks` вызывается для 1 задачи

**Стало (с аффинити):**
- N задач, каждая с `CsWriteAffinityShardId` в `TaskParams`
- `BuildSinks` вызывается для каждой задачи

#### 2.3.7 `BuildSinks()` → `BuildInternalSinks()` — заполнение TargetShardIds

[`BuildInternalSinks()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3895) заполняет `TargetShardIds` в sink settings.

**Вход:** `stageInfo.Tasks` с `CsWriteAffinityShardId` в `TaskParams` (раздел 2.3.4). `effectiveShardingColumns` из [`GetEffectiveShardingColumns()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1384). `HasColumnShardHashV1Input(stageInfo)` — признак affinity. `ColumnTableInfoPtr` или `ShardKey` — источники `resolvedShardIds`.

**Было:**
- `TargetShardIds` остаётся пустым
- WriteActor обрабатывает все шарды (без фильтрации)
- 1 задача пишет все шарды

**Стало (без аффинити):**
- `TargetShardIds` остаётся пустым — условие `!effectiveShardingColumns.empty() && HasColumnShardHashV1Input` не выполняется (нет `ColumnShardHashV1`-входа)
- WriteActor обрабатывает все шарды (без фильтрации) — как в "Было"
- 1 задача пишет все шарды

**Стало (с аффинити):**
- При `!effectiveShardingColumns.empty() && HasColumnShardHashV1Input`:
  - `resolvedShardIds` заполняется из `ColumnTableInfoPtr` или `ShardKey->GetPartitions()`
  - **Multi-task path** (N > 1): каждая задача получает ровно 1 шард:
    ```cpp
    auto it = task.Meta.TaskParams.find("CsWriteAffinityShardId");
    shardId = std::stoull(it->second);
    settings.AddTargetShardIds(shardId);
    ```
  - **Single-task path** (N = 1): единственная задача получает все шарды
- WriteActor фильтрует строки по `TargetShardIds` (отбрасывает строки для чужих шардов)

При N>1 задач: `TargetShardIds = {shard_i}` для задачи i. Используется [`GetCsShardingOrderedShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1399) для корректного порядка шардов. **CTAS fallback**: когда `ColumnTableInfoPtr == nullptr`, `BuildInternalSinks` использует `ShardKey->GetPartitions()` для получения `resolvedShardIds`. Если `ShardKey` пуст — `resolvedShardIds` остаётся пустым, `TargetShardIds` не заполняется.

#### 2.3.8 `BuildKqpStageChannels()` → `BuildColumnShardHashV1ForWriteAffinity()` — построение hash routing

[`BuildKqpStageChannels()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1776) строит каналы между стадиями.
[`BuildColumnShardHashV1ForWriteAffinity()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1426) настраивает ColumnShardHashV1 routing.

**Вход:** `stageInfo` с заполненными задачами (раздел 2.3.6) и `TargetShardIds` (раздел 2.3.7). `effectiveShardingColumns` из [`GetEffectiveShardingColumns()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1384). `TaskParams["CsWriteAffinityShardId"]` — маппинг задач на шарды. `sinkSettings` (KeyColumns, Columns) — из `ResolvedSinkSettings` или raw proto.

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
  1. Получает `effectiveShardingColumns` через `GetEffectiveShardingColumns()` (из `CsShardingColumns`, заполненного `FillStages()` из HashShuffle proto)
  2. Проверяет `sinkSettings` (KeyColumns, Columns) — из `ResolvedSinkSettings` или raw proto
  3. Строит `shardToTaskIdx` из `TaskParams["CsWriteAffinityShardId"]` (включая shard ID 0)
  4. Строит `orderedShardIds` (канонический порядок `GetOrderedShardIds()`, fallback на отсортированные shard IDs из task params)
  5. Строит `taskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а
  6. Заполняет `ColumnShardHashV1Params` на Transform-стадии:
     - `SourceShardCount` = N
     - `TaskIndexByHash` = mapping
     - `SourceTableKeyColumnTypes` = типы ключевых колонок
  7. Возвращает `hashShuffleKeyColumns` (числовые индексы для широких каналов, имена для узких)
- Канал: HashShuffle → строки маршрутизируются по hash(sharding_key) → bucket → task

В case `kColumnShardHashV1` вызывается [`BuildColumnShardHashV1ForWriteAffinity`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1426). `TaskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а. Shuffle Elimination отключён для CTAS sink с `ColumnShardHashV1`-входом.

**Обработка числовых индексов**: `BuildColumnShardHashV1ForWriteAffinity` получает `effectiveShardingColumns` через [`GetEffectiveShardingColumns()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1384), который возвращает `CsShardingColumns` (заполнено `FillStages()` из HashShuffle proto для CTAS, или Table Resolver'ом для существующих таблиц). В ветке `useNumericIndices` (широкие каналы) каждая запись интерпретируется как:
1. Имя колонки → поиск в `columnNameToIndex` (из `sinkSettings.GetColumns()`)
2. Числовой индекс → проверка диапазона и использование как есть

В узкой ветке (Struct) числовые индексы конвертируются обратно в имена колонок.

#### 2.3.9 Runtime — выполнение задач и запись

**Вход:** Граф задач, полностью построенный в разделах 2.3.2–2.3.8. Для affinity: N задач с `TargetShardIds` (раздел 2.3.7) и `ColumnShardHashV1` routing (раздел 2.3.8). Каждая задача размещена на ноде своего шарда (раздел 2.3.5).

**Было:**
- 1 задача вычисляет все строки и пишет во все шарды
- WriteActor: `TColumnShardPayloadSerializer` обрабатывает все шарды
- `ShardAndFlushBatch()`: нет проверки `TargetShardIds`
- Node affinity: нет (всё на executer-ноде)

**Стало (без аффинити):**
- 1 задача вычисляет все строки и пишет во все шарды (как в "Было")
- WriteActor: `TColumnShardPayloadSerializer` обрабатывает все шарды
- `ShardAndFlushBatch()`: нет проверки `TargetShardIds` (поле пустое)
- Node affinity: нет (всё на executer-ноде)

**Стало (с аффинити):**
- N задач (по одной на шард), каждая на ноде своего шарда
- Transform-стадия: `ColumnShardHashV1` hash-функция маршрутизирует строки по bucket'ам
- Каждая sink-задача получает только строки для своего шарда (через HashShuffle routing)
- WriteActor: `TargetShardIds` содержит ровно 1 шард
- `ShardAndFlushBatch()`: `AFL_VERIFY(TargetShardIds->contains(shardId))` — каждая строка попадает в нужный шард
- Node affinity: каждая задача на ноде своего шарда → данные не пересекают ноды

Инварианты в [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp):
- [`ShardAndFlushBatch()`](ydb/core/kqp/runtime/kqp_write_table.cpp:569): `AFL_VERIFY(TargetShardIds->contains(shardId))` — каждая строка попадает в нужный шард
- Конструктор ([`kqp_write_table.cpp:494`](ydb/core/kqp/runtime/kqp_write_table.cpp:494)): `GetColumnShards()[i] == OrderedShardIds[i]`
- Деструктор ([`kqp_write_table.cpp:516`](ydb/core/kqp/runtime/kqp_write_table.cpp:516)): `ActualShardIds ⊆ TargetShardIds`

#### 2.3.10 Proto поля: `TargetShardIds`, `ExpectedNodeId`

Поля в `TKqpTableSinkSettings` ([`kqp.proto:933-937`](ydb/core/protos/kqp.proto:933)):
```protobuf
message TKqpTableSinkSettings {
    // Target shard IDs for per-shard write affinity.
    // When set, the WriteActor only writes to the specified shards.
    repeated uint64 TargetShardIds = 30;
    // Expected node ID for task scheduling affinity.
    // When set, the task will be scheduled on the specified node.
    optional uint64 ExpectedNodeId = 31;
}
```

**`TargetShardIds` (repeated uint64, field #30)**

Назначение: список shard ID, которые эта задача WriteActor должна записывать. Каждая задача владеет ровно одним шардом.

Где задаётся: [`kqp_tasks_graph.cpp:3966`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3966) в `BuildInternalSinks()`:
```cpp
settings.AddTargetShardIds(shardId);
```

Как используется:
- [`kqp_write_actor.cpp:2859`](ydb/core/kqp/runtime/kqp_write_actor.cpp:2859) — `TargetShardIdsFromSettings()` читает proto, создаёт `THashSet<ui64>`
- [`kqp_write_table.cpp:573`](ydb/core/kqp/runtime/kqp_write_table.cpp:573) — `AFL_VERIFY(TargetShardIds->contains(shardId))` проверяет, что строка адресована правильному шарду
- [`kqp_write_table.cpp:516`](ydb/core/kqp/runtime/kqp_write_table.cpp:516) — destructor проверяет `ActualShardIds ⊆ TargetShardIds`

**`ExpectedNodeId` (optional uint64, field #31)**

Назначение: node ID, на котором должна выполняться задача. Обеспечивает node affinity — задача запускается на той же ноде, что и ColumnShard.

Где задаётся: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) при построении графа задач, используя `ShardIdToNodeId` маппинг.

Как используется:
- [`kqp_planner.cpp:349`](ydb/core/kqp/executer_actor/kqp_planner.cpp:349) — задачи с `ExpectedNodeId` исключаются из общего планирования (уже назначены)
- [`kqp_data_executer.cpp:1143`](ydb/core/kqp/executer_actor/kqp_data_executer.cpp:1143) — используется для сопоставления задач с нодами при исполнении

**Было (origin/main):**
- Поля `TargetShardIds` и `ExpectedNodeId` отсутствуют в `TKqpTableSinkSettings`
- WriteActor обрабатывает все шарды без фильтрации
- Node affinity не поддерживается

**Стало (без аффинити):**
- Поля `TargetShardIds` и `ExpectedNodeId` существуют в proto, но не заполняются (пустые)
- WriteActor обрабатывает все шарды без фильтрации (как в "Было")
- Node affinity не поддерживается

**Стало (с аффинити):**
- `TargetShardIds` заполняется в `BuildInternalSinks()`: каждая задача получает ровно 1 shard ID
- `ExpectedNodeId` заполняется при построении графа задач: задача пиннится к ноде своего шарда
- WriteActor фильтрует строки по `TargetShardIds` (отбрасывает строки для чужих шардов)
- Node affinity: задача выполняется на ноде своего ColumnShard → запись локально


### 2.4 Конфигурация

#### 2.4.1 EnableCsWriteAffinity (Server Config Setting)

Оптимизация управляется флагом `EnableCsWriteAffinity`, который определяет режим работы: новый (с affinity) или старый (без affinity).

**Определение** ([`yql_kikimr_settings.h:129`](ydb/core/kqp/provider/yql_kikimr_settings.h:129)):
```cpp
NCommon::TConfSetting<bool, Static> EnableCsWriteAffinity;
```

**Регистрация** ([`yql_kikimr_settings.cpp:175`](ydb/core/kqp/provider/yql_kikimr_settings.cpp:175)):
```cpp
REGISTER_SETTING(*this, EnableCsWriteAffinity);
```

**Getter** ([`yql_kikimr_settings.cpp:401-402`](ydb/core/kqp/provider/yql_kikimr_settings.cpp:401)):
```cpp
bool TKikimrConfiguration::GetEnableCsWriteAffinity() const {
    return EnableCsWriteAffinity.Get().GetOrElse(false);
}
```

Значение задаётся через серверные KQP-настройки (`TKikimrSettings::SetKqpSettings`), а не через per-query PRAGMA:
```cpp
// В тестах:
settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(true));
```

| `EnableCsWriteAffinity` | Режим | План CTAS |
|---|---|---|
| `true` | Новый (с affinity) | Transform → HashShuffle(`ColumnShardHashV1`) → Sink (N per-shard задач) |
| `false` | Старый (без affinity) | Один stage с inlined sink (стандартный путь) |

[2] Флаг читается в двух местах:
- **Rewrite-фаза** ([`kqp_statement_rewrite.cpp:319`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:319)): `GetEnableCsWriteAffinity()` — при `true` в FILL-стейтмент добавляются `CtasShardingColumns` (раздел 2.1.1).
- **Компилятор** ([`kqp_query_compiler.cpp:1223`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1223)): значение копируется в proto `TKqpPhyTx.EnableCsWriteAffinity` (информационное поле, не используется исполнителем как gate — раздел 2.2.4).

[3] Оптимизатор ([`kqp_opt_effects.cpp:221`](ydb/core/kqp/opt/kqp_opt_effects.cpp:221)) использует `node.CtasShardingColumns().IsValid()` как индикатор affinity: sharding columns установлены только когда флаг включён.

## 3. Routing и модель шардирования

### 3.1 Схема routing'а

[59] `строка → hash(sharding_key) → bucket i → TaskIndexByHash[i] → task i → ColumnShards[i]`

### 3.2 Совместимость hash-функций

[60] DQ `TColumnShardHashV1` и ColumnShard `TXX64::Execute()` используют одинаковую реализацию:
`NXX64::TStreamStringHashCalcer(seed=0)` + `Update(raw_bytes)` per column.

[61] Bucket mapping: `min(h/(Max/N), N-1)` совпадает в обоих компонентах.

### 3.3 Модель shard assignment

[62] Per-Shard (K = N):
```
StageShards[i] = {sᵢ}            — ровно один шард на задачу
StageNode[i]   = P(sᵢ)           — нода шарда sᵢ
TargetShardIds = {sᵢ}            — один шард
TaskIndexByHash[bucket] = i       — bucket = hash(sharding_key) / (Max/N)
```

---

## 4. Гарантии корректности

[63] **Точный routing**: каждая задача получает строки только своих шардов.

[64] **Совместимость hash**: DQ routing и runtime sharding используют одинаковую hash-функцию (см. [60]).

[65] **Единство порядка**: `CountComputeTasks`, `BuildInternalSinks` и `BuildKqpStageChannels` используют один порядок `GetColumnShards()` (см. [50], [54], [55]).

[66] **AFL_VERIFY**: если строка чужого шарда попала в задачу — crash с диагностикой (см. [56]).

[67] **Destructor validation**: `ActualShardIds ⊆ TargetShardIds` (см. [58]).

[68] **Invariant порядка**: `GetColumnShards()[i] == OrderedShardIds[i]` (см. [57]).

---

## 5. Изменённые файлы

### Передача sharding columns (Путь A — оптимизация)

| Файл | Роль |
|------|------|
| [`kqp_expr_nodes.json`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json) | `CtasShardingColumns` в `TKqlFillTable` |
| [`kqp_statement_rewrite.cpp`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:313) | PartitionBy/PK → `CtasShardingColumns` |
| [`kqp_opt_kql.cpp`](ydb/core/kqp/opt/kqp_opt_kql.cpp:478) | Setting → `TKqlFillTable` |
| [`kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:216) | `CtasShardingColumns` → KeyColumns |
| [`kqp_type_ann.cpp`](ydb/core/kqp/opt/kqp_type_ann.cpp:835) | Allow 4-5 args + type check для `TKqlFillTable` |

### Write affinity (Путь B — runtime)

| Файл | Роль |
|------|------|
| [`kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:183) | Transform→HashShuffle→Sink |
| [`kqp_opt_hash_func_propagate_transformer.cpp`](ydb/core/kqp/opt/kqp_opt_hash_func_propagate_transformer.cpp:100) | Сохранение ColumnShardHashV1 |
| [`kqp_query_compiler.cpp`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1214) | EnableCsWriteAffinity |
| [`kqp.proto`](ydb/core/protos/kqp.proto:930) | TargetShardIds, ExpectedNodeId |
| [`kqp_physical.proto`](ydb/core/protos/kqp_physical.proto:751) | EnableCsWriteAffinity flag |
| [`kqp_prepared_query.h`](ydb/core/kqp/query_data/kqp_prepared_query.h:71) | EnableCsWriteAffinity() accessor |
| [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:230) | CsShardingColumns + ShardKey |
| [`kqp_executer_impl.h`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:319) | Destination shard resolution |
| [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) | Tasks, channels, sinks + helper функции |
| [`kqp_tasks_graph.h`](ydb/core/kqp/executer_actor/kqp_tasks_graph.h) | CsShardingColumns в meta |
| [`kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp) | TargetShardIds propagation |
| [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp:569) | AFL_VERIFY routing |
| [`kqp_write_table.h`](ydb/core/kqp/runtime/kqp_write_table.h:259) | TargetShardIds в settings |
| [`yql_kikimr_settings.cpp`](ydb/core/kqp/provider/yql_kikimr_settings.cpp:175) | Server setting registration |
| [`yql_kikimr_settings.h`](ydb/core/kqp/provider/yql_kikimr_settings.h:130) | Server setting declaration |

**Helper функции в `kqp_tasks_graph.cpp`** (добавлены для CTAS fallback):

| Функция | Строка | Роль |
|---------|--------|------|
| [`ExtractShardingColumnsFromHashShuffle()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1349) | 1349 | Извлекает sharding columns из proto HashShuffle |
| [`GetEffectiveShardingColumns()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1368) | 1368 | Возвращает effective sharding columns. Основной селектор: наличие `ColumnShardHashV1`-входа (есть → HashShuffle proto, нет → Table Resolver с fallback на HashShuffle) |
| [`GetCsShardingOrderedShardIds()`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1383) | 1383 | Получает ordered shard IDs из sharding description |

### Тесты

| Файл | Роль |
|------|------|
| [`kqp_write_affinity_ut.cpp`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp) | 12 TWIN тестов (24 запуска) |
| [`kqp_write_affinity_ut_README.md`](ydb/core/kqp/ut/query/kqp_write_affinity_ut_README.md) | Документация тестов |
| [`ya.make`](ydb/core/kqp/ut/query/ya.make) | Тестовый файл в билде |

---

## 6. Тестовое покрытие

Все тесты в [`kqp_write_affinity_ut.cpp`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp), suite `CS_WriteAffinity`.

**12 TWIN тестов = 24 запуска** (каждый тест запускается с `EnableCsWriteAffinity=true` и `EnableCsWriteAffinity=false`).

Настройка `EnableCsWriteAffinity` задаётся через серверные KQP-настройки (`TKikimrSettings::SetKqpSettings`), а не через per-query PRAGMA.

| # | Тест | Источник | PartitionBy | PK | Сценарий |
|---|------|----------|-------------|-----|----------|
| 1 | `CtasTableSourcePkMatchesPartitionBy` | Table (80 строк) | HASH(Col1) | (Col1) | PK = PartitionBy |
| 2 | `CtasTableSourceMultipleShardingColumns` | Table | HASH(Col1) / HASH(Col1,Col2) | (Col1) / (Col1,Col2) | 1 и 2 sharding колонки |
| 3 | `CtasTableSourceNoPartitionByUsesPrimaryKey` | Table | (none) | (Col1) | Fallback к PK |
| 4 | `CtasTableSourcePartitionBySubsetOfPrimaryKey` | Table | HASH(Col2) | (Col1,Col2) | PartitionBy ⊂ PK |
| 5 | `CtasGeneratedDataWithPartitionBy` | AS_TABLE($data), 100 строк | HASH(Col1) | (Col1) | Generated data + explicit PartitionBy |
| 6 | `CtasPureLiteralWithPartitionBy` | Pure literal (1u, 42) | HASH(Col1) | (Col1) | Чистое литеральное выражение |
| 7 | `CtasGeneratedDataWithoutPartitionBy` | AS_TABLE($data), 80 строк | (none) | (Col1) | Generated data + PK fallback |
| 8 | `CtasGeneratedDataPartitionBySubsetOfPrimaryKey` | AS_TABLE($data) | HASH(Col2) | (Col1,Col2) | Generated + PartitionBy ⊂ PK |
| 9 | `CtasTableSourceSelectWithAliases` | Table | HASH(A) | (A) | Алиасы колонок в SELECT |
| 10 | `CtasTableSourceWithWhereFilter` | Table | HASH(Col1) | (Col1) | WHERE Col1 > 40 |
| 11 | `CtasTableSourceVerifyAffinityFlagTogglesHashShuffle` | Table | HASH(Col1) | (Col1) | Проверка переключения HashShuffle |
| 12 | `CtasPureLiteralVerifyAffinityFlagTogglesHashShuffle` | Pure literal | HASH(Col1) | (Col1) | Проверка переключения HashShuffle (литерал) |

**Helper функции**:
- `ExplainAndExecuteQuery()` — выполняет EXPLAIN + Execute для одного запроса, возвращая план для верификации
- `VerifyCtasPlanFull()` — проверяет количество стадий, структуру плана и KeyColumns
- `BuildKqpSettingsWithCsWriteAffinity()` — создаёт KQP-настройки с включённым/выключенным affinity

**Запуск тестов**:
```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/query/ -F 'CS_WriteAffinity'
```

---

## 7. Итоги: исправление проблем

### 7.1 Проблема 1: CTAS без PARTITION BY

Тест `CS_WriteAffinity::CtasGeneratedDataWithoutPartitionBy+EnableCsWriteAffinity` падал с ошибкой:
```
AFL_VERIFY failed: TargetShardIds->contains(shardId)
```

**Корневая причина**: `BuildColumnShardHashV1ForWriteAffinity` не обрабатывала числовые индексы (например, `"0"`) в `KeyColumns` proto HashShuffle. Для CTAS без PARTITION BY оптимизатор генерирует HashShuffle с числовыми индексами вместо имён колонок, что приводило к `nullopt` и последующему `Y_ENSURE(false)`.

**Решение**:
1. **Добавлены helper функции** в [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp):
   - `ExtractShardingColumnsFromHashShuffle()` — извлекает sharding columns из proto HashShuffle
   - `GetEffectiveShardingColumns()` — возвращает effective sharding columns. **Основной селектор: наличие `ColumnShardHashV1`-входа**:
     - Есть `ColumnShardHashV1`-вход (CTAS с affinity) → всегда HashShuffle proto (компилятор знает sharding columns)
     - Нет `ColumnShardHashV1`-входа (не-CTAS или CTAS без affinity) → Table Resolver с fallback на HashShuffle
   - `GetCsShardingOrderedShardIds()` — получение ordered shard IDs

2. **Обработка числовых индексов** в `BuildColumnShardHashV1ForWriteAffinity`:
   - Ветка `useNumericIndices`: каждая запись интерпретируется как имя колонки ИЛИ числовой индекс
   - Узкая ветка (Struct): числовые индексы конвертируются обратно в имена колонок

3. **CTAS fallback в `CountComputeTasks`**:
   - Когда `ColumnTableInfoPtr == nullptr`, используется `ShardKey->GetPartitions()`
   - Если `ShardKey` пуст — `YQL_ENSURE(false)` с ошибкой (оба источника недоступны — это баг)

4. **CTAS fallback в `BuildInternalSinks`**:
   - Когда `ColumnTableInfoPtr == nullptr`, используется `ShardKey->GetPartitions()` для `resolvedShardIds`

### 7.2 Проблема 2: `isOlap=false` в `BuildInternalSinks` и `CountComputeTasks`

После исправления проблемы 1 тесты продолжали падать с ошибкой:
```
ColumnShardHashV1 write affinity: params couldn't be built for stage 1
```

**Корневая причина**: `BuildInternalSinks` и `CountComputeTasks` использовали `settings.GetIsOlap()` для определения, является ли sink OLAP. Для CTAS целевая таблица не существует на момент работы Table Resolver'а, поэтому `entry.Kind != KindColumnTable` и `IsOlap` устанавливается в `false`. Это приводило к тому, что affinity-путь пропускался, `TargetShardIds` оставался пустым, и `BuildColumnShardHashV1ForWriteAffinity` возвращал `nullopt` (пустой `shardToTaskIdx`).

**Решение**: Affinity определяется по наличию `ColumnShardHashV1`-входа в DQ-графе ([`HasColumnShardHashV1Input`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1342)), а не по флагу `EnableCsWriteAffinity`:
- [`BuildInternalSinks`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3921): условие `!effectiveShardingColumns.empty() && HasColumnShardHashV1Input(stageInfo)`
- [`CountComputeTasks`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4854): условие `!isPureStage && HasColumnShardHashV1Input(stageInfo)`

Если запрос был скомпилирован с аффинити (оптимизатор построил `ColumnShardHashV1` HashShuffle), он выполняется с аффинити. Для не-CTAS операций (INSERT/REPLACE) `ColumnShardHashV1`-входа нет, поэтому старое поведение сохраняется.

### 7.3 Верификация

Все 12 TWIN тестов (24 запуска) проходят успешно:
- Тесты с PARTITION BY — используют `ColumnTableInfoPtr` (основной путь)
- Тесты без PARTITION BY — используют `ShardKey->GetPartitions()` (fallback)
- Тесты с PK fallback — работают через `CtasShardingColumns` из Rewrite-фазы
