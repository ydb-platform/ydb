# CTAS Write Affinity: Описание решения

## 1. Введение

Оптимизация **ColumnShard Write Node Affinity** для операции CTAS (CREATE TABLE AS SELECT) маршрутизирует строки напрямую к соответствующему ColumnShard через `ColumnShardHashV1` HashShuffle.

[1] Решение касается **только CTAS**. Не-CTAS операции (INSERT/REPLACE/UPDATE/DELETE) используют отдельный путь через Table Resolver и не требуют изменений.

---

## 2. EnableCsWriteAffinity (Server Config Setting)

Оптимизация управляется флагом `EnableCsWriteAffinity`, который определяет режим работы: новый (с affinity) или старый (без affinity).

### 2.1 Определение и регистрация

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

### 2.2 Как задаётся значение

Значение задаётся через серверные KQP-настройки (`TKikimrSettings::SetKqpSettings`), а не через per-query PRAGMA:
```cpp
// В тестах:
settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(true));
```

### 2.3 Влияние флага на режим работы

| `EnableCsWriteAffinity` | Режим | План CTAS |
|---|---|---|
| `true` | Новый (с affinity) | Transform → HashShuffle(`ColumnShardHashV1`) → Sink (N per-shard задач) |
| `false` | Старый (без affinity) | Один stage с inlined sink (стандартный путь) |

[2] Флаг читается в двух местах:
- **Rewrite-фаза** ([`kqp_statement_rewrite.cpp:319`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:319)): `GetEnableCsWriteAffinity()` — при `true` в FILL-стейтмент добавляются `CtasShardingColumns` (раздел 5.2).
- **Компилятор** ([`kqp_query_compiler.cpp:1215`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1215)): значение копируется в proto `TKqpPhyTx.EnableCsWriteAffinity` для передачи на сторону executer'а (раздел 6.1).

[3] Оптимизатор ([`kqp_opt_effects.cpp:221`](ydb/core/kqp/opt/kqp_opt_effects.cpp:221)) использует `node.CtasShardingColumns().IsValid()` как индикатор affinity: sharding columns установлены только когда флаг включён.

### 2.4 Поток данных от настройки до исполнителя (обзор)

Детальное описание каждой стадии — в разделах 5 и 6.

```
TKikimrSettings::SetKqpSettings(EnableCsWriteAffinity=true)
        │
        ▼
TKikimrConfiguration::EnableCsWriteAffinity  (Config Setting)
        │
        ├──► RewriteCreateTableAs()  (kqp_statement_rewrite.cpp:319)
        │       │  GetEnableCsWriteAffinity() → true
        │       └──► CtasShardingColumns → TKqlFillTable
        │
        ├──► kqp_opt_effects.cpp:221  (Оптимизатор: node.CtasShardingColumns().IsValid())
        │
        └──► kqp_query_compiler.cpp:1215  (Компилятор копирует в Proto)
                │
                ▼
        TKqpPhyTx.EnableCsWriteAffinity  (Proto Field)
                │
                ├──► kqp_executer_impl.h:324  (Executer: Shard Resolution)
                ├──► kqp_tasks_graph.cpp:4801  (Executer: CountComputeTasks)
                └──► kqp_tasks_graph.cpp:3787  (Executer: BuildInternalSinks)
```

---

## 3. Архитектура: Было / Стало

### 3.1 Было (без оптимизации)

```
Upstream ComputeActor
    ↓ TDqCnMap (1:1, COPY)
TKqpDirectWriteActor (1 задача, executer-нода)
    └── TShardedWriteController
            ├── Hash(PK) → CS[0]  (сеть)
            ├── Hash(PK) → CS[1]  (сеть)
            └── Hash(PK) → CS[N]  (сеть)
```

[4] Один WriteActor на executer-ноде маршрутизирует все строки по сети.
[5] Нет node affinity — WriteActor может быть на любой ноде.
[6] Все per-shard буферы сосредоточены в одном месте.

### 3.2 Стало (с оптимизацией)

```
Upstream ComputeActor
    ↓ TDqCnMap (1:1)
Transform Stage (1 задача, любая нода)
    ↓ TDqCnHashShuffle с ColumnShardHashV1 (hash(sharding_key) → task i)
Sink Stage (N задач, по одной на шард)
    WriteActor[0] на Node(CS[0]) → CS[0]  (local)
    WriteActor[1] на Node(CS[1]) → CS[1]  (local)
    ...
    WriteActor[N] на Node(CS[N]) → CS[N]  (local)
```

[7] Transform Stage преобразует данные, Sink Stage записывает.
[8] HashShuffle с `ColumnShardHashV1` маршрутизирует строки к задачам по хэшу ключа шардирования.
[9] Каждая задача пиннена к ноде своего ColumnShard → запись локальная.
[10] N задач = N шардов (Per-Shard модель).

---

## 4. CTAS: декомпозиция на 3 стейтмента

[11] CTAS декомпозируется на три стейтмента в [`RewriteCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:176):
1. **CREATE TABLE** — создаёт temp-таблицу `/.tmp/sessions/.../Destination_uuid`
2. **FILL** — записывает данные в temp-таблицу (именно здесь применяется write affinity)
3. **MOVE** — атомарно переименовывает temp-таблицу в финальное имя

[12] Оптимизация write affinity применяется к **FILL** стейтменту. CREATE TABLE и MOVE не требуют изменений.

[13] FILL стейтмент создаётся как `Write!`-callable с settings:
- `["mode", "fill_table"]` — режим записи
- `["OriginalPath", tableName]` — оригинальный путь таблицы
- `["AllowInconsistentWrites"]` — разрешение неконсистентных записей
- `["CtasShardingColumns", [Col1, ...]]` — ключ шардирования для CTAS (добавлено)

---

## 5. Полный поток CtasShardingColumns

### 5.1 Обзор

`CtasShardingColumns` — список имён колонок, определяющих ключ шардирования для CTAS в ColumnShard таблицы. Данные проходят 4 стадии:

```
SQL PARSE → REWRITE → TYPE ANNOTATION → OPTIMIZER (Build TKqlFillTable) → EFFECTS (Build Plan)
    │            │              │                         │                      │
  PARTITION  insertSettings   4-5 args check      TKqlFillTable node       KeyColumns для
   BY / PK    в Write!        (4 required +        с CtasSharding           HashShuffle
            FILL settings      1 optional)          Columns atom list
```

### 5.2 Стадия 1: Rewrite — извлечение sharding columns из CREATE TABLE

[`RewriteCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:313) получает `TWriteTableSettings settings` с полями:
- `settings.PartitionBy` — колонки из `PARTITION BY HASH(...)`
- `primariKeyColumns` — колонки из `PRIMARY KEY (...)`
- `settings.TableSettings` — настройки таблицы (включая `storeType = column`)

[14] Для ColumnShard таблиц ([`IsOlapCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:96) проверяет `storeType = column`) извлекаются sharding columns с приоритетом:
1. `PartitionBy` columns (явный ключ шардирования)
2. PRIMARY KEY columns (fallback)
3. Ошибка, если оба отсутствуют

[15] Sharding columns добавляются в `insertSettings`:
```cpp
// kqp_statement_rewrite.cpp:313-336
if (IsOlapCreateTableAs(root, exprCtx)) {
    NYql::TExprNode::TListType keyColumnsList;
    if (settings.PartitionBy.IsValid()) {
        for (const auto& col : settings.PartitionBy.Cast()) {
            keyColumnsList.push_back(exprCtx.NewAtom(pos, col.Value()));
        }
    } else if (!primariKeyColumns.empty()) {
        for (const auto& col : primariKeyColumns) {
            keyColumnsList.push_back(exprCtx.NewAtom(pos, TString(col)));
        }
    }
    if (!keyColumnsList.empty()) {
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "CtasShardingColumns"),
                exprCtx.NewList(pos, std::move(keyColumnsList)),
            }));
    } else {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(pos),
            "CTAS to ColumnShard table requires either PARTITION BY or PRIMARY KEY for sharding column resolution"));
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

### 5.3 Стадия 2: Типизация — проверка аргументов TKqlFillTable

[17] [`AnnotateFillTable()`](ydb/core/kqp/opt/kqp_type_ann.cpp:835): `EnsureMinMaxArgsCount(*node, 4, 5, ctx)` — позволяет 4-5 аргументов у `TKqlFillTable` (4 required + 1 optional `CtasShardingColumns`).

[18] Когда 5-й аргумент присутствует, проверяется, что это список имён колонок:
```cpp
// kqp_type_ann.cpp:863-869
if (node->ChildrenSize() > TKqlFillTable::idx_CtasShardingColumns) {
    if (!EnsureTupleOfAtoms(*node->Child(TKqlFillTable::idx_CtasShardingColumns), ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
            "CtasShardingColumns must be a list of column names"));
        return TStatus::Error;
    }
}
```

[19] Поле в схеме [`kqp_expr_nodes.json`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:300):
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

[20] `TCoAtomList` — список атомов (строк), каждый атом содержит имя колонки.

[21] Поле опционально (`"Optional": true`), так как для не-CTAS случаев (INSERT/REPLACE) sharding columns не нужны — они берутся из метаданных таблицы.

### 5.4 Стадия 3: Оптимизатор — построение TKqlFillTable из Write! callable

[22] [`BuildFillTable()`](ydb/core/kqp/opt/kqp_opt_kql.cpp:478) извлекает `CtasShardingColumns` из FILL settings и передаёт в builder `TKqlFillTable`.

#### 5.4.1 Полный поток: от Write! callable к TKqlFillTable

Данные `CtasShardingColumns` проходят через несколько промежуточных представлений:

```
Rewrite (kqp_statement_rewrite.cpp)
    │
    ▼
Write!-callable (AST)
    │  arg[4] = insertSettings
    │  insertSettings = [
    │      ["mode", "fill_table"],
    │      ["OriginalPath", tableName],
    │      ["AllowInconsistentWrites"],
    │      ["CtasShardingColumns", [Col1, Col2, ...]]   ← добавлено на стадии Rewrite
    │  ]
    │
    ▼
TKiWriteTable (после типизации)
    │  Settings() → TCoNameValueTupleList
    │  Settings().Ref() → TExprNode (список пар [Name, Value])
    │
    ▼
BuildFillTable() (kqp_opt_kql.cpp:478)
    │  GetSetting() → извлекает ["CtasShardingColumns", [...]]
    │  TCoNameValueTuple → разворачивает пару (Name, Value)
    │  Cast<TCoAtomList>() → получает список атомов
    │
    ▼
TKqlFillTable (builder pattern)
    │  .Input(...)
    │  .Table(...)
    │  .Cluster(...)
    │  .OriginalPath(...)
    │  .CtasShardingColumns(ctasShardingColumns)  ← TMaybeNode<TCoAtomList>
    │
    ▼
TKqlFillTable node (готовый AST-узел)
    │  Child[0] = Input (TExprBase)
    │  Child[1] = Table (TCoAtom)
    │  Child[2] = Cluster (TCoAtom)
    │  Child[3] = OriginalPath (TCoAtom)
    │  Child[4] = CtasShardingColumns (TCoAtomList)  ← опционально
```

#### 5.4.2 Функция GetSetting()

[23] [`GetSetting()`](yql/essentials/core/yql_opt_utils.cpp:674) — утилита для поиска элемента в settings-списке:

```cpp
// yql_opt_utils.cpp:674-681
TExprNode::TPtr GetSetting(const TExprNode& settings, const TStringBuf& name) {
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && setting->Child(0)->Content() == name) {
            return setting;
        }
    }
    return nullptr;
}
```

**Алгоритм**:
1. Итерирует по всем дочерним узлам `settings` (каждый — пара `[Name, Value]`).
2. Проверяет, что у узла есть хотя бы один дочерний элемент.
3. Сравнивает `Content()` первого дочернего элемента (имя настройки) с искомым `name`.
4. Возвращает найденный узел или `nullptr`.

**Структура settings-списка** (после Rewrite):
```
TExprNode (List)
├── Child[0]: TExprNode (List)
│   ├── Child[0]: TCoAtom("mode")
│   └── Child[1]: TCoAtom("fill_table")
├── Child[1]: TExprNode (List)
│   ├── Child[0]: TCoAtom("OriginalPath")
│   └── Child[1]: TCoAtom("/tmp/sessions/.../Destination_uuid")
├── Child[2]: TExprNode (List)
│   └── Child[0]: TCoAtom("AllowInconsistentWrites")
└── Child[3]: TExprNode (List)
    ├── Child[0]: TCoAtom("CtasShardingColumns")
    └── Child[1]: TExprNode (List)          ← TCoAtomList
        ├── Child[0]: TCoAtom("Col1")
        └── Child[1]: TCoAtom("Col2")
```

`GetSetting(settings, "CtasShardingColumns")` возвращает `Child[3]` — узел списка `["CtasShardingColumns", [Col1, Col2]]`.

#### 5.4.3 Разбор TCoNameValueTuple

[24] `TCoNameValueTuple` — обёртка над парой `[Name, Value]` в settings-списке:

```cpp
// kqp_opt_kql.cpp:488-492
if (ctasShardingColumnsNode) {
    const auto ctasShardingColumns = TCoNameValueTuple(ctasShardingColumnsNode)
        .Value()                          // Извлекаем Value (Child[1])
        .Cast<TCoAtomList>();             // Приводим к TCoAtomList
    if (ctasShardingColumns.Ref().ChildrenSize() > 0) {
        builder.CtasShardingColumns(ctasShardingColumns);
    }
}
```

**Цепочка преобразований**:
1. `ctasShardingColumnsNode` — `TExprNode::TPtr` на узел `["CtasShardingColumns", [Col1, Col2]]`
2. `TCoNameValueTuple(ctasShardingColumnsNode)` — обёртка, предоставляющая `.Name()` и `.Value()`
3. `.Value()` — `TExprNode::TPtr` на `[Col1, Col2]` (Child[1] пары)
4. `.Cast<TCoAtomList>()` — приводит к `TCoAtomList` (список атомов-строк)
5. `ctasShardingColumns.Ref().ChildrenSize() > 0` — проверка, что список не пустой

#### 5.4.4 Builder pattern для TKqlFillTable

[25] `Build<TKqlFillTable>()` — fluent builder, генерируемый из [`kqp_expr_nodes.json`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:291):

```cpp
// kqp_opt_kql.cpp:483-494
auto builder = Build<TKqlFillTable>(ctx, write.Pos())
    .Input(write.Input())                    // Child[0]: TExprBase — входные данные
    .Table(write.Table())                    // Child[1]: TCoAtom — имя таблицы
    .Cluster(write.DataSink().Cluster())     // Child[2]: TCoAtom — кластер
    .OriginalPath(TCoNameValueTuple(originalPathNode).Value().Cast<TCoAtom>());
//          Child[3]: TCoAtom — оригинальный путь таблицы
if (ctasShardingColumnsNode) {
    const auto ctasShardingColumns = TCoNameValueTuple(ctasShardingColumnsNode).Value().Cast<TCoAtomList>();
    if (ctasShardingColumns.Ref().ChildrenSize() > 0) {
        builder.CtasShardingColumns(ctasShardingColumns);  // Child[4]: TCoAtomList (опционально)
    }
}
return builder.Done();
```

**Почему опционально**: поле `"Optional": true` в JSON-схеме означает, что builder-метод `.CtasShardingColumns()` можно не вызывать. В этом случае `CtasShardingColumns()` вернёт пустой `TMaybeNode<TCoAtomList>` (`.IsValid() == false`).

**Итоговая структура TKqlFillTable**:
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

#### 5.4.5 Типы данных

| Тип | Роль | Пример |
|-----|------|--------|
| `TExprNode::TPtr` | Raw AST-узел | Узел `["CtasShardingColumns", [...]]` |
| `TCoNameValueTuple` | Обёртка над парой `[Name, Value]` | `.Name()` → "CtasShardingColumns", `.Value()` → `[Col1, Col2]` |
| `TCoAtomList` | Список атомов (строк) | `[Col1, Col2]` |
| `TCoAtom` | Атом (строка) | `"Col1"` |
| `TMaybeNode<TCoAtomList>` | Опциональный узел | `.IsValid()` → true/false, `.Cast()` → `TCoAtomList` |

[26] `TMaybeNode<T>` — обёртка для опциональных полей:
- `.IsValid()` — проверяет, установлено ли поле
- `.Cast()` — извлекает внутренний тип `T` (при `.IsValid() == true`)
- Не поддерживает range-based for напрямую — нужно сначала `.Cast()`

[27] `TCoAtomList` — типизированный список `TCoAtom` узлов. Каждый атом содержит строковое значение (имя колонки). Не поддерживает конструирование по умолчанию — нельзя создать пустой `TCoAtomList` без родительского узла.

### 5.5 Стадия 4: Построение эффектов — использование CtasShardingColumns в плане

[28] [`BuildFillTableEffect()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:232) проверяет `node.CtasShardingColumns().IsValid()` как индикатор affinity. Когда sharding columns установлены — создаётся два stage, соединённых HashShuffle.

[29] **Путь с входными данными** (path with input data):
```cpp
// kqp_opt_effects.cpp
effect = Build<TKqpSinkEffect>(ctx, node.Pos())
    .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), node.CtasShardingColumns(), sink.Ptr()))
    .SinkIndex().Build("0")
    .Done();
```

[30] **Путь без входных данных** (pure expression):
```cpp
// kqp_opt_effects.cpp
effect = Build<TKqpSinkEffect>(ctx, node.Pos())
    .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), node.CtasShardingColumns(), sink.Ptr()))
    .SinkIndex().Build("0")
    .Done();
```

[31] Оба пути передают `node.CtasShardingColumns()` напрямую в `BuildCsWriteAffinitySinkStage()`, которая строит `keyColumnAtoms` из `TMaybeNode<TCoAtomList>` внутри себя.

### 5.6 Helper: BuildCsWriteAffinitySinkStage

[32] [`BuildCsWriteAffinitySinkStage()`](ydb/core/kqp/opt/kqp_opt_effects.cpp:183) инкапсулирует паттерн Transform→HashShuffle→Sink:

```cpp
static TExprNode::TPtr BuildCsWriteAffinitySinkStage(
    TExprContext& ctx,                      // Контекст для builders
    TPositionHandle pos,                    // Позиция для ошибок
    TExprNode::TPtr transformStage,         // Уже построенный Transform Stage
    const TMaybeNode<TCoAtomList>& ctasShardingColumns, // CtasShardingColumns из TKqlFillTable
    TExprNode::TPtr sinkNode)              // TDqSink (запись в таблицу)
```

**Шаг 0: Построение keyColumnAtoms из CtasShardingColumns** (строки 190-195)
```cpp
TVector<TCoAtom> keyColumnAtoms;
if (ctasShardingColumns.IsValid()) {
    for (const auto& col : ctasShardingColumns.Cast()) {
        keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, pos).Value(col.Value()).Done());
    }
}
```
- `ctasShardingColumns.IsValid()` — проверяет, что sharding columns установлены
- `.Cast()` — извлекает `TCoAtomList` из `TMaybeNode<TCoAtomList>`
- Каждый элемент списка — атом с именем колонки, копируется в `keyColumnAtoms`

**Шаг 1: TDqCnHashShuffle** (строки 197-207)
```cpp
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
- `Output` связывает HashShuffle с Transform Stage (источник строк)
- `KeyColumns` — колонки по которым считается хэш (из `CtasShardingColumns`)
- `HashFunc = "ColumnShardHashV1"` — та же функция, что использует ColumnShard для распределения данных
- `UseSpilling = false` — переполнение в диск отключено для CTAS

**Шаг 2: Аргумент программы Sink Stage** (строки 209-211)
```cpp
const auto sinkRowArgument = Build<TCoArgument>(ctx, pos)
    .Name("sinkRow")
    .Done();
```
- Создаёт аргумент `sinkRow` — входная переменная программы Sink Stage

**Шаг 3: TDqStage (Sink Stage)** (строки 213-229)
```cpp
auto sinkStage = Build<TDqStage>(ctx, pos)
    .Inputs()
        .Add(sinkInput)            // Вход — HashShuffle (маршрутизирует строки)
        .Build()
    .Program()
        .Args({sinkRowArgument})   // Аргументы программы
        .Body<TCoToFlow>()
            .Input(sinkRowArgument) // ToFlow(sinkRow) — преобразует строку в поток
            .Build()
        .Build()
    .Outputs<TDqStageOutputsList>()
        .Add(sinkNode)             // Выход — TDqSink (запись в ColumnShard)
        .Build()
    .Settings().Build()
    .Done();
```
- `Inputs` — HashShuffle, который маршрутизирует строки от Transform Stage
- `Program` — `ToFlow(sinkRow)`, идентичный Transform Stage (просто пропускает данные)
- `Outputs` — `sinkNode` (TDqSink), который записывает данные в таблицу

**Итоговая топология**:
```
Transform Stage (1 задача)
    │
    ▼
TDqCnHashShuffle(ColumnShardHashV1, KeyColumns=CtasShardingColumns)
    │ hash(sharding_key) → task i
    ▼
Sink Stage (N задач, по одной на шард)
    │ ToFlow(sinkRow)
    ▼
TDqSink → TKqpDirectWriteActor → ColumnShard[i] (локально)
```

[33] `PropogateHashFuncToHashShuffles` сохраняет `ColumnShardHashV1`, не перезаписывая на `HashV2`.

---

## 6. Компиляция и исполнение

### 6.1 EnableCsWriteAffinity (Proto Field)

[34] Config Setting `TKikimrConfiguration::EnableCsWriteAffinity` описан в разделе 2. На стороне компилятора его значение копируется в proto `TKqpPhyTx.EnableCsWriteAffinity` для передачи на сторону executer'а (runtime), где Config недоступен.

[35] Определение ([`kqp_physical.proto:751`](ydb/core/protos/kqp_physical.proto:751)):
```protobuf
bool EnableCsWriteAffinity = 12;
```

[36] Заполняется компилятором ([`kqp_query_compiler.cpp:1215`](ydb/core/kqp/query_compiler/kqp_query_compiler.cpp:1215)):
```cpp
txProto.SetEnableCsWriteAffinity(Config->GetEnableCsWriteAffinity());
```

[37] Getter ([`kqp_prepared_query.h:71-73`](ydb/core/kqp/query_data/kqp_prepared_query.h:71)):
```cpp
bool EnableCsWriteAffinity() const {
    return Proto->GetEnableCsWriteAffinity();
}
```

[38] Используется на стороне executer'а (runtime), так как Config недоступен:
- **Shard Resolution** ([`kqp_executer_impl.h:324`](ydb/core/kqp/executer_actor/kqp_executer_impl.h:324)): добавить шарды CTAS-таблицы в `ShardIdToNodeId`
- **CountComputeTasks** ([`kqp_tasks_graph.cpp:4801`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4801)): создать N задач (по одной на шард)
- **BuildInternalSinks** ([`kqp_tasks_graph.cpp:3787`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3787)): назначить `TargetShardIds` каждой задаче

### 6.2 TKqpTableSinkSettings: поля для write affinity

[39] Proto поля в `TKqpTableSinkSettings` ([`kqp.proto:930-935`](ydb/core/protos/kqp.proto:930)):
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

[40] Назначение: список shard ID, которые эта задача WriteActor должна записывать. Каждая задача владеет ровно одним шардом.

[41] Где задаётся: [`kqp_tasks_graph.cpp:3826`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3826) в `BuildInternalSinks()`:
```cpp
settings.AddTargetShardIds(shardId);
```

[42] Как используется:
- [`kqp_write_actor.cpp:2859`](ydb/core/kqp/runtime/kqp_write_actor.cpp:2859) — `TargetShardIdsFromSettings()` читает proto, создаёт `THashSet<ui64>`
- [`kqp_write_table.cpp:573`](ydb/core/kqp/runtime/kqp_write_table.cpp:573) — `AFL_VERIFY(TargetShardIds->contains(shardId))` проверяет, что строка адресована правильному шарду
- [`kqp_write_table.cpp:516`](ydb/core/kqp/runtime/kqp_write_table.cpp:516) — destructor проверяет `ActualShardIds ⊆ TargetShardIds`

**`ExpectedNodeId` (optional uint64, field #31)**

[43] Назначение: node ID, на котором должна выполняться задача. Обеспечивает node affinity — задача запускается на той же ноде, что и ColumnShard.

[44] Где задаётся: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) при построении графа задач, используя `ShardIdToNodeId` маппинг.

[45] Как используется:
- [`kqp_planner.cpp:349`](ydb/core/kqp/executer_actor/kqp_planner.cpp:349) — задачи с `ExpectedNodeId` исключаются из общего планирования (уже назначены)
- [`kqp_data_executer.cpp:1105`](ydb/core/kqp/executer_actor/kqp_data_executer.cpp:1105) — используется для сопоставления задач с нодами при исполнении

### 6.3 Исполнение (KqpExecuter)

#### 6.3.1 Table Resolver

[46] В `HandleResolveKeys` ([`kqp_table_resolver.cpp:230`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:230)) для OLAP sink:
- Заполняется `stageMeta.CsShardingColumns` из `ColumnTableInfo.GetColumnShards()`
- Заполняется `stageMeta.ShardKey->Partitioning` в порядке `GetColumnShards()`

#### 6.3.2 Shard Resolution

[47] Для FILL-sink стадий с `EnableCsWriteAffinity` шарды temp-таблицы добавляются в `shardIds`.

[48] Это обеспечивает попадание destination-шардов в `ShardIdToNodeId` → node affinity достижим.

#### 6.3.3 CountComputeTasks

[49] При OLAP sink + `EnableCsWriteAffinity` создаются per-shard задачи из `ColumnTableInfoPtr->GetColumnShards()`.

[50] Порядок задач совпадает с порядком `GetColumnShards()` — критично для `TaskIndexByHash`.

#### 6.3.4 BuildKqpStageChannels

[51] В case `kColumnShardHashV1` вызывается `BuildColumnShardHashV1ForWriteAffinity` ([`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)).

[52] `TaskIndexByHash[bucket]` = индекс задачи, владеющей шардом bucket'а.

[53] Shuffle Elimination отключён для CTAS sink с `EnableCsWriteAffinity=true`.

#### 6.3.5 BuildInternalSinks

[54] При `IsOlap` + `EnableCsWriteAffinity` + N>1 задач: `TargetShardIds = {shard_i}` задаче i.

[55] Используется `GetCsShardingOrderedShardIds()` для корректного порядка шардов.

#### 6.3.6 WriteActor Validation

[56] В `ShardAndFlushBatch()` ([`kqp_write_table.cpp:569`](ydb/core/kqp/runtime/kqp_write_table.cpp:569)):
```cpp
AFL_VERIFY(TargetShardIds->contains(shardId))
```

[57] Invariant в конструкторе ([`kqp_write_table.cpp:489`](ydb/core/kqp/runtime/kqp_write_table.cpp:489)):
`GetColumnShards()[i] == OrderedShardIds[i]`

[58] Destructor validation ([`kqp_write_table.cpp:555`](ydb/core/kqp/runtime/kqp_write_table.cpp:555)):
`ActualShardIds ⊆ TargetShardIds`

---

## 7. Routing и модель шардирования

### 7.1 Схема routing'а

[59] `строка → hash(sharding_key) → bucket i → TaskIndexByHash[i] → task i → ColumnShards[i]`

### 7.2 Совместимость hash-функций

[60] DQ `TColumnShardHashV1` и ColumnShard `TXX64::Execute()` используют одинаковую реализацию:
`NXX64::TStreamStringHashCalcer(seed=0)` + `Update(raw_bytes)` per column.

[61] Bucket mapping: `min(h/(Max/N), N-1)` совпадает в обоих компонентах.

### 7.3 Модель shard assignment

[62] Per-Shard (K = N):
```
StageShards[i] = {sᵢ}            — ровно один шард на задачу
StageNode[i]   = P(sᵢ)           — нода шарда sᵢ
TargetShardIds = {sᵢ}            — один шард
TaskIndexByHash[bucket] = i       — bucket = hash(sharding_key) / (Max/N)
```

---

## 8. Гарантии корректности

[63] **Точный routing**: каждая задача получает строки только своих шардов.

[64] **Совместимость hash**: DQ routing и runtime sharding используют одинаковую hash-функцию (см. [60]).

[65] **Единство порядка**: `CountComputeTasks`, `BuildInternalSinks` и `BuildKqpStageChannels` используют один порядок `GetColumnShards()` (см. [50], [54], [55]).

[66] **AFL_VERIFY**: если строка чужого шарда попала в задачу — crash с диагностикой (см. [56]).

[67] **Destructor validation**: `ActualShardIds ⊆ TargetShardIds` (см. [58]).

[68] **Invariant порядка**: `GetColumnShards()[i] == OrderedShardIds[i]` (см. [57]).

---

## 9. Изменённые файлы

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
| [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp) | Tasks, channels, sinks |
| [`kqp_tasks_graph.h`](ydb/core/kqp/executer_actor/kqp_tasks_graph.h) | CsShardingColumns в meta |
| [`kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp) | TargetShardIds propagation |
| [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp:569) | AFL_VERIFY routing |
| [`kqp_write_table.h`](ydb/core/kqp/runtime/kqp_write_table.h:259) | TargetShardIds в settings |
| [`yql_kikimr_settings.cpp`](ydb/core/kqp/provider/yql_kikimr_settings.cpp:175) | Server setting registration |
| [`yql_kikimr_settings.h`](ydb/core/kqp/provider/yql_kikimr_settings.h:130) | Server setting declaration |

### Тесты

| Файл | Роль |
|------|------|
| [`kqp_write_affinity_ut.cpp`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp) | 12 TWIN тестов (24 запуска) |
| [`kqp_write_affinity_ut_README.md`](ydb/core/kqp/ut/query/kqp_write_affinity_ut_README.md) | Документация тестов |
| [`ya.make`](ydb/core/kqp/ut/query/ya.make) | Тестовый файл в билде |

---

## 10. Тестовое покрытие

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
