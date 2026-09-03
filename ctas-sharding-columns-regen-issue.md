# CTAS Sharding Columns - Полный Анализ Ошибки

## Ошибка

```
assertion failed at ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:240,
virtual void NKikimr::NKqp::NTestSuiteCS_WriteAffinity::TTestCaseCtasShardingColumnsInPlan::Execute_(NUnitTest::TTestContext &):
(result.IsSuccess())
<main>: Error: Execution, code: 1060
    <main>:8:20: Error: At tuple, At tuple, At function: KqlFillTable, At function: KqlFillTable
        <main>:8:20: Error: Expected at most 4 argument(s), but got 5
```

## Полный Анализ

### 1. Точка Падения

Ошибка возникает в тесте [`kqp_write_affinity_ut.cpp:240`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:240):

```cpp
auto result = client.ExecuteQuery(
    ctasQuery,  // CTAS с PARTITION BY HASH(Col1)
    NYdb::NQuery::TTxControl::NoTx(),
    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
).ExtractValueSync();
UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());  // ← падает здесь
```

Запрос CTAS:
```sql
PRAGMA ydb.EnableCsWriteAffinity = "true";
CREATE TABLE `/Root/Dest1` (PRIMARY KEY (Col1))
PARTITION BY HASH(Col1)
WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
AS SELECT * FROM `/Root/Source`;
```

### 2. Цепочка Вызовов (Call Chain)

```
Тест ExecuteQuery(CTAS)
  → kqp_statement_rewrite.cpp: RewriteCreateTableAs()
    → Создаёт Write! callable с настройками insertSettings (line 328-326)
      → Добавляет CtasShardingColumns в insertSettings (line 318-326)
  → kqp_opt_kql.cpp: BuildFillTable() (line 478-494)
    → Читает CtasShardingColumns из настроек (line 482)
    → Создаёт builder для TKqlFillTable (line 483-487)
    → Вызывает builder.CtasShardingColumns(...) (line 491)  ← ОШИБКА ЗДЕСЬ
  → Сгенерированный код TKqlFillTableBuilder
    → Проверяет количество аргументов
    → Ожидает максимум 4, получает 5  ← "Expected at most 4 argument(s), but got 5"
```

### 3. Детальный Анализ Кода

#### Фаза 1: Rewrite (kqp_statement_rewrite.cpp)

В [`RewriteCreateTableAs()`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:328) создаётся `Write!` callable:

```cpp
// Line 312-326: Добавление CtasShardingColumns в настройки
if (settings.PartitionBy.IsValid()) {
    NYql::TExprNode::TListType keyColumnsList;
    for (const auto& col : settings.PartitionBy.Cast()) {
        keyColumnsList.push_back(exprCtx.NewAtom(pos, col.Value()));
    }
    insertSettings.push_back(
        exprCtx.NewList(pos, {
            exprCtx.NewAtom(pos, "CtasShardingColumns"),  // ← Название настройки
            exprCtx.NewList(pos, std::move(keyColumnsList)),
        }));
}

// Line 328: Создание Write! callable
const auto insert = exprCtx.NewCallable(pos, "Write!", {
    // ... input, dataSink, key, settings (включая CtasShardingColumns)
});
```

**Результат**: `Write!` callable содержит настройку `CtasShardingColumns` со значением `["Col1"]`.

#### Фаза 2: Optimization (kqp_opt_kql.cpp)

В [`BuildFillTable()`](ydb/core/kqp/opt/kqp_opt_kql.cpp:478) конвертируется `Write!` в `TKqlFillTable`:

```cpp
TExprBase BuildFillTable(const TKiWriteTable& write, TExprContext& ctx) {
    auto originalPathNode = GetSetting(write.Settings().Ref(), "OriginalPath");
    AFL_ENSURE(originalPathNode);

    // Line 482: Чтение CtasShardingColumns из настроек
    auto ctasShardingColumnsNode = GetSetting(write.Settings().Ref(), "CtasShardingColumns");

    // Line 483-487: Создание builder'а с 4 обязательными параметрами
    auto builder = Build<TKqlFillTable>(ctx, write.Pos())
        .Input(write.Input())           // ← Аргумент 1
        .Table(write.Table())           // ← Аргумент 2
        .Cluster(write.DataSink().Cluster())  // ← Аргумент 3
        .OriginalPath(TCoNameValueTuple(originalPathNode).Value().Cast<TCoAtom>());  // ← Аргумент 4

    // Line 488-493: Попытка добавить 5-й аргумент
    if (ctasShardingColumnsNode) {
        const auto ctasShardingColumns = TCoNameValueTuple(ctasShardingColumnsNode).Value().Cast<TCoAtomList>();
        if (ctasShardingColumns.Ref().ChildrenSize() > 0) {
            builder.CtasShardingColumns(ctasShardingColumns);  // ← Аргумент 5 — ОШИБКА!
        }
    }
    return builder.Done();
}
```

#### Фаза 3: Сгенерированный Код (ПРОБЛЕМА)

JSON схема [`kqp_expr_nodes.json:291-301`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:291) определяет:

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

**Ожидается**, что сгенерированный `TKqlFillTableBuilder` должен иметь метод `.CtasShardingColumns()`.

**Реальность**: Сгенерированный код в build-директории **не был обновлён** после изменения JSON. Builder всё ещё ожидает максимум 4 аргумента (Input, Table, Cluster, OriginalPath).

### 4. Почему Сгенерированный Код Не Обновился

1. **JSON изменён** в source directory: [`kqp_expr_nodes.json:300`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:300)
2. **Сгенерированные файлы** (`kqp_expr_nodes.gen.h`, `.decl.inl.h`, `.defs.inl.h`) создаются в build directory
3. **Build cache**: Система `ya` кэширует сгенерированные файлы и не пересоздаёт их, если считает, что зависимости не изменились
4. **Результат**: Старый сгенерированный код (без `CtasShardingColumns`) используется при компиляции

### 5. Структура Файлов

```
ydb/core/kqp/expr_nodes/
├── kqp_expr_nodes.json      ← ИЗМЕНЁН (5 children для TKqlFillTable)
├── kqp_expr_nodes.h         ← Включает сгенерированные файлы
├── kqp_expr_nodes.cpp       ← Реализация
└── ya.make                  ← Определяет генерацию

build_relwithdebinfo/ydb/core/kqp/expr_nodes/
├── kqp_expr_nodes.gen.h     ← НЕ ОБНОВЛЁН (4 children)
├── kqp_expr_nodes.decl.inl.h ← НЕ ОБНОВЛЁН
└── kqp_expr_nodes.defs.inl.h ← НЕ ОБНОВЛЁН
```

### 6. Как Исправить

```bash
# Вариант 1: Touch + rebuild
touch ydb/core/kqp/expr_nodes/kqp_expr_nodes.json
./ya make --build relwithdebinfo ydb/core/kqp/expr_nodes
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/query -F '*CtasShardingColumnsInPlan*'

# Вариант 2: Ручная генерация
python3 yql/essentials/core/expr_nodes_gen/gen/__main__.py \
    ydb/core/kqp/expr_nodes/yql_expr_nodes_gen.jnj \
    ydb/core/kqp/expr_nodes/kqp_expr_nodes.json \
    ydb/core/kqp/expr_nodes/kqp_expr_nodes.gen.h \
    ydb/core/kqp/expr_nodes/kqp_expr_nodes.decl.inl.h \
    ydb/core/kqp/expr_nodes/kqp_expr_nodes.defs.inl.h

# Вариант 3: Очистка кэша
rm -rf build_relwithdebinfo/ydb/core/kqp/expr_nodes/
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/query -F '*CtasShardingColumnsInPlan*'
```

## Резюме

| Фаза | Файл | Статус |
|------|------|--------|
| JSON Schema | [`kqp_expr_nodes.json`](ydb/core/kqp/expr_nodes/kqp_expr_nodes.json:300) | ✅ Обновлён (5 children) |
| Rewrite | [`kqp_statement_rewrite.cpp:318-326`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:318) | ✅ Добавляет CtasShardingColumns |
| Optimizer | [`kqp_opt_kql.cpp:482-491`](ydb/core/kqp/opt/kqp_opt_kql.cpp:482) | ✅ Читает и передаёт CtasShardingColumns |
| Effect Builder | [`kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp:216) | ✅ Использует CtasShardingColumns |
| Generated Code | `kqp_expr_nodes.gen.h` | ❌ НЕ ОБНОВЛЁН (4 children) |
| Test | [`kqp_write_affinity_ut.cpp:202`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:202) | ⏳ Ждёт обновления генерации |

**Корневая причина**: Сгенерированный C++ код из JSON схемы не был пересоздан после добавления поля `CtasShardingColumns`.
