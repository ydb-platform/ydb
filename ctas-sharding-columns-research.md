# Исследование: Передача sharding columns в оптимизатор FILL фазы для CTAS

## 1. Текущая архитектура CTAS

### 1.1 Декомпозиция CTAS на стейтменты

`RewriteCreateTableAs()` в [`kqp_statement_rewrite.cpp:176`](ydb/core/kqp/host/kqp_statement_rewrite.cpp:176) разбивает CTAS на 3 стейтмента:

```
CREATE TABLE Destination (...) AS SELECT ... FROM Source
    ↓
┌─────────────────────────────────────────────────────────┐
│ 1. CREATE TABLE (result.CreateTable)                    │
│    - Создаёт temp-таблицу /.tmp/sessions/.../uuid       │
│    - Schema из SELECT output type                       │
│    - PRIMARY KEY из запроса                             │
├─────────────────────────────────────────────────────────┤
│ 2. FILL / Write! (result.ReplaceInto)                   │
│    - Записывает данные в temp-таблицу                   │
│    - Settings: mode=fill_table, OriginalPath, ...       │
│    - НЕ содержит информацию о sharding columns          │
├─────────────────────────────────────────────────────────┤
│ 3. MOVE (result.MoveTable)                              │
│    - Атомарно переименовывает temp → destination        │
└─────────────────────────────────────────────────────────┘
```

### 1.2 Что известно в RewriteCreateTableAs

В функции `RewriteCreateTableAs` **вся информация доступна**:

```cpp
// kqp_statement_rewrite.cpp:232-236
auto primaryKey = create->Child(4)->Child(2)->Child(1);
THashSet<TStringBuf> primariKeyColumns;
primaryKey->ForEachChild([&](const auto& child) {
    primariKeyColumns.insert(child.Content());
});

// Schema из SELECT output type
const auto rowType = type->Cast<NYql::TStructExprType>();  // line 221

// Settings CREATE TABLE (включая SHARDING_KEY если указан)
// create->Child(4) содержит все settings
```

### 1.3 Что передаётся в FILL стейтмент

```cpp
// kqp_statement_rewrite.cpp:297-311
NYql::TExprNode::TListType insertSettings;
insertSettings.push_back({ "mode", "fill_table" });
insertSettings.push_back({ "OriginalPath", tableName });       // destination path
insertSettings.push_back({ "AllowInconsistentWrites" });
// ❌ PRIMARY KEY НЕ передаётся
// ❌ SHARDING COLUMNS НЕ передаются
```

### 1.4 Что видит оптимизатор FILL

`BuildFillTableEffect()` в [`kqp_opt_effects.cpp:216`](ydb/core/kqp/opt/kqp_opt_effects.cpp:216) получает `TKqlFillTable` с:
- `node.Table()` — путь к temp-таблице
- `node.OriginalPath()` — путь destination
- `node.Input()` — источник данных

**Не имеет доступа к**:
- PRIMARY KEY целевой таблицы
- Sharding columns
- AST CREATE TABLE

---

## 2. Варианты решения

### Вариант A: Передать sharding columns через settings FILL стейтмента

**Идея**: В `RewriteCreateTableAs` добавить sharding columns в `insertSettings`:

```cpp
// kqp_statement_rewrite.cpp:297-311 (изменённый)
NYql::TExprNode::TListType insertSettings;
insertSettings.push_back({ "mode", "fill_table" });
insertSettings.push_back({ "OriginalPath", tableName });
insertSettings.push_back({ "AllowInconsistentWrites" });

// НОВОЕ: передать sharding columns
TVector<NYql::TExprNodePtr> shardingColNodes;
for (const auto& col : primariKeyColumns) {
    shardingColNodes.push_back(exprCtx.NewAtom(pos, TString(col)));
}
insertSettings.push_back({
    "CtasShardingColumns",
    exprCtx.NewList(pos, std::move(shardingColNodes))
});
```

В оптимизаторе извлечь:
```cpp
// kqp_opt_effects.cpp
auto shardingColsNode = GetSetting(settings.Settings().Ref(), "CtasShardingColumns");
if (shardingColsNode) {
    for (const auto& col : shardingColsNode->Cast<TExprList>()) {
        keyColumnAtoms.emplace_back(
            Build<TCoAtom>(ctx, node.Pos()).Value(col.Cast<TCoAtom>().StringValue()).Done());
    }
}
```

**Плюсы**:
- ✅ Прямой путь передачи данных
- ✅ Не требует изменения инфраструктуры
- ✅ Работает для всех случаев

**Минусы**:
- ❌ Нужно парсить SHARDING_KEY из CREATE TABLE settings (если отличается от PK)
- ❌ Для ColumnShard sharding columns = PK columns в большинстве случаев, но нужно учитывать `WITH (SHARDING_KEY = ...)`

### Вариант B: Использовать type annotation SELECT output

**Идея**: Sharding columns для ColumnShard обычно совпадают с PRIMARY KEY, который совпадает с первыми колонками SELECT output.

**Текущий fallback** уже использует первую колонку:
```cpp
keyColumnAtoms.emplace_back(
    Build<TCoAtom>(ctx, node.Pos()).Value(structType.GetItems().front()->GetName()).Done());
```

**Плюсы**:
- ✅ Не требует изменений
- ✅ Работает для типичных случаев (single-column PK)

**Минусы**:
- ❌ Не работает для multi-column PK
- ❌ Не работает когда sharding columns ≠ PK
- ❌ Placeholder, а не реальные данные

### Вариант C: Резолвить таблицу после CREATE TABLE

**Идея**: Перед оптимизацией FILL стейтмента дождаться завершения CREATE TABLE и резолвить metadata temp-таблицы.

**Плюсы**:
- ✅ Получаем точные sharding columns из metadata
- ✅ Работает для всех случаев

**Минусы**:
- ❌ Требует синхронизации между стейтментами
- ❌ CREATE TABLE и FILL компилируются независимо
- ❌ Сложная реализация

### Вариант D: Передать полный schema через settings

**Идея**: Передать полную информацию о таблице (PK, sharding config) в settings:

```cpp
insertSettings.push_back({
    "CtasTableSchema",
    exprCtx.NewList(pos, {
        exprCtx.NewList(pos, { "PrimaryKey", pkColumns }),
        exprCtx.NewList(pos, { "ShardingColumns", shardingColumns }),
    })
});
```

**Плюсы**:
- ✅ Максимальная информация
- ✅ Готово для будущих расширений

**Минусы**:
- ❌ Избыточно для текущих нужд
- ❌ Нужно парсить SHARDING_KEY из CREATE TABLE

---

## 3. Анализ SHARDING_KEY для ColumnShard

### 3.1 Как определяется sharding key

Для ColumnShard таблицы sharding columns определяются так:

```cpp
// Из ColumnTableInfo
const auto& sharding = desc.GetSharding();
if (sharding.HasHashSharding()) {
    // Sharding columns из hash sharding config
    for (const auto& col : sharding.GetHashSharding().GetColumns()) {
        // ...
    }
}
```

### 3.2 Сценарии

| Сценарий | PK | Sharding Columns | Совпадают? |
|----------|-----|-----------------|------------|
| CTAS без явного SHARDING_KEY | (Id) | (Id) | ✅ |
| CTAS с multi-column PK | (Id, Cat) | (Id, Cat) | ✅ |
| CTAS с `WITH (SHARDING_KEY = 'Id')` | (Id, Cat) | (Id) | ❌ |
| CTAS с PARTITION BY | (Id) | (Id) | ✅ |

В **большинстве случаев** sharding columns = PK columns.

### 3.3 Извлечение SHARDING_KEY из CREATE TABLE

В `RewriteCreateTableAs` settings CREATE TABLE доступны через `create->Child(4)`:

```cpp
// Нужно найти setting "shardingKey" или аналогичный
// в AST CREATE TABLE
```

---

## 4. Рекомендуемое решение

### Вариант A (модифицированный)

**Передавать PRIMARY KEY columns через settings FILL стейтмента**:

1. В `RewriteCreateTableAs` добавить `CtasKeyColumns` в `insertSettings`
2. В `TKqlFillTable` добавить accessor для `CtasKeyColumns`
3. В `BuildFillTableEffect` использовать `CtasKeyColumns` вместо placeholder

**Обоснование**:
- Для **большинства случаев** PK = sharding columns
- Реальные sharding columns всё равно подставляются на runtime из `CsShardingColumns`
- KeyColumns в оптимизаторе нужны только для type validation + построения HashShuffle плана
- На runtime `BuildColumnShardHashV1ForWriteAffinity` использует `CsShardingColumns`, а не proto KeyColumns

**Риск**: Если sharding columns ≠ PK (редкий случай с `WITH (SHARDING_KEY = ...)`), оптимизатор построит план с неправильными key columns, но на runtime они будут заменены на правильные из `CsShardingColumns`. Это безопасно, потому что:
1. Type validation пройдёт (PK columns существуют в SELECT output)
2. Runtime заменит на правильные sharding columns

### Альтернатива: Не менять ничего

Текущий placeholder (первая колонка SELECT output) **работает корректно**:
- Type validation проходит
- Runtime заменяет на правильные sharding columns из `CsShardingColumns`
- Для single-column PK (наиболее частый случай) placeholder = правильный ключ

**Вывод**: Текущее решение с placeholder является **корректным и безопасным**. Передача реальных key columns из CREATE TABLE — это оптимизация для точности EXPLAIN плана, но не обязательна для корректности выполнения.

---

## 5. Выводы

1. **Информация о sharding columns доступна** в `RewriteCreateTableAs`, но не передаётся в FILL стейтмент
2. **Текущий placeholder работает корректно** — реальные sharding columns подставляются на runtime
3. **Для улучшения EXPLAIN плана** можно передать PK columns через settings
4. **Для полной точности** нужно парсить SHARDING_KEY из CREATE TABLE settings, но это редкий случай
