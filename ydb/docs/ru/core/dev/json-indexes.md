# JSON-индексы

{% note warning %}

Функциональность JSON-индексов находится в разработке. Функция должна быть явно включена (флаги `EnableJsonIndex` и, для автоматического выбора индекса, `EnableJsonIndexAutoSelect`). В конфигурации по умолчанию оба флага выключены.

{% endnote %}

## Характеристики JSON-индексов {#characteristics}

JSON-индекс — глобальный синхронный вторичный индекс для [строковых](../concepts/datamodel/table.md#row-oriented-tables) таблиц. Он ускоряет фильтрацию строк по содержимому колонки типа `Json` или `JsonDocument`, если в предикате `WHERE` используются функции [JSON_EXISTS](../yql/reference/builtins/json.md) и [JSON_VALUE](../yql/reference/builtins/json.md) с выражениями [JsonPath](../yql/reference/builtins/json.md#jsonpath).

Индекс обновляется синхронно вместе с основной таблицей и может быть выбран при выполнении запроса:

- явно — через оператор `<имя таблицы> VIEW <имя_индекса>`;
- автоматически — оптимизатором запросов, если на кластере включён автовыбор и предикат подходит под формальные правила.

Каждый путь в JSON-документе (и при необходимости скалярные значения в списках в составе JSON-документа) кодируется в токен. Поиск по пути или по равенству значения сводится к инвертированному поиску по токенам — по той же схеме, что и у [базового полнотекстового индекса](../dev/fulltext-indexes.md#basic), но с собственным токенизатором JSON.

JSON-индекс сужает множество строк по токенам в запросе (и, для проверки на равенство значению, по токену «путь + значение»). Затем движок исполнения запросов повторно проверяет полный предикат. Запросы с `!=`, `<`, `>=`, `BETWEEN`, `starts with`, `like_regex` и т. п. могут использовать индекс для отсечения по пути, но итоговую точность сравнения обеспечивает пост-фильтр. Результат поиска по индексу всегда совпадает с выполнением того же запроса без индекса.

Ограничения реализации JSON-индексов:

- поддерживаются только для строковых таблиц;
- первичный ключ таблицы должен состоять из единственной колонки типа `Uint64` (временное ограничение, будет снято в ходе дальнейшего развития);
- JSON-индекс строится над единственной колонкой типа `Json` или `JsonDocument`;
- пакетные операции (операторы `BATCH UPDATE` и `BATCH DELETE`) для таблиц с JSON-индексами запрещены;
- значения числовых атрибутов в JSON-документе, превышающие по модулю 2⁵³, не индексируются (ограничение внутреннего представления).

## Создание индекса {#create}

### Создание индекса вместе с таблицей

```yql
CREATE TABLE `Documents` (
    `id` Uint64,
    `payload` JsonDocument,
    PRIMARY KEY (`id`),
    INDEX `json_idx` GLOBAL SYNC USING json ON (`payload`)
);
```

### Добавление индекса к существующей таблице

```yql
ALTER TABLE `Documents`
    ADD INDEX `json_idx` GLOBAL SYNC USING json ON (`payload`);
```

Ключевое слово `SYNC` можно опустить — для JSON-индекса синхронный режим является единственным поддерживаемым вариантом.

При добавлении индекса к непустой таблице выполняется фоновое построение, как у других глобальных индексов. До завершения построения индекс в состоянии «не готов» и не используется.

## Запросы с использованием индекса {#queries}

### Явное обращение через оператор VIEW {#view}

Как и для других вторичных индексов, имя индекса может указываться в секции `VIEW`:

```yql
SELECT *
FROM `Documents` VIEW `json_idx`
WHERE JSON_EXISTS(`payload`, '$.user.id');
```

Если предикат не поддерживается для выполнения через JSON-индекс, запрос с указанием `VIEW` завершится ошибкой на этапе компиляции или планирования (сообщение вида «Failed to extract jsonpath tokens from the predicate»).

### Автоматический выбор индекса {#autoselect}

При включённом флаге `EnableJsonIndexAutoSelect` оптимизатор может подставить JSON-индекс без `VIEW`, если:

- не выбран другой вторичный индекс для этого чтения;
- индекс готов к использованию (построен);
- предикат в секции `WHERE` полностью поддерживается для выполнения через JSON-индекс;
- все индексируемые фрагменты `JSON_EXISTS` / `JSON_VALUE` ссылаются на одну и ту же индексированную колонку.

Если автовыбор невозможен, запрос выполняется обычным сканированием таблицы или другого индекса, без ошибки.

Для отладки и гарантированного использования индекса рекомендуется явное указание секции `VIEW`.

## Поддерживаемые предикаты {#predicates}

Для выполнения через JSON-индексы поддерживаются только выражения на основе функций `JSON_EXISTS` и `JSON_VALUE` в блоке `WHERE`, объединённые операторами `AND` / `OR` по правилам ниже.

### JSON_EXISTS {#json-exists}

Проверка существования пути или значения внутри фильтра JsonPath.

**Разрешено:**

```yql
-- Корень документа (значение не NULL)
WHERE JSON_EXISTS(doc, '$')

-- Цепочка ключей; индексы массивов «прозрачны»
WHERE JSON_EXISTS(doc, '$.user.name')
WHERE JSON_EXISTS(doc, '$.items[*].sku')
WHERE JSON_EXISTS(doc, '$.items[0 to last].active')

-- Фильтр ? (...) — предикаты внутри фильтра допустимы
WHERE JSON_EXISTS(doc, '$.items ? (@.price == 100)')
WHERE JSON_EXISTS(doc, '$.items ? (@.qty >= 1 && @.qty <= 10)')
WHERE JSON_EXISTS(doc, '$.items ? (@.tag == $t)' PASSING "sale" AS t)

-- Методы JsonPath (путь индексируется до метода; точная проверка — пост-фильтр)
WHERE JSON_EXISTS(doc, '$.value.type()')
WHERE JSON_EXISTS(doc, '$.arr.size()')

-- Комбинации на одной колонке
WHERE JSON_EXISTS(doc, '$.a') AND JSON_EXISTS(doc, '$.b')
WHERE JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b')
```

**Запрещено** (ошибка при использовании оператора `VIEW` или отказ автовыбора):

```yql
-- Предикаты сравнения на верхнем уровне пути (вне ? (...))
WHERE JSON_EXISTS(doc, '$.key == 10')
WHERE JSON_EXISTS(doc, 'exists($.key)')
WHERE JSON_EXISTS(doc, '$.key starts with "a"')

-- Отрицание в JsonPath
WHERE JSON_EXISTS(doc, '!($.key == 10)')

-- ON ERROR TRUE
WHERE JSON_EXISTS(doc, '$.key' TRUE ON ERROR)

-- «Пустой» путь из одного литерала
WHERE JSON_EXISTS(doc, '1')
```

{% note info %}

Функция `JSON_EXISTS` возвращает `true` для любого непустого результата JsonPath. Предикат `$.key == 10`, указанный на верхнем уровне, дал бы «существование пути» даже когда сравнение ложно, что не соответствует ожидаемой семантике. Сравнения нужно выносить в вызовы `JSON_VALUE` или в фильтр вида `? (...)`.

{% endnote %}

### JSON_VALUE {#json-value}

Извлечение скалярного значения с обязательным `RETURNING <тип>`.

Поддерживаемые типы для секции RETURNING: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`.

**Разрешено:**

```yql
-- Равенство (путь + значение попадают в индекс)
WHERE JSON_VALUE(doc, '$.user.age' RETURNING Int32) == 25
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) = true
WHERE JSON_VALUE(doc, '$.name' RETURNING Utf8) = "Alice"u

-- Неявное сравнение с true для Bool
WHERE JSON_VALUE(doc, '$.active' RETURNING Bool)

-- Сравнения по пути (в индекс идёт только путь; точность — пост-фильтр)
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) > 0
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) != 100
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) BETWEEN 1 AND 10
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) NOT BETWEEN 0 AND 5

-- IN: список литералов — OR токенов; параметр-список — один токен пути
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN ("open"u, "pending"u)
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN $status_list

-- Параметры
WHERE JSON_VALUE(doc, '$.user.id' RETURNING Int64) = $id
WHERE JSON_VALUE(doc, '$.tag' RETURNING Utf8) = $tag

-- PASSING для переменных JsonPath
WHERE JSON_VALUE(doc, '$.x ? (@.y == $v)' RETURNING Int64 PASSING 42 AS v) = 10

-- Предикаты JsonPath внутри пути (как у JSON_EXISTS)
WHERE JSON_VALUE(doc, '$.user ? (@.role == "admin")' RETURNING Utf8) = "ok"u
WHERE JSON_VALUE(doc, '$.code starts with "A"' RETURNING String) != ""
WHERE JSON_VALUE(doc, 'exists($.meta)' RETURNING Bool)

-- Комбинации AND / OR на одной колонке
WHERE JSON_VALUE(doc, '$.a' RETURNING Int32) == 1
   OR JSON_VALUE(doc, '$.b' RETURNING Int32) == 2
WHERE JSON_EXISTS(doc, '$.a') AND JSON_VALUE(doc, '$.a' RETURNING Int32) == 10
```

**Запрещено или не индексируется:**

```yql
-- Вызов JSON_VALUE без RETURNING
WHERE JSON_VALUE(doc, '$.key') = "x"

-- DEFAULT при ON EMPTY / ON ERROR (кроме NULL)
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8 DEFAULT "x" ON ERROR) = "y"

-- Типы даты/времени
WHERE JSON_VALUE(doc, '$.ts' RETURNING Timestamp) = ...

-- RETURNING Bool с операторами сравнения (кроме неявного = true)
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) >= true

-- IS NULL / IS NOT NULL — семантически противоречат индексу «существования пути»
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8) IS NULL

-- Сравнение двух JSON_VALUE с разных колонок
WHERE JSON_VALUE(doc1, '$.k' RETURNING Utf8) = JSON_VALUE(doc2, '$.k' RETURNING Utf8)

-- Вложенные JSON_* в аргументах
WHERE JSON_VALUE(JSON_VALUE(doc, '$.a' RETURNING Utf8), '$.b' RETURNING Utf8) = "x"
```

{% note info %}

Для проверки «значение равно `false`» или «путь отсутствует / равен `null`» используйте JsonPath внутри `JSON_EXISTS`, например `JSON_EXISTS(doc, '$.k == false')` или `JSON_EXISTS(doc, '$.k ? (@ == null)')`, а не `JSON_VALUE(...) IS NULL`.

{% endnote %}

### Обработка AND и OR на уровне SQL-запроса {#and-or}

| Контекст | Поведение |
|----------|-----------|
| `AND` | Индексируемые части объединяются для проверки через индекс. Неиндексируемое условие игнорируется для построения токенов, но остаётся в пост-фильтре (например, `JSON_EXISTS(doc,'$.a') AND Data = 'x'`). |
| `OR` | Обе ветки должны быть индексируемы; иначе вся `OR`-группа не использует индекс. |
| `NOT` | Не индексируется. В `AND` выражение с `NOT` отбрасывается из индекса; в `OR` — вся ветка не индексируется. |
| Смешение режимов `AND`/`OR` | При конфликте побеждает режим OR (более широкое чтение). |

Пример: условие `JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b') AND other_col = 1` в SQL трактуется как `(JE(a) OR JE(b)) AND (other_col = 1)`; индексируемая часть — `OR` двух `JSON_EXISTS`, фильтр по `other_col` обрабатывается как пост-фильтр.

### Передача параметров в предикаты {#parameters}

Поддерживаются:

1. Прямое сравнение результата с параметром: `JSON_VALUE(doc, '$.k' RETURNING Text) = $p`
2. Проверка наличия результата в списке значений: `JSON_VALUE(doc, '$.k' RETURNING Text) IN $list`
3. Передача параметра через секцию PASSING: `JSON_EXISTS(doc, '$.k ? (@ == $v)' PASSING $p AS v)`

Поддерживаемые типы параметров: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`.

## Примеры сценариев {#examples}

### Каталог со вложенными атрибутами

```yql
CREATE TABLE `Products` (
    `sku_id` Uint64,
    `attrs` JsonDocument,
    PRIMARY KEY (`sku_id`),
    INDEX `attrs_json_idx` GLOBAL USING json ON (`attrs`)
);

-- Товары с брендом и ценой в диапазоне
SELECT `sku_id`, `attrs`
FROM `Products` VIEW `attrs_json_idx`
WHERE JSON_VALUE(`attrs`, '$.brand' RETURNING Utf8) = "ACME"u
  AND JSON_VALUE(`attrs`, '$.price' RETURNING Double) BETWEEN 10.0 AND 100.0;

-- Есть вложенный массив с элементом, у которого stock > 0
SELECT `sku_id`
FROM `Products` VIEW `attrs_json_idx`
WHERE JSON_EXISTS(`attrs`, '$.warehouses ? (@.stock > 0)');
```

### Документы с параметризованным запросом

```yql
DECLARE $user_id AS Int64;

SELECT `id`, `payload`
FROM `Documents` VIEW `json_idx`
WHERE JSON_VALUE(`payload`, '$.owner_id' RETURNING Int64) = $user_id
  AND JSON_EXISTS(`payload`, '$.archived ? (@ == false)');
```

### Проверка типа поля

```yql
SELECT `id`
FROM `Documents` VIEW `json_idx`
WHERE JSON_VALUE(`payload`, '$.data.type()' RETURNING Utf8) = "array"u;
```

Метод `.type()` в этом примере завершает построение токена на пути `$.data`; проверка строки на значение `"array"` выполняется пост-фильтром.

## Связанные материалы {#see-also}

- [Функции для работы с JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, синтаксис JsonPath.
- [Вторичные индексы](secondary-indexes.md) — общие сведения о глобальных индексах и `VIEW`.
- [Полнотекстовые индексы](fulltext-indexes.md) — родственный механизм инвертированного поиска по токенам.
