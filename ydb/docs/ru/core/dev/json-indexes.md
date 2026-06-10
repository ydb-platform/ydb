# JSON-индексы

{% note warning %}

Функциональность JSON-индексов находится в разработке. Функция должна быть явно включена (требуется [включить флаги](../reference/configuration/feature_flags.md): `enable_json_index` и, для автоматического выбора индекса, `enable_json_index_auto_select`). В конфигурации по умолчанию оба флага выключены.

{% endnote %}

JSON-индексы — это специализированный тип [полнотекстового индекса](../concepts/glossary.md#fulltext-index), который ускоряет фильтрацию строк таблицы по условиям, накладываемым на содержимое колонок типа `Json` и `JsonDocument`. Индекс задействуется, если в предикате `WHERE` используются функции [JSON_EXISTS](../yql/reference/builtins/json.md) и [JSON_VALUE](../yql/reference/builtins/json.md) с выражениями [JsonPath](../yql/reference/builtins/json.md#jsonpath). В отличие от традиционных вторичных индексов, оптимизированных для поиска по равенству или диапазону отдельных колонок таблицы, JSON-индекс работает с произвольными путями внутри JSON-документа.

Общее описание поиска по JSON и устройства инвертированного индекса по путям JSON-документа см. в разделе [{#T}](../concepts/query_execution/json_search.md).

## Характеристики JSON-индексов {#characteristics}

JSON-индексы в {{ ydb-short-name }} позволяют:

* быстро фильтровать строки по [JSON_EXISTS](#json-exists) и [JSON_VALUE](#json-value) с выражениями [JsonPath](../yql/reference/builtins/json.md#jsonpath);
* комбинировать индексируемые условия операторами `AND` и `OR`;
* подставлять параметры запроса, переданные приложением, в проверяемые предикаты.

JSON-индекс является [глобальным синхронным](../concepts/glossary.md#secondary-index) индексом — его данные всегда согласованы с основной таблицей.

При выполнении запроса JSON-индекс может быть применён:

- явно — через оператор `<имя_таблицы> VIEW <имя_индекса>`;
- автоматически — [оптимизатором](../concepts/glossary.md#optimizer), если на кластере включён автовыбор и предикат подходит под формальные правила.

## Синтаксис JSON-индексов {#syntax}

Создание JSON-индекса:

* при создании таблицы: [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md);
* добавление к существующей таблице: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#add-index).

Удаление JSON-индексов выполняется через [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#drop-index):

```yql
ALTER TABLE documents DROP INDEX json_idx
```

Синтаксис запроса с явным указанием JSON-индекса:

* [VIEW (JSON-индекс)](../yql/reference/syntax/select/json_index.md).

Функции и выражения для работы с JSON в предикатах:

* [Функции для работы с JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`;
* [JsonPath](../yql/reference/builtins/json.md#jsonpath) — язык запросов для обращения к значениям внутри JSON.

Готовые сценарии использования собраны в разделе [рецептов поиска по JSON-документам](../recipes/json-search/index.md).

## Обновление JSON-индексов {#update}

JSON-индексы автоматически поддерживаются при модификации данных и обновляются синхронно вместе с основной таблицей. Таблицы с JSON-индексами поддерживают:

* `INSERT`
* `UPSERT`
* `REPLACE`
* `UPDATE`
* `DELETE`

Пакетные операции (`BATCH UPDATE` и `BATCH DELETE`) для таблиц с JSON-индексами не поддерживаются.

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

-- Методы JsonPath (путь индексируется до метода; точную проверку выполняет пост-фильтр)
WHERE JSON_EXISTS(doc, '$.value.type()')
WHERE JSON_EXISTS(doc, '$.arr.size()')

-- Комбинации на одной колонке
WHERE JSON_EXISTS(doc, '$.a') AND JSON_EXISTS(doc, '$.b')
WHERE JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b')
```

**Запрещено** (ошибка при использовании оператора `VIEW` или отказ автовыбора индекса):

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

Поддерживаемые типы для секции `RETURNING`: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`.

**Разрешено:**

```yql
-- Равенство (путь + значение попадают в индекс)
WHERE JSON_VALUE(doc, '$.user.age' RETURNING Int32) = 25
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) = true
WHERE JSON_VALUE(doc, '$.name' RETURNING Utf8) = "Alice"u

-- Неявное сравнение с true для Bool
WHERE JSON_VALUE(doc, '$.active' RETURNING Bool)

-- Параметры
WHERE JSON_VALUE(doc, '$.user.id' RETURNING Int64) = $id
WHERE JSON_VALUE(doc, '$.tag' RETURNING Utf8) = $tag

-- Сравнения (в индексе задействован только путь, сравнение выполняет пост-фильтр)
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) > 0
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) != 100
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) BETWEEN 1 AND 10
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) NOT BETWEEN 0 AND 5

-- IN: список литералов
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN ("open"u, "pending"u)

-- IN: заданный параметр типа List<Utf8>
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN $status_list

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

-- RETURNING Bool с операторами сравнения
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) >= true

-- IS NULL / IS NOT NULL — семантически противоречат индексу «существования пути»
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8) IS NULL

-- Сравнение двух JSON_VALUE с разных колонок
WHERE JSON_VALUE(doc1, '$.k' RETURNING Utf8) = JSON_VALUE(doc2, '$.k' RETURNING Utf8)

-- Вложенные JSON_* в аргументах
WHERE JSON_VALUE(JSON_VALUE(doc, '$.a' RETURNING Utf8), '$.b' RETURNING Utf8) = "x"
```

{% note info %}

Для проверки «значение равно `false`» или «значение равно `null`» используйте фильтр JsonPath внутри `JSON_EXISTS`, например `JSON_EXISTS(doc, '$.k ? (@ == false)')` или `JSON_EXISTS(doc, '$.k ? (@ == null)')`, а не `JSON_VALUE(...) IS NULL`.

{% endnote %}

## Ограничения {#limitations}

* JSON-индексы поддерживаются только для [строковых](../concepts/datamodel/table.md#row-oriented-tables) таблиц.
* Первичный ключ таблицы должен состоять из единственной колонки типа `Uint64` (временное ограничение, будет снято в ходе дальнейшего развития).
* В одном JSON-индексе индексируется ровно одна колонка типа `Json` или `JsonDocument`.
* Покрывающие индексы (выражение `COVER`) для JSON-индексов не поддерживаются.
* Пакетные операции (`BATCH UPDATE` и `BATCH DELETE`) для таблиц с JSON-индексами запрещены.
* Значения числовых атрибутов в JSON-документе, превышающие по модулю 2⁵³, не индексируются (ограничение внутреннего представления).

## Рецепты {#recipes}

Готовые сценарии работы с JSON-индексом:

* [{#T}](../recipes/json-search/json-index-quickstart.md) — быстрый старт.
* [{#T}](../recipes/json-search/json-index-catalog.md) — каталог товаров со вложенными атрибутами.
* [{#T}](../recipes/json-search/json-index-parameters.md) — параметризованные запросы и переменные JsonPath.
* [{#T}](../recipes/json-search/json-index-typecheck.md) — проверка типа поля и наличия пути.

## Связанные материалы {#see-also}

- [Функции для работы с JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, синтаксис JsonPath.
- [Вторичные индексы](secondary-indexes.md) — общие сведения о глобальных индексах и `VIEW`.
- [Полнотекстовые индексы](fulltext-indexes.md) — родственный механизм инвертированного поиска по токенам.
- [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md) и [VIEW (JSON-индекс)](../yql/reference/syntax/select/json_index.md) — справка по синтаксису.
