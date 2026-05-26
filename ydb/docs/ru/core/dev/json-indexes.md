# JSON-индексы

{% note warning %}

Документ является **прототипом** пользовательской документации.

Функциональность JSON-индексов находится в разработке. Функция должна быть явно включена (флаги `EnableJsonIndex` и, для автоматического выбора индекса, `EnableJsonIndexAutoSelect`). В конфигурации по умолчанию оба флага выключены.

{% endnote %}

## Обзор {#overview}

JSON-индекс — глобальный **синхронный** вторичный индекс для [строковых](../concepts/datamodel/table.md#row-oriented-tables) таблиц. Он ускоряет фильтрацию строк по содержимому колонки типа `Json` или `JsonDocument`, если в предикате `WHERE` используются функции [JSON_EXISTS](../yql/reference/builtins/json.md) и [JSON_VALUE](../yql/reference/builtins/json.md) с выражениями [JsonPath](../yql/reference/builtins/json.md#jsonpath).

Индекс обновляется синхронно вместе с основной таблицей и может быть выбран при выполнении запроса:

- **явно** — через оператор `<имя таблицы> VIEW <имя_индекса>`;
- **автоматически** — оптимизатором запросов, если на кластере включён автовыбор и предикат подходит под формальные правила.

Каждый путь в JSON-документе (и при необходимости скалярные значения в списках в составе JSON-документа) кодируется в токен. Поиск по пути или по равенству значения сводится к инвертированному поиску по токенам — по той же схеме, что и у базового полнотекстового индекса (вида `fulltext_plain`), но с собственным токенизатором JSON.

JSON-индекс сужает множество строк по токенам пути (и, для равенства, по токену «путь + значение»). Затем движок исполнения запросов повторно проверяет полный предикат. Поэтому:

- запросы с `!=`, `<`, `>=`, `BETWEEN`, `starts with`, `like_regex` и т. п. **могут** использовать индекс для отсечения по пути, но итоговую точность сравнения обеспечивает пост-фильтр;
- результат всегда совпадает с выполнением того же запроса без индекса.

## Требования к таблице и индексу {#requirements}

| Требование | Значение |
|------------|----------|
| Тип таблицы | Только **строковые** (row) таблицы. Для колоночных (OLAP) таблиц JSON-индекс недоступен. |
| Первичный ключ | Ровно **одна** колонка типа `Uint64`. Составные и не-`Uint64` ключи пока не поддерживаются (временное ограничение). |
| Индексируемая колонка | Ровно **одна** колонка типа `Json` или `JsonDocument` на каждый индекс. |
| Тип индекса | `GLOBAL SYNC USING json` — поддерживается только глобальный синхронный индекс. |
| Покрывающие колонки | Не поддерживается (операнд `COVER` недоступен). |
| Параметры `WITH` | Не поддерживаются (в отличие от полнотекстовых индексов). |
| Пакетные операции | `BATCH UPDATE` / `BATCH DELETE` для таблиц с JSON-индексом **запрещены**. |

При добавлении индекса к **непустой** таблице выполняется фоновое построение, как у других глобальных индексов. До завершения построения индекс в состоянии «не готов» и не используется.

## Создание индекса {#create}

### Вместе с таблицей

CREATE TABLE `Documents` (
    `id` Uint64,
    `payload` JsonDocument,
    PRIMARY KEY (`id`),
    INDEX `json_idx` GLOBAL SYNC USING json ON (`payload`)
);
```

Тип колонки может быть `Json` или `JsonDocument`.

### Добавление к существующей таблице

ALTER TABLE `Documents`
    ADD INDEX `json_idx` GLOBAL SYNC USING json ON (`payload`);
```

Ключевое слово `SYNC` можно опустить — для JSON-индекса синхронный режим является единственным поддерживаемым вариантом.

## Запросы с использованием индекса {#queries}

### Явное обращение через оператор VIEW {#view}

Как и для других вторичных индексов, имя индекса указывается в секции `VIEW`:

SELECT *
FROM `Documents` VIEW `json_idx`
WHERE JSON_EXISTS(`payload`, '$.user.id');
```

Если предикат **не соответствует** правилам индексации, запрос с `VIEW` завершится ошибкой на этапе компиляции или планирования (сообщение вида «Failed to extract jsonpath tokens from the predicate»).

### Автоматический выбор индекса {#autoselect}

При включённом флаге `EnableJsonIndexAutoSelect` оптимизатор может подставить JSON-индекс **без** `VIEW`, если:

- не выбран другой первичный или вторичный индекс для этого чтения;
- индекс в состоянии **Ready**;
- предикат `WHERE` целиком укладывается в поддерживаемую грамматику;
- все индексируемые фрагменты `JSON_EXISTS` / `JSON_VALUE` ссылаются на **одну и ту же** индексированную колонку.

Если автовыбор невозможен, запрос выполняется обычным сканированием таблицы или другого индекса — **без ошибки**.

Для отладки и гарантированного использования индекса рекомендуется явное указание оператора `VIEW`.

## Поддерживаемые предикаты {#predicates}

Индексируются только выражения на основе `JSON_EXISTS` и `JSON_VALUE` в блоке `WHERE`, объединённые операторами `AND` / `OR` по правилам ниже. Остальные условия ведут себя по-разному в зависимости от оператора.

### JSON_EXISTS {#json-exists}

Проверка **существования** пути или значения внутри фильтра JsonPath.

**Разрешено:**

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

Смысл ограничения для верхнего уровня: `JSON_EXISTS` возвращает `true` для любого **непустого** результата JsonPath. Предикат `$.key == 10` на верхнем уровне дал бы «существование пути», даже когда сравнение ложно — это не соответствует ожидаемой семантике. Сравнения нужно выносить в `JSON_VALUE` или в фильтр `? (...)`.

### JSON_VALUE {#json-value}

Извлечение скалярного значения с обязательным `RETURNING <тип>`.

**Поддерживаемые типы `RETURNING`:** `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `String`, `Utf8`, `Bool`.

**Разрешено:**

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
WHERE JSON_VALUE(doc, '$.x ? (@ == $v)' RETURNING Int64 PASSING 42 AS v) = 10

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

-- Без RETURNING (по умолчанию Utf8 — некорректные сравнения с числами)
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

Для проверки «значение равно `false`» или «путь отсутствует / равен `null`» используйте JsonPath внутри `JSON_EXISTS`, например `JSON_EXISTS(doc, '$.k == false')` или `JSON_EXISTS(doc, '$.k ? (@ == null)')`, а не `JSON_VALUE(...) IS NULL`.

### Обязательность RETURNING для JSON_VALUE {#returning}

Без `RETURNING` тип результата по умолчанию — `Utf8`, и сравнение с литералами других типов может вести себя неожиданно:

```sql
$doc = Json('["1", 1]');
SELECT JSON_VALUE($doc, '$[0]') == "1";  -- true: строка и строка
SELECT JSON_VALUE($doc, '$[1]') == "1";  -- true: число приводится к строке — ошибка логики
```

В предикатах для JSON-индекса `RETURNING` **обязателен**.

### Семантика AND и OR на уровне SQL {#and-or}

| Контекст | Поведение |
|----------|-----------|
| `AND` | Индексируемые части объединяются. Неиндексируемое условие **игнорируется** для построения токенов, но остаётся в пост-фильтре (например, `JSON_EXISTS(doc,'$.a') AND Data = 'x'`). |
| `OR` | Обе ветки должны быть индексируемы; иначе вся `OR`-группа не использует индекс. |
| `NOT` | Не индексируется. В `AND` выражение с `NOT` отбрасывается из индекса; в `OR` — вся ветка не индексируется. |
| Смешение режимов `AND`/`OR` | При конфликте побеждает режим **OR** (более широкое чтение). |

Пример: условие `JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b') AND other_col = 1` в SQL трактуется как `(JE(a) OR JE(b)) AND (other_col = 1)`; индексируемая часть — `OR` двух `JSON_EXISTS`, фильтр по `other_col` — только пост-фильтр.

### Параметры и PASSING {#parameters}

Поддерживаются:

1. **Прямое сравнение:** `JSON_VALUE(doc, '$.k' RETURNING T) = $p`
2. **PASSING:** `JSON_EXISTS(doc, '$.k ? (@ == $v)' PASSING $p AS v)`
3. **`IN $list`:** один токен пути, значения списка подставляются при выполнении.

Типы параметров: `String`, `Utf8`, `Bool`, целые и вещественные типы из списка поддерживаемых `RETURNING`.

Целые литералы с модулем больше 2⁵³ не кодируются в токен точно (ограничение представления `double`); такой литерал не расширяет токен суффиксом — остаётся только путь, уточнение выполняет пост-фильтр.

## Примеры сценариев {#examples}

### Каталог с вложенными атрибутами

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

DECLARE $user_id AS Int64;

SELECT `id`, `payload`
FROM `Documents` VIEW `json_idx`
WHERE JSON_VALUE(`payload`, '$.owner_id' RETURNING Int64) = $user_id
  AND JSON_EXISTS(`payload`, '$.archived ? (@ == false)');
```

### Проверка типа поля

SELECT `id`
FROM `Documents` VIEW `json_idx`
WHERE JSON_VALUE(`payload`, '$.data.type()' RETURNING Utf8) = "array"u;
```

Метод `.type()` завершает построение токена на пути `$.data`; проверка строки `"array"` выполняется пост-фильтром.

## Сравнение с полнотекстовым индексом {#vs-fulltext}

| Критерий | JSON-индекс | Полнотекстовый индекс |
|---|-------------|------------------------|
| Колонка | `Json` / `JsonDocument` | `String` / `Utf8` |
| Запрос | `JSON_EXISTS` / `JSON_VALUE` + JsonPath | `FulltextMatch`, `FulltextScore`, `LIKE` (с N-граммами) |
| Семантика | Структурированные пути и скаляры JSON | Слова, фразы, подстроки текста |
| `VIEW` | Обязателен для гарантированного использования; автовыбор опционален | Обязателен (автовыбора нет) |
| Настройки `WITH` | Нет | Токенизатор, фильтры, N-граммы и др. |
| `COVER` | Нет | Да |

Подробнее о полнотекстовых индексах: [Полнотекстовые индексы](fulltext-indexes.md).

## Ограничения и типичные ошибки {#limitations}

- **Один JSON-индекс на колонку** в предикате: нельзя объединить в одном индексном чтении `JSON_EXISTS(col_a, ...)` и `JSON_EXISTS(col_b, ...)`, даже если на обеих колонках есть индексы.
- **Отрицание** (`NOT`, `!` в JsonPath) не поддерживается для индексной выборки.
- **`JSON_VALUE(..., '$.k' RETURNING Bool) = false`** и **`IS NULL`** намеренно не индексируются (легко получить неверную семантику «все строки без пути»).
- **Кросс-колоночные** сравнения двух `JSON_VALUE` — ошибка.
- **Вложенные** `JSON_*` в аргументах — не индексируются.
- **BATCH**-операции над таблицей с JSON-индексом запрещены.
- Функция выключена по умолчанию — при попытке использования возвращается сообщение `JSON index support is disabled`, для включения нужно установить флаг `EnableJsonIndex`.

## Справочник: итоговая таблица поведения {#summary}

| Ситуация | Результат |
|----------|-----------|
| Подходящий предикат на индексированной колонке + `VIEW` | Индекс используется, затем пост-фильтр |
| Подходящий предикат, автовыбор включён, нет другого индекса | Индекс подставляется автоматически |
| Неподходящий предикат + `VIEW` | Ошибка |
| Неподходящий фрагмент в `AND` | Фрагмент пропускается при построении токенов; остальное индексируется |
| Неподходящий фрагмент в `OR` | Индекс для этой `OR`-группы не используется |
| Условие на неиндексированной колонке в `AND` | Игнорируется при индексации, участвует в пост-фильтре |
| Пустое множество токенов из JsonPath | Ошибка |

## Связанные материалы {#see-also}

- [Функции для работы с JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, синтаксис JsonPath.
- [Вторичные индексы](secondary-indexes.md) — общие сведения о глобальных индексах и `VIEW`.
- [Полнотекстовые индексы](fulltext-indexes.md) — родственный механизм инвертированного поиска по токенам.
