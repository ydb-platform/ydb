# VIEW (Полнотекстовый индекс)

Для выполнения запроса `SELECT` с использованием [полнотекстового индекса](../../../../dev/fulltext-indexes.md) в строковой таблице используйте выражение `VIEW`:

```yql
SELECT ...
FROM TableName VIEW IndexName
WHERE FulltextMatch(TextColumn, "query")
ORDER BY ...
```

{% note info %}

Полнотекстовый индекс не будет автоматически выбран [оптимизатором](../../../../concepts/glossary.md#optimizer), поэтому его нужно указывать явно с помощью `VIEW IndexName`.

Функции полнотекстового поиска (`FulltextMatch`, `FulltextScore`) требуют `VIEW`. Если `VIEW` не используется, запрос завершится с ошибкой.

Несколько предикатов `FulltextMatch` с настройками по умолчанию можно объединять через `AND`. В одном чтении нельзя смешивать `FulltextMatch` и `FulltextScore` или использовать несколько различных предикатов `FulltextScore`. `FulltextMatch` / `FulltextScore` нельзя использовать под `OR` или `NOT`.
Для доступа к индексу по релевантности требуется ограничение `FulltextScore(...) > 0` в `WHERE`.

Описание функций см. в разделе [{#T}](../../builtins/fulltext.md), включая
[FulltextMatch](../../builtins/fulltext.md#fulltext-match) и
[FulltextScore](../../builtins/fulltext.md#fulltext-score).

{% endnote %}

## FulltextMatch

[FulltextMatch(text, query)](../../builtins/fulltext.md#fulltext-match) фильтрует строки по совпадению текста с полнотекстовым запросом:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "машинное обучение")
LIMIT 20;
```

Только первые два аргумента могут быть позиционными. Дополнительные параметры нужно передавать как **именованные аргументы**:

* `Mode` (String): вид запроса:
  * `Keywords` (по умолчанию) — текст запроса разбивается на отдельные термы; логика их объединения определяется `DefaultOperator`
  * `Wildcard` — поиск с подстановочными символами: `%` заменяет любую подстроку, `_` — один символ (аналогично `LIKE`); требует N-граммного индекса
* `DefaultOperator` (String): оператор объединения термов в режиме `Keywords`:
  * `And` (по умолчанию) — все термы запроса должны присутствовать в тексте
  * `Or` — термы делятся на **обязательные** (с префиксом `+`, должны присутствовать в каждом результате) и **необязательные** (без префикса, учитываются в `MinimumShouldMatch`). Без префикса `+` все термы считаются необязательными, и достаточно совпадения хотя бы одного (см. [пример](../../builtins/fulltext.md#required-term-example))
* `MinimumShouldMatch` (String): минимальное число **необязательных** термов, которые должны совпасть при `DefaultOperator = "Or"` — задаётся как абсолютное число (например, `"3"`) или процент от числа необязательных термов (например, `"50%"`). Обязательные (`+`) термы в счёт не входят

### Режим `Wildcard` и шаблоны `%` / `_` (требуются N-граммы)

Если индекс создан с фильтрацией N-грамм, можно использовать шаблоны с `%` и `_` (по аналогии с `LIKE`).
Чтобы явно указать такой режим, передайте именованный аргумент `"Wildcard" AS Mode`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "маш% обу%ние", "Wildcard" AS Mode)
LIMIT 20;
```

## LIKE / ILIKE {#like-ilike}

Для поиска по шаблону используйте N-граммный индекс. `LIKE` учитывает регистр, а `ILIKE` — нет:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%обуч%ние%"
LIMIT 20;
```

Для поиска через полнотекстовый индекс по `ILIKE` требуется `use_filter_lowercase=true`.

Шаблон можно передать параметром или составить из параметров. Он не должен зависеть от строк таблицы или меняться во время выполнения запроса. Поддерживается [`ESCAPE`](../expressions.md#check-match) для поиска буквальных `%` и `_`.

Пример поиска подстроки с экранированием:

```yql
DECLARE $pattern AS String;

SELECT id, title
FROM articles VIEW ft_idx
WHERE body LIKE "%" || $pattern || "%" ESCAPE "!"
LIMIT 20;
```

При `$pattern = "abc!%def"` запрос ищет буквальную подстроку `abc%def`.

## Равенство, StartsWith и EndsWith {#string-predicates}

N-граммный индекс также поддерживает равенство по текстовой колонке и [StartsWith / EndsWith](../../builtins/basic.md#starts_ends_with). Эти условия учитывают регистр; `%` и `_` в них означают обычные символы.

Примеры поиска по всему тексту, началу и концу строки:

```yql
DECLARE $text AS String;

SELECT id FROM articles VIEW ft_idx WHERE body = $text;
SELECT id FROM articles VIEW ft_idx WHERE StartsWith(body, $text);
SELECT id FROM articles VIEW ft_idx WHERE EndsWith(body, $text);
```

## Поиск по коротким шаблонам {#scan-fallback}

Для шаблона `%at%` индекс с `filter_ngram_min_length=3` не подходит: фрагмент `at` содержит только два символа. В таком случае {{ ydb-short-name }} ищет совпадения в основной таблице, что может быть медленнее.

Этот подход согласуется с [частичными индексами](https://en.wikipedia.org/wiki/Partial_index), которые хранят только часть строк: если индекс не позволяет выполнить поиск, запрос обращается к таблице.

Переход к сканированию доступен для строковых условий (`LIKE`, `ILIKE`, равенства, `StartsWith`, `EndsWith`) и `FulltextMatch` в режиме `Wildcard`.
Если запрос использует поиск по словам (`Keywords`) или оценку релевантности (`FulltextScore`), такой переход не выполняется.

## FulltextScore

[FulltextScore(text, query)](../../builtins/fulltext.md#fulltext-score) возвращает оценку релевантности ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) и может использоваться для ранжирования.
Ранжирование требует индекса типа [fulltext_relevance](../../../../dev/fulltext-indexes.md#relevance).

```yql
SELECT id, title, FulltextScore(body, "машинное обучение") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "машинное обучение") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Дополнительные параметры

Дополнительные параметры нужно передавать как **именованные аргументы**:

* `DefaultOperator` (String): оператор объединения термов — `And` (по умолчанию, все термы должны присутствовать) или `Or` (термы делятся на **обязательные** с префиксом `+` и **необязательные** без префикса; каждый обязательный должен присутствовать, а из необязательных должны совпасть не менее `MinimumShouldMatch`)
* `MinimumShouldMatch` (String): при `DefaultOperator = "Or"` — минимальное число **необязательных** термов, которые должны совпасть; задаётся как абсолютное число (например, `"2"`) или процент от числа необязательных термов (например, `"50%"`). Обязательные (`+`) термы в счёт не входят
* `K1` (Double или Float): параметр насыщения частоты терма в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — определяет, насколько сильно влияет повторное появление терма на итоговую оценку; по умолчанию `1.2`
* `B` (Double или Float): параметр нормализации длины документа в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — `0.0` отключает нормализацию, `1.0` полностью нормализует по длине документа; по умолчанию `0.75`

Пример:

```yql
SELECT id, FulltextScore(body, "машинное обучение", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "машинное обучение", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) > 0
ORDER BY relevance DESC;
```

{% note info %}

Только первые два аргумента `FulltextMatch` / `FulltextScore` могут быть позиционными. Для дополнительных параметров используйте именованные аргументы.

Выражение `FulltextScore(...)` повторяется в `SELECT` и `WHERE` целиком — YQL, как и стандартный SQL, вычисляет `WHERE` раньше `SELECT`, поэтому псевдонимы из `SELECT` недоступны в `WHERE`. Оба вхождения должны быть идентичны.

{% endnote %}
