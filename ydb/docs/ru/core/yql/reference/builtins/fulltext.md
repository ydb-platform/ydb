# Полнотекстовый поиск

Функции `FulltextMatch` и `FulltextScore` предназначены для выполнения полнотекстового поиска в {{ ydb-short-name }} **через полнотекстовый индекс**.

{% note info %}

Эти функции используют полнотекстовый индекс только при выполнении запроса через `VIEW IndexName`. См. [{#T}](../syntax/select/fulltext_index.md) и [{#T}](../../../dev/fulltext-indexes.md).

{% endnote %}

## FulltextMatch {#fulltext-match}

`FulltextMatch(text, query)` фильтрует строки по совпадению текста с полнотекстовым запросом.

```yql
SELECT id, body
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "машинное обучение");
```

Только первые два аргумента могут быть позиционными. Дополнительные параметры нужно передавать как **именованные аргументы**.

Поддерживаемые именованные аргументы:

* `Mode` (String): вид запроса — `"Keywords"` (по умолчанию), `"Query"`, `"Wildcard"`
* `DefaultOperator` (String): логика для режима `"Keywords"` — `"And"` или `"Or"` (по умолчанию `"And"`)
* `MinimumShouldMatch` (String): когда `DefaultOperator` установлен в `"Or"`, минимальное число совпавших термов (абсолютное, например `"3"`, или процент, например `"50%"`)

Значения аргументов `Mode` и `DefaultOperator` регистронезависимы.

### Пример для Wildcard {#wildcard}

Если индекс создан с фильтрацией N-грамм, можно использовать шаблоны с `%` и `_` (по аналогии с `LIKE`).

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "маш% обу%ние", "Wildcard" AS Mode)
ORDER BY id;
```

### Пример для Keywords {#keywords}

Запрос из 8 слов, где требуется совпадение не менее половины слов:

```yql
SELECT id
FROM articles VIEW ft_idx
WHERE FulltextMatch(
  body,
  "машинное обучение нейронные сети глубокое обучение большие данные рекомендации поиск",
  "Keywords" AS Mode,
  "Or" AS DefaultOperator,
  "50%" AS MinimumShouldMatch
);
```

В этом примере `Mode` установлен в `"Keywords"` (запрос трактуется как набор термов), `DefaultOperator` установлен в `"Or"` (разрешается совпадение по любому подмножеству термов), а `MinimumShouldMatch` установлен в `"50%"` (требуется совпадение минимум 4 термов из 8). Это помогает отсеять документы, где встретилось лишь одно-два слова из длинного запроса.

## FulltextScore {#fulltext-score}

`FulltextScore(text, query)` возвращает оценку релевантности на основе алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) и может использоваться для ранжирования результатов.

Требует индекс типа `fulltext_relevance`.

```yql
SELECT id, FulltextScore(body, "быстрый лис") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "быстрый лис") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Только первые два аргумента могут быть позиционными. Дополнительные параметры нужно передавать как **именованные аргументы**.

Поддерживаемые именованные аргументы:

* `DefaultOperator` (String): `"And"` или `"Or"` (по умолчанию `"And"`)
* `MinimumShouldMatch` (String): когда `DefaultOperator` установлен в `"Or"` — минимальное число совпавших термов (абсолютное или процент)
* `K1` (Double): параметр \(k_1\) алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)
* `B` (Double): параметр \(b\) алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)

Пример:

```yql
SELECT id,
       FulltextScore(body, "быстрый лис", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "быстрый лис", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) > 0
ORDER BY relevance DESC;
```

{% note info %}

`MinimumShouldMatch` поддерживается только когда `DefaultOperator` установлен в `"Or"`. Когда `DefaultOperator` установлен в `"And"`, используйте `FulltextScore` без `MinimumShouldMatch`.

{% endnote %}
