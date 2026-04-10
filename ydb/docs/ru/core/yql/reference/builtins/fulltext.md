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

* `Mode` (String): вид запроса:
  * `Keywords` (по умолчанию) — текст запроса разбивается на отдельные термы; логика их объединения определяется `DefaultOperator`
  * `Query` — расширенный синтаксис с логическими операторами: обязательные термы через `+`, исключённые через `-`, точные фразы в двойных кавычках
  * `Wildcard` — поиск с подстановочными символами: `%` заменяет любую подстроку, `_` — один символ (аналогично `LIKE`); требует N-граммного индекса
* `DefaultOperator` (String): оператор объединения термов в режиме `Keywords`:
  * `And` (по умолчанию) — все термы запроса должны присутствовать в тексте
  * `Or` — достаточно совпадения хотя бы одного терма; для уточнения минимума используйте `MinimumShouldMatch`
* `MinimumShouldMatch` (String): минимальное число совпавших термов при `DefaultOperator = "Or"` — задаётся как абсолютное число (например, `"3"`) или процент от числа термов запроса (например, `"50%"`)

Значения аргументов `Mode` и `DefaultOperator` регистронезависимы.

### Пример для Wildcard {#wildcard}

Если индекс создан с фильтрацией N-грамм, можно использовать шаблоны с `%` и `_` (по аналогии с `LIKE`).

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "маш% обу%ние", "Wildcard" AS Mode)
LIMIT 20;
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

В этом примере `Mode` установлен в `Keywords` (запрос трактуется как набор термов), `DefaultOperator` установлен в `Or` (разрешается совпадение по любому подмножеству термов), а `MinimumShouldMatch` установлен в `50%` (требуется совпадение минимум 4 термов из 8). Это помогает отсеять документы, где встретилось лишь одно-два слова из длинного запроса.

## FulltextScore {#fulltext-score}

`FulltextScore(text, query)` возвращает оценку релевантности на основе алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) и может использоваться для ранжирования результатов.

Требует индекс типа `fulltext_relevance`.

```yql
SELECT id, FulltextScore(body, "машинное обучение") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "машинное обучение") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Только первые два аргумента могут быть позиционными. Дополнительные параметры нужно передавать как **именованные аргументы**.

Поддерживаемые именованные аргументы:

* `DefaultOperator` (String): оператор объединения термов — `And` (по умолчанию, все термы должны присутствовать) или `Or` (достаточно одного совпавшего терма)
* `MinimumShouldMatch` (String): при `DefaultOperator = "Or"` — минимальное число совпавших термов; задаётся как абсолютное число (например, `"2"`) или процент (например, `"50%"`)
* `K1` (Double): параметр насыщения частоты терма в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — определяет, насколько сильно влияет повторное появление терма на итоговую оценку; типичный диапазон: 1.2–2.0
* `B` (Double): параметр нормализации длины документа в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — `0.0` отключает нормализацию, `1.0` полностью нормализует по длине документа; типичное значение: 0.75

Пример:

```yql
SELECT id,
       FulltextScore(body, "машинное обучение", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "машинное обучение", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) > 0
ORDER BY relevance DESC;
```

{% note info %}

`MinimumShouldMatch` поддерживается только когда `DefaultOperator` установлен в `Or`. Когда `DefaultOperator` установлен в `And`, используйте `FulltextScore` без `MinimumShouldMatch`.

{% endnote %}
