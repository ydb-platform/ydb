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

В одном чтении через `VIEW` поддерживается только один полнотекстовый предикат. `FulltextMatch` / `FulltextScore` нельзя использовать под `OR` или `NOT`.
Для доступа к индексу по релевантности требуется ограничение `FulltextScore(...) > 0` в `WHERE`.

Описание функций см. в разделе [{#T}](../../builtins/fulltext.md), включая
[`FulltextMatch`](../../builtins/fulltext.md#fulltext-match) и
[`FulltextScore`](../../builtins/fulltext.md#fulltext-score).

{% endnote %}

## FulltextMatch

`FulltextMatch(text, query)` фильтрует строки по совпадению текста с полнотекстовым запросом:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "машинное обучение")
ORDER BY id;
```

### Режим `Wildcard` и шаблоны `%` / `_` (требуются N-граммы)

Если индекс создан с фильтрацией N-грамм, можно использовать шаблоны с `%` и `_` (по аналогии с `LIKE`).
Чтобы явно указать такой режим, передайте именованный аргумент `"Wildcard" AS Mode`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "маш% обу%ние", "Wildcard" AS Mode)
ORDER BY id;
```

### LIKE / ILIKE (используют полнотекстовый индекс)

Для полнотекстовых индексов с N-граммами поддерживается `LIKE`/`ILIKE` по текстовой колонке. Такие предикаты используют ту же логику, что и `FulltextMatch(..., "Wildcard" AS Mode)`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%обуч%ние%"
ORDER BY id;
```

## FulltextScore

`FulltextScore(text, query)` возвращает оценку релевантности и может использоваться для ранжирования.
Ранжирование требует индекса типа `fulltext_relevance`.

```yql
SELECT id, title, FulltextScore(body, "быстрый лис") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "быстрый лис") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Дополнительные параметры

Дополнительные параметры нужно передавать как **именованные аргументы**.

* `DefaultOperator` (String): `"And"` или `"Or"`
* `MinimumShouldMatch` (String): когда `DefaultOperator` установлен в `"Or"` — минимальное число совпавших термов (абсолютное, например `"1"`, или процент, например `"50%"`)
* `K1` (Double): параметр K1 алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)
* `B` (Double): параметр B алгоритма [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)

Пример:

```yql
SELECT id, FulltextScore(body, "быстрый лис", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "быстрый лис", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) > 0
ORDER BY relevance DESC;
```

{% note info %}

Только первые два аргумента `FulltextMatch` / `FulltextScore` могут быть позиционными. Для дополнительных параметров используйте именованные аргументы.

{% endnote %}
