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
  * `Query` — расширенный синтаксис с логическими операторами: обязательные термы через `+`, исключённые через `-`, точные фразы в двойных кавычках
  * `Wildcard` — поиск с подстановочными символами: `%` заменяет любую подстроку, `_` — один символ (аналогично `LIKE`); требует N-граммного индекса
* `DefaultOperator` (String): оператор объединения термов в режиме `Keywords`:
  * `And` (по умолчанию) — все термы запроса должны присутствовать в тексте
  * `Or` — достаточно совпадения хотя бы одного терма; для уточнения минимума используйте `MinimumShouldMatch`
* `MinimumShouldMatch` (String): минимальное число совпавших термов при `DefaultOperator = "Or"` — задаётся как абсолютное число (например, `"3"`) или процент от числа термов запроса (например, `"50%"`)

### Режим `Wildcard` и шаблоны `%` / `_` (требуются N-граммы)

Если индекс создан с фильтрацией N-грамм, можно использовать шаблоны с `%` и `_` (по аналогии с `LIKE`).
Чтобы явно указать такой режим, передайте именованный аргумент `"Wildcard" AS Mode`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "маш% обу%ние", "Wildcard" AS Mode)
LIMIT 20;
```

### LIKE / ILIKE (используют полнотекстовый индекс)

Для полнотекстовых индексов с N-граммами поддерживается `LIKE`/`ILIKE` по текстовой колонке. Такие предикаты используют ту же логику, что и `FulltextMatch(..., "Wildcard" AS Mode)`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%обуч%ние%"
LIMIT 20;
```

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

* `DefaultOperator` (String): оператор объединения термов — `And` (по умолчанию, все термы должны присутствовать) или `Or` (достаточно одного совпавшего терма)
* `MinimumShouldMatch` (String): при `DefaultOperator = "Or"` — минимальное число совпавших термов; задаётся как абсолютное число (например, `"2"`) или процент (например, `"50%"`)
* `K1` (Double): параметр насыщения частоты терма в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — определяет, насколько сильно влияет повторное появление терма на итоговую оценку; типичный диапазон: 1.2–2.0
* `B` (Double): параметр нормализации длины документа в [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — `0.0` отключает нормализацию, `1.0` полностью нормализует по длине документа; типичное значение: 0.75

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
