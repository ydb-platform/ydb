# Гибридный поиск (HybridRank)

[Гибридный поиск](../../../../dev/hybrid-search.md) объединяет [полнотекстовое](../../../../dev/fulltext-indexes.md) и [векторное](../../../../dev/vector-indexes.md) ранжирование в едином результате. Гибридный запрос — это `SELECT` по основной таблице, у которого ключ `ORDER BY` представляет собой единственный вызов `HybridRank`:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector))
LIMIT 10;
```

{% note info %}

Гибридный запрос читает данные из основной таблицы и **не** использует `VIEW IndexName`: индекс каждой ветви определяется по соответствующему аргументу `HybridRank`.

Запросу требуются оба индекса: [fulltext_relevance](../../../../dev/fulltext-indexes.md#relevance) по колонке из `FullTextScore` и непрефиксный [vector_kmeans_tree](../../../../dev/vector-indexes.md) по колонке из `Knn`. Полнота векторной ветви управляется параметром [KMeansTreeSearchTopSize](vector_index.md#KMeansTreeSearchTopSize).

{% endnote %}

## HybridRank {#hybrid-rank}

Общая форма функции:

```yql
HybridRank(
    score1, score2 [, ... scoreN]        -- 2+ оценивающих выражения: FullTextScore(...) или Knn::<Distance|Similarity>(...)
    [, "rrf" | "linear"  AS Mode]        -- способ объединения (по умолчанию "rrf")
    [, (w1, w2, ...)     AS Weights]     -- веса ветвей (по умолчанию 1.0)
    [, k                 AS K]           -- константа RRF (по умолчанию 60.0)
    [, true | false      AS Normalize]   -- только для режима "linear": min-max нормализация (по умолчанию true)
    [, (idx1, idx2, ...) AS Indexes]     -- имена индексов по ветвям (по умолчанию: автоопределение)
    [, (lim1, lim2, ...) AS Limits]      -- размеры пулов кандидатов по ветвям (по умолчанию: LIMIT * 10)
    [, ($x) -> {...}     AS RankLambda]  -- пользовательское объединение по рангам ветвей (заменяет встроенный Mode)
    [, ($x) -> {...}     AS ScoreLambda] -- пользовательское объединение по исходным оценкам ветвей (заменяет встроенный Mode)
)
```

`HybridRank` может использоваться только как единственный ключ сортировки в секции `ORDER BY`. Он принимает **два или более оценивающих выражения** (позиционно), за которыми следуют необязательные **именованные аргументы**.

Каждое оценивающее выражение — это одна *ветвь* объединения, классифицируемая по своей форме:

* выражение [FullTextScore(text, query)](../../builtins/fulltext.md#fulltext-score) — **полнотекстовая** ветвь, разрешается через индекс `fulltext_relevance` по колонке `text`;
* [Knn](../../udf/list/knn.md)-расстояние или сходство по колонке (например, `Knn::CosineDistance(embedding, $queryVector)` или `Knn::CosineSimilarity(embedding, $queryVector)`) — **векторная** ветвь, разрешается через индекс `vector_kmeans_tree` по этой колонке.

Порядок аргументов не имеет значения, а ветвей может быть больше двух (например, полнотекстовая ветвь и две векторные). Каждая ветвь извлекает собственный пул кандидатов и вносит один член в объединённую оценку каждого документа.

### Режимы объединения {#modes}

Через аргумент `Mode` доступны два способа объединения:

* **`rrf`** (по умолчанию) — [Reciprocal Rank Fusion](https://learn.microsoft.com/azure/search/hybrid-search-ranking#how-rrf-ranking-works). Каждая ветвь вносит вклад `weight / (K + rank)`, где `rank` — позиция документа внутри этой ветви. Так как RRF использует ранги, а не исходные оценки, несопоставимые величины оценок ветвей не имеют значения.
* **`linear`** — взвешенная сумма оценок ветвей: сумма `weight * score` по всем ветвям. По умолчанию оценки ветвей нормализуются методом min-max в диапазон `[0, 1]` перед суммированием; задайте `Normalize` равным `false`, чтобы объединять исходные оценки.

В обоих режимах документ, отсутствующий в пуле кандидатов ветви, просто не получает вклада этой ветви.

### Пользовательская лямбда объединения {#custom-fusion}

Вместо встроенного `Mode` (`rrf`/`linear`) можно передать собственную лямбду объединения, которая вычисляет итоговую оценку из значений ветвей. Доступны два взаимоисключающих вида лямбд:

* `RankLambda` — лямбда получает ранги документа по ветвям в виде `Dict<Int64, Int64>` (индекс ветви → 1-based ранг внутри этой ветви). Если документ отсутствует в ветви, соответствующей записи нет, поэтому `$ranks[i]` равно `NULL`.
* `ScoreLambda` — лямбда получает исходные оценки документа по ветвям в виде `Dict<Int64, Double>` (индекс ветви → значение оценки ветви: релевантность полнотекстового поиска или векторное расстояние/сходство). Если документ отсутствует в ветви, соответствующей записи нет, поэтому `$scores[i]` равно `NULL`.

Лямбда принимает один аргумент (словарь) и должна возвращать числовое значение (рекомендуется `Double`); чем больше значение, тем выше позиция в ранжировании. Лямбда заменяет встроенное объединение, поэтому её нельзя комбинировать с `Mode`, `Weights`, `K` или `Normalize` — перенесите все веса и константы в тело лямбды.

```yql
SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($ranks) -> {
        RETURN 1.0 / (60 + COALESCE($ranks[0], 100000))
             + 1.0 / (60 + COALESCE($ranks[1], 100000));
    } AS RankLambda)
LIMIT 10;
```

### Именованные аргументы {#named-args}

Все необязательные параметры передаются как именованные аргументы. Аргументы `Weights`, `Limits` и `Indexes` — это кортежи, позиционно соответствующие оценивающим выражениям: в них должно быть ровно по одному элементу на ветвь.

| Аргумент | Тип | Описание |
|----------|-----|----------|
| `Mode` | String | Способ объединения: `"rrf"` (по умолчанию) или `"linear"`. Регистр не учитывается (`"RRF"` / `"Linear"` также допустимы). |
| `Weights` | Кортеж чисел | Вес каждой ветви, по одному на оценивающий аргумент. По умолчанию `1.0` для каждой ветви. Вес `0` отключает ветвь. |
| `K` | Double | Константа RRF (только режим `rrf`). По умолчанию `60.0`. Большие значения сглаживают влияние верхних рангов. |
| `Normalize` | Bool | Только режим `linear`. При `true` (по умолчанию) оценки ветвей нормализуются методом min-max перед суммированием; при `false` объединяются исходные оценки. |
| `Indexes` | Кортеж строк | Явное имя индекса для каждой ветви вместо автоопределения. Требуется, когда колонке ветви соответствует более одного подходящего индекса. |
| `Limits` | Кортеж положительных целых | Явный размер пула кандидатов для каждой ветви. По умолчанию каждая ветвь извлекает `LIMIT * HybridSearchFactor` кандидатов (фактор по умолчанию `10`). |
| `RankLambda` | Лямбда | Пользовательское объединение по рангам ветвей. Лямбда принимает `Dict<Int64, Int64>` (индекс ветви → 1-based ранг; для отсутствующих ветвей записи нет) и возвращает числовую оценку. Заменяет встроенный `Mode`; несовместим с `Mode`, `Weights`, `K` и `Normalize`. |
| `ScoreLambda` | Лямбда | Пользовательское объединение по исходным оценкам ветвей. Лямбда принимает `Dict<Int64, Double>` (индекс ветви → исходная оценка; для отсутствующих ветвей записи нет) и возвращает числовую оценку. Заменяет встроенный `Mode`; несовместим с `Mode`, `Weights`, `K` и `Normalize`. |

### Примеры {#examples}

Взвешенный RRF — смещение ранжирования в сторону векторной ветви:

```yql
$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    (1, 2) AS Weights)
LIMIT 10;
```

RRF по трём ветвям — комбинация текстовой релевантности с двумя векторными ветвями по разным колонкам эмбеддингов:

```yql
$queryText = "машинное обучение";
$titleVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);
$bodyVector = Knn::ToBinaryStringFloat([0.7, 0.8, 0.9, 1.0]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(title_embedding, $titleVector),
    Knn::CosineDistance(body_embedding, $bodyVector),
    (1, 2, 3) AS Weights)
LIMIT 10;
```

Линейное объединение нормализованных оценок:

```yql
$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    "linear" AS Mode)
LIMIT 10;
```

Явные имена индексов и размеры пулов кандидатов. Так как размеры пулов по ветвям заданы через `Limits`, собственный `LIMIT` запроса больше не обязан быть литералом и может быть параметром:

```yql
DECLARE $limit AS Uint64;

$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ("ft_idx", "vec_idx") AS Indexes,
    (100, 200) AS Limits)
LIMIT $limit;
```

Пользовательский `RankLambda` — RRF вручную, с весом векторной ветви в 100 раз больше текстовой:

```yql
$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($ranks) -> {
        RETURN   1.0 / (60 + COALESCE($ranks[0], 100000))
             + 100.0 / (60 + COALESCE($ranks[1], 100000));
    } AS RankLambda)
LIMIT 10;
```

Пользовательский `ScoreLambda` — комбинация исходной релевантности полнотекстового поиска и векторного расстояния со знаком минус (ближе = больше оценка):

```yql
$queryText = "машинное обучение";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($scores) -> {
        RETURN COALESCE($scores[0], 0.0) - COALESCE($scores[1], 1000000.0);
    } AS ScoreLambda)
LIMIT 10;
```

## Ограничения {#limitations}

* `HybridRank(...)` должен быть единственным ключом `ORDER BY`; его нельзя отрицать, вкладывать в большее выражение или комбинировать с другими ключами сортировки.
* Требуется не менее двух оценивающих аргументов — одна ветвь не является гибридным запросом.
* `LIMIT` должен быть литералом (он задаёт размеры пулов кандидатов по ветвям). Чтобы использовать параметризованный `LIMIT`, передайте явный `AS Limits`.
* Пользовательская лямбда объединения (`RankLambda` или `ScoreLambda`) заменяет встроенное объединение и не может комбинироваться с `Mode`, `Weights`, `K` или `Normalize`. Можно указать не более одного из `RankLambda` или `ScoreLambda`.
* [Префиксные векторные индексы](../../../../dev/vector-indexes.md) пока не поддерживаются.
