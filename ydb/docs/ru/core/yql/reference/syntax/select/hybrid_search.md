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

## Ограничения {#limitations}

* `HybridRank(...)` должен быть единственным ключом `ORDER BY`; его нельзя отрицать, вкладывать в большее выражение или комбинировать с другими ключами сортировки.
* Требуется не менее двух оценивающих аргументов — одна ветвь не является гибридным запросом.
* `LIMIT` должен быть литералом (он задаёт размеры пулов кандидатов по ветвям). Чтобы использовать параметризованный `LIMIT`, передайте явный `AS Limits`.
* [Префиксные векторные индексы](../../../../dev/vector-indexes.md) пока не поддерживаются.
