# VIEW (Fulltext index)

To select data from a row-oriented table using a [fulltext index](../../../../dev/fulltext-indexes.md), use the `VIEW` expression:

```yql
SELECT ...
FROM TableName VIEW IndexName
WHERE FulltextMatch(TextColumn, "query")
ORDER BY ...
```

{% note info %}

A fulltext index isn't automatically selected by the [optimizer](../../../../concepts/glossary.md#optimizer) and must be specified explicitly using `VIEW IndexName`.

Fulltext search functions (`FulltextMatch`, `FulltextScore`) require `VIEW`. If `VIEW` isn't used, the query fails.

Only one fulltext predicate is supported per read through `VIEW`. `FulltextMatch` / `FulltextScore` can't be used under `OR` or `NOT`.
For relevance access, you must include `FulltextScore(...) > 0` in `WHERE`.

For function details, see [{#T}](../../builtins/fulltext.md), including
[`FulltextMatch`](../../builtins/fulltext.md#fulltext-match) and
[`FulltextScore`](../../builtins/fulltext.md#fulltext-score).

{% endnote %}

## FulltextMatch

`FulltextMatch(text, query)` filters rows by whether the text matches the fulltext query:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "machine learning")
ORDER BY id;
```

### `Wildcard` mode and `%` / `_` patterns (requires n-grams)

If the index is created with n-gram filtering, you can use `%` and `_` patterns similar to `LIKE`.
To explicitly enable this behavior, pass `"Wildcard" AS Mode` as a named argument:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "mach% learn%", "Wildcard" AS Mode)
ORDER BY id;
```

### LIKE / ILIKE (use the fulltext index)

For fulltext indexes with n-grams, `LIKE`/`ILIKE` predicates over the indexed text column use the same logic as `FulltextMatch(..., "Wildcard" AS Mode)`:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%learn%ing%"
ORDER BY id;
```

## FulltextScore ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance)

`FulltextScore(text, query)` returns a relevance score and can be used for ranking.
Relevance scoring requires the `fulltext_relevance` index type.

```yql
SELECT id, title, FulltextScore(body, "quick fox") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "quick fox") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Optional parameters

Additional parameters must be passed as **named arguments**.

* `DefaultOperator` (String): `"And"` or `"Or"`
* `MinimumShouldMatch` (String): when `DefaultOperator` is set to `"Or"`, minimum number of matched terms (absolute like `"1"` or percent like `"50%"`)
* `K1` (Double): [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) K1 parameter
* `B` (Double): [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) B parameter

Example:

```yql
SELECT id, FulltextScore(body, "quick fox", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "quick fox", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) > 0
ORDER BY relevance DESC;
```

{% note info %}

Only the first two arguments of `FulltextMatch` / `FulltextScore` can be positional. Use named arguments for all additional parameters.

{% endnote %}
