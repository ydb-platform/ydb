# Fulltext search

The `FulltextMatch` and `FulltextScore` functions are intended for fulltext search in {{ ydb-short-name }} **via a fulltext index**.

{% note info %}

These functions use a fulltext index only when the query is executed with `VIEW IndexName`. See [{#T}](../syntax/select/fulltext_index.md) and [{#T}](../../../dev/fulltext-indexes.md).

{% endnote %}

## FulltextMatch {#fulltext-match}

`FulltextMatch(text, query)` filters rows by whether the text matches a fulltext query.

```yql
SELECT id, body
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "machine learning");
```

Only the first two arguments can be positional. Additional parameters must be passed as **named arguments**.

Supported named arguments:

* `Mode` (String): query mode — `"Keywords"` (default), `"Query"`, `"Wildcard"`
* `DefaultOperator` (String): operator for `"Keywords"` mode — `"And"` or `"Or"` (default `"And"`)
* `MinimumShouldMatch` (String): when `DefaultOperator` is set to `"Or"`, minimum number of matched terms (absolute like `"3"` or percent like `"50%"`)

`Mode` and `DefaultOperator` values are case-insensitive.

### `Wildcard` example {#wildcard}

If the index is created with n-gram filtering, you can use `%` and `_` patterns similar to `LIKE`.

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "mach% learn%", "Wildcard" AS Mode)
ORDER BY id;
```

### MinimumShouldMatch example {#minimum-should-match-example}

An 8-term query where at least half of the terms must match:

```yql
SELECT id
FROM articles VIEW ft_idx
WHERE FulltextMatch(
  body,
  "machine learning neural networks deep learning large scale data recommendations search",
  "Keywords" AS Mode,
  "Or" AS DefaultOperator,
  "50%" AS MinimumShouldMatch
);
```

In this example, `Mode` is set to `"Keywords"` (treating the query as a set of terms), `DefaultOperator` is set to `"Or"` (allowing matching any subset of terms), and `MinimumShouldMatch` is set to `"50%"` (requiring at least 4 out of 8 terms to match). This helps filter out documents that contain only one or two words from a long query.

## FulltextScore {#fulltext-score}

`FulltextScore(text, query)` returns a relevance score based on the [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) algorithm and can be used for ranking results.

Requires the `fulltext_relevance` index type.

```yql
SELECT id, FulltextScore(body, "quick fox") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "quick fox") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Only the first two arguments can be positional. Additional parameters must be passed as **named arguments**.

Supported named arguments:

* `DefaultOperator` (String): `"And"` or `"Or"` (default `"And"`)
* `MinimumShouldMatch` (String): when `DefaultOperator` is set to `"Or"`, minimum number of matched terms (absolute or percentage)
* `K1` (Double): [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) \(k_1\) parameter
* `B` (Double): [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) \(b\) parameter

Example:

```yql
SELECT id,
       FulltextScore(body, "quick fox", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "quick fox", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) > 0
ORDER BY relevance DESC;
```

{% note info %}

`MinimumShouldMatch` is supported only when `DefaultOperator` is set to `"Or"`. When `DefaultOperator` is set to `"And"`, use `FulltextScore` without `MinimumShouldMatch`.


{% endnote %}
