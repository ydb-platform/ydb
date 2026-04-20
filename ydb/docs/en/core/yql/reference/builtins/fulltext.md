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

Supports named arguments:

* `Mode` (String): query mode:
  * `Keywords` (default) — the query is split into individual terms; how they are combined is determined by `DefaultOperator`
  * `Query` — extended syntax with logical operators: required terms via `+`, excluded terms via `-`, exact phrases in double quotes
  * `Wildcard` — wildcard search: `%` matches any substring, `_` matches a single character (similar to `LIKE`); requires an n-gram index
* `DefaultOperator` (String): term combination operator in `Keywords` mode:
  * `And` (default) — all query terms must be present in the text
  * `Or` — matching at least one term is sufficient; use `MinimumShouldMatch` to set a minimum threshold
* `MinimumShouldMatch` (String): minimum number of matched terms when `DefaultOperator = "Or"` — specified as an absolute number (for example, `"3"`) or a percentage of query terms (for example, `"50%"`)

`Mode` and `DefaultOperator` values are case-insensitive.

### `Wildcard` example {#wildcard}

If the index is created with n-gram filtering, you can use `%` and `_` patterns similar to `LIKE`.

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "mach% learn%", "Wildcard" AS Mode)
LIMIT 20;
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

In this example, `Mode` is set to `Keywords` (treating the query as a set of terms), `DefaultOperator` is set to `Or` (allowing matching any subset of terms), and `MinimumShouldMatch` is set to `"50%"` (requiring at least 4 out of 8 terms to match). This helps filter out documents that contain only one or two words from a long query.

## FulltextScore {#fulltext-score}

`FulltextScore(text, query)` returns a relevance score based on the [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) algorithm and can be used for ranking results.

Requires the `fulltext_relevance` index type.

```yql
SELECT id, FulltextScore(body, "machine learning") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "machine learning") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Only the first two arguments can be positional. Additional parameters must be passed as **named arguments**.

Supports named arguments:

* `DefaultOperator` (String): term combination operator — `And` (default, all terms must be present) or `Or` (at least one term must match)
* `MinimumShouldMatch` (String): when `DefaultOperator = "Or"`, minimum number of matched terms — specified as an absolute number (for example, `"2"`) or a percentage (for example, `"50%"`)
* `K1` (Double): term frequency saturation parameter in [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — controls how strongly repeated occurrences of a term affect the score; typical range: 1.2–2.0
* `B` (Double): document length normalization parameter in [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — `0.0` disables normalization, `1.0` fully normalizes by document length; typical value: 0.75

Example:

```yql
SELECT id,
       FulltextScore(body, "machine learning", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "machine learning", "Or" AS DefaultOperator, "1" AS MinimumShouldMatch, 0.75 AS K1, 1.2 AS B) > 0
ORDER BY relevance DESC;
```

{% note info %}

`MinimumShouldMatch` is supported only when `DefaultOperator` is set to `Or`. When `DefaultOperator` is set to `And`, use `FulltextScore` without `MinimumShouldMatch`.


{% endnote %}
