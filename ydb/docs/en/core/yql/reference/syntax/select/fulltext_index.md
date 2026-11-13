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

Multiple `FulltextMatch` predicates with default keyword settings can be combined with `AND`. A single read cannot mix `FulltextMatch` and `FulltextScore` or use multiple distinct `FulltextScore` predicates. `FulltextMatch` / `FulltextScore` can't be used under `OR` or `NOT`.
For relevance access, you must include `FulltextScore(...) > 0` in `WHERE`.

For function details, see [{#T}](../../builtins/fulltext.md), including
[FulltextMatch](../../builtins/fulltext.md#fulltext-match) and
[FulltextScore](../../builtins/fulltext.md#fulltext-score).

{% endnote %}

## FulltextMatch

[FulltextMatch(text, query)](../../builtins/fulltext.md#fulltext-match) filters rows by whether the text matches the fulltext query:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "machine learning")
LIMIT 20;
```

Only the first two arguments can be positional. Additional parameters must be passed as **named arguments**:

* `Mode` (String): query mode:
  * `Keywords` (default) — the query is split into individual terms; how they are combined is determined by `DefaultOperator`
  * `Wildcard` — wildcard search: `%` matches any substring, `_` matches a single character (similar to `LIKE`); requires an n-gram index
* `DefaultOperator` (String): term combination operator in `Keywords` mode:
  * `And` (default) — all query terms must be present in the text
  * `Or` — terms split into **required** (prefixed with `+`, must appear in every result) and **optional** (no prefix, count toward `MinimumShouldMatch`). Without any `+` prefix, all terms are optional and at least one must match (see [example](../../builtins/fulltext.md#required-term-example))
* `MinimumShouldMatch` (String): minimum number of **optional** terms that must match when `DefaultOperator = "Or"` — specified as an absolute number (for example, `"3"`) or a percentage of the optional terms (for example, `"50%"`). Required (`+`) terms are not counted

### `Wildcard` mode and `%` / `_` patterns (requires n-grams)

If the index is created with n-gram filtering, you can use `%` and `_` patterns similar to `LIKE`.
To explicitly enable this behavior, pass `"Wildcard" AS Mode` as a named argument:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "mach% learn%", "Wildcard" AS Mode)
LIMIT 20;
```

## LIKE / ILIKE {#like-ilike}

Use an n-gram index to search with patterns. `LIKE` is case-sensitive; `ILIKE` is not:

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%learn%ing%"
LIMIT 20;
```

Using `ILIKE` to search through a fulltext index requires `use_filter_lowercase=true`.

A pattern can be a parameter or an expression built from parameters. It must not depend on table rows or change during query execution. [`ESCAPE`](../expressions.md#check-match) lets you search for literal `%` and `_` characters.

Example substring search with escaping:

```yql
DECLARE $pattern AS String;

SELECT id, title
FROM articles VIEW ft_idx
WHERE body LIKE "%" || $pattern || "%" ESCAPE "!"
LIMIT 20;
```

With `$pattern = "abc!%def"`, the query searches for the literal substring `abc%def`.

## Equality, StartsWith, and EndsWith {#string-predicates}

An n-gram index also supports equality on the text column and [StartsWith / EndsWith](../../builtins/basic.md#starts_ends_with). These conditions are case-sensitive; `%` and `_` are ordinary characters.

Examples matching the whole text, the beginning, and the end of a string:

```yql
DECLARE $text AS String;

SELECT id FROM articles VIEW ft_idx WHERE body = $text;
SELECT id FROM articles VIEW ft_idx WHERE StartsWith(body, $text);
SELECT id FROM articles VIEW ft_idx WHERE EndsWith(body, $text);
```

## Searching with short patterns {#scan-fallback}

An index with `filter_ngram_min_length=3` cannot help with the pattern `%at%`: the fragment `at` has only two characters. In this case, {{ ydb-short-name }} searches the main table for matches, which can be slower.

This approach is consistent with [partial indexes](https://en.wikipedia.org/wiki/Partial_index), which store only some rows: if the index cannot answer a query, the query reads the table.

Scan fallback is available for string conditions (`LIKE`, `ILIKE`, equality, `StartsWith`, `EndsWith`) and `FulltextMatch` in `Wildcard` mode.
It does not apply if the query uses keyword matching (`Keywords`) or relevance scoring (`FulltextScore`).

## FulltextScore ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance)

[FulltextScore(text, query)](../../builtins/fulltext.md#fulltext-score) returns a relevance score and can be used for ranking.
Relevance scoring requires the [fulltext_relevance](../../../../dev/fulltext-indexes.md#relevance) index type.

```yql
SELECT id, title, FulltextScore(body, "machine learning") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "machine learning") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Optional parameters

Additional parameters must be passed as **named arguments**:

* `DefaultOperator` (String): term combination operator — `And` (default, all terms must be present) or `Or` (terms split into **required** with `+` prefix and **optional** without; at least `MinimumShouldMatch` optional terms must match, and every required term must be present)
* `MinimumShouldMatch` (String): when `DefaultOperator = "Or"`, minimum number of **optional** terms that must match — specified as an absolute number (for example, `"2"`) or a percentage of the optional terms (for example, `"50%"`). Required (`+`) terms are not counted
* `K1` (Double or Float): term frequency saturation parameter in [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — controls how strongly repeated occurrences of a term affect the score; default: `1.2`
* `B` (Double or Float): document length normalization parameter in [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) — `0.0` disables normalization, `1.0` fully normalizes by document length; default: `0.75`

Example:

```yql
SELECT id, FulltextScore(body, "machine learning", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "machine learning", "Or" AS DefaultOperator, "50%" AS MinimumShouldMatch) > 0
ORDER BY relevance DESC;
```

{% note info %}

Only the first two arguments of `FulltextMatch` / `FulltextScore` can be positional. Use named arguments for all additional parameters.

The `FulltextScore(...)` expression is repeated in full in both `SELECT` and `WHERE` — YQL, like standard SQL, evaluates `WHERE` before `SELECT`, so aliases defined in `SELECT` are not available in `WHERE`. Both occurrences must be identical.

{% endnote %}
