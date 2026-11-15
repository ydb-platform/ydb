### LIKE / ILIKE with a fulltext index {#like-ilike-with-fulltext-index}

For [fulltext indexes](../../../../dev/fulltext-indexes.md) with n-grams (`use_filter_ngram` / `use_filter_edge_ngram`), `LIKE`/`ILIKE` over the indexed text column is supported.

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%learn%ing%"
LIMIT 20;
```

### Examples
