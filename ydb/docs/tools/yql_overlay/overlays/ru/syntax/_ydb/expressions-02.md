### LIKE / ILIKE с полнотекстовым индексом {#like-ilike-with-fulltext-index}

Для [полнотекстовых индексов](../../../../dev/fulltext-indexes.md) с N-граммами (параметры `use_filter_ngram` / `use_filter_edge_ngram`) поддерживается `LIKE`/`ILIKE` по индексируемой текстовой колонке.

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE body ILIKE "%обуч%ние%"
LIMIT 20;
```

### Примеры
