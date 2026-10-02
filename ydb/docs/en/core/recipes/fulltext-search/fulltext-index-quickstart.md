# Fulltext index - quick start

This quick start shows how to create a fulltext index and execute fulltext queries in {{ ydb-short-name }}.

## Create a table and a fulltext index

```yql
CREATE TABLE articles (
  id Uint64,
  title String,
  body String,
  PRIMARY KEY (id)
);

ALTER TABLE articles ADD INDEX ft_idx
  GLOBAL USING fulltext_relevance
  ON (body) COVER (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

## Insert sample data

```yql
UPSERT INTO articles (id, title, body) VALUES
  (1, "Intro",  "Machine learning is a field of AI"),
  (2, "Search", "Fulltext search finds documents by words"),
  (3, "Notes",  "Neural networks are used in machine learning");
```

## Filter documents

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "machine learning")
LIMIT 20;
```

Query execution result:

```bash
id title
1  Intro
3  Notes
```

## Rank documents

```yql
SELECT id, title, FulltextScore(body, "machine learning") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "machine learning") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Query execution result:

```bash
id title relevance
1  Intro  0.8762718444094963
3  Notes  0.8762718444094963
```

For more details, see:

* [Fulltext indexes](../../dev/fulltext-indexes.md)
* [VIEW (Fulltext index)](../../yql/reference/syntax/select/fulltext_index.md)
* [Fulltext built-in functions](../../yql/reference/builtins/fulltext.md)
