# Bloom skip index quickstart

Below is a minimal example: a column-oriented table with a primary key and a local `bloom_filter` index on a column that is frequently used in filters.

```yql
CREATE TABLE `/Root/events` (
    id Uint64 NOT NULL,
    resource_id Utf8 NOT NULL,
    payload String,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_res LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
)
WITH (
    STORE = COLUMN
);
```

## Extending the example: n-gram index

You can add `bloom_ngram_filter` on a string column to the same table from the example above (column-oriented tables):

```yql
ALTER TABLE `/Root/events`
  ADD INDEX idx_msg LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

## Queries and the effect

After you load data, selective queries that filter on indexed columns may read less data: while scanning storage, the Bloom skip index skips fragments that cannot contain the requested value (compared to reading the full column without this filter).

Sample data and queries for the table above:

```yql
INSERT INTO `/Root/events` (id, resource_id, payload, message) VALUES
    (1, "res-1", "{}", "started"),
    (2, "res-42", "{}", "error: timeout"),
    (3, "res-2", "{}", "done");
```

Equality on the column protected by `bloom_filter` — the engine can prune irrelevant fragments when reading `resource_id` and related columns:

```yql
SELECT id, message
FROM `/Root/events`
WHERE resource_id = "res-42";
```

Substring search on the column with `bloom_ngram_filter` — the n-gram index helps drop fragments that cannot contain matching n-grams in `message`:

```yql
SELECT id, message
FROM `/Root/events`
WHERE message LIKE '%timeout%';
```

Further reading:

* Details and limitations: [Bloom skip indexes](../../dev/bloom-skip-indexes.md)
* Parameter tuning: [Parameter tuning](../../dev/bloom-skip-indexes.md#tuning)
* Full syntax: [`ALTER TABLE ADD INDEX`](../../yql/reference/syntax/alter_table/indexes.md#local-bloom)
