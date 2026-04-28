# Bloom skip index quickstart

Below is a minimal example: a column-oriented table with a primary key and a local `bloom_filter` index on a column that is frequently used in filters.

```yql
CREATE TABLE `/Root/events` (
    id Uint64 NOT NULL,
    resource_id Utf8 NOT NULL,
    payload String,
    PRIMARY KEY (id),
    INDEX idx_res LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
)
WITH (
    STORE = COLUMN
);
```

For substring-style filtering on string columns, use `bloom_ngram_filter`:

```yql
ALTER TABLE `/Root/logs`
  ADD INDEX idx_msg LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

Next steps:

* Details and limitations: [Bloom skip indexes](../../dev/bloom-skip-indexes.md)
* Full syntax: [`ALTER TABLE ADD INDEX`](../../yql/reference/syntax/alter_table/indexes.md#local-bloom)
