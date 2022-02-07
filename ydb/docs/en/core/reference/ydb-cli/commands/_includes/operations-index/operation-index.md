## Adding a secondary index {#operation-index}

Add an index named `title_index` by the `title` column of the `series` table:

```bash
{{ ydb-cli }} table index add global \
  --index-name title_index \
  --columns title series
```

