## Добавление вторичного индекса {#operation-index}

Добавьте индекс `title_index` по колонке `title` таблицы `series`:

```bash
{{ ydb-cli }} table index add global \
  --index-name title_index \
  --columns title series
```
