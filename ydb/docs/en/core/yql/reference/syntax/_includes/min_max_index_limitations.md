* Supported only for [columnar tables](../../../../concepts/glossary.md#column-oriented-table).
* `ON (...)` must specify exactly one column.
* `COVER (...)` and additional data columns are not supported.
* Specific parameters of `WITH (...)` are not supported.
* `ALTER INDEX` is not supported for the min_max index.
* Columns of types `Json` and `JsonDocument` are not supported.
