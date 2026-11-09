Export is available only for objects of the following types:

- [directory](../../../../concepts/datamodel/dir.md).
- [row table](../../../../concepts/datamodel/table.md#row-oriented-tables).
- [column table](../../../../concepts/datamodel/table.md#column-oriented-tables).
- [secondary index](../../../../concepts/glossary.md#secondary-index).
- [vector index](../../../../concepts/glossary.md#vector-index).
- [full-text index](../../../../concepts/glossary.md#fulltext-index).
- [topic](../../../../concepts/datamodel/topic.md) (schema only).
- [view](../../../../concepts/datamodel/view.md).
- [asynchronous replication](../../../../concepts/async-replication.md).
- [transfer](../../../../concepts/transfer.md).
- [external data source](../../../../concepts/datamodel/external_data_source.md) (with `--include-external`).
- [external table](../../../../concepts/datamodel/external_table.md) (with `--include-external`).

Use `--include-external` to include external data sources and external tables from the selected paths. Without this option, the existing export behavior is preserved; it does not guarantee that external objects are included.

With `--include-external`, the CLI lists selected paths and traverses their directories before submitting the export. This requires schema-description permission on the listed paths and adds listing requests for each directory. Listing errors and rejection of selected external objects fail the command; the CLI does not retry by silently omitting those objects. Older CLI versions do not recognize this option.

External data sources and external tables are exported as definitions only; data in external storage is not copied. The server must support exporting these object types and have their export controls enabled. Required secrets must be backed up separately and restored before restoring objects that reference them. Object discovery and export do not form an atomic schema snapshot.
