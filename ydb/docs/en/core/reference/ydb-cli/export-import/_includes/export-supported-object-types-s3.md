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
- [external data source](../../../../concepts/datamodel/external_data_source.md).
- [external table](../../../../concepts/datamodel/external_table.md).

External data sources and external tables are exported as definitions only; data in external storage is not copied. The server must support exporting these object types and have their export controls enabled. Required secrets must be backed up separately and restored before restoring objects that reference them. Object discovery and export do not form an atomic schema snapshot.
