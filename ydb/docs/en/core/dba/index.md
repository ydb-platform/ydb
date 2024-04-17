# {{ ydb-short-name }} for Database Administrators (DBA)

This section of {{ ydb-short-name }} documentation covers everything you need to know to properly manage contents of {{ ydb-short-name}} databases: [tables](../concepts/datamodel/table.md), [topics](../concepts/topic.md), and other entities.

The creation and management of databases are out of the scope of this section because in {{ ydb-short-name }} clusters these operations are either [executed by DevOps Engineers](../devops/index.md) or automated externally if you are using a managed service.

- [Backup and recovery](backup-and-recovery.md)
- Choosing a primary key for:
  - [Row-oriented tables](primary-key/row-oriented.md)
  - [Column-oriented tables](primary-key/column-oriented.md)
- [Secondary indices](secondary-indexes.md)
- [Change Data Capture](cdc.md)
- [Database system views](system-views.md)
- [Custom attributes](custom-attributes.md)
