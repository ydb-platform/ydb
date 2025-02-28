# Data model and schema

This section describes the entities that {{ ydb-short-name }} uses within DBs. The {{ ydb-short-name }} core lets you flexibly implement various storage primitives, so new entities may appear in the future.

{{ ydb-short-name }} is a relational database where the data is stored in [tables](table.md) with each table consisting of rows and columns. Database objects in {{ ydb-short-name }} can be organized into a hierarchy of [folders](dir.md).

* [Folder](dir.md)
* [Table](table.md)

{% if feature_view %}
* [View](view.md)
{% endif %}

* [Topic](../topic.md)
* [Secret](secrets.md)
* [External table](external_table.md)
* [External data source](external_data_source.md)

[Scheme objects](../../concepts/glossary.md#scheme-object) in {{ ydb-short-name }} all follow the same naming rules described in the section below. However, requirements for column names are slightly different.

{% include [object naming rules](./_includes/object-naming-rules.md) %}
