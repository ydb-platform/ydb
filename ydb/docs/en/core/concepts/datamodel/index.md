# Cluster structure

This section describes the {{ ydb-short-name }} cluster scheme entities.

## {{ ydb-short-name }} cluster scheme {#cluster-scheme}

{{ ydb-short-name }} cluster scheme is a hierarchical namespace of a {{ ydb-short-name }} cluster. The top-level element of the namespace is the **cluster scheme root** that contains [databases](../../concepts/glossary.md#database) as its children. Scheme objects inside a database can use nested directories to form a hierarchy.

![cluster scheme diagram](_assets/cluster-scheme.png =500x)

## {{ ydb-short-name }} scheme objects

Scheme objects in {{ ydb-short-name }} databases:

* [Folder](dir.md)
* [Table](table.md)

{% if feature_view %}
* [View](view.md)
{% endif %}

* [Topic](../topic.md)
* [Secret](secrets.md)
* [External Table](external_table.md)
* [External Data Source](external_data_source.md)

[Scheme objects](../../concepts/glossary.md#scheme-object) in {{ ydb-short-name }} all follow the same naming rules described in the section below.

{% include [object naming rules](./_includes/object-naming-rules.md) %}
