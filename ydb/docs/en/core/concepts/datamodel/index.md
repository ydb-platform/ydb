# Cluster structure

This section describes the {{ ydb-short-name }} cluster scheme entities.

## {{ ydb-short-name }} cluster scheme {#cluster-scheme}

{{ ydb-short-name }} cluster scheme is a hierarchical namespace of a {{ ydb-short-name }} cluster. The only root element of this namespace is a **cluster scheme root**. [Databases](../../concepts/glossary.md#database) are children elements of the cluster scheme root. It is also possible to create other [scheme objects](../../concepts/glossary.md#scheme-object) in the cluster scheme root. These objects exist at the cluster level and do not belong to tenant databases. Scheme objects inside a database can use nested directories to form a hierarchy.

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
* [External table](external_table.md)
* [External data source](external_data_source.md)

[Scheme objects](../../concepts/glossary.md#scheme-object) in {{ ydb-short-name }} all follow the same naming rules described in the section below. However, requirements for column names are slightly different.

{% include [object naming rules](./_includes/object-naming-rules.md) %}
