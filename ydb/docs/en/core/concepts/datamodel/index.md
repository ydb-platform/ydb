# Cluster structure

This section describes the {{ ydb-short-name }} cluster scheme entities.

## {{ ydb-short-name }} cluster scheme {#cluster-scheme}

{{ ydb-short-name }} cluster scheme is a hierarchical namespace of a {{ ydb-short-name }} cluster. The only root element of this namespace is a **cluster scheme root**. A root of the cluster scheme can be a directory or a root database. Children elements of the cluster scheme root can be [databases](../../concepts/glossary.md#database) or other [scheme objects](../../concepts/glossary.md#scheme-object). Scheme objects can use nested directories to form a hierarchy.

```plaintext
Cluster scheme root/
├── Database 1/
│   ├── Table 1
│   ├── Directory 1/
│   │   ├── Table 2
│   │   └── Table 3
│   └── Directory 2/
│       ├── Directory 3/
│       │   └── ...
│       └── ...
└── Database 2/
    └── ...
```

## Data model

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
