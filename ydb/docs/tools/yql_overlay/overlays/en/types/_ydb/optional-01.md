{% if backend_name == "YDB" %}

{% note info %}

Container types (including `Optional<T>` containers and more complex types derived from them) can't currently be used as column data types when creating {{ ydb-short-name }} tables.
YQL queries can return values of container types and accept them as input parameters.

{% endnote %}

{% endif %}

### Example
