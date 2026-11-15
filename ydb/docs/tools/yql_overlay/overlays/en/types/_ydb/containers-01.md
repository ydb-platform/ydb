YQL supports container types to define complex data structures organized in various ways.
Values of container types can be passed to YQL queries as input parameters or returned from YQL queries as columns of the set of results.
{% if backend_name == "YDB" %}
Container types can't be used as column data types for {{ ydb-short-name }} tables.
{% endif %}
