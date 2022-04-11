# USE

Specifying the "database". As a rule, one of the {{ backend_name }} clusters is used as a database. This database will be used by default to search for tables whenever the database hasn't been specified explicitly.

If the query doesn't include `USE`, then the cluster must be specified at the beginning of the table path in the format ``` cluster.`path/to/table` ```, for example ``` hahn.`home/yql/test` ```. Backticks are used to automatically escape special characters (in this case, slashes).

Usually the cluster name is specified explicitly, but you can use an expression for it. For example, this will let you use the parameters declared by [DECLARE](../declare.md).
In this case, `USE` must have the notation ```USE yt:$cluster_name```, where `$cluster_name` is the [named expression](../expressions.md#named-nodes) of the `String` type.
Alternatively, you can specify the cluster right at the beginning of the table path in the format ``` yt:$cluster_name.`path/to/table` ```.

As far as `USE` is concerned, you can add it inside [actions](../action.md){% if feature_subquery %} or [subquery templates](../subquery.md){% endif %}. The value of the current cluster is inherited by declarations of nested actions{% if feature_subquery %} or subqueries{% endif %}. The scope of `USE` is terminated at the end of the action{% if feature_subquery %} or {% endif %} subquery template where it has been declared.

**Examples:**

```yql
USE {{ example_cluster }};
```

{% note info %}

`USE` **doesn't** guarantee that the query will be executed against the specified cluster. The query might be executed against other cluster if it doesn't use any input data (for example, `USE foo; SELECT 2 + 2;` ) or if the full path to the table on other cluster has been specified (for example, `USE foo; SELECT * FROM bar.``path/to/table``;`).

{% endnote %}

