## IN {#in}

Checking whether a value is inside of a set of values. It's logically equivalent to a chain of equality comparisons using `OR` but implemented more efficiently.

{% note warning "Warning" %}

Unlike a similar keyword in Python, in YQL `IN ` **DOES NOT** searches for a substring inside a string. To search for a substring, use the function [String::Contains](../../../udf/list/string.md) or [LIKE/REGEXP](#like) mentioned above.

{% endnote %}

Immediately after `IN`, you can specify the `COMPACT` modifier.
If `COMPACT` is not specified, then `IN` with a subquery is executed as a relevant `JOIN` (`LEFT SEMI` for `IN` and `LEFT ONLY` for `NOT IN`), if possible.
Using the `COMPACT` modifier forces the in-memory execution strategy: a hash table is immediately built from the contents of the right `IN` part in-memory, and then the left part is filtered.

The `COMPACT` modifier must be used with care. Since the hash table is built in-memory, the query may fail if the right part of `IN` contains many large or different elements.

{% if feature_mapreduce %}
Since YQL imposes a limit on the query size in bytes (it's about 1Mb), add large lists of values to your query by URLs and use the [ParseFile](../../../builtins/basic.md#parsefile) function.
{% endif %}

**Examples**

```yql
SELECT column IN (1, 2, 3)
FROM my_table;
```

```yql
SELECT * FROM my_table
WHERE string_column IN ("a", "b", "c");
```

```yql
$foo = AsList(1, 2, 3);
SELECT 1 IN $foo;
```

```yql
$values = (SELECT column + 1 FROM table);
SELECT * FROM my_table WHERE
    -- filtering by an in-memory hash table for one_table
    column1 IN COMPACT $values AND
    -- followed by LEFT ONLY JOIN with other_table
    column2 NOT IN (SELECT other_column FROM other_table);
```

