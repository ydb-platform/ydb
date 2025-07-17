# ALTER TABLE

Using the `ALTER TABLE` command, you can modify the columns and additional parameters of {% if backend_name == "YDB" %}row and column tables{% else %}tables{% endif %}. Multiple actions can be specified in a single command. Generally, the `ALTER TABLE` command looks like this:

```yql
ALTER TABLE table_name action1, action2, ..., actionN;
```

An action is any modification to the table, as described below:

* [Renaming the table](rename.md).
* Managing [columns](columns.md) of row and column tables.
* Adding or removing a [changefeed](changefeed.md).
* Managing [indexes](indexes.md).
* Managing [column groups](family.md) of a row table.

{% if backend_name == "YDB" %}

* Modifying [additional table](set.md) parameters.

{% endif %}

