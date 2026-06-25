# ALTER TABLE

Using the `ALTER TABLE` command, you can change the column composition and additional parameters of {% if backend_name == "YDB" and oss == true %} [row](../../../../concepts/datamodel/table.md#row-tables) and [columnar](../../../../concepts/datamodel/table.md#colums-tables) tables{% else %}tables {% endif %}. You can specify multiple actions in a single command. In general, the `ALTER TABLE` command looks like this:


```yql
ALTER TABLE table_name action1, action2, ..., actionN;
```


`action` is any action to modify a table, from those described below:

* [Renaming a table](rename.md).
* Working with [columns](columns.md) of row and columnar tables.
* Adding or removing a [change stream](changefeed.md).
* Working with [indexes](indexes.md).
* Working with [column groups](family.md) of a row table.

{% if backend_name == "YDB" and oss == true %}

* Changing [additional table parameters](set.md).

{% endif %}

* [Running forced compaction](compact.md).
