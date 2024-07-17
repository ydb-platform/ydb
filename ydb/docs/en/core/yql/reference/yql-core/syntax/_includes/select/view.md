# Data views (VIEW)

YQL supports two types of data views:

* Linked to specific tables.
* Independent, that might use an arbitrary number of tables within the cluster.
Both of the views are non-materialized. It means that they are substituted into the calculation graph at each use.

Accessing a `VIEW`:

* Views linked to a table require a special syntax: ```[cluster.]`path/to/table` VIEW view_name```.
* Views that aren't linked to tables, look like regular tables from the user perspective.

If the meta attributes of the table specify an automatic UDF call to convert raw data into a structured set of columns, you can access raw data using a special `raw` view like this: ```[cluster.]`path/to/table` VIEW raw```.

**Examples:**

```yql
USE some_cluster;
SELECT *
FROM my_table VIEW my_view;
```

