# SELECT

{% include [alert_preview.md](../_includes/alert_preview.md) %}

Syntax of the `SELECT` statement:

{% include [syntax](../_includes/statements/select/syntax.md) %}


## Calling SELECT without specifying a target table {#select_func}
`SELECT` is used to return computations to the client side and can be used even without specifying a table, as seen in constructs like `SELECT NOW()`, or for performing operations such as working with dates, converting numbers, or calculating string lengths. However, `SELECT` is also used in conjunction with `FROM ...` to retrieve data from a specified table. When used with `INSERT INTO ...`, `SELECT` serves to select data that will be inserted into another table. In subqueries, `SELECT` is utilized within a larger query, not necessarily with `FROM ...`, to contribute to the overall computation or condition.

For example, `SELECT` can be used for working with dates, converting numbers, or calculating string length:

{% include [select_no_target](../_includes/statements/select/select_no_target.md) %}

Such use of `SELECT` can be useful for testing, debugging expressions, or SQL functions without accessing a real table, but more often `SELECT` is used to retrieve rows from one or more tables.


## Retrieving values from one or multiple columns {#select_from}
To return values from one or several columns of a table, `SELECT` is used in the following form:

{% include [select_few_col_syntax](../_includes/statements/select/select_few_col_syntax.md) %}

To read all data from a table, for example, the `people` table, you need to execute the command `SELECT * FROM people;`, where `*` is the special symbol indicating all columns. With this statement, all rows from the table will be returned with data from all columns.

To display the "id", "name", and "lastname" columns for all rows of the `people` table, you can do it as follows:

{% include [select_few_col_example](../_includes/statements/select/select_few_col_example.md) %}


## Limiting the results obtained from a query using WHERE {#select_from_where}
To select only a subset of rows, the `WHERE` clause with filtering criteria is used: `WHERE <column name> <condition> <column value>;`:

{% include [select_few_col_where](../_includes/statements/select/select_few_col_where.md) %}

`WHERE` allows the use of multiple conditional selection operators (`AND`, `OR`) to create complex selection conditions, such as ranges:

{% include [select_few_col_and](../_includes/statements/select/select_few_col_and.md) %}


## Retrieving a subset of rows using LIMIT and OFFSET conditions {#select_from_where_limit}
To limit the number of rows in the result set, `LIMIT` is used with the specified number of rows:

{% include [select_few_col_where_limit](../_includes/statements/select/select_few_col_where_limit.md) %}

Thus, the first 5 rows from the query will be printed out. With `OFFSET`, you can specify how many rows to skip before starting to print out rows:

{% include [select_few_col_where_limit_offset](../_includes/statements/select/select_few_col_where_limit_offset.md) %}

When specifying `OFFSET 3`, the first 3 rows of the resulting selection from the `people` table will be skipped.


## Sorting the results of a query using ORDER BY {#select_from_where_order_by}
By default, the database does not guarantee the order of returned rows, and it may vary from query to query. If a specific order of rows is required, the `ORDER BY` clause is used with the designation of the column for sorting and the direction of the sort:

{% include [select_few_col_where_order_by](../_includes/statements/select/select_few_col_where_order_by.md) %}

Sorting is applied to the results returned by the `SELECT` clause, not to the original columns of the table specified in the `FROM` clause. Sorting can be done in ascending order – `ASC` (from smallest to largest - this is the default option and does not need to be specified) or in descending order – `DESC` (from largest to smallest). How sorting is executed depends on the data type of the column. For example, strings are stored in utf-8 and are compared according to "unicode collate" (based on character codes).


## Grouping the results of a query from one or more tables using GROUP BY {#select_from_where_group_by}
`GROUP BY` is used to aggregate data across multiple records and group the results by one or several columns. The syntax for using `GROUP BY` is as follows:

{% include [select_group_by_syntax](../_includes/statements/select/select_group_by_syntax.md) %}

Example of grouping data from the "people" table by gender ("sex") and age ("age") with a selection limit (`WHERE`) based on age:

{% include [select_few_col_where_group_by_few_cond](../_includes/statements/select/select_few_col_where_group_by_few_cond.md) %}

In the previous example, we used `WHERE` – an optional parameter for filtering the result, which filters individual rows before applying `GROUP BY`. In the next example, we use `HAVING` to exclude from the result the rows of groups that do not meet the condition. `HAVING` filters the rows of groups created by `GROUP BY`. When using `HAVING`, the query becomes grouped, even if `GROUP BY` is absent. All selected rows are considered to form one group, and in the `SELECT` list and `HAVING` clause, one can refer to the table columns only from aggregate functions. Such a query will yield a single row if the result of the `HAVING` condition is true, and zero rows otherwise.

**Examples of Using `HAVING`**:

#|
|| **HAVING + GROUP BY** | **HAVING + WHERE + GROUP BY** ||
||
{% include [select_having_group_by](../_includes/statements/select/select_having_group_by.md) %}
|
{% include [select_having_where_group_by](../_includes/statements/select/select_having_where_group_by.md) %}
||
|#


## Joining tables using the JOIN clause {#select_from_join_on}
`SELECT` can be applied to multiple tables with the specification of the type of table join. The joining of tables is set through the `JOIN` clause, which comes in five types: `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, `CROSS JOIN`, `FULL JOIN`. When a `JOIN` is performed on a specific condition, such as a key, and one of the tables has several rows with the same value of this key, a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) occurs. This means that each row from one table will be joined with every corresponding row from the other table.


### Joining tables using LEFT JOIN, RIGHT JOIN, or INNER JOIN {#select_from_left_right__inner_join_on}
The syntax for `SELECT` using `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, `FULL JOIN` is the same:

{% include [select_join_syntax](../_includes/statements/select/select_join_syntax.md) %}

All `JOIN` modes, except `CROSS JOIN`, use the keyword `ON` for joining tables. In the case of `CROSS JOIN`, its usage syntax will be as follows: `CROSS JOIN <table name> AS <table name alias>;`. Let's consider an example of using each `JOIN` mode separately.

#### LEFT JOIN {#left_join}
Returns all rows from the left table and the matching rows from the right table. If there are no matches, it returns `NULL` (the output will be empty) for all columns of the right table. Example of using `LEFT JOIN`:

{% include [select_left_join](../_includes/statements/select/select_left_join.md) %}

The result of executing an SQL query using `LEFT JOIN` without one record in the right table `social_card`:

{% include [select_left_join_output](../_includes/statements/select/select_left_join_output.md) %}

#### RIGHT JOIN {#right_join}
Returns all rows from the right table and the matching rows from the left table. If there are no matches, it returns `NULL` for all columns of the left table. This type of `JOIN` is rarely used, as its functionality can be replaced by `LEFT JOIN`, and swapping the tables. Example of using `RIGHT JOIN`:

{% include [select_right_join](../_includes/statements/select/select_right_join.md) %}

The result of executing an SQL query using `RIGHT JOIN` without one record in the left table `people`:

{% include [select_right_join_output](../_includes/statements/select/select_right_join_output.md) %}

#### INNER JOIN {#inner_join}
Returns rows when there are matching values in both tables. Excludes from the results those rows for which there are no matches in the joined tables. Example of using `INNER JOIN`:

{% include [select_inner_join](../_includes/statements/select/select_inner_join.md) %}

Such an SQL query will return only those rows for which there are matches in both tables:

{% include [select_inner_join_output](../_includes/statements/select/select_inner_join_output.md) %}

#### CROSS JOIN {#cross_join}
Returns the combined result of every row from the left table with every row from the right table. `CROSS JOIN` is usually used when all possible combinations of rows from two tables are needed. `CROSS JOIN` simply combines each row of one table with every row of another without any condition, which is why its syntax lacks the `ON` keyword: `CROSS JOIN <table name> AS <table name alias>;`.

Example of using `CROSS JOIN` with the result output limited by `LIMIT 5`:

{% include [select_cross_join](../_includes/statements/select/select_cross_join.md) %}

The example above will return all possible combinations of columns participating in the selection from the two tables:

{% include [select_cross_join_output](../_includes/statements/select/select_cross_join_output.md) %}

#### FULL JOIN {#full_join}
Returns both matched and unmatched rows in both tables, filling in `NULL` for columns from the table for which there is no match. Example of executing an SQL query using `FULL JOIN`:

{% include [select_full_join](../_includes/statements/select/select_full_join.md) %}

As a result of executing the SQL query, the following output will be returned:

{% include [select_full_join_output](../_includes/statements/select/select_full_join_output.md) %}