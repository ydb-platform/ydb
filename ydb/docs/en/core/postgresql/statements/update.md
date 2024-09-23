# UPDATE

{% include [alert_preview.md](../_includes/alert_preview.md) %}

The syntax of the `UPDATE` statement:

{% include [syntax](../_includes/statements/update/syntax.md) %}

The `UPDATE ... SET ... WHERE` statements works as follows:
1. **Table name is specified** – `UPDATE <table name>`, where the data will be updated;
2. **Column name is indicated** – `SET <column name>`, where the data will be updated;
3. **New value is set** – `<new value>`;
4. **Search criteria are specified** – `WHERE` with the indication of the search column `<search column name>` and the value that the search criterion should match `<search value>`. If `CASE` is used, then the `IN` operator is specified with a list of values `<column name>`.


## Updating a single row in a table with conditions

#|
|| **Update without conditions** | **Update with conditions** ||
|| 
{% include [update_where](../_includes/statements/update/update_where.md) %}
| 
{% include [update_where_and](../_includes/statements/update/update_where_and.md) %}
||
|#

In the example "Update with conditions", the condition combining operator `AND` is used – the condition will only be satisfied when both parts meet the truth conditions. The operator `OR` can also be used – the condition will be satisfied if at least one part meets the truth conditions. There can be multiple condition operators:

{% include [update_where_and_or](../_includes/statements/update/update_where_and_or.md) %}

## Updating a single record in a table using expressions or functions: {#update_set_func_where}
Frequently during updates, it is necessary to perform mathematical actions on the data or to modify it using functions.

#|
|| **Update with the use of expressions** | **Update with the use of functions** ||
|| 
{% include [update_set_where](../_includes/statements/update/update_set_where.md) %}
| 
{% include [update_set_func_where](../_includes/statements/update/update_set_func_where.md) %}
||
|#


## Updating multiple fields of a table row {#update_set_where}
Data can be updated in multiple columns simultaneously. For this, a list of `<column name> = <column new value>` is made after the keyword `SET`:

{% include [update_set_few_col_where](../_includes/statements/update/update_set_few_col_where.md) %}

## Updating multiple rows in a table using the **CASE ... END** construction {#update_set_case_end_where}
For simultaneous updating of different values in different rows, the `CASE ... END` instruction can be used with nested data selection conditions `WHEN <column name> <condition> (=, >, <) THEN <new value>`. This is followed by the `WHERE <column name> IN (<column value>, ...)` construct, which allows setting a list of values for which the condition will be executed.

Example of changing the age (`age`) of people (`people`) depending on their names:

{% include [update_set_case_where](../_includes/statements/update/update_set_case_where.md) %}

{% include [alert_locks](../_includes/alert_locks.md) %}