# CREATE TABLE


{% include [alert_preview](../_includes/alert_preview.md) %}


The `CREATE TABLE` statement is used to create an empty table in the current database. The syntax of the command is:


{% include [syntax](../_includes/statements/create_table/syntax.md) %}


When creating a table, you can specify:
1. **Table Type**: `TEMPORARY` / `TEMP` – a temporary table that is automatically deleted at the end of the session. If this parameter is not set (left empty), a permanent table is created. Any indexes created on a temporary table will also be deleted at the end of the session, which means that they are temporary as well. A temporary table and a permanent table with the same name are allowed, in which case a temporary table will be selected.
2. **Table Name**: `<table name>` – you can use English letters in lowercase, numbers, underscores and dollar signs ($). For example, the table name "People" will be stored as "people". For more information, see [Identifiers and Key Words](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS).
3. **Column Name**: `<column name>` – the same naming rules apply as for table names.
4. **Data Type**: `<column data type>` – [standard PostgreSQL data types](https://www.postgresql.org/docs/14/datatype.html) are specified.
5. **Collation Rule**: `COLLATE` – [collation rules](https://www.postgresql.org/docs/current/collation.html) allow setting sorting order and character classification features in individual columns or even when performing individual operations. Sortable types include: `text`, `varchar`, and `char`. You can specify the locale (e.g., `en_US`, `ru_RU`) used to determine the sorting and string comparison rules in the specified columns.
6. Table's Primary Key: `PRIMARY KEY` – a mandatory condition when creating a table in YDB's PostgreSQL compatibility mode.
7. Table-level Constraints (there can be multiple, delimited by commas): `CONSTRAINT` – this type of constraint is used as an alternative syntax to column constraints, or when the same constraint conditions need to be applied to multiple columns. To specify a constraint, you need to state:
    + The keyword `CONSTRAINT`;
    + The constraint name `<constraint name>`. The rules for creating an identifier for the constraint are the same as for table names and column names;
    + The constraint. For example, a primary key constraint can be defined for a single column as `PRIMARY KEY (<column name>)` or for multiple columns as a composite key: `PRIMARY KEY (<column name1>, <column name2>, ...)`.


## Creating two tables with primary key autoincrement {#create_table_pk_serial}
#|
|| **Table people** | **Table social_card** ||
||


{% include [create_table_people](../_includes/statements/create_table/create_table_people.md) %}

|

{% include [create_table_social_card](../_includes/statements/create_table/create_table_social_card.md) %}

||
|#


In this example, we used the pseudo data type `Serial` – it's a convenient and straightforward way to create an auto-increment that automatically increases by 1 each time a new row is added to the table.


## Creating a table with constraints {#create_table_constraint_table}

{% include [create_table_people_const](../_includes/statements/create_table/create_table_people_const.md) %}

In this example, we created the "people" table with a constraint block (`CONSTRAINT`), where we defined a primary key (`PRIMARY KEY`) consisting of the "id" column. An alternative notation could look like this: `PRIMARY KEY(id)` without mentioning the `CONSTRAINT` keyword.


## Creating a temporary table {#create_table_temp_table}

{% include [create_table_temp.md](../_includes/statements/create_table/create_table_temp.md) %}

The temporary table is defined using the `TEMPORARY` or `TEMP` keywords.


## Creating a table with sorting conditions {#create_table_collate}

{% include [create_table_sort_cond](../_includes/statements/create_table/create_table_sort_cond.md) %}

In this example, the "name" and "lastname" columns use sorting with `en_US` localization.

{% include [../_includes/alert_locks](../_includes/alert_locks.md) %}
