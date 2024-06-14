# INSERT INTO

{% include [alert_preview.md](../_includes/alert_preview.md) %}

Syntax of the `INSERT INTO` statement:

{% include [syntax](../_includes/statements/insert_into/syntax.md) %}

The `INSERT INTO` statement is adds rows to a table. It can insert one or several rows in a single execution. Example of inserting a single row into the "people" table:

{% include [insert_into_table_people](../_includes/statements/insert_into/insert_into_table_people.md) %}

In this query, we did not specify the `id` column and did not assign a value to it. This is intentional, as the "id" column in the "people" table is set to the `Serial` data type. When executing the `INSERT INTO` statement, the value of the "id" column will be assigned automatically, taking into account previous values, the current "id" value will be incremented.

For inserting multiple rows into a table, the same construction is used with the enumeration of groups of data to be inserted, separated by commas:

{% include [insert_into_table_people_many_rows](../_includes/statements/insert_into/insert_into_table_people_many_rows.md) %}

In both examples, to specify the release date of the movie, we used the `CAST()` function, which is used to convert one data type to another. In this case, using the keyword `AS` and the data type `Date`, we explicitly indicated that we want to convert the string representation of the date in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format.

You can specify the required data type, for example, `DATE`, by using the type cast operator `::`, which is a PostgreSQL-specific syntax for explicit conversion of a value from one data type to another. This is in contrast to the `CAST` function, which is used more broadly across different SQL databases for the same purpose. An example of using the `::` operator might look like this: `'2023-01-01'::DATE`, which explicitly converts the string to a `DATE` type, ensuring the database treats the value as a date. This explicit casting with `::` is particularly useful when you want to override implicit type conversion rules of the database.

An example of using the `::` operator might look like this:

{% include [insert_into_table_people_set_date](../_includes/statements/insert_into/insert_into_table_people_set_date.md) %}

{% include [alert_locks](../_includes/alert_locks.md) %}