# CREATE VIEW

CREATE VIEW defines a view of a query.

A view is a logical representation of a table formed by the query, specified at the moment of the view's creation. The view does not physically store the table, but runs the query to produce the data whenever the view is selected from.

## Syntax

```sql
CREATE VIEW name
[ WITH ( view_option_name [= view_option_value] [, ... ] ) ]
AS query
```

### Parameters

* `name`

The name of the view to be created. The name must be distinct from the name of any other schema object.
* `query`

The SELECT query, which will be used to produce the logical table the view represents.

`WITH ( view_option_name [= view_option_value] [, ... ] )`

This clause specifies optional parameters for a view. The following parameters are supported:

* `security_invoker` (Bool)

This option causes the underlying base relations to be checked against the privileges of the user of the view rather than the view owner.

## Notes

`security_invoker` option must be always set to true, because the default behavior for views is to execute the query on behalf of the view's creator, which is not supported yet.

The execution context of the view's query is different from the enclosing context of the SELECT from the view statement. It does not "see" previously defined PRAGMAs like TablePathPrefix, named expressions, etc. Most importantly, users must specify the tables (or views) they select from in the view's query by their schema-qualified names. You can see in the [examples](#examples) that the absolute path like `/domain/database/path/to/underlying_table` is used to specify the table, from which the view selects. We are going to change the context of the view's query compilation in the next releases.

The only way to specify the columns you want to see in the view is to select them in particular in the view's query. Star (\*) expansions in the view's query happens each time you recompile it. The columns returned by the following statement:
```sql
/*
CREATE VIEW view_with_a_star WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/domain/database/path/to/underlying_table`;
*/

SELECT * FROM view_with_a_star;
```
might change if the columns list of the `underlying_table` changes.

## Examples {#examples}

Create a view that will list only recent series from the series table:

```sql
CREATE VIEW recent_series WITH (security_invoker = TRUE) AS
    SELECT
        *
    FROM `/domain/database/path/to/series`
    WHERE
        release_date > Date("2020-01-01");
```

Create a view that will list the titles of the first episodes of the recent series:

```sql
CREATE VIEW recent_series_first_episodes_titles WITH (security_invoker = TRUE) AS
    SELECT
        episodes.title AS first_episode
    FROM `/domain/database/path/to/recent_series`
        AS recent_series
    JOIN `/domain/database/path/to/episodes`
        AS episodes
    ON recent_series.series_id = episodes.series_id
    WHERE episodes.season_id = 1 AND episodes.episode_id = 1;
```

## See also

[ALTER VIEW](alter_view), [DROP VIEW](drop_view)