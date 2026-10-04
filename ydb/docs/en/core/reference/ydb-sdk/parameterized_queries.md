## Parameterized queries

<<<<<<< HEAD
{{ ydb-short-name }} supports and recommends the use of so-called [parameterized queries](https://en.wikipedia.org/wiki/Prepared_statement). In such queries, the data is transmitted separately from the request body itself, and in the SQL query, special parameters are used to indicate the location of the data.
=======
{{ ydb-short-name }} supports and recommends the use of so-called [parameterized queries](../../yql/reference/syntax/lexer.md#query-params). In such queries, data is transmitted separately from the query body itself, and special parameters in the SQL query are used to indicate where the data goes. Parameterized query plans are cached on the server (by default, up to 1000 entries), which lets you reuse plans for similar queries that differ only in parameter values.

In many {{ ydb-short-name }} SDKs, parameterized query caching is enabled by default. You can disable it if needed by setting the `KeepInCache` parameter to `false`.
>>>>>>> e78cf9d117c ([YDBDOCS-2857] Document query parameterization limitations and link related pages (#52765))

Request with data in the request body:

```yql
SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = 15 AND sa.season_id = 3
```

The corresponding parameterized query:

```yql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId
```

Parameterized queries are written in the form of a template in which certain types of names are replaced with specific parameters each time the query is executed. Tokens starting with the sign `$` such as `$seriesId` and `$seasonId` in the query above are used to denote parameters.

Parameterized queries provide the following advantages:

* For repeated requests, the database server has the ability to cache the query plan for parameterized requests. This radically reduces CPU consumption and increases system throughput.
* The use of parameterized queries saves from vulnerabilities like [SQL Injection](https://en.wikipedia.org/wiki/SQL_injection).

{{ ydb-short-name }} SDK automatically caches parameterized query plans by default, the setting `KeepInCache = true` is usually used for this.
