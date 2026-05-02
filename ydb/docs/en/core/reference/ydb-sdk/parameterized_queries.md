## Parameterized queries

{{ ydb-short-name }} supports two approaches: using [prepared statements](https://en.wikipedia.org/wiki/Prepared_statement) and caching the parameterized query plan. The recommended approach is caching, in which the compiled query plan is stored in an [LRU cache](https://en.wikipedia.org/wiki/Cache_replacement_policies) on the [{{ ydb-short-name }} node](../../concepts/glossary.md#node) (up to 1,000 entries by default). This approach is optimal for the distributed architecture of {{ ydb-short-name }}.

In many SDKs, parameterized query caching is enabled by default. If needed, it can be disabled by setting the `KeepInCache` parameter to `false`.

Parameterized queries separate data from the query text; special parameters in the SQL query indicate where the data values should be substituted.

Query with data embedded in the query body:

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

Parameterized queries are written as templates in which certain names are replaced with specific parameter values each time the query is executed. Tokens starting with `$`, such as `$seriesId` and `$seasonId` in the example above, are used to denote parameters.

Parameterized queries provide the following advantages:

* For repeated queries, the database server can cache the query execution plan. This radically reduces CPU consumption and increases system throughput.
* Parameterized queries protect against [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) vulnerabilities.
* Code reduction: only the query arguments change, not the query itself. No manual string concatenation is required.

## See also

- [{#T}](../../dev/example-app/index.md#param-queries)
