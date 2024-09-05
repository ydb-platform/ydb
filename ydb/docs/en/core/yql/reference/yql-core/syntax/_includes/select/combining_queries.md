### Combining queries {#combining-queries}

Results of several SELECT statements (or subqueries) can be combined using `UNION` and `UNION ALL` keywords.
```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```
Union of more than two queries is interpreted as a left-associative operation, that is

```yql
query1 UNION query2 UNION ALL query3
```
is interpreted as
```yql
(query1 UNION query2) UNION ALL query3
```

If the underlying queries have one of the `ORDER BY/LIMIT/DISCARD/INTO RESULT` operators, the following rules apply:
* `ORDER BY/LIMIT/INTO RESULT` is only allowed after the last query;
* `DISCARD` is only allowed before the first query;
* the operators apply to the `UNION [ALL]` as a whole, instead of referring to one of the queries;
* to apply the operator to one of the queries, enclose the query in parantheses
