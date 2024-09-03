`PRAGMA TablePathPrefix` adds a specified prefix to the paths of database tables. It uses standard file system path concatenation, meaning it supports parent folder referencing and does not require a trailing slash. For example:

```sql
PRAGMA TablePathPrefix = "/cluster/database";
SELECT * FROM episodes;
```

For more information about `PRAGMA` in YQL, refer to the [YQL documentation](../../../../yql/reference/index.md).