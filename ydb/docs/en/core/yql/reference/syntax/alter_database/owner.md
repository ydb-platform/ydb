# Change database owner

Changes the database owner. Only a database administrator can perform this operation.

## Syntax

```yql
ALTER DATABASE path OWNER TO user_name;
```

### Parameters

* `path` — path to the database;
* `user_name` — name of the user who will become the database owner.

## Examples

Make user `user1` the owner of database `/Root/test`:

```yql
ALTER DATABASE `/Root/test` OWNER TO user1;
```
