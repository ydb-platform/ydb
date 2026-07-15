# DROP STREAMING QUERY

`DROP STREAMING QUERY` deletes a [streaming query](../../../concepts/streaming-query.md).

## Syntax

```sql
DROP STREAMING QUERY [IF EXISTS] <query_name>
```

### Parameters

* `IF EXISTS` — do not fail if the streaming query does not exist.
* `query_name` — name of the streaming query to delete.

## Permissions

Requires [permission](./grant.md#permissions-list) `REMOVE SCHEMA` on the streaming query. Example grant for `my_streaming_query`:

```sql
GRANT REMOVE SCHEMA ON my_streaming_query TO `user@domain`
```

## Examples

Delete `my_streaming_query`:

```sql
DROP STREAMING QUERY my_streaming_query
```

## See also

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](alter-streaming-query.md)
