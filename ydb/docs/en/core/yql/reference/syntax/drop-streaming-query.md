# DROP STREAMING QUERY

`DROP STREAMING QUERY` deletes a [streaming query](../../../concepts/streaming-query/streaming-query.md).

## Syntax


```sql
DROP STREAMING QUERY [IF EXISTS] <query_name>
```


### Parameters

* `IF EXISTS` — do not output an error if the streaming query does not exist.
* `query_name` is the name of the streaming query to be deleted.

## Permissions

Requires [permission](./grant.md#permissions-list) `REMOVE SCHEMA` on the streaming query, an example of granting such permission for query `my_streaming_query`:


```sql
GRANT REMOVE SCHEMA ON my_streaming_query TO `user@domain`
```


## Examples

Deleting query `my_streaming_query`:


```sql
DROP STREAMING QUERY my_streaming_query
```


## See also

* [{#T}](../../../concepts/streaming-query/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](alter-streaming-query.md)
