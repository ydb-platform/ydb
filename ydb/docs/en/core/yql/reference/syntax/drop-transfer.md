# DROP TRANSFER

The `DROP TRANSFER` statement deletes a [transfer](../../../concepts/transfer.md) instance. If a [consumer](../../../concepts/datamodel/topic.md#consumer) was created automatically when the transfer was created, it is also deleted. The system will keep trying to delete the consumer until the operation is successful.

The `DROP TRANSFER` statement does not delete the destination table or the source topic.

## Syntax {#syntax}

```yql
DROP TRANSFER <name>
```

where:

* `name` — the name of the transfer instance.

## Permissions

The following [permissions](grant.md#permissions-list) are required to delete a transfer:

* `REMOVE SCHEMA` — to delete the transfer instance;
* `ALTER SCHEMA` — to delete the automatically created topic consumer (if applicable).

## Examples {#examples}

The following query deletes the transfer named `my_transfer`:

```yql
DROP TRANSFER my_transfer;
```

## See Also

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)
* [{#T}](../../../concepts/transfer.md)
