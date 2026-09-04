# DROP SECRET

The `DROP SECRET` statement deletes an existing [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```sql
DROP SECRET [IF EXISTS] secret_name
```

* `IF EXISTS` — the statement does not return an error if the secret does not exist; in this case, it is a no-op.
* `secret_name` — the name of the secret to delete.

## Permissions {#permissions}

Deleting a secret requires the [rights](grant.md#permissions-list) `REMOVE SCHEMA` and `ALTER SCHEMA`.

## Examples {#examples}

Delete the secret named `secret_name`:

```sql
DROP SECRET secret_name;
```

Delete the secret named `secret_name` only if it exists; if it does not exist, the statement is a no-op:

```sql
DROP SECRET IF EXISTS secret_name;
```

## See also {#see-also}

* [CREATE SECRET](create-secret.md)
* [ALTER SECRET](alter-secret.md)
