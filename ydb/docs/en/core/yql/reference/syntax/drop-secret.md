# DROP SECRET

The `DROP SECRET` statement deletes an existing [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```sql
DROP SECRET secret_name
```

* `secret_name` — the name of the secret to delete.

## Permissions {#permissions}

Deleting a secret requires the [rights](grant.md#permissions-list) `REMOVE SCHEMA` and `ALTER SCHEMA`.

## Examples {#examples}

Delete the secret named `secret_name`:

```sql
DROP SECRET secret_name;
```

## See also {#see-also}

* [CREATE SECRET](create-secret.md)
* [ALTER SECRET](alter-secret.md)
