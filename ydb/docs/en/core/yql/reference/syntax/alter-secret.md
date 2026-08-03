# ALTER SECRET

The `ALTER SECRET` statement modifies an existing [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```sql
ALTER SECRET [IF EXISTS] secret_name
WITH (option = value[, ...])
```

* `IF EXISTS` — the statement does not return an error if the secret does not exist; in this case, it is a no-op.
* `secret_name` — the name of the secret to modify.
* `option` — command option:
  * `value` — string with the secret value.

## Permissions {#permissions}

Modifying a secret requires the [right](grant.md#permissions-list) `ALTER SCHEMA`.

## Examples {#examples}

Change the value of secret `secret_name` to `secret_value_new`:

```sql
ALTER SECRET secret_name WITH (value = "secret_value_new");
```

Change the value of secret `secret_name` to `secret_value_new` only if it exists; if it does not exist, the statement is a no-op:

```sql
ALTER SECRET IF EXISTS secret_name WITH (value = "secret_value_new");
```

## See also {#see-also}

* [CREATE SECRET](create-secret.md)
* [DROP SECRET](drop-secret.md)
