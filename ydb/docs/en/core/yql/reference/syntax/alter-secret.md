# ALTER SECRET

The `ALTER SECRET` statement modifies an existing [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```sql
ALTER SECRET secret_name
WITH (option = value[, ...])
```

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

## See also {#see-also}

* [CREATE SECRET](create-secret.md)
* [DROP SECRET](drop-secret.md)
