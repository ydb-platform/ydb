# CREATE SECRET

The `CREATE SECRET` statement creates a [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```sql
CREATE [OR REPLACE] SECRET [IF NOT EXISTS] secret_name
WITH (option = value[, ...])
```

* `OR REPLACE` — if a secret with this name already exists, it will be replaced with a new definition.
* `IF NOT EXISTS` — the statement does not return an error if a secret with this name already exists; in this case, the existing secret remains unchanged.
* `secret_name` — the name of the secret to create.
* `option` — command option:
  * `value` — string with the secret value.
  * `inherit_permissions` — when enabled, [rights](grant.md) on the secret are inherited from the directory where the secret is created. When disabled, only the [right](grant.md#permissions-list) `DESCRIBE SCHEMA` is inherited from the directory. The secret owner gets all possible rights on it in any case. Default is `False`.

{% note warning %}

The `OR REPLACE` and `IF NOT EXISTS` clauses cannot be used simultaneously.

{% endnote %}

## Permissions {#permissions}

Creating a secret requires the [right](grant.md#permissions-list) `CREATE TABLE`.

When using `CREATE OR REPLACE SECRET` on an existing secret, the [right](grant.md#permissions-list) `ALTER SCHEMA` on the secret is required, since this form of `CREATE` modifies the secret. When the secret does not exist, `CREATE TABLE` on the parent directory is sufficient.

## Examples {#examples}

Create a secret in the database root named `secret_name` with value `secret_value`:

```sql
CREATE SECRET secret_name WITH (value = "secret_value");
```

Create a secret in directory `dir` in the database root named `secret_name` with value `secret_value`. If directory `dir` does not exist, it will be created:

```sql
CREATE SECRET `dir/secret_name` WITH (value = "secret_value");
```

Create a secret in the database root named `secret_name` with value `secret_value` with the same rights as the secret's parent directory:

```sql
CREATE SECRET secret_name WITH (value = "secret_value", inherit_permissions = True);
```

Create a secret named `secret_name` only if it does not already exist; if it exists, the existing secret will remain unchanged:

```sql
CREATE SECRET IF NOT EXISTS secret_name WITH (value = "secret_value");
```

Create or replace a secret named `secret_name`; if it exists, it will be replaced with a new definition:

```sql
CREATE OR REPLACE SECRET secret_name WITH (value = "secret_value");
```

## See also {#see-also}

* [ALTER SECRET](alter-secret.md)
* [DROP SECRET](drop-secret.md)
