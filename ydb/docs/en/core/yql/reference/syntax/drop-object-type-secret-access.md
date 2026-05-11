# DROP OBJECT (TYPE SECRET_ACCESS)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Deletes the specified access rule for a [secret](../../../concepts/datamodel/secrets.md#secret_access).

If no rule with that name exists, an error is returned.

## Example

```yql
DROP OBJECT (TYPE SECRET_ACCESS) `MySecretName:another_user`;
```
