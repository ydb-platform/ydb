# DROP OBJECT (TYPE SECRET)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Deletes the specified [secret](../../../concepts/datamodel/secrets.md).

If no secret with that name exists, an error is returned.

## Example

```yql
DROP OBJECT my_secret (TYPE SECRET);
```
