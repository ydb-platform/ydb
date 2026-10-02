# CREATE OBJECT (TYPE SECRET)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

The following SQL statement creates a [secret](../../../concepts/datamodel/secrets.md):

```yql
CREATE OBJECT <secret_name> (TYPE SECRET) WITH value = "<secret_value>";
```

Where:

* `secret_name` - the name of the secret.
* `secret_value` - the contents of the secret.

## Example {#examples}

The following statement creates a secret named `MySecretName` with `MySecretData` as a value.

```yql
CREATE OBJECT MySecretName (TYPE SECRET) WITH value = "MySecretData";
```
