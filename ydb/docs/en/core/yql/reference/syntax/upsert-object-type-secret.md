# UPSERT OBJECT (TYPE SECRET)

{% note warning %}

The syntax for managing secrets will change in future {{ ydb-full-name }} releases.

{% endnote %}

To change the contents of a [secret](../../../concepts/datamodel/secrets.md), use the following statement:

```yql
UPSERT OBJECT `secret_name` (TYPE SECRET) WITH value = `secret_value`;
```

Where:

* `secret_name` — Name of the secret.
* `secret_value` — Secret payload.

## Example

The following statement sets the secret named `MySecretName` to `MySecretData`:

```yql
UPSERT OBJECT `MySecretName` (TYPE SECRET) WITH value = `MySecretData`;
```
