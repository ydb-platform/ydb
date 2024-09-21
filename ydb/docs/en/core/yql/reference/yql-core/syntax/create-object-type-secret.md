# CREATE OBJECT (TYPE SECRET)

{% note warning %}

The syntax for managing secrets might change in future {{ydb-full-name}} releases.

{% endnote %}

The `CREATE OBJECT (TYPE SECRET)` statement creates a [secret](../../../concepts/datamodel/secrets.md).

## Syntax {#syntax}

```yql
CREATE OBJECT `secret_name` (TYPE SECRET) WITH value=`secret_value`;
```

### Parameters

* `secret_name` - the name of the secret.
* `secret_value` - the contents of the secret.

## Example {#examples}

The following statement creates a secret with the `MySecretName` name and `MySecretData` value.

```yql
CREATE OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```
