# Secrets

To work with external data sources in {{ ydb-full-name }}, [federated queries](../federated_query/index.md) are used. Federated queries utilize various access credentials for authentication in external systems. These credentials are stored in separate objects called secrets. Secrets are only available for writing and updating; their values cannot be retrieved.

{% note warning %}

The current syntax for working with secrets is temporary and will be changed in future releases of {{ ydb-full-name }}.

{% endnote %}

## Creating secrets { #create_secret }

Secrets are created using an SQL query:

```sql
CREATE OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```

## Access management { #secret_access }

All rights to use the secret belong to its creator. The creator can grant another user read access to the secret through [access management](#secret_access) for secrets.

Special objects called `SECRET_ACCESS` are used to manage access to secrets. To grant permission to use the secret `MySecretName` to the user `another_user`, a `SECRET_ACCESS` object named `MySecretName:another_user` must be created.

```sql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS)
```