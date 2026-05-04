# Secrets

Various access credentials are used for authentication in external systems. These credentials are stored in separate objects called secrets. Secrets are only available for writing and updating; their values cannot be retrieved.
In {{ ydb-full-name }}, secrets are used, for example, in [federated queries](../query_execution/federated_query/index.md) and [data transfers](../transfer.md).

## Syntax {#syntax}

The following YQL operators are used to manage secrets:

<<<<<<< HEAD
To work with external data sources in {{ ydb-full-name }}, [federated queries](../federated_query/index.md) are used. Federated queries utilize various access credentials for authentication in external systems. These credentials are stored in separate objects called secrets. Secrets are only available for writing and updating; their values cannot be retrieved.
=======
- [CREATE SECRET](../../yql/reference/syntax/create-secret.md) — create a secret.
- [ALTER SECRET](../../yql/reference/syntax/alter-secret.md) — modify an existing secret.
- [DROP SECRET](../../yql/reference/syntax/drop-secret.md) — delete a secret.
>>>>>>> 4a883929856 (DOCSUP-123457: Перевод секретов. Организация процесса перевода (1 архив) (1 шт.) (#34191))

## Usage {#secret-usage}

Examples of using secrets and working with them are provided in the following sections:

* [{#T}](../../yql/reference/recipes/ttl.md)
* [{#T}](../../recipes/import-export-column-tables.md)

## Access management {#secret_access}

Secrets are schema objects, so rights to them are granted using the [GRANT](../../yql/reference/syntax/grant.md) command and revoked using the [REVOKE](../../yql/reference/syntax/revoke.md) command. To use a secret in a query, for example, when creating an [external data source](../../yql/reference/syntax/create-external-data-source.md) or [data transfer](../../yql/reference/syntax/create-transfer.md), the [right](../../yql/reference/syntax/grant.md#permissions-list) `SELECT ROW` is required.
