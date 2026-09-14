# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` creates a [resource pool classifier](../../../concepts/glossary.md#resource-pool-classifier.md).

## Syntax


```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```


- `name` — the name of the resource pool classifier being created. It must be unique. The name must not contain characters prohibited for schema objects.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — allows you to set parameter values that define the behavior of the resource pool classifier.

### Parameters

* `RANK` (Int64) — an optional field that specifies the selection order of the resource pool classifier. If the value is not specified, the maximum existing `RANK` is taken and 1000 is added to it. Valid values: a unique number in the range $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — a required field that specifies the name of the resource pool to which queries that meet the classifier criteria will be sent.
* `MEMBER_NAME` (String) — an optional field that determines which user or group of users will be sent to the specified resource pool. The value is compared with the user's SID or any group SID from their authentication token; see [below](#member-name-format) for details on the format. If the field is not specified, the classifier ignores `MEMBER_NAME`, and classification is performed based on other criteria.

### MEMBER_NAME format {#member-name-format}

`MEMBER_NAME` is compared character by character with the user's [SID](../../../concepts/glossary.md#access-sid) or any group SID from their authentication token. The SID format depends on how the user logged into the system.

- **Built-in users {{ ydb-short-name }} (login/password)** — the SID matches the username, without a suffix. For example, `user1`. For more information, see [{#T}](../../../security/authentication.md#static-credentials).
- **Cloud users (Access Service)** — the SID has the form `<subject_id>@as`, where `<subject_id>` is the user ID in IAM. The suffix is set by the [`access_service_domain`](../../../reference/configuration/auth_config.md#iam-auth-config) parameter (default `as`). For example, `ajeb89hv69nujke769fa@as`. For more information, see [{#T}](../../../security/authentication.md#iam).
- **LDAP** — the SID has the form `<login>@<domain>`, where the domain is set by the [`ldap_authentication_domain`](../../../reference/configuration/auth_config.md#ldap-auth-config) parameter (default `ldap`). For example, `user1@ldap`. For more information, see [{#T}](../../../security/authentication.md#ldap).
- **External identity providers (OIDC)** — the SID has the form `<login>@<domain>`, where the domain is set by the `external_idp_authentication_domain` parameter in the [authentication configuration](../../../reference/configuration/auth_config.md) (default `sso`). For example, `user1@sso`.

You can specify either the SID of a specific user or the SID of a group. The group `all-users@well-known` is automatically added to all authenticated users — it is convenient to use if you need to direct queries from all authenticated clients to the pool.

## Notes {#remarks}

If `RANK` is not specified in the DDL for creating a resource pool classifier, it will be assigned the default value $RANK = MAX(existing_ranks) + 1000$. All `RANK` values must be unique to ensure a strictly deterministic order of resource pool selection in case of conflicting conditions. This behavior is chosen to allow adding new resource pool classifiers between existing ones.

It is also possible to have a classifier that references a non-existent resource pool or one to which the user does not have access. In such a case, such classifiers will be skipped.

For limitations on the number of classifiers, see the [limitations](../../../../concepts/limits-ydb#resource_pool) page.

## Permissions

The [permission](./grant.md#permissions-list) `ALL` on the database is required.

Example of granting such a permission:


```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```


## Examples {#examples}


```yql
CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
    RANK=1000,
    RESOURCE_POOL="olap",
    MEMBER_NAME="user1@domain"
)
```


In the example above, a resource pool classifier named `olap_classifier` is created, which directs queries from user `user1@domain` to a resource pool named `olap`. Queries from all other users will be sent to the resource pool `default`, provided that no other resource pool classifiers exist.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
