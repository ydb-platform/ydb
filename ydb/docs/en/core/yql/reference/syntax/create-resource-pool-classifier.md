# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` creates a [resource pool classifier](../../../concepts/glossary.md#resource-pool-classifier).

## Syntax

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

- `name` — the name of the resource pool classifier being created. It must be unique. The name must not contain characters prohibited for schema objects.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — allows you to set parameter values that define the behavior of the resource pool classifier.

### Common parameters

* `RANK` (Int64) — an optional field that specifies the selection order of the resource pool classifier. If the value is not specified, the maximum existing `RANK` is taken and 1000 is added to it. Valid values: a unique number in the range $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — the name of the resource pool to which requests matching the classifier's predicates are directed.
* `ACTION` (Enum) — the action applied to a request when the classifier matches. Allowed value — `reject`: the request is rejected, and the user receives an error of the form `Request is rejected by classifier '<name>' (rank=<rank>)`.

{% note info %}

The `RESOURCE_POOL` and `ACTION` parameters are mutually exclusive: exactly one of them must be specified in a classifier.

{% endnote %}

### Predicate parameters

A predicate is a condition checked for an incoming request. A classifier matches if **all** of its predicates are satisfied (logical **AND**). To express **OR** logic, create multiple classifiers with different `RANK` values. Classifiers are processed in ascending order of `RANK`; processing stops at the first match — the request has that classifier's `ACTION` applied or is directed to its `RESOURCE_POOL`.

Predicate parameters are optional. A classifier without any predicates matches any request — this is useful for a "catch-all" classifier with the maximum `RANK`, for example, to direct all unclassified traffic to a specific pool (`RESOURCE_POOL`) or to reject it (`ACTION='reject'`).

{% note warning %}

Be careful when creating a "catch-all" classifier: all classifiers following it by `RANK` will never fire — processing stops at the first match, and the "catch-all" always matches.

{% endnote %}

List of predicates:

* `MEMBER_NAME` (String) — the SID of the user or group on whose behalf the request was made. See [below](#member-name) for details.
* `HAS_PATH` (String) — path to a YDB object accessed by the request; supports wildcards `*` and `?`. See [below](#has-path) for details.
* `HAS_APP_NAME` (String) — client application identifier. See [below](#has-app-name) for details.
* `HAS_FULL_SCAN` (String) — path to an object for which a full scan is expected; supports wildcards `*` and `?`. See [below](#has-full-scan) for details.
* `HAS_STREAM` (Bool) — indicates whether the request is streaming. See [below](#has-stream) for details.

#### MEMBER_NAME {#member-name} {#member-name-format}

`MEMBER_NAME` is compared character by character with the user's [SID](../../../concepts/glossary.md#access-sid) or any group SID from their authentication token. The SID format depends on how the user logged into the system.

- **Built-in {{ ydb-short-name }} users (login/password)** — the SID matches the username, without a suffix. For example, `user1`. For more information, see [{#T}](../../../security/authentication.md#static-credentials).
- **Cloud users (Access Service)** — the SID has the form `<subject_id>@as`, where `<subject_id>` is the user ID in IAM. The suffix is set by the [`access_service_domain`](../../../reference/configuration/auth_config.md#iam-auth-config) parameter (default `as`). For example, `ajeb89hv69nujke769fa@as`. For more information, see [{#T}](../../../security/authentication.md#iam).
- **LDAP** — the SID has the form `<login>@<domain>`, where the domain is set by the [`ldap_authentication_domain`](../../../reference/configuration/auth_config.md#ldap-auth-config) parameter (default `ldap`). For example, `user1@ldap`. For more information, see [{#T}](../../../security/authentication.md#ldap).
- **External identity providers (OIDC)** — the SID has the form `<login>@<domain>`, where the domain is set by the `external_idp_authentication_domain` parameter in the [authentication configuration](../../../reference/configuration/auth_config.md) (default `sso`). For example, `user1@sso`.

You can specify either the SID of a specific user or the SID of a group. The `all-users@well-known` group is automatically added to all authenticated users — it is convenient when you need to direct queries from all authenticated clients to a pool.

**Example.** Direct requests from user `user1@ldap` to the `olap` pool:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_user WITH (
    RANK=100,
    RESOURCE_POOL='olap',
    MEMBER_NAME='user1@ldap'
);
```

#### HAS_PATH {#has-path}

`HAS_PATH` compares the paths of YDB objects accessed by the request against the specified mask. The mask supports wildcards: `*` — any sequence of characters, `?` — any single character. The predicate matches if at least one object in the request plan matches the mask.

**Example.** Direct requests to archive tables to the `pool_archive` pool:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_archive WITH (
    RANK=700,
    RESOURCE_POOL='pool_archive',
    HAS_PATH='/Root/db/archive/*'
);
```

#### HAS_APP_NAME {#has-app-name}

`HAS_APP_NAME` compares the value against the client application identifier. The value is passed by the client in gRPC request metadata via the `x-ydb-application-name` header; YDB SDKs provide a way to set it on the client side. Comparison is exact match (no wildcards).

{% note warning %}

The `HAS_APP_NAME` value is set by the client and is not authenticated by the server — do not use it as an access control mechanism. For effective isolation, combine it with `MEMBER_NAME` or direct unrecognized requests to a sandbox pool with strict limits.

{% endnote %}

Setting the application identifier on the client:

- **{{ ydb-short-name }} Embedded UI** — fixed value `ydb-ui`, set by the viewer and not user-configurable.
- **YDB CLI** — not supported: the client application identifier is not sent in requests.
- **YDB C++ SDK** — per request via the `Header` parameter of [`TRequestSettings`](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h): `settings.Header({{ NYdb::YDB_APPLICATION_NAME, "my-app" }})`, where the [`YDB_APPLICATION_NAME`](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h) constant equals `x-ydb-application-name`.
- **YDB Go SDK** — at the driver level via the [`WithApplicationName`](https://github.com/ydb-platform/ydb-go-sdk/blob/v3.151.1/options.go#L163) option in the `ydb.Open` call.
- **YDB Java SDK** — at the transport level via the [`GrpcTransportBuilder.withApplicationName`](https://github.com/ydb-platform/ydb-java-sdk/blob/v2.4.11/core/src/main/java/tech/ydb/core/grpc/GrpcTransportBuilder.java#L280) method.
- **YDB Python SDK** — no dedicated parameter; the value is set per request via a generic header: `settings.with_header("x-ydb-application-name", "my-app")` (the [`BaseRequestSettings.with_header`](https://github.com/ydb-platform/ydb-python-sdk/blob/3.31.4/ydb/settings.py#L66) method).

**Example.** Direct requests from the Embedded UI to the `pool_adhoc` pool:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_adhoc_ui WITH (
    RANK=200,
    RESOURCE_POOL='pool_adhoc',
    HAS_APP_NAME='ydb-ui'
);
```

#### HAS_FULL_SCAN {#has-full-scan}

`HAS_FULL_SCAN` identifies requests that contain a full scan of the specified objects. A full scan is a table read without a key or key-range constraint. The argument is a path mask supporting wildcards `*` and `?`; the predicate matches if there is at least one such object in the request plan. Both [row-oriented](../../../concepts/glossary.md#row-oriented-table) and [column-oriented](../../../concepts/glossary.md#column-oriented-table) tables are supported. Objects to which the full-scan concept does not apply (for example, topics) are not considered.

Details of full-scan detection:

- **`LIMIT` does not cancel a full scan.** Without a key condition, the physical plan processes the entire table; `LIMIT` only limits the size of the result.
- **Secondary indexes.** A full scan of a secondary index's implementation table is counted against its own path. For example, for the table

    ```yql
    CREATE TABLE orders (
        Id Uint64 NOT NULL,
        Status Utf8,
        PRIMARY KEY (Id),
        INDEX by_status GLOBAL ON (Status)
    );
    ```

    the index table has path `/Root/orders/by_status/indexImplTable`. The query `SELECT * FROM orders VIEW by_status` performs a full scan of the index table — the main `/Root/orders` is not scanned. Therefore:

    - `HAS_FULL_SCAN='/Root/orders'` — **will not match**;
    - `HAS_FULL_SCAN='/Root/orders/by_status/indexImplTable'` or `HAS_FULL_SCAN='/Root/orders/*'` — **will match**.

**Example.** Reject requests that cause a full scan of the orders archive:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_fullscan_reject WITH (
    RANK=100,
    ACTION='reject',
    HAS_FULL_SCAN='/Root/db/orders_archive/*'
);
```

#### HAS_STREAM {#has-stream}

`HAS_STREAM` determines whether a request is [streaming](create-streaming-query.md) — that is, performs a long-running continuous read and/or write over data streams. Allowed values:

- `true` — the classifier matches streaming requests;
- `false` — the classifier matches non-streaming requests.

**Example.** Direct streaming requests to the `pool_stream` pool:

```yql
CREATE RESOURCE POOL CLASSIFIER cl_stream WITH (
    RANK=500,
    RESOURCE_POOL='pool_stream',
    HAS_STREAM=true
);
```

## Notes {#remarks}

If `RANK` is not specified in the DDL for creating a resource pool classifier, it will be assigned the default value $RANK = MAX(existing\_ranks) + 1000$. All `RANK` values must be unique to ensure a strictly deterministic order of resource pool selection in case of conflicting conditions. This behavior is chosen to allow adding new resource pool classifiers between existing ones.

It is also possible to have a classifier that references a non-existent resource pool or one to which the user does not have access. In that case, it is skipped.

For limitations on the number of classifiers, see the [limitations](../../../concepts/limits-ydb.md#resource_pool) page.

## Permissions

The [permission](./grant.md#permissions-list) `USE` on the database is required.

Example of granting such a permission:

```yql
GRANT 'USE' ON `/my_db` TO `user1@domain`;
```

## Examples {#examples}

Below is a combined example that composes several classifiers and predicates: rejecting full scans of archive tables, isolating streaming requests, and dedicating a pool for interactive admin queries from the Embedded UI.

Creating resource pools:

```yql
CREATE RESOURCE POOL pool_stream WITH (
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=20
);

CREATE RESOURCE POOL pool_adhoc_admin WITH (
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=10
);
```

Creating classifiers:

```yql
-- Reject full scans of archive tables.
CREATE RESOURCE POOL CLASSIFIER cl_fullscan_reject WITH (
    RANK=100,
    ACTION='reject',
    HAS_FULL_SCAN='/Root/db/orders_archive/*'
);

-- Direct streaming requests to a dedicated pool.
CREATE RESOURCE POOL CLASSIFIER cl_stream WITH (
    RANK=200,
    RESOURCE_POOL='pool_stream',
    HAS_STREAM=true
);

-- Admin requests from the Embedded UI — into the interactive-queries pool.
-- AND condition: both MEMBER_NAME and HAS_APP_NAME must match.
CREATE RESOURCE POOL CLASSIFIER cl_adhoc_admin WITH (
    RANK=300,
    RESOURCE_POOL='pool_adhoc_admin',
    MEMBER_NAME='admin',
    HAS_APP_NAME='ydb-ui'
);
```

Classifiers are processed in ascending order of `RANK`; the first matching classifier is applied to the request. A request that does not match any classifier is directed to the `default` pool.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
