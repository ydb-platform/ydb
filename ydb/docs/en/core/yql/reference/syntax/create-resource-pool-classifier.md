# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` creates a [resource pool classifier](../../../dev/resource-consumption-management.md).

## Syntax

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

- `name` — the name of the resource pool classifier being created. It must be unique. The name must not contain characters that are invalid for schema objects.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — sets parameters that define the behavior of the resource pool classifier.

### Parameters

* `RANK` (Int64) — optional; determines the order in which resource pool classifiers are considered. If omitted, the maximum existing `RANK` is taken and 1000 is added. Allowed values: a unique number in $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — required; the name of the resource pool to which queries matching the classifier are sent.
* `MEMBER_NAME` (String) — optional; which user or group of users is routed to the specified resource pool. If omitted, the classifier ignores `MEMBER_NAME` and classification uses other criteria.

## Remarks {#remarks}

If `RANK` is omitted in the DDL for a new resource pool classifier, the default is $RANK = MAX(existing\_ranks) + 1000$. All `RANK` values must be unique so that the choice of resource pool remains strictly deterministic when conditions conflict. This behavior allows inserting new resource pool classifiers between existing ones.

A classifier may reference a resource pool that does not exist or that the user cannot access. Such classifiers are skipped.

For limits on the number of classifiers, see [Database limits](../../../concepts/limits-ydb.md#resource_pool).

## Permissions

You need the [`ALL`](./grant.md#permissions-list) permission on the database.

Example:

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

The example above creates a resource pool classifier named `olap_classifier` that routes queries from user `user1@domain` to the resource pool named `olap`. Queries from all other users go to the `default` resource pool, assuming no other resource pool classifiers exist.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
