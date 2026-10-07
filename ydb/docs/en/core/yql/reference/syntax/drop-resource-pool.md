# DROP RESOURCE POOL

`DROP RESOURCE POOL` removes a [resource pool](../../../concepts/glossary.md#resource-pool).

## Syntax

```yql
DROP RESOURCE POOL <name>
```

### Parameters

* `name` — name of the resource pool to drop.

## Permissions

The `REMOVE SCHEMA` [permission](grant.md#permissions-list) on the pool under `.metadata/workload_manager/pools` is required. Example:

```yql
GRANT 'REMOVE SCHEMA' ON `.metadata/workload_manager/pools` TO `user1@domain`;
```

## Examples

The following removes the resource pool named `olap`:

```yql
DROP RESOURCE POOL olap;
```

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool.md)
* [{#T}](alter-resource-pool.md)
