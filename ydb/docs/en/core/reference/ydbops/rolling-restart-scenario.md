## Performing {{ ydb-short-name }} cluster restart using ydbops

{% note info %}

The article is being updated. Expect new content to appear and minor fixes to existing content.

{% endnote %}

`ydbops` can be used to perform the rolling restart operation: restarting all or some of {{ ydb-short-name }} cluster nodes while maintaining cluster availability. Why this is not trivial and requires a special utility is explained in [{#T}](../../devops/manual/maintenance-without-downtime)

The subcommand responsible for this operation is `ydbops restart`.

## General algorithm

There are multiple options for `ydbops restart` that act as filters. Filters are implicitly connected with a logical "and", meaning that if you supply multiple filters, only the nodes that satisfy all of them at once will be targeted. Therefore, specifying no filters targets all nodes for restart.

There are two special filters that are an exception to this rule, `--storage` and `--tenant`:
- Specifying only `--storage` will filter storage nodes only.
- Specifying only `--tenant` will filter tenant nodes only.
- However, specifying both selects all nodes and is equivalent to not specifying these two filters.

The algorithm will always work in two phases:

- First, it will determine whether any storage nodes fit the restart filters and restart these nodes only.
- After all storage nodes have been restarted or it has been determined that no storage nodes are selected, the process repeats for tenant nodes.

## Examples

The following examples assume you have specified all the required connection options (such as endpoint or credentials).

### Restarting all nodes in the cluster

The command will restart all the nodes in the cluster: all storage nodes first, followed by all tenant nodes.

```
ydbops restart 
```

### Restarting only storage\tenant nodes

It is possible to restart storage nodes only:

```
ydbops restart --storage
```

Or tenant nodes only:

```
ydbops restart --tenant
```

Additionally, only specific tenants may be restarted by specifying `--tenant-list`:

```
ydbops restart --tenant-list=/domain/database_name_1,/domain/database_name_2
```

### Restarting only specific nodes

It is possible to restart only specific nodes by supplying FQDNs with a `--hosts` option:

```
ydbops restart --hosts=node1.some.zone,node2.some.zone
```

Or by supplying node ids directly:

```
ydbops restart --hosts=1,2,3
```

### Restarting based on node uptime

It is possible to restart only the nodes that have specific uptime by using the `--started` option:

```
ydbops restart --started >2024-03-13T17:00:00Z
```

It might be convenient to restart only the nodes that have been up for over a few minutes, as the others have just restarted recently.

### Restarting based on {{ ydb-short-name }} version

It is possible to restart only the nodes which version is equal, not equal, more, or less than desired:

```
ydbops restart --version !=24.1.2
```
