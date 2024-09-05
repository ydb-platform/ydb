# Scenarios

## Performing {{ ydb-short-name }} cluster restart using ydbops

{% include [warning.md](_includes/warning.md) %}

`ydbops` can be used to perform the rolling restart operation: restarting all or some of {{ ydb-short-name }} cluster nodes while maintaining cluster availability. Why this is not trivial and requires a special utility is explained in the [article about maintenance without downtime](../../devops/manual/maintenance-without-downtime).

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

```bash
ydbops restart
```

### Restarting only storage or tenant nodes

It is possible to restart storage nodes only:

```bash
ydbops restart --storage
```

Or tenant nodes only:

```bash
ydbops restart --tenant
```

Additionally, only specific tenants may be restarted by specifying `--tenant-list`:

```bash
ydbops restart --tenant-list=</domain/database_name_1>,</domain/database_name_2>,...
```

### Restarting only specific nodes

It is possible to restart nodes only on specific hosts by supplying FQDNs with the `--hosts` option:

```bash
ydbops restart --hosts=<node1.some.zone>,<node2.some.zone>
```

Or by supplying node ids directly:

```bash
ydbops restart --hosts=1,2,3
```

### Restarting based on node uptime

It is possible to restart only the nodes that have specific uptime by using the `--started` option:

The option argument needs to be enclosed in quotes, otherwise shell might interpret `>` or `<` signs as stream redirections. See the example:

```bash
ydbops restart --started '>2024-03-13T17:00:00Z'
```

It might be convenient to restart only the nodes that have been up for over a few minutes, as the others have just restarted recently.

### Restarting based on {{ ydb-short-name }} version

It is possible to restart only the nodes whose version is equal to (`==`), not equal to (`!=`), greater than (`>`), or less than (`<`) the desired version.

The option argument needs to be enclosed in quotes; otherwise, the shell might interpret the `>` or `<` signs as stream redirections. See the examples:

```bash
ydbops restart --version '>24.1.2'
ydbops restart --version '<24.1.2'
ydbops restart --version '!=24.1.2'
ydbops restart --version '==24.1.2'
```
