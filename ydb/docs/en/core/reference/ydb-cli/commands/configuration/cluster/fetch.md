# admin cluster config fetch

With the `admin cluster config fetch` command, you can retrieve the current [dynamic](../../../../../maintenance/manual/dynamic-config.md) configuration of the YDB cluster.

General command syntax:

```bash
ydb [global options...] admin cluster config fetch
```

* `global options` — Global parameters.

View the description of the dynamic configuration fetch command:

```bash
ydb admin cluster config fetch --help
```

## Examples {#examples}

Fetch the current dynamic configuration of the cluster:

```bash
ydb --endpoint grpc://localhost:2135 admin cluster config fetch > config.yaml
```
