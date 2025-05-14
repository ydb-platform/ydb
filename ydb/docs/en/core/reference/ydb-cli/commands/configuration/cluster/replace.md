# admin cluster config replace

With the `admin cluster config replace` command, you can upload a [dynamic configuration](../../../../../maintenance/manual/dynamic-config.md) to the {{ ydb-short-name }} cluster.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
ydb [global options...] admin cluster config replace [options...]
```

* `global options` — Global parameters.
* `options` — [Subcommand parameters](#options).

View the description of the dynamic configuration replacement command:

```bash
ydb admin cluster config replace --help
```

## Subcommand Parameters {#options}

#|
|| Name | Description ||
|| `-f`, `--filename` | Path to the file containing the configuration. ||
|| `--allow-unknown-fields`
| Allow unknown fields in the configuration.

If the flag is not set, unknown fields in the configuration result in an error.
    ||
|| `--ignore-local-validation`
| Ignore basic client-side configuration validation.

If the flag is not set, YDB CLI performs basic client-side configuration validation.
    ||
|#

## Examples {#examples}

Upload the dynamic configuration file to the cluster:

```bash
ydb admin cluster config replace --filename config.yaml
```

Upload the dynamic configuration file to the cluster, ignoring local applicability checks:

```bash
ydb admin cluster config replace -f config.yaml --ignore-local-validation
```

Upload the dynamic configuration file to the cluster, ignoring the check for unknown fields:

```bash
ydb admin cluster config replace -f config.yaml --allow-unknown-fields
```
