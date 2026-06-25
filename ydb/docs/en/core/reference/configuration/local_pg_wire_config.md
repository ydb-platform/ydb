# local_pg_wire_config

The `local_pg_wire_config` section of the {{ ydb-short-name }} configuration file enables and configures the embedded PostgreSQL wire protocol listener (pgwire).

{% note warning %}

PostgreSQL wire protocol support is an experimental feature. Keep it disabled on production clusters unless you explicitly need it for testing or migration.

{% endnote %}

## Description of parameters

| Parameter | Type | Default value | Description |
| --- | --- | --- | --- |
| `enable_local_pg_wire` | bool | `false` | Enables or disables the PostgreSQL wire protocol listener. |
| `listening_port` | int32 | `5432` | The port on which pgwire accepts connections. Used only when `enable_local_pg_wire` is `true`. |
| `address` | string | `::` | The address to bind the listener to. |
| `ssl_certificate` | string | — | PEM-encoded SSL certificate with a private key for TLS connections. |
| `tcp_not_delay` | bool | `true` | Enables the `TCP_NODELAY` socket option. |

## Example of a completed config

```yaml
local_pg_wire_config:
  enable_local_pg_wire: true
  listening_port: 5432
  address: "::"
```

You can also enable pgwire via the hidden `ydbd` command-line option `--pgwire-port`, which sets `enable_local_pg_wire` to `true` automatically.

In the local {{ ydb-short-name }} Docker image, pgwire is enabled by default through the `YDB_ENABLE_LOCAL_PGWIRE` environment variable (default `1`). This is separate from `YDB_EXPERIMENTAL_PG`, which enables additional experimental PostgreSQL feature flags and is off by default.
