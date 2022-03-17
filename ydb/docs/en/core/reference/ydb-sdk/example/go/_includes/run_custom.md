To run the example using any available YDB database, you need to know the [Endpoint](../../../../../concepts/connect.md#endpoint) and [Database location](../../../../../concepts/connect.md#database).

If authentication is enabled in the database, you also need to choose the [authentication mode](../../../../../concepts/connect.md#auth-modes) and obtain secrets: a token or username/password.

Run the command as follows:

```bash
( export <auth_mode_var>="<auth_mode_value>" && cd ydb-go-examples && \
go run ./basic -ydb="<endpoint>?database=<database>" )
```

Where:

* `<endpoint>` is the [Endpoint](../../../../../concepts/connect.md#endpoint)
* `<database>` is the [DB location](../../../../../concepts/connect.md#database).
* `<auth_mode_var`> is the [Environment variable](../../../auth.md#env) that determines the authentication mode.
* `<auth_mode_value>` is the authentication parameter value for the selected mode.

For example:

```bash
( export YDB_ACCESS_TOKEN_CREDENTIALS="t1.9euelZqOnJuJlc..." && cd ydb-go-examples && \
go run ./basic -ydb="grpcs://ydb.example.com:2135?database=/somepath/somelocation" )
```

{% include [../../_includes/pars_from_profile_hint.md](../../_includes/pars_from_profile_hint.md) %}
