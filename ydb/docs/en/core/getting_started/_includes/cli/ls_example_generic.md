For example, if:

* Endpoint: `grpc://ydb.example.com:2136`.
* Database name: `/john/db1`.
* The database doesn't require authentication, or the proper environment variable has been set, as described [here](../../auth.md).
* A database has just been created and contains no objects.

the command will look like this:

```bash
{{ ydb-cli }} -e grpc://ydb.example.com:2136 -d /john/db1 scheme ls
```

The command execution result on the created empty database:

```text
.sys_health .sys
```

