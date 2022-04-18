### Connecting to a local database

If you deployed a local YDB using a scenario for self-hosted deployment [in Docker](../../self_hosted/ydb_docker.md) with the suggested configuration, you can check a YDB connection using the command:

```bash
{{ ydb-cli }} -e grpc://localhost:2136 -d /local scheme ls
```

