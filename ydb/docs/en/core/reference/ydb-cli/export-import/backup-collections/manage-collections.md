# Manage backup collections

Inspect

```bash
ydb admin backup collection list
ydb admin backup collection describe --name <name>
```

Retention and cleanup

```bash
ydb admin backup retention apply --collection <name>
```

Verification

```bash
ydb admin backup verify --collection <name> [--backup-id <id>]
```

Observability

- Metrics and logs are TBA; document names as they stabilize.
