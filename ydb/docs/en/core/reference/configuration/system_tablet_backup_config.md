# system_tablet_backup_config

The [system tablet backup](../../devops/backup-and-recovery/system-tablet-backup.md) mechanism provides incremental copying of cluster metadata — such as [Hive](../../concepts/glossary.md#hive), [BSController](../../concepts/glossary.md#ds-controller), and [SchemeShard](../../concepts/glossary.md#scheme-shard) — to the local file system of cluster hosts. Backup behavior is configured in the `system_tablet_backup_config` section of the {{ ydb-short-name }} configuration.

## Syntax

```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
    exclude_tablet_ids:
        - 72057594037968897
    max_backups_limit: 3
```

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `filesystem.path` | — | Absolute path to a directory on the host's local file system for storing backups. The directory must be writable by the {{ ydb-short-name }} process on every host where system tablets run. Only local file systems are supported; network file systems, such as NFS, are not supported. |
| `exclude_tablet_ids` | — | List of system tablet IDs to exclude from backup, for example, to reduce disk load or save space. |
| `max_backups_limit` | `3` | Maximum number of backups of one tablet stored on a host. After a new backup is successfully created, the oldest one is automatically deleted when the limit is exceeded. |
