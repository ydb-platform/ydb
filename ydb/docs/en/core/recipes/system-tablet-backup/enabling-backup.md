# Enabling and configuring system tablet backup {#enabling-backup}

{% note info %}

For conceptual information about the mechanism, see [Backup of system tablets](../../concepts/backup.md#system-tablet-backup).

{% endnote %}

{% note warning %}

Only local file systems are supported as backup storage. Network file systems such as NFS are not supported and may corrupt backups or cause tablet failures.

{% endnote %}

By default, system tablet backup is **disabled** because it requires storage configuration. {{ ydb-short-name }} nodes usually do not use the local file system, so enabling backup requires a deliberate administrator decision.

To enable backup, add the `system_tablet_backup_config` section to the [cluster configuration](../../reference/configuration/index.md):

```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
```

The `path` parameter is the absolute path to a directory on the host's local file system for storing backup copies.

{% note warning %}

The backup directory must be writable by the {{ ydb-short-name }} process on every host where system tablets run. Make sure each host has enough free disk space for several backup copies of all system tablets. For large production environments with dozens of storage nodes, at least 5 GB is recommended.

{% endnote %}

After changing the configuration, restart the cluster nodes where system tablets may run. Backup will start automatically.

{% note warning %}

By default, system tablets may run on any static cluster node, including nodes whose disks are part of a [static group](../../concepts/glossary.md#static-group). Backups are stored locally on the hosts where system tablets run. This means that if static group hosts are lost, the backups needed to restore the static group may be lost as well.

Configure the `bootstrap_config` section in the [cluster configuration](../../reference/configuration/index.md) so that system tablets run on nodes that are not part of the static group. In that case, backups are stored separately from static group data.

{% endnote %}
