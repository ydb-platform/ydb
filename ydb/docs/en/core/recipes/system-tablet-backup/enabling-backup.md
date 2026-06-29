# Enabling and configuring backup of system tablets {#enabling-backup}

{% note info %}

For conceptual information about the mechanism, see the [System tablet backup](../../concepts/backup.md#system-tablet-backup) section.

{% endnote %}

{% note warning %}

Only local file systems are supported as backup storage. Network file systems, such as NFS, are not supported and may cause backup corruption or tablet failures.

{% endnote %}

By default, system tablet backup is **disabled** because it requires storage configuration. {{ ydb-short-name }} nodes typically do not use a local file system, so enabling it requires an informed decision by the administrator.

To enable it, add the `system_tablet_backup_config` section to the [cluster configuration](../../reference/configuration/index.md):


```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
```


The `path` parameter is an absolute path to a directory on the host's local file system for storing backups.

{% note warning %}

The backup directory must be writable by the {{ ydb-short-name }} process on each host where system tablets run. Ensure that each host's disk has enough free space to store several backups of all system tablets. For large production environments consisting of dozens of storage nodes, at least 5 GB is recommended.

{% endnote %}

After changing the configuration, restart the cluster nodes where system tablets may run. Backup will start automatically.

{% note warning %}

By default, system tablets can run on any static nodes of the cluster, including those whose disks are part of a [static group](../../concepts/glossary.md#static-group). Backups are stored locally on the hosts where system tablets run. This means that if static group hosts are lost, the backups that may be needed to restore the static group can also be lost.

It is recommended to configure the `bootstrap_config` section in the [cluster configuration](../../reference/configuration/index.md) so that system tablets run on nodes that are not part of a static group. In this case, backups will be stored separately from the static group data.

{% endnote %}
