# Updating {{ ydb-short-name }} version on clusters deployed with Ansible

During the [initial deployment](initial-deployment.md), the Ansible playbook provides several options for selecting the {{ ydb-short-name }} server executable (`ydbd`). This article explains the available options for changing the cluster's [version](../../downloads/index.md) after the initial deployment.

{% note warning %}

{{ ydb-short-name }} has specific rules regarding version compatibility. It is essential to refer to the [version compatibility guide](../manual/upgrade.md#version-compatability) and [changelog](../../changelog-server.md) to correctly choose a new version to upgrade to and prepare for any nuances.

{% endnote %}

## Update executables via Ansible playbook

The [ydb-ansible](https://github.com/ydb-platform/ydb-ansible) repository contains a playbook called `ydb_platform.ydb.update_executable` that can be used to upgrade or downgrade a {{ ydb-short-name }} cluster to another version. Navigate to the same directory used for the [initial deployment](initial-deployment.md), edit `inventory/50-inventory.yaml` to specify the target {{ ydb-short-name }} version to install (typically, via the `ydb_version` or `ydb_git_version` variables), and then run this playbook:

```bash
ansible-playbook ydb_platform.ydb.update_executable
```

The playbook obtains a new binary and then deploys it to the cluster via Ansible's cross-server copying. After that, it performs a [rolling restart](restart.md).

### Filter by node type

Tasks in the `ydb_platform.ydb.update_executable` playbook are tagged with node types, so you can use Ansible's tags functionality to filter nodes by their kind.

These two commands are equivalent and will change the configuration of all [storage nodes](../../concepts/glossary.md#storage-node):

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags storage
ansible-playbook ydb_platform.ydb.update_executable --tags static
```

These two commands are equivalent and will change the configuration of all [database nodes](../../concepts/glossary.md#database-node):

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags database
ansible-playbook ydb_platform.ydb.update_executable --tags dynamic
```

### Skip restart

There's a `no_restart` tag to only deploy the executable files and skip the cluster restart. This might be useful if the cluster will be [restarted](restart.md) later manually or as part of some other maintenance tasks. Example:

```bash
ansible-playbook ydb_platform.ydb.update_executable --tags no_restart
```