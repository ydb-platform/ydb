# Updating configuration of {{ ydb-short-name }} clusters deployed with Ansible

During [initial deployment](initial-deployment.md), the Ansible playbook used the provided config file to create the initial cluster configuration. Technically, it generates two variants of the original config file and deploys them to all hosts via Ansible's mechanism for cross-server file copy. This article explains which options are available to change the cluster's configuration after the initial deployment.

## Update configuration via Ansible playbook

[ydb-ansible](https://github.com/ydb-platform/ydb-ansible) repository contains a playbook called `ydb_platform.ydb.update_config` that can be used to update {{ ydb-short-name }} cluster's configuration. Go to the same directory used for the [initial deployment](initial-deployment.md), edit `files/config.yaml` as needed, and then run this playbook:

```bash
ansible-playbook ydb_platform.ydb.update_config
```

The playbook deploys the new version of the config files and then performs a [rolling restart](restart.md).

### Filter by node type

Tasks in the `ydb_platform.ydb.update_config` playbook are tagged with node types, so you can use Ansible's tags functionality to filter nodes by their kind. 

These two commands are equivalent and will change the configuration of all [storage nodes](../../concepts/glossary.md#storage-node):

```bash
ansible-playbook ydb_platform.ydb.update_config --tags storage
ansible-playbook ydb_platform.ydb.update_config --tags static
```

These two commands are equivalent and will change the configuration of all [database nodes](../../concepts/glossary.md#database-node):
```bash
ansible-playbook ydb_platform.ydb.update_config --tags database
ansible-playbook ydb_platform.ydb.update_config --tags dynamic
```

### Skip restart

There's a `no_restart` tag to only deploy the config files and skip the cluster restart. This might be useful if the cluster will be [restarted](restart.md) later manually or as part of some other maintenance tasks. Example:

```bash
ansible-playbook ydb_platform.ydb.update_config --tags no_restart
```

## Dynamic configuration

{{ ydb-short-name }} has its own configuration management mechanism called [dynamic configuration](../../maintenance/manual/dynamic-config.md). It is agnostic of the chosen cluster deployment method and allows for flexible cluster configuration even in multitenant environments.

Consider switching to dynamic configuration if manually deploying config files and doing a complete rolling restart on any change is no longer feasible.