# Updating configuration of manually deployed {{ ydb-short-name }} clusters

When manually deploying a {{ ydb-short-name }} cluster, configuration management is performed through [YDB CLI](../../../reference/ydb-cli/index.md). This article covers methods for changing cluster configuration after initial deployment.

## Basic configuration operations

### Getting current configuration

To get the current cluster configuration, use the command:

```bash
ydb -e grpcs://<endpoint>:2135 admin cluster config fetch > config.yaml
```

Use the address of any cluster node as `<endpoint>`.

### Applying new configuration

To upload updated configuration to the cluster, use the following command:

```bash
ydb -e grpcs://<endpoint>:2135 admin cluster config replace -f config.yaml
```

Some configuration parameters are applied on the fly after executing the command, however some require performing the [cluster restart procedure](../../../maintenance/manual/node_restarting.md).