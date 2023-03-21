# Cluster extension

You can extend a {{ ydb-short-name }} cluster by adding new nodes to its configuration. The instructions to extend the {{ ydb-short-name }} cluster installed manually on virtual machines or bare metal servers are provided below. {{ ydb-short-name }} cluster extension in a Kubernetes environment is performed by adjusting the {{ ydb-short-name }} Kubernetes operator configuration.

{{ ydb-short-name }} cluster extension is performed without suspending user access to the hosted databases. The expansion procedure includes the rolling restart of the cluster components, which is needed to apply the configuration changes. Some of the transactions running on the cluster at the moment may fail, and need to be repeated. Failing transactions are repeated automatically by the built-in error control and retry logic of  {{ ydb-short-name }} SDK.

## Preparing new servers

When new servers are used to host the static and dynamic nodes being added, each new {{ ydb-short-name }} server needs to be prepared in accordance to the procedures defined in the [cluster deployment instruction](../../deploy/manual/deploy-ydb-on-premises.md). In particular, the following actions need to be performed:

1. create the operating system user account to run {{ ydb-short-name }} processes;
1. install the {{ ydb-short-name }} software;
1. prepare and install the TLS key and certificate for the server;
1. copy the current {{ ydb-short-name }} cluster configuration file to the server.

New servers' TLS certificates must be prepared according to the [defined requirements](../../deploy/manual/deploy-ydb-on-premises.md#tls-certificates), and must be signed by the Certification Authority (CA) trusted by the existing hosts of the {{ ydb-short-name }} cluster being expanded.

## Adding dynamic nodes

Adding dynamic nodes to the cluster allows to increase the amount of compute resources (processor cores and RAM) used to execute user queries by {{ ydb-short-name }} cluster.

To add the dynamic node to the cluster, it is sufficient to start the operating system process serving that node, passing the path to the cluster configuration file, databasae name and the addresses of any three {{ ydb-short-name }} static nodes as command line parameters, as shown in the [cluster deployment instruction](../../deploy/manual/deploy-ydb-on-premises.md#start-dynnode).

After the successful start of the dynamic node, the information about it will be available on the [cluster monitoring page in the embedded UI](../embedded_monitoring/ydb_monitoring.md).

To remove the dynamic node from the cluster, it is enough to stop its process.

## Adding static nodes

Adding static nodes to the cluster allow to increase the input-output throughput and data storage capacity in the {{ ydb-short-name }} cluster.

To add the additional static nodes, perform the following sequence of steps:

1. Format the disks intended for {{ ydb-short-name }} data storage, using the procedure described in the [cluster deployment instruction](../../deploy/manual/deploy-ydb-on-premises.md#prepare-disks).

1. Update the [cluster configuration file](../../deploy/manual/deploy-ydb-on-premises.md#config):
    * include the definition of the hosts being added (into the `hosts` section) and their disk layouts (into the `host_configs` section);
    * specify the configuration version number as the `storage_config_generation: K` top-level parameter, where `K` is a change number (`K=0` or not specified at the initial installation, `K=1` at the first cluster expansion, `K=2` at the second expansion, and so on).

1. Distribute the updated cluster configuration file on all existing and new cluster nodes, replacing the older version of that file.

1. Perform the rolling restart of all existing static nodes, waiting for each node being restarted to come up and recover before restarting the next one.

1. Perform the rolling restart of all existing dynamic nodes.

1. Start the new static nodes on their corresponding servers.

1. Ensure that the new static nodes are shown on the [cluster monitoring page in the embedded UI](../embedded_monitoring/ydb_monitoring.md).

1. Obtain the authentication token to run the administrative commands using the {{ ydb-short-name }} CLI, for example:

    ```bash
    ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
        --user root auth get-token --force >ydbd-token-file
    ```

    The following parameters are used in the command above:
    * `node1.ydb.tech` - FQDN of any server hosting the existing static nodes;
    * `2135` - grpcs port number for the static nodes in the cluster;
    * `ca.crt` - path to CA certificate file being used;
    * `root` - administrative user login;
    * `ydbd-token-file` - file name to put the authentication token.

    {{ ydb-short-name }} CLI will interactively ask for password to authenticate the user account specified.

1. Allow the {{ ydb-short-name }} cluster to use the disks on the new static nodes for data storage, by running the following command on any cluster node:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
        admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
    echo $?
    ```

    The following parameters are used in the command above:
    * `ydbd-token-file` - path to file containing the authentication token;
    * `2135` - grpcs port number for the static nodes in the cluster;
    * `ca.crt` - path to CA certificate file being used.

    If the command listed above returns the error relating to the configuration version number mismatch, that means that the field `storage_config_generation` was not correctly specified in the cluster configuration file, and needs to be updated. Error message contains the expected configuration version number, which can be used to fix the cluster configuration file. The example error message looks like the following:

    ```protobuf
    ErrorDescription: "ItemConfigGeneration mismatch ItemConfigGenerationProvided# 0 ItemConfigGenerationExpected# 1"
    ```

1.  Add the additional storage groups to one or more database(s) within the cluster, by running the following command:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
        admin database /Root/testdb pools add ssd:1
    echo $?
    ```

    The following parameters are used in the command above:
    * `ydbd-token-file` - path to file containing the authentication token;
    * `2135` - grpcs port number for the static nodes in the cluster;
    * `ca.crt` - path to CA certificate file being used;
    * `/Root/testdb` - full path to the database;
    * `ssd:1` - name of the storage pool and the number of storage groups to be added.

1.  Ensure that the newly added storage groups are visible on the [cluster monitoring page in the embedded UI](../embedded_monitoring/ydb_monitoring.md).

The removal of static nodes from the {{ ydb-short-name }} cluster is performed using the [documented decomissioning procedure](../../administration/decommissioning.md).

In the event of fatal damage to the server supporting the static node, it has to be replaced with another server, having the same or larger storage capacity.
