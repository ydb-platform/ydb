# Cluster Expansion

{% include [deprecated](_includes/deprecated.md) %}

You can expand a {{ ydb-short-name }} cluster by adding new nodes to the cluster configuration. Below are the necessary actions for expanding a {{ ydb-short-name }} cluster installed manually on virtual machines or physical servers. Cluster expansion in a Kubernetes environment is performed by adjusting the {{ ydb-short-name }} controller settings for Kubernetes.

Expanding a {{ ydb-short-name }} cluster does not require suspending user access to databases. When expanding the cluster, its components are restarted to apply configuration changes, which in turn may lead to the need to retry transactions running on the cluster. Transaction retries are performed automatically through applications using {{ ydb-short-name }} SDK capabilities for error control and operation retry.

## Preparing New Servers {#add-host}

When placing new static or dynamic cluster nodes on new servers that were not previously part of the expanded {{ ydb-short-name }} cluster, each new server must install {{ ydb-short-name }} software according to the procedures described in the [cluster deployment instructions](../../deployment-options/manual/initial-deployment.md). In particular, it is necessary to:

1. Create an account and group in the operating system for the {{ ydb-short-name }} service
1. Install {{ ydb-short-name }} software
1. Prepare and place the corresponding TLS key and certificate on the server
1. Copy the current {{ ydb-short-name }} cluster configuration file to the server

TLS certificates used on new servers must comply with [field filling requirements](../initial-deploymentmd#tls-certificates), and be signed by a trusted certificate authority used on existing servers of the expanded {{ ydb-short-name }} cluster.

## Adding Dynamic Nodes {#add-dynamic-node}

Adding dynamic nodes allows increasing available computing resources (processor cores and RAM) for executing user queries by the {{ ydb-short-name }} cluster.

To add a dynamic node to the cluster, it is sufficient to start the process serving this node, passing it the path to the cluster configuration file, the name of the served database, and addresses of any three static cluster nodes in the command line parameters, as shown in the [cluster deployment instructions](../../deployment-options/manual/initial-deployment.md#start-dynnode).

After successfully adding a dynamic node to the cluster, information about it will be available on the [cluster monitoring page in the embedded UI](../../../reference/embedded-ui/ydb-monitoring.md).

To remove a dynamic node from the cluster, it is sufficient to stop the dynamic node process.

## Adding Static Nodes {#add-static-node}

Adding static nodes allows increasing throughput when performing input-output operations and increasing available capacity for data storage in the {{ ydb-short-name }} cluster.

To add static nodes to the cluster, you need to perform the following sequence of actions:

1. Format disks that will be used for storing {{ ydb-short-name }} data using the [procedure described for the cluster deployment stage](../../deployment-options/manual/initial-deployment.md#prepare-disks).

1. Correct the cluster configuration file located on nodes:
    * Include a description of the added nodes (in the `hosts` section) and disks used on them (in the `host_configs` section).
    * Set the configuration change number as the `storage_config_generation: K` parameter at the top level, where `K` is an integer, the change number (during initial installation the value `K=0` or not specified, during the first cluster expansion `K=1`, during the second `K=2`, and so on).

2. Copy the updated cluster configuration file to all existing and all added cluster servers, replacing the old version of the configuration file.

3. Perform rolling restart of all existing static cluster nodes, waiting for initialization and recovery of each restarted node.

4. Perform rolling restart of all existing dynamic cluster nodes.

5. Start processes serving new static cluster nodes on the corresponding servers.

6. Ensure that new static nodes are displayed on the [cluster monitoring page in the embedded UI](../../../reference/embedded-ui/ydb-monitoring.md).

7. Obtain an authentication token for executing administrative commands using {{ ydb-short-name }} CLI, for example:

    ```bash
    ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
        --user root auth get-token --force >token-file
    ```

    The example command above uses the following parameters:

    * `node1.ydb.tech` - FQDN of any server hosting static cluster nodes
    * `2135` - grpcs service port number for static nodes
    * `ca.crt` - certificate authority certificate file name
    * `root` - username with administrative rights
    * `token-file` - name of the file where the authentication token is saved for subsequent use

    When executing the above command, {{ ydb-short-name }} CLI will request a password for authenticating the specified user.

8. Allow the {{ ydb-short-name }} cluster to use disks on new static nodes for data storage by executing the following command on any cluster node:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
        admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
    echo $?
    ```

    The example command above uses the following parameters:

    * `ydbd-token-file` - name of the previously obtained authentication token file
    * `2135` - grpcs service port number for static nodes
    * `ca.crt` - certificate authority certificate file name

    If the above command returns a configuration number verification error, this means that the `storage_config_generation` field was incorrectly set when correcting the cluster configuration file. The error text contains the expected configuration number value that can be used to correct the cluster settings file. Example configuration number verification error message:

    ```proto
    ErrorDescription: "ItemConfigGeneration mismatch ItemConfigGenerationProvided# 0 ItemConfigGenerationExpected# 1"
    ```

9. Add additional storage groups to one or more databases by executing commands of the following type on any cluster node:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
        admin database /Root/testdb pools add ssd:1
    echo $?
    ```

    The example command above uses the following parameters:

    * `ydbd-token-file` - name of the previously obtained authentication token file
    * `2135` - grpcs service port number for static nodes
    * `ca.crt` - certificate authority certificate file name
    * `/Root/testdb` - full path to the database
    * `ssd:1` - storage pool name and number of allocated storage groups

10. Ensure that added storage groups are displayed on the [cluster monitoring page in the embedded UI](../../../reference/embedded-ui/ydb-monitoring.md).

Removing static nodes from the {{ ydb-short-name }} cluster is performed according to the [documented decommissioning procedure](../../../devops/deployment-options/manual/decommissioning.md).

In case of damage and inability to repair a server running a static cluster node, it is necessary to place the unavailable static node on a new server containing a similar or greater number and volume of disks.