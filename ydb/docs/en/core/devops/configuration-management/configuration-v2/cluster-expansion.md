# Cluster Expansion

You can expand a {{ ydb-short-name }} cluster by adding new nodes to the cluster configuration. Below are the necessary actions for expanding a {{ ydb-short-name }} cluster installed manually on virtual machines or physical servers. Cluster expansion in a Kubernetes environment is performed by adjusting the {{ ydb-short-name }} controller settings for Kubernetes.

Expanding a {{ ydb-short-name }} cluster does not require suspending user access to databases. When expanding the cluster, its components are restarted to apply configuration changes, which in turn may lead to the need to retry transactions running on the cluster. Transaction retries are performed automatically by applications using {{ ydb-short-name }} SDK capabilities for error control and operation retry.

## Preparing New Servers {#add-host}

When placing new static or dynamic cluster nodes on new servers that were not previously part of the expanded {{ ydb-short-name }} cluster, each new server must install {{ ydb-short-name }} software according to the procedures described in the [cluster deployment instructions](../../deployment-options/manual/initial-deployment.md). In particular, it is necessary to:

1. create an account and group in the operating system for the {{ ydb-short-name }} service
2. install {{ ydb-short-name }} software
3. prepare and place the corresponding TLS key and certificate on the server
4. copy the current {{ ydb-short-name }} cluster configuration file to the server

TLS certificates used on new servers must comply with [field filling requirements](../../deployment-options/manual/initial-deployment.md#tls-certificates), and be signed by a trusted certificate authority used on existing servers of the expanded {{ ydb-short-name }} cluster.

## Adding Dynamic Nodes {#add-dynamic-node}

Adding dynamic nodes allows increasing available computing resources (processor cores and RAM) for executing user queries by the {{ ydb-short-name }} cluster.

To add a dynamic node to the cluster, it is sufficient to start the process serving this node, passing it the path to the cluster configuration folder, the name of the served database, and addresses of any three static cluster nodes in the command line parameters, as shown in the [cluster deployment instructions](../../deployment-options/manual/initial-deployment.md#start-dynnode).

After successfully adding a dynamic node to the cluster, information about it will be available on the [cluster monitoring page in the embedded UI](../../../reference/embedded-ui/ydb-monitoring.md).

To remove a dynamic node from the cluster, it is sufficient to stop the dynamic node process.

## Adding Static Nodes {#add-static-node}

Adding static nodes allows increasing throughput when performing input-output operations and increasing available capacity for data storage in the {{ ydb-short-name }} cluster.

To add static nodes to the cluster, you need to perform the following sequence of actions:

1. Format disks that will be used for storing {{ ydb-short-name }} data using the [procedure described for the cluster deployment stage](../../deployment-options/manual/initial-deployment.md#prepare-disks).

2. Obtain an authentication token for executing administrative commands using {{ ydb-short-name }} CLI, for example:

    ```bash
    ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
        --user root auth get-token --force > token-file
    ```

    The example command above uses the following parameters:

    * `node1.ydb.tech` — FQDN of any server hosting static cluster nodes
    * `2135` — grpcs service port number for static nodes
    * `ca.crt` — certificate authority certificate file name
    * `root` — username with administrative rights
    * `token-file` — name of the file where the authentication token is saved for subsequent use

    When executing the above command, {{ ydb-short-name }} CLI will request a password for authenticating the specified user.

3. Get the current cluster configuration by executing the following command on any cluster node:

    ```bash
    ydb --token-file token-file --ca-file ca.crt -e grpcs://<node1.ydb.tech>:2135 \
        admin cluster config fetch > config.yaml
    ```

4. Correct the [cluster configuration file](../../deployment-options/manual/initial-deployment.md#config), including in the configuration a description of the added nodes (in the `hosts` section) and disks used on them (in the `host_configs` section).

5. Allow the {{ ydb-short-name }} cluster to use disks on new static nodes for data storage by executing the following command on any cluster node:

    ```bash
    export LD_LIBRARY_PATH=/opt/ydb/lib
    ydb --token-file ydbd-token-file --ca-file ca.crt -e grpcs://<node1.ydb.tech>:2135 \
        admin cluster config replace -f config.yaml
    echo $?
    ```

     The example command above uses the following parameters:

    * `ydbd-token-file` — name of the previously obtained authentication token file
    * `2135` — grpcs service port number for static nodes
    * `ca.crt` — certificate authority certificate file name

    If the above command returns a configuration version verification error, this means that the current config version is outdated, and you need to get a new one from the cluster by repeating step 3. Example configuration version verification error message:

    ```proto
    ErrorDescription: "ConfigVersion mismatch ConfigVersionProvided# 0 ConfigVersionExpected# 1"
    ```

6. Create an empty directory `/opt/ydb/cfg` on each new machine for the cluster to work with configuration. If multiple nodes are started on one machine, use the same directory. By executing a special command on each new machine, initialize this directory using any static cluster node as the configuration source.

    ```bash
    sudo mkdir -p /opt/ydb/cfg
    sudo chown -R ydb:ydb /opt/ydb/cfg
    ydb admin node config init --config-dir /opt/ydb/cfg --seed-node <node.ydb.tech:2135>
    ```

7. Start processes serving new static cluster nodes on the corresponding servers.

8. Ensure that new static nodes are displayed on the [cluster monitoring page in the embedded UI](../../../reference/embedded-ui/ydb-monitoring.md).

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

Removing static nodes from the {{ ydb-short-name }} cluster is performed according to the [documented decommissioning procedure](../../deployment-options/manual/decommissioning.md).

In case of damage and inability to repair a server running a static cluster node, it is necessary to place the unavailable static node on a new server containing a similar or greater number and volume of disks.