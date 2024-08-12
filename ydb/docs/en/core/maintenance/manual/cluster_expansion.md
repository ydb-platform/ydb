# Expanding a cluster

You can expand a {{ ydb-short-name }} cluster by adding new nodes to its configuration. Below is the list of actions for expanding a {{ ydb-short-name }} cluster installed manually on VM instances or physical servers. In the Kubernetes environment, clusters are expanded by adjusting the {{ ydb-short-name }} controller settings for Kubernetes.

When expanding your {{ ydb-short-name }} cluster, you do not have to pause user access to databases. When the cluster is expanded, its components are restarted to apply the updated configurations. This means that any transactions that were in progress at the time of expansion may need to be executed again on the cluster. The transactions are rerun automatically because the applications leverage the {{ ydb-short-name }} SDK features for error control and transaction rerun.

## Preparing new servers {#add-host}

If you deploy new static or dynamic nodes of the cluster on new servers added to the expanded {{ ydb-short-name }} cluster, on each new server, you need to install the {{ ydb-short-name }} software according to the procedures described in the [cluster deployment instructions](../../deploy/manual/deploy-ydb-on-premises.md). Among other things, you need to:

1. Create an account and a group in the operating system to enable {{ ydb-short-name }} operation.
1. Install the {{ ydb-short-name }} software.
1. Generate the appropriate TLS key and certificate for the software and add them to the server.
1. Copy the up-to-date configuration file for the {{ ydb-short-name }} cluster to the server.

The TLS certificates used on the new servers must meet the [requirements for filling out the fields](../../deploy/manual/deploy-ydb-on-premises.md#tls-certificates) and be signed by the same trusted certification authority that signed the certificates for the existing servers of the expanded {{ ydb-short-name }} cluster.

## Adding dynamic nodes {#add-dynamic-node}

By adding dynamic nodes, you can expand the available computing resources (CPU cores and RAM) needed for your {{ ydb-short-name }} cluster to process user queries.

To add a dynamic node to the cluster, run the process that serves this node, passing to it, in the command line options, the name of the served database and the addresses of any three static nodes of the {{ ydb-short-name }} cluster, as shown in the [cluster deployment instructions](../../deploy/manual/deploy-ydb-on-premises.md#start-dynnode).

Once you have added the dynamic node to the cluster, the information about it becomes available on the [cluster monitoring page in the built-in UI](../../reference/embedded-ui/ydb-monitoring.md).

To remove a dynamic node from the cluster, stop the process on the dynamic node.

## Adding static nodes {#add-static-node}

By adding static nodes, you can increase the throughput of your I/O operations and increase the available storage capacity in your {{ ydb-short-name }} cluster.

To add static nodes to the cluster, perform the following steps:

1. Format the disks that will be used to store the {{ ydb-short-name }} data by following the [procedure for the cluster deployment step](../../deploy/manual/deploy-ydb-on-premises.md#prepare-disks)

1. Edit the [cluster's configuration file](../../deploy/manual/deploy-ydb-on-premises.md#config):
   * Add, to the configuration, the description of the added nodes (in the `hosts` section) and disks used by them (in the `host_configs` section).
   * Use the `storage_config_generation: K` option to set the ID of the configuration update at the top level, where `K` is the integer update ID (for the initial config, `K=0` or omitted; for the first expansion, `K=1`; for the second expansion, `K=2`; and so on).

1. Copy the updated cluster's configuration file to all the existing and added servers in the cluster, overwriting the old version of the configuration file.

1. Restart all the existing static nodes in the cluster one-by-one, waiting for each restarted node to initialize and become fully operational.

1. Restart all the existing static nodes in the cluster one-by-one.

1. Start the processes that serve the new static nodes in the cluster, on the appropriate servers.

1. Make sure that all the new static nodes now show up on the [cluster monitoring page in the built-in UI](../../reference/embedded-ui/ydb-monitoring.md).

1. Issue an authentication token using the {{ ydb-short-name }} CLI, for example:

   ```bash
   ydb -e grpcs://<node1.ydb.tech>:2135 -d /Root --ca-file ca.crt \
       --user root auth get-token --force >token-file
   ```

   The command example above uses the following options:
   * `node1.ydb.tech`: The FQDN of any server hosting the cluster's static nodes.
   * `2135`: Port number of the gRPCs service for the static nodes.
   * `ca.crt`: Name of the file with the certificate authority certificate.
   * `root`: The name of a user who has administrative rights.
   * `token-file`: name of the file where the authentication token is saved for later use.

   When you run the above command, {{ ydb-short-name }} CLI will request the password to authenticate the given user.

1. Allow the {{ ydb-short-name }} cluster to use disks to store data on the new static nodes. For this, run the following command on any cluster node:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
   echo $?
   ```

   The command example above uses the following options:
   * `ydbd-token-file`: File name of the previously issued authentication token.
   * `2135`: Port number of the gRPCs service for the static nodes.
   * `ca.crt`: Name of the file with the certificate authority certificate.

   If the above command results in the error of the configuration ID mismatch, it means that you made an error editing the `storage_config_generation` field in the cluster configuration file. In the error text, you can find the expected configuration ID that can be used to edit the cluster configuration file. Sample error message for the configuration ID mismatch:

   ```protobuf
   ErrorDescription: "ItemConfigGeneration mismatch ItemConfigGenerationProvided# 0 ItemConfigGenerationExpected# 1"
   ```

2. Add additional storage groups to one or more databases by running the following commands on any cluster node:

   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd -f ydbd-token-file --ca-file ca.crt -s grpcs://`hostname -f`:2135 \
       admin database /Root/testdb pools add ssd:1
   echo $?
   ```

   The command example above uses the following options:
   * `ydbd-token-file`: File name of the previously issued authentication token.
   * `2135`: Port number of the gRPCs service for the static nodes.
   * `ca.crt`: Name of the file with the certificate authority certificate.
   * `/Root/testdb`: Full path to the database.
   * `ssd:1`: Name of the storage pool and the number of storage groups allocated.

3. Make sure that all the new storage groups now show up on the [cluster monitoring page in the built-in UI](../../reference/embedded-ui/ydb-monitoring.md).

To remove a static node from the {{ ydb-short-name }} cluster, use the [documented decommissioning procedure](../../devops/manual/decommissioning.md).

If the server running the static cluster node is damaged or becomes irreparable, deploy the unavailable static node on a new server with the same or higher number of disks.
