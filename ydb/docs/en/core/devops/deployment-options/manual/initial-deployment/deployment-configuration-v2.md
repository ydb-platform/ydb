# Deploying a cluster using V2 configuration

{% note alert %}

This article is about {{ ydb-short-name }} clusters that use **V2 configuration**. This configuration method is currently experimental and is only available for {{ ydb-short-name }} versions starting from v25.1. For production use, we recommend choosing [V1 configuration](./deployment-configuration-v1.md) — it is the primary and officially supported configuration for all {{ ydb-short-name }} clusters.

{% endnote %}

## Prepare the environment {#deployment-preparation}

Before deploying the system, be sure to perform the preparatory steps. Refer to the document [{#T}](deployment-preparation.md).

## Prepare configuration files {#config}

Prepare the {{ ydb-short-name }} configuration file:
Also, if you need to enable Kafka API support for topics, add the kafka_proxy_config section to the configuration file (see [Kafka API configuration](../../../reference/configuration/kafka_proxy_config)).


```yaml
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  erasure: mirror-3-dc
  fail_domain_type: disk
  self_management_config:
    enabled: true
  default_disk_type: SSD
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      type: SSD
  hosts:
  - host: ydb-node-zone-a.local
    host_config_id: 1
    location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  - host: ydb-node-zone-b.local
    host_config_id: 1
    location:
      body: 2
      data_center: 'zone-b'
      rack: '2'
  - host: ydb-node-zone-c.local
    host_config_id: 1
    location:
      body: 3
      data_center: 'zone-c'
      rack: '3'
  actor_system_config:
    use_auto_config: true
    cpu_count: 8
  interconnect_config:
    start_tcp: true
    encryption_mode: OPTIONAL
    path_to_certificate_file: "/opt/ydb/certs/node.crt"
    path_to_private_key_file: "/opt/ydb/certs/node.key"
    path_to_ca_file: "/opt/ydb/certs/ca.crt"
  grpc_config:
    cert: "/opt/ydb/certs/node.crt"
    key: "/opt/ydb/certs/node.key"
    ca: "/opt/ydb/certs/ca.crt"
    services_enabled:
    - legacy
  security_config:
    enforce_user_token_requirement: true
    monitoring_allowed_sids:
    - "root"
    - "ADMINS"
    - "DATABASE-ADMINS"
    administration_allowed_sids:
    - "root"
    - "ADMINS"
    - "DATABASE-ADMINS"
    viewer_allowed_sids:
    - "root"
    - "ADMINS"
    - "DATABASE-ADMINS"
    register_dynamic_node_allowed_sids:
    - databaseNodes@cert
    - root@builtin
    bootstrap_allowed_sids:
    - "root"
    - "ADMINS"
    - "DATABASE-ADMINS"
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
        - member_groups: ["ADMINS"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
```


To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already contains most of the settings for setting up the cluster. You just need to replace the default FQDN hosts in the `hosts` section and the disk paths with the actual ones in the `hosts_configs` section.

* Section `hosts`:


  ```yaml
  ...
  hosts:
  - host: ydb-node-zone-a.local
    host_config_id: 1
    location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  ...
  ```

* Section `hosts_configs`:


  ```yaml
  ...
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
      type: SSD
  ...
  ```

The remaining sections and settings of the configuration file remain unchanged.

Save the YDB configuration file as `/tmp/config.yaml` on each cluster server.

For more information on creating the configuration file, see the [{#T}](../../../../reference/configuration/index.md) section.

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each cluster node {{ ydb-short-name }}. Below is an example of commands for creating a secure directory and copying the key and certificate files.


```bash
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```


## Prepare the configuration on static cluster nodes

Create an empty `opt/ydb/cfg` directory on each machine for the cluster to work with the configuration. If you run multiple cluster nodes on a single machine, create separate directories for each node.


```bash
sudo mkdir -p /opt/ydb/cfg
sudo chown -R ydb:ydb /opt/ydb/cfg
```


Run a special command on each machine to initialize this directory with the configuration file.


```bash
sudo /opt/ydb/bin/ydb admin node config init --config-dir /opt/ydb/cfg --from-config /tmp/config.yaml
```


The original `/tmp/config.yaml` file is no longer used after running this command; you can delete it.

## Start static nodes {#start-storage}

{% list tabs group=manual-systemd %}

* Manually

  Start the data storage service {{ ydb-short-name }} on each static cluster node:


  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --kafka-port 9092 --mon-cert /opt/ydb/certs/web.pem --node static
  ```

* Using systemd

  On each server that will host a static cluster node, create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` according to the sample below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).


  ```ini
  [Unit]
  Description=YDB storage node
  After=network-online.target rc-local.service
  Wants=network-online.target
  StartLimitInterval=10
  StartLimitBurst=15

  [Service]
  Restart=always
  RestartSec=1
  User=ydb
  PermissionsStartOnly=true
  StandardOutput=syslog
  StandardError=syslog
  SyslogIdentifier=ydbd
  SyslogFacility=daemon
  SyslogLevel=err
  Environment=LD_LIBRARY_PATH=/opt/ydb/lib
  ExecStart=/opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --kafka-port 9092 \
      --mon-cert /opt/ydb/certs/web.pem --node static
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=3221225472

  [Install]
  WantedBy=multi-user.target
  ```


  Start the service on each static node {{ ydb-short-name }}:


  ```bash
  sudo systemctl start ydbd-storage
  ```

{% endlist %}

After starting the static nodes, verify their operation through the built-in web interface {{ ydb-short-name }} (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in your browser, where `<node.ydb.tech>` is the FQDN of the server running any static node
2. Go to the **Nodes** tab
3. Make sure all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file for data storage {{ ydb-short-name }}.

To initialize the cluster, you will need the certificate authority file `ca.crt`, whose path must be specified when running the relevant commands. Before running the commands, copy the `ca.crt` file to the server where the commands will be executed.

On one of the storage servers in the cluster, run the commands:

Initialize the cluster using the obtained token


```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydb --ca-file ca.crt \
    --client-cert-file node.crt \
    --client-cert-key-file node.key \
    -e grpcs://`hostname -f`:2135 \
    admin cluster bootstrap --uuid <string>
echo $?
```


After initializing the cluster, to run further administrative commands, you must first obtain an authentication token.


```bash
/opt/ydb/bin/ydb -e grpcs://`hostname -f`:2135 -d /Root --ca-file ca.crt \
--user root --no-password auth get-token --force > token-file
```


If the cluster initialization succeeds, the exit code of the cluster initialization command displayed on the screen should be zero.

## Create a database {#create-db}

To work with row-based or column-based tables, you need to create at least one database and start the process or processes that serve this database (dynamic nodes).

To run the administrative command for creating a database, you will need the certificate authority file `ca.crt`, similar to the procedure described above for cluster initialization.

When creating a database, the initial number of storage groups used is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after the database is created if necessary.

On one of the storage servers in the cluster, run the commands:


```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```


If the database is created successfully, the exit code of the command displayed on the screen should be zero.

The command example above uses the following parameters:

* `/Root` — the name of the root domain automatically generated during cluster initialization
* `testdb` — the name of the database being created
* `ssd:8` — specifies the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of allocated storage groups.

## Start dynamic nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

* Manually

  Run the dynamic node {{ ydb-short-name }} for the database `/Root/testdb`:


  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --kafka-port 9093 \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  ```


  In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers on which the static cluster nodes are running.
* Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service` using the sample below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).


  ```ini
  [Unit]
  Description=YDB testdb dynamic node
  After=network-online.target rc-local.service
  Wants=network-online.target
  StartLimitInterval=10
  StartLimitBurst=15

  [Service]
  Restart=always
  RestartSec=1
  User=ydb
  PermissionsStartOnly=true
  StandardOutput=syslog
  StandardError=syslog
  SyslogIdentifier=ydbd
  SyslogFacility=daemon
  SyslogLevel=err
  Environment=LD_LIBRARY_PATH=/opt/ydb/lib
  ExecStart=/opt/ydb/bin/ydbd server \
      --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --kafka-port 9093 \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=32212254720

  [Install]
  WantedBy=multi-user.target
  ```


  In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers on which the static cluster nodes are running.

  Run the dynamic node {{ ydb-short-name }} for the database `/Root/testdb`:


  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Run additional dynamic nodes on other servers to scale and ensure fault tolerance of the database.

## Setting up accounts {#security-setup}

1. Set a password for the `root` account using the previously obtained token:


   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
       yql -s 'ALTER USER root PASSWORD "passw0rd"'
   ```


   Instead of the `passw0rd` value, substitute the required password. Save the password to a separate file. Subsequent commands on behalf of user `root` will be executed using the password passed via the `--password-file <path_to_user_password>` key. You can also save the password in a connection profile, as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).
2. Create additional accounts:


   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
   ```

3. Set account permissions by adding them to built-in groups:


   ```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
   ```

In the command examples listed above, `<node.ydb.tech>` is the FQDN of the server on which any dynamic node serving the database `/Root/testdb` is running. When connecting via SSH to the dynamic node {{ ydb-short-name }}, it is convenient to use the `grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create user accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt you to enter the password of user `root`. To avoid repeatedly entering the password, you can create a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).
2. Create a test row (`test_row_table`) or columnar table (`test_column_table`):

{% list tabs %}

- Creating a row table

  ```bash
  ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
  ```

- Creating a columnar table

  ```bash
  ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
      yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
  ```

{% endlist %}

Where `<node.ydb.tech>` is the FQDN of the server on which the dynamic node serving the `/Root/testdb` database is running.

## Checking access to the built-in web interface

To check access to the built-in web interface {{ ydb-short-name }}, open a web browser page at `https://<node.ydb.tech>:8765`, where `<node.ydb.tech>` is the FQDN of the server on which any static node {{ ydb-short-name }} is running.

The web browser must be configured to trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster; otherwise, a warning about using an untrusted certificate will be displayed.

If authentication is enabled in the cluster, a login and password prompt should appear in the web browser. After entering valid authentication data, the initial page of the built-in web interface should be displayed. A description of the available functions and user interface is provided in the [{#T}](../../../../reference/embedded-ui/index.md) section.

{% note info %}

Typically, to provide access to the built-in web interface {{ ydb-short-name }}, a fault-tolerant HTTP load balancer based on `haproxy`, `nginx`, or similar software is configured. Details of configuring the HTTP load balancer are beyond the scope of the standard {{ ydb-short-name }} installation instructions.

{% endnote %}

## Features of installing {{ ydb-short-name }} in unsecured mode

{% note warning %}

We do not recommend using the unsecured mode of {{ ydb-short-name }} either during operation or during application development.

{% endnote %}

The installation procedure described above provides for deploying {{ ydb-short-name }} in the standard secure mode.

The unsecured operation mode of {{ ydb-short-name }} is designed for test tasks, primarily related to software development and testing {{ ydb-short-name }}. In unsecured mode:

* Traffic between cluster nodes, as well as between applications and the cluster, uses unencrypted connections.
* user authentication is not used (enabling authentication without traffic encryption is pointless, since login and password would be transmitted over the network in plain text in such a configuration).

Installation of {{ ydb-short-name }} for operation in unsecured mode is performed in the order described above, with the following exceptions:

1. When preparing for installation, you do not need to generate TLS certificates and keys, and you do not need to copy certificates and keys to the cluster nodes.
2. The sections `security_config`, `interconnect_config`, and `grpc_config` are excluded from the configuration files of cluster nodes.
3. A simplified version of the commands for starting static and dynamic cluster nodes is used: options with certificate and key file names are excluded, and the `grpc` protocol is used instead of `grpcs` when specifying connection endpoints.
4. The unnecessary step of obtaining an authentication token before initializing the cluster and creating a database is skipped in the unsecured mode.
5. The cluster initialization command is executed in the following form:


   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   ydb admin cluster bootstrap --uuid <string>
   echo $?
   ```

6. The database creation command is executed in the following form:


   ```bash
   export LD_LIBRARY_PATH=/opt/ydb/lib
   /opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
   ```

7. When accessing the database from the {{ ydb-short-name }} CLI and applications, the grpc protocol is used instead of grpcs, and authentication is not used.
