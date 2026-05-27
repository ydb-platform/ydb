# Deploying a cluster using configuration V2

{% note alert %}

This article is about {{ ydb-short-name }} clusters that use **configuration V2**. This configuration method is still experimental and is available only for {{ ydb-short-name }} versions starting from v25.1. For production use, we recommend choosing [configuration V1](./deployment-configuration-v1.md) — it is the main and officially supported configuration for all {{ ydb-short-name }} clusters.

{% endnote %}

## Prepare the environment {#deployment-preparation}

Before deploying the system, be sure to complete the preparatory steps. See the document [{#T}](deployment-preparation.md).

## Prepare configuration files {#config}

Prepare the {{ ydb-short-name }} configuration file:
Also, if you need to enable Kafka API with topics, add the kafka_proxy_config section to the configuration file (see [configuring Kafka API](../../../reference/configuration/kafka_proxy_config)).

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

To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already includes most of the settings for cluster installation. You just need to replace the standard FQDN hosts in the `hosts` section and the disk paths in the `hosts_configs` section with the actual ones.

* `hosts` section:

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

* `hosts_configs` section:

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

The rest of the configuration file sections and settings remain unchanged.

Save the YDB configuration file as `/tmp/config.yaml` on each cluster server.

For more detailed information on creating a configuration file, see [{#T}](../../../../reference/configuration/index.md).

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each {{ ydb-short-name }} cluster node. Below are example commands for creating a secure directory and copying the key and certificate files.

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

Create an empty `opt/ydb/cfg` directory on each machine for the cluster to work with the configuration. If you are running multiple cluster nodes on the same machine, create separate directories for each node.

```bash
sudo mkdir -p /opt/ydb/cfg
sudo chown -R ydb:ydb /opt/ydb/cfg
```

Run a special command on each machine to initialize this directory with the configuration file.

```bash
sudo /opt/ydb/bin/ydb admin node config init --config-dir /opt/ydb/cfg --from-config /tmp/config.yaml
```

The original `/tmp/config.yaml` file is no longer used after this command is executed and can be deleted.

## Start static nodes {#start-storage}

{% list tabs group=manual-systemd %}

* Manually

  Run the {{ ydb-short-name }} data storage service on each static cluster node:
  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --kafka-port 9092 --mon-cert /opt/ydb/certs/web.pem --node static
  ```
* Using systemd

Create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` on each server where a static cluster node will be located, using the example below. You can also [download the file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).
  ```ini
  [Unit]
  Description=YDB storage node[скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service)
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
Launch the service on each static {{ ydb-short-name }} node:
  ```bash
  sudo systemctl start ydbd-storage
  ```
{% endlist %}

After starting the static nodes, check their functionality via the {{ ydb-short-name }} embedded web interface (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in your browser, where `<node.ydb.tech>` is the FQDN of the server running any static node;
2. Go to the **Nodes** tab;
3. Make sure that all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file to store {{ ydb-short-name }} data.

To initialize the cluster, you need a registration authority certificate file `ca.crt`, the path to which must be specified when executing the corresponding commands. Before running the commands, copy the `ca.crt` file to the server where these commands will be executed.

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

After initializing the cluster, you need to obtain an authentication token before executing further administrative commands.

```bash
/opt/ydb/bin/ydb -e grpcs://`hostname -f`:2135 -d /Root --ca-file ca.crt \
--user root --no-password auth get-token --force > token-file
```

If the cluster initialization is successful, the command completion code displayed on the screen should be zero.

## Create a database {#create-db}

To work with string or columnar tables, you need to create at least one database and start the process or processes serving this database (dynamic nodes).

To execute the administrative command to create a database, you need the `ca.crt` registration authority certificate file, following the same procedure as described above for cluster initialization.

When creating a database, the initial number of storage groups is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after creating the database if necessary.

On one of the storage servers in the cluster, run the commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

If the database is created successfully, the command completion code displayed on the screen should be zero.

The following parameters are used in the command example above:

* `/Root` — the name of the root domain generated automatically during cluster initialization;
* `testdb` — the name of the database being created;
* `ssd:8` — specifies the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of storage groups allocated.

## Start dynamic nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

* Manually

  Start a dynamic node of {{ ydb-short-name }} for the `/Root/testdb` database:
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
In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

* Using systemd

Create a systemd config file `/etc/systemd/system/ydbd-testdb.service` using the example below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).
  ```ini
  [Unit]
  Description=YDB testdb dynamic node
  After=network-online.target rc-local.service
  Wants=network-online.target
  StartLimitInterval=10
  StartLimitBurst=15

  [Service][скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service)
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
In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

Start a dynamic {{ ydb-short-name }} node for the `/Root/testdb` database:
  ```bash
  sudo systemctl start ydbd-testdb
  ```
{% endlist %}

Launch additional dynamic nodes on other servers to scale and ensure database fault tolerance.

## Setting up accounts {#security-setup}

1. Set a password for the `root` account using the previously obtained token:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Replace `passw0rd` with the desired password. Save the password to a separate file. Subsequent commands on behalf of the `root` user will be executed using the password passed with the `--password-file <path_to_user_password>` key. You can also save the password in the connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

1. Create additional accounts:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
    ```

1. Set account permissions by adding them to built-in groups:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'ALTER GROUP `ADMINS` ADD USER user1'[документации {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/profile/index.md)
    ```

In the command examples listed above, `<node.ydb.tech>` is the FQDN of the server running any dynamic node serving the `/Root/testdb` database. When connecting to a dynamic {{ ydb-short-name }} node via SSH, it is convenient to use the `grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt for the `root` user password. You can avoid entering the password multiple times by creating a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

## Test working with the created database {#try-first-db}

1. Install {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).

1. Create a test row (`test_row_table`) or column (`test_column_table`) table:

{% list tabs %}

* String table creation
    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'[документации {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/profile/index.md)
    ```
* Creating a columnar table
    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \[документации](../../../../reference/ydb-cli/install.md)
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
    ```
{% endlist %}

Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node that serves the `/Root/testdb` database.

## Checking access to the built-in web interface

To check access to the built-in {{ ydb-short-name }} web interface, open the page `https://<node.ydb.tech>:8765` in your web browser, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

Your web browser must trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster; otherwise, you will see a warning about an untrusted certificate.

If authentication is enabled in the cluster, your web browser will prompt for a username and password. After entering the correct authentication credentials, the initial page of the built-in web interface should appear. A description of the available functions and user interface is provided in the section [{#T}](../../../../reference/embedded-ui/index.md).

{% note info %}

Typically, to provide access to the built-in {{ ydb-short-name }} web interface, a fault-tolerant HTTP load balancer based on software like `haproxy`, `nginx`, or similar is configured. The details of configuring an HTTP load balancer are beyond the scope of the standard {{ ydb-short-name }} installation instructions.

{% endnote %}

## Features of installing {{ ydb-short-name }} in insecure mode

{% note warning %}

We do not recommend using the insecure mode of {{ ydb-short-name }} either in production or during application development.

{% endnote %}

The installation procedure described above deploys {{ ydb-short-name }} in the standard secure mode.

The insecure mode of {{ ydb-short-name }} is intended for solving test tasks primarily related to the development and testing of {{ ydb-short-name }} software. In insecure mode:

* the traffic between cluster nodes and between applications and the cluster uses unencrypted connections;
* user authentication is not used (enabling authentication without traffic encryption makes no sense, as the username and password would be transmitted over the network in plain text in such a configuration).

Installing {{ ydb-short-name }} to operate in insecure mode follows the procedure described above, with the following exceptions:

1. When preparing for installation, there is no need to generate TLS certificates and keys, and no need to copy certificates and keys to the cluster nodes.
1. The `security_config`, `interconnect_config`, and `grpc_config` sections are removed from the cluster node configuration files.
1. A simplified version of the commands for starting static and dynamic cluster nodes is used: options with certificate and key file names are excluded, and the `grpc` protocol is used instead of `grpcs` when specifying connection points.
1. The step of obtaining an authentication token before initializing the cluster and creating a database, which is unnecessary in insecure mode, is skipped.
1. The cluster initialization command is executed in the following form:

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

7. When accessing the database from {{ ydb-short-name }} CLI and applications, the `grpc` protocol is used instead of `grpcs`, and authentication is not used.
