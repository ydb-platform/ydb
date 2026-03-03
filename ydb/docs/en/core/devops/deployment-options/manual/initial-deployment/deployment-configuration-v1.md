# Deploying a Cluster Using Configuration V1

## Prepare the Environment {#deployment-preparation}

Before deploying the system, complete the preparatory steps. Review the [{#T}](deployment-preparation.md) document.

## Prepare Configuration Files {#config}

Prepare the {{ ydb-short-name }} configuration file according to your chosen topology (see [cluster topology](./deployment-preparation.md)). Examples for each supported topology are provided below in tabs — select and use the one that fits your case.

{% list tabs %}

- mirror-3-dc-3nodes

  ```yaml
  static_erasure: mirror-3-dc
  host_configs:
    - drive:
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          type: SSD
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          type: SSD
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
          type: SSD
      host_config_id: 1
    hosts:
    - host: static-node-1.ydb-cluster.com
      host_config_id: 1
      walle_location:
        body: 1
        data_center: "zone-a"
        rack: "1"
    - host: static-node-2.ydb-cluster.com
      host_config_id: 1
      walle_location:
        body: 2
        data_center: "zone-b"
        rack: "2"
    - host: static-node-3.ydb-cluster.com
      host_config_id: 1
      walle_location:
        body: 3
        data_center: "zone-d"
        rack: "3"
    domains_config:
    security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: "root"
        password: ""
    default_access:
      - "+(F):root"
    domain:
    - name: Root
    storage_pool_types:
    - kind: ssd
    pool_config:
    box_id: 1
    erasure_species: mirror-3-dc
    kind: ssd
    geometry:
      realm_level_begin: 10
      realm_level_end: 20
      domain_level_begin: 10
      domain_level_end: 256
    pdisk_filter:
      - property:
          - type: SSD
    vdisk_kind: Default
   state_storage:
    - ring:
        node: [1, 2, 3]
        nto_select: 3
        ssid: 1
    table_service_config:
    sql_version: 1
    actor_system_config:
    executor:
      - name: System
        threads: 2
        type: BASIC
      - name: User
        threads: 3
        type: BASIC
      - name: Batch
        threads: 2
        type: BASIC
      - name: IO
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: IO
      - name: IC
        spin_threshold: 10
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: BASIC
    scheduler:
      progress_threshold: 10000
      resolution: 256
      spin_threshold: 0
   blob_storage_config:
    service_set:
      groups:
        - erasure_species: mirror-3-dc
          rings:
            - fail_domains:
                - vdisk_locations:
                    - node_id: static-node-1.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                    - node_id: static-node-1.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_02
                    - node_id: static-node-1.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_03
                - vdisk_locations:
                    - node_id: static-node-2.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                    - node_id: static-node-2.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_02
                    - node_id: static-node-2.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_03
                - vdisk_locations:
                    - node_id: static-node-3.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                    - node_id: static-node-3.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_02
                    - node_id: static-node-3.ydb-cluster.com
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    channel_profile_config:
    profile:
      - channel:
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
        profile_id: 0
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
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
            - short_name: "O"
              values: ["YDB"]
  ```

- mirror-3-dc-9nodes

  ```yaml
  static_erasure: mirror-3-dc
  host_configs:
    - drive:
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          type: SSD
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          type: SSD
      host_config_id: 1
  hosts:
    - host: ydb-node-zone-a-1.local
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
    - host: ydb-node-zone-a-2.local
      host_config_id: 1
      walle_location:
        body: 2
        data_center: 'zone-a'
        rack: '2'
    - host: ydb-node-zone-a-3.local
      host_config_id: 1
      walle_location:
        body: 3
        data_center: 'zone-a'
        rack: '3'
    - host: ydb-node-zone-b-1.local
      host_config_id: 1
      walle_location:
        body: 4
        data_center: 'zone-b'
        rack: '4'
    - host: ydb-node-zone-b-2.local
      host_config_id: 1
      walle_location:
        body: 5
        data_center: 'zone-b'
        rack: '5'
    - host: ydb-node-zone-b-3.local
      host_config_id: 1
      walle_location:
        body: 6
        data_center: 'zone-b'
        rack: '6'
    - host: ydb-node-zone-d-1.local
      host_config_id: 1
      walle_location:
        body: 7
        data_center: 'zone-d'
        rack: '7'
    - host: ydb-node-zone-d-2.local
      host_config_id: 1
      walle_location:
        body: 8
        data_center: 'zone-d'
        rack: '8'
    - host: ydb-node-zone-d-3.local
      host_config_id: 1
      walle_location:
        body: 9
        data_center: 'zone-d'
        rack: '9'
  domains_config:
    security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: "root"
        password: ""
    default_access:
      - "+(F):root"
  domain:
    - name: Root
  storage_pool_types:
    - kind: ssd
  pool_config:
    box_id: 1
    erasure_species: mirror-3-dc
    kind: ssd
    pdisk_filter:
      - property:
          - type: SSD
    vdisk_kind: Default
  state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
        nto_select: 9
        ssid: 1
  table_service_config:
    sql_version: 1
  actor_system_config:
    executor:
      - name: System
        threads: 2
        type: BASIC
      - name: User
        threads: 3
        type: BASIC
      - name: Batch
        threads: 2
        type: BASIC
      - name: IO
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: IO
      - name: IC
        spin_threshold: 10
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: BASIC
    scheduler:
      progress_threshold: 10000
      resolution: 256
      spin_threshold: 0
  blob_storage_config:
    service_set:
      groups:
        - erasure_species: mirror-3-dc
          rings:
            - fail_domains:
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-1.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-2.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-3.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-b-1.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-b-2.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-b-3.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-d-1.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-d-2.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-d-3.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
  channel_profile_config:
    profile:
      - channel:
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: mirror-3-dc
            pdisk_category: 1
            storage_pool_kind: ssd
        profile_id: 0
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
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
            - short_name: "O"
              values: ["YDB"]
  ```

- block-4-2

  ```yaml
  static_erasure: block-4-2
  host_configs:
    - drive:
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          type: SSD
        - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          type: SSD
      host_config_id: 1
  hosts:
    - host: ydb-node-zone-a-1.local
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
    - host: ydb-node-zone-a-2.local
      host_config_id: 1
      walle_location:
        body: 2
        data_center: 'zone-a'
        rack: '2'
    - host: ydb-node-zone-a-3.local
      host_config_id: 1
      walle_location:
        body: 3
        data_center: 'zone-a'
        rack: '3'
    - host: ydb-node-zone-a-4.local
      host_config_id: 1
      walle_location:
        body: 4
        data_center: 'zone-a'
        rack: '4'
    - host: ydb-node-zone-a-5.local
      host_config_id: 1
      walle_location:
        body: 5
        data_center: 'zone-a'
        rack: '5'
    - host: ydb-node-zone-a-6.local
      host_config_id: 1
      walle_location:
        body: 6
        data_center: 'zone-a'
        rack: '6'
    - host: ydb-node-zone-a-7.local
      host_config_id: 1
      walle_location:
        body: 7
        data_center: 'zone-a'
        rack: '7'
    - host: ydb-node-zone-a-8.local
      host_config_id: 1
      walle_location:
        body: 8
        data_center: 'zone-a'
        rack: '8'
  domains_config:
    security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: "root"
        password: ""
    default_access:
      - "+(F):root"
  domain:
    - name: Root
  storage_pool_types:
    - kind: ssd
  pool_config:
    box_id: 1
    erasure_species: block-4-2
    kind: ssd
    pdisk_filter:
      - property:
          - type: SSD
    vdisk_kind: Default
  state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8]
        nto_select: 5
        ssid: 1
  table_service_config:
    sql_version: 1
  actor_system_config:
    executor:
      - name: System
        threads: 2
        type: BASIC
      - name: User
        threads: 3
        type: BASIC
      - name: Batch
        threads: 2
        type: BASIC
      - name: IO
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: IO
      - name: IC
        spin_threshold: 10
        threads: 1
        time_per_mailbox_micro_secs: 100
        type: BASIC
    scheduler:
      progress_threshold: 10000
      resolution: 256
      spin_threshold: 0
  blob_storage_config:
    service_set:
      groups:
        - erasure_species: block-4-2
          rings:
            - fail_domains:
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-1.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-2.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-3.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-4.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-5.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-6.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-7.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
                - vdisk_locations:
                    - node_id: "ydb-node-zone-a-8.local"
                      pdisk_category: SSD
                      path: /dev/disk/by-partlabel/ydb_disk_ssd_01
  channel_profile_config:
    profile:
      - channel:
          - erasure_species: block-4-2
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: block-4-2
            pdisk_category: 1
            storage_pool_kind: ssd
          - erasure_species: block-4-2
            pdisk_category: 1
            storage_pool_kind: ssd
        profile_id: 0
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
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
            - short_name: "O"
              values: ["YDB"]
  ```

{% endlist %}

To speed up and simplify the initial {{ ydb-short-name }} deployment, the configuration file already contains most cluster setup settings. Replace the default host FQDNs with your actual ones in the `hosts` and `blob_storage_config` sections.

- `hosts` section:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com  # VM FQDN
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```

- `blob_storage_config` section:

  ```yaml
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com  # VM FQDN
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

Leave all other configuration sections and settings unchanged.

Save the {{ ydb-short-name }} configuration file as `/opt/ydb/cfg/config.yaml` on each cluster server.

For more detailed information about creating the configuration file, see [{#T}](../../../../reference/configuration/index.md).

## Copy TLS Keys and Certificates to Each Server {#tls-copy-cert}

Copy the prepared TLS keys and certificates to a protected directory on each {{ ydb-short-name }} cluster node. Below are sample commands to create a protected directory and copy the key and certificate files.

```bash
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```

## Start Static Nodes {#start-storage}

{% list tabs group=manual-systemd %}

- Manually

  Run the {{ ydb-short-name }} data storage service on each static cluster node:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static &
  ```

- Using systemd

  On each server that will host a static cluster node, create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` using the template below. You can also [download the sample file](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service) from the repository.

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
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 \
      --mon-cert /opt/ydb/certs/web.pem --node static
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=3221225472

  [Install]
  WantedBy=multi-user.target
  ```

  Run the service on each static {{ ydb-short-name }} node:

  ```bash
  sudo systemctl start ydbd-storage
  ```

{% endlist %}

After starting the static nodes, verify they are running via the {{ ydb-short-name }} built-in web interface (Embedded UI):

1. Open `https://<node.ydb.tech>:8765` in your browser, where `<node.ydb.tech>` is the FQDN of the server running any static node;
2. Go to the **Nodes** tab;
3. Ensure all 3 static nodes are listed.

![Manual installation, static nodes running](../../_assets/manual_installation_1.png)

## Initialize the Cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file for storing {{ ydb-short-name }} data.

To initialize the cluster, you need the Certificate Authority `ca.crt` file; its path must be specified when running the commands. Before running the commands, copy `ca.crt` to the server where you will execute them.

On one of the storage servers in the cluster, run the following commands:

First, obtain an authentication token for request authorization. Run the command below:

```bash
/opt/ydb/bin/ydb --ca-file ca.crt -e grpcs://`hostname -f`:2135 -d /Root --user root --no-password auth get-token -f > auth_token
```

Initialize the cluster using the obtained token:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
```

On successful cluster initialization, the cluster initialization command exit code should be zero.

## Create a Database {#create-db}

To work with row-oriented or column-oriented tables, you need to create at least one database and run the process or processes serving it (dynamic nodes).

To run the administrative database creation command, you need the Certificate Authority `ca.crt` file, same as for cluster initialization above.

When creating the database, you set the initial number of storage groups, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after database creation if needed.

On one of the storage servers in the cluster, run the following commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

On successful database creation, the command exit code should be zero.

The command example above uses the following parameters:

- `/Root` — name of the root domain, automatically generated during cluster initialization;
- `testdb` — name of the database to create;
- `ssd:8` — defines the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of storage groups to allocate.

## Run Dynamic Nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135 &
  ```

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running the cluster's static nodes.

- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service` using the template below. You can also [download the sample file](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service) from the repository.

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

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running the cluster's static nodes.

  Run the {{ ydb-short-name }} dynamic node for the `/Root/testdb` database:

  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Run additional dynamic nodes on other servers for database scaling and fault tolerance.

## Account Setup {#security-setup}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).

1. Set the password for the `root` account using the token obtained earlier:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Replace `passw0rd` with the desired password. Save the password in a separate file. Subsequent commands as the `root` user will use the password passed with the `--password-file <path_to_user_password>` option. You can also save the password in a connection profile, as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

1. Create additional accounts:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
    ```

1. Set account permissions by adding them to built-in groups:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
        yql -s 'ALTER GROUP `ADMINS` ADD USER user1'
    ```

In the command examples above, `<node.ydb.tech>` is the FQDN of the server running any dynamic node serving the `/Root/testdb` database. When connecting via SSH to a {{ ydb-short-name }} dynamic node, you can use `grpcs://$(hostname -f):2136` to get the FQDN.

When running account creation and group assignment commands, the {{ ydb-short-name }} CLI client will prompt for the `root` user password. To avoid repeated password entry, create a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

## Test the Created Database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).

1. Create a test row-oriented table (`test_row_table`) or column-oriented table (`test_column_table`):

{% list tabs %}

- Creating a row-oriented table

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
    ```

- Creating a column-oriented table

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
    ```

{% endlist %}

Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node serving the `/Root/testdb` database.

## Checking Access to the Built-in Web Interface

To check access to the {{ ydb-short-name }} built-in web interface, open `https://<node.ydb.tech>:8765` in your browser, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

Configure your browser to trust the Certificate Authority that issued certificates for the {{ ydb-short-name }} cluster. Otherwise, you will see a warning about an untrusted certificate.

If authentication is enabled in the cluster, the browser will prompt for login and password. After entering valid credentials, the built-in web interface welcome page will appear. The available features and user interface are described in [{#T}](../../../../reference/embedded-ui/index.md).

{% note info %}

A common way to provide access to the {{ ydb-short-name }} built-in web interface is to set up a fault-tolerant HTTP balancer using `haproxy`, `nginx`, or similar software. HTTP balancer configuration details are beyond the scope of the standard {{ ydb-short-name }} installation guide.

{% endnote %}

## Installing {{ ydb-short-name }} in Unprotected Mode

{% note warning %}

We do not recommend using the unprotected {{ ydb-short-name }} mode for production or application development.

{% endnote %}

The installation procedure above assumes {{ ydb-short-name }} deployment in the standard protected mode.

The unprotected {{ ydb-short-name }} mode is intended for test scenarios, primarily related to {{ ydb-short-name }} software development and testing. In unprotected mode:

- Traffic between cluster nodes and between applications and the cluster uses unencrypted connections;
- User authentication is not used (enabling authentication without traffic encryption makes no sense, since the login and password would be transmitted over the network in plain text).

To install {{ ydb-short-name }} for operation in unprotected mode, follow the procedure above with the following exceptions:

1. When preparing for installation, you do not need to generate TLS certificates and keys or copy certificates and keys to the cluster nodes.
1. Remove the `security_config`, `interconnect_config`, and `grpc_config` sections from the cluster node configuration files.
1. Use simplified commands to run static and dynamic cluster nodes: omit options specifying certificate and key file names; use the `grpc` protocol instead of `grpcs` when specifying connection endpoints.
1. Skip the authentication token step before cluster initialization and database creation, as it is not needed in unprotected mode.
1. The cluster initialization command has the following format:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
```

6. The database creation command has the following format:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

7. When accessing the database from the {{ ydb-short-name }} CLI and applications, use grpc instead of grpcs and do not use authentication.
