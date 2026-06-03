# Deploying a cluster using V1 configuration

## Prepare the environment {#deployment-preparation}

Before deploying the system, be sure to perform the preparatory steps. See the document [{#T}](deployment-preparation.md).

## Prepare configuration files {#config}

Prepare the {{ ydb-short-name }} configuration file depending on the topology you selected (see [topology selection](../../../deployment-options/ansible/initial-deployment/deployment-preparation.md#topology-select)). Examples for each supported topology are provided below in the tabs — select and use the one suitable for your case.
Also, if you need to enable Kafka API support for topics, add the kafka_proxy_config section to the configuration file (see [Kafka API configuration](../../../reference/configuration/kafka_proxy_config)).

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
      data_center: 'zone-a'
      rack: '1'
  - host: static-node-2.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 2
      data_center: 'zone-b'
      rack: '2'
  - host: static-node-3.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 3
      data_center: 'zone-c'
      rack: '3'
  domains_config:
    security_config:
      enforce_user_token_requirement: true
      monitoring_allowed_sids:
      - "root"
      - ADMINS
      - DATABASE-ADMINS
      administration_allowed_sids:
      - "root"
      - ADMINS
      - DATABASE-ADMINS
      viewer_allowed_sids:
      - "root"
      - ADMINS
      - DATABASE-ADMINS
      register_dynamic_node_allowed_sids:
      - databaseNodes@cert
      - root@builtin
      bootstrap_allowed_sids:
      - "root"
      - ADMINS
      - DATABASE-ADMINS
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
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          - vdisk_locations:
            - node_id: static-node-1.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_03
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_03
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_02
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_03
  channel_profile_config:
    profile:
    - channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 0
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 0
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 0
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
  ```- mirror-3-dc-9nodes```yaml
  static_erasure: mirror-3-dc
  host_configs:
  - drive:
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      type: SSD
    - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
      type: SSD
    host_config_id: 1
  hosts:
  - host: static-node-1.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 1
      data_center: 'zone-a'
      rack: '1'
  - host: static-node-2.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 2
      data_center: 'zone-a'
      rack: '2'
  - host: static-node-3.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 3
      data_center: 'zone-a'
      rack: '3'
  - host: static-node-4.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 4
      data_center: 'zone-b'
      rack: '4'
  - host: static-node-5.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 5
      data_center: 'zone-b'
      rack: '5'
  - host: static-node-6.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 6
      data_center: 'zone-b'
      rack: '6'
  - host: static-node-7.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 7
      data_center: 'zone-c'
      rack: '7'
  - host: static-node-8.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 8
      data_center: 'zone-c'
      rack: '8'
  - host: static-node-9.ydb-cluster.com
    host_config_id: 1
    walle_location:
      body: 9
      data_center: 'zone-c'
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
      spin_threshold: 0
      threads: 2
      type: BASIC
    - name: User
      spin_threshold: 0
      threads: 3
      type: BASIC
    - name: Batch
      spin_threshold: 0
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
          - vdisk_locations:
            - node_id: static-node-2.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-3.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-4.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-5.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-6.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: static-node-7.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-8.ydb-cluster.com
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
          - vdisk_locations:
            - node_id: static-node-9.ydb-cluster.com
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
  ```- block-4-2```yaml
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
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-2.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-3.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-4.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-5.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-6.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
          - vdisk_locations:
            - node_id: "ydb-node-zone-a-7.local"
              pdisk_category: SSD
              path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - fail_domains:
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

To speed up and simplify the initial deployment of {{ ydb-short-name }}, the configuration file already contains most of the settings for installing the cluster. Just replace the default FQDN hosts with the actual ones in the `hosts `and` blob_storage_config` sections.

- Section `hosts`:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com #FQDN VM
      host_config_id: 1
      walle_location:
        body: 1
        data_center: 'zone-a'
        rack: '1'
  ...
  ```
  ...
  - fail_domains:
    - vdisk_locations:
      - node_id: static-node-1.ydb-cluster.com #FQDN VM
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```yaml

The remaining sections and settings of the configuration file remain unchanged.

Save the {{ydb-short-name}} configuration file as `/opt/ydb/cfg/config.yaml` on each server of the cluster.

For more information on creating the configuration file, see the [{#T}](../../../../reference/configuration/index.md) section.

## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each cluster node {{ ydb-short-name }}.[{#T}](../../../../reference/configuration/index.md)

Details on preparation are in the [{#T}](deployment-preparation.md#tls-certificates) section. If certificates were generated using the `tls_cert_gen`script, take `ca.crt` from the`CA/certs/YYYY-MM-DD_hh-mi-ss`directory (use the timestamp from the script output or choose the appropriate directory in`CA/certs/`), and take `node.crt `, `node.key`, and ` web.pem`from the subdirectory named after the server's FQDN in`ydb-ca-nodes.txt`. Transfer these four files to the corresponding cluster server (for example, via `scp`) into a single local directory.

On each server, in the directory where `ca.crt `, `node.crt `, ` node.key`, and ` web.pem` for this node are already located, run the commands below.

Below is an example of creating a secure directory and copying:[{#T}](deployment-preparation.md#tls-certificates)

The prepared TLS keys and certificates must be copied to a secure directory on each cluster node {{ ydb-short-name }}. Below is an example of commands for creating a secure directory and copying the key and certificate files.

  ```
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```bash

## Start static nodes {#start-storage}

{% list tabs group=manual-systemd %}

- Manually

  Start the {{ ydb-short-name }} storage service on each static cluster node:

     ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config /opt/ydb/cfg/config.yaml --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --mon-cert /opt/ydb/certs/web.pem --node static
     ```
- Using systemd

  On each server that will host a static cluster node, create a systemd configuration file `/etc/systemd/system/ydbd-storage.service`according to the sample below. You can also [download the sample from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).```bash
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
  WantedBy=multi-user.target [`web.pem`check](#tls-copy-cert)```bash
  sudo systemctl start ydbd-storage 
     ```bash

{% endlist %}

After starting the static nodes, verify their operation via the {{ ydb-short-name }} embedded web interface (Embedded UI):[скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service)

1. Open the address `https://<static-node.ydb.tech>:8765`in a browser, where`<static-node.ydb.tech>` is the FQDN of the server running any static node;
2. If the browser requests a login and password, log in with the `root`account using an empty password — at this deployment stage, the`root` password is not yet set;
3. Go to the **Nodes** tab.
3. Make sure all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file for {{ ydb-short-name }} data storage.

To initialize the cluster, you need the certificate authority file `ca.crt`, whose path must be specified when running the relevant commands. Before running the commands, copy the `ca.crt` file to the server where the commands will be executed.

On one of the storage servers in the cluster, run the commands:

First, obtain an authorization token for registering requests. To do this, run the command below.

     ```bash
/opt/ydb/bin/ydb --ca-file ca.crt -e grpcs://`hostname -f`:2135 -d /Root --user root --no-password auth get-token -f > auth_token
     ```
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
     ```bash

If the cluster initialization is successful, the exit code of the cluster initialization command displayed on the screen should be zero.

## Create a database {#create-db}

To work with row or column tables, you need to create at least one database and start the process or processes serving that database (dynamic nodes).

To run the administrative command for creating a database, you need the certificate authority file `ca.crt`, similar to the cluster initialization procedure described above.

When creating a database, the initial number of storage groups used is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after the database is created if necessary.

On one of the storage servers in the cluster, run the commands:

     ```
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file /opt/ydb/certs/ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
     ```bash

If the database is created successfully, the exit code of the command displayed on the screen should be zero.

If the database is created successfully, the exit code of the command displayed on the screen should be zero.

The command example above uses the following parameters:

- `/Root` — the root domain name automatically generated during cluster initialization.
- `testdb` — the name of the database being created.
- `ssd:8` — sets the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of allocated storage groups.

## Start dynamic nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Start the dynamic node {{ ydb-short-name }} for database `/Root/testdb`. Perform the commands below sequentially. Do not paste all lines together with `sudo su - ydb`— otherwise, subsequent commands may not run as the`ydb` user.

After step 1, wait for the `ydb@...$` prompt.

On step 4, wait for `ydbd`output in the same terminal. In the command below, replace`<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` with the FQDNs of any three servers running static nodes.

1. Switch to the `ydb`user:```bash
     sudo su - ydb
     ```bash
     cd /opt/ydb
     ```

     If the [`web.pem`check](#tls-copy-cert) is not passed, you will get a`Permission denied`error for`/opt/ydb/certs/web.pem`.

In the example command above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.
- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service`using the sample below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).```bash
  [Unit]
  Description=YDB testdb dynamic node
  After=network-online.target rc-local.service [{#T}](deployment-preparation.md#requirements)
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
  SyslogLevel=err [{#T}](deployment-preparation.md#requirements)
  Environment=LD_LIBRARY_PATH=/opt/ydb/lib
  ExecStart=/opt/ydb/bin/ydbd server \
      --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
      --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
      --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \[{#T}](deployment-preparation.md#requirements)
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
     ```bash
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
      --kafka-port 9093 \[проверка `web.pem`](#tls-copy-cert)
      --yaml-config  /opt/ydb/cfg/config.yaml \
      --tenant /Root/testdb \
      --grpc-cert /opt/ydb/certs/node.crt \
      --grpc-key /opt/ydb/certs/node.key \[скачать из репозитория](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service)
      --node-broker grpcs://<ydb-static-node1>:2135 \
      --node-broker grpcs://<ydb-static-node2>:2135 \
      --node-broker grpcs://<ydb-static-node3>:2135
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=32212254720

  [Install]
  WantedBy=multi-user.target
     ```

Run additional dynamic nodes on other servers to scale and ensure fault tolerance of the database.

{% list tabs group=manual-systemd %}

- Manually

  Start the dynamic node {{ ydb-short-name }} for database `/Root/testdb`. Perform the commands below sequentially. Do not paste all lines together with `sudo su - ydb`— otherwise, subsequent commands may not run as the `ydb` user.

  After step 1, wait for the `ydb@...$` prompt.

  On step 4, wait for `ydbd`output in the same terminal. In the command below, replace`<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` with the FQDNs of any three servers running static nodes.

  1. Switch to the `ydb`user:```sudo su - ydb```ini

  2. Go to the installation directory:

     ```bash

     If the [`web.pem`check](#tls-copy-cert) is not passed, you will get a`Permission denied`error for`/opt/ydb/certs/web.pem`.

- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service`using the sample below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).```
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
  ExecStart=/opt/ydb/bin/ydbd server \[документации](../../../../reference/ydb-cli/install.md)
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
      --node-broker grpcs://<ydb-static-node3>:2135 [документации {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/profile/index.md)
  LimitNOFILE=65536
  LimitCORE=0
  LimitMEMLOCK=32212254720

  [Install]
  WantedBy=multi-user.target
     ```

{% endlist %}

Run additional dynamic nodes on other servers to scale and ensure fault tolerance of the database.

If the [`web.pem`check](#tls-copy-cert) is not passed, you will get a`Permission denied`error for`/opt/ydb/certs/web.pem`.

## Setting up accounts {#security-setup}

1. Install the {{ ydb-short-name }} CLI as described in the [{#T}](../../../../reference/embedded-ui/index.md).
2. Set a password for the `root`account using the token obtained earlier:

  ```[документации](../../../../reference/ydb-cli/install.md)
    ydb --ca-file /opt/ydb/certs/ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'[{#T}](#security-setup).
```bash
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'CREATE USER user1 PASSWORD "passw0rd"'
```
   ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root --password-file <path_to_root_pass_file> \
       yql -s 'ALTER GROUP `ADMINS`ADD USER user1'```In the command examples listed above,`<node.ydb.tech>`is the FQDN of the server where any dynamic node serving the`/Root/testdb`database is running. When connecting via SSH to a dynamic node {{ ydb-short-name }}, it is convenient to use the`grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create user accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt for the password of user `root`. To avoid entering the password multiple times, you can create a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

## Test the created database {#try-first-db}

1. Install the {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).
2. Create a test row-oriented (`test_row_table`) or column-oriented table (`test_column_table`):

{% list tabs %}

- Creating a row table

    ```bash
    ydb --ca-file /opt/ydb/certs/ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
    ```
    ydb --ca-file /opt/ydb/certs/ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'``` ```

{% endlist %}

Where `<node.ydb.tech>`is the FQDN of the server running the dynamic node serving the`/Root/testdb` database.

## Checking access to the built-in web interface

To check access to the built-in web interface {{ ydb-short-name }}, simply open the page with the address `https://<static-node.ydb.tech>:8765`in a web browser, where`<node.ydb.tech>` is the FQDN of the server running any static node {{ ydb-short-name }}.

The web browser must be configured to trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster; otherwise, a warning about using an untrusted certificate will be displayed.

If authentication is enabled in the cluster, a login and password prompt should appear in the web browser. After entering valid authentication credentials, the initial page of the built-in web interface should be displayed. A description of the available functions and user interface is provided in the [{#T}](../../../../reference/embedded-ui/index.md) section.

{% note info %}

Typically, to provide access to the built-in web interface {{ ydb-short-name }}, a fault-tolerant HTTP load balancer based on software `haproxy `, ` nginx`, or analogues is configured. Details of configuring the HTTP load balancer are beyond the scope of the standard installation instructions for {{ ydb-short-name }}.

{% endnote %}

## Checking access to the built-in web interface

To check access to the built-in web interface {{ ydb-short-name }}, simply open the page with the address `https://<static-node.ydb.tech>:8765`in a web browser, where`<static-node.ydb.tech>` is the FQDN of the server running any static node {{ ydb-short-name }}.

The web browser must be configured to trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster; otherwise, a warning about using an untrusted certificate will be displayed.

If authentication is enabled in the cluster, a login and password prompt should appear in the web browser. After entering valid authentication credentials, the initial page of the built-in web interface should be displayed. A description of the available functions and user interface is provided in the [{#T}](../../../../reference/embedded-ui/index.md) section.
