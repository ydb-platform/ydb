# Deploying a cluster using configuration V1

## Prepare the environment {#deployment-preparation}

Before deploying the system, be sure to complete the preparatory steps. See the document [{#T}](deployment-preparation.md).

## Prepare configuration files {#config}

Prepare the {{ ydb-short-name }} configuration file depending on the topology you have chosen (see [selecting a topology](../../../deployment-options/ansible/initial-deployment/deployment-preparation.md#topology-select)). Examples for each supported topology are provided below in the tabs — select and use the one that suits your case.
Also, if you need to enable Kafka API with topics, add the kafka_proxy_config section to the configuration file (see [configuring Kafka API](../../../reference/configuration/kafka_proxy_config)).

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

To speed up and simplify the initial deployment of YDB, the configuration file already contains most of the settings for installing the cluster. It is enough to replace the standard FQDN hosts with the actual ones in the `hosts` and `blob_storage_config` sections.

- `hosts` section:

  ```yaml
  ...
  hosts:
    - host: static-node-1.ydb-cluster.com # FQDN of the VM
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
      - node_id: static-node-1.ydb-cluster.com # FQDN of the VM
        pdisk_category: SSD
        path: /dev/disk/by-partlabel/ydb_disk_1
  ...
  ```

The rest of the sections and settings in the configuration file remain unchanged.

Save the YDB configuration file as `/opt/ydb/cfg/config.yaml` on each server in the cluster.

For more detailed information on creating a configuration file, see the section [{#T}](../../../../reference/configuration/index.md).
## Copy TLS keys and certificates to each server {#tls-copy-cert}

The prepared TLS keys and certificates must be copied to a secure directory on each node of the {{ ydb-short-name }} cluster. Below is an example of commands for creating a secure directory and copying the key and certificate files.

```bash
sudo mkdir -p /opt/ydb/certs
sudo cp -v ca.crt /opt/ydb/certs/
sudo cp -v node.crt /opt/ydb/certs/
sudo cp -v node.key /opt/ydb/certs/
sudo cp -v web.pem /opt/ydb/certs/
sudo chown -R ydb:ydb /opt/ydb/certs
sudo chmod 700 /opt/ydb/certs
```

## Start static nodes {#start-storage}

{% list tabs group=manual-systemd %}

- Manually

  Start the {{ ydb-short-name }} data storage service on each static node of the cluster:

  ```bash
  sudo su - ydb
  cd /opt/ydb
  export LD_LIBRARY_PATH=/opt/ydb/lib
  /opt/ydb/bin/ydbd server --log-level 3 --syslog --tcp --yaml-config  /opt/ydb/cfg/config.yaml \
      --grpcs-port 2135 --ic-port 19001 --mon-port 8765 --kafka-port 9092 --mon-cert /opt/ydb/certs/web.pem --node static &
  ```

- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-storage.service` on each server that will host a static cluster node, using the example below. You can also [download the file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-storage.service).

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

  Start the service on each static {{ ydb-short-name }} node:

  ```bash
  sudo systemctl start ydbd-storage
  ```

{% endlist %}

After starting the static nodes, check their functionality through the {{ ydb-short-name }} embedded web interface (Embedded UI):

1. Open the address `https://<node.ydb.tech>:8765` in a browser, where `<node.ydb.tech>` is the FQDN of the server running any static node;
2. Go to the **Nodes** tab;
3. Make sure that all 3 static nodes are displayed in the list.

![Manual installation, running static nodes](../../_assets/manual_installation_1.png)

## Initialize the cluster {#initialize-cluster}

The cluster initialization operation configures the set of static nodes listed in the cluster configuration file to store {{ ydb-short-name }} data.

To initialize the cluster, you need a registration authority certificate file `ca.crt`, the path to which must be specified when executing the corresponding commands. Before running the commands, copy the `ca.crt` file to the server where these commands will be executed.

On one of the storage servers in the cluster, run the following commands:

First, get an authorization token for request registration. To do this, run the command below.

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

If the cluster initialization is successful, the command completion code displayed on the screen should be zero.
## Create a Database {#create-db}

To work with string or columnar tables, you need to create at least one database and start the process or processes that serve this database (dynamic nodes).

To execute the administrative command to create a database, you will need the registration authority certificate file `ca.crt`, following the procedure described above for cluster initialization.

When creating a database, the initial number of storage groups is set, which determines the available I/O throughput and maximum storage capacity. The number of storage groups can be increased after the database is created if necessary.

On one of the storage servers in the cluster, run the following commands:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd --ca-file ca.crt -s grpcs://`hostname -f`:2135 -f auth_token \
    admin database /Root/testdb create ssd:8
echo $?
```

If the database is created successfully, the command exit code displayed on the screen should be zero.

The following parameters are used in the command example above:

- `/Root` — the name of the root domain generated automatically during cluster initialization;
- `testdb` — the name of the database to be created;
- `ssd:8` — specifies the storage pool for the database and the number of groups in it. The pool name (`ssd`) must match the disk type specified in the cluster configuration (for example, in `default_disk_type`) and is case-insensitive. The number after the colon is the number of storage groups allocated.

## Start Dynamic Nodes {#start-dynnode}

{% list tabs group=manual-systemd %}

- Manually

  Start the YDB dynamic node for the `/Root/testdb` database:

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
      --node-broker grpcs://<ydb-static-node3>:2135 &
  ```

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

- Using systemd

  Create a systemd configuration file `/etc/systemd/system/ydbd-testdb.service` using the example below. You can also [download the sample file from the repository](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/systemd_services/ydbd-testdb.service).

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

  In the command example above, `<ydb-static-node1>`, `<ydb-static-node2>`, `<ydb-static-node3>` are the FQDNs of any three servers running static cluster nodes.

  Start the YDB dynamic node for the `/Root/testdb` database:

  ```bash
  sudo systemctl start ydbd-testdb
  ```

{% endlist %}

Start additional dynamic nodes on other servers to scale and ensure database fault tolerance.
## Setting Up Accounts {#security-setup}

1. Install {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).

1. Set a password for the `root` account using the previously obtained token:

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --token-file auth_token \
        yql -s 'ALTER USER root PASSWORD "passw0rd"'
    ```

    Replace `passw0rd` with the desired password. Save the password to a separate file. Subsequent commands on behalf of the `root` user will use the password passed via the `--password-file <path_to_user_password>` key. You can also save the password in the connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

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

In the command examples above, `<node.ydb.tech>` is the FQDN of the server running any dynamic node serving the `/Root/testdb` database. When connecting via SSH to a dynamic {{ ydb-short-name }} node, it is convenient to use the `grpcs://$(hostname -f):2136` construct to obtain the FQDN.

When executing commands to create accounts and assign groups, the {{ ydb-short-name }} CLI client will prompt for the `root` user password. You can avoid repeatedly entering the password by creating a connection profile as described in the [{{ ydb-short-name }} CLI documentation](../../../../reference/ydb-cli/profile/index.md).

## Test Working with the Created Database {#try-first-db}

1. Install {{ ydb-short-name }} CLI as described in the [documentation](../../../../reference/ydb-cli/install.md).

1. Create a test row (`test_row_table`) or column table (`test_column_table`):

{% list tabs %}

- Creating a row table

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_row_table` (id Uint64, title Utf8, PRIMARY KEY (id));'
    ```

- Creating a column table

    ```bash
    ydb --ca-file ca.crt -e grpcs://<node.ydb.tech>:2136 -d /Root/testdb --user root \
        yql -s 'CREATE TABLE `testdir/test_column_table` (id Uint64 NOT NULL, title Utf8, PRIMARY KEY (id)) WITH (STORE = COLUMN);'
    ```

{% endlist %}

Where `<node.ydb.tech>` is the FQDN of the server running the dynamic node serving the `/Root/testdb` database.

## Checking Access to the Built-in Web Interface

To check access to the built-in {{ ydb-short-name }} web interface, simply open the page with the address `https://<node.ydb.tech>:8765` in a web browser, where `<node.ydb.tech>` is the FQDN of the server running any static {{ ydb-short-name }} node.

The web browser must be configured to trust the certificate authority that issued the certificates for the {{ ydb-short-name }} cluster, otherwise a warning about an untrusted certificate will be displayed.

If authentication is enabled in the cluster, the web browser will display a login and password prompt. After entering the correct authentication data, the initial page of the built-in web interface should appear. A description of the available functions and user interface is provided in the [section](../../../../reference/embedded-ui/index.md).

{% note info %}

Typically, to provide access to the built-in {{ ydb-short-name }} web interface, a fault-tolerant HTTP load balancer based on software such as `haproxy`, `nginx`, or similar is configured. The details of configuring an HTTP load balancer are beyond the scope of the standard {{ ydb-short-name }} installation instructions.

{% endnote %}
## Features of {{ ydb-short-name }} installation in unsecured mode

{% note warning %}

We do not recommend using the unsecured mode of {{ ydb-short-name }} either in production or during application development.

{% endnote %}

The installation procedure described above предусматривает deploying {{ ydb-short-name }} in the standard secured mode.

The unsecured mode of {{ ydb-short-name }} is intended for solving test tasks primarily related to the development and testing of {{ ydb-short-name }} software. In unsecured mode:

- traffic between cluster nodes, as well as between applications and the cluster, uses unencrypted connections;
- user authentication is not used (enabling authentication without traffic encryption makes no sense, as the login and password would be transmitted over the network in plain text in such a configuration).

Installing {{ ydb-short-name }} for operation in unsecured mode is done following the procedure described above, with the following exceptions:

1. When preparing for installation, there is no need to generate TLS certificates and keys, and no copying of certificates and keys to the cluster nodes is performed.
1. The `security_config`, `interconnect_config`, and `grpc_config` sections are excluded from the configuration files of the cluster nodes.
1. A simplified version of the commands for starting static and dynamic cluster nodes is used: options with certificate and key file names are excluded, and the `grpc` protocol is used instead of `grpcs` when specifying connection points.
1. The step of obtaining an authentication token before initializing the cluster and creating a database, which is unnecessary in unsecured mode, is skipped.
1. The cluster initialization command is executed in the following form:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin blobstorage config init --yaml-file  /opt/ydb/cfg/config.yaml
echo $?
```

6. The database creation command is executed in the following form:

```bash
export LD_LIBRARY_PATH=/opt/ydb/lib
/opt/ydb/bin/ydbd admin database /Root/testdb create ssd:1
```

7. When accessing the database from {{ ydb-short-name }} CLI and applications, the `grpc` protocol is used instead of `grpcs`, and authentication is not used.
