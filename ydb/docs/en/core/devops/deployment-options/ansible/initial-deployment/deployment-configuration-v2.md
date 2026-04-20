# Deploying a cluster using configuration V2

{% note alert  %}

This article is about {{ ydb-short-name }} clusters that use **configuration V2**. This configuration method is still experimental and is only available for {{ ydb-short-name }} versions starting with v25.1. For production we recommend [configuration V1](./deployment-configuration-v1.md), which is the main, officially supported configuration for all {{ ydb-short-name }} clusters.

{% endnote %}

## Prepare the environment {#deployment-preparation}

Before deploying the system, complete the preparation steps. See the [{#T}](deployment-preparation.md) document.

## Create a working directory {#prepare-directory}

```bash
mkdir deployment
cd deployment
mkdir -p inventory/group_vars/ydb
mkdir files
```

## Create the Ansible configuration file {#ansible-creat-config}

Create `ansible.cfg` with Ansible configuration suitable for your target deployment environment. See the [Ansible configuration reference](https://docs.ansible.com/ansible/latest/reference_appendices/config.html) for details. This guide assumes the `./inventory` subdirectory of the working directory is set up for inventory files.

{% cut "Example starter ansible.cfg" %}

{% note info %}

Using the `StrictHostKeyChecking=no` parameter in `ssh_args` makes automation easier but reduces SSH connection security (disables host key verification). For production environments, we recommend omitting this argument and configuring trusted keys manually. Use this parameter only for test and temporary installations.

{% endnote %}

```ini
[defaults]
conditional_bare_variables = False
force_handlers = True
forks = 300
gathering = explicit
host_key_checking = False
interpreter_python = /usr/bin/python3
inventory = ./inventory
module_name = shell
pipelining = True
private_role_vars = True
retry_files_enabled = False
timeout = 5
vault_password_file = ./ansible_vault_password_file
verbosity = 1
log_path = ./ydb.log

[ssh_connection]
retries = 5
ssh_args = -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ControlMaster=auto -o ControlPersist=60s -o ControlPath=/tmp/ssh-%h-%p-%r -o ServerAliveCountMax=3 -o ServerAliveInterval=10 
```

{% endcut %}

## Create the main inventory file {#inventory-create}

Create the file `inventory/group_vars/ydb/all.yaml` and fill it.

{% list tabs %}

- mirror-3-dc-3nodes

  ```yaml
  # Ansible
    ansible_user: username
    ansible_ssh_private_key_file: "/path/to/your/id_rsa"
  
  # System
    system_timezone: UTC
    system_ntp_servers: [time.cloudflare.com, time.google.com, ntp.ripe.net, pool.ntp.org]
  
  # Database
    ydb_user: root
    ydb_dbname: db

  # Storage
    ydb_disks:
      - name: /dev/vdb
        label: ydb_disk_1
      - name: /dev/vdc
        label: ydb_disk_2
      - name: /dev/vdd
        label: ydb_disk_3
    ydb_pool_kind: ssd
    ydb_allow_format_drives: true
    ydb_skip_data_loss_confirmation_prompt: false
    ydbops_local: true
    ydb_cores_dynamic: 2
    ydb_dynnodes:
      - {"instance": "a", offset: 1}
    ydb_cores_static:  2
  
  # Authorization settings
    ydb_enforce_user_token_requirement: true

  # Nodes
    ydb_version: "system_version"
   ```

{% endlist %}

Required settings to adapt for your environment in the chosen template:

1. **SSH access configuration.** Specify the `ansible_user` and the path to the private key `ansible_ssh_private_key_file` that Ansible will use to connect to your servers.
2. **Filesystem paths to block devices.** In the `ydb_disks` section, the template assumes `/dev/vda` is for the operating system and the following disks, such as `/dev/vdb`, are for the {{ ydb-short-name }} storage layer. Disk labels are created by the playbooks automatically, and their names can be arbitrary.
3. **System version.** In the `ydb_version` parameter, specify the {{ ydb-short-name }} version to install. The list of available versions is on the [downloads](../../../../downloads/ydb-open-source-database.md) page.

{% include notitle [_](./_includes/recommended-settings.md) %}

{% cut "Additional settings" %}

{% include notitle [_](./_includes/additional-settings.md) %}

{% endcut %}

## Change the root user password {#change-password}

Create the file `ansible_vault_password_file` with the following content:

```bash
password
```

This file contains the password that Ansible will use to encrypt and decrypt sensitive data automatically, for example user password files. This way passwords are not stored in plain text in the repository. For more on how Ansible Vault works, see the [official documentation](https://docs.ansible.com/ansible/latest/vault_guide/index.html).

Next, set the password for the initial user specified in the `ydb_user` setting (default `root`). This user will have full access rights in the cluster initially; you can change this later if needed. Create `inventory/group_vars/ydb/vault.yaml` with the following content (replace `<password>` with the actual password):

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

Encrypt this file with the command `ansible-vault encrypt inventory/group_vars/ydb/vault.yaml`.

## Prepare the {{ ydb-short-name }} configuration file {#ydb-config-prepare}

Create the file `files/config.yaml` and fill it.

{% list tabs %}

- mirror-3-dc-3nodes

  ```yaml
  metadata:
    kind: MainConfig
    cluster: ""
    version: 0
  config:
    yaml_config_enabled: true
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
        rack: '1'
    - host: ydb-node-zone-d.local
      host_config_id: 1
      location:
        body: 3
        data_center: 'zone-d'
        rack: '1'
  actor_system_config:
    use_auto_config: true
    cpu_count: 1
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
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_definitions:
    - member_groups: ["ADMINS"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
  domains_config:
    security_config:
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
    ```

{% endlist %}

To speed up and simplify the initial {{ ydb-short-name }} deployment, the configuration file already contains most of the cluster setup. You only need to replace the placeholder host FQDNs in the `hosts` section and the disk paths in the `host_configs` section with the actual values.

- The `hosts` section:

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

- The `host_configs` section:

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

Leave the rest of the configuration file sections and settings unchanged.

Create the file `inventory/ydb_inventory.yaml` with the following content:

```yaml
plugin: ydb_platform.ydb.ydb_inventory
ydb_config: "files/config.yaml"
```

## Deploy the {{ ydb-short-name }} cluster {#cluster-deployment}

After completing all the preparation steps above, the actual initial cluster deployment is running the following command from the working directory:

```bash
ansible-playbook ydb_platform.ydb.initial_setup
```

Shortly after it starts, you will need to confirm full wipe of the configured disks. Completion can then take tens of minutes depending on the environment and settings. This playbook performs roughly the same steps as in the [manual {{ ydb-short-name }} cluster deployment](../../manual/initial-deployment/index.md) instructions.

### Check cluster state {#cluster-state}

On the last step, the playbook runs several test queries using real temporary tables to verify correct operation. On success, you will see status ok, failed=0, and test query results (3 and 6) for each server when the playbook output is verbose enough.

{% cut "Example output" %}

```txt
...

TASK [ydb_platform.ydb.ydbd_dynamic : run test queries] *******************************************************************************************************************************************************
ok: [static-node-1.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-1.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-2.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-2.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-3.ydb-cluster.com] => (item={'instance': 'a'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "a"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
ok: [static-node-3.ydb-cluster.com] => (item={'instance': 'b'}) => {"ansible_loop_var": "item", "changed": false, "item": {"instance": "b"}, "msg": "all test queries were successful, details: {\"count\":3,\"sum\":6}\n"}
PLAY RECAP ****************************************************************************************************************************************************************************************************
static-node-1.ydb-cluster.com : ok=167  changed=80   unreachable=0    failed=0    skipped=167  rescued=0    ignored=0
static-node-2.ydb-cluster.com : ok=136  changed=69   unreachable=0    failed=0    skipped=113  rescued=0    ignored=0
static-node-3.ydb-cluster.com : ok=136  changed=69   unreachable=0    failed=0    skipped=113  rescued=0    ignored=0
```

{% endcut %}

Running the `ydb_platform.ydb.initial_setup` playbook creates a {{ ydb-short-name }} cluster. It will contain a [domain](../../../../concepts/glossary.md#domain) named from the `ydb_domain` setting (default `Root`), a [database](../../../../concepts/glossary.md#database) named from the `ydb_dbname` setting (default `database`), and an initial [user](../../../../concepts/glossary.md#access-user) named from the `ydb_user` setting (default `root`).

## Additional steps {#additional-steps}

The easiest way to explore the newly deployed cluster is [Embedded UI](../../../../reference/embedded-ui/index.md), which runs on port 8765 on each server. If you do not have direct browser access to that port, set up SSH tunneling by running `ssh -L 8765:localhost:8765 -i <private-key> <user>@<any-ydb-server-hostname>` on your local machine (add more options if needed). After the connection is established, open [localhost:8765](http://localhost:8765) in your browser. The browser may ask you to accept a security exception. Example:

![ydb-web-ui](../../../../_assets/ydb-web-console.png)

After the {{ ydb-short-name }} cluster is created, check its state on this Embedded UI page: [http://localhost:8765/monitoring/cluster/tenants](http://localhost:8765/monitoring/cluster/tenants). It might look like this:

![ydb-cluster-check](../../../../_assets/ydb-cluster-check.png)

This section shows the following {{ ydb-short-name }} cluster parameters:

- `Tablets` — list of running [tablets](../../../../concepts/glossary.md#tablet). All tablet state indicators should be green.
- `Nodes` — number and state of storage and database nodes running in the cluster. The node state indicator should be green, and the number of created and running nodes should match (e.g., 18/18 for a nine-node cluster with one database node per server).

The `Load` (RAM used) and `Storage` (disk space used) indicators should also be green.

You can check the storage group state in the `storage` section — [http://localhost:8765/monitoring/cluster/storage](http://localhost:8765/monitoring/cluster/storage):

![ydb-storage-gr-check](../../../../_assets/ydb-storage-gr-check.png)

The `VDisks` indicators should be green, and the `state` status (in the tooltip when hovering over the Vdisk indicator) should be `Ok`. For more on cluster state indicators and monitoring, see [{#T}](../../../../reference/embedded-ui/ydb-monitoring.md).

### Cluster testing {#testing}

You can test the cluster using the built-in load tests in {{ ydb-short-name }} CLI. [Install {{ ydb-short-name }} CLI](../../../../reference/ydb-cli/install.md) and create a profile with connection parameters, replacing the placeholders:

```shell
{{ ydb-cli }} \
  config profile create <profile-name> \
  -d /<ydb-domain>/<ydb-database> \
  -e grpcs://<any-ydb-cluster-hostname>:2135 \
  --ca-file $(pwd)/files/TLS/certs/ca.crt \
  --user root \
  --password-file <path-to-a-file-with-password>
```

Command parameters and their meaning:

- `config profile create` — creates a connection profile. Specify the profile name. For more on creating and changing profiles, see [{#T}](../../../../reference/ydb-cli/profile/create.md).
- `-e` — endpoint, a string in the form `protocol://host:port`. You can specify the FQDN of any cluster node and omit the port. Port 2135 is used by default.
- `--ca-file` — path to the root certificate for database connections over `grpcs`.
- `--user` — user for database connection.
- `--password-file` — path to the password file. Omit to enter the password manually.

To verify the profile was created, use `{{ ydb-cli }} config profile list`. Activate the profile with `{{ ydb-cli }} config profile activate <profile-name>`. To confirm it is active, run `{{ ydb-cli }} config profile list` again — the active profile will show `(active)`.

To run a [YQL](../../../../yql/reference/index.md) query, use `{{ ydb-cli }} sql -s 'SELECT 1;'`, which returns the result of `SELECT 1` in table form in the terminal. After checking the connection, create a test table with:
`{{ ydb-cli }} workload kv init --init-upserts 1000 --cols 4`. This creates the test table `kv_test` with 4 columns and 1000 rows. To verify the table and data, run `{{ ydb-cli }} sql -s 'select * from kv_test limit 10;'`.

The terminal will show a table of 10 rows. You can then run cluster performance tests. [{#T}](../../../../reference/ydb-cli/workload-kv.md) describes workload types (`upsert`, `insert`, `select`, `read-rows`, `mixed`) and their parameters. Example for the `upsert` workload with `--print-timestamp` and default parameters: `{{ ydb-cli }} workload kv run upsert --print-timestamp`:

```text
Window Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)        Timestamp
1          727 0       0       11      27      71      116     2024-02-14T12:56:39Z
2          882 0       0       10      21      29      38      2024-02-14T12:56:40Z
3          848 0       0       10      22      30      105     2024-02-14T12:56:41Z
4          901 0       0       9       20      27      42      2024-02-14T12:56:42Z
5          879 0       0       10      22      31      59      2024-02-14T12:56:43Z
...
```

When you are done, remove the `kv_test` table with `{{ ydb-cli }} workload kv clean`. For more on test table options and tests, see [{#T}](../../../../reference/ydb-cli/workload-kv.md).

## See also

- [Additional Ansible configuration examples](https://github.com/ydb-platform/ydb-ansible-examples)
- [{#T}](../restart.md)
- [{#T}](../update-config.md)
- [{#T}](../update-executable.md)
