# Configuration {#configuration}

Before running the deployment, you need to configure the Ansible inventory files. All configuration files are located in the `examples/inventory` directory.

## Host Inventory Setup {#inventory}

Edit the file `examples/inventory/50-inventory.yaml` — it contains the cluster host definitions.

### Host Groups {#host-groups}

The inventory defines two host groups:

* **`ydb_em`** — hosts where {{ ydb-short-name }} EM Gateway and {{ ydb-short-name }} EM Control Plane will be installed.
* **`ydbd_dynamic`** — hosts where {{ ydb-short-name }} EM Agent will be installed. {{ ydb-short-name }} binaries must already be installed on these hosts.

### Inventory Example {#inventory-example}

```yaml
ydb:
  children:
    ydbd_dynamic:
      hosts:
        ydb-node01.ru-central1.internal:
          ydb_em_agent_cpu: 4
          ydb_em_agent_memory: 8
          location: db-dc-1
        ydb-node02.ru-central1.internal:
          ydb_em_agent_cpu: 4
          ydb_em_agent_memory: 16
          location: db-dc-2
        ydb-node03.ru-central1.internal:
          location: db-dc-3
    # The ydb_em group is required
    ydb_em:
      hosts:
        ydb-node01.ru-central1.internal:
```

### Host Parameters {#host-parameters}

The following parameters can be set for hosts in the `ydbd_dynamic` group:

| Parameter | Description |
|-----------|-------------|
| `ydb_em_agent_cpu` | Number of CPUs available for dynamic slots on the host |
| `ydb_em_agent_memory` | Amount of RAM (in gigabytes) available for dynamic slots on the host |
| `ydb_em_agent_name` | Hostname used by the agent |
| `location` | Host location (data center) |

<!-- TODO: Clarify default values for ydb_em_agent_cpu and ydb_em_agent_memory if not specified -->
<!-- TODO: Clarify whether these parameters are required or optional -->

## {{ ydb-short-name }} Password Setup {#password}

The file `examples/inventory/99-inventory-vault.yaml` contains the password for connecting to the {{ ydb-short-name }} database.

{% note warning %}

It is recommended to use [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html) to encrypt the password file.

{% endnote %}

<!-- TODO: Clarify the format of 99-inventory-vault.yaml and provide an example of its contents -->

## SSH Connection Settings {#ssh}

Check the SSH connection settings in the inventory:

```yaml
# Remote user with sudo privileges
ansible_user: ansible
# Settings for connecting via Bastion/Jump host (JUMP_IP)
# ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
# Common settings
ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
# Private key for the remote user
ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
```

## {{ ydb-short-name }} Connection Settings {#ydb-connection}

Specify the connection parameters for the {{ ydb-short-name }} database:

```yaml
# YDB EM database address
ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
ydb_em_db_connection_db_root: "/Root"
ydb_em_db_connection_db: "db"
ydb_user: root
# YDB password is set in 99-inventory-vault.yaml,
# but can be defined here
# ydb_password: password
```

<!-- TODO: Clarify whether the database needs to be created in advance or is created automatically -->
<!-- TODO: Provide a more detailed description of ydb_em_db_connection_db_root and ydb_em_db_connection_db parameters -->

## Control Plane Connection Settings {#cp-connection}

Specify the {{ ydb-short-name }} EM Control Plane connection address:

```yaml
# Must point to a host in the ydb_em group.
# This parameter is used in YDB EM Gateway
# and YDB EM Agent configurations.
# You can also use a proxy endpoint
# as a frontend for high-availability solutions.
ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
```

<!-- TODO: Clarify the default port for CP (8787) and whether it can be changed -->
<!-- TODO: Describe high-availability configuration options for CP -->
