# Installation

This guide describes how to prepare and install YDB Enterprise Manager.

## Requirements

YDB EM uses a YDB database as a storage backend for metadata for its two main components (Gateway and Control Plane). Therefore, you must have the following information about your YDB database:
- **Endpoint**: e.g., `grpcs://ydb-node01.ru-central1.internal:2135`
- **Credentials**: Username and password to obtain a token.

Additionally, you ensure that the YDB nodes for the dynamic slots are configured properly:
- Time is synchronized across nodes.
- Hostnames use their Fully Qualified Domain Name (FQDN).
- TLS certificates are configured for every node.
- Binaries are located in the `/opt/ydb` path.
- YDB has dynamic configuration enabled.

<!-- TODO: Detail step-by-step verification of these requirements if necessary -->

## Download Package

Download the latest package from our website:

| Release | URL |
|---|---|
| 1.0.1  | [http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz](http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz) |

<!-- TODO: Update the website URL to the official release page instead of direct download link if available -->

## Prepare and Install

1. Check the YDB cluster and obtain the [required information](#requirements).
2. Download the package to your local computer and unpack it. The package consists of:
   - `ydb-em-gateway`: Binary file (YDB EM Gateway).
   - `ydb-em-cp`: Binary file (YDB EM CP).
   - `ydb-em-agent`: Binary file (YDB EM CP Agent).
   - `ydb_platform-ydb-XXX.tar.gz`: Ansible collection for YDB.
   - `ydb_platform-ydb_em-YYY.tar.gz`: Ansible collection for YDB EM.
   - `examples.tar.gz`: Sample configuration for Ansible.
   - `prepare.sh`: Script for automating steps 3-5.
3. Install the YDB Ansible collection if it's absent:
   ```bash
   ansible-galaxy collection install ydb_platform-ydb-1.2.0.tar.gz
   ```
4. Install the YDB EM Ansible collection:
   ```bash
   ansible-galaxy collection install ydb_platform-ydb_em-1.0.1.tar.gz
   ```
5. Unpack `examples.tar.gz`.
6. Move all binaries from the package to the `examples/files` folder.
7. Put the TLS certificates in the `examples/files/certs` folder.
8. Update the Ansible inventory files: `examples/inventory/50-inventory.yaml` and `examples/inventory/99-inventory-vault.yaml`.
   - `99-inventory-vault.yaml`: This contains the password for YDB.
   - Update the list of hosts:
     - Group `ydb_em`: Hosts for the YDB EM Gateway and YDB EM CP.
     - Group `ydbd_dynamic`: Hosts for the YDB EM CP Agent (YDB binaries must be installed before this installation).

     ```yaml
     ydb:
       children:
         ydbd_dynamic:
           hosts:
             ydb-node01.ru-central1.internal:
             # Example of special values for a host
             # ydb_em_agent_cpu - CPU available for slots on the host
             # ydb_em_agent_memory - RAM in gigabytes available for slots on the host
             # ydb_em_agent_name - hostname for the host for agent
                 ydb_em_agent_cpu: 4
                 ydb_em_agent_memory: 8
                 location: db-dc-1
             ydb-node02.ru-central1.internal:
                 ydb_em_agent_cpu: 4
                 ydb_em_agent_memory: 16
                 location: db-dc-2
             ydb-node03.ru-central1.internal:
                 location: db-dc-3
         # Group `ydb-em` is required
         ydb_em:
           hosts:
             ydb-node01.ru-central1.internal:
     ```

9. Check Ansible SSH connection settings:
   ```yaml
   # Remote user with sudo rights
   ansible_user: ansible
   # Settings for connection via Bastion/Jump host (JUMP_IP)
   # ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
   # Common settings
   ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
   # Private key for the remote user
   ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
   ```

10. Check YDB connection settings:
    ```yaml
    # The location of the YDB DB. It can be any location
    ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
    ydb_em_db_connection_db_root: "/Root"
    ydb_em_db_connection_db: "db"
    ydb_user: root
    # YDB password is placed in 99-inventory-vault.yaml
    # but you can define it here
    # ydb_password: password
    ```

11. Check YDB CP connection settings:
    ```yaml
    # It must be pointing to a host in the `ydb_em` group
    # This setting is used for YDB EM Gateway and YDB EM CP Agent configurations.
    # But you may use any proxy endpoint as a frontend for HA solutions.
    ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
    ```

12. Run `./install.sh` in the `examples` folder or execute the playbook manually:
    ```bash
    ansible-playbook ydb_platform.ydb_em.initial_setup
    ```
