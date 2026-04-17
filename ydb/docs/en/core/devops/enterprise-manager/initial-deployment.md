# Initial deployment of YDB EM

<!-- markdownlint-disable blanks-around-fences -->

This guide describes the process of initial deployment of {{ ydb-short-name }} Enterprise Manager (hereinafter referred to as YDB EM) using [Ansible](https://www.ansible.com/). Upon completion, you will have a running YDB EM instance connected to an existing {{ ydb-short-name }} cluster.

<!-- TODO: add alternative deployment methods (manual, Kubernetes) when they become available -->

## Before you start {#before-start}

### Requirements {#requirements}

To deploy YDB EM, you need:

1. **A running {{ ydb-short-name }} cluster**. YDB EM connects to an existing cluster to manage its resources; EM cannot operate without a cluster. A service database for storing YDB EM metadata will be automatically created in this cluster. For more information on deploying a cluster, see [{#T}](../deployment-options/index.md).

1. **{{ ydb-short-name }} database connection parameters** for the database that will be used to store YDB EM metadata:
    * Endpoint (for example, `grpcs://ydb-node01.ru-central1.internal:2135`).
    * Username and password for obtaining an authorization token. The user must have permissions to modify the schema, write, and read data. If the {{ ydb-short-name }} cluster is configured for LDAP authentication, use an account from the corresponding directory.

    <!-- TODO: describe YDB EM security topics (LDAP, TLS, etc.) in a separate article -->

1. **A host for Gateway and Control Plane components** — a single server (physical or virtual) where the YDB EM server components will be installed. This can be any of the {{ ydb-short-name }} cluster hosts or a separate machine.

2. **[Ansible](https://www.ansible.com/)** on the control machine (from which the installation will be performed). ansible-core 2.14–2.18 is recommended; for more details, see Deploying cluster with ansible.

3. **SSH access** from the control machine to all servers where YDB EM components will be installed (the Gateway/CP host and cluster hosts for agents).

### Network requirements {#network-requirements}

The network configuration must allow TCP connections on the following ports (default values):

Source | Destination | Port | Protocol | Purpose
--- | --- | --- | --- | ---
User | Gateway | 8789 | HTTP/HTTPS | Web interface and API
Gateway, Agent | Control Plane | 8787 | gRPC | Management commands
Gateway, Control Plane | {{ ydb-short-name }} | 2135 | gRPC | Connection to YDB EM DB

## Downloading the package {#download}

<!-- TODO: replace the link with the Downloads page when it becomes available for YDB EM -->

Download the YDB EM package to the control machine (from which the installation via Ansible will be performed). Check the latest release information for the current version number and archive link:

Version | Link
--- | ---
`<VERSION>` | `https://binaries.ydbem.website.yandexcloud.net/builds/<VERSION>/ydb-em-<VERSION>-stable-linux-amd64.tar.xz`

Extract the downloaded archive into a working directory (replace `<VERSION>` with the actual version):

```bash
tar -xf ydb-em-<VERSION>-stable-linux-amd64.tar.xz
```

Package contents:

File | Description
--- | ---
`bin/ydb-em-gateway` | Gateway binary
`bin/ydb-em-cp` | Control Plane binary
`bin/ydb-em-agent` | Agent binary
`collections/ydb_platform-ydb-*.tar.gz` | Ansible collection for {{ ydb-short-name }}
`collections/ydb_platform-ydb_em-*.tar.gz` | Ansible collection for YDB EM
`examples.tar.gz` | Ansible configuration templates (inventory and playbooks)
`install.sh` | Automatic installation script

## Installing Ansible collections {#install-collections}

Install the Ansible collection for {{ ydb-short-name }} if it is not already installed:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb-*.tar.gz
```

Install the Ansible collection for YDB EM:

```bash
ansible-galaxy collection install collections/ydb_platform-ydb_em-*.tar.gz
```

{% note info %}

Ansible collection versions are included in the YDB EM package. Use the files from the `collections/` directory of your package.

{% endnote %}

## Preparing the configuration {#prepare-configuration}

### Extracting configuration templates {#unpack-examples}

The YDB EM package includes ready-made Ansible configuration templates — inventory files, playbooks, and directories for placing binaries and certificates. Extract this archive:

```bash
tar -xf examples.tar.gz
```

This will create the `examples/` directory, which will be used as the working directory for the installation.

### Placing files {#place-files}

1. Copy the binary files from the `bin/` directory of the package to the `examples/files/` directory:

    ```bash
    cp bin/ydb-em-gateway bin/ydb-em-cp bin/ydb-em-agent examples/files/
    ```

1. Place TLS certificates in the `examples/files/certs/` directory. Use the same certificates as for the {{ ydb-short-name }} cluster nodes (CA certificate, node certificates, and keys):

    ```bash
    cp /path/to/your/certs/* examples/files/certs/
    ```

### Configuring the Ansible inventory {#configure-inventory}

Ansible uses inventory files to describe target servers and their parameters. The YDB EM configuration is split into several files:

* `examples/inventory/50-inventory.yaml` — common parameters: SSH connection, YDB connection, Control Plane settings.
* `examples/inventory/90-inventory.yaml` — server descriptions (which hosts are used for which components).
* `examples/inventory/99-inventory-vault.yaml` — sensitive data (passwords).

#### Configuring hosts {#configure-hosts}

Open the file `examples/inventory/90-inventory.yaml` and configure two host groups.

**Group `ydb_em`** — hosts for YDB EM server components (Gateway and Control Plane). A single host is sufficient — it can be any of the {{ ydb-short-name }} cluster hosts or a separate server:

```yaml
ydb_em:
  hosts:
    ydb-node01.ru-central1.internal:
```

**Group `ydbd_dynamic`** — {{ ydb-short-name }} cluster hosts where the Agent will be installed. An Agent is needed on every host that runs {{ ydb-short-name }} dynamic nodes — YDB EM manages node processes through these agents. Typically, all cluster hosts should be included:

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
    ydb_em:
      hosts:
        ydb-node01.ru-central1.internal:
```

For each host in the `ydbd_dynamic` group, you can specify additional parameters. If parameters are not specified, default values will be used. Resource settings can be adjusted later through the YDB EM web interface.

Parameter | Description
--- | ---
`ydb_em_agent_cpu` | Number of CPUs available for nodes on the host
`ydb_em_agent_memory` | Amount of RAM (in gigabytes) available for nodes on the host
`ydb_em_agent_name` | Host name used by the agent
`location` | Host location (availability zone)

{% note warning %}

{{ ydb-short-name }} binaries must be installed on all hosts in the `ydbd_dynamic` group before deploying YDB EM. For more details, see Deploying cluster with ansible.

{% endnote %}

#### Configuring SSH connection {#configure-ssh}

In the file `examples/inventory/50-inventory.yaml`, verify the SSH connection parameters from the control machine to the target servers:

```yaml
# Remote user with sudo privileges
ansible_user: ansible

# Settings for connecting through a Bastion/Jump host (JUMP_IP)
# ansible_ssh_common_args: "-o ProxyJump=ansible@{{ lookup('env','JUMP_IP') }} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

# Common SSH settings
ansible_ssh_common_args: "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

# Private key for connection
ansible_ssh_private_key_file: "~/.ssh/id_ed25519"
```

{% note warning %}

The `UserKnownHostsFile=/dev/null` and `StrictHostKeyChecking=no` parameters disable SSH host key verification, making the connection vulnerable to man-in-the-middle (MITM) attacks. This configuration is only acceptable for test environments. In production environments, it is recommended to use host key verification by specifying the path to a real `known_hosts` file:

```yaml
ansible_ssh_common_args: "-o StrictHostKeyChecking=yes"
```

{% endnote %}

{% note info %}

If a Bastion/Jump host is used to access the servers, uncomment and configure the corresponding `ansible_ssh_common_args` line.

{% endnote %}

#### Configuring the {{ ydb-short-name }} database connection {#configure-ydb-connection}

In the file `examples/inventory/50-inventory.yaml`, specify the connection parameters for the {{ ydb-short-name }} database that will be used to store YDB EM metadata:

```yaml
# YDB connection endpoint
ydb_em_db_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:2135"
# Root domain
ydb_em_db_connection_db_root: "/Root"
# Database name
ydb_em_db_connection_db: "db"
# YDB user
ydb_user: root
```

The {{ ydb-short-name }} user password is set in the file `examples/inventory/99-inventory-vault.yaml`:

```yaml
all:
  children:
    ydb:
      vars:
        ydb_password: <password>
```

{% note tip %}

To protect sensitive data, it is recommended to encrypt the `99-inventory-vault.yaml` file using [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html):

```bash
ansible-vault encrypt examples/inventory/99-inventory-vault.yaml
```

{% endnote %}

#### Configuring the Control Plane connection {#configure-cp-connection}

In the file `examples/inventory/50-inventory.yaml`, specify the YDB EM Control Plane connection endpoint:

```yaml
# The endpoint must point to a host from the ydb_em group
ydb_em_cp_connection_endpoint: "grpcs://ydb-node01.ru-central1.internal:8787"
```

This parameter is used in the configuration of **Gateway** and **Agent** for connecting to the **Control Plane**.

{% note info %}

For high availability, you can specify a load balancer address (for example, an L3/L4 load balancer or DNS-based balancing) instead of a direct host address.

{% endnote %}

## Running the installation {#run-installation}

After completing the configuration, navigate to the `examples` directory and run the installation using one of the following methods:

### Automatic installation {#auto-install}

Run the automatic installation script:

```bash
cd examples
./install.sh
```

The script will automatically perform all the necessary steps to deploy YDB EM components.

### Manual Ansible run {#manual-install}

You can also run the installation manually by executing the Ansible playbook:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup
```

{% note info %}

If the `99-inventory-vault.yaml` file is encrypted with Ansible Vault, add the `--ask-vault-pass` flag:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup --ask-vault-pass
```

{% endnote %}

## Verifying the installation {#verify}

After the installation completes successfully, open the YDB EM web interface in a browser:

```text
https://<FQDN of the host from the ydb_em group>:8789/ui/clusters
```

For example:

```text
https://ydb-node01.ru-central1.internal:8789/ui/clusters
```

The web interface should display a page with a list of {{ ydb-short-name }} clusters.

## Troubleshooting {#troubleshooting}

### Cannot connect to the web interface {#cannot-connect-ui}

1. Make sure port `8789` is accessible from your machine:

    ```bash
    curl -k https://<FQDN>:8789/ui/clusters
    ```

1. Check the status of the YDB EM Gateway service on the target host:

    ```bash
    sudo systemctl status ydb-em-gateway
    ```

1. Check the service logs:

    ```bash
    sudo journalctl -u ydb-em-gateway -n 100
    ```

### Agent cannot connect to Control Plane {#agent-not-connected}

1. Make sure port `8787` is accessible between the cluster nodes and the Control Plane host.

1. Check the status of the YDB EM Agent service on the node:

    ```bash
    sudo systemctl status ydb-em-agent
    ```

1. Verify the correctness of TLS certificates and the Control Plane endpoint in the agent configuration.

### {{ ydb-short-name }} database connection errors {#db-connection-errors}

1. Make sure the endpoint, database name, username, and password are specified correctly.

1. Check network accessibility of port `2135` on the {{ ydb-short-name }} node from the YDB EM host.

1. Check the YDB EM Control Plane logs:

    ```bash
    sudo journalctl -u ydb-em-cp -n 100
    ```
