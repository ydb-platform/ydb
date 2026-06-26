# Deployment preparation

## Prerequisites {#prerequisites}

### Choose a topology for installation {#topology-select}

Before installing, choose a suitable {{ ydb-short-name }} cluster topology — it determines the number of servers and disks required:

**To get started with {{ ydb-short-name }}**, we recommend the `mirror-3dc-3-nodes` topology. It requires only 3 servers and 9 disks for user data — the simplest and fastest way to create a pilot cluster.

**For large-scale deployments**, choose one of the following options:

- `mirror-3-dc` — for clusters spread across multiple datacenters. Requires at least 9 servers and 9 disks for user data.
- `block-4-2` — for a cluster in a single datacenter. Requires at least 8 servers and 8 disks for user data.

Each server must have at least one dedicated disk for user data. We also recommend adding a separate small disk for the operating system. For more on topology and redundancy options, see [this article](../../../../concepts/topology.md).

You can [expand the cluster](../../../configuration-management/configuration-v1/cluster-expansion.md) later without stopping the cluster or interrupting user access to data.

### Server configuration {#server-configuration}

{% note info %}

Recommended server requirements:

- 16 CPU cores (based on 8 CPUs used by the storage node and 8 CPUs by the dynamic node).
- 16 GB RAM (recommended minimum).
- Additional SSD disks for data, at least 120 GB each.
- SSH access.
- Network connectivity between machines in the cluster.
- OS: Ubuntu 18+, Debian 9+.
- Internet access for updating repositories and downloading required packages.

See [{#T}](../../../concepts/system-requirements.md) for more details.

{% endnote %}

When using virtual machines from a public cloud provider, we recommend following [{#T}](../preparing-vms-with-terraform.md).

### Software setup

To work with the project on your local (staging or installation) machine, you need:

- Python 3 version 3.10+
- Ansible core version 2.11 through 2.18
- A working directory on a machine with SSH access to all cluster servers

{% note tip %}

We recommend keeping the working directory and all files created during this guide in a [version control system](https://en.wikipedia.org/wiki/Version_control) repository, for example [Git](https://git-scm.com/). If multiple DevOps engineers will work with the cluster being deployed, they should collaborate in a shared repository.

{% endnote %}

If Ansible is already installed, you can go to the [Ansible project setup](#ansible-project-setup) section. If Ansible is not installed yet, install it using one of the following methods:

{% cut "Installing Ansible globally" %}

Using Ubuntu 22.04 LTS as an example:

- Update the apt package list with `sudo apt-get update`.
- Upgrade packages with `sudo apt-get upgrade`.
- Install the `software-properties-common` package to manage your distribution's software sources — `sudo apt install software-properties-common`.
- Add a new PPA to apt — `sudo add-apt-repository --yes --update ppa:ansible/ansible`.
- Install Ansible — `sudo apt-get install ansible-core` (ensure the installed version is no higher than 2.18 and no lower than 2.11; installing the `ansible` package alone will result in an outdated, unsuitable version).
- Check the Ansible core version — `ansible --version`.

{% endcut %}

{% cut "Installing Ansible in a Python virtual environment" %}

Using Ubuntu 22.04 LTS as an example:

- Update the list of available deb packages — `sudo apt-get update`.
- Install the `python3-venv` package for managing Python virtual environments — `sudo apt-get install python3-venv`.
- Create a directory where the virtual environment will be created. For example, `mkdir venv-ansible`.
- Create a Python virtual environment — `python3 -m venv venv-ansible`, where `venv-ansible` is the path to the directory created in the previous step.
- Activate the virtual environment — `source venv-ansible/bin/activate`. All further Ansible steps are performed inside the virtual environment. You can leave it with the `deactivate` command.
- Install the recommended Ansible version with `pip3 install "ansible-core>=2.11,<2.19"` (ensure the installed version is no higher than 2.18 and no lower than 2.11). Check the installed Ansible version — `ansible --version`.

{% endcut %}

For more information and other installation options, see the [Ansible installation guide](https://docs.ansible.com/ansible/latest/installation_guide/index.html).

## Ansible project setup {#ansible-project-setup}

### Installing {{ ydb-short-name }} Ansible playbooks

{% list tabs %}

- Via requirements.yaml

  ```bash
  cat <<EOF > requirements.yaml
  roles: []
  collections:
    - name: git+https://github.com/ydb-platform/ydb-ansible
      type: git
      version: latest
  EOF
  ansible-galaxy install -r requirements.yaml
  ```

- One-time

  ```bash
  ansible-galaxy collection install git+https://github.com/ydb-platform/ydb-ansible.git,latest
  ```

{% endlist %}

After completing the preparation steps, you can proceed to deploying the system. Choose the instruction that matches your configuration:

- [{#T}](deployment-configuration-v1.md)
- [{#T}](deployment-configuration-v2.md)
