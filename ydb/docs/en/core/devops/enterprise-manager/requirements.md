# Prerequisites {#requirements}

Before installing {{ ydb-short-name }} Enterprise Manager, ensure that all of the following requirements are met.

## {{ ydb-short-name }} Database {#ydb-database}

{{ ydb-short-name }} Enterprise Manager uses a {{ ydb-short-name }} database for storing metadata of its two main components — Gateway and Control Plane. You will need the following information about the database for installation:

* **Endpoint** — the database connection address (for example, `grpcs://ydb-node01.ru-central1.internal:2135`).
* **Username and password** — credentials for obtaining an authentication token.

## {{ ydb-short-name }} Node Configuration {#node-configuration}

Ensure that the {{ ydb-short-name }} nodes intended to work with dynamic slots are configured properly:

* Time is synchronized across all cluster nodes.
* Hostnames match their FQDN.
* TLS certificates are installed on each node.
* {{ ydb-short-name }} binaries are located in the `/opt/ydb` directory.
* The {{ ydb-short-name }} cluster has [dynamic configuration](../configuration-management/index.md) enabled.

<!-- TODO: Clarify the minimum supported YDB version -->
<!-- TODO: Clarify hardware resource requirements (CPU, RAM, disk) for Gateway and CP nodes -->
<!-- TODO: Clarify network connectivity requirements and required ports -->

## Software on the Control Machine {#control-machine}

The following software is required on the machine from which the installation will be performed:

* [Ansible](https://www.ansible.com/) (recommended versions: 2.11 through 2.18).
* SSH access to all cluster servers.

<!-- TODO: Clarify the minimum Python version -->
