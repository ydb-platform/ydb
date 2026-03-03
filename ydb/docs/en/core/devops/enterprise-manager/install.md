# Downloading and Preparing the Package {#install}

## Downloading the Package {#download}

Download the latest version of the {{ ydb-short-name }} Enterprise Manager package:

| Version | Link |
|---------|------|
| 1.0.1   | [ydb-em-1.0.1-stable-linux-amd64.tar.xz](http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz) |

<!-- TODO: Clarify the actual download URL and the mechanism for updating the version list -->

## Package Contents {#package-contents}

After unpacking the archive, you will get the following files:

| File | Description |
|------|-------------|
| `ydb-em-gateway` | {{ ydb-short-name }} EM Gateway executable |
| `ydb-em-cp` | {{ ydb-short-name }} EM Control Plane executable |
| `ydb-em-agent` | {{ ydb-short-name }} EM Agent executable |
| `ydb_platform-ydb-XXX.tar.gz` | Ansible collection for {{ ydb-short-name }} |
| `ydb_platform-ydb_em-YYY.tar.gz` | Ansible collection for {{ ydb-short-name }} EM |
| `examples.tar.gz` | Sample Ansible configuration |
| `prepare.sh` | Script for automating preparatory steps |

<!-- TODO: Clarify what prepare.sh does exactly and which steps it automates -->
<!-- TODO: Clarify Ansible collection version numbers (XXX, YYY) -->

## Installing Ansible Collections {#install-collections}

1. Install the Ansible collection for {{ ydb-short-name }}, if it is not already installed:

   ```bash
   ansible-galaxy collection install ydb_platform-ydb-1.2.0.tar.gz
   ```

2. Install the Ansible collection for {{ ydb-short-name }} Enterprise Manager:

   ```bash
   ansible-galaxy collection install ydb_platform-ydb_em-1.0.1.tar.gz
   ```

## Preparing the Working Directory {#prepare-workspace}

1. Unpack the `examples.tar.gz` file:

   ```bash
   tar -xzf examples.tar.gz
   ```

2. Move all executables from the package to the `examples/files` directory:

   ```bash
   cp ydb-em-gateway ydb-em-cp ydb-em-agent examples/files/
   ```

3. Place the TLS certificates in the `examples/files/certs` directory.

<!-- TODO: Clarify the format and requirements for TLS certificates (CA, server, client) -->
<!-- TODO: Clarify the directory structure of examples after unpacking -->
