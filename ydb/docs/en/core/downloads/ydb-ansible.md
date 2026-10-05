# Downloading Ansible Playbooks for {{ ydb-short-name }}

A set of automated scripts for installing and maintaining the server part of [YDB Open-Source](ydb-open-source-database.md) or [Yandex Corporate DBMS](yandex-enterprise-database.md) using the [Ansible](https://docs.ansible.com/) tool is available for download via the links below. The source code of the scripts is published [on GitHub](https://github.com/ydb-platform/ydb-ansible) under the Apache 2.0 license.

| Version | Release date | Download | Changelog |
| --- | --- | --- | --- |
| v2.1.0 | 29.06.2026 | [ydb-ansible-2.1.0.tar.gz](https://github.com/ydb-platform/ydb-ansible/releases/download/v2.1.0/ydb_platform-ydb-2.1.0.tar.gz) | [See changelog](#2-1-0) |
| v2.0.0 | 23.12.2025 | [ydb-ansible-2.0.0.tar.gz](https://github.com/ydb-platform/ydb-ansible/releases/download/v2.0.0/ydb_platform-ydb-2.0.0.tar.gz) | [See changelog](#2-0-0) |
| v1.3.2 | 02.12.2025 | [ydb-ansible-1.3.2.tar.gz](https://github.com/ydb-platform/ydb-ansible/releases/download/v1.3.2/ydb_platform-ydb-1.3.2.tar.gz) |  |
| v0.15 | 13.12.2024 | [ydb-ansible-0.15.zip](https://github.com/ydb-platform/ydb-ansible/archive/refs/tags/v0.15.zip) | Minimal set of scripts and roles for launch (deprecated) — [see changelog](#0-15) |

## Ansible Playbooks changelog

## Version 2.1.0 {#2-1-0}

### Features

* Added support for Ansible 2.19 and newer versions.
* Added playbooks for migrating clusters between V1 and V2 configurations:

  * `migrate_to_v1`
  * `migrate_to_v2`
* Added support for new minor versions of Linux distributions.
* Disabled healthcheck for YDB 25.*.
* Added new playbooks:

  * `generate_conf`
  * `run_dstool`
  * `run_ydbd`
* Added support for separate installation of static and dynamic nodes.

### Improvements

* Optimized the `ydbd_newdb` role.
* Grafana dashboards synchronized with the YDB repository.

## Version 2.0.0 {#2-0-0}

### Features

* Added support for installing a cluster with YDB V2 configuration (Distconf).
* Added support for installing a cluster in 2DC mode.
* Added new playbooks for CI:

  * `ydb_platform.ydb.run_ydbd`
  * `ydb_platform.ydb.run_dstool`
  * `ydb_platform.ydb.prepare_drives`

### Improvements

* Documentation updated.
* Extended validation and improvements to simplify the installation process.
* Support for YDB 25+

## Version 1.3.2 {#1-3-2}

25+ is not recommended.

### Features

* The collection has been reworked: Ansible playbooks are now used instead of separate scripts.
* Added a single playbook for installing the entire YDB cluster.
* Added support for installation from YDB distribution files.
* Extended support for modern operating systems.

### Improvements

* Optimized the cluster installation process.
* Improved installation error diagnostics.

## Version 0.15 {#0-15}

Deprecated version.
A collection of separate scripts and roles. Was replaced by playbooks of versions 1.x.x.
