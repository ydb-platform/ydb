<img width="64" src="ydb/docs/_assets/logo.svg"/><br/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![PyPI version](https://badge.fury.io/py/ydb.svg)](https://badge.fury.io/py/ydb)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/yandexdatabase_ru)

## YDB Platform

[Website](https://ydb.tech) |
[Documentation](https://ydb.tech/docs) |
[Official Repository](https://github.com/ydb-platform/ydb) |
[YouTube Channel](https://www.youtube.com/channel/UCHrVUvA1cRakxRP3iwA-yyw)

YDB is an open-source Distributed SQL Database that combines high availability and scalability with strict consistency and ACID transactions.

<p align="center">
  <a href=""><img src="ydb/docs/_assets/ydb-promo-video.png" width="70%"/></a>
</p>

## Main YDB Advantages

YDB was designed from scratch as a response to growing demand for scalable interactive web services. Scalability, strict consistency and effective cross-row transactions were a must for such OLTP-like workload. YDB was built by people with strong background in databases and distributed systems, who had an experience of developing No-SQL database and the Map-Reduce system for one of the largest search engines in the world.
We found that YDB's flexible design allows us to build more services on top of it including persistent queues and virtual block devices.

Basic YDB features:

  - Fault-tolerant configuration that survive disk, node, rack or even datacenter outage;
  - Horizontal scalability;
  - Automatic disaster recovery with minimum latency disruptions for applications;
  - SQL dialect (YQL) for data manipulation and scheme definition;
  - ACID transactions across multiple nodes and tables with strict consistency.

### Fault-tolerant configurations

YDB could be deployed in three availability zones. Cluster remains available for both reads and writes during complete outage of a single zone.

Availability zones and regions are covered in more detail [in documentation](https://ydb.tech/en/docs/concepts/databases#regions-az).

### Horizontal scalability

Unlike traditional relational databases YDB [scales out](https://en.wikipedia.org/wiki/Scalability#Horizontal_or_scale_out) providing developers with capability to simply extend cluster with computation or storage resources to handle increasing load.

Current production installations have more than 10,000 nodes, store petabytes of data and handle more than 100,000 distributed transactions per second.

### Automatic disaster recovery

YDB Platform has built-in automatic recovery support to survive a hardware failure. After unpredictable disk, node, rack or even datacenter failure YDB platform remains fully available for reads and writes and restores required data redundancy automatically.

## Supported platforms

### Minimal system requirements

YDB runs on x86 64bit platforms with minimum 8 GB of RAM.

### Operating systems

We have major experience running production systems on 64-bit x86 machines working under Ubuntu Linux.

For development purposes we test that YDB could be built and run under latest versions of MacOS and Microsoft Windows on a regular basis.

## Getting started

1. Install YDB using [pre-built executables](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local), [build it from source](BUILD.md) or [use Docker container](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_docker).
1. Install [command line interace](https://ydb.tech/en/docs/getting_started/cli) tool to work with scheme and run queries.
1. Start [local cluster](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local) or container and run [YQL query](https://ydb.tech/en/docs/yql/reference/) via [YDB CLI](https://ydb.tech/en/docs/getting_started/cli).
1. Access [Embedded UI](https://ydb.tech/en/docs/maintenance/embedded_monitoring/) via browser for schema navigation, query execution and other database development related tasks.
1. Run available [example application](https://ydb.tech/en/docs/reference/ydb-sdk/example/go/).
1. Develop an application using [YDB SDK](https://ydb.tech/en/docs/reference/ydb-sdk/).


## How to build
* Build ydb and ydbd binaries [from source](BUILD.md).

## How to deploy

* Deploy a cluster [using Kubernetes](https://ydb.tech/en/docs/deploy/orchestrated/concepts).
* Deploy a cluster using [pre-built executables](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local).

## How to contribute

We are glad to welcome new contributors!

1. Please read [contributor's guide](CONTRIBUTING).
2. We can accept your work to YDB Platform after you have read contributor's license agreement (aka CLA).
3. Please don't forget to add a note to your pull request, that you agree to the terms of the CLA.

More information can be found in [CONTRIBUTING](CONTRIBUTING) file.

## Success stories

See YDB Platform [official web site](https://ydb.tech/) for the latest success stories and projects using YDB Platform.
