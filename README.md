<img width="64" src="ydb/docs/_assets/logo.svg"/><br/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/ydb-platform/ydb.svg?style=flat-square)](https://github.com/ydb-platform/ydb/releases)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ydb_en)

## YDB

[Website](https://ydb.tech) |
[Documentation](https://ydb.tech/docs) |
[Official Repository](https://github.com/ydb-platform/ydb) |
[Blog](https://medium.com/ydbtech) |
[YouTube](https://www.youtube.com/c/YDBPlatform) |
[Twitter](https://twitter.com/YDBPlatform) |
[LinkedIn](https://www.linkedin.com/company/ydb-platform)

YDB is an open-source Distributed SQL Database that combines high availability and scalability with strict consistency and ACID transactions.

[![YDB Product Video](ydb/docs/_assets/ydb-promo-video.png)](https://youtu.be/bxZRUtMAlFI)

## Main YDB Advantages

YDB is designed from scratch as a response to growing demand for scalable interactive web services. Scalability, strict consistency and effective cross-row transactions were a must for such OLTP-like workload. YDB is built by people with strong background in databases and distributed systems, who had an experience of developing No-SQL database and the Map-Reduce system for one of the largest search engines in the world.
We found that YDB's flexible design allows us to build more services on top of it including persistent queues and virtual block devices.

Basic YDB features:

  - Fault-tolerant configuration that survive disk, node, rack or even datacenter outage;
  - Horizontal scalability;
  - Automatic disaster recovery with minimum latency disruptions for applications;
  - SQL dialect (YQL) for data manipulation and scheme definition;
  - ACID transactions across multiple nodes and tables with strict consistency.

### Fault-tolerant Configurations

YDB could be deployed in three availability zones. Cluster remains available for both reads and writes during complete outage of a single zone. Availability zones and regions are covered in more detail [in documentation](https://ydb.tech/en/docs/concepts/databases#regions-az).

### Horizontal Scalability

Unlike traditional relational databases YDB [scales out](https://en.wikipedia.org/wiki/Scalability#Horizontal_or_scale_out) providing developers with capability to simply extend cluster with computation or storage resources to handle increasing load. YDB has desaggregated storage and compute layers which allow you to scale storage and compute resources independently.

Current production installations have more than 10,000 nodes, store petabytes of data and handle millions distributed transactions per second.

### Automatic Disaster Recovery

YDB has built-in automatic recovery support to survive a hardware failure. After unpredictable disk, node, rack or even datacenter failure YDB remains fully available for reads and writes and restores required data redundancy automatically.

### Multitenant and Serverless Database
YDB has support for multitenant and serverless setups. A user can run a YDB cluster and create several databases that share one pool of storage and have different compute nodes. Alternatively a user can run several serverless databases that share one pool of compute resources to utilize them effectively.

## Supported Platforms

### Minimal system requirements

YDB runs on x86 64bit platforms with minimum 8 GB of RAM.

### Operating Systems

We have major experience running production systems on 64-bit x86 machines working under Ubuntu Linux.

For development purposes we test that YDB could be built and run under latest versions of MacOS and Microsoft Windows on a regular basis.

## Getting Started

1. Install YDB using [pre-built executables](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local), [build it from source](BUILD.md) or [use Docker container](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_docker).
1. Install [command line interface](https://ydb.tech/en/docs/getting_started/cli) tool to work with scheme and run queries.
1. Start [local cluster](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local) or container and run [YQL query](https://ydb.tech/en/docs/yql/reference/) via [YDB CLI](https://ydb.tech/en/docs/getting_started/cli).
1. Access [Embedded UI](https://ydb.tech/en/docs/maintenance/embedded_monitoring/) via browser for schema navigation, query execution and other database development related tasks.
1. Run available [example application](https://ydb.tech/en/docs/reference/ydb-sdk/example/go/).
1. Develop an application using [YDB SDK](https://ydb.tech/en/docs/reference/ydb-sdk/).


## How to Build from Source Code
* Build server (ydbd) and client (ydb) binaries [from source code](BUILD.md).

## How to Deploy

* Deploy a cluster [using Kubernetes](https://ydb.tech/en/docs/deploy/orchestrated/concepts).
* Deploy a cluster using [pre-built executables](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_local).

## How to Contribute

We are glad to welcome new contributors!

1. Please read the [contributor's guide](CONTRIBUTING.md).
2. We can accept your work to YDB after you have signed contributor's license agreement (aka CLA).
3. Please don't forget to add a note to your pull request, that you agree to the terms of the CLA.

## Success Stories

Take a look at YDB [web site](https://ydb.tech/) for the latest success stories and user scenarios.
