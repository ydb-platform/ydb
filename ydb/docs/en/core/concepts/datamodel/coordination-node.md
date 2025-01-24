# Coordination node

A coordination node is an object in {{ ydb-short-name }} that allows client applications to coordinate their actions in a distributed manner. Typical use cases for coordination nodes include:

* Distributed [semaphores](https://en.wikipedia.org/wiki/Semaphore_(programming)) and [mutexes](https://en.wikipedia.org/wiki/Mutual_exclusion).
* Service discovery.
* Leader election.
* Task queues.
* Publishing small amounts of data with the ability to receive change notifications.
* Ephemeral locking of arbitrary entities not known in advance.

## Semaphores {#semaphore}

Coordination nodes allow you to create and manage semaphores within them. Typical operations with semaphores include:

* Create.
* Acquire.
* Release.
* Describe.
* Subscribe.
* Delete.

A semaphore can have a counter that limits the number of simultaneous acquisitions, as well as a small amount of arbitrary data attached to it.

{{ ydb-short-name }} supports two types of semaphores: persistent and ephemeral. A persistent semaphore must be created before acquisition and will exist either until it is explicitly deleted or until the coordination node in which it was created is deleted. Ephemeral semaphores are automatically created at the moment of their first acquisition and deleted at the last release, which is convenient to use, for example, in distributed locking scenarios.

{% note info %}

Semaphores in {{ ydb-short-name }} are **not** recursive. Thus, semaphore acquisition and release are idempotent operations.

{% endnote %}

## Usage {#usage}

Working with coordination nodes and semaphores is done through [dedicated methods in {{ ydb-short-name }} SDK](../../reference/ydb-sdk/coordination.md).

## Similar systems {#similar-systems}

{{ ydb-short-name }} coordination nodes can solve tasks that are traditionally performed using systems such as [Apache Zookeeper](https://zookeeper.apache.org/), [etcd](https://etcd.io/), [Consul](https://www.consul.io/), and others. If a project uses {{ ydb-short-name }} for data storage along with one of these third-party systems for coordination, switching to {{ ydb-short-name }} coordination nodes can reduce the number of systems that need to be operated and maintained.