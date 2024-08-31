## How it works?

Fully explaining how YDB works in detail takes quite a while. Below you can review several key highlights and then continue exploring documentation to learn more.

### YDB architecture

![YDB architecture](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/grps.png)

YDB clusters typically run on commodity hardware with shared-nothing architecture. If you look at YDB from a bird's eye view, you'll see a layered architecture. The compute and storage layers are disaggregated, they can either run on separate sets of nodes or be co-located.

One of the key building blocks of YDB's compute layer is called a *tablet*. They are stateful logical components implementing various aspects of YDB.

The next level of detail of overall YDB architecture is explained in the [YDB cluster documentation section](../../../contributor/general-schema.md).

### Hierarchy

![Hierarchy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/organization.png)

From the user's perspective, everything inside YDB is organized in a hierarchical structure using directories. It can have arbitrary depth depending on how you choose to organize your data and projects. Even though YDB does not have a fixed hierarchy depth like in other SQL implementations, it will still feel familiar as this is exactly how any virtual filesystem looks like.

### Table

![Table](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/table.png)

YDB provides users with a well-known abstraction: tables. In YDB tables must contain a primary key, the data is physically sorted by the primary key. Tables are automatically sharded by primary key ranges by size or load. Each primary key range of a table is handled by a specific tablet called *data shard*.

#### Split by load

![Split by load](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/nagruz%201.5.png)

Data shards will automatically split into more ones as the load increases. They automatically merge back to the appropriate number when the peak load goes away.

#### Split by size

![Split by size](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/size%201.5%20(1).png)

Data shards also will automatically split when the data size increases. They automatically merge back if enough data will be deleted.

### Automatic balancing

![Automatic balancing](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/pills%201.5.png)

YDB evenly distributes tablets among available nodes. It moves heavily loaded tablets from overloaded nodes. CPU, Memory, and Network metrics are tracked to facilitate this.

### Distributed Storage internals

![Distributed Storage internals](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/distributed.png)

YDB doesn't rely on any third-party filesystem. It stores data by directly working with disk drives as block devices. All major disk kinds are supported: NVMe, SSD, or HDD. The PDisk component is responsible for working with a specific block device. The abstraction layer above PDisk is called VDisk. There is a special component called DSProxy between a tablet and VDisk. DSProxy analyzes disk availability and characteristics and chooses which disks will handle a request and which won't.

### Distributed Storage proxy (DSProxy)

![DSProxy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/proxy%202.png)

A common fault-tolerant setup of YDB spans 3 datacenters or availability zones (AZ). When YDB writes data to 3 AZ, it doesn’t send requests to obviously bad disks and continues to operate without interruption even if one AZ and a disk in another AZ are lost.