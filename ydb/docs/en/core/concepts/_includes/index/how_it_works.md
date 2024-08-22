## How it works?

Fully explaining how YDB works in detail takes quite a while. Below you can review several key highlights and then continue exploring documentation to learn more.

### {{ ydb-short-name }} architecture {#ydb-architecture}

![YDB architecture](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/grps.png)

{{ ydb-short-name }} clusters typically run on commodity hardware with shared-nothing architecture. If you look at {{ ydb-short-name }} from a bird's eye view, you'll see a layered architecture. The compute and storage layers are disaggregated, they can either run on separate sets of nodes or be co-located.

One of the key building blocks of {{ ydb-short-name }}'s compute layer is called a *tablet*. They are stateful logical components implementing various aspects of {{ ydb-short-name }}.

The next level of detail of overall {{ ydb-short-name }} architecture is explained in the [YDB cluster documentation section](../../cluster/index.md).

### Hierarchy {#ydb-hierarchy}

![Hierarchy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/organization.png)

From the user's perspective, everything inside {{ ydb-short-name }} is organized in a hierarchical structure using directories. It can have arbitrary depth depending on how you choose to organize your data and projects. Even though {{ ydb-short-name }} does not have a fixed hierarchy depth like in other SQL implementations, it will still feel familiar as this is exactly how any virtual filesystem looks like.

### Table {#table}

![Table](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/table.png)

{{ ydb-short-name }} provides users with the well-known abstraction — tables. In {{ ydb-short-name }}, there are two main types of tables:
* [Row tables](../../datamodel/table.md#row-tables) are designed for OLTP workloads.
* [Column tables](../../datamodel/table.md#column-tables) are designed for OLAP workloads.

Logically, from the user's point of view, both types of tables look the same. The main difference between row tables and column tables lies in the way data is stored. In row tables, the values of all columns in each row are stored together, while in column tables — on the contrary, each column is stored separately, so cells related to different rows are stored next to each other.

Regardless of the type of table, it must contain a primary key. In the primary key of column tables, only columns with `NOT NULL` can be used. Data in tables is physically sorted by the primary key. Row tables are automatically partitioned by the ranges of primary keys depending on the volume of data, while data in column tables is partitioned not by the primary key but by the hash of the partitioning columns. Each range of primary keys in a table is processed by a specific [tablet](../../cluster/common_scheme_ydb.md#tablets), called a *data shard* for row tables and a *column shard* for column tables.

#### Split by load {#split-by-load}

![Split by load](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/nagruz%201.5.png)

Data shards will automatically split into more ones as the load increases. They automatically merge back to the appropriate number when the peak load goes away.

#### Split by size {#split-by-size}

![Split by size](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/size%201.5%20(1).png)

Data shards also will automatically split when the data size increases. They automatically merge back if enough data will be deleted.

### Automatic balancing {#automatic-balancing}

![Automatic balancing](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/pills%201.5.png)

{{ ydb-short-name }} evenly distributes tablets among available nodes. It moves heavily loaded tablets from overloaded nodes. CPU, Memory, and Network metrics are tracked to facilitate this.

### Distributed Storage internals {#ds-internals}

![Distributed Storage internals](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/distributed.png)

{{ ydb-short-name }} doesn't rely on any third-party filesystem. It stores data by directly working with disk drives as block devices. All major disk kinds are supported: NVMe, SSD, or HDD. The PDisk component is responsible for working with a specific block device. The abstraction layer above PDisk is called VDisk. There is a special component called DSProxy between a tablet and VDisk. DSProxy analyzes disk availability and characteristics and chooses which disks will handle a request and which won't.

### Distributed Storage proxy (DSProxy) {#ds-proxy}

![DSProxy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/proxy%202.png)

A common fault-tolerant setup of {{ ydb-short-name }} spans 3 datacenters or availability zones (AZ). When {{ ydb-short-name }} writes data to 3 AZ, it doesn’t send requests to obviously bad disks and continues to operate without interruption even if one AZ and a disk in another AZ are lost.