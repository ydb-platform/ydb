## Working with Raw Disk Drives in Kubernetes — YDB's Experience | 在Kubernetes中使用原始磁盘驱动器——YDB的经验

{% include notitle [internals_tag](../../tags.md#database_internals) %}

YDB is an open-source distributed database management system that, for performance reasons, uses raw disk drives (block devices) to store all data without any filesystem. It was relatively straightforward to manage such a setup in the bare-metal world of the past, but the dynamic nature of cloud-native environments introduced new challenges to keep this performance benefit. In this talk at [KubeCon + CloudNativeCon + Open Source Summit Hong Kong](https://kccncossaidevchn2024.sched.com/event/1eYZz), [{{ team.blinkov.name }}]({{ team.blinkov.profile }}) ({{ team.blinkov.position }}) explores how to leverage Kubernetes and the Operator design pattern to modernize how stateful distributed database clusters are managed without changing the primary approach to how the data is physically stored.

@[YouTube](https://youtu.be/hXi7k2kGc38?si=K0yQ-CVJklXJe7Hq)

YDB是一个开源的分布式数据库管理系统，为了性能考虑，使用原始磁盘驱动器（块设备）存储所有数据，而不使用任何文件系统。在过去的裸金属世界中管理这样的设置相对比较简单，但云原生环境的动态特性引入了新的挑战，以保持这种性能优势。在这次演讲中，我们将探讨如何利用Kubernetes和运算符设计模式来现代化管理有状态的分布式数据库集群，而不改变数据物理存储的主要方法。

[Slides](https://presentations.ydb.tech/2024/en/kubecon_hongkong/presentation.pdf)
