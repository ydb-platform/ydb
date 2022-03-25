## Regions and availability zones {#regions-az}

_An availability zone_ is a data processing center or an isolated segment thereof with minimal physical distance between nodes and minimal risk of failure at the same time as other availability zones.
_A large geographic region_ is an area within which the distance between availability zones is 500 km or smaller.

A geo-distributed {{ ydb-short-name }} cluster contains nodes located in different availability zones within a wide geographic region. {{ ydb-short-name }} performs synchronous data writes to each of the availability zones, ensuring uninterrupted performance if an availability zone fails.

In geographically distributed clusters, you can choose a policy for distributing computing resources across data centers. This lets you find the right balance between minimum execution time and minimum downtime if a data center goes offline.

