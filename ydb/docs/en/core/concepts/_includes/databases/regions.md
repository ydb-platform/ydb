## Regions and availability zones {#regions-az}

_An availability zone_ is a data processing center or its isolated segment where the minimum physical distance between nodes is ensured and the risk of simultaneous failures with other availability zones is minimized.
_A wide geographic region_ is a territory where the distance between availability zones does not exceed 500 km.

A geo-distributed {{ ydb-short-name }} cluster contains nodes located in different availability zones within a wide geographic region. {{ ydb-short-name }} performs synchronous data writes to each of the availability zones, ensuring uninterrupted performance if an availability zone fails.

In geographically distributed clusters, you can choose a policy for distributing computing resources across data centers. This lets you find the right balance between minimum execution time and minimum downtime if a data center goes offline.
