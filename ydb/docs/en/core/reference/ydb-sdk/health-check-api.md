---
title: "Instructions for initiating Health Check API in {{ ydb-short-name }}"
description: "The article will tell you how to initiate the check using the Health Check API built-in self-diagnostic system in {{ ydb-short-name }}."
---

# Health Check API

{{ ydb-short-name }} has a built-in self-diagnostic system, which can be used to get a brief report on the database status and information about existing problems.

To initiate the check, call the `SelfCheck` method from `Ydb.Monitoring`. You must also pass the name of the checked DB as usual.

## Response Structure {#response-structure}
Calling the method will return the following structure:

```protobuf
message SelfCheckResult {
    SelfCheck.Result self_check_result = 1;
    repeated IssueLog issue_log = 2;
}
```

The shortest `HealthCheck` response looks like this. It is returned if there is nothing wrong with the database
```protobuf
SelfCheckResult {
    self_check_result: GOOD
}
```

If any issues are detected, the `issue_log` field will contain descriptions of the problems with the following structure:
```protobuf
message IssueLog {
    string id = 1;
    StatusFlag.Status status = 2;
    string message = 3;
    Location location = 4;
    repeated string reason = 5;
    string type = 6;
    uint32 level = 7;
}
```
These issues can be arranged hierarchically with `id` and `reason` fields, which help to visualize how problems in a separate module affect the state of the system as a whole. All issues are arranged in a hierarchy where higher levels can depend on nested levels:

![cards_hierarchy](./_assets/hc_cards_hierarchy.png)

Each issue has a nesting `level` - the higher the `level`, the deeper the ish is in the hierarchy. Issues with the same `type` always have the same `level` and they can be represented as a hierarchy.

![issues_hierarchy](./_assets/hc_types_hierarchy.png)

| Field | Description |
|:----|:----|
| `self_check_result` | enum field which contains the DB check result:<ul><li>`GOOD`: No problems were detected.</li><li>`DEGRADED`: Degradation of one of the database systems was detected, but the database is still functioning (for example, allowable disk loss).</li><li>`MAINTENANCE_REQUIRED`: Significant degradation was detected, there is a risk of availability loss, and human maintenance is required.</li><li>`EMERGENCY`: A serious problem was detected in the database, with complete or partial loss of availability.</li></ul> |
| `issue_log` | This is a set of elements, each of which describes a problem in the system at a certain level. |
| `issue_log.id` | A unique problem ID within this response. |
| `issue_log.status` | Status (severity) of the current problem. <br/>It can take one of the following values:</li><li>`RED`: A component is faulty or unavailable.</li><li>`ORANGE`: A serious problem, we are one step away from losing availability. Maintenance may be required.</li><li>`YELLOW`: A minor problem, no risks to availability. We recommend you continue monitoring the problem.</li><li>`BLUE`: Temporary minor degradation that does not affect database availability. The system is expected to switch to `GREEN`.</li><li>`GREEN`: No problems were detected.</li><li>`GREY`: Failed to determine the status (a problem with the self-diagnostic mechanism).</li></ul> |
| `issue_log.message` | Text that describes the problem. |
| `issue_log.location` | Location of the problem. |
| `issue_log.reason` | This is a set of elements, each of which describes a problem in the system at a certain level. |
| `issue_log.type` | Problem category (by subsystem). |
| `issue_log.level` | Depth of the problem nesting. |
| `database_status` | If settings contains `verbose` parameter than `database_status` field will be filled. <br/>It provides a summary of the overall health of the database. <br/>It's used to quickly review the overall health of the database, helping to assess its health and whether there are any serious problems at a high level. |
| `location` | Contains information about host, where `HealthCheck` service was called |


## Call parameters {#call-parameters}
The whole list of extra parameters presented below:
```c++
struct TSelfCheckSettings : public TOperationRequestSettings<TSelfCheckSettings>{
    FLUENT_SETTING_OPTIONAL(bool, ReturnVerboseStatus);
    FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
    FLUENT_SETTING_OPTIONAL(ui32, MaximumLevel);
};
```

| Parameter | Description |
|:----|:----|
| `ReturnVerboseStatus` | as was said earlier this parameter affects the filling of `database_status` field. |
| `MinimumStatus` | the minimum status that will be given in the response. Issues with a better status will be discarded. |
| `MaximumLevel` | maximum depth of issues in response. Deeper levels will be discarded |

## Possible problems {#problems}

| Message | Description |
|:----|:----|
| **DATABASE** ||
| `Database has multiple issues`</br>`Database has compute issues`</br>`Database has storage issues` | These issues depend solely on the underlying `COMPUTE` and `STORAGE` layers. This is the most general status of the database. |
| **STORAGE** ||
| `There are no storage pools` | Unable to determine `STORAGE_POOLS` issues below. |
| `Storage degraded`</br>`Storage has no redundancy`</br>`Storage failed` | These issues depend solely on the underlying `STORAGE_POOLS` layer. |
| `System tablet BSC didn't provide information` | Storage diagnostics will be generated with alternative way. |
| `Storage usage over 75%/85%/90%` | Need to increase disk space. |
| **STORAGE_POOL** ||
| `Pool degraded/has no redundancy/failed` | These issues depend solely on the underlying `STORAGE_GROUP` layer. |
| **STORAGE_GROUP** ||
| `Group has no vslots` ||
| `Group degraded` | The number of disks allowed in the group is not available. |
| `Group has no redundancy` | A storage group lost its redundancy. |
| `Group failed` | A storage group lost its integrity. |
||`HealthCheck` checks various parameters (fault tolerance mode, number of failed disks, disk status, etc.) and, depending on this, sets the appropriate status and displays a message. |
| **VDISK** ||
| `System tablet BSC didn't provide known status` | This case is not expected, it inner problem. |
| `VDisk is not available` | the disk is not operational at all. |
| `VDisk is being initialized` | initialization in process. |
| `Replication in progress` | the disk accepts queries, but not all the data was replicated. |
| `VDisk have space issue` | These issues depend solely on the underlying `PDISK` layer. |
| **PDISK** ||
| `Unknown PDisk state` | `HealthCheck` the system can't parse pdisk state. |
| `PDisk is inactive/PDisk state is FAULTY/BROKEN/TO_BE_REMOVED` | Indicates problems with a physical disk. |
| `Available size is less than 12%/9%/6%` | Free space on the physical disk is running out. |
| `PDisk is not available` | A physical disk is not available. |
| **STORAGE_NODE** ||
| `Storage node is not available` | A node with disks is not available. |
| **COMPUTE** ||
| `There are no compute nodes` | The database has no nodes to start the tablets. </br>Unable to determine `COMPUTE_NODE` issues below. |
| `Compute has issues with system tablets` | These issues depend solely on the underlying `SYSTEM_TABLET` layer. |
| `Some nodes are restarting too often` | These issues depend solely on the underlying `NODE_UPTIME` layer. |
| `Compute is overloaded` | These issues depend solely on the underlying `COMPUTE_POOL` layer. |
| `Compute quota usage` | These issues depend solely on the underlying `COMPUTE_QUOTA` layer. |
| `Compute has issues with tablets`| These issues depend solely on the underlying `TABLET` layer. |
| **COMPUTE_QUOTA** ||
| `Paths quota usage is over than 90%/99%/Paths quota exhausted` </br>`Shards quota usage is over than 90%/99%/Shards quota exhausted` |Quotas exhausted|
| **COMPUTE_NODE** | *There is no specific issues on this layer.* |
| **SYSTEM_TABLET** ||
| `System tablet is unresponsive / response time over 1000ms/5000ms`|  The system tablet is not responding or it takes too long to respond. |
| **TABLET** ||
| `Tablets are restarting too often` | Tablets are restarting too often. |
| `Tablets/Followers are dead` | Tablets are not running (probably cannot be started). |
| **LOAD_AVERAGE** ||
| `LoadAverage above 100%` | A physical host is overloaded. </br> The `Healthcheck` tool monitors system load by evaluating the current workload in terms of running and waiting processes (load) and comparing it to the total number of logical cores on the host (cores). For example, if a system has 8 logical cores and the current load value is 16, the load is considered to be 200%. </br> `Healthcheck` only checks if the load exceeds the number of cores (load > cores) and reports based on this condition. This indicates that the system is working at or beyond its capacity, potentially due to a high number of processes waiting for I/O operations. </br></br> Load Information: </br> Source: </br>`/proc/loadavg` </br> Logical Cores Information </br></br>The number of logical cores: </br>Primary Source: </br>`/sys/fs/cgroup/cpu.max` </br></br>Fallback Source: </br>`/sys/fs/cgroup/cpu/cpu.cfs_quota_us` </br> `/sys/fs/cgroup/cpu/cpu.cfs_period_us` </br>The number of cores is calculated by dividing the quota by the period (quota / period)
| **COMPUTE_POOL** ||
| `Pool usage is over than 90/95/99%` | One of the pools' CPUs is overloaded. |
| **NODE_UPTIME** ||
| `Node is restarting too often/The number of node restarts has increased` | The number of node restarts has exceeded the threshold. |
| **NODES_SYNC** ||
| `The nodes have a time difference of ... ms` | Time drift on nodes might lead to potential issues with coordinating distributed transactions. |
