---
title: "Instructions for initiating Health Check API in {{ ydb-short-name }}"
description: "The article will tell you how to initiate the check using the Health Check API built-in self-diagnostic system in {{ ydb-short-name }}."
---

# Health Check API

{{ ydb-short-name }} has a built-in self-diagnostic system, which can be used to get a brief report on the database status and information about existing problems.

To initiate the check, call the `SelfCheck` method from `Ydb.Monitoring`. You must also pass the name of the checked DB as usual.

Calling the method will return the following structure:

```protobuf
message SelfCheckResult {
    SelfCheck.Result self_check_result = 1;
    repeated IssueLog issue_log = 2;
}
```

The `self_check_result` field of the `enum` type contains the DB check result:

* `GOOD`: No problems were detected.
* `DEGRADED`: Degradation of one of the database systems was detected, but the database is still functioning (for example, allowable disk loss).
* `MAINTENANCE_REQUIRED`: Significant degradation was detected, there is a risk of accessibility loss, and human intervention is required.
* `EMERGENCY`: A serious problem was detected in the database, with complete or partial loss of accessibility.

If problems are detected, the `issue_log` field will contain problem descriptions with the following structure:

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

* `id`: A unique problem ID within this response.
* `status`: Status (severity) of the current problem. It can take one of the following values:
   * `RED`: A component is faulty or unavailable.
   * `ORANGE`: A serious problem, we are one step away from losing accessibility. Intervention may be required.
   * `YELLOW`: A minor problem, no risks to accessibility. We recommend you continue monitoring the problem.
   * `BLUE`: Temporary minor degradation that does not affect database accessibility.
   * `GREEN`: No problems were detected.
   * `GREY`: Failed to determine the status (a problem with the self-diagnostic mechanism).
* `message`: [Text that describes the problem](#problems).
* `location`: Location of the problem.
* `reason`: Possible IDs of the nested problems that led to the current problem.
* `type`: Problem category (by subsystem).
* `level`: Depth of the problem nesting.

## Possible problems {#problems}

* `Pool usage over 90/95/99%`: One of the pools' CPUs is overloaded.
* `System tablet is unresponsive / response time over 1000ms/5000ms`: The system tablet is not responding or it takes too long to respond.
* `Tablets are restarting too often`: Tablets are restarting too often.
* `Tablets/Followers are dead`: Tablets are not running (probably cannot be started).
* `Node is restarting too often`.
* `The number of node restarts has increased`: The number of node restarts has exceeded the threshold.
* `LoadAverage above 100%`: A physical host is overloaded.
* `There are no compute nodes`: The database has no nodes to start the tablets.
* `PDisk state is ...`: Indicates problems with a physical disk.
* `PDisk is not available`: A physical disk is not available.
* `Available size is less than 12%/9%/6%`: Free space on the physical disk is running out.
* `VDisk is not available`: A virtual disk is not available.
* `VDisk state is ...`: Indicates problems with a virtual disk.
* `DiskSpace is ...`: Indicates problems with virtual disk space.
* `Storage node is not available`: A node with disks is not available.
* `Replication in progress`: Disk replication is in progress.
* `Group has no redundancy`: A storage group lost its redundancy.
* `Group failed`: A storage group lost its integrity.
* `Group degraded`: The number of disks allowed in the group is not available.
* `The nodes have a time difference of ... ms`: Time drift on nodes might lead to potential issues with coordinating distributed transactions.
