# Health Check API

{{ ydb-short-name }} has a built-in self-diagnosis system that allows you to get a brief report on the database status and information about existing issues.

To initiate a check, call the `SelfCheck` method from the `NYdb::NMonitoring` namespace in the SDK. You also need to pass the name of the database being checked in the standard way.

{% list tabs group=lang %}

- C++

  Example of application code for creating a client:


  ```cpp
  auto client = NYdb::NMonitoring::TMonitoringClient(driver);
  ```


  Calling the `SelfCheck` method:


  ```cpp
  auto settings =  NYdb::NMonitoring::TSelfCheckSettings();
  settings.ReturnVerboseStatus(true);
  auto result = client.SelfCheck(settings).GetValueSync();
  ```

- Go

  This functionality is not currently supported
- Java

  This functionality is not currently supported
- Python

  This functionality is not currently supported
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  This functionality is not currently supported

  You can create a client for monitoring and call the check methods yourself:


  ```javascript
  const monitoring = driver.createClient(MonitoringServiceDefinition);
  await monitoring.selfCheck();
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#494](https://github.com/ydb-platform/ydb-rs-sdk/issues/494)
- PHP

  This functionality is not currently supported

{% endlist %}

## Call parameters {#call-parameters}

`SelfCheck` returns information in the form of a [list of issues](#example-emergency), each of which may look like this:


```json
{
  "id": "RED-27c3-70fb",
  "status": "RED",
  "message": "Database has multiple issues",
  "location": {
    "database": {
      "name": "/slice"
    }
  },
  "reason": [
    "RED-27c3-4e47",
    "RED-27c3-53b5",
    "YELLOW-27c3-5321"
  ],
  "type": "DATABASE",
  "level": 1
}
```


This is a short message about one of the issues. All call parameters can affect the amount of information contained in the response.

The full list of additional parameters is presented below:

{% list tabs group=lang %}

- C++


  ```cpp
  struct TSelfCheckSettings : public TOperationRequestSettings<TSelfCheckSettings>{
      FLUENT_SETTING_OPTIONAL(bool, ReturnVerboseStatus);
      FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
      FLUENT_SETTING_OPTIONAL(ui32, MaximumLevel);
  };
  ```

- Go

  This functionality is not currently supported
- Java

  This functionality is not currently supported
- Python

  This functionality is not currently supported
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  This functionality is not currently supported
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#494](https://github.com/ydb-platform/ydb-rs-sdk/issues/494)
- PHP

  This functionality is not currently supported

{% endlist %}

| Field | Type | Description |
| :--- | :--- | :--- |
| `ReturnVerboseStatus` | `bool` | If set, the response will also contain a summary of the overall database status in the `database_status` field ( [Example](#example-verbose)). Default is `false`. |
| `MinimumStatus` | [EStatusFlag](#issue-status) | Each issue contains the `status` field. If `minimum_status` is defined, issues with a less severe status will be discarded. By default, all issues will be listed. |
| `MaximumLevel` | `int32` | Each issue contains the `level` field. If `maximum_level` is defined, more severe issues will be discarded. By default, all issues will be listed. |

## Response structure {#response-structure}

The full response structure can be viewed in the [ydb_monitoring.proto](https://github.com/ydb-platform/ydb/public/api/protos/ydb_monitoring.proto) file in the {{ ydb-short-name }} Git repository.
As a result of calling this method, the following structure will be returned:


```protobuf
message SelfCheckResult {
    SelfCheck.Result self_check_result = 1;
    repeated IssueLog issue_log = 2;
    repeated DatabaseStatus database_status = 3;
    LocationNode location = 4;
}
```


If issues are detected, the `issue_log` field will contain descriptions of these issues with the following structure:


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


### Description of fields in the response {#fields-Description}

| Field | Description |
| :--- | :--- |
| `self_check_result` | An enumerated field containing the [database check result](#selfcheck-result). |
| `issue_log` | A set of items, each describing an issue in the system at a certain level. |
| `issue_log.id` | Unique identifier of the issue in this response. |
| `issue_log.status` | An enumerated field containing the [issue status](#issue-status). |
| `issue_log.message` | Text describing the issue. |
| `issue_log.location` | Location of the issue. This can be a physical location or execution context. |
| `issue_log.reason` | A set of items, each describing the cause of the issue in the system at a certain level. |
| `issue_log.type` | Issue category. Each type is at a certain level and is related to others through a [strict hierarchy](#issues-hierarchy) (as shown in the image above). |
| `issue_log.level` | [Nesting depth](#issues-hierarchy) of the issue. |
| `database_status` | If the `verbose` parameter is set in the settings, the `database_status` field will be populated. It provides a summary of the overall database status and is used for quick assessment of the database state and identification of serious issues at a high level. [Example](#example-verbose). The full response structure can be viewed in the [ydb_monitoring.proto](https://github.com/ydb-platform/ydb/public/api/protos/ydb_monitoring.proto) file in the {{ ydb-short-name }} Git repository. |
| `location` | Contains information about the host on which the `HealthCheck` service was called. |

### Issue hierarchy {#issues-hierarchy}

These issues can be organized into a hierarchy using the `id` and `reason` fields, which helps visualize how issues in an individual module affect the overall system state. All issues are organized into a hierarchy where upper levels may depend on nested ones:

! [cards_hierarchy](%E2%9F%A6S1%E2%9F%A7)

Each issue has a nesting level `level` — the higher `level`, the deeper the issue is in the hierarchy. Issues of the same type (`type` field) always have the same `level`, and they can be represented as a hierarchy.

! [issues_hierarchy](%E2%9F%A6S1%E2%9F%A7)

### Database check result {#selfcheck-result}

The most general database status can take the following values:

| Field | Description |
| :--- | :--- |
| `GOOD` | No issues detected. |
| `DEGRADED` | Degradation of one of the database systems has been detected, but the database is still functioning (e.g., acceptable disk loss). |
| `MAINTENANCE_REQUIRED` | Significant degradation has been detected, there is a risk of availability loss, maintenance is required. |
| `EMERGENCY` | A serious problem has been detected in the database with full or partial loss of availability. |

#### Problem status {#issue-status}

Status (severity) of the current problem:

| Field | Description |
| :--- | :--- |
| `GREY` | Degradation of one of the database systems has been detected, but the database is still functioning (e.g., acceptable disk loss). |
| `GREEN` | No problems detected. |
| `BLUE` | Temporary minor degradation that does not affect database availability. The system is expected to transition to `GREEN`. |
| `YELLOW` | Minor problem, no risks to availability. It is recommended to continue monitoring the problem. |
| `ORANGE` | Serious problem, we are one step away from losing availability. Maintenance may be required. |
| `RED` | Component is faulty or unavailable. |

## Possible problems {#issues}

### DATABASE

#### Database has multiple issues, Database has compute issues, Database has storage issues

**Description:** Depends on the underlying layers `COMPUTE` and `STORAGE`. This is the most general database status.

### STORAGE

#### There are no storage pools

**Description:** No information about storage pools. Most likely, storage pools are not configured.

#### Storage degraded, Storage has no redundancy, Storage failed

**Description:** Depends on the underlying layer `STORAGE_POOLS`.

#### System tablet BSC didn't provide information

**Description:** Information about the distributed storage is unavailable.

#### Storage usage over 75%, Storage usage over 85%, Storage usage over 90%

**Description:** Disk space needs to be increased.

### STORAGE_POOL

#### Pool degraded, Pool has no redundancy, Pool failed

**Description:** Depends on the underlying layer `STORAGE_GROUP`.

### STORAGE_GROUP

#### Group has no vslots

**Description:** This error is not expected. Internal error.

#### Group layout is incorrect

**Description:** The storage group was configured incorrectly.

**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, and check the configuration of nodes and disks by the known `id` of the group.

#### Group degraded

**Description:** The group has an acceptable number of unavailable disks.
**Logic:** `HealthCheck` checks various parameters (fault tolerance mode, number of failed disks, disk status, etc.) and sets the corresponding group status accordingly.
**Actions on trigger:** In the [YDB Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`, and check the availability of nodes and disks on nodes by the known `id` of the group.

#### Group has no redundancy

**Description:** The storage group has lost redundancy. Another disk failure could result in group loss.
**Logic:** `HealthCheck` checks various parameters (fault tolerance mode, number of failed disks, disk status, etc.) and sets the corresponding group status accordingly.
**Actions on trigger:** In the [YDB Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`, and check the availability of nodes and disks on nodes by the known `id` of the group.

#### Group failed

**Description:** The storage group has lost integrity and is inoperable. Data is unavailable.
**Logic:** `HealthCheck` checks various parameters (fault tolerance mode, number of failed disks, disk status, etc.) and sets the corresponding group status accordingly.
**Actions on trigger:** In the [YDB Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`, and check the availability of nodes and disks on nodes by the known `id` of the group.

### VDISK

#### System tablet BSC didn't provide known status

**Description:** This error is not expected. Internal error.

#### VDisk is not available

**Description:** Virtual disk is missing.
**Actions on trigger:** In the [YDB Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`. Using the related issue `STORAGE_GROUP`, you can find out `id` of the group. Hover over the required vdisk — it will show which node has the problem. Check the availability of nodes and disks on the nodes.

#### VDisk is being initialized

**Description:** Virtual disk initialization is in progress.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`. Using the related issue `STORAGE_GROUP`, you can find out `id` of the group. Hover over the required vdisk — it will show which node has the problem. Check the availability of nodes and disks on the nodes.

#### Replication in progress

**Description:** The disk is being replicated but can accept requests.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Groups` and `Degraded`. Using the related issue `STORAGE_GROUP`, you can find out `id` of the group. Hover over the required vdisk — it will show which node has the problem. Check the availability of nodes and disks on the nodes.

#### VDisk have space issue

**Description:** Depends on the underlying layer `PDISK`.

### PDISK

#### Unknown PDisk state

**Description:** `HealthCheck` cannot determine the PDisk state. Internal error.

#### PDisk state is

**Description:** Reports the physical disk state.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Nodes` and `Degraded`. Using the known `id` node and pDisk, check the availability of nodes and disks on the nodes.

#### Available size is less than 12%, Available size is less than 9%, Available size is less than 6%

**Description:** Free space on the physical disk is running out.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Nodes` and `Out of Space`, using the known `id` node and PDisk, view the available space.

#### PDisk is not available

**Description:** Physical disk is missing.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the database page, select the `Storage` tab, set filters `Nodes` and `Degraded`, using the known `id` node and PDisk, check the availability of nodes and disks on the nodes.

### STORAGE_NODE

#### Storage node is not available

**Description:** Storage node is missing. This information helps in diagnosing the upper layer `PDISK`.

### COMPUTE

#### There are no compute nodes

**Description:** There are no nodes in the database to run tablets. It is impossible to determine the level `COMPUTE_NODE` below.

#### Compute has issues with system tablets

**Description:** Depends on the underlying layer `SYSTEM_TABLET`.

#### Some nodes are restarting too often

**Description:** Depends on the underlying layer `NODE_UPTIME`.

#### Compute is overloaded

**Description:** Depends on the underlying layer `COMPUTE_POOL`.

#### Compute quota usage

**Description:** Depends on the underlying layer `COMPUTE_QUOTA`.

#### Compute has issues with tablets

**Description:** Depends on the underlying layer `TABLET`.

### COMPUTE_QUOTA

#### Paths quota usage is over than 90%, Paths quota usage is over than 99%, Paths quota exhausted, Shards quota usage is over than 90%, Shards quota usage is over than 99%, Shards quota exhausted

**Description:** Quotas are exhausted.
**Actions on trigger:** Check the number of objects (tables, topics) in the database, delete unnecessary ones.

### SYSTEM_TABLET

#### System tablet is unresponsive, System tablet response time over 1000ms, System tablet response time over 5000ms

**Description:** System tablet is unresponsive or responds with a delay.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), on the `Storage` tab, set filter `Nodes`. Check `Uptime` nodes and their status. If `Uptime` is small, check the logs for reasons of node restarts.

### TABLET

#### Tablets are restarting too often

**Description:** Tablets are restarting too often.
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the `Nodes` tab. Check `Uptime` nodes and their status. If `Uptime` is small, check the logs to determine the reasons for frequent node restarts.

#### Tablets are dead, Followers are dead

**Description:** Tablets are not running (or cannot be started).
**Actions on trigger:** In the [Embedded UI](../embedded-ui/ydb-monitoring.md), go to the `Nodes` tab. Check `Uptime` nodes and their status. If `Uptime` is small, check the logs to determine the reasons for node restarts.

### LOAD_AVERAGE

#### LoadAverage above 100%

**Description:** The physical host is overloaded ( [Load](https://en.wikipedia.org/wiki/Load_(computing))). This indicates that the system is operating at its limit, most likely due to a large number of processes waiting for I/O operations.

**Logic:**
Load information:

- Source: `/proc/loadavg`
- Logical core information:

  - Primary source: `/sys/fs/cgroup/cpu.max`
  - Secondary source: `/sys/fs/cgroup/cpu/cpu.cfs_quota_us`, `/sys/fs/cgroup/cpu/cpu.cfs_period_us`
- The number of cores is calculated by dividing the quota by the period (`quota / period`).

**Actions on trigger:** Check node CPU load.

### COMPUTE_POOL

#### Pool usage is over than 90%, Pool usage is over than 95%, Pool usage is over than 99%

**Description:** One of the CPU pools is overloaded.
**Actions on trigger:** Add cores to the actor system configuration of the corresponding CPU pool.

### NODE_UPTIME

#### The number of node restarts has increased

**Description:** The number of node restarts has exceeded the threshold. By default, this is 10 restarts per hour.
**Actions on trigger:** Check the logs for the reasons for the process restart.

#### Node is restarting too often

**Description:** Nodes are restarting too often. By default, this is 30 restarts per hour.
**Actions on trigger:** Check the logs for the reasons for the process restart.

### NODES_TIME_DIFFERENCE

#### Node is ... ms behind peer [id], Node is ... ms ahead of peer [id]

**Description:** Time difference on nodes, which can lead to problems with coordination of distributed transactions. The problem starts to manifest at a difference of 5 ms.
**Actions on trigger:** Check the system time difference between the nodes listed in the alert and verify the time synchronization process.

## Response examples {#examples}

The shortest service response looks as follows. It is returned if the database is healthy:


```json
{
  "self_check_result": "GOOD"
}
```


### Verbose example {#example-verbose}

Response `GOOD` when using the `verbose` parameter:


```json
{
    "self_check_result": "GOOD",
    "database_status": [
        {
            "name": "/amy/db",
            "overall": "GREEN",
            "storage": {
                "overall": "GREEN",
                "pools": [
                    {
                        "id": "/amy/db:ssdencrypted",
                        "overall": "GREEN",
                        "groups": [
                            {
                                "id": "2181038132",
                                "overall": "GREEN",
                                "vdisks": [
                                    {
                                        "id": "9-1-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "9-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "11-1004-1009",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "11-1004",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "10-1003-1011",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "10-1003",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "8-1005-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "8-1005",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "7-1-1008",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "7-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "6-1-1007",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "6-1",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "4-1005-1010",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "4-1005",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "2-1003-1013",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "2-1003",
                                            "overall": "GREEN"
                                        }
                                    },
                                    {
                                        "id": "1-1-1008",
                                        "overall": "GREEN",
                                        "pdisk": {
                                            "id": "1-1",
                                            "overall": "GREEN"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            "compute": {
                "overall": "GREEN",
                "nodes": [
                    {
                        "id": "50073",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000405479
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00265229
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000347933
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.000312022
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.000945925
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.2,
                            "cores": 4
                        }
                    },
                    {
                        "id": "50074",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000619053
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00463859
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000596071
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.0006241
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.00218465
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.08,
                            "cores": 4
                        }
                    },
                    {
                        "id": "50075",
                        "overall": "GREEN",
                        "pools": [
                            {
                                "overall": "GREEN",
                                "name": "System",
                                "usage": 0.000579126
                            },
                            {
                                "overall": "GREEN",
                                "name": "User",
                                "usage": 0.00344293
                            },
                            {
                                "overall": "GREEN",
                                "name": "Batch",
                                "usage": 0.000592347
                            },
                            {
                                "overall": "GREEN",
                                "name": "IO",
                                "usage": 0.000525747
                            },
                            {
                                "overall": "GREEN",
                                "name": "IC",
                                "usage": 0.00174265
                            }
                        ],
                        "load": {
                            "overall": "GREEN",
                            "load": 0.26,
                            "cores": 4
                        }
                    }
                ],
                "tablets": [
                    {
                        "overall": "GREEN",
                        "type": "SchemeShard",
                        "state": "GOOD",
                        "count": 1
                    },
                    {
                        "overall": "GREEN",
                        "type": "SysViewProcessor",
                        "state": "GOOD",
                        "count": 1
                    },
                    {
                        "overall": "GREEN",
                        "type": "Coordinator",
                        "state": "GOOD",
                        "count": 3
                    },
                    {
                        "overall": "GREEN",
                        "type": "Mediator",
                        "state": "GOOD",
                        "count": 3
                    },
                    {
                        "overall": "GREEN",
                        "type": "Hive",
                        "state": "GOOD",
                        "count": 1
                    }
                ]
            }
        }
    ]
}
```


### EMERGENCY example {#example-emergency}

In case of problems, the response may look like this:


```json
{
  "self_check_result": "EMERGENCY",
  "issue_log": [
    {
      "id": "RED-27c3-70fb",
      "status": "RED",
      "message": "Database has multiple issues",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-4e47",
        "RED-27c3-53b5",
        "YELLOW-27c3-5321"
      ],
      "type": "DATABASE",
      "level": 1
    },
    {
      "id": "RED-27c3-4e47",
      "status": "RED",
      "message": "Compute has issues with system tablets",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-c138-BSController"
      ],
      "type": "COMPUTE",
      "level": 2
    },
    {
      "id": "RED-27c3-c138-BSController",
      "status": "RED",
      "message": "System tablet is unresponsive",
      "location": {
        "compute": {
          "tablet": {
            "type": "BSController",
            "id": [
              "72057594037989391"
            ]
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "type": "SYSTEM_TABLET",
      "level": 3
    },
    {
      "id": "RED-27c3-53b5",
      "status": "RED",
      "message": "System tablet BSC didn't provide information",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "type": "STORAGE",
      "level": 2
    },
    {
      "id": "YELLOW-27c3-5321",
      "status": "YELLOW",
      "message": "Storage degraded",
      "location": {
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "YELLOW-27c3-595f-8d1d"
      ],
      "type": "STORAGE",
      "level": 2
    },
    {
      "id": "YELLOW-27c3-595f-8d1d",
      "status": "YELLOW",
      "message": "Pool degraded",
      "location": {
        "storage": {
          "pool": {
            "name": "static"
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "YELLOW-27c3-ef3e-0"
      ],
      "type": "STORAGE_POOL",
      "level": 3
    },
    {
      "id": "RED-84d8-3-3-1",
      "status": "RED",
      "message": "PDisk is not available",
      "location": {
        "storage": {
          "node": {
            "id": 3,
            "host": "man0-0026.ydb-dev.nemax.nebiuscloud.net",
            "port": 19001
          },
          "pool": {
            "group": {
              "vdisk": {
                "pdisk": [
                  {
                    "id": "3-1",
                    "path": "/dev/disk/by-partlabel/NVMEKIKIMR01"
                  }
                ]
              }
            }
          }
        }
      },
      "type": "PDISK",
      "level": 6
    },
    {
      "id": "RED-27c3-4847-3-0-1-0-2-0",
      "status": "RED",
      "message": "VDisk is not available",
      "location": {
        "storage": {
          "node": {
            "id": 3,
            "host": "man0-0026.ydb-dev.nemax.nebiuscloud.net",
            "port": 19001
          },
          "pool": {
            "name": "static",
            "group": {
              "vdisk": {
                "id": [
                  "0-1-0-2-0"
                ]
              }
            }
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-84d8-3-3-1"
      ],
      "type": "VDISK",
      "level": 5
    },
    {
      "id": "YELLOW-27c3-ef3e-0",
      "status": "YELLOW",
      "message": "Group degraded",
      "location": {
        "storage": {
          "pool": {
            "name": "static",
            "group": {
              "id": [
                "0"
              ]
            }
          }
        },
        "database": {
          "name": "/slice"
        }
      },
      "reason": [
        "RED-27c3-4847-3-0-1-0-2-0"
      ],
      "type": "STORAGE_GROUP",
      "level": 4
    }
  ],
  "location": {
    "id": 5,
    "host": "man0-0028.ydb-dev.nemax.nebiuscloud.net",
    "port": 19001
  }
}
```
