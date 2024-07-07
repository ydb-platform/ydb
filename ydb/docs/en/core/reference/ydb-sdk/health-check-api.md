---
title: "Instructions for initiating Health Check API in {{ ydb-short-name }}"
description: "The article will tell you how to initiate the check using the Health Check API built-in self-diagnostic system in {{ ydb-short-name }}."
---

# Health Check API

{{ ydb-short-name }} has a built-in self-diagnostic system, which can be used to get a brief report on the database status and information about existing issues.

To initiate the check, call the `SelfCheck` method from `NYdb::NMonitoring` namespace in the SDK. You must also pass the name of the checked DB as usual.

{% list tabs %}

- C++

  App code snippet for creating a client:
  ```cpp
  auto client = NYdb::NMonitoring::TMonitoringClient(driver);
  ```

  Calling `SelfCheck` method:

  ```
  auto settings = TSelfCheckSettings();
  settings.ReturnVerboseStatus(true);
  auto result = client.SelfCheck(settings).GetValueSync();
  ```

{% endlist %}

## Call parameters {#call-parameters}

`SelfCheck` method provides information in the form of a [set of issues](#emergency-example) which could look like this:

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

This is a short messages each about a single problem. All parameters will affect the amount of information the service returns for the specified database.

The whole list of extra parameters presented below:

{% list tabs %}

- C++

  ```c++
  struct TSelfCheckSettings : public TOperationRequestSettings<TSelfCheckSettings>{
      FLUENT_SETTING_OPTIONAL(bool, ReturnVerboseStatus);
      FLUENT_SETTING_OPTIONAL(EStatusFlag, MinimumStatus);
      FLUENT_SETTING_OPTIONAL(ui32, MaximumLevel);
  };
  ```

{% endlist %}

| Parameter | Type | Description |
|:----|:----|:----|
| `ReturnVerboseStatus` | `bool`         | If `ReturnVerboseStatus` is specified, the response will also include a summary of the overall health of the database in the `database_status` field ([Example](#example-verbose)). Default is false. |
| `MinimumStatus`       | [EStatusFlag] (#issue-status) | Each issue has a `status` field. If `minimum_status` is specified, issues with a higher `status` will be discarded. By default, all issues will be listed. |
| `MaximumLevel`        | `int32`        | Each issue has a `level` field. If `maximum_level` is specified, issues with deeper levels will be discarded. By default, all issues will be listed. |

## Response structure {#response-structure}

For the full response structure, see the [ydb_monitoring.proto](https://github.com/ydb-platform/ydb/public/api/protos/ydb_monitoring.proto) file in the {{ ydb-short-name }} Git repository.
Calling the `SelfCheck` method will return the following message:

```protobuf
message SelfCheckResult {
    SelfCheck.Result self_check_result = 1;
    repeated IssueLog issue_log = 2;
    repeated DatabaseStatus database_status = 3;
    LocationNode location = 4;
}
```

The shortest HealthCheck response looks like [this](#examples) . It is returned if there is nothing wrong with the database.

If any issues are detected, the `issue_log` field will contain descriptions of the issues with the following structure:

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

These issues can be arranged hierarchically with `id` and `reason` fields, which help to visualize how issues in a separate module affect the state of the system as a whole. All issues are arranged in a hierarchy where higher levels can depend on nested levels:

![cards_hierarchy](./_assets/hc_cards_hierarchy.png)

Each issue has a nesting `level` - the higher the `level`, the deeper the ish is in the hierarchy. Issues with the same `type` always have the same `level` and they can be represented as a hierarchy.

![issues_hierarchy](./_assets/hc_types_hierarchy.png)

Description of all fields in the response is provided below:

| Field | Description |
|:----|:----|
| `self_check_result` | enum field which contains the [DB check result](#selfcheck-result) |
| `issue_log.id` | A unique issue ID within this response. |
| `issue_log.status` |  enum field which contains the [issue status](#issue-status) |
| `issue_log.message` | Text that describes the issue. |
| `issue_log.location` | Location of the issue. This can be a physical location or an execution context. |
| `issue_log.reason` | This is a set of elements, each of which describes a issue in the system at a certain level. |
| `issue_log.type` | Issue category (by subsystem). Each type is at a certain level and interconnected with others through a rigid hierarchy (as shown in the picture above). |
| `issue_log.level` | The depth of issue nesting. |
| `database_status` | If settings contains `ReturnVerboseStatus` parameter than `database_status` field will be filled. <br/>It provides a summary of the overall health of the database. <br/>It's used to quickly review the overall health of the database, helping to assess its health and whether there are any serious issues at a high level. [Example](#example-verbose). |
| `location` | Contains information about host, where `HealthCheck` service was called |

### DB check result {#selfcheck-result}

The most general statuses of the database, which can have the following values:

| Value | Description |
|:----|:----|
| `GOOD` | No issues were detected. |
| `DEGRADED` | Degradation of one of the database systems was detected, but the database is still functioning (for example, allowable disk loss). |
| `MAINTENANCE_REQUIRED` | Significant degradation was detected, there is a risk of availability loss, and human maintenance is required. |
| `EMERGENCY` | A serious issue was detected in the database, with complete or partial loss of availability. |

### Issue status {#issue-status}

Status (severity) of the current issue:

| Value | Description |
|:----|:----|
| `GREY` | Failed to determine the status (a issue with the self-diagnostic mechanism). |
| `GREEN` | No issues were detected. |
| `BLUE` | Temporary minor degradation that does not affect database availability. The system is expected to switch to `GREEN`. |
| `YELLOW` | A minor issue, no risks to availability. We recommend you continue monitoring the issue. |
| `ORANGE` | A serious issue, we are one step away from losing availability. Maintenance may be required. |
| `RED` | A component is faulty or unavailable. |

## Possible issues {#issues}

| Message | Description |
|:----|:----|
| **DATABASE** ||
| `Database has multiple issues`</br>`Database has compute issues`</br>`Database has storage issues` | These issues depend solely on the underlying `COMPUTE` and `STORAGE` layers. This is the most general status of a database. |
| **STORAGE** ||
| `There are no storage pools` | Storage pools aren't configured. |
| `Storage degraded`</br>`Storage has no redundancy`</br>`Storage failed` | These issues depend solely on the underlying `STORAGE_POOLS` layer. |
| `System tablet BSC didn't provide information` | Storage diagnostics will be generated alternatively. |
| `Storage usage over 75%` <br>`Storage usage over 85%` <br>`Storage usage over 90%` | Some data needs to be removed, or the database needs to be reconfigured with additional disk space. |
| **STORAGE_POOL** ||
| `Pool degraded` <br>`Pool has no redundancy` <br>`Pool failed` | These issues depend solely on the underlying `STORAGE_GROUP` layer. |
| **STORAGE_GROUP** ||
| `Group has no vslots` | This case is not expected; it is an internal issue. |
| `Group degraded` | A number of disks allowed in the group are not available. |
| `Group has no redundancy` | A storage group lost its redundancy. Аnother failure of vdisk may lead to the loss of the group. |
| `Group failed` | A storage group lost its integrity. Data is not available |
||`HealthCheck` checks various parameters (fault tolerance mode, number of failed disks, disk status, etc.) and, depending on them, sets the appropriate status and displays a message. |
| **VDISK** ||
| `System tablet BSC didn't provide known status` | This case is not expected; it is an internal issue. |
| `VDisk is not available` | the disk is not operational at all. |
| `VDisk is being initialized` | initialization in process. |
| `Replication in progress` | the disk accepts queries, but not all the data was replicated. |
| `VDisk have space issue` | These issues depend solely on the underlying `PDISK` layer. |
| **PDISK** ||
| `Unknown PDisk state` | `HealthCheck` the system can't parse pdisk state. |
| `PDisk state is ...` | Indicates state of physical disk. |
| `Available size is less than 12%` <br>`Available size is less than 9%` <br>`Available size is less than 6%` | Free space on the physical disk is running out. |
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
| `Paths quota usage is over than 90%` <br>`Paths quota usage is over than 99%` <br>`Paths quota exhausted` </br>`Shards quota usage is over than 90%` </br>`Shards quota usage is over than 99%` </br>`Shards quota exhausted` | Quotas exhausted |
| **SYSTEM_TABLET** ||
| `System tablet is unresponsive ` <br>`System tablet response time over 1000ms` <br>`System tablet response time over 5000ms`|  The system tablet is not responding or it takes too long to respond. |
| **TABLET** ||
| `Tablets are restarting too often` | Tablets are restarting too often. |
| `Tablets/Followers are dead` | Tablets are not running (probably cannot be started). |
| **LOAD_AVERAGE** ||
| `LoadAverage above 100%` | ([Load](https://en.wikipedia.org/wiki/Load_(computing))) A physical host is overloaded . </br> This indicates that the system is working at or beyond its capacity, potentially due to a high number of processes waiting for I/O operations. </br></br> Load Information: </br> Source: </br>`/proc/loadavg` </br> Logical Cores Information </br></br>The number of logical cores: </br>Primary Source: </br>`/sys/fs/cgroup/cpu.max` </br></br>Fallback Source: </br>`/sys/fs/cgroup/cpu/cpu.cfs_quota_us` </br> `/sys/fs/cgroup/cpu/cpu.cfs_period_us` </br>The number of cores is calculated by dividing the quota by the period (quota / period)
| **COMPUTE_POOL** ||
| `Pool usage is over than 90%` <br>`Pool usage is over than 95%` <br>`Pool usage is over than 99%` | One of the pools' CPUs is overloaded. |
| **NODE_UPTIME** ||
| `The number of node restarts has increased` | The number of node restarts has exceeded the threshold. By default, 10 restarts per hour |
| `Node is restarting too often` | The number of node restarts has exceeded the threshold. By default, 30 restarts per hour |
| **NODES_TIME_DIFFERENCE** ||
| `Node is ... ms behind peer [id]` <br>`Node is ... ms ahead of peer [id]` | Time drift on nodes might lead to potential issues with coordinating distributed transactions. This issus starts to appear from 5 ms |

## Example {#examples}

The shortest `HealthCheck` response looks like this. It is returned if there is nothing wrong with the database
```json
{
  "self_check_result": "GOOD"
}
```

#### Verbose example {#example-verbose}

`GOOD` response with `verbose` parameter:
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

#### Emergency example {#example-emergency}

Response with `EMERGENCY` status:
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
