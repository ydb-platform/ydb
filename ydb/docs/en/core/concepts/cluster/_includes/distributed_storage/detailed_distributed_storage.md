## BlobStorage implementation details

### Managing BlobStorage resources on nodes via NodeWarden

### How PDisk works

#### Data format

### How VDisk works

#### Large and small blobs

#### Fresh segment

#### SSTable

#### Syncing metadata

#### Replicating data

#### Data format

##### Donor disks

### How DS proxy works

## BS_CONTROLLER

### Communicating with NodeWarden

### Box and Storage Pool

### Self Heal

### Entities and tables

All information stored in BS_CONTROLLER can be grouped into:

1. BS_CONTROLLER settings.
1. Low-level configuration (PDisks, VDisks, and groups).
1. Top-level configuration (Boxes and StoragePools).
1. Runtime information (persistent disk and group metrics, the status of scrubbing of individual VDisks, information about node drive serial numbers).
1. OperationLogs.

The functional logic is built to load all table data (with the exception of OperationLog) into memory when the BS_CONTROLLER tablet starts and to keep them there for the entire time it is up. When executing transactions, the data is updated in tables and in memory.

The tables are discussed in more detail below.

#### BS_CONTROLLER settings

The settings are stored in the State table.

#### Low-level configuration

Low-level configuration includes information about the available cluster resources: PDisks, VDisks, and groups. These tables are interconnected via PK <-> FK links.

#### Top-level configuration

Top-level configuration stores data necessary for linking Boxes and nodes where they are located...

#### Database schema

Box

| Column | ID | PK | Type | Default | Purpose |
| ------------ | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| Name | 2 |  | Utf8 |  |  |
| Generation | 3 |  | Uint64 |  |  |

BoxHost

| Column | ID | PK | Type | Default | Purpose |
| -------------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| FQDN | 3 | * | Utf8 |  |  |
| IcPort | 4 | * | Int32 |  |  |
| HostConfigId | 5 |  | Uint64 |  |  |

BoxHostV2

| Column | ID | PK | Type | Default | Purpose |
| -------------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| FQDN | 2 | * | Utf8 |  |  |
| IcPort | 3 | * | Int32 |  |  |
| HostConfigId | 4 |  | Uint64 |  |  |

BoxStoragePool

| Column | ID | PK | Type | Default | Purpose |
| ---------------------------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| StoragePoolId | 2 | * | Uint64 |  |  |
| Name | 20 |  | Utf8 |  |  |
| ErasureSpecies | 3 |  | Int32 |  |  |
| RealmLevelBegin | 4 |  | Int32 |  |  |
| RealmLevelEnd | 5 |  | Int32 |  |  |
| DomainLevelBegin | 6 |  | Int32 |  |  |
| DomainLevelEnd | 7 |  | Int32 |  |  |
| NumFailRealms | 8 |  | Int32 |  |  |
| NumFailDomainsPerFailRealm | 9 |  | Int32 |  |  |
| NumVDisksPerFailDomain | 10 |  | Int32 |  |  |
| VDiskKind | 11 |  | Int32 |  |  |
| SpaceBytes | 12 |  | Uint64 |  |  |
| WriteIOPS | 13 |  | Uint64 |  |  |
| WriteBytesPerSecond | 14 |  | Uint64 |  |  |
| ReadIOPS | 15 |  | Uint64 |  |  |
| ReadBytesPerSecond | 16 |  | Uint64 |  |  |
| InMemCacheBytes | 17 |  | Uint64 |  |  |
| Kind | 18 |  | Utf8 |  |  |
| NumGroups | 19 |  | Uint32 |  |  |
| Generation | 21 |  | Uint64 |  |  |
| EncryptionMode | 22 |  | Uint32 | 0 |  |
| SchemeshardId | 23 |  | Uint64 |  |  |
| PathItemId | 24 |  | Uint64 |  |  |
| RandomizeGroupMapping | 25 |  | Bool | false |  |

BoxStoragePoolPDiskFilter

| Column | ID | PK | Type | Default | Purpose |
| --------------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| StoragePoolId | 2 | * | Uint64 |  |  |
| Type | 3 | * | Int32 |  |  |
| SharedWithOs | 4 | * | Bool |  |  |
| ReadCentric | 5 | * | Bool |  |  |
| Kind | 6 | * | Uint64 |  |  |

BoxStoragePoolUser

| Column | ID | PK | Type | Default | Purpose |
| --------------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| StoragePoolId | 2 | * | Uint64 |  |  |
| UserId | 3 | * | String |  |  |

BoxUser

| Column | ID | PK | Type | Default | Purpose |
| --------- | ---- | ---- | -------- | -------------- | ------------ |
| BoxId | 1 | * | Uint64 |  |  |
| UserId | 2 | * | String |  |  |

DriveSerial

| Column | ID | PK | Type | Default | Purpose |
| ------------- | ---- | ---- | -------- | -------------- | ------------ |
| Serial | 1 | * | String |  |  |
| BoxId | 2 |  | Uint64 |  |  |
| NodeId | 3 |  | Uint32 |  |  |
| PDiskId | 4 |  | Uint32 |  |  |
| Guid | 5 |  | Uint64 |  |  |
| LifeStage | 6 |  | Uint32 |  |  |
| Kind | 7 |  | Uint64 |  |  |
| PDiskType | 8 |  | Int32 |  |  |
| PDiskConfig | 9 |  | String |  |  |

DriveStatus

| Column | ID | PK | Type | Default | Purpose |
| ----------- | ---- | ---- | -------- | -------------- | ------------ |
| FQDN | 1 | * | Utf8 |  |  |
| IcPort | 2 | * | Int32 |  |  |
| Path | 3 | * | Utf8 |  |  |
| Status | 4 |  | Uint32 |  |  |
| Timestamp | 5 |  | Uint64 |  |  |

Group

| Column | ID | PK | Type | Default | Purpose |
| ---------------------- | ---- | ---- | -------- | -------------- | ------------ |
| ID | 1 | * | Uint32 |  |  |
| Generation | 2 |  | Uint32 |  |  |
| ErasureSpecies | 3 |  | Uint32 |  |  |
| Owner | 4 |  | Uint64 |  |  |
| DesiredPDiskCategory | 5 |  | Uint64 |  |  |
| DesiredVDiskCategory | 6 |  | Uint64 |  |  |
| EncryptionMode | 7 |  | Uint32 | 0 |  |
| LifeCyclePhase | 8 |  | Uint32 | 0 |  |
| MainKeyId | 9 |  | String |  |  |
| EncryptedGroupKey | 10 |  | String |  |  |
| GroupKeyNonce | 11 |  | Uint64 | 0 |  |
| MainKeyVersion | 12 |  | Uint64 | 0 |  |
| Down | 13 |  | Bool | false |  |
| SeenOperational | 14 |  | Bool | false |  |

GroupLatencies

| Column | ID | PK | Type | Default | Purpose |
| ----------------------- | ---- | ---- | -------- | -------------- | ------------ |
| GroupId | 1 | * | Uint32 |  |  |
| PutTabletLogLatencyUs | 4 |  | Uint32 |  |  |
| PutUserDataLatencyUs | 5 |  | Uint32 |  |  |
| GetFastLatencyUs | 6 |  | Uint32 |  |  |

GroupStoragePool

| Column | ID | PK | Type | Default | Purpose |
| --------------- | ---- | ---- | -------- | -------------- | ------------ |
| GroupId | 1 | * | Uint32 |  |  |
| BoxId | 2 |  | Uint64 |  |  |
| StoragePoolId | 3 |  | Uint64 |  |  |

HostConfig

| Column | ID | PK | Type | Default | Purpose |
| -------------- | ---- | ---- | -------- | -------------- | ------------ |
| HostConfigId | 1 | * | Uint64 |  |  |
| Name | 2 |  | Utf8 |  |  |
| Generation | 3 |  | Uint64 |  |  |

HostConfigDrive

| Column | ID | PK | Type | Default | Purpose |
| -------------- | ---- | ---- | -------- | -------------- | ------------ |
| HostConfigId | 1 | * | Uint64 |  |  |
| Path | 2 | * | Utf8 |  |  |
| Type | 3 |  | Int32 |  |  |
| SharedWithOs | 4 |  | Bool |  |  |
| ReadCentric | 5 |  | Bool |  |  |
| Kind | 6 |  | Uint64 |  |  |
| PDiskConfig | 7 |  | String |  |  |

MigrationEntry

| Column | ID | PK | Type | Default | Purpose |
| --------------- | ---- | ---- | -------- | -------------- | ------------ |
| PlanName | 1 | * | String |  |  |
| EntryIndex | 2 | * | Uint64 |  |  |
| GroupId | 3 |  | Uint32 |  |  |
| OriginNodeId | 4 |  | Uint32 |  |  |
| OriginPDiskId | 5 |  | Uint32 |  |  |
| OriginVSlotId | 6 |  | Uint32 |  |  |
| TargetNodeId | 7 |  | Uint32 |  |  |
| TargetPDiskId | 8 |  | Uint32 |  |  |
| Done | 9 |  | Bool |  |  |

MigrationPlan

| Column | ID | PK | Type | Default | Purpose |
| --------- | ---- | ---- | -------- | -------------- | ------------ |
| Name | 1 | * | String |  |  |
| State | 2 |  | Uint32 |  |  |
| Size | 3 |  | Uint64 |  |  |
| Done | 4 |  | Uint64 |  |  |

Node

| Column | ID | PK | Type | Default | Purpose |
| ------------------- | ---- | ---- | -------- | -------------- | ------------ |
| ID | 1 | * | Uint32 |  |  |
| NextPDiskID | 2 |  | Uint32 |  |  |
| NextGroupKeyNonce | 9 |  | Uint64 | 0 |  |

OperationLog

| Column | ID | PK | Type | Default | Purpose |
| --------------- | ---- | ---- | -------- | ------------------- | ------------ |
| Index | 1 | * | Uint64 |  |  |
| Timestamp | 2 |  | Uint64 |  |  |
| Request | 3 |  | String |  |  |
| Response | 4 |  | String |  |  |
| ExecutionTime | 5 |  | Uint64 | TDuration::Zero() |  |

PDisk

| Column | ID | PK | Type | Default | Purpose |
| ---------------- | ---- | ---- | -------- | -------------- | ------------ |
| NodeID | 1 | * | Uint32 |  |  |
| PDiskID | 2 | * | Uint32 |  |  |
| Path | 3 |  | Utf8 |  |  |
| Category | 4 |  | Uint64 |  |  |
| Guid | 7 |  | Uint64 |  |  |
| SharedWithOs | 8 |  | Bool |  |  |
| ReadCentric | 9 |  | Bool |  |  |
| NextVSlotId | 10 |  | Uint32 | 1000 |  |
| PDiskConfig | 11 |  | String |  |  |
| Status | 12 |  | Uint32 |  |  |
| Timestamp | 13 |  | Uint64 |  |  |
| ExpectedSerial | 14 |  | String |  |  |
| LastSeenSerial | 15 |  | String |  |  |
| LastSeenPath | 16 |  | String |  |  |

PDiskMetrics

| Column | ID | PK | Type | Default | Purpose |
| --------- | ---- | ---- | -------- | -------------- | ------------ |
| NodeID | 1 | * | Uint32 |  |  |
| PDiskID | 2 | * | Uint32 |  |  |
| Metrics | 3 |  | String |  |  |

ScrubState

| Column | ID | PK | Type | Default | Purpose |
| ---------------------- | ---- | ---- | -------- | ------------------ | ------------ |
| NodeId | 1 | * | Uint32 |  |  |
| PDiskId | 2 | * | Uint32 |  |  |
| VSlotId | 3 | * | Uint32 |  |  |
| State | 5 |  | String |  |  |
| ScrubCycleStartTime | 6 |  | Uint64 | TInstant::Zero() |  |
| ScrubCycleFinishTime | 8 |  | Uint64 | TInstant::Zero() |  |
| Success | 7 |  | Bool | false |  |

State

| Column | ID | PK | Type | Default | Purpose |
| -------------------------- | ---- | ---- | -------- | --------------------------------------------- | ------------ |
| FixedKey | 1 | * | Bool |  |  |
| NextGroupID | 2 |  | Uint32 |  |  |
| SchemaVersion | 4 |  | Uint32 | 0 |  |
| NextOperationLogIndex | 5 |  | Uint64 |  |  |
| DefaultMaxSlots | 6 |  | Uint32 | 16 |  |
| InstanceId | 7 |  | String |  |  |
| SelfHealEnable | 8 |  | Bool | false |  |
| DonorModeEnable | 9 |  | Bool | true |  |
| ScrubPeriodicity | 10 |  | Uint32 | 86400*30 |  |
| SerialManagementStage | 11 |  | Uint32 |  |  |
| NextStoragePoolId | 12 |  | Uint64 | 0 |  |
| PDiskSpaceMarginPromille | 13 |  | Uint32 | 150 |  |
| GroupReserveMin | 14 |  | Uint32 | 0 |  |
| GroupReservePart | 15 |  | Uint32 | 0 |  |
| MaxScrubbedDisksAtOnce | 16 |  | Uint32 | Max<ui32>() |  |
| PDiskSpaceColorBorder | 17 |  | Uint32 | NKikimrBlobStorage::TPDiskSpaceColor::GREEN |  |

VDiskMetrics

| Column | ID | PK | Type | Default | Purpose |
| ----------------- | ---- | ---- | -------- | -------------- | ------------ |
| GroupID | 1 | * | Uint32 |  |  |
| GroupGeneration | 2 | * | Uint32 |  |  |
| Ring | 3 | * | Uint32 |  |  |
| FailDomain | 4 | * | Uint32 |  |  |
| VDisk | 5 | * | Uint32 |  |  |
| Metrics | 11 |  | String |  |  |

VSlot

| Column | ID | PK | Type | Default | Purpose |
| --------------------- | ---- | ---- | -------- | ------------------ | ------------ |
| NodeID | 1 | * | Uint32 |  |  |
| PDiskID | 2 | * | Uint32 |  |  |
| VSlotID | 3 | * | Uint32 |  |  |
| Category | 4 |  | Uint64 |  |  |
| GroupID | 5 |  | Uint32 |  |  |
| GroupGeneration | 6 |  | Uint32 |  |  |
| RingIdx | 7 |  | Uint32 |  |  |
| FailDomainIdx | 8 |  | Uint32 |  |  |
| VDiskIdx | 9 |  | Uint32 |  |  |
| GroupPrevGeneration | 10 |  | Uint32 | 0 |  |
| Mood | 11 |  | Uint32 | 0 |  |
| LastSeenReady | 12 |  | Uint64 | TInstant::Zero() |  |

### Messages via pipe

### Transaction types

