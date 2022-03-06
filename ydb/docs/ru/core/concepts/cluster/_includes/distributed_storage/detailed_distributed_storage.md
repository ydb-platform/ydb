## Детали реализации BlobStorage

### Управление ресурсами BlobStorage на узлах через NodeWarden

### Как работает PDisk

#### Формат данных

### Как работает VDisk

#### Большие и маленькие блобы

#### Fresh-сегмент

#### SSTable

#### Синхронизация метаданных

#### Репликация данных

#### Формат данных

##### Диски-доноры

### Как работает DS proxy

## BS_CONTROLLER

### Связь с NodeWarden

### Box и Storage Pool

### Self Heal

### Сущности и таблицы

Всю информацию, которая хранится в BS_CONTROLLER можно условно разбить на несколько частей:

1. Настройки BS_CONTROLLER.
1. Конфигурация нижнего уровня (PDisk'и, VDisk'и, группы).
1. Конфигурация верхнего уровня (Box'ы и StoragePool'ы).
1. Runtime-информация (персистентные метрики дисков и групп, состояние скраббинга отдельных VDisk'ов, информация о серийных номерах накопителей на узлах).
1. Логи операций (OperationLog).

Логика работы построена таким образом, что вся информация из таблиц на запуске таблетки BS_CONTROLLER'а загружается в память и хранится там все время его работы (за исключением OperationLog). При выполнении транзакций информация обновляется в таблицах и в памяти.

Более подробно таблицы рассматриваются ниже.

#### Настройки BS_CONTROLLER

Настройки хранятся в таблице State.

#### Конфигурация нижнего уровня

Конфигурация нижнего уровня включает в себя информацию об имеющихся ресурсах кластера: PDisk'и, VDisk'и и группы. Эти таблицы связаны друг с другом через связи PK <-> FK.

#### Конфигурация верхнего уровня

Конфигурация верхнего уровня хранит данные, необходимые для связывания Box'ов, узлов, где они размещены...

#### Схема базы

Box

| Колонка    | ID | PK | Тип    | По умолчанию | Назначение |
|------------|----|----|--------|--------------|------------|
| BoxId      | 1  | *  | Uint64 |              |            |
| Name       | 2  |    | Utf8   |              |            |
| Generation | 3  |    | Uint64 |              |            |

BoxHost

| Колонка      | ID | PK | Тип    | По умолчанию | Назначение |
|--------------|----|----|--------|--------------|------------|
| BoxId        | 1  | *  | Uint64 |              |            |
| Fqdn         | 3  | *  | Utf8   |              |            |
| IcPort       | 4  | *  | Int32  |              |            |
| HostConfigId | 5  |    | Uint64 |              |            |

BoxHostV2

| Колонка      | ID | PK | Тип    | По умолчанию | Назначение |
|--------------|----|----|--------|--------------|------------|
| BoxId        | 1  | *  | Uint64 |              |            |
| Fqdn         | 2  | *  | Utf8   |              |            |
| IcPort       | 3  | *  | Int32  |              |            |
| HostConfigId | 4  |    | Uint64 |              |            |

BoxStoragePool

| Колонка                    | ID | PK | Тип    | По умолчанию | Назначение |
|----------------------------|----|----|--------|--------------|------------|
| BoxId                      | 1  | *  | Uint64 |              |            |
| StoragePoolId              | 2  | *  | Uint64 |              |            |
| Name                       | 20 |    | Utf8   |              |            |
| ErasureSpecies             | 3  |    | Int32  |              |            |
| RealmLevelBegin            | 4  |    | Int32  |              |            |
| RealmLevelEnd              | 5  |    | Int32  |              |            |
| DomainLevelBegin           | 6  |    | Int32  |              |            |
| DomainLevelEnd             | 7  |    | Int32  |              |            |
| NumFailRealms              | 8  |    | Int32  |              |            |
| NumFailDomainsPerFailRealm | 9  |    | Int32  |              |            |
| NumVDisksPerFailDomain     | 10 |    | Int32  |              |            |
| VDiskKind                  | 11 |    | Int32  |              |            |
| SpaceBytes                 | 12 |    | Uint64 |              |            |
| WriteIOPS                  | 13 |    | Uint64 |              |            |
| WriteBytesPerSecond        | 14 |    | Uint64 |              |            |
| ReadIOPS                   | 15 |    | Uint64 |              |            |
| ReadBytesPerSecond         | 16 |    | Uint64 |              |            |
| InMemCacheBytes            | 17 |    | Uint64 |              |            |
| Kind                       | 18 |    | Utf8   |              |            |
| NumGroups                  | 19 |    | Uint32 |              |            |
| Generation                 | 21 |    | Uint64 |              |            |
| EncryptionMode             | 22 |    | Uint32 | 0            |            |
| SchemeshardId              | 23 |    | Uint64 |              |            |
| PathItemId                 | 24 |    | Uint64 |              |            |
| RandomizeGroupMapping      | 25 |    | Bool   | false        |            |

BoxStoragePoolPDiskFilter

| Колонка       | ID | PK | Тип    | По умолчанию | Назначение |
|---------------|----|----|--------|--------------|------------|
| BoxId         | 1  | *  | Uint64 |              |            |
| StoragePoolId | 2  | *  | Uint64 |              |            |
| Type          | 3  | *  | Int32  |              |            |
| SharedWithOs  | 4  | *  | Bool   |              |            |
| ReadCentric   | 5  | *  | Bool   |              |            |
| Kind          | 6  | *  | Uint64 |              |            |

BoxStoragePoolUser

| Колонка       | ID | PK | Тип    | По умолчанию | Назначение |
|---------------|----|----|--------|--------------|------------|
| BoxId         | 1  | *  | Uint64 |              |            |
| StoragePoolId | 2  | *  | Uint64 |              |            |
| UserId        | 3  | *  | String |              |            |

BoxUser

| Колонка | ID | PK | Тип    | По умолчанию | Назначение |
|---------|----|----|--------|--------------|------------|
| BoxId   | 1  | *  | Uint64 |              |            |
| UserId  | 2  | *  | String |              |            |

DriveSerial

| Колонка     | ID | PK | Тип    | По умолчанию | Назначение |
|-------------|----|----|--------|--------------|------------|
| Serial      | 1  | *  | String |              |            |
| BoxId       | 2  |    | Uint64 |              |            |
| NodeId      | 3  |    | Uint32 |              |            |
| PDiskId     | 4  |    | Uint32 |              |            |
| Guid        | 5  |    | Uint64 |              |            |
| LifeStage   | 6  |    | Uint32 |              |            |
| Kind        | 7  |    | Uint64 |              |            |
| PDiskType   | 8  |    | Int32  |              |            |
| PDiskConfig | 9  |    | String |              |            |

DriveStatus

| Колонка   | ID | PK | Тип    | По умолчанию | Назначение |
|-----------|----|----|--------|--------------|------------|
| Fqdn      | 1  | *  | Utf8   |              |            |
| IcPort    | 2  | *  | Int32  |              |            |
| Path      | 3  | *  | Utf8   |              |            |
| Status    | 4  |    | Uint32 |              |            |
| Timestamp | 5  |    | Uint64 |              |            |

Group

| Колонка              | ID | PK | Тип    | По умолчанию | Назначение |
|----------------------|----|----|--------|--------------|------------|
| ID                   | 1  | *  | Uint32 |              |            |
| Generation           | 2  |    | Uint32 |              |            |
| ErasureSpecies       | 3  |    | Uint32 |              |            |
| Owner                | 4  |    | Uint64 |              |            |
| DesiredPDiskCategory | 5  |    | Uint64 |              |            |
| DesiredVDiskCategory | 6  |    | Uint64 |              |            |
| EncryptionMode       | 7  |    | Uint32 | 0            |            |
| LifeCyclePhase       | 8  |    | Uint32 | 0            |            |
| MainKeyId            | 9  |    | String |              |            |
| EncryptedGroupKey    | 10 |    | String |              |            |
| GroupKeyNonce        | 11 |    | Uint64 | 0            |            |
| MainKeyVersion       | 12 |    | Uint64 | 0            |            |
| Down                 | 13 |    | Bool   | false        |            |
| SeenOperational      | 14 |    | Bool   | false        |            |

GroupLatencies

| Колонка               | ID | PK | Тип    | По умолчанию | Назначение |
|-----------------------|----|----|--------|--------------|------------|
| GroupId               | 1  | *  | Uint32 |              |            |
| PutTabletLogLatencyUs | 4  |    | Uint32 |              |            |
| PutUserDataLatencyUs  | 5  |    | Uint32 |              |            |
| GetFastLatencyUs      | 6  |    | Uint32 |              |            |

GroupStoragePool

| Колонка       | ID | PK | Тип    | По умолчанию | Назначение |
|---------------|----|----|--------|--------------|------------|
| GroupId       | 1  | *  | Uint32 |              |            |
| BoxId         | 2  |    | Uint64 |              |            |
| StoragePoolId | 3  |    | Uint64 |              |            |

HostConfig

| Колонка      | ID | PK | Тип    | По умолчанию | Назначение |
|--------------|----|----|--------|--------------|------------|
| HostConfigId | 1  | *  | Uint64 |              |            |
| Name         | 2  |    | Utf8   |              |            |
| Generation   | 3  |    | Uint64 |              |            |

HostConfigDrive

| Колонка      | ID | PK | Тип    | По умолчанию | Назначение |
|--------------|----|----|--------|--------------|------------|
| HostConfigId | 1  | *  | Uint64 |              |            |
| Path         | 2  | *  | Utf8   |              |            |
| Type         | 3  |    | Int32  |              |            |
| SharedWithOs | 4  |    | Bool   |              |            |
| ReadCentric  | 5  |    | Bool   |              |            |
| Kind         | 6  |    | Uint64 |              |            |
| PDiskConfig  | 7  |    | String |              |            |

MigrationEntry

| Колонка       | ID | PK | Тип    | По умолчанию | Назначение |
|---------------|----|----|--------|--------------|------------|
| PlanName      | 1  | *  | String |              |            |
| EntryIndex    | 2  | *  | Uint64 |              |            |
| GroupId       | 3  |    | Uint32 |              |            |
| OriginNodeId  | 4  |    | Uint32 |              |            |
| OriginPDiskId | 5  |    | Uint32 |              |            |
| OriginVSlotId | 6  |    | Uint32 |              |            |
| TargetNodeId  | 7  |    | Uint32 |              |            |
| TargetPDiskId | 8  |    | Uint32 |              |            |
| Done          | 9  |    | Bool   |              |            |

MigrationPlan

| Колонка | ID | PK | Тип    | По умолчанию | Назначение |
|---------|----|----|--------|--------------|------------|
| Name    | 1  | *  | String |              |            |
| State   | 2  |    | Uint32 |              |            |
| Size    | 3  |    | Uint64 |              |            |
| Done    | 4  |    | Uint64 |              |            |

Node

| Колонка           | ID | PK | Тип    | По умолчанию | Назначение |
|-------------------|----|----|--------|--------------|------------|
| ID                | 1  | *  | Uint32 |              |            |
| NextPDiskID       | 2  |    | Uint32 |              |            |
| NextGroupKeyNonce | 9  |    | Uint64 | 0            |            |

OperationLog

| Колонка       | ID | PK | Тип    | По умолчанию      | Назначение |
|---------------|----|----|--------|-------------------|------------|
| Index         | 1  | *  | Uint64 |                   |            |
| Timestamp     | 2  |    | Uint64 |                   |            |
| Request       | 3  |    | String |                   |            |
| Response      | 4  |    | String |                   |            |
| ExecutionTime | 5  |    | Uint64 | TDuration::Zero() |            |

PDisk

| Колонка        | ID | PK | Тип    | По умолчанию | Назначение |
|----------------|----|----|--------|--------------|------------|
| NodeID         | 1  | *  | Uint32 |              |            |
| PDiskID        | 2  | *  | Uint32 |              |            |
| Path           | 3  |    | Utf8   |              |            |
| Category       | 4  |    | Uint64 |              |            |
| Guid           | 7  |    | Uint64 |              |            |
| SharedWithOs   | 8  |    | Bool   |              |            |
| ReadCentric    | 9  |    | Bool   |              |            |
| NextVSlotId    | 10 |    | Uint32 | 1000         |            |
| PDiskConfig    | 11 |    | String |              |            |
| Status         | 12 |    | Uint32 |              |            |
| Timestamp      | 13 |    | Uint64 |              |            |
| ExpectedSerial | 14 |    | String |              |            |
| LastSeenSerial | 15 |    | String |              |            |
| LastSeenPath   | 16 |    | String |              |            |

PDiskMetrics

| Колонка | ID | PK | Тип    | По умолчанию | Назначение |
|---------|----|----|--------|--------------|------------|
| NodeID  | 1  | *  | Uint32 |              |            |
| PDiskID | 2  | *  | Uint32 |              |            |
| Metrics | 3  |    | String |              |            |

ScrubState

| Колонка              | ID | PK | Тип    | По умолчанию     | Назначение |
|----------------------|----|----|--------|------------------|------------|
| NodeId               | 1  | *  | Uint32 |                  |            |
| PDiskId              | 2  | *  | Uint32 |                  |            |
| VSlotId              | 3  | *  | Uint32 |                  |            |
| State                | 5  |    | String |                  |            |
| ScrubCycleStartTime  | 6  |    | Uint64 | TInstant::Zero() |            |
| ScrubCycleFinishTime | 8  |    | Uint64 | TInstant::Zero() |            |
| Success              | 7  |    | Bool   | false            |            |

State

| Колонка                  | ID | PK | Тип    | По умолчанию                                | Назначение |
|--------------------------|----|----|--------|---------------------------------------------|------------|
| FixedKey                 | 1  | *  | Bool   |                                             |            |
| NextGroupID              | 2  |    | Uint32 |                                             |            |
| SchemaVersion            | 4  |    | Uint32 | 0                                           |            |
| NextOperationLogIndex    | 5  |    | Uint64 |                                             |            |
| DefaultMaxSlots          | 6  |    | Uint32 | 16                                          |            |
| InstanceId               | 7  |    | String |                                             |            |
| SelfHealEnable           | 8  |    | Bool   | false                                       |            |
| DonorModeEnable          | 9  |    | Bool   | true                                        |            |
| ScrubPeriodicity         | 10 |    | Uint32 | 86400*30                                    |            |
| SerialManagementStage    | 11 |    | Uint32 |                                             |            |
| NextStoragePoolId        | 12 |    | Uint64 | 0                                           |            |
| PDiskSpaceMarginPromille | 13 |    | Uint32 | 150                                         |            |
| GroupReserveMin          | 14 |    | Uint32 | 0                                           |            |
| GroupReservePart         | 15 |    | Uint32 | 0                                           |            |
| MaxScrubbedDisksAtOnce   | 16 |    | Uint32 | Max<ui32>()                                 |            |
| PDiskSpaceColorBorder    | 17 |    | Uint32 | NKikimrBlobStorage::TPDiskSpaceColor::GREEN |            |

VDiskMetrics

| Колонка         | ID | PK | Тип    | По умолчанию | Назначение |
|-----------------|----|----|--------|--------------|------------|
| GroupID         | 1  | *  | Uint32 |              |            |
| GroupGeneration | 2  | *  | Uint32 |              |            |
| Ring            | 3  | *  | Uint32 |              |            |
| FailDomain      | 4  | *  | Uint32 |              |            |
| VDisk           | 5  | *  | Uint32 |              |            |
| Metrics         | 11 |    | String |              |            |

VSlot

| Колонка             | ID | PK | Тип    | По умолчанию     | Назначение |
|---------------------|----|----|--------|------------------|------------|
| NodeID              | 1  | *  | Uint32 |                  |            |
| PDiskID             | 2  | *  | Uint32 |                  |            |
| VSlotID             | 3  | *  | Uint32 |                  |            |
| Category            | 4  |    | Uint64 |                  |            |
| GroupID             | 5  |    | Uint32 |                  |            |
| GroupGeneration     | 6  |    | Uint32 |                  |            |
| RingIdx             | 7  |    | Uint32 |                  |            |
| FailDomainIdx       | 8  |    | Uint32 |                  |            |
| VDiskIdx            | 9  |    | Uint32 |                  |            |
| GroupPrevGeneration | 10 |    | Uint32 | 0                |            |
| Mood                | 11 |    | Uint32 | 0                |            |
| LastSeenReady       | 12 |    | Uint64 | TInstant::Zero() |            |

### Сообщения через pipe

### Виды транзакций
