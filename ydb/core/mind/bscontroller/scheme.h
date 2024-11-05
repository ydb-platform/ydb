#pragma once

#include "defs.h"
#include "mood.h"

namespace NKikimr {
namespace NBsController {

struct Schema : NIceDb::Schema {

    enum : ui32 {
        CurrentSchemaVersion = 2,
        BoxHostMigrationSchemaVersion = 2,
    };

    struct Node : Table<2> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> {};
        struct NextPDiskID : Column<2, NScheme::NTypeIds::Uint32> {};
        struct LastConnectTimestamp : Column<10, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };
        struct LastDisconnectTimestamp : Column<11, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, NextPDiskID, LastConnectTimestamp, LastDisconnectTimestamp>;
    };

    struct PDisk : Table<3> {
        struct NodeID : Column<1, Node::ID::ColumnType> {}; // PK
        struct PDiskID : Column<2, Node::NextPDiskID::ColumnType> {}; // PK
        struct Path : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Category : Column<4, NScheme::NTypeIds::Uint64> { using Type = TPDiskCategory;};
        //struct SystemConfig : Column<5, NScheme::NTypeIds::String> {};
        //struct PhysicalLocation : Column<6, NScheme::NTypeIds::String> {};
        struct Guid : Column<7, NScheme::NTypeIds::Uint64> {};
        struct SharedWithOs : Column<8, NScheme::NTypeIds::Bool> {};
        struct ReadCentric : Column<9, NScheme::NTypeIds::Bool> {};
        struct NextVSlotId : Column<10, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 1000; };
        struct PDiskConfig : Column<11, NScheme::NTypeIds::String> {};
        struct Status : Column<12, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::EDriveStatus; };
        struct Timestamp : Column<13, NScheme::NTypeIds::Uint64> { using Type = TInstant; };
        struct ExpectedSerial : Column<14, NScheme::NTypeIds::String> {};
        struct LastSeenSerial : Column<15, NScheme::NTypeIds::String> {};
        struct LastSeenPath : Column<16, NScheme::NTypeIds::String> {};
        struct DecommitStatus : Column<17, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::EDecommitStatus; static constexpr Type Default = Type::DECOMMIT_NONE; };
        struct Mood : Column<18, NScheme::NTypeIds::Uint8> { using Type = TPDiskMood::EValue; static constexpr Type Default = Type::Normal; };

        using TKey = TableKey<NodeID, PDiskID>; // order is important
        using TColumns = TableColumns<NodeID, PDiskID, Path, Category, Guid, SharedWithOs, ReadCentric, NextVSlotId,
              Status, Timestamp, PDiskConfig, ExpectedSerial, LastSeenSerial, LastSeenPath, DecommitStatus, Mood>;
    };

    struct Group : Table<4> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> { using Type = TGroupId; static constexpr Type Default = TGroupId::Zero();}; // PK
        struct Generation : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ErasureSpecies : Column<3, NScheme::NTypeIds::Uint32> { using Type = TErasureType::EErasureSpecies; };
        struct Owner : Column<4, NScheme::NTypeIds::Uint64> {};
        struct DesiredPDiskCategory : Column<5, NScheme::NTypeIds::Uint64> { using Type = TPDiskCategory; };
        struct DesiredVDiskCategory : Column<6, NScheme::NTypeIds::Uint64> { using Type = NKikimrBlobStorage::TVDiskKind::EVDiskKind; };
        struct EncryptionMode : Column<7, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; };
        struct LifeCyclePhase : Column<8, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; };
        struct MainKeyId : Column<9, NScheme::NTypeIds::String> {};
        struct EncryptedGroupKey : Column<10, NScheme::NTypeIds::String> {};
        struct GroupKeyNonce : Column<11, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 0; };
        struct MainKeyVersion : Column<12, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 0; };
        struct Down : Column<13, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };
        struct SeenOperational : Column<14, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };
        struct DecommitStatus : Column<15, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::TGroupDecommitStatus::E; };

        // VirtualGroup management code
        struct VirtualGroupName  : Column<112, NScheme::NTypeIds::Utf8>   {}; // unique name of the virtual group
        struct VirtualGroupState : Column<102, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::EVirtualGroupState; };
        struct HiveId            : Column<113, NScheme::NTypeIds::Uint64> {}; // hive id for this vg
        struct Database          : Column<120, NScheme::NTypeIds::String> {}; // database path
        struct BlobDepotConfig   : Column<106, NScheme::NTypeIds::String> {}; // serialized blob depot config protobuf
        struct BlobDepotId       : Column<109, NScheme::NTypeIds::Uint64> {}; // created blobdepot tablet id
        struct ErrorReason       : Column<110, NScheme::NTypeIds::Utf8>   {}; // creation error reason
        struct NeedAlter         : Column<111, NScheme::NTypeIds::Bool>   {}; // did the BlobDepotConfig change?
        struct Metrics           : Column<114, NScheme::NTypeIds::String> {}; // for virtual groups only

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, Generation, ErasureSpecies, Owner, DesiredPDiskCategory, DesiredVDiskCategory,
              EncryptionMode, LifeCyclePhase, MainKeyId, EncryptedGroupKey, GroupKeyNonce, MainKeyVersion, Down,
              SeenOperational, DecommitStatus, VirtualGroupName, VirtualGroupState, HiveId, Database, BlobDepotConfig,
              BlobDepotId, ErrorReason, NeedAlter, Metrics>;
    };

    struct State : Table<1> {
        struct FixedKey : Column<1, NScheme::NTypeIds::Bool> {}; // PK, forever 'true'
        struct NextGroupID : Column<2, Group::ID::ColumnType> {};
        //struct GroupSelectionPercent : Column<3, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 7; };
        struct SchemaVersion : Column<4, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; };
        struct NextOperationLogIndex : Column<5, NScheme::NTypeIds::Uint64> {};
        struct DefaultMaxSlots : Column<6, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 16; };
        struct InstanceId : Column<7, NScheme::NTypeIds::String> {};
        struct SelfHealEnable : Column<8, NScheme::NTypeIds::Bool> { static constexpr Type Default = true; };
        struct DonorModeEnable : Column<9, NScheme::NTypeIds::Bool> { static constexpr Type Default = true; };
        struct ScrubPeriodicity : Column<10, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 86400 * 30; };
        struct SerialManagementStage : Column<11, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::TSerialManagementStage::E; };
        struct NextStoragePoolId : Column<12, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 0; };
        struct PDiskSpaceMarginPromille : Column<13, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 150; }; // 15% default margin (85% max usage)
        struct GroupReserveMin : Column<14, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; };
        struct GroupReservePart : Column<15, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; }; // parts per million
        struct MaxScrubbedDisksAtOnce : Column<16, NScheme::NTypeIds::Uint32> { static constexpr Type Default = Max<ui32>(); }; // no limit
        struct PDiskSpaceColorBorder : Column<17, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::TPDiskSpaceColor::E; static constexpr Type Default = NKikimrBlobStorage::TPDiskSpaceColor::GREEN; };
        struct GroupLayoutSanitizer : Column<18, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };
        struct NextVirtualGroupId : Column<19, Group::ID::ColumnType> { static constexpr Type Default = 0; };
        struct AllowMultipleRealmsOccupation : Column<20, NScheme::NTypeIds::Bool> { static constexpr Type Default = true; };
        struct CompatibilityInfo : Column<21, NScheme::NTypeIds::String> {};
        struct UseSelfHealLocalPolicy : Column<22, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };
        struct TryToRelocateBrokenDisksLocallyFirst : Column<23, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };

        using TKey = TableKey<FixedKey>;
        using TColumns = TableColumns<FixedKey, NextGroupID, SchemaVersion, NextOperationLogIndex, DefaultMaxSlots,
              InstanceId, SelfHealEnable, DonorModeEnable, ScrubPeriodicity, SerialManagementStage, NextStoragePoolId,
              PDiskSpaceMarginPromille, GroupReserveMin, GroupReservePart, MaxScrubbedDisksAtOnce, PDiskSpaceColorBorder,
              GroupLayoutSanitizer, NextVirtualGroupId, AllowMultipleRealmsOccupation, CompatibilityInfo,
              UseSelfHealLocalPolicy, TryToRelocateBrokenDisksLocallyFirst>;
    };

    struct VSlot : Table<5> {
        struct NodeID : Column<1, PDisk::NodeID::ColumnType> {}; // PK + FK PDisk.NodeID;
        struct PDiskID : Column<2, PDisk::PDiskID::ColumnType> {}; // PK + FK PDisk.PDiskID
        struct VSlotID : Column<3, NScheme::NTypeIds::Uint32> {}; // PK
        struct Category : Column<4, NScheme::NTypeIds::Uint64> { using Type = NKikimrBlobStorage::TVDiskKind::EVDiskKind; };
        struct GroupID : Column<5, Group::ID::ColumnType> {using Type = TGroupId; static constexpr Type Default = TGroupId::Zero(); }; // FK Group.ID
        struct GroupGeneration : Column<6, Group::Generation::ColumnType> {};
        struct RingIdx : Column<7, NScheme::NTypeIds::Uint32> {};
        struct FailDomainIdx : Column<8, NScheme::NTypeIds::Uint32> {};
        struct VDiskIdx : Column<9, NScheme::NTypeIds::Uint32> {};
        struct GroupPrevGeneration : Column<10, Group::Generation::ColumnType> { static constexpr ui32 Default = 0; };
        struct Mood : Column<11, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 0; };
        struct LastSeenReady : Column<12, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };
        struct LastGotReplicating : Column<13, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };
        struct ReplicationTime : Column<14, NScheme::NTypeIds::Uint64> { using Type = TDuration; static constexpr Type Default = TDuration::Zero(); };

        using TKey = TableKey<NodeID, PDiskID, VSlotID>; // order is important
        using TColumns = TableColumns<
            NodeID,
            PDiskID,
            VSlotID,
            Category,
            GroupID,
            GroupGeneration,
            RingIdx,
            FailDomainIdx,
            VDiskIdx,
            GroupPrevGeneration,
            Mood,
            LastSeenReady,
            LastGotReplicating,
            ReplicationTime>;
    };

    struct VDiskMetrics : Table<6> {
        struct GroupID : Column<1, Group::ID::ColumnType> {};
        struct GroupGeneration : Column<2, Group::Generation::ColumnType> {};
        struct Ring : Column<3, VSlot::RingIdx::ColumnType> {};
        struct FailDomain : Column<4, VSlot::FailDomainIdx::ColumnType> {};
        struct VDisk : Column<5, VSlot::VDiskIdx::ColumnType> {};
        struct Metrics : Column<11, NScheme::NTypeIds::String> { using Type = NKikimrBlobStorage::TVDiskMetrics; };

        using TKey = TableKey<GroupID, GroupGeneration, Ring, FailDomain, VDisk>;
        using TColumns = TableColumns<
            GroupID,
            GroupGeneration,
            Ring,
            FailDomain,
            VDisk,
            Metrics
        >;
    };

    struct PDiskMetrics : Table<7> {
        struct NodeID : Column<1, Node::ID::ColumnType> {}; // PK
        struct PDiskID : Column<2, Node::NextPDiskID::ColumnType> {}; // PK
        struct Metrics : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrBlobStorage::TPDiskMetrics; };

        using TKey = TableKey<NodeID, PDiskID>;
        using TColumns = TableColumns<
            NodeID,
            PDiskID,
            Metrics
        >;
    };

    struct GroupLatencies : Table<8> {
        struct GroupId : Column<1, NScheme::NTypeIds::Uint32> {};
        //struct GetLatencyUs : Column<2, NScheme::NTypeIds::Uint32> {};
        //struct PutLatencyUs : Column<3, NScheme::NTypeIds::Uint32> {};
        struct PutTabletLogLatencyUs : Column<4, NScheme::NTypeIds::Uint32> {};
        struct PutUserDataLatencyUs : Column<5, NScheme::NTypeIds::Uint32> {};
        struct GetFastLatencyUs : Column<6, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<GroupId>;
        using TColumns = TableColumns<GroupId, PutTabletLogLatencyUs, PutUserDataLatencyUs, GetFastLatencyUs>;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BLOBSTORAGE CONFIGURATION USER-DEFINED TABLES
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct Box : Table<100> {
        struct BoxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Generation : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<BoxId>;
        using TColumns = TableColumns<BoxId, Name, Generation>;
    };

    struct BoxUser : Table<101> {
        struct BoxId : Column<1, Box::BoxId::ColumnType> {};
        struct UserId : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BoxId, UserId>;
        using TColumns = TableColumns<BoxId, UserId>;
    };

    struct HostConfig : Table<102> {
        struct HostConfigId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Generation : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<HostConfigId>;
        using TColumns = TableColumns<HostConfigId, Name, Generation>;
    };

    struct HostConfigDrive : Table<103> {
        struct HostConfigId : Column<1, HostConfig::HostConfigId::ColumnType> {};
        struct Path : Column<2, NScheme::NTypeIds::Utf8> {};
        struct TypeCol : Column<3, NScheme::NTypeIds::Int32> { static TString GetColumnName(const TString&) { return "Type"; } using Type = NKikimrBlobStorage::EPDiskType; };
        struct SharedWithOs : Column<4, NScheme::NTypeIds::Bool> {};
        struct ReadCentric : Column<5, NScheme::NTypeIds::Bool> {};
        struct Kind : Column<6, NScheme::NTypeIds::Uint64> {};
        struct PDiskConfig : Column<7, NScheme::NTypeIds::String> {};

        using TKey = TableKey<HostConfigId, Path>;
        using TColumns = TableColumns<HostConfigId, Path, TypeCol, SharedWithOs, ReadCentric, Kind, PDiskConfig>;
    };

    struct BoxHostV2 : Table<105> {
        struct BoxId : Column<1, Box::BoxId::ColumnType> {};
        struct Fqdn : Column<2, NScheme::NTypeIds::Utf8> {};
        struct IcPort : Column<3, NScheme::NTypeIds::Int32> {};
        struct HostConfigId : Column<4, HostConfig::HostConfigId::ColumnType> {};
        struct EnforcedNodeId : Column<5, Node::ID::ColumnType> {};

        using TKey = TableKey<BoxId, Fqdn, IcPort>;
        using TColumns = TableColumns<BoxId, Fqdn, IcPort, HostConfigId, EnforcedNodeId>;
    };

    struct BoxStoragePool : Table<120> {
        // the box this storage pool belongs to
        struct BoxId : Column<1, Box::BoxId::ColumnType> {};
        // unique identifier of the storage pool itself (must be unique to the box)
        struct StoragePoolId : Column<2, NScheme::NTypeIds::Uint64> {};
        // user-friendly name
        struct Name : Column<20, NScheme::NTypeIds::Utf8> {};
        // erasure kind for groups of the pool
        struct ErasureSpecies : Column<3, NScheme::NTypeIds::Int32> { using Type = TErasureType::EErasureSpecies; };
        // first level to distinguish between fail realms while generating groups
        struct RealmLevelBegin : Column<4, NScheme::NTypeIds::Int32> {};
        // last level to distinguish between fail realms while generating groups
        struct RealmLevelEnd : Column<5, NScheme::NTypeIds::Int32> {};
        // first level to distinguish between fail domains while generating groups
        struct DomainLevelBegin : Column<6, NScheme::NTypeIds::Int32> {};
        // last level to distinguish between fail domains while generating groups
        struct DomainLevelEnd : Column<7, NScheme::NTypeIds::Int32> {};
        // number of fail realms per single group
        struct NumFailRealms : Column<8, NScheme::NTypeIds::Int32> {};
        // number of fail domains per fail realm
        struct NumFailDomainsPerFailRealm : Column<9, NScheme::NTypeIds::Int32> {};
        // number of vdisks per fail domain
        struct NumVDisksPerFailDomain : Column<10, NScheme::NTypeIds::Int32> {};
        // kind of vdisks to create (as defined in NKikimrBlobStorage.TVDiskKind.EVDiskKind enum)
        struct VDiskKind : Column<11, NScheme::NTypeIds::Int32> { using Type = NKikimrBlobStorage::TVDiskKind::EVDiskKind; };
        // how much space user wants to reserve in this storage pool (measured in bytes)
        struct SpaceBytes : Column<12, NScheme::NTypeIds::Uint64> {};
        // planned number of write IOPS
        struct WriteIOPS : Column<13, NScheme::NTypeIds::Uint64> {};
        // planned write throughput (measured in bytes per second)
        struct WriteBytesPerSecond : Column<14, NScheme::NTypeIds::Uint64> {};
        // planned number of read IOPS
        struct ReadIOPS : Column<15, NScheme::NTypeIds::Uint64> {};
        // planned read throughput (measured in bytes per second)
        struct ReadBytesPerSecond : Column<16, NScheme::NTypeIds::Uint64> {};
        // size of in-memory cache for this storage pool (measured in bytes)
        struct InMemCacheBytes : Column<17, NScheme::NTypeIds::Uint64> {};
        // user-defined nontransparent storage pool kind used for searching
        struct Kind : Column<18, NScheme::NTypeIds::Utf8> {};
        // internally defined value -- number of groups required to fulfil user requirements
        struct NumGroups : Column<19, NScheme::NTypeIds::Uint32> {};
        // configuration item generation to prevent concurrent access
        struct Generation : Column<21, NScheme::NTypeIds::Uint64> {};
        // DS_PROXY encryption params
        struct EncryptionMode : Column<22, NScheme::NTypeIds::Uint32> { static constexpr Type Default = 0; };
        // scope id for the tenant
        struct SchemeshardId : Column<23, NScheme::NTypeIds::Uint64> {};
        struct PathItemId : Column<24, NScheme::NTypeIds::Uint64> {};
        // flag used to minimize correlation between groups and drives
        struct RandomizeGroupMapping : Column<25, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };

        using TKey = TableKey<BoxId, StoragePoolId>;

        using TColumns = TableColumns<BoxId, StoragePoolId, Name, ErasureSpecies, RealmLevelBegin, RealmLevelEnd,
          DomainLevelBegin, DomainLevelEnd, NumFailRealms, NumFailDomainsPerFailRealm, NumVDisksPerFailDomain,
          VDiskKind, SpaceBytes, WriteIOPS, WriteBytesPerSecond, ReadIOPS, ReadBytesPerSecond, InMemCacheBytes,
          Kind, NumGroups, Generation, EncryptionMode, SchemeshardId, PathItemId, RandomizeGroupMapping>;
    };

    struct BoxStoragePoolUser : Table<121> {
        struct BoxId : Column<1, BoxStoragePool::BoxId::ColumnType> {};
        struct StoragePoolId : Column<2, BoxStoragePool::StoragePoolId::ColumnType> {};
        struct UserId : Column<3, BoxUser::UserId::ColumnType> {};

        using TKey = TableKey<BoxId, StoragePoolId, UserId>;
        using TColumns = TableColumns<BoxId, StoragePoolId, UserId>;
    };

    struct BoxStoragePoolPDiskFilter : Table<122> {
        struct BoxId : Column<1, BoxStoragePool::BoxId::ColumnType> {};
        struct StoragePoolId : Column<2, BoxStoragePool::StoragePoolId::ColumnType> {};
        struct TypeCol : Column<3, HostConfigDrive::TypeCol::ColumnType> { static TString GetColumnName(const TString&) { return "Type"; } using Type = NKikimrBlobStorage::EPDiskType; };
        struct SharedWithOs : Column<4, HostConfigDrive::SharedWithOs::ColumnType> {};
        struct ReadCentric : Column<5, HostConfigDrive::ReadCentric::ColumnType> {};
        struct Kind : Column<6, HostConfigDrive::Kind::ColumnType> {};

        using TKey = TableKey<BoxId, StoragePoolId, TypeCol, SharedWithOs, ReadCentric, Kind>;
        using TColumns = TableColumns<BoxId, StoragePoolId, TypeCol, SharedWithOs, ReadCentric, Kind>;
    };

    struct GroupStoragePool : Table<123> {
        struct GroupId : Column<1, Group::ID::ColumnType> {};
        struct BoxId : Column<2, BoxStoragePool::BoxId::ColumnType> {};
        struct StoragePoolId : Column<3, BoxStoragePool::StoragePoolId::ColumnType> {};

        using TKey = TableKey<GroupId>;
        using TColumns = TableColumns<GroupId, BoxId, StoragePoolId>;
    };

    static constexpr ui32 DriveStatusTableId = 124;

    struct OperationLog : Table<125> {
        struct Index : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Timestamp : Column<2, NScheme::NTypeIds::Uint64> { using Type = TInstant; };
        struct Request : Column<3, NScheme::NTypeIds::String> {};
        struct Response : Column<4, NScheme::NTypeIds::String> {};
        struct ExecutionTime : Column<5, NScheme::NTypeIds::Uint64> { using Type = TDuration; static constexpr Type Default = TDuration::Zero(); };

        using TKey = TableKey<Index>;
        using TColumns = TableColumns<Index, Timestamp, Request, Response, ExecutionTime>;
    };

    struct MigrationPlan : Table<126> {
        enum EState : ui32 {
            CREATED,
            ACTIVE,
            PAUSED,
            FINISHED
        };

        struct Name : Column<1, NScheme::NTypeIds::String> {};
        struct State : Column<2, NScheme::NTypeIds::Uint32> { using Type = EState; };
        struct Size : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Done : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<Name, State, Size, Done>;
    };

    struct MigrationEntry : Table<127> {
        struct PlanName : Column<1, MigrationPlan::Name::ColumnType> {};
        struct EntryIndex : Column<2, NScheme::NTypeIds::Uint64> {};
        struct GroupId : Column<3, NScheme::NTypeIds::Uint32> {};
        struct OriginNodeId : Column<4, NScheme::NTypeIds::Uint32> {};
        struct OriginPDiskId : Column<5, NScheme::NTypeIds::Uint32> {};
        struct OriginVSlotId : Column<6, NScheme::NTypeIds::Uint32> {};
        struct TargetNodeId : Column<7, NScheme::NTypeIds::Uint32> {};
        struct TargetPDiskId : Column<8, NScheme::NTypeIds::Uint32> {};
        struct Done : Column<9, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<PlanName, EntryIndex>;
        using TColumns = TableColumns<PlanName, EntryIndex, GroupId, OriginNodeId,
                OriginPDiskId, OriginVSlotId, TargetNodeId, TargetPDiskId, Done>;
    };

    struct ScrubState : Table<128> {
        struct NodeId : Column<1, VSlot::NodeID::ColumnType> {};
        struct PDiskId : Column<2, VSlot::PDiskID::ColumnType> {};
        struct VSlotId : Column<3, VSlot::VSlotID::ColumnType> {};
        struct State : Column<5, NScheme::NTypeIds::String> {};
        struct ScrubCycleStartTime : Column<6, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };
        struct ScrubCycleFinishTime : Column<8, NScheme::NTypeIds::Uint64> { using Type = TInstant; static constexpr Type Default = TInstant::Zero(); };
        struct Success : Column<7, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };

        using TKey = TableKey<NodeId, PDiskId, VSlotId>;
        using TColumns = TableColumns<NodeId, PDiskId, VSlotId, State, ScrubCycleStartTime, ScrubCycleFinishTime, Success>;
    };

    struct DriveSerial : Table<129> {
        struct Serial : Column<1, NScheme::NTypeIds::String> {}; // PK
        struct BoxId : Column<2, Box::BoxId::ColumnType> {};
        struct NodeId : Column<3, Node::ID::ColumnType> {}; // FK PDisk.NodeID
        struct PDiskId : Column<4, Node::NextPDiskID::ColumnType> {}; // FK PDisk.PDiskID
        struct Guid : Column<5, PDisk::Guid::ColumnType> {}; // Check-only column for PDisk.Guid
        struct LifeStage : Column<6, NScheme::NTypeIds::Uint32> { using Type = NKikimrBlobStorage::TDriveLifeStage::E; };
        struct Kind : Column<7, HostConfigDrive::Kind::ColumnType> {};
        struct PDiskType : Column<8, HostConfigDrive::TypeCol::ColumnType> { using Type = NKikimrBlobStorage::EPDiskType; };
        struct PDiskConfig : Column<9, NScheme::NTypeIds::String> {};
        struct Path : Column<10, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Serial>;
        using TColumns = TableColumns<Serial, BoxId, NodeId, PDiskId, Guid, LifeStage, Kind, PDiskType, PDiskConfig, Path>;
    };

    // struct VirtualGroupPool : Table<130> {};

    struct BlobDepotDeleteQueue : Table<131> {
        struct GroupId : Column<1, NScheme::NTypeIds::Uint32> {}; // PK
        struct HiveId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct BlobDepotId : Column<3, NScheme::NTypeIds::Uint64> {}; // may be null if creation wasn't confirmed

        using TKey = TableKey<GroupId>;
        using TColumns = TableColumns<GroupId, HiveId, BlobDepotId>;
    };

    using TTables = SchemaTables<
        Node,
        PDisk,
        Group,
        State,
        VSlot,
        VDiskMetrics,
        PDiskMetrics,
        GroupLatencies,
        Box,
        BoxUser,
        HostConfig,
        HostConfigDrive,
        BoxHostV2,
        BoxStoragePool,
        BoxStoragePoolUser,
        BoxStoragePoolPDiskFilter,
        GroupStoragePool,
        OperationLog,
        MigrationPlan,
        MigrationEntry,
        ScrubState,
        DriveSerial,
        BlobDepotDeleteQueue
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
    >;
};

} // NBsController
} // NKikimr
