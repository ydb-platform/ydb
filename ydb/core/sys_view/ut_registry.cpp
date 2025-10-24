#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/sys_view/common/registry.h>

namespace NKikimr {
namespace NSysView {

template <typename Schema>
extern void FillSchema(ISystemViewResolver::TSchema& schema);

namespace {

using NKikimrSysView::ESysViewType;
using ESource = ISystemViewResolver::ESource;

// SCHEMAS MODIFICATION POLICY:
// ---------------------------
//  These schemas are CANONICAL and subject to strict modification rules:
//
//  FORBIDDEN:
//  - Changing column data types
//  - Renaming existing columns
//  - Removing columns from the primary key
//  - Deleting existing columns
//
//  ALLOWED:
//  - Adding new columns (use the next available sequential ID)
//  - Adding new schema definitions
//
//  When adding columns, maintain sequential Column IDs and update the TColumns list.
//  When adding tables, maintain sequential Table IDs .

struct Schema : NIceDb::Schema {
    struct PartitionStats : Table<1> {
        struct OwnerId                  : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PathId                   : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PartIdx                  : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DataSize                 : Column<4, NScheme::NTypeIds::Uint64> {};
        struct RowCount                 : Column<5, NScheme::NTypeIds::Uint64> {};
        struct IndexSize                : Column<6, NScheme::NTypeIds::Uint64> {};
        struct CPUCores                 : Column<7, NScheme::NTypeIds::Double> {};
        struct TabletId                 : Column<8, NScheme::NTypeIds::Uint64> {};
        struct Path                     : Column<9, NScheme::NTypeIds::Utf8> {};
        struct NodeId                   : Column<10, NScheme::NTypeIds::Uint32> {};
        struct StartTime                : Column<11, NScheme::NTypeIds::Timestamp> {};
        struct AccessTime               : Column<12, NScheme::NTypeIds::Timestamp> {};
        struct UpdateTime               : Column<13, NScheme::NTypeIds::Timestamp> {};
        struct InFlightTxCount          : Column<14, NScheme::NTypeIds::Uint32> {};
        struct RowUpdates               : Column<15, NScheme::NTypeIds::Uint64> {};
        struct RowDeletes               : Column<16, NScheme::NTypeIds::Uint64> {};
        struct RowReads                 : Column<17, NScheme::NTypeIds::Uint64> {};
        struct RangeReads               : Column<18, NScheme::NTypeIds::Uint64> {};
        struct RangeReadRows            : Column<19, NScheme::NTypeIds::Uint64> {};
        struct ImmediateTxCompleted     : Column<20, NScheme::NTypeIds::Uint64> {};
        struct CoordinatedTxCompleted   : Column<21, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedByOverload     : Column<22, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedByOutOfStorage : Column<23, NScheme::NTypeIds::Uint64> {};
        struct LastTtlRunTime           : Column<24, NScheme::NTypeIds::Timestamp> {};
        struct LastTtlRowsProcessed     : Column<25, NScheme::NTypeIds::Uint64> {};
        struct LastTtlRowsErased        : Column<26, NScheme::NTypeIds::Uint64> {};
        struct FollowerId               : Column<27, NScheme::NTypeIds::Uint32> {};
        struct LocksAcquired            : Column<28, NScheme::NTypeIds::Uint64> {};
        struct LocksWholeShard          : Column<29, NScheme::NTypeIds::Uint64> {};
        struct LocksBroken              : Column<30, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerId, PathId, PartIdx, FollowerId>;
        using TColumns = TableColumns<
            OwnerId,
            PathId,
            PartIdx,
            DataSize,
            RowCount,
            IndexSize,
            CPUCores,
            TabletId,
            Path,
            NodeId,
            AccessTime,
            UpdateTime,
            StartTime,
            InFlightTxCount,
            RowUpdates,
            RowDeletes,
            RowReads,
            RangeReads,
            RangeReadRows,
            ImmediateTxCompleted,
            CoordinatedTxCompleted,
            TxRejectedByOverload,
            TxRejectedByOutOfStorage,
            LastTtlRunTime,
            LastTtlRowsProcessed,
            LastTtlRowsErased,
            FollowerId,
            LocksAcquired,
            LocksWholeShard,
            LocksBroken
            >;
    };

    struct Nodes : Table<2> {
        struct NodeId     : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Address    : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Host       : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Port       : Column<4, NScheme::NTypeIds::Uint32> {};
        struct StartTime  : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct UpTime     : Column<6, NScheme::NTypeIds::Interval> {};
        struct CpuThreads : Column<7, NScheme::NTypeIds::Uint32> {};
        struct CpuUsage   : Column<8, NScheme::NTypeIds::Double> {};
        struct CpuIdle    : Column<9, NScheme::NTypeIds::Double> {};

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<
            NodeId,
            Host,
            Address,
            Port,
            StartTime,
            UpTime,
            CpuThreads,
            CpuUsage,
            CpuIdle>;
    };

    struct QueryStats : Table<3> {
        struct IntervalEnd       : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank              : Column<2, NScheme::NTypeIds::Uint32> {};
        struct QueryText         : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Duration          : Column<4, NScheme::NTypeIds::Interval> {};
        struct EndTime           : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct ReadRows          : Column<6, NScheme::NTypeIds::Uint64> {};
        struct ReadBytes         : Column<7, NScheme::NTypeIds::Uint64> {};
        struct UpdateRows        : Column<8, NScheme::NTypeIds::Uint64> {};
        struct UpdateBytes       : Column<9, NScheme::NTypeIds::Uint64> {};
        struct DeleteRows        : Column<10, NScheme::NTypeIds::Uint64> {};
        struct DeleteBytes       : Column<11, NScheme::NTypeIds::Uint64> {}; // deprecated, always 0
        struct Partitions        : Column<12, NScheme::NTypeIds::Uint64> {};
        struct UserSID           : Column<13, NScheme::NTypeIds::Utf8> {};
        struct ParametersSize    : Column<14, NScheme::NTypeIds::Uint64> {};
        struct CompileDuration   : Column<15, NScheme::NTypeIds::Interval> {};
        struct FromQueryCache    : Column<16, NScheme::NTypeIds::Bool> {};
        struct CPUTime           : Column<17, NScheme::NTypeIds::Uint64> {};
        struct ShardCount        : Column<18, NScheme::NTypeIds::Uint64> {};
        struct SumShardCPUTime   : Column<19, NScheme::NTypeIds::Uint64> {};
        struct MinShardCPUTime   : Column<20, NScheme::NTypeIds::Uint64> {};
        struct MaxShardCPUTime   : Column<21, NScheme::NTypeIds::Uint64> {};
        struct ComputeNodesCount : Column<22, NScheme::NTypeIds::Uint64> {};
        struct SumComputeCPUTime : Column<23, NScheme::NTypeIds::Uint64> {};
        struct MinComputeCPUTime : Column<24, NScheme::NTypeIds::Uint64> {};
        struct MaxComputeCPUTime : Column<25, NScheme::NTypeIds::Uint64> {};
        struct CompileCPUTime    : Column<26, NScheme::NTypeIds::Uint64> {};
        struct ProcessCPUTime    : Column<27, NScheme::NTypeIds::Uint64> {};
        struct TypeCol           : Column<28, NScheme::NTypeIds::Utf8> { static TString GetColumnName(const TString&) { return "Type"; } };
        struct RequestUnits      : Column<29, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<IntervalEnd, Rank>;
        using TColumns = TableColumns<
            IntervalEnd,
            Rank,
            QueryText,
            Duration,
            EndTime,
            ReadRows,
            ReadBytes,
            UpdateRows,
            UpdateBytes,
            DeleteRows,
            DeleteBytes,
            Partitions,
            UserSID,
            ParametersSize,
            CompileDuration,
            FromQueryCache,
            CPUTime,
            ShardCount,
            SumShardCPUTime,
            MinShardCPUTime,
            MaxShardCPUTime,
            ComputeNodesCount,
            SumComputeCPUTime,
            MinComputeCPUTime,
            MaxComputeCPUTime,
            CompileCPUTime,
            ProcessCPUTime,
            TypeCol,
            RequestUnits>;
    };

    struct PDisks : Table<4> {
        struct NodeId                          : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PDiskId                         : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TypeCol                         : Column<3, NScheme::NTypeIds::Utf8> { static TString GetColumnName(const TString&) { return "Type"; } };
        struct Kind                            : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Path                            : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Guid                            : Column<6, NScheme::NTypeIds::Uint64> {};
        struct BoxId                           : Column<7, NScheme::NTypeIds::Uint32> {};
        struct SharedWithOS                    : Column<8, NScheme::NTypeIds::Bool> {};
        struct ReadCentric                     : Column<9, NScheme::NTypeIds::Bool> {};
        struct AvailableSize                   : Column<10, NScheme::NTypeIds::Uint64> {};
        struct TotalSize                       : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Status                          : Column<12, NScheme::NTypeIds::Utf8> {};
        struct StatusChangeTimestamp           : Column<14, NScheme::NTypeIds::Timestamp> {};
        struct ExpectedSlotCount               : Column<15, NScheme::NTypeIds::Uint32> {};
        struct NumActiveSlots                  : Column<16, NScheme::NTypeIds::Uint32> {};
        struct DecommitStatus                  : Column<17, NScheme::NTypeIds::Utf8> {};
        struct State                           : Column<18, NScheme::NTypeIds::Utf8> {};
        struct SlotSizeInUnits                 : Column<19, NScheme::NTypeIds::Uint32> {};
        struct InferPDiskSlotCountFromUnitSize : Column<20, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId, PDiskId>;
        using TColumns = TableColumns<
            NodeId,
            PDiskId,
            TypeCol,
            Kind,
            Path,
            Guid,
            BoxId,
            SharedWithOS,
            ReadCentric,
            AvailableSize,
            TotalSize,
            Status,
            State,
            StatusChangeTimestamp,
            ExpectedSlotCount,
            NumActiveSlots,
            DecommitStatus,
            SlotSizeInUnits,
            InferPDiskSlotCountFromUnitSize>;
    };

    struct VSlots : Table<5> {
        struct NodeId          : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PDiskId         : Column<2, NScheme::NTypeIds::Uint32> {};
        struct VSlotId         : Column<3, NScheme::NTypeIds::Uint32> {};
        struct GroupId         : Column<5, NScheme::NTypeIds::Uint32> {};
        struct GroupGeneration : Column<6, NScheme::NTypeIds::Uint32> {};
        struct FailDomain      : Column<8, NScheme::NTypeIds::Uint32> {};
        struct VDisk           : Column<9, NScheme::NTypeIds::Uint32> {};
        struct AllocatedSize   : Column<10, NScheme::NTypeIds::Uint64> {};
        struct AvailableSize   : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Status          : Column<12, NScheme::NTypeIds::Utf8> {};
        struct Kind            : Column<14, NScheme::NTypeIds::Utf8> {};
        struct FailRealm       : Column<15, NScheme::NTypeIds::Uint32> {};
        struct Replicated      : Column<16, NScheme::NTypeIds::Bool> {};
        struct DiskSpace       : Column<17, NScheme::NTypeIds::Utf8> {};
        struct State           : Column <18, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<NodeId, PDiskId, VSlotId>;
        using TColumns = TableColumns<
            NodeId,
            PDiskId,
            VSlotId,
            GroupId,
            GroupGeneration,
            FailDomain,
            VDisk,
            AllocatedSize,
            AvailableSize,
            Status,
            State,
            Kind,
            FailRealm,
            Replicated,
            DiskSpace>;
    };

    struct Groups : Table<6> {
        struct GroupId                       : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Generation                    : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ErasureSpecies                : Column<3, NScheme::NTypeIds::Utf8> {};
        struct BoxId                         : Column<4, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId                 : Column<5, NScheme::NTypeIds::Uint64> {};
        struct EncryptionMode                : Column<6, NScheme::NTypeIds::Uint32> {};
        struct LifeCyclePhase                : Column<7, NScheme::NTypeIds::Uint32> {};
        struct AllocatedSize                 : Column<8, NScheme::NTypeIds::Uint64> {};
        struct AvailableSize                 : Column<9, NScheme::NTypeIds::Uint64> {};
        struct SeenOperational               : Column<12, NScheme::NTypeIds::Bool> {};
        struct PutTabletLogLatency           : Column<13, NScheme::NTypeIds::Interval> {};
        struct PutUserDataLatency            : Column<14, NScheme::NTypeIds::Interval> {};
        struct GetFastLatency                : Column<15, NScheme::NTypeIds::Interval> {};
        struct LayoutCorrect                 : Column<16, NScheme::NTypeIds::Bool> {};
        struct OperatingStatus               : Column<17, NScheme::NTypeIds::Utf8> {};
        struct ExpectedStatus                : Column<18, NScheme::NTypeIds::Utf8> {};
        struct ProxyGroupId                  : Column<19, NScheme::NTypeIds::Uint32> {};
        struct BridgePileId                  : Column<20, NScheme::NTypeIds::Uint32> {};
        struct GroupSizeInUnits              : Column<21, NScheme::NTypeIds::Uint32> {};
        struct BridgeSyncStage               : Column<22, NScheme::NTypeIds::Utf8> {};
        struct BridgeDataSyncProgress        : Column<23, NScheme::NTypeIds::Double> {};
        struct BridgeDataSyncErrors          : Column<24, NScheme::NTypeIds::Bool> {};
        struct BridgeSyncLastError           : Column<25, NScheme::NTypeIds::Utf8> {};
        struct BridgeSyncLastErrorTimestamp  : Column<26, NScheme::NTypeIds::Uint64> {};
        struct BridgeSyncFirstErrorTimestamp : Column<27, NScheme::NTypeIds::Uint64> {};
        struct BridgeSyncErrorCount          : Column<28, NScheme::NTypeIds::Uint32> {};
        struct BridgeSyncRunning             : Column<29, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<GroupId>;
        using TColumns = TableColumns<
            GroupId,
            Generation,
            ErasureSpecies,
            BoxId,
            StoragePoolId,
            EncryptionMode,
            LifeCyclePhase,
            AllocatedSize,
            AvailableSize,
            SeenOperational,
            PutTabletLogLatency,
            PutUserDataLatency,
            GetFastLatency,
            LayoutCorrect,
            OperatingStatus,
            ExpectedStatus,
            ProxyGroupId,
            BridgePileId,
            GroupSizeInUnits,
            BridgeSyncStage,
            BridgeDataSyncProgress,
            BridgeDataSyncErrors,
            BridgeSyncLastError,
            BridgeSyncLastErrorTimestamp,
            BridgeSyncFirstErrorTimestamp,
            BridgeSyncErrorCount,
            BridgeSyncRunning>;
    };

    struct StoragePools : Table<7> {
        struct BoxId                   : Column<1, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId           : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Name                    : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Generation              : Column<4, NScheme::NTypeIds::Uint64> {};
        struct ErasureSpecies          : Column<5, NScheme::NTypeIds::Utf8> {};
        struct VDiskKind               : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Kind                    : Column<7, NScheme::NTypeIds::Utf8> {};
        struct NumGroups               : Column<8, NScheme::NTypeIds::Uint32> {};
        struct EncryptionMode          : Column<9, NScheme::NTypeIds::Uint32> {};
        struct SchemeshardId           : Column<10, NScheme::NTypeIds::Uint64> {};
        struct PathId                  : Column<11, NScheme::NTypeIds::Uint64> {};
        struct DefaultGroupSizeInUnits : Column<12, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<BoxId, StoragePoolId>;
        using TColumns = TableColumns<
            BoxId,
            StoragePoolId,
            Name,
            Generation,
            ErasureSpecies,
            VDiskKind,
            Kind,
            NumGroups,
            EncryptionMode,
            SchemeshardId,
            PathId,
            DefaultGroupSizeInUnits>;
    };

    struct Tablets : Table<8> {
        struct TabletId      : Column<1, NScheme::NTypeIds::Uint64> {};
        struct FollowerId    : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TypeCol       : Column<3, NScheme::NTypeIds::Utf8> { static TString GetColumnName(const TString&) { return "Type"; } };
        struct State         : Column<4, NScheme::NTypeIds::Utf8> {};
        struct VolatileState : Column<5, NScheme::NTypeIds::Utf8> {};
        struct BootState     : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Generation    : Column<7, NScheme::NTypeIds::Uint32> {};
        struct NodeId        : Column<8, NScheme::NTypeIds::Uint32> {};
        struct CPU           : Column<9, NScheme::NTypeIds::Double> {};
        struct Memory        : Column<10, NScheme::NTypeIds::Uint64> {};
        struct Network       : Column<11, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TabletId, FollowerId>;
        using TColumns = TableColumns<
            TabletId,
            FollowerId,
            TypeCol,
            State,
            VolatileState,
            BootState,
            Generation,
            NodeId,
            CPU,
            Memory,
            Network>;
    };

    struct QueryMetrics : Table<9> {
        struct IntervalEnd     : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank            : Column<2, NScheme::NTypeIds::Uint32> {};
        struct QueryText       : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Count           : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SumCPUTime      : Column<5, NScheme::NTypeIds::Uint64> {};
        struct MinCPUTime      : Column<6, NScheme::NTypeIds::Uint64> {};
        struct MaxCPUTime      : Column<7, NScheme::NTypeIds::Uint64> {};
        struct SumDuration     : Column<8, NScheme::NTypeIds::Interval> {};
        struct MinDuration     : Column<9, NScheme::NTypeIds::Interval> {};
        struct MaxDuration     : Column<10, NScheme::NTypeIds::Interval> {};
        struct MinReadRows     : Column<11, NScheme::NTypeIds::Uint64> {};
        struct MaxReadRows     : Column<12, NScheme::NTypeIds::Uint64> {};
        struct SumReadRows     : Column<13, NScheme::NTypeIds::Uint64> {};
        struct MinReadBytes    : Column<14, NScheme::NTypeIds::Uint64> {};
        struct MaxReadBytes    : Column<15, NScheme::NTypeIds::Uint64> {};
        struct SumReadBytes    : Column<16, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateRows   : Column<17, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateRows   : Column<18, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateRows   : Column<19, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateBytes  : Column<20, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateBytes  : Column<21, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateBytes  : Column<22, NScheme::NTypeIds::Uint64> {};
        struct MinDeleteRows   : Column<23, NScheme::NTypeIds::Uint64> {};
        struct MaxDeleteRows   : Column<24, NScheme::NTypeIds::Uint64> {};
        struct SumDeleteRows   : Column<25, NScheme::NTypeIds::Uint64> {};
        struct MinRequestUnits : Column<26, NScheme::NTypeIds::Uint64> {};
        struct MaxRequestUnits : Column<27, NScheme::NTypeIds::Uint64> {};
        struct SumRequestUnits : Column<28, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<IntervalEnd, Rank>;
        using TColumns = TableColumns<
            IntervalEnd,
            Rank,
            QueryText,
            Count,
            SumCPUTime, MinCPUTime, MaxCPUTime,
            SumDuration, MinDuration, MaxDuration,
            SumReadRows, MinReadRows, MaxReadRows,
            SumReadBytes, MinReadBytes, MaxReadBytes,
            SumUpdateRows, MinUpdateRows, MaxUpdateRows,
            SumUpdateBytes, MinUpdateBytes, MaxUpdateBytes,
            SumDeleteRows, MinDeleteRows, MaxDeleteRows,
            SumRequestUnits, MinRequestUnits, MaxRequestUnits>;
    };

    struct StorageStats : Table<10> {
        struct PDiskFilter             : Column<2, NScheme::NTypeIds::Utf8> {};
        struct ErasureSpecies          : Column<3, NScheme::NTypeIds::Utf8> {};
        struct CurrentGroupsCreated    : Column<4, NScheme::NTypeIds::Uint32> {};
        struct CurrentAllocatedSize    : Column<5, NScheme::NTypeIds::Uint64> {};
        struct CurrentAvailableSize    : Column<6, NScheme::NTypeIds::Uint64> {};
        struct AvailableGroupsToCreate : Column<7, NScheme::NTypeIds::Uint32> {};
        struct AvailableSizeToCreate   : Column<8, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PDiskFilter, ErasureSpecies>;
        using TColumns = TableColumns<PDiskFilter, ErasureSpecies, CurrentGroupsCreated, CurrentAllocatedSize,
                                      CurrentAvailableSize, AvailableGroupsToCreate, AvailableSizeToCreate>;
    };

    struct TopPartitions : Table<11> {
        struct IntervalEnd     : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank            : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TabletId        : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Path            : Column<4, NScheme::NTypeIds::Utf8> {};
        struct PeakTime        : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct CPUCores        : Column<6, NScheme::NTypeIds::Double> {};
        struct NodeId          : Column<7, NScheme::NTypeIds::Uint32> {};
        struct DataSize        : Column<8, NScheme::NTypeIds::Uint64> {};
        struct RowCount        : Column<9, NScheme::NTypeIds::Uint64> {};
        struct IndexSize       : Column<10, NScheme::NTypeIds::Uint64> {};
        struct InFlightTxCount : Column<11, NScheme::NTypeIds::Uint32> {};
        struct FollowerId      : Column<12, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<IntervalEnd, Rank>;
        using TColumns = TableColumns<
            IntervalEnd,
            Rank,
            TabletId,
            Path,
            PeakTime,
            CPUCores,
            NodeId,
            DataSize,
            RowCount,
            IndexSize,
            InFlightTxCount,
            FollowerId>;
    };

    struct QuerySessions : Table<12> {
        struct SessionId          : Column<1, NScheme::NTypeIds::Utf8> {};
        struct NodeId             : Column<2, NScheme::NTypeIds::Uint32> {};
        struct State              : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Query              : Column<4, NScheme::NTypeIds::Utf8> {};
        struct QueryCount         : Column<5, NScheme::NTypeIds::Uint32> {};
        struct ClientAddress      : Column<6, NScheme::NTypeIds::Utf8> {};
        struct ClientPID          : Column<7, NScheme::NTypeIds::Utf8> {};
        struct ClientUserAgent    : Column<8, NScheme::NTypeIds::Utf8> {};
        struct ClientSdkBuildInfo : Column<9, NScheme::NTypeIds::Utf8> {};
        struct ApplicationName    : Column<10, NScheme::NTypeIds::Utf8> {};
        struct SessionStartAt     : Column<11, NScheme::NTypeIds::Timestamp> {};
        struct QueryStartAt       : Column<12, NScheme::NTypeIds::Timestamp> {};
        struct StateChangeAt      : Column<13, NScheme::NTypeIds::Timestamp> {};
        struct UserSID            : Column<14, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<SessionId>;
        using TColumns = TableColumns<
            SessionId,
            NodeId,
            State,
            Query,
            QueryCount,
            ClientAddress,
            ClientPID,
            ClientUserAgent,
            ClientSdkBuildInfo,
            ApplicationName,
            SessionStartAt,
            QueryStartAt,
            StateChangeAt,
            UserSID>;
    };

    struct AuthUsers : Table<13> {
        struct Sid                     : Column<1, NScheme::NTypeIds::Utf8> {};
        struct IsEnabled               : Column<2, NScheme::NTypeIds::Bool> {};
        struct IsLockedOut             : Column<3, NScheme::NTypeIds::Bool> {};
        struct CreatedAt               : Column<4, NScheme::NTypeIds::Timestamp> {};
        struct LastSuccessfulAttemptAt : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct LastFailedAttemptAt     : Column<6, NScheme::NTypeIds::Timestamp> {};
        struct FailedAttemptCount      : Column<7, NScheme::NTypeIds::Uint32> {};
        struct PasswordHash            : Column<8, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Sid>;
        using TColumns = TableColumns<
            Sid,
            IsEnabled,
            IsLockedOut,
            CreatedAt,
            LastSuccessfulAttemptAt,
            LastFailedAttemptAt,
            FailedAttemptCount,
            PasswordHash
        >;
    };

    struct AuthGroups : Table<14> {
        struct Sid : Column<1, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Sid>;
        using TColumns = TableColumns<
            Sid
        >;
    };

    struct AuthGroupMembers : Table<15> {
        struct GroupSid  : Column<1, NScheme::NTypeIds::Utf8> {};
        struct MemberSid : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<GroupSid, MemberSid>;
        using TColumns = TableColumns<
            GroupSid,
            MemberSid
        >;
    };

    struct AuthOwners : Table<16> {
        struct Path : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Sid  : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path>;
        using TColumns = TableColumns<
            Path,
            Sid
        >;
    };

    struct AuthPermissions : Table<17> {
        struct Path       : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Sid        : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Permission : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path, Sid, Permission>;
        using TColumns = TableColumns<
            Path,
            Sid,
            Permission
        >;
    };

    struct ResourcePoolClassifiers : Table<18> {
        struct Name         : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Rank         : Column<2, NScheme::NTypeIds::Int64> {};
        struct MemberName   : Column<4, NScheme::NTypeIds::Utf8> {};
        struct ResourcePool : Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<
            Name,
            Rank,
            MemberName,
            ResourcePool>;
    };

    struct ShowCreate : Table<19> {
        struct Path        : Column<1, NScheme::NTypeIds::Utf8> {};
        struct CreateQuery : Column<2, NScheme::NTypeIds::Utf8> {};
        struct PathType    : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path, PathType>;
        using TColumns = TableColumns<
            Path,
            CreateQuery,
            PathType
        >;
    };

    struct ResourcePools : Table<20> {
        struct Name                           : Column<1, NScheme::NTypeIds::Utf8> {};
        struct ConcurrentQueryLimit           : Column<2, NScheme::NTypeIds::Int32> {};
        struct QueueSize                      : Column<3, NScheme::NTypeIds::Int32> {};
        struct DatabaseLoadCpuThreshold       : Column<4, NScheme::NTypeIds::Double> {};
        struct ResourceWeight                 : Column<5, NScheme::NTypeIds::Double> {};
        struct TotalCpuLimitPercentPerNode    : Column<6, NScheme::NTypeIds::Double> {};
        struct QueryCpuLimitPercentPerNode    : Column<7, NScheme::NTypeIds::Double> {};
        struct QueryMemoryLimitPercentPerNode : Column<8, NScheme::NTypeIds::Double> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<
            Name,
            ConcurrentQueryLimit,
            QueueSize,
            DatabaseLoadCpuThreshold,
            ResourceWeight,
            TotalCpuLimitPercentPerNode,
            QueryCpuLimitPercentPerNode,
            QueryMemoryLimitPercentPerNode>;
    };

    struct TopPartitionsTli : Table<21> {
        struct IntervalEnd     : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank            : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TabletId        : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Path            : Column<4, NScheme::NTypeIds::Utf8> {};
        struct LocksAcquired   : Column<5, NScheme::NTypeIds::Uint64> {};
        struct LocksWholeShard : Column<6, NScheme::NTypeIds::Uint64> {};
        struct LocksBroken     : Column<7, NScheme::NTypeIds::Uint64> {};
        struct NodeId          : Column<8, NScheme::NTypeIds::Uint32> {};
        struct DataSize        : Column<9, NScheme::NTypeIds::Uint64> {};
        struct RowCount        : Column<10, NScheme::NTypeIds::Uint64> {};
        struct IndexSize       : Column<11, NScheme::NTypeIds::Uint64> {};
        struct FollowerId      : Column<12, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<IntervalEnd, Rank>;
        using TColumns = TableColumns<
            IntervalEnd,
            Rank,
            TabletId,
            Path,
            LocksAcquired,
            LocksWholeShard,
            LocksBroken,
            NodeId,
            DataSize,
            RowCount,
            IndexSize,
            FollowerId>;
    };

    struct PrimaryIndexStats : Table<22> {
        struct PathId           : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Kind             : Column<2, NScheme::NTypeIds::Utf8> {};
        struct TabletId         : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Rows             : Column<4, NScheme::NTypeIds::Uint64> {};
        struct RawBytes         : Column<5, NScheme::NTypeIds::Uint64> {};
        struct PortionId        : Column<6, NScheme::NTypeIds::Uint64> {};
        struct ChunkIdx         : Column<7, NScheme::NTypeIds::Uint64> {};
        struct EntityName       : Column<8, NScheme::NTypeIds::Utf8> {};
        struct InternalEntityId : Column<9, NScheme::NTypeIds::Uint32> {};
        struct BlobId           : Column<10, NScheme::NTypeIds::Utf8> {};
        struct BlobRangeOffset  : Column<11, NScheme::NTypeIds::Uint64> {};
        struct BlobRangeSize    : Column<12, NScheme::NTypeIds::Uint64> {};
        struct Activity         : Column<13, NScheme::NTypeIds::Uint8> {};
        struct TierName         : Column<14, NScheme::NTypeIds::Utf8> {};
        struct EntityType       : Column<15, NScheme::NTypeIds::Utf8> {};
        struct ChunkDetails     : Column<16, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, TabletId, PortionId, InternalEntityId, ChunkIdx>;
        using TColumns = TableColumns<
            PathId,
            Kind,
            TabletId,
            Rows,
            RawBytes,
            PortionId,
            ChunkIdx,
            EntityName,
            InternalEntityId,
            BlobId,
            BlobRangeOffset,
            BlobRangeSize,
            Activity,
            TierName,
            EntityType,
            ChunkDetails>;
    };

    struct PrimaryIndexSchemaStats : Table<23> {
        struct TabletId               : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PresetId               : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SchemaVersion          : Column<3, NScheme::NTypeIds::Uint64> {};
        struct SchemaSnapshotPlanStep : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SchemaSnapshotTxId     : Column<5, NScheme::NTypeIds::Uint64> {};
        struct SchemaDetails          : Column<6, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<TabletId, PresetId, SchemaVersion>;
        using TColumns = TableColumns<
            TabletId,
            PresetId,
            SchemaVersion,
            SchemaSnapshotPlanStep,
            SchemaSnapshotTxId,
            SchemaDetails>;
    };

    struct PrimaryIndexPortionStats : Table<24> {
        struct PathId          : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Kind            : Column<2, NScheme::NTypeIds::Utf8> {};
        struct TabletId        : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Rows            : Column<4, NScheme::NTypeIds::Uint64> {};
        struct ColumnRawBytes  : Column<5, NScheme::NTypeIds::Uint64> {};
        struct IndexRawBytes   : Column<6, NScheme::NTypeIds::Uint64> {};
        struct ColumnBlobBytes : Column<7, NScheme::NTypeIds::Uint64> {};
        struct IndexBlobBytes  : Column<8, NScheme::NTypeIds::Uint64> {};
        struct PortionId       : Column<9, NScheme::NTypeIds::Uint64> {};
        struct Activity        : Column<10, NScheme::NTypeIds::Uint8> {};
        struct TierName        : Column<11, NScheme::NTypeIds::Utf8> {};
        struct Stats           : Column<12, NScheme::NTypeIds::Utf8> {};
        struct Optimized       : Column<13, NScheme::NTypeIds::Uint8> {};
        struct CompactionLevel : Column<14, NScheme::NTypeIds::Uint64> {};
        struct Details         : Column<15, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, TabletId, PortionId>;
        using TColumns = TableColumns<
            PathId,
            Kind,
            TabletId,
            Rows,
            ColumnRawBytes,
            IndexRawBytes,
            ColumnBlobBytes,
            IndexBlobBytes,
            PortionId,
            Activity,
            TierName,
            Stats,
            Optimized,
            CompactionLevel,
            Details>;
    };

    struct PrimaryIndexGranuleStats : Table<25> {
        struct PathId         : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId       : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PortionsCount  : Column<3, NScheme::NTypeIds::Uint64> {};
        struct HostName       : Column<4, NScheme::NTypeIds::Utf8> {};
        struct NodeId         : Column<5, NScheme::NTypeIds::Uint64> {};
        struct InternalPathId : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, TabletId>;
        using TColumns = TableColumns<
            PathId,
            TabletId,
            PortionsCount,
            HostName,
            NodeId,
            InternalPathId>;
    };

    struct PrimaryIndexOptimizerStats: Table<26> {
        struct PathId   : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct TaskId   : Column<3, NScheme::NTypeIds::Uint64> {};
        struct HostName : Column<4, NScheme::NTypeIds::Utf8> {};
        struct NodeId   : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Start    : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Finish   : Column<7, NScheme::NTypeIds::Utf8> {};
        struct Details  : Column<8, NScheme::NTypeIds::Utf8> {};
        struct Category : Column<9, NScheme::NTypeIds::Uint64> {};
        struct Weight   : Column<10, NScheme::NTypeIds::Int64> {};

        using TKey = TableKey<PathId, TabletId, TaskId>;
        using TColumns = TableColumns<
            PathId,
            TabletId,
            TaskId,
            HostName,
            NodeId,
            Start,
            Finish,
            Details,
            Category,
            Weight>;
    };

    struct StreamingQueries : Table<27> {
        struct Path                 : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Status               : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Issues               : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Plan                 : Column<4, NScheme::NTypeIds::Utf8> {};
        struct Ast                  : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Text                 : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Run                  : Column<7, NScheme::NTypeIds::Bool> {};
        struct ResourcePool         : Column<8, NScheme::NTypeIds::Utf8> {};
        struct RetryCount           : Column<9, NScheme::NTypeIds::Uint64> {};
        struct LastFailAt           : Column<10, NScheme::NTypeIds::Timestamp> {};
        struct SuspendedUntil       : Column<11, NScheme::NTypeIds::Timestamp> {};
        struct LastExecutionId      : Column<12, NScheme::NTypeIds::Utf8> {};
        struct PreviousExecutionIds : Column<13, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path>;
        using TColumns = TableColumns<
            Path,
            Status,
            Issues,
            Plan,
            Ast,
            Text,
            Run,
            ResourcePool,
            RetryCount,
            LastFailAt,
            SuspendedUntil,
            LastExecutionId,
            PreviousExecutionIds>;
    };
};


// SYSTEM VIEWS REGISTRY MODIFICATION POLICY:
// ---------------------------
//  These system view lists are CANONICAL and subject to strict modification rules:
//
//  FORBIDDEN:
//  - Changing sysview types (implementation ids)
//  - Renaming existing sysviews
//  - Adding sysviews with existing names and same source object types
//  - Changing sysview schemas
//  - Deleting existing sysviews
//  - Deleting related source objects of existing sysviews
//
//  ALLOWED:
//  - Adding new sysviews
//  - Adding new source object types of existing sysviews

const TVector<SysViewsRegistryRecord> SysViews = {
    {"partition_stats", ESysViewType::EPartitionStats, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::PartitionStats>},
    {"nodes", ESysViewType::ENodes, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::Nodes>},

    {"top_queries_by_duration_one_minute", ESysViewType::ETopQueriesByDurationOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_duration_one_hour", ESysViewType::ETopQueriesByDurationOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_read_bytes_one_minute", ESysViewType::ETopQueriesByReadBytesOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_read_bytes_one_hour", ESysViewType::ETopQueriesByReadBytesOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_cpu_time_one_minute", ESysViewType::ETopQueriesByCpuTimeOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_cpu_time_one_hour", ESysViewType::ETopQueriesByCpuTimeOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_request_units_one_minute", ESysViewType::ETopQueriesByRequestUnitsOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},
    {"top_queries_by_request_units_one_hour", ESysViewType::ETopQueriesByRequestUnitsOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryStats>},

    {"query_sessions", ESysViewType::EQuerySessions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QuerySessions>},
    {"query_metrics_one_minute", ESysViewType::EQueryMetricsOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::QueryMetrics>},

    {"ds_pdisks", ESysViewType::EPDisks, {ESource::Domain},  &FillSchema<Schema::PDisks>},
    {"ds_vslots", ESysViewType::EVSlots, {ESource::Domain},  &FillSchema<Schema::VSlots>},
    {"ds_groups", ESysViewType::EGroups, {ESource::Domain},  &FillSchema<Schema::Groups>},
    {"ds_storage_pools", ESysViewType::EStoragePools, {ESource::Domain},  &FillSchema<Schema::StoragePools>},
    {"ds_storage_stats", ESysViewType::EStorageStats, {ESource::Domain},  &FillSchema<Schema::StorageStats>},
    {"hive_tablets", ESysViewType::ETablets, {ESource::Domain},  &FillSchema<Schema::Tablets>},

    {"top_partitions_one_minute", ESysViewType::ETopPartitionsByCpuOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitions>},
    {"top_partitions_one_hour", ESysViewType::ETopPartitionsByCpuOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitions>},
    {"top_partitions_by_tli_one_minute", ESysViewType::ETopPartitionsByTliOneMinute, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitionsTli>},
    {"top_partitions_by_tli_one_hour", ESysViewType::ETopPartitionsByTliOneHour, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::TopPartitionsTli>},

    {"resource_pool_classifiers", ESysViewType::EResourcePoolClassifiers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::ResourcePoolClassifiers>},
    {"resource_pools", ESysViewType::EResourcePools, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::ResourcePools>},

    {"auth_users", ESysViewType::EAuthUsers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthUsers>},
    {"auth_groups", ESysViewType::EAuthGroups, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthGroups>},
    {"auth_group_members", ESysViewType::EAuthGroupMembers, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthGroupMembers>},
    {"auth_owners", ESysViewType::EAuthOwners, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthOwners>},
    {"auth_permissions", ESysViewType::EAuthPermissions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthPermissions>},
    {"auth_effective_permissions", ESysViewType::EAuthEffectivePermissions, {ESource::Domain, ESource::SubDomain},  &FillSchema<Schema::AuthPermissions>},

    {"store_primary_index_stats", ESysViewType::EStorePrimaryIndexStats, {ESource::OlapStore},  &FillSchema<Schema::PrimaryIndexStats>},
    {"store_primary_index_schema_stats", ESysViewType::EStorePrimaryIndexSchemaStats, {ESource::OlapStore},  &FillSchema<Schema::PrimaryIndexSchemaStats>},
    {"store_primary_index_portion_stats", ESysViewType::EStorePrimaryIndexPortionStats, {ESource::OlapStore},  &FillSchema<Schema::PrimaryIndexPortionStats>},
    {"store_primary_index_granule_stats", ESysViewType::EStorePrimaryIndexGranuleStats, {ESource::OlapStore},  &FillSchema<Schema::PrimaryIndexGranuleStats>},
    {"store_primary_index_optimizer_stats", ESysViewType::EStorePrimaryIndexOptimizerStats, {ESource::OlapStore},  &FillSchema<Schema::PrimaryIndexOptimizerStats>},

    {"primary_index_stats", ESysViewType::ETablePrimaryIndexStats, {ESource::ColumnTable},  &FillSchema<Schema::PrimaryIndexStats>},
    {"primary_index_schema_stats", ESysViewType::ETablePrimaryIndexSchemaStats, {ESource::ColumnTable},  &FillSchema<Schema::PrimaryIndexSchemaStats>},
    {"primary_index_portion_stats", ESysViewType::ETablePrimaryIndexPortionStats, {ESource::ColumnTable},  &FillSchema<Schema::PrimaryIndexPortionStats>},
    {"primary_index_granule_stats", ESysViewType::ETablePrimaryIndexGranuleStats, {ESource::ColumnTable},  &FillSchema<Schema::PrimaryIndexGranuleStats>},
    {"primary_index_optimizer_stats", ESysViewType::ETablePrimaryIndexOptimizerStats, {ESource::ColumnTable},  &FillSchema<Schema::PrimaryIndexOptimizerStats>},

    {"streaming_queries", ESysViewType::EStreamingQueries, {ESource::Domain, ESource::SubDomain}, &FillSchema<Schema::StreamingQueries>},
};

const TVector<SysViewsRegistryRecord> RewrittenSysViews = {
    {"show_create", ESysViewType::EShowCreate, {},  &FillSchema<Schema::ShowCreate>},
};

const THashMap<ESource, TStringBuf> SourceObjectTypeNames = {
    {ESource::Domain, "domain"},
    {ESource::SubDomain, "subdomain"},
    {ESource::OlapStore, "olap store"},
    {ESource::ColumnTable, "column table"}
};

struct TSysViewDescription {
    ESysViewType Type;
    ISystemViewResolver::TSchema Schema;
};

THashMap<ESource, THashMap<TStringBuf, TSysViewDescription>> GroupSysViewsBySource(const TVector<SysViewsRegistryRecord>& sysViews,
    TStringBuf registryName)
{
    THashMap<ESource, THashMap<TStringBuf, TSysViewDescription>> groupedSysViews;
    for (const auto& registryRecord : sysViews) {
        ISystemViewResolver::TSchema schema;
        registryRecord.FillSchemaFunc(schema);

        for (auto sourceObjectType : registryRecord.SourceObjectTypes) {
            auto& sourceTypeSysViews = groupedSysViews[sourceObjectType];
            UNIT_ASSERT_C(!sourceTypeSysViews.contains(registryRecord.Name),
                TStringBuilder() << "Dupliсate sysview name '" << registryRecord.Name
                    << "' among " << SourceObjectTypeNames.at(sourceObjectType) << " sysviews"
                    << " in " << registryName << " registry");

            sourceTypeSysViews[registryRecord.Name] = {registryRecord.Type, schema};
        }
    }

    return groupedSysViews;
}

THashMap<TStringBuf, TSysViewDescription> MakeRewrittenSysViewsMap(const TVector<SysViewsRegistryRecord>& rewrittenSysViews,
    TStringBuf registryName)
{
    THashMap<TStringBuf, TSysViewDescription> sysViewsMap;
    for (const auto& registryRecord : rewrittenSysViews) {
        ISystemViewResolver::TSchema schema;
        registryRecord.FillSchemaFunc(schema);

        UNIT_ASSERT_C(!sysViewsMap.contains(registryRecord.Name),
            TStringBuilder() << "Dupliсate sysview name '" << registryRecord.Name << "'"
                << " among rewritten sysviews in " << registryName << " registry");

        sysViewsMap[registryRecord.Name] = {registryRecord.Type, schema};
    }

    return sysViewsMap;
}

TSet<NTable::TTag> FindColumnsSymmetricDifference(const THashMap<NTable::TTag, TSysTables::TTableColumnInfo>& lhs,
    const THashMap<NTable::TTag, TSysTables::TTableColumnInfo>& rhs)
{
    TSet<NTable::TTag> difference;
    for (const auto& [id, column] : lhs) {
        if (!rhs.contains(id)) {
            difference.emplace(id);
        }
    }

    return difference;
}

void CheckSysViewDescription(const TSysViewDescription& canonicalSysViewDescription,
    const TSysViewDescription& actualSysViewDescription, TStringBuf errorSuffix)
{
    UNIT_ASSERT_C(canonicalSysViewDescription.Type == actualSysViewDescription.Type,
        TStringBuilder() << "Type (implementation id) changed in " << errorSuffix);

    const auto& canonicalSchema = canonicalSysViewDescription.Schema;
    const auto& actualSchema = actualSysViewDescription.Schema;
    const auto& canonicalColumns = canonicalSchema.Columns;
    const auto& actualColumns = actualSchema.Columns;

    const auto deletedColumns = FindColumnsSymmetricDifference(canonicalColumns, actualColumns);
    UNIT_ASSERT_C(deletedColumns.empty(),
        TStringBuilder() << "Column '" << canonicalColumns.at(*deletedColumns.begin()).Name << "'"
            << " was deleted or its column id was changed in " << errorSuffix);

    const auto addedColumns = FindColumnsSymmetricDifference(actualColumns, canonicalColumns);
    UNIT_ASSERT_C(addedColumns.empty(),
        TStringBuilder() << "Column '" << actualColumns.at(*addedColumns.begin()).Name << "'"
            << " was added or its column id was changed in " << errorSuffix);

    for (const auto& [canonicalId, canonicalColumn] : canonicalColumns) {
        const auto actualColumnsIter = actualColumns.find(canonicalId);
        if (actualColumnsIter != actualColumns.end()) {
            const auto& actualColumn = actualColumnsIter->second;
            UNIT_ASSERT_C(canonicalColumn.Name == actualColumn.Name,
                TStringBuilder() << "Column '" << canonicalColumn.Name << "' was renamed in " << errorSuffix);

            const auto canonicalTypeName = NScheme::TypeName(canonicalColumn.PType, canonicalColumn.PTypeMod);
            const auto actualTypeName = NScheme::TypeName(actualColumn.PType, actualColumn.PTypeMod);
            UNIT_ASSERT_C(canonicalTypeName == actualTypeName,
                TStringBuilder() << "Column '" << canonicalColumn.Name << "' changed type in " << errorSuffix);

            UNIT_ASSERT_C(canonicalColumn.KeyOrder == actualColumn.KeyOrder,
                TStringBuilder() << "Primary key differs in " << errorSuffix);

            UNIT_ASSERT_C(canonicalColumn.IsNotNullColumn == actualColumn.IsNotNullColumn,
                TStringBuilder() << "IsNotNull attribute of '" << canonicalColumn.Name << "' column"
                    << " changed in " << errorSuffix);
        }
    }


    UNIT_ASSERT_C(canonicalSchema.KeyColumnTypes.size() == actualSchema.KeyColumnTypes.size(),
        TStringBuilder() << "Primary key differs in " << errorSuffix);

    for (size_t i = 0; i < canonicalSchema.KeyColumnTypes.size(); ++i) {
        UNIT_ASSERT_C(canonicalSchema.KeyColumnTypes[i] == actualSchema.KeyColumnTypes[i],
            TStringBuilder() << "Primary key differs in " << errorSuffix);
    }
}

void CheckSysViewsLists(const THashMap<TStringBuf, TSysViewDescription>& canonicalSysViewsList,
    THashMap<TStringBuf, TSysViewDescription>& actualSysViewsList, TStringBuf sysViewListName)
{
    for (const auto& [sysViewName, sysViewDescription] : canonicalSysViewsList) {
        const auto actualSysViewsIter = actualSysViewsList.find(sysViewName);
        UNIT_ASSERT_C(actualSysViewsIter != actualSysViewsList.end(),
            TStringBuilder() << "Sysview '" << sysViewName << "'"
                << " was deleted from" << sysViewListName << " sysviews");

        CheckSysViewDescription(sysViewDescription, actualSysViewsIter->second,
            TStringBuilder() << "sysview '" << sysViewName << "' from " << sysViewListName << " sysviews");

        actualSysViewsList.erase(actualSysViewsIter);
    }

    UNIT_ASSERT_C(actualSysViewsList.empty(),
        TStringBuilder() << "Sysview '" << actualSysViewsList.begin()->first << "'"
            << " was added to " << sysViewListName << " sysviews");
}

}

Y_UNIT_TEST_SUITE(SysViewsRegistry) {
    Y_UNIT_TEST(SysViews) {
        auto canonicalSysViews = GroupSysViewsBySource(SysViews, "canonical");
        auto actualSysViews = GroupSysViewsBySource(SysViewsRegistry::SysViews, "actual");

        for (const auto& [sourceObjectType, sourceObjectName] : SourceObjectTypeNames) {
            auto& canonicalSysViewsList = canonicalSysViews[sourceObjectType];
            auto& actualSysViewsList = actualSysViews[sourceObjectType];

            CheckSysViewsLists(canonicalSysViewsList, actualSysViewsList, sourceObjectName);
        }
    }

    Y_UNIT_TEST(RewrittenSysViews) {
        auto canonicalSysViews = MakeRewrittenSysViewsMap(RewrittenSysViews, "canonical");
        auto actualSysViews = MakeRewrittenSysViewsMap(SysViewsRegistry::RewrittenSysViews, "actual");

        CheckSysViewsLists(canonicalSysViews, actualSysViews, "rewritten");
    }
}

} // NSysView
} // NKikimr
