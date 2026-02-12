#pragma once

#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/sys_view/common/resolver.h>

namespace NKikimr {
namespace NSysView {

template <typename Table>
struct TSchemaFiller {

    using TSchema = ISystemViewResolver::TSchema;

    template <typename...>
    struct TFiller;

    template <typename Column>
    struct TFiller<Column> {
        static void Fill(TSchema& schema) {
            schema.Columns[Column::ColumnId] = TSysTables::TTableColumnInfo(
                Table::template TableColumns<Column>::GetColumnName(),
                Column::ColumnId, NScheme::TTypeInfo(Column::ColumnType), "", -1);
        }
    };

    template <typename Column, typename... Columns>
    struct TFiller<Column, Columns...> {
        static void Fill(TSchema& schema) {
            TFiller<Column>::Fill(schema);
            TFiller<Columns...>::Fill(schema);
        }
    };

    template <typename... Columns>
    using TColumnsType = typename Table::template TableColumns<Columns...>;

    template <typename... Columns>
    static void FillColumns(TSchema& schema, TColumnsType<Columns...>) {
        TFiller<Columns...>::Fill(schema);
    }

    template <typename...>
    struct TKeyFiller;

    template <typename Key>
    struct TKeyFiller<Key> {
        static void Fill(TSchema& schema, i32 index) {
            auto& column = schema.Columns[Key::ColumnId];
            column.KeyOrder = index;
            schema.KeyColumnTypes.push_back(column.PType);
        }
    };

    template <typename Key, typename... Keys>
    struct TKeyFiller<Key, Keys...> {
        static void Fill(TSchema& schema, i32 index) {
            TKeyFiller<Key>::Fill(schema, index);
            TKeyFiller<Keys...>::Fill(schema, index + 1);
        }
    };

    template <typename... Keys>
    using TKeysType = typename Table::template TableKey<Keys...>;

    template <typename... Keys>
    static void FillKeys(TSchema& schema, TKeysType<Keys...>) {
        TKeyFiller<Keys...>::Fill(schema, 0);
    }

    static void Fill(TSchema& schema) {
        FillColumns(schema, typename Table::TColumns());
        FillKeys(schema, typename Table::TKey());
    }
};

template <typename Schema>
void FillSchema(ISystemViewResolver::TSchema& schema) {
    TSchemaFiller<Schema>::Fill(schema);
}

constexpr TStringBuf PgTablesName = "pg_tables";
constexpr TStringBuf InformationSchemaTablesName = "tables";
constexpr TStringBuf PgClassName = "pg_class";

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
        struct TxCompleteLag            : Column<31, NScheme::NTypeIds::Interval> {};

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
            LocksBroken,
            TxCompleteLag
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
        //struct StopFactor                    : Column<13, NScheme::NTypeIds::Double> {};
        struct StatusChangeTimestamp           : Column<14, NScheme::NTypeIds::Timestamp> {};
        struct ExpectedSlotCount               : Column<15, NScheme::NTypeIds::Uint32> {};
        struct NumActiveSlots                  : Column<16, NScheme::NTypeIds::Uint32> {};
        struct DecommitStatus                  : Column<17, NScheme::NTypeIds::Utf8> {};
        struct State                           : Column<18, NScheme::NTypeIds::Utf8> {};
        struct SlotSizeInUnits                 : Column<19, NScheme::NTypeIds::Uint32> {};
        // struct InferPDiskSlotCountFromUnitSize : Column<20, NScheme::NTypeIds::Uint64> {};

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
            SlotSizeInUnits>;
    };

    struct VSlots : Table<5> {
        struct NodeId          : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PDiskId         : Column<2, NScheme::NTypeIds::Uint32> {};
        struct VSlotId         : Column<3, NScheme::NTypeIds::Uint32> {};
        //struct Category      : Column<4, NScheme::NTypeIds::Uint64> {};
        struct GroupId         : Column<5, NScheme::NTypeIds::Uint32> {};
        struct GroupGeneration : Column<6, NScheme::NTypeIds::Uint32> {};
        //struct Ring          : Column<7, NScheme::NTypeIds::Uint32> {};
        struct FailDomain      : Column<8, NScheme::NTypeIds::Uint32> {};
        struct VDisk           : Column<9, NScheme::NTypeIds::Uint32> {};
        struct AllocatedSize   : Column<10, NScheme::NTypeIds::Uint64> {};
        struct AvailableSize   : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Status          : Column<12, NScheme::NTypeIds::Utf8> {};
        //struct StopFactor    : Column<13, NScheme::NTypeIds::Double> {};
        struct Kind            : Column<14, NScheme::NTypeIds::Utf8> {};
        struct FailRealm       : Column<15, NScheme::NTypeIds::Uint32> {};
        struct Replicated      : Column<16, NScheme::NTypeIds::Bool> {};
        struct DiskSpace       : Column<17, NScheme::NTypeIds::Utf8> {};
        struct State           : Column<18, NScheme::NTypeIds::Utf8> {};

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
        //struct Usage                       : Column<10, NScheme::NTypeIds::Double> {};
        //struct StopFactor                  : Column<11, NScheme::NTypeIds::Double> {};
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
        struct IntervalEnd          : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank                 : Column<2, NScheme::NTypeIds::Uint32> {};
        struct QueryText            : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Count                : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SumCPUTime           : Column<5, NScheme::NTypeIds::Uint64> {};
        struct MinCPUTime           : Column<6, NScheme::NTypeIds::Uint64> {};
        struct MaxCPUTime           : Column<7, NScheme::NTypeIds::Uint64> {};
        struct SumDuration          : Column<8, NScheme::NTypeIds::Interval> {};
        struct MinDuration          : Column<9, NScheme::NTypeIds::Interval> {};
        struct MaxDuration          : Column<10, NScheme::NTypeIds::Interval> {};
        struct MinReadRows          : Column<11, NScheme::NTypeIds::Uint64> {};
        struct MaxReadRows          : Column<12, NScheme::NTypeIds::Uint64> {};
        struct SumReadRows          : Column<13, NScheme::NTypeIds::Uint64> {};
        struct MinReadBytes         : Column<14, NScheme::NTypeIds::Uint64> {};
        struct MaxReadBytes         : Column<15, NScheme::NTypeIds::Uint64> {};
        struct SumReadBytes         : Column<16, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateRows        : Column<17, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateRows        : Column<18, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateRows        : Column<19, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateBytes       : Column<20, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateBytes       : Column<21, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateBytes       : Column<22, NScheme::NTypeIds::Uint64> {};
        struct MinDeleteRows        : Column<23, NScheme::NTypeIds::Uint64> {};
        struct MaxDeleteRows        : Column<24, NScheme::NTypeIds::Uint64> {};
        struct SumDeleteRows        : Column<25, NScheme::NTypeIds::Uint64> {};
        struct MinRequestUnits      : Column<26, NScheme::NTypeIds::Uint64> {};
        struct MaxRequestUnits      : Column<27, NScheme::NTypeIds::Uint64> {};
        struct SumRequestUnits      : Column<28, NScheme::NTypeIds::Uint64> {};
        struct LocksBrokenAsBreaker : Column<29, NScheme::NTypeIds::Uint64> {};
        struct LocksBrokenAsVictim  : Column<30, NScheme::NTypeIds::Uint64> {};

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
            SumRequestUnits, MinRequestUnits, MaxRequestUnits,
            LocksBrokenAsBreaker, LocksBrokenAsVictim>;
    };

    struct PrimaryIndexStats : Table<10> {
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

    struct StorageStats : Table<11> {
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

    struct TopPartitions : Table<12> {
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

    struct QuerySessions : Table<13> {
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

    struct PrimaryIndexPortionStats : Table<14> {
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

    struct PrimaryIndexGranuleStats : Table<14> {
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

    struct PrimaryIndexOptimizerStats : Table<14> {
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

    struct AuthUsers : Table<15> {
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

    struct AuthGroups : Table<16> {
        struct Sid : Column<1, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Sid>;
        using TColumns = TableColumns<
            Sid
        >;
    };

    struct AuthGroupMembers : Table<17> {
        struct GroupSid  : Column<1, NScheme::NTypeIds::Utf8> {};
        struct MemberSid : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<GroupSid, MemberSid>;
        using TColumns = TableColumns<
            GroupSid,
            MemberSid
        >;
    };

    struct AuthOwners : Table<18> {
        struct Path : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Sid  : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path>;
        using TColumns = TableColumns<
            Path,
            Sid
        >;
    };

    struct AuthPermissions : Table<19> {
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

    struct PgColumn {
        NIceDb::TColumnId _ColumnId;
        NScheme::TTypeInfo _ColumnTypeInfo;
        TString _ColumnName;
        PgColumn(NIceDb::TColumnId columnId, TStringBuf columnTypeName, TStringBuf columnName);
    };

    class PgTablesSchemaProvider {
    public:
        PgTablesSchemaProvider();
        const TVector<PgColumn>& GetColumns(TStringBuf tableName) const;
    private:
        std::unordered_map<TString, TVector<PgColumn>> columnsStorage;
    };

    struct ResourcePoolClassifiers : Table<20> {
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

    struct ShowCreate : Table<21> {
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

    struct ResourcePools : Table<22> {
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

    struct TopPartitionsTli : Table<23> {
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

    struct PrimaryIndexSchemaStats : Table<24> {
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

    struct CompileCacheQueries : Table<25> {
        struct NodeId                : Column<1, NScheme::NTypeIds::Uint32> {};
        struct QueryId               : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Query                 : Column<3, NScheme::NTypeIds::Utf8> {};
        struct AccessCount           : Column<4, NScheme::NTypeIds::Uint64> {};
        struct CompiledAt            : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct UserSID               : Column<6, NScheme::NTypeIds::Utf8> {};
        struct LastAccessedAt        : Column<7, NScheme::NTypeIds::Timestamp> {};
        struct CompilationDurationMs : Column<8, NScheme::NTypeIds::Uint64> {};
        struct Warnings              : Column<9, NScheme::NTypeIds::Utf8> {};
        struct Metadata              : Column<10, NScheme::NTypeIds::Utf8> {};
        struct IsTruncated           : Column<11, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<NodeId, QueryId>;
        using TColumns = TableColumns<
            NodeId,
            QueryId,
            Query,
            AccessCount,
            CompiledAt,
            UserSID,
            LastAccessedAt,
            CompilationDurationMs,
            Warnings,
            Metadata,
            IsTruncated>;
    };

    struct StreamingQueries : Table<26> {
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

struct SysViewsRegistryRecord {
    using FillSchemaPointer = void(*)(ISystemViewResolver::TSchema&);

    const TStringBuf Name;
    const NKikimrSysView::ESysViewType Type;
    const TSet<ISystemViewResolver::ESource> SourceObjectTypes;
    const FillSchemaPointer FillSchemaFunc;
};

struct SysViewsRegistry {
    using ESysViewType = NKikimrSysView::ESysViewType;
    using ESource = ISystemViewResolver::ESource;

    SysViewsRegistry();

    static const TVector<SysViewsRegistryRecord> SysViews;
    static const TVector<SysViewsRegistryRecord> RewrittenSysViews;

    THashMap<TStringBuf, ESysViewType> SysViewTypesMap;

} const extern Registry;

} // NSysView
} // NKikimr
