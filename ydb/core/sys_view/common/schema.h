#pragma once

#include "path.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr {
namespace NSysView {

constexpr TStringBuf PartitionStatsName = "partition_stats";
constexpr TStringBuf NodesName = "nodes";
constexpr TStringBuf QuerySessions = "query_sessions";

constexpr TStringBuf TopQueriesByDuration1MinuteName = "top_queries_by_duration_one_minute";
constexpr TStringBuf TopQueriesByDuration1HourName = "top_queries_by_duration_one_hour";
constexpr TStringBuf TopQueriesByReadBytes1MinuteName = "top_queries_by_read_bytes_one_minute";
constexpr TStringBuf TopQueriesByReadBytes1HourName = "top_queries_by_read_bytes_one_hour";
constexpr TStringBuf TopQueriesByCpuTime1MinuteName = "top_queries_by_cpu_time_one_minute";
constexpr TStringBuf TopQueriesByCpuTime1HourName = "top_queries_by_cpu_time_one_hour";
constexpr TStringBuf TopQueriesByRequestUnits1MinuteName = "top_queries_by_request_units_one_minute";
constexpr TStringBuf TopQueriesByRequestUnits1HourName = "top_queries_by_request_units_one_hour";

constexpr TStringBuf PDisksName = "ds_pdisks";
constexpr TStringBuf VSlotsName = "ds_vslots";
constexpr TStringBuf GroupsName = "ds_groups";
constexpr TStringBuf StoragePoolsName = "ds_storage_pools";
constexpr TStringBuf StorageStatsName = "ds_storage_stats";

constexpr TStringBuf TabletsName = "hive_tablets";

constexpr TStringBuf QueryMetricsName = "query_metrics_one_minute";

constexpr TStringBuf StorePrimaryIndexStatsName = "store_primary_index_stats";
constexpr TStringBuf StorePrimaryIndexPortionStatsName = "store_primary_index_portion_stats";
constexpr TStringBuf StorePrimaryIndexGranuleStatsName = "store_primary_index_granule_stats";
constexpr TStringBuf StorePrimaryIndexOptimizerStatsName = "store_primary_index_optimizer_stats";
constexpr TStringBuf TablePrimaryIndexStatsName = "primary_index_stats";
constexpr TStringBuf TablePrimaryIndexPortionStatsName = "primary_index_portion_stats";
constexpr TStringBuf TablePrimaryIndexGranuleStatsName = "primary_index_granule_stats";
constexpr TStringBuf TablePrimaryIndexOptimizerStatsName = "primary_index_optimizer_stats";

constexpr TStringBuf TopPartitions1MinuteName = "top_partitions_one_minute";
constexpr TStringBuf TopPartitions1HourName = "top_partitions_one_hour";

constexpr TStringBuf PgTablesName = "pg_tables";
constexpr TStringBuf InformationSchemaTablesName = "tables";
constexpr TStringBuf PgClassName = "pg_class";


struct Schema : NIceDb::Schema {
    struct PartitionStats : Table<1> {
        struct OwnerId              : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PathId               : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PartIdx              : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DataSize             : Column<4, NScheme::NTypeIds::Uint64> {};
        struct RowCount             : Column<5, NScheme::NTypeIds::Uint64> {};
        struct IndexSize            : Column<6, NScheme::NTypeIds::Uint64> {};
        struct CPUCores             : Column<7, NScheme::NTypeIds::Double> {};
        struct TabletId             : Column<8, NScheme::NTypeIds::Uint64> {};
        struct Path                 : Column<9, NScheme::NTypeIds::Utf8> {};
        struct NodeId               : Column<10, NScheme::NTypeIds::Uint32> {};
        struct StartTime            : Column<11, NScheme::NTypeIds::Timestamp> {};
        struct AccessTime           : Column<12, NScheme::NTypeIds::Timestamp> {};
        struct UpdateTime           : Column<13, NScheme::NTypeIds::Timestamp> {};
        struct InFlightTxCount      : Column<14, NScheme::NTypeIds::Uint32> {};
        struct RowUpdates           : Column<15, NScheme::NTypeIds::Uint64> {};
        struct RowDeletes           : Column<16, NScheme::NTypeIds::Uint64> {};
        struct RowReads             : Column<17, NScheme::NTypeIds::Uint64> {};
        struct RangeReads           : Column<18, NScheme::NTypeIds::Uint64> {};
        struct RangeReadRows        : Column<19, NScheme::NTypeIds::Uint64> {};
        struct ImmediateTxCompleted : Column<20, NScheme::NTypeIds::Uint64> {};
        struct CoordinatedTxCompleted : Column<21, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedByOverload : Column<22, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedByOutOfStorage : Column<23, NScheme::NTypeIds::Uint64> {};
        struct LastTtlRunTime       : Column<24, NScheme::NTypeIds::Timestamp> {};
        struct LastTtlRowsProcessed : Column<25, NScheme::NTypeIds::Uint64> {};
        struct LastTtlRowsErased    : Column<26, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerId, PathId, PartIdx>;
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
            LastTtlRowsErased>;
    };

    struct Nodes : Table<2> {
        struct NodeId    : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Address   : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Host      : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Port      : Column<4, NScheme::NTypeIds::Uint32> {};
        struct StartTime : Column<5, NScheme::NTypeIds::Timestamp> {};
        struct UpTime    : Column<6, NScheme::NTypeIds::Interval> {};
        struct CpuThreads: Column<7, NScheme::NTypeIds::Uint32> {};
        struct CpuUsage  : Column<8, NScheme::NTypeIds::Double> {};
        struct CpuIdle   : Column<9, NScheme::NTypeIds::Double> {};

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
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PDiskId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TypeCol : Column<3, NScheme::NTypeIds::Utf8> { static TString GetColumnName(const TString&) { return "Type"; } };
        struct Kind : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Path : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Guid : Column<6, NScheme::NTypeIds::Uint64> {};
        struct BoxId : Column<7, NScheme::NTypeIds::Uint32> {};
        struct SharedWithOS : Column<8, NScheme::NTypeIds::Bool> {};
        struct ReadCentric : Column<9, NScheme::NTypeIds::Bool> {};
        struct AvailableSize : Column<10, NScheme::NTypeIds::Uint64> {};
        struct TotalSize : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Status : Column<12, NScheme::NTypeIds::Utf8> {};
        //struct StopFactor : Column<13, NScheme::NTypeIds::Double> {};
        struct StatusChangeTimestamp : Column<14, NScheme::NTypeIds::Timestamp> {};
        struct ExpectedSlotCount : Column<15, NScheme::NTypeIds::Uint32> {};
        struct NumActiveSlots : Column<16, NScheme::NTypeIds::Uint32> {};
        struct DecommitStatus : Column<17, NScheme::NTypeIds::Utf8> {};

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
            StatusChangeTimestamp,
            ExpectedSlotCount,
            NumActiveSlots,
            DecommitStatus>;
    };

    struct VSlots : Table<5> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PDiskId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct VSlotId : Column<3, NScheme::NTypeIds::Uint32> {};
        //struct Category : Column<4, NScheme::NTypeIds::Uint64> {};
        struct GroupId : Column<5, NScheme::NTypeIds::Uint32> {};
        struct GroupGeneration : Column<6, NScheme::NTypeIds::Uint32> {};
        //struct Ring : Column<7, NScheme::NTypeIds::Uint32> {};
        struct FailDomain : Column<8, NScheme::NTypeIds::Uint32> {};
        struct VDisk : Column<9, NScheme::NTypeIds::Uint32> {};
        struct AllocatedSize : Column<10, NScheme::NTypeIds::Uint64> {};
        struct AvailableSize : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Status : Column<12, NScheme::NTypeIds::Utf8> {};
        //struct StopFactor : Column<13, NScheme::NTypeIds::Double> {};
        struct Kind : Column<14, NScheme::NTypeIds::Utf8> {};
        struct FailRealm : Column<15, NScheme::NTypeIds::Uint32> {};

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
            Kind,
            FailRealm>;
    };

    struct Groups : Table<6> {
        struct GroupId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Generation : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ErasureSpecies : Column<3, NScheme::NTypeIds::Utf8> {};
        struct BoxId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId : Column<5, NScheme::NTypeIds::Uint64> {};
        struct EncryptionMode : Column<6, NScheme::NTypeIds::Uint32> {};
        struct LifeCyclePhase : Column<7, NScheme::NTypeIds::Uint32> {};
        struct AllocatedSize : Column<8, NScheme::NTypeIds::Uint64> {};
        struct AvailableSize : Column<9, NScheme::NTypeIds::Uint64> {};
        //struct Usage : Column<10, NScheme::NTypeIds::Double> {};
        //struct StopFactor : Column<11, NScheme::NTypeIds::Double> {};
        struct SeenOperational : Column<12, NScheme::NTypeIds::Bool> {};
        struct PutTabletLogLatency : Column<13, NScheme::NTypeIds::Interval> {};
        struct PutUserDataLatency : Column<14, NScheme::NTypeIds::Interval> {};
        struct GetFastLatency : Column<15, NScheme::NTypeIds::Interval> {};

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
            GetFastLatency>;
    };

    struct StoragePools : Table<7> {
        struct BoxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct StoragePoolId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Name : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Generation : Column<4, NScheme::NTypeIds::Uint64> {};
        struct ErasureSpecies : Column<5, NScheme::NTypeIds::Utf8> {};
        struct VDiskKind : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Kind : Column<7, NScheme::NTypeIds::Utf8> {};
        struct NumGroups : Column<8, NScheme::NTypeIds::Uint32> {};
        struct EncryptionMode : Column<9, NScheme::NTypeIds::Uint32> {};
        struct SchemeshardId : Column<10, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<11, NScheme::NTypeIds::Uint64> {};

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
            PathId>;
    };

    struct Tablets : Table<8> {
        struct TabletId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct FollowerId  : Column<2, NScheme::NTypeIds::Uint32> {};
        struct TypeCol  : Column<3, NScheme::NTypeIds::Utf8> { static TString GetColumnName(const TString&) { return "Type"; } };
        struct State    : Column<4, NScheme::NTypeIds::Utf8> {};
        struct VolatileState : Column<5, NScheme::NTypeIds::Utf8> {};
        struct BootState : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Generation : Column<7, NScheme::NTypeIds::Uint32> {};
        struct NodeId : Column<8, NScheme::NTypeIds::Uint32> {};
        struct CPU : Column<9, NScheme::NTypeIds::Double> {};
        struct Memory : Column<10, NScheme::NTypeIds::Uint64> {};
        struct Network : Column<11, NScheme::NTypeIds::Uint64> {};

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
        struct IntervalEnd    : Column<1, NScheme::NTypeIds::Timestamp> {};
        struct Rank           : Column<2, NScheme::NTypeIds::Uint32> {};
        struct QueryText      : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Count          : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SumCPUTime     : Column<5, NScheme::NTypeIds::Uint64> {};
        struct MinCPUTime     : Column<6, NScheme::NTypeIds::Uint64> {};
        struct MaxCPUTime     : Column<7, NScheme::NTypeIds::Uint64> {};
        struct SumDuration    : Column<8, NScheme::NTypeIds::Interval> {};
        struct MinDuration    : Column<9, NScheme::NTypeIds::Interval> {};
        struct MaxDuration    : Column<10, NScheme::NTypeIds::Interval> {};
        struct MinReadRows    : Column<11, NScheme::NTypeIds::Uint64> {};
        struct MaxReadRows    : Column<12, NScheme::NTypeIds::Uint64> {};
        struct SumReadRows    : Column<13, NScheme::NTypeIds::Uint64> {};
        struct MinReadBytes   : Column<14, NScheme::NTypeIds::Uint64> {};
        struct MaxReadBytes   : Column<15, NScheme::NTypeIds::Uint64> {};
        struct SumReadBytes   : Column<16, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateRows  : Column<17, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateRows  : Column<18, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateRows  : Column<19, NScheme::NTypeIds::Uint64> {};
        struct MinUpdateBytes : Column<20, NScheme::NTypeIds::Uint64> {};
        struct MaxUpdateBytes : Column<21, NScheme::NTypeIds::Uint64> {};
        struct SumUpdateBytes : Column<22, NScheme::NTypeIds::Uint64> {};
        struct MinDeleteRows  : Column<23, NScheme::NTypeIds::Uint64> {};
        struct MaxDeleteRows  : Column<24, NScheme::NTypeIds::Uint64> {};
        struct SumDeleteRows  : Column<25, NScheme::NTypeIds::Uint64> {};
        struct MinRequestUnits: Column<26, NScheme::NTypeIds::Uint64> {};
        struct MaxRequestUnits: Column<27, NScheme::NTypeIds::Uint64> {};
        struct SumRequestUnits: Column<28, NScheme::NTypeIds::Uint64> {};

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

    struct PrimaryIndexStats : Table<10> {
        struct PathId   : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Kind     : Column<2, NScheme::NTypeIds::Utf8> {};
        struct TabletId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Rows     : Column<4, NScheme::NTypeIds::Uint64> {};
        struct RawBytes : Column<5, NScheme::NTypeIds::Uint64> {};
        struct PortionId: Column<6, NScheme::NTypeIds::Uint64> {};
        struct ChunkIdx : Column<7, NScheme::NTypeIds::Uint64> {};
        struct EntityName: Column<8, NScheme::NTypeIds::Utf8> {};
        struct InternalEntityId : Column<9, NScheme::NTypeIds::Uint32> {};
        struct BlobId : Column<10, NScheme::NTypeIds::Utf8> {};
        struct BlobRangeOffset : Column<11, NScheme::NTypeIds::Uint64> {};
        struct BlobRangeSize : Column<12, NScheme::NTypeIds::Uint64> {};
        struct Activity : Column<13, NScheme::NTypeIds::Uint8> {};
        struct TierName: Column<14, NScheme::NTypeIds::Utf8> {};
        struct EntityType: Column<15, NScheme::NTypeIds::Utf8> {};

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
            EntityType
            >;
    };

    struct StorageStats : Table<11> {
        struct PDiskFilter : Column<2, NScheme::NTypeIds::Utf8> {};
        struct ErasureSpecies : Column<3, NScheme::NTypeIds::Utf8> {};
        struct CurrentGroupsCreated : Column<4, NScheme::NTypeIds::Uint32> {};
        struct CurrentAllocatedSize : Column<5, NScheme::NTypeIds::Uint64> {};
        struct CurrentAvailableSize : Column<6, NScheme::NTypeIds::Uint64> {};
        struct AvailableGroupsToCreate : Column<7, NScheme::NTypeIds::Uint32> {};
        struct AvailableSizeToCreate : Column<8, NScheme::NTypeIds::Uint64> {};

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
            InFlightTxCount>;
    };

    struct QuerySessions : Table<13> {
        struct SessionId : Column<1, NScheme::NTypeIds::Utf8> {};
        struct NodeId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct State : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Query : Column<4, NScheme::NTypeIds::Utf8> {};
        struct QueryCount : Column<5, NScheme::NTypeIds::Uint32> {};
        struct ClientAddress : Column<6, NScheme::NTypeIds::Utf8> {};
        struct ClientPID : Column<7, NScheme::NTypeIds::Utf8> {};
        struct ClientUserAgent : Column<8, NScheme::NTypeIds::Utf8> {};
        struct ClientSdkBuildInfo : Column<9, NScheme::NTypeIds::Utf8> {};
        struct ApplicationName : Column<10, NScheme::NTypeIds::Utf8> {};
        struct SessionStartAt : Column<11, NScheme::NTypeIds::Timestamp> {};
        struct QueryStartAt : Column<12, NScheme::NTypeIds::Timestamp> {};
        struct StateChangeAt : Column<13, NScheme::NTypeIds::Timestamp> {};
        struct UserSID : Column<14, NScheme::NTypeIds::Utf8> {};

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

    struct PrimaryIndexPortionStats: Table<14> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct Kind: Column<2, NScheme::NTypeIds::Utf8> {};
        struct TabletId: Column<3, NScheme::NTypeIds::Uint64> {};
        struct Rows: Column<4, NScheme::NTypeIds::Uint64> {};
        struct ColumnRawBytes: Column<5, NScheme::NTypeIds::Uint64> {};
        struct IndexRawBytes: Column<6, NScheme::NTypeIds::Uint64> {};
        struct ColumnBlobBytes: Column<7, NScheme::NTypeIds::Uint64> {};
        struct IndexBlobBytes: Column<8, NScheme::NTypeIds::Uint64> {};
        struct PortionId: Column<9, NScheme::NTypeIds::Uint64> {};
        struct Activity: Column<10, NScheme::NTypeIds::Uint8> {};
        struct TierName: Column<11, NScheme::NTypeIds::Utf8> {};
        struct Stats: Column<12, NScheme::NTypeIds::Utf8> {};
        struct Optimized: Column<13, NScheme::NTypeIds::Uint8> {};

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
            Optimized
        >;
    };

    struct PrimaryIndexGranuleStats: Table<14> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct PortionsCount: Column<3, NScheme::NTypeIds::Uint64> {};
        struct HostName: Column<4, NScheme::NTypeIds::Utf8> {};
        struct NodeId: Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, TabletId>;
        using TColumns = TableColumns<
            PathId,
            TabletId,
            PortionsCount,
            HostName,
            NodeId
        >;
    };

    struct PrimaryIndexOptimizerStats: Table<14> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct TaskId: Column<3, NScheme::NTypeIds::Uint64> {};
        struct HostName: Column<4, NScheme::NTypeIds::Utf8> {};
        struct NodeId: Column<5, NScheme::NTypeIds::Uint64> {};
        struct Start: Column<6, NScheme::NTypeIds::Utf8> {};
        struct Finish: Column<7, NScheme::NTypeIds::Utf8> {};
        struct Details: Column<8, NScheme::NTypeIds::Utf8> {};
        struct Category: Column<9, NScheme::NTypeIds::Uint64> {};
        struct Weight: Column<10, NScheme::NTypeIds::Int64> {};

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
            Weight
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
};

bool MaybeSystemViewPath(const TVector<TString>& path);
bool MaybeSystemViewFolderPath(const TVector<TString>& path);

class ISystemViewResolver {
public:
    virtual ~ISystemViewResolver() = default;

    enum class ETarget : ui8 {
        Domain,
        SubDomain,
        OlapStore,
        ColumnTable
    };

    struct TSystemViewPath {
        TVector<TString> Parent;
        TString ViewName;
        };

    struct TSchema {
        THashMap<NTable::TTag, TSysTables::TTableColumnInfo> Columns;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
    };

    virtual bool IsSystemViewPath(const TVector<TString>& path, TSystemViewPath& sysViewPath) const = 0;

    virtual TMaybe<TSchema> GetSystemViewSchema(const TStringBuf viewName, ETarget target) const = 0;

    virtual TVector<TString> GetSystemViewNames(ETarget target) const = 0;
};

ISystemViewResolver* CreateSystemViewResolver();

} // NSysView
} // NKikimr
