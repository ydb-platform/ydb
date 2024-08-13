#pragma once

#include "schemeshard_types.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/login/protos/login.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NSchemeShard {

struct Schema : NIceDb::Schema {
    struct Paths : Table<1> {
        struct Id :                    Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct ParentId :              Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Name :                  Column<3, NScheme::NTypeIds::Utf8> {};
        struct CreateFinished :        Column<4, NScheme::NTypeIds::Bool> {};  // legacy
        struct PathType :              Column<5, NScheme::NTypeIds::Uint32> {};
        struct StepCreated :           Column<6, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct CreateTxId :            Column<7, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct StepDropped :           Column<8, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct DropTxId :              Column<9, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct Owner :                 Column<10, NScheme::NTypeIds::Utf8> {};
        struct ACL :                   Column<11, NScheme::NTypeIds::String> {};
        struct LastTxId :              Column<12, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DirAlterVersion :       Column<13, NScheme::NTypeIds::Uint64> {};
        struct UserAttrsAlterVersion : Column<14, NScheme::NTypeIds::Uint64> {};
        struct ACLVersion :            Column<15, NScheme::NTypeIds::Uint64> {};
        struct ParentOwnerId :         Column<16, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };
        struct TempDirOwnerActorId :   Column<17, NScheme::NTypeIds::String> {}; // Only for EPathType::EPathTypeDir.
                                                                                 // Not empty if dir must be deleted after loosing connection with TempDirOwnerActorId actor.
                                                                                 // See schemeshard__background_cleaning.cpp.

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, ParentId, Name, CreateFinished, PathType, StepCreated, CreateTxId,
            StepDropped, DropTxId, Owner, ACL, LastTxId, DirAlterVersion, UserAttrsAlterVersion, ACLVersion,
            ParentOwnerId, TempDirOwnerActorId>;
    };

    struct MigratedPaths : Table<50> {
        struct OwnerPathId :           Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :           Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct ParentOwnerId :         Column<3, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct ParentLocalId :         Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Name :                  Column<5, NScheme::NTypeIds::Utf8> {};
        struct PathType :              Column<6, NScheme::NTypeIds::Uint32> {};
        struct StepCreated :           Column<7, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct CreateTxId :            Column<8, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct StepDropped :           Column<9, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct DropTxId :              Column<10, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct Owner :                 Column<11, NScheme::NTypeIds::Utf8> {};
        struct ACL :                   Column<12, NScheme::NTypeIds::String> {};
        struct LastTxId :              Column<13, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DirAlterVersion :       Column<14, NScheme::NTypeIds::Uint64> {};
        struct UserAttrsAlterVersion : Column<15, NScheme::NTypeIds::Uint64> {};
        struct ACLVersion :            Column<16, NScheme::NTypeIds::Uint64> {};
        struct TempDirOwnerActorId :   Column<17, NScheme::NTypeIds::String> {}; // Only for EPathType::EPathTypeDir.
                                                                                 // Not empty if dir must be deleted after loosing connection with TempDirOwnerActorId actor.
                                                                                 // See schemeshard__background_cleaning.cpp.

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, ParentOwnerId, ParentLocalId, Name, PathType, StepCreated, CreateTxId,
            StepDropped, DropTxId, Owner, ACL, LastTxId, DirAlterVersion, UserAttrsAlterVersion, ACLVersion, TempDirOwnerActorId>;
    };

    struct TxInFlight : Table<2> { // not in use
        struct TxId :           Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TxType :         Column<2, NScheme::NTypeIds::Byte> {};
        struct TargetPathId :   Column<3, NScheme::NTypeIds::Uint64> {};
        struct State :          Column<4, NScheme::NTypeIds::Byte> {};
        struct MinStep :        Column<8, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct ExtraBytes :     Column<10, NScheme::NTypeIds::String> {};
        struct StartTime :      Column<11, NScheme::NTypeIds::Uint64> {};
        struct DataTotalSize :  Column<12, NScheme::NTypeIds::Uint64> {};
        struct PlanStep :       Column<13, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId>;
        using TColumns = TableColumns<
            TxId,
            TxType,
            TargetPathId,
            State,
            MinStep,
            ExtraBytes,
            StartTime,
            DataTotalSize,
            PlanStep
        >;
    };

    struct TxDependencies : Table<3> {
        struct TxId :               Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DependentTxId :      Column<2, NScheme::NTypeIds::Uint64> { using Type = TTxId; };

        using TKey = TableKey<TxId, DependentTxId>;
        using TColumns = TableColumns<TxId, DependentTxId>;
    };

    struct TxShards : Table<11> {
        struct TxId :           Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct ShardIdx :       Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct Operation :      Column<3, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<TxId, ShardIdx>;
        using TColumns = TableColumns<TxId, ShardIdx, Operation>;
    };

    struct SysParams : Table<4> {
        struct Id :             Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value :          Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Value>;
    };

    /// @note (Tables|Columns).TabId = Paths.Id = Shards.PathId (correct name - PathId)
    struct Tables : Table<5> {
        struct TabId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct NextColId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct PartitionConfig : Column<3, NScheme::NTypeIds::Utf8> {}; // TPartitionConfig, String?
        struct AlterVersion : Column<4, NScheme::NTypeIds::Uint64> {};
        struct AlterTable : Column<5, NScheme::NTypeIds::Utf8> {};   // TTableDescription
        struct AlterTableFull : Column<6, NScheme::NTypeIds::String> {};   // TTableDescription
        struct PartitioningVersion : Column<7, NScheme::NTypeIds::Uint64> {};
        struct TTLSettings : Column<8, NScheme::NTypeIds::String> {};
        struct IsBackup : Column<9, NScheme::NTypeIds::Bool> {};
        struct ReplicationConfig : Column<10, NScheme::NTypeIds::String> {};
        struct IsTemporary : Column<11, NScheme::NTypeIds::Bool> {};
        struct OwnerActorId : Column<12, NScheme::NTypeIds::String> {}; // deprecated

        using TKey = TableKey<TabId>;
        using TColumns = TableColumns<
            TabId,
            NextColId,
            PartitionConfig,
            AlterVersion,
            AlterTable,
            AlterTableFull,
            PartitioningVersion,
            TTLSettings,
            IsBackup,
            ReplicationConfig,
            IsTemporary,
            OwnerActorId
        >;
    };

    struct MigratedTables : Table<54> {
        struct OwnerPathId :         Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :         Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        struct NextColId :           Column<3, NScheme::NTypeIds::Uint32> {};
        struct PartitionConfig :     Column<4, NScheme::NTypeIds::Utf8> {}; // TPartitionConfig, String?
        struct AlterVersion :        Column<5, NScheme::NTypeIds::Uint64> {};
        struct AlterTable :          Column<6, NScheme::NTypeIds::Utf8> {};   // TTableDescription
        struct AlterTableFull :      Column<7, NScheme::NTypeIds::String> {};   // TTableDescription
        struct PartitioningVersion : Column<8, NScheme::NTypeIds::Uint64> {};
        struct TTLSettings :         Column<9, NScheme::NTypeIds::String> {};
        struct IsBackup :            Column<10, NScheme::NTypeIds::Bool> {};
        struct ReplicationConfig :   Column<11, NScheme::NTypeIds::String> {};
        struct IsTemporary :         Column<12, NScheme::NTypeIds::Bool> {};
        struct OwnerActorId :        Column<13, NScheme::NTypeIds::String> {}; // deprecated

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            NextColId,
            PartitionConfig,
            AlterVersion,
            AlterTable,
            AlterTableFull,
            PartitioningVersion,
            TTLSettings,
            IsBackup,
            ReplicationConfig,
            IsTemporary,
            OwnerActorId
        >;
    };

    struct Columns : Table<6> {
        struct TabId :          Column<1, NScheme::NTypeIds::Uint64> {};
        struct ColId :          Column<2, NScheme::NTypeIds::Uint32> {};
        struct ColName :        Column<3, NScheme::NTypeIds::Utf8> {};
        struct ColType :        Column<4, NScheme::NTypeIds::Uint32> {};
        struct ColTypeData :    Column<12, NScheme::NTypeIds::String> {};
        struct ColKeyOrder :    Column<5, NScheme::NTypeIds::Uint32> {};
        struct CreateVersion :  Column<6, NScheme::NTypeIds::Uint64> {};
        struct DeleteVersion :  Column<7, NScheme::NTypeIds::Uint64> {};
        struct Family :         Column<8, NScheme::NTypeIds::Uint32> {};
        struct DefaultKind :    Column<9, NScheme::NTypeIds::Uint32> { using Type = ETableColumnDefaultKind; static constexpr Type Default = Type::None; };
        struct DefaultValue :   Column<10, NScheme::NTypeIds::String> {};
        struct NotNull :        Column<11, NScheme::NTypeIds::Bool> {};
        struct IsBuildInProgress :  Column<13, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<TabId, ColId>;
        using TColumns = TableColumns<TabId, ColId, ColName, ColType, ColKeyOrder,
            CreateVersion, DeleteVersion, Family, DefaultKind, DefaultValue, NotNull, ColTypeData, IsBuildInProgress>;
    };

    struct MigratedColumns : Table<55> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct ColId :          Column<3, NScheme::NTypeIds::Uint32> {};

        struct ColName :        Column<4, NScheme::NTypeIds::Utf8> {};
        struct ColType :        Column<5, NScheme::NTypeIds::Uint32> {};
        struct ColTypeData :    Column<13, NScheme::NTypeIds::String> {};
        struct ColKeyOrder :    Column<6, NScheme::NTypeIds::Uint32> {};
        struct CreateVersion :  Column<7, NScheme::NTypeIds::Uint64> {};
        struct DeleteVersion :  Column<8, NScheme::NTypeIds::Uint64> {};
        struct Family :         Column<9, NScheme::NTypeIds::Uint32> {};
        struct DefaultKind :    Column<10, NScheme::NTypeIds::Uint32> { using Type = ETableColumnDefaultKind; static constexpr Type Default = Type::None; };
        struct DefaultValue :   Column<11, NScheme::NTypeIds::String> {};
        struct NotNull :        Column<12, NScheme::NTypeIds::Bool> {};
        struct IsBuildInProgress :  Column<14, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, ColId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, ColId, ColName, ColType, ColKeyOrder,
            CreateVersion, DeleteVersion, Family, DefaultKind, DefaultValue, NotNull, ColTypeData, IsBuildInProgress>;
    };

    struct ColumnAlters : Table<13> {
        struct TabId :          Column<1, NScheme::NTypeIds::Uint64> {};
        struct ColId :          Column<2, NScheme::NTypeIds::Uint32> {};
        struct ColName :        Column<3, NScheme::NTypeIds::Utf8> {};
        struct ColType :        Column<4, NScheme::NTypeIds::Uint32> {};
        struct ColTypeData :    Column<12, NScheme::NTypeIds::String> {};
        struct ColKeyOrder :    Column<5, NScheme::NTypeIds::Uint32> {};
        struct CreateVersion :  Column<6, NScheme::NTypeIds::Uint64> {};
        struct DeleteVersion :  Column<7, NScheme::NTypeIds::Uint64> {};
        struct Family :         Column<8, NScheme::NTypeIds::Uint32> {};
        struct DefaultKind :    Column<9, NScheme::NTypeIds::Uint32> { using Type = ETableColumnDefaultKind; static constexpr Type Default = Type::None; };
        struct DefaultValue :   Column<10, NScheme::NTypeIds::String> {};
        struct NotNull :        Column<11, NScheme::NTypeIds::Bool> {};
        struct IsBuildInProgress :  Column<13, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<TabId, ColId>;
        using TColumns = TableColumns<TabId, ColId, ColName, ColType, ColKeyOrder,
            CreateVersion, DeleteVersion, Family, DefaultKind, DefaultValue, NotNull, ColTypeData, IsBuildInProgress>;
    };

    struct MigratedColumnAlters : Table<63> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct ColId :          Column<3, NScheme::NTypeIds::Uint32> {};

        struct ColName :        Column<4, NScheme::NTypeIds::Utf8> {};
        struct ColType :        Column<5, NScheme::NTypeIds::Uint32> {};
        struct ColTypeData :    Column<13, NScheme::NTypeIds::String> {};
        struct ColKeyOrder :    Column<6, NScheme::NTypeIds::Uint32> {};
        struct CreateVersion :  Column<7, NScheme::NTypeIds::Uint64> {};
        struct DeleteVersion :  Column<8, NScheme::NTypeIds::Uint64> {};
        struct Family :         Column<9, NScheme::NTypeIds::Uint32> {};
        struct DefaultKind :    Column<10, NScheme::NTypeIds::Uint32> { using Type = ETableColumnDefaultKind; static constexpr Type Default = Type::None; };
        struct DefaultValue :   Column<11, NScheme::NTypeIds::String> {};
        struct NotNull :        Column<12, NScheme::NTypeIds::Bool> {};
        struct IsBuildInProgress :  Column<14, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, ColId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, ColId, ColName, ColType, ColKeyOrder,
            CreateVersion, DeleteVersion, Family, DefaultKind, DefaultValue, NotNull, ColTypeData, IsBuildInProgress>;
    };

    struct Shards : Table<7> {
        struct ShardIdx :       Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct TabletId :       Column<2, NScheme::NTypeIds::Uint64> { using Type = TTabletId; };
        struct PathId :         Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; }; // LocalPathId
        // there can be only 1 schema Tx on a shard at any moment in time
        struct LastTxId :       Column<4, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TabletType :     Column<5, NScheme::NTypeIds::Uint32> { using Type = TTabletTypes::EType; static constexpr Type Default = ETabletType::TypeInvalid; };
        struct OwnerPathId :    Column<6, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };

        using TKey = TableKey<ShardIdx>;
        using TColumns = TableColumns<ShardIdx, TabletId, PathId, LastTxId, TabletType, OwnerPathId>;
    };

    struct MigratedShards : Table<52> {
        struct OwnerShardId :   Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardId :   Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct TabletId :       Column<3, NScheme::NTypeIds::Uint64> { using Type = TTabletId; };
        struct OwnerPathId :    Column<4, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<5, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        // there can be only 1 schema Tx on a shard at any moment in time
        struct LastTxId :       Column<6, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TabletType :     Column<7, NScheme::NTypeIds::Uint32> { using Type = TTabletTypes::EType; static constexpr Type Default = ETabletType::TypeInvalid; };

        using TKey = TableKey<OwnerShardId, LocalShardId>;
        using TColumns = TableColumns<OwnerShardId, LocalShardId, TabletId, OwnerPathId, LocalPathId, LastTxId, TabletType>;
    };

    struct ChannelsBinding : Table<28> {
        struct ShardId :        Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct ChannelId :      Column<2, NScheme::NTypeIds::Uint32> {};

        struct PoolName :       Column<4, NScheme::NTypeIds::Utf8> {};
        struct Binding :        Column<5, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ShardId, ChannelId>;
        using TColumns = TableColumns<ShardId, ChannelId, PoolName, Binding>;
    };

    struct MigratedChannelsBinding : Table<53> {
        struct OwnerShardId :   Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardId :   Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct ChannelId :      Column<3, NScheme::NTypeIds::Uint32> {};

        struct PoolName :       Column<4, NScheme::NTypeIds::Utf8> {};
        struct Binding :        Column<5, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerShardId, LocalShardId, ChannelId>;
        using TColumns = TableColumns<OwnerShardId, LocalShardId, ChannelId, PoolName, Binding>;
    };

    struct TablePartitions : Table<8> {
        struct TabId :          Column<1, NScheme::NTypeIds::Uint64> {};
        struct Id :             Column<2, NScheme::NTypeIds::Uint64> {};
        struct RangeEnd :       Column<3, NScheme::NTypeIds::String> { using Type = TString; };
        struct DatashardIdx :   Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct LastCondErase :  Column<5, NScheme::NTypeIds::Uint64> {};
        struct NextCondErase :  Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TabId, Id>;
        using TColumns = TableColumns<TabId, Id, RangeEnd, DatashardIdx, LastCondErase, NextCondErase>;
    };

    struct TablePartitionStats : Table<101> { // HACK: Force to use policy with multiple levels
        // key
        struct TableOwnerId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct TableLocalId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct PartitionId : Column<3, NScheme::NTypeIds::Uint64> {};

        // seqno
        struct SeqNoGeneration : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SeqNoRound : Column<5, NScheme::NTypeIds::Uint64> {};

        // stats
        struct RowCount : Column<6, NScheme::NTypeIds::Uint64> {};
        struct DataSize : Column<7, NScheme::NTypeIds::Uint64> {};
        struct IndexSize : Column<8, NScheme::NTypeIds::Uint64> {};

        struct LastAccessTime : Column<9, NScheme::NTypeIds::Uint64> { using Type = TInstant::TValue; };
        struct LastUpdateTime : Column<10, NScheme::NTypeIds::Uint64> { using Type = TInstant::TValue; };

        struct ImmediateTxCompleted : Column<11, NScheme::NTypeIds::Uint64> {};
        struct PlannedTxCompleted : Column<12, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedByOverload : Column<13, NScheme::NTypeIds::Uint64> {};
        struct TxRejectedBySpace : Column<14, NScheme::NTypeIds::Uint64> {};
        struct TxCompleteLag : Column<15, NScheme::NTypeIds::Uint64> { using Type = TInstant::TValue; };
        struct InFlightTxCount : Column<16, NScheme::NTypeIds::Uint64> {};

        struct RowUpdates : Column<17, NScheme::NTypeIds::Uint64> {};
        struct RowDeletes : Column<18, NScheme::NTypeIds::Uint64> {};
        struct RowReads : Column<19, NScheme::NTypeIds::Uint64> {};
        struct RangeReads : Column<20, NScheme::NTypeIds::Uint64> {};
        struct RangeReadRows : Column<21, NScheme::NTypeIds::Uint64> {};

        struct CPU : Column<22, NScheme::NTypeIds::Uint64> {};
        struct Memory : Column<23, NScheme::NTypeIds::Uint64> {};
        struct Network : Column<24, NScheme::NTypeIds::Uint64> {};
        struct Storage : Column<25, NScheme::NTypeIds::Uint64> {};
        struct ReadThroughput : Column<26, NScheme::NTypeIds::Uint64> {};
        struct WriteThroughput : Column<27, NScheme::NTypeIds::Uint64> {};
        struct ReadIops : Column<28, NScheme::NTypeIds::Uint64> {};
        struct WriteIops : Column<29, NScheme::NTypeIds::Uint64> {};

        struct SearchHeight : Column<30, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 0; };
        struct FullCompactionTs : Column<31, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 0; };
        struct MemDataSize : Column<32, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 0; };

        // PartCount, PartOwners & ShardState are volatile data

        // Represented by NKikimrTableStats::TStoragePoolsStats.
        struct StoragePoolsStats : Column<33, NScheme::NTypeIds::String> { using Type = TString; };

        struct ByKeyFilterSize : Column<34, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TableOwnerId, TableLocalId, PartitionId>;
        using TColumns = TableColumns<
            TableOwnerId,
            TableLocalId,
            PartitionId,
            SeqNoGeneration,
            SeqNoRound,
            RowCount,
            DataSize,
            IndexSize,
            LastAccessTime,
            LastUpdateTime,
            ImmediateTxCompleted,
            PlannedTxCompleted,
            TxRejectedByOverload,
            TxRejectedBySpace,
            TxCompleteLag,
            InFlightTxCount,
            RowUpdates,
            RowDeletes,
            RowReads,
            RangeReads,
            RangeReadRows,
            CPU,
            Memory,
            Network,
            Storage,
            ReadThroughput,
            WriteThroughput,
            ReadIops,
            WriteIops,
            SearchHeight,
            FullCompactionTs,
            MemDataSize,
            StoragePoolsStats,
            ByKeyFilterSize
        >;
    };

    struct MigratedTablePartitions : Table<56> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Id :             Column<3, NScheme::NTypeIds::Uint64> {};

        struct RangeEnd :       Column<4, NScheme::NTypeIds::String> { using Type = TString; };
        struct OwnerShardIdx :  Column<5, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardIdx :  Column<6, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct LastCondErase :  Column<7, NScheme::NTypeIds::Uint64> {};
        struct NextCondErase :  Column<8, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, Id>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, Id, RangeEnd, OwnerShardIdx, LocalShardIdx, LastCondErase, NextCondErase>;
    };

    struct PersQueueGroups : Table<9> {
        struct PathId :          Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletConfig :    Column<2, NScheme::NTypeIds::Utf8> {};
        struct MaxPQPerShard :   Column<3, NScheme::NTypeIds::Uint32> {};
        struct AlterVersion :    Column<4, NScheme::NTypeIds::Uint64> {};
        struct NextPartitionId : Column<5, NScheme::NTypeIds::Uint32> {};
        struct TotalGroupCount : Column<6, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<
            PathId,
            TabletConfig,
            MaxPQPerShard,
            AlterVersion,
            NextPartitionId,
            TotalGroupCount
        >;
    };

    struct PersQueueGroupAlters : Table<12> {
        struct PathId :          Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletConfig :    Column<2, NScheme::NTypeIds::Utf8> {};
        struct MaxPQPerShard :   Column<3, NScheme::NTypeIds::Uint32> {};
        struct AlterVersion :    Column<4, NScheme::NTypeIds::Uint64> {};
        struct NextPartitionId : Column<6, NScheme::NTypeIds::Uint32> {};
        struct TotalGroupCount : Column<5, NScheme::NTypeIds::Uint32> {};
        struct BootstrapConfig : Column<7, NScheme::NTypeIds::String> { using Type = TString; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<
            PathId,
            TabletConfig,
            MaxPQPerShard,
            AlterVersion,
            NextPartitionId,
            TotalGroupCount,
            BootstrapConfig
        >;
    };

    struct PersQueues : Table<10> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct PqId :           Column<2, NScheme::NTypeIds::Uint32> {};
        struct ShardIdx :       Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct AlterVersion :   Column<4, NScheme::NTypeIds::Uint64> {};
        struct GroupId :        Column<5, NScheme::NTypeIds::Uint32> {};
        struct RangeBegin :     Column<6, NScheme::NTypeIds::String> { using Type = TString; };
        struct RangeEnd :       Column<7, NScheme::NTypeIds::String> { using Type = TString; };
        struct CreateVersion:   Column<8, NScheme::NTypeIds::Uint64> {};
        struct Status:          Column<9, NScheme::NTypeIds::Uint32> {};
        // Parent partition for split and merge operations
        struct Parent:          Column<10, NScheme::NTypeIds::Uint32> {};
        // Adjacent parent partition for merge operation
        struct AdjacentParent:  Column<11, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<PathId, PqId>;
        using TColumns = TableColumns<PathId, PqId, ShardIdx, AlterVersion, GroupId, RangeBegin, RangeEnd,
                                      CreateVersion, Status, Parent, AdjacentParent>;
    };

    struct RtmrVolumes : Table<20> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PartitionsCount : Column<2, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, PartitionsCount>;
    };

    struct RTMRPartitions : Table<25> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct PartitionId :    Column<2, NScheme::NTypeIds::String> {};
        struct BusKey :         Column<3, NScheme::NTypeIds::Uint64> {};
        struct ShardIdx :       Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        using TKey = TableKey<PathId, ShardIdx>;
        using TColumns = TableColumns<PathId, PartitionId, BusKey, ShardIdx>;
    };

    struct SolomonVolumes : Table<29> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct Version :        Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Version>;
    };

    struct SolomonPartitions : Table<30> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct ShardId :        Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct PartitionId :    Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, ShardId>;
        using TColumns = TableColumns<PathId, ShardId, PartitionId>;
    };

    struct AlterSolomonVolumes : Table<75> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Version :        Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, Version>;
    };

    struct AlterSolomonPartitions : Table<76> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        struct ShardOwnerId :   Column<3, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct ShardLocalIdx :  Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct PartitionId :    Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, ShardOwnerId, ShardLocalIdx>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, ShardOwnerId, ShardLocalIdx, PartitionId>;
    };

    struct ShardsToDelete : Table<14> {
        struct ShardIdx : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        using TKey = TableKey<ShardIdx>;
        using TColumns = TableColumns<ShardIdx>;
    };

    struct MigratedShardsToDelete : Table<60> {
        struct ShardOwnerId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct ShardLocalIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        using TKey = TableKey<ShardOwnerId, ShardLocalIdx>;
        using TColumns = TableColumns<ShardOwnerId, ShardLocalIdx>;
    };

    struct BackupSettings : Table<15> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct TableName : Column<2, NScheme::NTypeIds::Utf8> {};
        struct YTSettings : Column<3, NScheme::NTypeIds::String> {};
        struct S3Settings : Column<6, NScheme::NTypeIds::String> {};
        struct TableDescription : Column<7, NScheme::NTypeIds::String> {};
        struct NumberOfRetries : Column<8, NScheme::NTypeIds::Uint32> {};
        struct ScanSettings : Column<9, NScheme::NTypeIds::String> {};
        struct NeedToBill : Column<10, NScheme::NTypeIds::Bool> {};
        // deprecated
        struct CreateDestinationFlag : Column<4, NScheme::NTypeIds::Bool> {};
        struct EraseOldDataFlag : Column<5, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<
            PathId,
            TableName,
            YTSettings,
            CreateDestinationFlag,
            EraseOldDataFlag,
            S3Settings,
            TableDescription,
            NumberOfRetries,
            ScanSettings,
            NeedToBill
        >;
    };

    struct MigratedBackupSettings : Table<64> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        struct TableName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct YTSettings : Column<4, NScheme::NTypeIds::String> {};
        struct S3Settings : Column<7, NScheme::NTypeIds::String> {};
        struct TableDescription : Column<8, NScheme::NTypeIds::String> {};
        struct NumberOfRetries : Column<9, NScheme::NTypeIds::Uint32> {};
        struct ScanSettings : Column<10, NScheme::NTypeIds::String> {};
        struct NeedToBill : Column<11, NScheme::NTypeIds::Bool> {};
        // deprecated
        struct CreateDestinationFlag : Column<5, NScheme::NTypeIds::Bool> {};
        struct EraseOldDataFlag : Column<6, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            TableName,
            YTSettings,
            CreateDestinationFlag,
            EraseOldDataFlag,
            S3Settings,
            TableDescription,
            NumberOfRetries,
            ScanSettings,
            NeedToBill
        >;
    };

    struct CompletedBackups : Table<16> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DateTimeOfCompletion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct SuccessShardCount : Column<4, NScheme::NTypeIds::Uint32> {};
        struct TotalShardCount : Column<5, NScheme::NTypeIds::Uint32> {};
        struct StartTime : Column<6, NScheme::NTypeIds::Uint64> {};
        struct YTSettings : Column<7, NScheme::NTypeIds::String> {};
        struct S3Settings : Column<9, NScheme::NTypeIds::String> {};
        struct DataTotalSize : Column<8, NScheme::NTypeIds::Uint64> {};
        struct Kind : Column<10, NScheme::NTypeIds::Byte> {};

        using TKey = TableKey<PathId, TxId, DateTimeOfCompletion>;
        using TColumns = TableColumns<
            PathId,
            TxId,
            DateTimeOfCompletion,
            SuccessShardCount,
            TotalShardCount,
            StartTime,
            YTSettings,
            DataTotalSize,
            S3Settings,
            Kind
        >;
    };

    struct MigratedCompletedBackups : Table<65> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct TxId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DateTimeOfCompletion : Column<5, NScheme::NTypeIds::Uint64> {};
        struct SuccessShardCount : Column<6, NScheme::NTypeIds::Uint32> {};
        struct TotalShardCount : Column<7, NScheme::NTypeIds::Uint32> {};
        struct StartTime : Column<8, NScheme::NTypeIds::Uint64> {};
        struct YTSettings : Column<9, NScheme::NTypeIds::String> {};
        struct S3Settings : Column<11, NScheme::NTypeIds::String> {};
        struct DataTotalSize : Column<10, NScheme::NTypeIds::Uint64> {};
        struct Kind : Column<12, NScheme::NTypeIds::Byte> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, TxId, DateTimeOfCompletion>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            TxId,
            DateTimeOfCompletion,
            SuccessShardCount,
            TotalShardCount,
            StartTime,
            YTSettings,
            DataTotalSize,
            S3Settings,
            Kind
        >;
    };

    // *ShardBackupStatus is deprecated, use TxShardStatus instead
    struct ShardBackupStatus : Table<17> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct ShardIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct Explain : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<TxId, ShardIdx>;
        using TColumns = TableColumns<
            TxId,
            ShardIdx,
            Explain
        >;
    };

    struct MigratedShardBackupStatus : Table<66> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct OwnerShardId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct Explain : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<TxId, OwnerShardId, LocalShardId>;
        using TColumns = TableColumns<
            TxId,
            OwnerShardId,
            LocalShardId,
            Explain
        >;
    };

    struct TxShardStatus : Table<87> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct OwnerShardId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct Success : Column<4, NScheme::NTypeIds::Bool> {};
        struct Error : Column<5, NScheme::NTypeIds::Utf8> {};
        struct BytesProcessed : Column<6, NScheme::NTypeIds::Uint64> {};
        struct RowsProcessed : Column<7, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId, OwnerShardId, LocalShardId>;
        using TColumns = TableColumns<
            TxId,
            OwnerShardId,
            LocalShardId,
            Success,
            Error,
            BytesProcessed,
            RowsProcessed
        >;
    };

    struct SubDomains : Table<18> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PlanResolution : Column<3, NScheme::NTypeIds::Uint64> {};
        struct TimeCastBuckets : Column<4, NScheme::NTypeIds::Uint32> {};
        struct DepthLimit : Column<5, NScheme::NTypeIds::Uint64> {};
        struct PathsLimit : Column<6, NScheme::NTypeIds::Uint64> {};
        struct ChildrenLimit : Column<7, NScheme::NTypeIds::Uint64> {};
        struct ShardsLimit : Column<8, NScheme::NTypeIds::Uint64> {};
        struct PathShardsLimit : Column<9, NScheme::NTypeIds::Uint64> {};
        struct TableColumnsLimit : Column<10, NScheme::NTypeIds::Uint64> {};
        struct TableColumnNameLengthLimit : Column<11, NScheme::NTypeIds::Uint64> {};
        struct TableKeyColumnsLimit : Column<12, NScheme::NTypeIds::Uint64> {};
        struct TableIndicesLimit : Column<13, NScheme::NTypeIds::Uint64> {};
        struct AclByteSizeLimit : Column<14, NScheme::NTypeIds::Uint64> {};
        struct ConsistentCopyingTargetsLimit : Column<15, NScheme::NTypeIds::Uint64> {};
        struct PathElementLength : Column<16, NScheme::NTypeIds::Uint64> {};
        struct ExtraPathSymbolsAllowed : Column<17, NScheme::NTypeIds::Utf8> {};
        struct ResourcesDomainOwnerPathId : Column<18, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };
        struct ResourcesDomainLocalPathId : Column<19, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; static constexpr Type Default = InvalidLocalPathId; };
        struct SharedHiveId : Column<20, NScheme::NTypeIds::Uint64> { using Type = TTabletId; static constexpr Type Default = InvalidTabletId; };
        struct DeclaredSchemeQuotas : Column<21, NScheme::NTypeIds::String> {};
        struct PQPartitionsLimit : Column<22, NScheme::NTypeIds::Uint64> {};
        struct DatabaseQuotas : Column<23, NScheme::NTypeIds::String> {};
        struct StateVersion : Column<24, NScheme::NTypeIds::Uint64> {};
        struct DiskQuotaExceeded : Column<25, NScheme::NTypeIds::Bool> {};
        struct SecurityStateVersion : Column<26, NScheme::NTypeIds::Uint64> {};
        struct TableCdcStreamsLimit : Column<27, NScheme::NTypeIds::Uint64> {};
        struct ExportsLimit : Column<28, NScheme::NTypeIds::Uint64> {};
        struct ImportsLimit : Column<29, NScheme::NTypeIds::Uint64> {};
        struct AuditSettings : Column<30, NScheme::NTypeIds::String> {};
        struct ServerlessComputeResourcesMode : Column<31, NScheme::NTypeIds::Uint32> { using Type = EServerlessComputeResourcesMode; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<
            PathId,
            AlterVersion,
            PlanResolution,
            TimeCastBuckets,
            DepthLimit,
            PathsLimit,
            ChildrenLimit,
            ShardsLimit,
            PathShardsLimit,
            TableColumnsLimit,
            TableColumnNameLengthLimit,
            TableKeyColumnsLimit,
            TableIndicesLimit,
            AclByteSizeLimit,
            ConsistentCopyingTargetsLimit,
            PathElementLength,
            ExtraPathSymbolsAllowed,
            ResourcesDomainOwnerPathId,
            ResourcesDomainLocalPathId,
            SharedHiveId,
            DeclaredSchemeQuotas,
            PQPartitionsLimit,
            DatabaseQuotas,
            StateVersion,
            DiskQuotaExceeded,
            SecurityStateVersion,
            TableCdcStreamsLimit,
            ExportsLimit,
            ImportsLimit,
            AuditSettings,
            ServerlessComputeResourcesMode
        >;
    };

    struct SubDomainShards : Table<19> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct ShardIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        using TKey = TableKey<PathId, ShardIdx>;
        using TColumns = TableColumns<PathId, ShardIdx>;
    };

    struct SubDomainsAlterData : Table<33> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PlanResolution : Column<3, NScheme::NTypeIds::Uint64> {};
        struct TimeCastBuckets : Column<4, NScheme::NTypeIds::Uint32> {};
        struct ResourcesDomainOwnerPathId : Column<5, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };
        struct ResourcesDomainLocalPathId : Column<6, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; static constexpr Type Default = InvalidLocalPathId; };
        struct SharedHiveId : Column<7, NScheme::NTypeIds::Uint64> { using Type = TTabletId; static constexpr Type Default = InvalidTabletId; };
        struct DeclaredSchemeQuotas : Column<8, NScheme::NTypeIds::String> {};
        struct DatabaseQuotas : Column<9, NScheme::NTypeIds::String> {};
        struct AuditSettings : Column<10, NScheme::NTypeIds::String> {};
        struct ServerlessComputeResourcesMode : Column<11, NScheme::NTypeIds::Uint32> { using Type = EServerlessComputeResourcesMode; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<
            PathId,
            AlterVersion,
            PlanResolution,
            TimeCastBuckets,
            ResourcesDomainOwnerPathId,
            ResourcesDomainLocalPathId,
            SharedHiveId,
            DeclaredSchemeQuotas,
            DatabaseQuotas,
            AuditSettings,
            ServerlessComputeResourcesMode
        >;
    };

    struct SubDomainShardsAlterData : Table<34> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct ShardIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        using TKey = TableKey<PathId, ShardIdx>;
        using TColumns = TableColumns<PathId, ShardIdx>;
    };

    struct SubDomainSchemeQuotas : Table<77> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct QuotaIdx : Column<2, NScheme::NTypeIds::Uint64> {};
        struct BucketSize : Column<3, NScheme::NTypeIds::Double> {};
        struct BucketDurationUs : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Available : Column<5, NScheme::NTypeIds::Double> {};
        struct LastUpdateUs : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, QuotaIdx>;
        using TColumns = TableColumns<PathId, QuotaIdx, BucketSize, BucketDurationUs, Available, LastUpdateUs>;
    };

    struct StoragePools : Table<27> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PoolName : Column<4, NScheme::NTypeIds::Utf8> {};
        struct PoolKind : Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, PoolName, PoolKind>;
        using TColumns = TableColumns<PathId, PoolName, PoolKind>;
    };

    struct StoragePoolsAlterData : Table<35> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PoolName : Column<4, NScheme::NTypeIds::Utf8> {};
        struct PoolKind : Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, PoolName, PoolKind>;
        using TColumns = TableColumns<PathId, PoolName, PoolKind>;
    };

    struct BlockStorePartitions : Table<22> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PartitionId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ShardIdx : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct AlterVersion : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, PartitionId>;
        using TColumns = TableColumns<PathId, PartitionId, ShardIdx, AlterVersion>;
    };

    struct BlockStoreVolumes : Table<23> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct VolumeConfig : Column<2, NScheme::NTypeIds::String> {};
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct MountToken : Column<4, NScheme::NTypeIds::Utf8> {};
        struct TokenVersion : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, VolumeConfig, AlterVersion, MountToken, TokenVersion>;
    };

    struct BlockStoreVolumeAlters : Table<24> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct VolumeConfig : Column<2, NScheme::NTypeIds::String> {};
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct PartitionCount : Column<4, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, VolumeConfig, AlterVersion, PartitionCount>;
    };

    struct FileStoreInfos : Table<78> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> {};
        struct Version : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Config, Version>;
    };

    struct FileStoreAlters : Table<79> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> {};
        struct Version : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Config, Version>;
    };

    struct KesusInfos : Table<26> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> {};
        struct Version : Column<3, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 1; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Config, Version>;
    };

    struct MigratedKesusInfos : Table<83> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Config : Column<3, NScheme::NTypeIds::String> {};
        struct Version : Column<4, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 1; };

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, Config, Version>;
    };

    struct KesusAlters : Table<32> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> {};
        struct Version : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Config, Version>;
    };

    struct MigratedKesusAlters : Table<84> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Config : Column<3, NScheme::NTypeIds::String> {};
        struct Version : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, Config, Version>;
    };

    struct AdoptedShards : Table<31> {
        struct ShardIdx : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct PrevOwner : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PrevShardIdx : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct TabletId : Column<4, NScheme::NTypeIds::Uint64> { using Type = TTabletId; };

        using TKey = TableKey<ShardIdx>;
        using TColumns = TableColumns<ShardIdx, PrevOwner, PrevShardIdx, TabletId>;
    };

    struct UserAttributes : Table<36> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct AttrName : Column<2, NScheme::NTypeIds::Utf8> {};
        struct AttrValue : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, AttrName>;
        using TColumns = TableColumns<PathId, AttrName, AttrValue>;
    };

    struct MigratedUserAttributes : Table<51> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AttrName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct AttrValue : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, AttrName>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, AttrName, AttrValue>;
    };

    struct UserAttributesAlterData : Table<37> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct AttrName : Column<2, NScheme::NTypeIds::Utf8> {};
        struct AttrValue : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, AttrName>;
        using TColumns = TableColumns<PathId, AttrName, AttrValue>;
    };

    struct MigratedUserAttributesAlterData : Table<62> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AttrName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct AttrValue : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, AttrName>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, AttrName, AttrValue>;
    };

    struct TableIndex : Table<38> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct AlterVersion :   Column<3, NScheme::NTypeIds::Uint64> {};
        struct IndexType :      Column<4, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexType; static constexpr Type Default = NKikimrSchemeOp::EIndexTypeInvalid; };
        struct State :          Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexState; static constexpr Type Default = NKikimrSchemeOp::EIndexStateInvalid; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, IndexType, State>;
    };

    struct MigratedTableIndex : Table<67> {
        struct OwnerPathId :    Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :    Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion :   Column<3, NScheme::NTypeIds::Uint64> {};
        struct IndexType :      Column<4, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexType; static constexpr Type Default = NKikimrSchemeOp::EIndexTypeInvalid; };
        struct State :          Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexState; static constexpr Type Default = NKikimrSchemeOp::EIndexStateInvalid; };

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, AlterVersion, IndexType, State>;
    };

    struct TableIndexAlterData : Table<39> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct AlterVersion :   Column<3, NScheme::NTypeIds::Uint64> {};
        struct IndexType :      Column<4, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexType; static constexpr Type Default = NKikimrSchemeOp::EIndexTypeInvalid; };
        struct State :          Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexState; static constexpr Type Default = NKikimrSchemeOp::EIndexStateInvalid; };

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, IndexType, State>;
    };

    struct TableIndexKeys : Table<40> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct KeyId :          Column<2, NScheme::NTypeIds::Uint32> {};
        struct KeyName :        Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, KeyId>;
        using TColumns = TableColumns<PathId, KeyId, KeyName>;
    };

    struct MigratedTableIndexKeys : Table<68> {
        struct OwnerPathId :  Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId :  Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct KeyId :        Column<3, NScheme::NTypeIds::Uint32> {};
        struct KeyName :      Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<OwnerPathId, LocalPathId, KeyId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, KeyId, KeyName>;
    };

    struct TableIndexKeysAlterData : Table<41> {
        struct PathId :         Column<1, NScheme::NTypeIds::Uint64> {};
        struct KeyId :          Column<2, NScheme::NTypeIds::Uint32> {};
        struct KeyName :        Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathId, KeyId>;
        using TColumns = TableColumns<PathId, KeyId, KeyName>;
    };

    struct TxInFlightV2 : Table<42> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TxPartId : Column<2, NScheme::NTypeIds::Uint32> { using Type = TSubTxId; };

        struct TxType : Column<3, NScheme::NTypeIds::Byte>   {};
        struct TargetPathId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct State : Column<5, NScheme::NTypeIds::Byte>   {};
        struct MinStep : Column<6, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct ExtraBytes : Column<7, NScheme::NTypeIds::String> {};
        struct StartTime : Column<8, NScheme::NTypeIds::Uint64> {};
        struct DataTotalSize : Column<9, NScheme::NTypeIds::Uint64> {};
        struct CancelBackup : Column<10, NScheme::NTypeIds::Bool>  {};
        struct TargetOwnerPathId : Column<11, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };
        struct BuildIndexId : Column<12, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct SourceOwnerId : Column<13, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; static constexpr Type Default = InvalidOwnerId; };
        struct SourceLocalPathId : Column<14, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; static constexpr Type Default = InvalidLocalPathId; };
        struct PlanStep : Column<15, NScheme::NTypeIds::Uint64> { using Type = TStepId; };
        struct NeedUpdateObject : Column<16, NScheme::NTypeIds::Bool> {};
        struct NeedSyncHive : Column<17, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<TxId, TxPartId>;
        using TColumns = TableColumns<
            TxId,
            TxPartId,
            TxType,
            TargetPathId,
            State,
            MinStep,
            ExtraBytes,
            StartTime,
            DataTotalSize,
            CancelBackup,
            TargetOwnerPathId,
            BuildIndexId,
            SourceOwnerId,
            SourceLocalPathId,
            PlanStep,
            NeedUpdateObject,
            NeedSyncHive
        >;
    };

    struct TxDependenciesV2 : Table<43> { // has died before release
        struct TxId :               Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TxPartId :           Column<2, NScheme::NTypeIds::Uint32> { using Type = TSubTxId; };
        struct DependentTxId :      Column<3, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct DependentTxPartId :  Column<4, NScheme::NTypeIds::Uint32> { using Type = TSubTxId; };

        using TKey = TableKey<TxId, TxPartId, DependentTxId, DependentTxPartId>;
        using TColumns = TableColumns<TxId, DependentTxId, TxPartId, DependentTxPartId>;
    };

    struct TxShardsV2 : Table<44> {
        struct TxId :           Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TxPartId :       Column<2, NScheme::NTypeIds::Uint32> { using Type = TSubTxId; };
        struct ShardIdx :       Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct Operation :      Column<4, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<TxId, TxPartId, ShardIdx>;
        using TColumns = TableColumns<TxId, ShardIdx, Operation, TxPartId>;
    };

    struct MigratedTxShards : Table<61> {
        struct TxId :           Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TxPartId :       Column<2, NScheme::NTypeIds::Uint32> { using Type = TSubTxId; };
        struct ShardOwnerId :   Column<3, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct ShardLocalIdx :  Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct Operation :      Column<5, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<TxId, TxPartId, ShardOwnerId, ShardLocalIdx>;
        using TColumns = TableColumns<TxId, ShardOwnerId, ShardLocalIdx, Operation, TxPartId>;
    };

    struct Exports : Table<45> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Uid : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Kind : Column<12, NScheme::NTypeIds::Byte> {};
        struct Settings : Column<3, NScheme::NTypeIds::String> {};
        struct DomainPathId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct UserSID : Column<13, NScheme::NTypeIds::Utf8> {};
        struct Items : Column<5, NScheme::NTypeIds::Uint32> {};

        struct ExportPathId : Column<6, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct State : Column<7, NScheme::NTypeIds::Byte> {};
        struct WaitTxId : Column<8, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct Issue : Column<9, NScheme::NTypeIds::Utf8> {};
        struct ExportOwnerPathId : Column<10, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct DomainPathOwnerId : Column<11, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };

        struct StartTime : Column<14, NScheme::NTypeIds::Uint64> {};
        struct EndTime : Column<15, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            Uid,
            Settings,
            DomainPathId,
            Items,
            ExportPathId,
            State,
            WaitTxId,
            Issue,
            ExportOwnerPathId,
            DomainPathOwnerId,
            Kind,
            UserSID,
            StartTime,
            EndTime
        >;
    };

    struct ExportItems : Table<46> {
        struct ExportId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Index : Column<2, NScheme::NTypeIds::Uint32> {};
        struct SourcePathName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct SourcePathId : Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        struct State : Column<5, NScheme::NTypeIds::Byte> {};
        struct BackupTxId : Column<6, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct Issue : Column<7, NScheme::NTypeIds::Utf8> {};
        struct SourceOwnerPathId : Column<8, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };

        using TKey = TableKey<ExportId, Index>;
        using TColumns = TableColumns<
            ExportId,
            Index,
            SourcePathName,
            SourcePathId,
            State,
            BackupTxId,
            Issue,
            SourceOwnerPathId
        >;
    };

    struct TableShardPartitionConfigs : Table<47> {
        struct ShardIdx : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct PartitionConfig : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ShardIdx>;
        using TColumns = TableColumns<
            ShardIdx,
            PartitionConfig
        >;
    };

    struct MigratedTableShardPartitionConfigs : Table<57> {
        struct OwnerShardIdx : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };
        struct PartitionConfig : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerShardIdx, LocalShardIdx>;
        using TColumns = TableColumns<
            OwnerShardIdx,
            LocalShardIdx,
            PartitionConfig
        >;
    };

    struct PublishingPaths : Table<48> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct PathId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Version : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId, PathId, Version>;
        using TColumns = TableColumns<TxId, PathId, Version>;
    };

    struct MigratedPublishingPaths : Table<59> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct PathOwnerId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct LocalPathId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Version : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId, PathOwnerId, LocalPathId, Version>;
        using TColumns = TableColumns<TxId, PathOwnerId, LocalPathId, Version>;
    };

    struct FillIndexDesc : Table<49> { // DEPRECATED, left only for forbidding reuse table Id 49
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Body : Column<2,  NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, Body>;
    };

    // TODO: Snapshot {PlanStep, PathId, AlterVersion}

    struct RevertedMigrations : Table<58> {
        // it is only because we need to manage undo of upgrade subdomain, finally remove it
        struct LocalPathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct SchemeShardId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TTabletId; };

        using TKey = TableKey<LocalPathId, SchemeShardId>;
        using TColumns = TableColumns<LocalPathId, SchemeShardId>;
    };

    struct IndexBuild : Table<69> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TIndexBuildId; };
        struct Uid : Column<2, NScheme::NTypeIds::Utf8> {};

        struct DomainOwnerId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct DomainLocalId : Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct TableOwnerId : Column<5, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct TableLocalId : Column<6, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct IndexName : Column<7, NScheme::NTypeIds::Utf8> {};
        struct IndexType : Column<8, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::EIndexType; };

        struct State : Column<9, NScheme::NTypeIds::Uint32> {};
        struct Issue : Column<10, NScheme::NTypeIds::Utf8> {};

        struct InitiateTxId : Column<11, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct InitiateTxStatus : Column<12, NScheme::NTypeIds::Uint32> { using Type = NKikimrScheme::EStatus; };
        struct InitiateTxDone : Column<13, NScheme::NTypeIds::Bool> {};

        struct LockTxId : Column<14, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct LockTxStatus : Column<15, NScheme::NTypeIds::Uint32> { using Type = NKikimrScheme::EStatus; };
        struct LockTxDone : Column<16, NScheme::NTypeIds::Bool> {};

        struct MaxBatchRows : Column<17, NScheme::NTypeIds::Uint32> {};
        struct MaxBatchBytes : Column<18, NScheme::NTypeIds::Uint64> {};
        struct MaxShards : Column<19, NScheme::NTypeIds::Uint32> {};

        struct ApplyTxId : Column<20, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct ApplyTxStatus : Column<21, NScheme::NTypeIds::Uint32> { using Type = NKikimrScheme::EStatus; };
        struct ApplyTxDone : Column<22, NScheme::NTypeIds::Bool> {};

        struct UnlockTxId : Column<23, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct UnlockTxStatus : Column<24, NScheme::NTypeIds::Uint32> { using Type = NKikimrScheme::EStatus; };
        struct UnlockTxDone : Column<25, NScheme::NTypeIds::Bool> {};

        struct CancelRequest : Column<26, NScheme::NTypeIds::Bool> {};

        struct MaxRetries : Column<27, NScheme::NTypeIds::Uint32> {};

        struct RowsBilled : Column<28, NScheme::NTypeIds::Uint64> {};
        struct BytesBilled : Column<29, NScheme::NTypeIds::Uint64> {};

        struct BuildKind : Column<30, NScheme::NTypeIds::Uint32> {};

        struct AlterMainTableTxId : Column<31, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct AlterMainTableTxStatus : Column<32, NScheme::NTypeIds::Uint32> { using Type = NKikimrScheme::EStatus; };
        struct AlterMainTableTxDone : Column<33, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            Uid,
            DomainOwnerId,
            DomainLocalId,
            TableOwnerId,
            TableLocalId,
            IndexName,
            IndexType,
            State,
            Issue,
            InitiateTxId,
            InitiateTxStatus,
            InitiateTxDone,
            LockTxId,
            LockTxStatus,
            LockTxDone,
            MaxBatchRows,
            MaxBatchBytes,
            MaxShards,
            ApplyTxId,
            ApplyTxStatus,
            ApplyTxDone,
            UnlockTxId,
            UnlockTxStatus,
            UnlockTxDone,
            CancelRequest,
            MaxRetries,
            RowsBilled,
            BytesBilled,
            BuildKind,
            AlterMainTableTxId,
            AlterMainTableTxStatus,
            AlterMainTableTxDone
        >;
    };

    struct IndexBuildColumns : Table<70> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TIndexBuildId; };
        struct ColumnNo : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ColumnName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct ColumnKind : Column<4, NScheme::NTypeIds::Uint8> { using Type = EIndexColumnKind; };

        using TKey = TableKey<Id, ColumnNo>;
        using TColumns = TableColumns<
            Id,
            ColumnNo,
            ColumnName,
            ColumnKind
        >;
    };

    struct SnapshotTables : Table<71> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct TableOwnerId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct TableLocalId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };

        using TKey = TableKey<Id, TableOwnerId, TableLocalId>;
        using TColumns = TableColumns<
            Id,
            TableOwnerId,
            TableLocalId
        >;
    };

    struct SnapshotSteps : Table<72> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct StepId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TStepId; };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            StepId
        >;
    };

    struct LongLocks : Table<73> {
        struct PathOwnerId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct PathLocalId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct LockId : Column<3, NScheme::NTypeIds::Uint64> { using Type = TTxId; };

        using TKey = TableKey<PathOwnerId, PathLocalId>;
        using TColumns = TableColumns<
            PathOwnerId,
            PathLocalId,
            LockId
        >;
    };

    struct IndexBuildShardStatus : Table<74> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TIndexBuildId; };
        struct OwnerShardIdx : Column<2, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardIdx : Column<3, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct Range : Column<4, NScheme::NTypeIds::String> { using Type = NKikimrTx::TKeyRange; };
        struct LastKeyAck : Column<5, NScheme::NTypeIds::String> { using Type = TString; };

        struct Status : Column<6, NScheme::NTypeIds::Uint32> { using Type = NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus; };
        struct Message : Column<7, NScheme::NTypeIds::Utf8> {};
        struct UploadStatus : Column<8, NScheme::NTypeIds::Uint32> { using Type = Ydb::StatusIds::StatusCode; };

        struct RowsProcessed : Column<9, NScheme::NTypeIds::Uint64> {};
        struct BytesProcessed : Column<10, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id, OwnerShardIdx, LocalShardIdx>;
        using TColumns = TableColumns<
            Id,
            OwnerShardIdx,
            LocalShardIdx,
            Range,
            LastKeyAck,
            Status,
            Message,
            UploadStatus,
            RowsProcessed,
            BytesProcessed
        >;
    };

    struct TableIndexDataColumns : Table<80> {
        struct PathOwnerId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct PathLocalId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct DataColumnId : Column<3, NScheme::NTypeIds::Uint32> {};
        struct DataColumnName : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathOwnerId, PathLocalId, DataColumnId>;
        using TColumns = TableColumns<PathOwnerId, PathLocalId, DataColumnId, DataColumnName>;
    };

    struct TableIndexDataColumnsAlterData : Table<81> {
        struct PathOwnerId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct PathLocalId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct DataColumnId : Column<3, NScheme::NTypeIds::Uint32> {};
        struct DataColumnName : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<PathOwnerId, PathLocalId, DataColumnId>;
        using TColumns = TableColumns<PathOwnerId, PathLocalId, DataColumnId, DataColumnName>;
    };

    struct RestoreTasks : Table<82> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Task : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            Task
        >;
    };

    struct Imports : Table<85> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Uid : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Kind : Column<3, NScheme::NTypeIds::Byte> {};
        struct Settings : Column<4, NScheme::NTypeIds::String> {};
        struct DomainPathOwnerId : Column<5, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct DomainPathLocalId : Column<6, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct UserSID : Column<10, NScheme::NTypeIds::Utf8> {};
        struct Items : Column<7, NScheme::NTypeIds::Uint32> {};

        struct State : Column<8, NScheme::NTypeIds::Byte> {};
        struct Issue : Column<9, NScheme::NTypeIds::Utf8> {};

        struct StartTime : Column<11, NScheme::NTypeIds::Uint64> {};
        struct EndTime : Column<12, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            Uid,
            Kind,
            Settings,
            DomainPathOwnerId,
            DomainPathLocalId,
            Items,
            State,
            Issue,
            UserSID,
            StartTime,
            EndTime
        >;
    };

    struct ImportItems : Table<86> {
        struct ImportId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Index : Column<2, NScheme::NTypeIds::Uint32> {};
        struct DstPathName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct DstPathOwnerId : Column<4, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct DstPathLocalId : Column<5, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct Scheme : Column<6, NScheme::NTypeIds::String> {};

        struct State : Column<7, NScheme::NTypeIds::Byte> {};
        struct WaitTxId : Column<8, NScheme::NTypeIds::Uint64> { using Type = TTxId; };
        struct NextIndexIdx : Column<9, NScheme::NTypeIds::Uint32> {};
        struct Issue : Column<10, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<ImportId, Index>;
        using TColumns = TableColumns<
            ImportId,
            Index,
            DstPathName,
            DstPathOwnerId,
            DstPathLocalId,
            Scheme,
            State,
            WaitTxId,
            NextIndexIdx,
            Issue
        >;
    };

    struct OlapStores : Table<88> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TColumnStoreDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TColumnStoreSharding

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding>;
    };

    struct OlapStoresAlters : Table<89> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TOlapStoreDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TColumnStoreSharding
        struct AlterBody : Column<5, NScheme::NTypeIds::String> {}; // TAlterOlapStore

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding, AlterBody>;
    };

    struct ColumnTables : Table<90> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TColumnTableDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TColumnTableSharding
        struct StandaloneSharding : Column<5, NScheme::NTypeIds::String> {}; // TColumnStoreSharding

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding, StandaloneSharding>;
    };

    struct ColumnTablesAlters : Table<91> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TColumnTableDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TColumnTableSharding
        struct AlterBody : Column<5, NScheme::NTypeIds::String> {}; // TAlterColumnTable
        struct StandaloneSharding : Column<6, NScheme::NTypeIds::String> {}; // TColumnStoreSharding

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding, AlterBody, StandaloneSharding>;
    };

    struct LoginKeys : Table<92> {
        struct KeyId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct KeyDataPEM : Column<2, NScheme::NTypeIds::String> {};
        struct ExpiresAt : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<KeyId>;
        using TColumns = TableColumns<KeyId, KeyDataPEM, ExpiresAt>;
    };

    struct LoginSids : Table<93> {
        struct SidName : Column<1, NScheme::NTypeIds::String> {};
        struct SidType : Column<2, NScheme::NTypeIds::Uint64> { using Type = NLoginProto::ESidType::SidType; };
        struct SidHash : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SidName>;
        using TColumns = TableColumns<SidName, SidType, SidHash>;
    };

    struct LoginSidMembers : Table<94> {
        struct SidName : Column<1, NScheme::NTypeIds::String> {};
        struct SidMember : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SidName, SidMember>;
        using TColumns = TableColumns<SidName, SidMember>;
    };

    struct CdcStream : Table<95> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct State : Column<4, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamState; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamStateInvalid; };
        struct Mode : Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamMode; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamModeInvalid; };
        struct Format : Column<6, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamFormat; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamFormatInvalid; };
        struct VirtualTimestamps : Column<7, NScheme::NTypeIds::Bool> {};
        struct AwsRegion : Column<8, NScheme::NTypeIds::Utf8> {};
        struct ResolvedTimestampsIntervalMs : Column<9, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            AlterVersion,
            State,
            Mode,
            Format,
            VirtualTimestamps,
            AwsRegion,
            ResolvedTimestampsIntervalMs
        >;
    };

    struct CdcStreamAlterData : Table<96> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct State : Column<4, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamState; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamStateInvalid; };
        struct Mode : Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamMode; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamModeInvalid; };
        struct Format : Column<6, NScheme::NTypeIds::Uint32> { using Type = NKikimrSchemeOp::ECdcStreamFormat; static constexpr Type Default = NKikimrSchemeOp::ECdcStreamFormatInvalid; };
        struct VirtualTimestamps : Column<7, NScheme::NTypeIds::Bool> {};
        struct AwsRegion : Column<8, NScheme::NTypeIds::Utf8> {};
        struct ResolvedTimestampsIntervalMs : Column<9, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            AlterVersion,
            State,
            Mode,
            Format,
            VirtualTimestamps,
            AwsRegion,
            ResolvedTimestampsIntervalMs
        >;
    };

    struct CdcStreamScanShardStatus : Table<103> {
        // path id of cdc stream
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        // shard idx of datashard
        struct OwnerShardIdx : Column<3, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalShardIdx : Column<4, NScheme::NTypeIds::Uint64> { using Type = TLocalShardIdx; };

        struct Status : Column<5, NScheme::NTypeIds::Uint32> { using Type = NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus; };

        using TKey = TableKey<OwnerPathId, LocalPathId, OwnerShardIdx, LocalShardIdx>;
        using TColumns = TableColumns<
            OwnerPathId,
            LocalPathId,
            OwnerShardIdx,
            LocalShardIdx,
            Status
        >;
    };

    struct Sequences : Table<97> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TSequenceDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TSequenceSharding

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding>;
    };

    struct SequencesAlters : Table<98> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {}; // TSequenceDescription
        struct Sharding : Column<4, NScheme::NTypeIds::String> {}; // TSequenceSharding

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description, Sharding>;
    };

    struct Replications : Table<99> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description>;
    };

    struct ReplicationsAlterData : Table<100> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description>;
    };

    struct BlobDepots : Table<102> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Description : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, Description>;
    };

    struct ExternalTable : Table<104> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct SourceType : Column<4, NScheme::NTypeIds::Utf8> {};
        struct DataSourcePath : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Location : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Content : Column<7, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, SourceType, DataSourcePath, Location, AlterVersion, Content>;
    };

    struct ExternalDataSource : Table<105> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct SourceType : Column<4, NScheme::NTypeIds::Utf8> {};
        struct Location : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Installation : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Auth : Column<7, NScheme::NTypeIds::String> {};
        struct ExternalTableReferences : Column<8, NScheme::NTypeIds::String> {};
        struct Properties : Column<9, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, AlterVersion, SourceType, Location, Installation, Auth, ExternalTableReferences, Properties>;
    };

    struct PersQueueGroupStats : Table<106> {
        struct PathId :          Column<1, NScheme::NTypeIds::Uint64> {};

        struct SeqNoGeneration : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SeqNoRound :      Column<3, NScheme::NTypeIds::Uint64> {};

        struct DataSize :        Column<4, NScheme::NTypeIds::Uint64> {};
        struct UsedReserveSize : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, SeqNoGeneration, SeqNoRound, DataSize, UsedReserveSize>;
    };

    struct BuildColumnOperationSettings : Table<107> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> { using Type = TIndexBuildId; };
        struct ColumnNo : Column<2, NScheme::NTypeIds::Uint64> {};
        struct ColumnName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct DefaultFromLiteral : Column<4, NScheme::NTypeIds::String> {};
        struct NotNull : Column<5, NScheme::NTypeIds::Bool> {};
        struct FamilyName : Column<6, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id, ColumnNo>;
        using TColumns = TableColumns<
            Id,
            ColumnNo,
            ColumnName,
            DefaultFromLiteral,
            NotNull,
            FamilyName
        >;
    };

    struct View: Table<108> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion: Column<2, NScheme::NTypeIds::Uint64> {};
        struct QueryText: Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, AlterVersion, QueryText>;
    };

    struct BackgroundSessions: Table<109> {
        struct ClassName: Column<1, NScheme::NTypeIds::String> {};
        struct Identifier: Column<2, NScheme::NTypeIds::String> {};
        struct StatusChannel: Column<3, NScheme::NTypeIds::String> {};
        struct LogicDescription: Column<4, NScheme::NTypeIds::String> {};
        struct Progress: Column<5, NScheme::NTypeIds::String> {};
        struct State: Column<6, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ClassName, Identifier>;
        using TColumns = TableColumns<ClassName, Identifier, StatusChannel, LogicDescription, Progress, State>;
    };

    struct ResourcePool : Table<110> {
        struct OwnerPathId : Column<1, NScheme::NTypeIds::Uint64> { using Type = TOwnerId; };
        struct LocalPathId : Column<2, NScheme::NTypeIds::Uint64> { using Type = TLocalPathId; };
        struct AlterVersion : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Properties : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<OwnerPathId, LocalPathId>;
        using TColumns = TableColumns<OwnerPathId, LocalPathId, AlterVersion, Properties>;
    };

    using TTables = SchemaTables<
        Paths,
        TxInFlight,
        TxDependencies,
        SysParams,
        Tables,
        Columns,
        Shards,
        TablePartitions,
        PersQueueGroups,
        PersQueues,
        RtmrVolumes,
        RTMRPartitions,
        TxShards,
        PersQueueGroupAlters,
        ColumnAlters,
        ShardsToDelete,
        BackupSettings,
        CompletedBackups,
        ShardBackupStatus,
        SubDomains,
        SubDomainShards,
        BlockStorePartitions,
        BlockStoreVolumes,
        BlockStoreVolumeAlters,
        KesusInfos,
        KesusAlters,
        StoragePools,
        ChannelsBinding,
        SolomonVolumes,
        SolomonPartitions,
        AdoptedShards,
        SubDomainsAlterData,
        SubDomainShardsAlterData,
        StoragePoolsAlterData,
        UserAttributes,
        UserAttributesAlterData,
        TableIndex,
        TableIndexAlterData,
        TableIndexKeys,
        TableIndexKeysAlterData,
        TxInFlightV2,
        TxDependenciesV2,
        TxShardsV2,
        Exports,
        ExportItems,
        TableShardPartitionConfigs,
        PublishingPaths,
        FillIndexDesc,
        MigratedPaths,
        MigratedUserAttributes,
        MigratedShards,
        MigratedChannelsBinding,
        MigratedTables,
        MigratedColumns,
        MigratedTablePartitions,
        MigratedTableShardPartitionConfigs,
        RevertedMigrations,
        MigratedPublishingPaths,
        MigratedShardsToDelete,
        MigratedTxShards,
        MigratedUserAttributesAlterData,
        MigratedColumnAlters,
        MigratedBackupSettings,
        MigratedCompletedBackups,
        MigratedShardBackupStatus,
        MigratedTableIndex,
        MigratedTableIndexKeys,
        IndexBuild,
        IndexBuildColumns,
        SnapshotTables,
        SnapshotSteps,
        LongLocks,
        IndexBuildShardStatus,
        AlterSolomonVolumes,
        AlterSolomonPartitions,
        TablePartitionStats,
        SubDomainSchemeQuotas,
        FileStoreInfos,
        FileStoreAlters,
        TableIndexDataColumns,
        TableIndexDataColumnsAlterData,
        RestoreTasks,
        MigratedKesusInfos,
        MigratedKesusAlters,
        Imports,
        ImportItems,
        TxShardStatus,
        OlapStores,
        OlapStoresAlters,
        ColumnTables,
        ColumnTablesAlters,
        LoginKeys,
        LoginSids,
        LoginSidMembers,
        CdcStream,
        CdcStreamAlterData,
        Sequences,
        SequencesAlters,
        Replications,
        ReplicationsAlterData,
        BlobDepots,
        CdcStreamScanShardStatus,
        ExternalTable,
        ExternalDataSource,
        PersQueueGroupStats,
        BuildColumnOperationSettings,
        View,
        BackgroundSessions,
        ResourcePool
    >;

    static constexpr ui64 SysParam_NextPathId = 1;
    static constexpr ui64 SysParam_NextShardIdx = 2;
    static constexpr ui64 SysParam_IsReadOnlyMode = 3;
    static constexpr ui64 SysParam_ParentDomainSchemeShard = 4;
    static constexpr ui64 SysParam_ParentDomainPathId = 5;
    static constexpr ui64 SysParam_ParentDomainOwner = 6;
    static constexpr ui64 SysParam_ParentDomainEffectiveACL = 7;
    static constexpr ui64 SysParam_ParentDomainEffectiveACLVersion = 8;
    static constexpr ui64 SysParam_TenantInitState = 9;
    static constexpr ui64 SysParam_ServerlessStorageLastBillTime = 10;
    static constexpr ui64 SysParam_MaxIncompatibleChange = 11;

    // List of incompatible changes:
    // * Change 1: store migrated shards of local tables (e.g. after a rename) as a migrated record
    static constexpr ui64 MaxIncompatibleChangeSupported = 1;
};

}
