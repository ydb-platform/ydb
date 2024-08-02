#pragma once
#include "defs.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/operations/write.h>

#include <type_traits>

namespace NKikimr::NOlap {
class TColumnChunkLoadContext;
}

namespace NKikimr::NColumnShard {

using NOlap::TWriteId;
using NOlap::IBlobGroupSelector;
struct TFullTxInfo;

struct Schema : NIceDb::Schema {
    // These settings are persisted on each Init. So we use empty settings in order not to overwrite what
    // was changed by the user
    struct EmptySettings {
        static void Materialize(NIceDb::TToughDb&) {}
    };

    using TSettings = SchemaSettings<EmptySettings>;

    using TInsertedData = NOlap::TInsertedData;
    using TColumnRecord = NOlap::TColumnRecord;

    enum EIndexTables : ui32 {
        InsertTableId = 255,
        GranulesTableId,
        ColumnsTableId,
        CountersTableId,
        OperationsTableId,
        IndexesTableId,

        LocksTableId,
        LockRangesTableId,
        LockConflictsTableId,
        LockVolatileDependenciesTableId,

        SharedBlobIdsTableId,
        BorrowedBlobIdsTableId,
        SourceSessionsTableId,
        DestinationSessionsTableId,
        OperationTxIdsId,
        BackupIdsDeprecated,
        ExportSessionsId,
        PortionsTableId,
        BackgroundSessionsTableId,
        ShardingInfoTableId,
        RepairsTableId,
        NormalizersTableId,
        NormalizerEventsTableId
    };

    enum class ETierTables: ui32 {
        TierBlobsDraft = 1024,
        TierBlobsToKeep,
        TierBlobsToDelete,
        TierBlobsToDeleteWT
    };

    enum class EValueIds : ui32 {
        CurrentSchemeShardId = 1,
        ProcessingParams = 2,
        LastWriteId = 3,
        LastPlannedStep = 4,
        LastPlannedTxId = 5,
        LastSchemaSeqNoGeneration = 6,
        LastSchemaSeqNoRound = 7,
        LastGcBarrierGen = 8,
        LastGcBarrierStep = 9,
        LastExportNumber = 10,
        OwnerPathId = 11,
        OwnerPath = 12,
        LastCompletedStep = 13,
        LastCompletedTxId = 14,
        LastNormalizerSequentialId = 15,
        GCBarrierPreparationGen = 16,
        GCBarrierPreparationStep = 17
    };

    enum class EInsertTableIds : ui8 {
        Inserted = 0,
        Committed = 1,
        Aborted = 2,
    };

    enum class ECommonTables {
        Value = 1,
        TxInfo = 2,
        SchemaPresetInfo = 3,
        TtlSettingsPresetInfo = 4,
        TableInfo = 5,
        LongTxWrites = 6,
        BlobsToKeep = 7,
        BlobsToDelete = 8,
        SchemaPresetVersionInfo = 9,
        TtlSettingsPresetVersionInfo = 10,
        TableVersionInfo = 11,
        SmallBlobs = 12,
        OneToOneEvictedBlobs = 13,
        BlobsToDeleteWT = 14,
        InFlightSnapshots = 15
    };

    // Tablet tables

    struct Value : Table<(ui32)ECommonTables::Value> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {}; // one of EValueIds
        struct Digit : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Bytes : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Digit, Bytes>;
    };

    struct TxInfo : Table<(ui32)ECommonTables::TxInfo> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TxKind : Column<2, NScheme::NTypeIds::Uint32> { using Type = NKikimrTxColumnShard::ETransactionKind; };
        struct TxBody : Column<3, NScheme::NTypeIds::String> {};
        struct MaxStep : Column<4, NScheme::NTypeIds::Uint64> {};
        struct PlanStep : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Source : Column<6, NScheme::NTypeIds::ActorId> {};
        struct Cookie: Column<7, NScheme::NTypeIds::Uint64> {};
        struct SeqNo: Column<8, NScheme::NTypeIds::String> {};

        using TKey = TableKey<TxId>;
        using TColumns = TableColumns<TxId, TxKind, TxBody, MaxStep, PlanStep, Source, Cookie, SeqNo>;
    };

    struct SchemaPresetInfo : Table<(ui32)ECommonTables::SchemaPresetInfo> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DropStep : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Name, DropStep, DropTxId>;
    };

    struct SchemaPresetVersionInfo : Table<(ui32)ECommonTables::SchemaPresetVersionInfo> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TCommonSchemaVersionInfo

        using TKey = TableKey<Id, SinceStep, SinceTxId>;
        using TColumns = TableColumns<Id, SinceStep, SinceTxId, InfoProto>;
    };

    struct TtlSettingsPresetInfo : Table<(ui32)ECommonTables::TtlSettingsPresetInfo> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DropStep : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Name, DropStep, DropTxId>;
    };

    struct TtlSettingsPresetVersionInfo : Table<(ui32)ECommonTables::TtlSettingsPresetVersionInfo> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TTtlSettingsPresetVersionInfo

        using TKey = TableKey<Id, SinceStep, SinceTxId>;
        using TColumns = TableColumns<Id, SinceStep, SinceTxId, InfoProto>;
    };

    struct TableInfo : Table<(ui32)ECommonTables::TableInfo> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct DropStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct TieringUsage: Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, DropStep, DropTxId, TieringUsage>;
    };

    struct TableVersionInfo : Table<(ui32)ECommonTables::TableVersionInfo> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TTableVersionInfo

        using TKey = TableKey<PathId, SinceStep, SinceTxId>;
        using TColumns = TableColumns<PathId, SinceStep, SinceTxId, InfoProto>;
    };

    struct LongTxWrites : Table<(ui32)ECommonTables::LongTxWrites> {
        struct WriteId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct LongTxId : Column<2, NScheme::NTypeIds::String> {};
        struct WritePartId: Column<3, NScheme::NTypeIds::Uint32> {};
        struct GranuleShardingVersion: Column<4, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<WriteId>;
        using TColumns = TableColumns<WriteId, LongTxId, WritePartId, GranuleShardingVersion>;
    };

    struct BlobsToKeep : Table<(ui32)ECommonTables::BlobsToKeep> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId>;
    };

    struct BlobsToDelete: Table<(ui32)ECommonTables::BlobsToDelete> {
        struct BlobId: Column<1, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId>;
    };

    struct SmallBlobs : Table<(ui32)ECommonTables::SmallBlobs> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};
        struct Data : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId, Data>;
    };

    struct OneToOneEvictedBlobs : Table<(ui32)ECommonTables::OneToOneEvictedBlobs> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};
        struct Size : Column<2, NScheme::NTypeIds::Uint32> {}; // extracted from BlobId for better introspection
        struct State : Column<3, NScheme::NTypeIds::Byte> {}; // evicting -> (self) cached <-> exported
        struct Dropped : Column<4, NScheme::NTypeIds::Bool> {};
        struct Metadata : Column<5, NScheme::NTypeIds::String> {}; // NKikimrTxColumnShard.TEvictMetadata
        struct ExternBlobId : Column<6, NScheme::NTypeIds::String> {};
        //struct Format : Column<7, NScheme::NTypeIds::Byte> {};
        //struct CachedBlobId : Column<8, NScheme::NTypeIds::String> {}; // TODO

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId, Size, State, Dropped, Metadata, ExternBlobId>;
    };

    struct BlobsToDeleteWT: Table<(ui32)ECommonTables::BlobsToDeleteWT> {
        struct BlobId: Column<1, NScheme::NTypeIds::String> {};
        struct TabletId: Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<BlobId, TabletId>;
        using TColumns = TableColumns<BlobId, TabletId>;
    };

    struct InFlightSnapshots: Table<(ui32)ECommonTables::InFlightSnapshots> {
        struct PlanStep: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TxId: Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PlanStep, TxId>;
        using TColumns = TableColumns<PlanStep, TxId>;
    };

    // Index tables

    // InsertTable - common for all indices
    struct InsertTable : NIceDb::Schema::Table<InsertTableId> {
        struct Committed : Column<1, NScheme::NTypeIds::Byte> {};
        struct PlanStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct WriteTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct DedupId : Column<5, NScheme::NTypeIds::String> {};
        struct BlobId: Column<6, NScheme::NTypeIds::String> {};
        struct Meta : Column<7, NScheme::NTypeIds::String> {};
        struct IndexPlanStep : Column<8, NScheme::NTypeIds::Uint64> {};
        struct IndexTxId : Column<9, NScheme::NTypeIds::Uint64> {};
        struct SchemaVersion : Column<10, NScheme::NTypeIds::Uint64> {};

        struct BlobRangeOffset: Column<11, NScheme::NTypeIds::Uint64> {};
        struct BlobRangeSize: Column<12, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Committed, PlanStep, WriteTxId, PathId, DedupId>;
        using TColumns = TableColumns<Committed, PlanStep, WriteTxId, PathId, DedupId, BlobId, Meta, IndexPlanStep, IndexTxId, SchemaVersion, BlobRangeOffset, BlobRangeSize>;
    };

    struct IndexGranules : NIceDb::Schema::Table<GranulesTableId> {
        struct Index : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PathId : Column<2, NScheme::NTypeIds::Uint64> {};    // Logical table (if many)
        struct IndexKey : Column<3, NScheme::NTypeIds::String> {};  // Effective part of PK (serialized)
        struct Granule : Column<4, NScheme::NTypeIds::Uint64> {};   // FK: {Index, Granule} -> TIndexColumns
        struct PlanStep : Column<5, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<6, NScheme::NTypeIds::Uint64> {};
        struct Metadata : Column<7, NScheme::NTypeIds::String> {};  // NKikimrTxColumnShard.TIndexGranuleMeta

        using TKey = TableKey<Index, PathId, IndexKey>;
        using TColumns = TableColumns<Index, PathId, IndexKey, Granule, PlanStep, TxId, Metadata>;
    };

    struct IndexColumns : NIceDb::Schema::Table<ColumnsTableId> {
        struct Index : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Granule : Column<2, NScheme::NTypeIds::Uint64> {};
        struct ColumnIdx : Column<3, NScheme::NTypeIds::Uint32> {};
        struct PlanStep : Column<4, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Portion : Column<6, NScheme::NTypeIds::Uint64> {};
        struct Chunk : Column<7, NScheme::NTypeIds::Uint32> {};
        struct XPlanStep : Column<8, NScheme::NTypeIds::Uint64> {};
        struct XTxId : Column<9, NScheme::NTypeIds::Uint64> {};
        struct Blob : Column<10, NScheme::NTypeIds::String> {};
        struct Metadata : Column<11, NScheme::NTypeIds::String> {}; // NKikimrTxColumnShard.TIndexColumnMeta
        struct Offset : Column<12, NScheme::NTypeIds::Uint32> {};
        struct Size : Column<13, NScheme::NTypeIds::Uint32> {};
        struct PathId : Column<14, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Index, Granule, ColumnIdx, PlanStep, TxId, Portion, Chunk>;
        using TColumns = TableColumns<Index, Granule, ColumnIdx, PlanStep, TxId, Portion, Chunk,
                                    XPlanStep, XTxId, Blob, Metadata, Offset, Size, PathId>;
    };

    struct IndexCounters : NIceDb::Schema::Table<CountersTableId> {
        struct Index : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Counter : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ValueUI64 : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Index, Counter>;
        using TColumns = TableColumns<Index, Counter, ValueUI64>;
    };

    struct Operations : NIceDb::Schema::Table<OperationsTableId> {
        struct WriteId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LockId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Status : Column<3, NScheme::NTypeIds::Uint32> {};
        struct CreatedAt : Column<4, NScheme::NTypeIds::Uint64> {};
        struct GlobalWriteId : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Metadata : Column<6, NScheme::NTypeIds::String> {};
        struct Cookie: Column<7, NScheme::NTypeIds::Uint64> {};
        struct GranuleShardingVersionId: Column<8, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<WriteId>;
        using TColumns = TableColumns<LockId, WriteId, Status, CreatedAt, GlobalWriteId, Metadata, Cookie, GranuleShardingVersionId>;
    };

    struct OperationTxIds : NIceDb::Schema::Table<OperationTxIdsId> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LockId : Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId, LockId>;
        using TColumns = TableColumns<TxId, LockId>;
    };

    struct TierBlobsDraft: NIceDb::Schema::Table<(ui32)ETierTables::TierBlobsDraft> {
        struct StorageId: Column<1, NScheme::NTypeIds::String> {};
        struct BlobId: Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<StorageId, BlobId>;
        using TColumns = TableColumns<StorageId, BlobId>;
    };

    struct TierBlobsToDelete: NIceDb::Schema::Table<(ui32)ETierTables::TierBlobsToDelete> {
        struct StorageId: Column<1, NScheme::NTypeIds::String> {};
        struct BlobId: Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<StorageId, BlobId>;
        using TColumns = TableColumns<StorageId, BlobId>;
    };

    struct TierBlobsToDeleteWT: NIceDb::Schema::Table<(ui32)ETierTables::TierBlobsToDeleteWT> {
        struct StorageId: Column<1, NScheme::NTypeIds::String> {};
        struct BlobId: Column<2, NScheme::NTypeIds::String> {};
        struct TabletId: Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<StorageId, BlobId, TabletId>;
        using TColumns = TableColumns<StorageId, BlobId, TabletId>;
    };

    struct IndexIndexes: NIceDb::Schema::Table<IndexesTableId> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct PortionId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct IndexId: Column<3, NScheme::NTypeIds::Uint32> {};
        struct ChunkIdx: Column<4, NScheme::NTypeIds::Uint32> {};
        struct Blob: Column<5, NScheme::NTypeIds::String> {};
        struct Offset: Column<6, NScheme::NTypeIds::Uint32> {};
        struct Size: Column<7, NScheme::NTypeIds::Uint32> {};
        struct RecordsCount: Column<8, NScheme::NTypeIds::Uint32> {};
        struct RawBytes: Column<9, NScheme::NTypeIds::Uint64> {};
        struct BlobData: Column<10, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId, PortionId, IndexId, ChunkIdx>;
        using TColumns = TableColumns<PathId, PortionId, IndexId, ChunkIdx, Blob, Offset, Size, RecordsCount, RawBytes, BlobData>;
    };

    struct SharedBlobIds: NIceDb::Schema::Table<SharedBlobIdsTableId> {
        struct StorageId: Column<1, NScheme::NTypeIds::String> {};
        struct BlobId: Column<2, NScheme::NTypeIds::String> {};
        struct TabletId: Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<StorageId, BlobId, TabletId>;
        using TColumns = TableColumns<StorageId, BlobId, TabletId>;
    };

    struct BorrowedBlobIds: NIceDb::Schema::Table<BorrowedBlobIdsTableId> {
        struct StorageId: Column<1, NScheme::NTypeIds::String> {};
        struct BlobId: Column<2, NScheme::NTypeIds::String> {};
        struct TabletId: Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<StorageId, BlobId>;
        using TColumns = TableColumns<StorageId, BlobId, TabletId>;
    };

    struct SourceSessions: NIceDb::Schema::Table<SourceSessionsTableId> {
        struct SessionId: Column<1, NScheme::NTypeIds::String> {};
        struct Details: Column<2, NScheme::NTypeIds::String> {};
        struct CursorDynamic: Column<3, NScheme::NTypeIds::String> {};
        struct CursorStatic: Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SessionId>;
        using TColumns = TableColumns<SessionId, Details, CursorDynamic, CursorStatic>;
    };

    struct DestinationSessions: NIceDb::Schema::Table<DestinationSessionsTableId> {
        struct SessionId: Column<1, NScheme::NTypeIds::String> {};
        struct Details: Column<2, NScheme::NTypeIds::String> {};
        struct Cursor: Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SessionId>;
        using TColumns = TableColumns<SessionId, Details, Cursor>;
    };

    struct Locks : Table<LocksTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LockNodeId : Column<2, NScheme::NTypeIds::Uint32> {};
        struct Generation : Column<3, NScheme::NTypeIds::Uint32> {};
        struct Counter : Column<4, NScheme::NTypeIds::Uint64> {};
        struct CreateTimestamp : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Flags : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<LockId>;
        using TColumns = TableColumns<LockId, LockNodeId, Generation, Counter, CreateTimestamp, Flags>;
    };

    struct LockRanges : Table<LockRangesTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct RangeId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct PathOwnerId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct LocalPathId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Flags : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Data : Column<6, NScheme::NTypeIds::String> {};

        using TKey = TableKey<LockId, RangeId>;
        using TColumns = TableColumns<LockId, RangeId, PathOwnerId, LocalPathId, Flags, Data>;
    };

    struct LockConflicts : Table<LockConflictsTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct ConflictId : Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<LockId, ConflictId>;
        using TColumns = TableColumns<LockId, ConflictId>;
    };

    struct LockVolatileDependencies : Table<LockVolatileDependenciesTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<LockId, TxId>;
        using TColumns = TableColumns<LockId, TxId>;
    };

    struct IndexPortions: NIceDb::Schema::Table<PortionsTableId> {
        struct PathId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct PortionId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct SchemaVersion: Column<3, NScheme::NTypeIds::Uint64> {};
        struct XPlanStep: Column<4, NScheme::NTypeIds::Uint64> {};
        struct XTxId: Column<5, NScheme::NTypeIds::Uint64> {};
        struct Metadata: Column<6, NScheme::NTypeIds::String> {}; // NKikimrTxColumnShard.TIndexColumnMeta
        struct ShardingVersion: Column<7, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<PathId, PortionId>;
        using TColumns = TableColumns<PathId, PortionId, SchemaVersion, XPlanStep, XTxId, Metadata, ShardingVersion>;
    };

    struct BackgroundSessions: Table<BackgroundSessionsTableId> {
        struct ClassName: Column<1, NScheme::NTypeIds::String> {};
        struct Identifier: Column<2, NScheme::NTypeIds::String> {};
        struct StatusChannel: Column<3, NScheme::NTypeIds::String> {};
        struct LogicDescription: Column<4, NScheme::NTypeIds::String> {};
        struct Progress: Column<5, NScheme::NTypeIds::String> {};
        struct State: Column<6, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ClassName, Identifier>;
        using TColumns = TableColumns<ClassName, Identifier, StatusChannel, LogicDescription, Progress, State>;
    };

    struct ShardingInfo : Table<ShardingInfoTableId> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct VersionId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Snapshot : Column<3, NScheme::NTypeIds::String> {};
        struct Logic : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId, VersionId>;
        using TColumns = TableColumns<PathId, VersionId, Snapshot, Logic>;
    };

    struct Normalizers: Table<NormalizersTableId> {
        struct ClassName: Column<1, NScheme::NTypeIds::Utf8> {};
        struct Description: Column<2, NScheme::NTypeIds::Utf8> {};
        struct Identifier: Column<3, NScheme::NTypeIds::Utf8> {};
        struct Start: Column<4, NScheme::NTypeIds::Uint64> {};
        struct Finish: Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ClassName, Description, Identifier>;
        using TColumns = TableColumns<ClassName, Description, Identifier, Start, Finish>;
    };

    struct NormalizerEvents: Table<NormalizerEventsTableId> {
        struct NormalizerId: Column<1, NScheme::NTypeIds::Utf8> {};
        struct EventId: Column<2, NScheme::NTypeIds::Utf8> {};
        struct Instant: Column<3, NScheme::NTypeIds::Uint64> {};
        struct EventType: Column<4, NScheme::NTypeIds::Utf8> {};
        struct Description: Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<NormalizerId, EventId>;
        using TColumns = TableColumns<NormalizerId, EventId, Instant, EventType, Description>;
    };

    using TTables = SchemaTables<
        Value,
        TxInfo,
        SchemaPresetInfo,
        SchemaPresetVersionInfo,
        TtlSettingsPresetInfo,
        TtlSettingsPresetVersionInfo,
        TableInfo,
        TableVersionInfo,
        LongTxWrites,
        BlobsToKeep,
        BlobsToDelete,
        BlobsToDeleteWT,
        InsertTable,
        IndexGranules,
        IndexColumns,
        IndexCounters,
        SmallBlobs,
        OneToOneEvictedBlobs,
        Operations,
        TierBlobsDraft,
        TierBlobsToDelete,
        TierBlobsToDeleteWT,
        IndexIndexes,
        SharedBlobIds,
        BorrowedBlobIds,
        SourceSessions,
        DestinationSessions,
        OperationTxIds,
        IndexPortions,
        BackgroundSessions,
        ShardingInfo,
        Normalizers,
        NormalizerEvents
        >;

    //

    template <typename TTable>
    static bool Precharge(NIceDb::TNiceDb& db, const NTable::TScheme& schema)
    {
        if (schema.GetTableInfo(TTable::TableId)) {
            auto rowset = db.Table<TTable>().Range().Select();
            return rowset.IsReady();
        }
        return true;
    }

    template <typename T>
    static bool GetSpecialValue(NIceDb::TNiceDb& db, EValueIds key, T& value) {
        using TSource = std::conditional_t<std::is_integral_v<T> || std::is_enum_v<T>, Value::Digit, Value::Bytes>;

        auto rowset = db.Table<Value>().Key((ui32)key).Select<TSource>();
        if (rowset.IsReady()) {
            if (rowset.IsValid()) {
                value = T{rowset.template GetValue<TSource>()};
                return true;
            }
        }
        return false;
    }

    template <typename T>
    static bool GetSpecialValueOpt(NIceDb::TNiceDb& db, EValueIds key, T& value) {
        using TSource = std::conditional_t<std::is_integral_v<T> || std::is_enum_v<T>, Value::Digit, Value::Bytes>;

        auto rowset = db.Table<Value>().Key((ui32)key).Select<TSource>();
        if (rowset.IsReady()) {
            if (rowset.IsValid()) {
                value = T{rowset.template GetValue<TSource>()};
            }
            return true;
        }
        return false;
    }

    template <typename T>
    static bool GetSpecialValueOpt(NIceDb::TNiceDb& db, EValueIds key, std::optional<T>& value) {
        using TSource = std::conditional_t<std::is_integral_v<T> || std::is_enum_v<T>, Value::Digit, Value::Bytes>;

        auto rowset = db.Table<Value>().Key((ui32)key).Select<TSource>();
        if (rowset.IsReady()) {
            if (rowset.IsValid()) {
                value = T{ rowset.template GetValue<TSource>() };
            } else {
                value = {};
            }
            return true;
        }
        return false;
    }

    template<class TMessage>
    static bool GetSpecialProtoValue(NIceDb::TNiceDb& db, EValueIds key, std::optional<TMessage>& value) {
        auto rowset = db.Table<Value>().Key(ui32(key)).Select<Value::Bytes>();
        if (rowset.IsReady()) {
            if (rowset.IsValid()) {
                Y_ABORT_UNLESS(value.emplace().ParseFromString(rowset.GetValue<Value::Bytes>()));
            }
            return true;
        }
        return false;
    }

    static void AddNormalizerEvent(NIceDb::TNiceDb& db, const TString& normalizerId, const TString& eventType, const TString& description) {
        db.Table<NormalizerEvents>().Key(normalizerId, TGUID::CreateTimebased().AsUuidString())
            .Update(
                NIceDb::TUpdate<NormalizerEvents::Instant>(TInstant::Now().MicroSeconds()),
                NIceDb::TUpdate<NormalizerEvents::EventType>(eventType),
                NIceDb::TUpdate<NormalizerEvents::Description>(description)
            );
    }

    static void StartNormalizer(NIceDb::TNiceDb& db, const TString& className, const TString& description, const TString& normalizerId) {
        db.Table<Normalizers>().Key(className, description, normalizerId)
            .Update(
                NIceDb::TUpdate<Normalizers::Start>(TInstant::Now().MicroSeconds())
            );
    }

    static void RemoveNormalizer(NIceDb::TNiceDb& db, const TString& className, const TString& description, const TString& normalizerId) {
        db.Table<Normalizers>().Key(className, description, normalizerId).Delete();
    }

    static void FinishNormalizer(NIceDb::TNiceDb& db, const TString& className, const TString& description, const TString& normalizerId) {
        db.Table<Normalizers>().Key(className, description, normalizerId)
            .Update(
                NIceDb::TUpdate<Normalizers::Finish>(TInstant::Now().MicroSeconds())
            );
    }

    static void SaveSpecialValue(NIceDb::TNiceDb& db, EValueIds key, const TString& value) {
        db.Table<Value>().Key((ui32)key).Update(NIceDb::TUpdate<Value::Bytes>(value));
    }

    static void SaveSpecialValue(NIceDb::TNiceDb& db, EValueIds key, ui64 value) {
        db.Table<Value>().Key((ui32)key).Update(NIceDb::TUpdate<Value::Digit>(value));
    }

    template<class TMessage>
    static void SaveSpecialProtoValue(NIceDb::TNiceDb& db, EValueIds key, const TMessage& message) {
        TString serialized;
        Y_ABORT_UNLESS(message.SerializeToString(&serialized));
        SaveSpecialValue(db, key, serialized);
    }

    static void SaveTxInfo(NIceDb::TNiceDb& db, const TFullTxInfo& txInfo,
                           const TString& txBody);

    static void UpdateTxInfoSource(NIceDb::TNiceDb& db, const TFullTxInfo& txInfo);

    static void UpdateTxInfoSource(NIceDb::TNiceDb& db, ui64 txId, const TActorId& source, ui64 cookie) {
        db.Table<TxInfo>().Key(txId).Update(
            NIceDb::TUpdate<TxInfo::Source>(source),
            NIceDb::TUpdate<TxInfo::Cookie>(cookie));
    }

    static void UpdateTxInfoPlanStep(NIceDb::TNiceDb& db, ui64 txId, ui64 planStep) {
        db.Table<TxInfo>().Key(txId).Update(
            NIceDb::TUpdate<TxInfo::PlanStep>(planStep));
    }

    static void EraseTxInfo(NIceDb::TNiceDb& db, ui64 txId) {
        db.Table<TxInfo>().Key(txId).Delete();
    }

    static void SaveSchemaPresetInfo(NIceDb::TNiceDb& db, ui64 id, const TString& name) {
        db.Table<SchemaPresetInfo>().Key(id).Update(
            NIceDb::TUpdate<SchemaPresetInfo::Name>(name));
    }

    static void SaveSchemaPresetVersionInfo(
            NIceDb::TNiceDb& db,
            ui64 id, const NOlap::TSnapshot& version,
            const NKikimrTxColumnShard::TSchemaPresetVersionInfo& info)
    {
        TString serialized;
        Y_ABORT_UNLESS(info.SerializeToString(&serialized));
        db.Table<SchemaPresetVersionInfo>().Key(id, version.GetPlanStep(), version.GetTxId()).Update(
            NIceDb::TUpdate<SchemaPresetVersionInfo::InfoProto>(serialized));
    }

    static void SaveSchemaPresetDropVersion(NIceDb::TNiceDb& db, ui64 id, const NOlap::TSnapshot& dropVersion) {
        db.Table<SchemaPresetInfo>().Key(id).Update(
            NIceDb::TUpdate<SchemaPresetInfo::DropStep>(dropVersion.GetPlanStep()),
            NIceDb::TUpdate<SchemaPresetInfo::DropTxId>(dropVersion.GetTxId()));
    }

    static void EraseSchemaPresetVersionInfo(NIceDb::TNiceDb& db, ui64 id, const NOlap::TSnapshot& version) {
        db.Table<SchemaPresetVersionInfo>().Key(id, version.GetPlanStep(), version.GetTxId()).Delete();
    }

    static void EraseSchemaPresetInfo(NIceDb::TNiceDb& db, ui64 id) {
        db.Table<SchemaPresetInfo>().Key(id).Delete();
    }

    static void SaveTableInfo(NIceDb::TNiceDb& db, const ui64 pathId, const TString tieringUsage) {
        db.Table<TableInfo>().Key(pathId).Update(
            NIceDb::TUpdate<TableInfo::TieringUsage>(tieringUsage)
        );
    }


    static void SaveTableVersionInfo(
            NIceDb::TNiceDb& db,
            ui64 pathId, const NOlap::TSnapshot& version,
            const NKikimrTxColumnShard::TTableVersionInfo& info)
    {
        TString serialized;
        Y_ABORT_UNLESS(info.SerializeToString(&serialized));
        db.Table<TableVersionInfo>().Key(pathId, version.GetPlanStep(), version.GetTxId()).Update(
            NIceDb::TUpdate<TableVersionInfo::InfoProto>(serialized));
    }

    static void SaveTableDropVersion(
            NIceDb::TNiceDb& db, ui64 pathId, ui64 dropStep, ui64 dropTxId)
    {
        db.Table<TableInfo>().Key(pathId).Update(
            NIceDb::TUpdate<TableInfo::DropStep>(dropStep),
            NIceDb::TUpdate<TableInfo::DropTxId>(dropTxId));
    }

    static void EraseTableVersionInfo(NIceDb::TNiceDb& db, ui64 pathId, const NOlap::TSnapshot& version) {
        db.Table<TableVersionInfo>().Key(pathId, version.GetPlanStep(), version.GetTxId()).Delete();
    }

    static void EraseTableInfo(NIceDb::TNiceDb& db, ui64 pathId) {
        db.Table<TableInfo>().Key(pathId).Delete();
    }

    static void SaveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId, const std::optional<ui32> granuleShardingVersion) {
        NKikimrLongTxService::TLongTxId proto;
        longTxId.ToProto(&proto);
        TString serialized;
        Y_ABORT_UNLESS(proto.SerializeToString(&serialized));
        db.Table<LongTxWrites>().Key((ui64)writeId).Update(
            NIceDb::TUpdate<LongTxWrites::LongTxId>(serialized), 
            NIceDb::TUpdate<LongTxWrites::WritePartId>(writePartId), 
            NIceDb::TUpdate<LongTxWrites::GranuleShardingVersion>(granuleShardingVersion.value_or(0))
            );
    }

    static void EraseLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId) {
        db.Table<LongTxWrites>().Key((ui64)writeId).Delete();
    }

    // InsertTable activities

    static void InsertTable_Upsert(NIceDb::TNiceDb& db, EInsertTableIds recType, const TInsertedData& data) {
        db.Table<InsertTable>().Key((ui8)recType, data.PlanStep, data.WriteTxId, data.PathId, data.DedupId).Update(
            NIceDb::TUpdate<InsertTable::BlobId>(data.GetBlobRange().GetBlobId().ToStringLegacy()),
            NIceDb::TUpdate<InsertTable::BlobRangeOffset>(data.GetBlobRange().Offset),
            NIceDb::TUpdate<InsertTable::BlobRangeSize>(data.GetBlobRange().Size),
            NIceDb::TUpdate<InsertTable::Meta>(data.GetMeta().SerializeToProto().SerializeAsString()),
            NIceDb::TUpdate<InsertTable::SchemaVersion>(data.GetSchemaVersion())
        );
    }

    static void InsertTable_Erase(NIceDb::TNiceDb& db, EInsertTableIds recType, const TInsertedData& data) {
        db.Table<InsertTable>().Key((ui8)recType, data.PlanStep, data.WriteTxId, data.PathId, data.DedupId).Delete();
    }

    static void InsertTable_Insert(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Upsert(db, EInsertTableIds::Inserted, data);
    }

    static void InsertTable_Commit(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Upsert(db, EInsertTableIds::Committed, data);
    }

    static void InsertTable_Abort(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Upsert(db, EInsertTableIds::Aborted, data);
    }

    static void InsertTable_EraseInserted(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Erase(db, EInsertTableIds::Inserted, data);
    }

    static void InsertTable_EraseCommitted(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Erase(db, EInsertTableIds::Committed, data);
    }

    static void InsertTable_EraseAborted(NIceDb::TNiceDb& db, const TInsertedData& data) {
        InsertTable_Erase(db, EInsertTableIds::Aborted, data);
    }

    static bool InsertTable_Load(NIceDb::TNiceDb& db,
                                 const IBlobGroupSelector* dsGroupSelector,
                                 NOlap::TInsertTableAccessor& insertTable,
                                 const TInstant& loadTime);

    // IndexCounters

    static void IndexCounters_Write(NIceDb::TNiceDb& db, ui32 counterId, ui64 value) {
        db.Table<IndexCounters>().Key(0, counterId).Update(
            NIceDb::TUpdate<IndexCounters::ValueUI64>(value)
        );
    }

    static bool IndexCounters_Load(NIceDb::TNiceDb& db, const std::function<void(ui32 id, ui64 value)>& callback) {
        auto rowset = db.Table<IndexCounters>().Prefix(0).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui32 id = rowset.GetValue<IndexCounters::Counter>();
            ui64 value = rowset.GetValue<IndexCounters::ValueUI64>();

            callback(id, value);

            if (!rowset.Next())
                return false;
        }
        return true;
    }
};

}

namespace NKikimr::NOlap {
class TColumnChunkLoadContext {
private:
    YDB_READONLY_DEF(TBlobRange, BlobRange);
    TChunkAddress Address;
    YDB_READONLY_DEF(NKikimrTxColumnShard::TIndexColumnMeta, MetaProto);
public:
    const TChunkAddress& GetAddress() const {
        return Address;
    }

    TColumnChunkLoadContext(const TChunkAddress& address, const TBlobRange& bRange, const NKikimrTxColumnShard::TIndexColumnMeta& metaProto)
        : BlobRange(bRange)
        , Address(address)
        , MetaProto(metaProto)
    {

    }

    template <class TSource>
    TColumnChunkLoadContext(const TSource& rowset, const IBlobGroupSelector* dsGroupSelector)
        : Address(rowset.template GetValue<NColumnShard::Schema::IndexColumns::ColumnIdx>(), rowset.template GetValue<NColumnShard::Schema::IndexColumns::Chunk>()) {
        AFL_VERIFY(Address.GetColumnId())("event", "incorrect address")("address", Address.DebugString());
        TString strBlobId = rowset.template GetValue<NColumnShard::Schema::IndexColumns::Blob>();
        Y_ABORT_UNLESS(strBlobId.size() == sizeof(TLogoBlobID), "Size %" PRISZT "  doesn't match TLogoBlobID", strBlobId.size());
        TLogoBlobID logoBlobId((const ui64*)strBlobId.data());
        BlobRange.BlobId = NOlap::TUnifiedBlobId(dsGroupSelector->GetGroup(logoBlobId), logoBlobId);
        BlobRange.Offset = rowset.template GetValue<NColumnShard::Schema::IndexColumns::Offset>();
        BlobRange.Size = rowset.template GetValue<NColumnShard::Schema::IndexColumns::Size>();
        AFL_VERIFY(BlobRange.BlobId.IsValid() && BlobRange.Size)("event", "incorrect blob")("blob", BlobRange.ToString());

        const TString metadata = rowset.template GetValue<NColumnShard::Schema::IndexColumns::Metadata>();
        AFL_VERIFY(MetaProto.ParseFromArray(metadata.data(), metadata.size()))("event", "cannot parse metadata as protobuf");
    }

    const NKikimrTxColumnShard::TIndexPortionMeta* GetPortionMeta() const {
        if (MetaProto.HasPortionMeta()) {
            return &MetaProto.GetPortionMeta();
        } else {
            return nullptr;
        }
    }
};

class TIndexChunkLoadContext {
private:
    YDB_READONLY_DEF(std::optional<TBlobRange>, BlobRange);
    YDB_READONLY_DEF(std::optional<TString>, BlobData);
    TChunkAddress Address;
    const ui32 RecordsCount;
    const ui32 RawBytes;
public:
    TIndexChunk BuildIndexChunk(const TBlobRangeLink16::TLinkId blobLinkId) const {
        AFL_VERIFY(BlobRange);
        return TIndexChunk(Address.GetColumnId(), Address.GetChunkIdx(), RecordsCount, RawBytes, BlobRange->BuildLink(blobLinkId));
    }

    TIndexChunk BuildIndexChunk() const {
        AFL_VERIFY(BlobData);
        return TIndexChunk(Address.GetColumnId(), Address.GetChunkIdx(), RecordsCount, RawBytes, *BlobData);
    }

    template <class TSource>
    TIndexChunkLoadContext(const TSource& rowset, const IBlobGroupSelector* dsGroupSelector)
        : Address(rowset.template GetValue<NColumnShard::Schema::IndexIndexes::IndexId>(), rowset.template GetValue<NColumnShard::Schema::IndexIndexes::ChunkIdx>())
        , RecordsCount(rowset.template GetValue<NColumnShard::Schema::IndexIndexes::RecordsCount>())
        , RawBytes(rowset.template GetValue<NColumnShard::Schema::IndexIndexes::RawBytes>())
    {
        AFL_VERIFY(Address.GetColumnId())("event", "incorrect address")("address", Address.DebugString());
        if (rowset.template HaveValue<NColumnShard::Schema::IndexIndexes::Blob>()) {
            TBlobRange& bRange = BlobRange.emplace();
            TString strBlobId = rowset.template GetValue<NColumnShard::Schema::IndexIndexes::Blob>();
            Y_ABORT_UNLESS(strBlobId.size() == sizeof(TLogoBlobID), "Size %" PRISZT "  doesn't match TLogoBlobID", strBlobId.size());
            TLogoBlobID logoBlobId((const ui64*)strBlobId.data());
            bRange.BlobId = NOlap::TUnifiedBlobId(dsGroupSelector->GetGroup(logoBlobId), logoBlobId);
            bRange.Offset = rowset.template GetValue<NColumnShard::Schema::IndexIndexes::Offset>();
            bRange.Size = rowset.template GetValue<NColumnShard::Schema::IndexIndexes::Size>();
            AFL_VERIFY(bRange.BlobId.IsValid() && bRange.Size)("event", "incorrect blob")("blob", bRange.ToString());
        } else if (rowset.template HaveValue<NColumnShard::Schema::IndexIndexes::BlobData>()) {
            BlobData = rowset.template GetValue<NColumnShard::Schema::IndexIndexes::BlobData>();
        } else {
            AFL_VERIFY(false);
        }
    }
};

}
