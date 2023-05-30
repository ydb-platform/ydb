#pragma once
#include "defs.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/insert_table.h>
#include <ydb/core/tx/columnshard/engines/granules_table.h>
#include <ydb/core/tx/columnshard/engines/columns_table.h>

#include <type_traits>

namespace NKikimr::NColumnShard {

using NOlap::TWriteId;
using NOlap::IBlobGroupSelector;

struct Schema : NIceDb::Schema {
    // These settings are persisted on each Init. So we use empty settings in order not to overwrite what
    // was changed by the user
    struct EmptySettings {
        static void Materialize(NIceDb::TToughDb&) {}
    };

    using TSettings = SchemaSettings<EmptySettings>;

    using TInsertedData = NOlap::TInsertedData;
    using TGranuleRecord = NOlap::TGranuleRecord;
    using TColumnRecord = NOlap::TColumnRecord;

    enum EIndexTables : ui32 {
        InsertTableId = 255,
        GranulesTableId,
        ColumnsTableId,
        CountersTableId,
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
    };

    enum class EInsertTableIds : ui8 {
        Inserted = 0,
        Committed = 1,
        Aborted = 2,
    };

    // Tablet tables

    struct Value : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {}; // one of EValueIds
        struct Digit : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Bytes : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Digit, Bytes>;
    };

    struct TxInfo : Table<2> {
        struct TxId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TxKind : Column<2, NScheme::NTypeIds::Uint32> { using Type = NKikimrTxColumnShard::ETransactionKind; };
        struct TxBody : Column<3, NScheme::NTypeIds::String> {};
        struct MaxStep : Column<4, NScheme::NTypeIds::Uint64> {};
        struct PlanStep : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Source : Column<6, NScheme::NTypeIds::ActorId> {};
        struct Cookie : Column<7, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TxId>;
        using TColumns = TableColumns<TxId, TxKind, TxBody, MaxStep, PlanStep, Source, Cookie>;
    };

    struct SchemaPresetInfo : Table<3> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DropStep : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Name, DropStep, DropTxId>;
    };

    struct SchemaPresetVersionInfo : Table<9> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TCommonSchemaVersionInfo

        using TKey = TableKey<Id, SinceStep, SinceTxId>;
        using TColumns = TableColumns<Id, SinceStep, SinceTxId, InfoProto>;
    };

    struct TtlSettingsPresetInfo : Table<4> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Name : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DropStep : Column<3, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Name, DropStep, DropTxId>;
    };

    struct TtlSettingsPresetVersionInfo : Table<10> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TTtlSettingsPresetVersionInfo

        using TKey = TableKey<Id, SinceStep, SinceTxId>;
        using TColumns = TableColumns<Id, SinceStep, SinceTxId, InfoProto>;
    };

    struct TableInfo : Table<5> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct DropStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct DropTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct TieringUsage: Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<PathId>;
        using TColumns = TableColumns<PathId, DropStep, DropTxId, TieringUsage>;
    };

    struct TableVersionInfo : Table<11> {
        struct PathId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct SinceStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct SinceTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct InfoProto : Column<4, NScheme::NTypeIds::String> {}; // TTableVersionInfo

        using TKey = TableKey<PathId, SinceStep, SinceTxId>;
        using TColumns = TableColumns<PathId, SinceStep, SinceTxId, InfoProto>;
    };

    struct LongTxWrites : Table<6> {
        struct WriteId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct LongTxId : Column<2, NScheme::NTypeIds::String> {};
        struct WritePartId: Column<3, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<WriteId>;
        using TColumns = TableColumns<WriteId, LongTxId, WritePartId>;
    };

    struct BlobsToKeep : Table<7> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId>;
    };

    struct BlobsToDelete : Table<8> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId>;
    };

    struct SmallBlobs : Table<12> {
        struct BlobId : Column<1, NScheme::NTypeIds::String> {};
        struct Data : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<BlobId>;
        using TColumns = TableColumns<BlobId, Data>;
    };

    struct OneToOneEvictedBlobs : Table<13> {
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

    // Index tables

    // InsertTable - common for all indices
    struct InsertTable : NIceDb::Schema::Table<InsertTableId> {
        struct Committed : Column<1, NScheme::NTypeIds::Byte> {};
        struct ShardOrPlan : Column<2, NScheme::NTypeIds::Uint64> {};
        struct WriteTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct DedupId : Column<5, NScheme::NTypeIds::String> {};
        struct BlobId : Column<6, NScheme::NTypeIds::String> {};
        struct Meta : Column<7, NScheme::NTypeIds::String> {};
        struct IndexPlanStep : Column<8, NScheme::NTypeIds::Uint64> {};
        struct IndexTxId : Column<9, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Committed, ShardOrPlan, WriteTxId, PathId, DedupId>;
        using TColumns = TableColumns<Committed, ShardOrPlan, WriteTxId, PathId, DedupId, BlobId, Meta, IndexPlanStep, IndexTxId>;
    };

    struct IndexGranules : NIceDb::Schema::Table<GranulesTableId> {
        struct Index : Column<1, NScheme::NTypeIds::Uint32> {};
        struct PathId : Column<2, NScheme::NTypeIds::Uint64> {};    // Logical table (if many)
        struct IndexKey : Column<3, NScheme::NTypeIds::String> {};  // Effective part of PK (serialized)
        struct Granule : Column<4, NScheme::NTypeIds::Uint64> {};   // FK: {Index, Granule} -> TIndexColumns
        struct PlanStep : Column<5, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<6, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Index, PathId, IndexKey>;
        using TColumns = TableColumns<Index, PathId, IndexKey, Granule, PlanStep, TxId>;
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

        using TKey = TableKey<Index, Granule, ColumnIdx, PlanStep, TxId, Portion, Chunk>;
        using TColumns = TableColumns<Index, Granule, ColumnIdx, PlanStep, TxId, Portion, Chunk,
                                    XPlanStep, XTxId, Blob, Metadata, Offset, Size>;
    };

    struct IndexCounters : NIceDb::Schema::Table<CountersTableId> {
        struct Index : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Counter : Column<2, NScheme::NTypeIds::Uint32> {};
        struct ValueUI64 : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Index, Counter>;
        using TColumns = TableColumns<Index, Counter, ValueUI64>;
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
        InsertTable,
        IndexGranules,
        IndexColumns,
        IndexCounters,
        SmallBlobs,
        OneToOneEvictedBlobs
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
            if (rowset.IsValid())
                value = T{rowset.template GetValue<TSource>()};
            return true;
        }
        return false;
    }

    template<class TMessage>
    static bool GetSpecialProtoValue(NIceDb::TNiceDb& db, EValueIds key, std::optional<TMessage>& value) {
        auto rowset = db.Table<Value>().Key(ui32(key)).Select<Value::Bytes>();
        if (rowset.IsReady()) {
            if (rowset.IsValid()) {
                Y_VERIFY(value.emplace().ParseFromString(rowset.GetValue<Value::Bytes>()));
            }
            return true;
        }
        return false;
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
        Y_VERIFY(message.SerializeToString(&serialized));
        SaveSpecialValue(db, key, serialized);
    }

    static void SaveTxInfo(NIceDb::TNiceDb& db, ui64 txId, NKikimrTxColumnShard::ETransactionKind txKind,
                           const TString& txBody, ui64 maxStep, const TActorId& source, ui64 cookie)
    {
        db.Table<TxInfo>().Key(txId).Update(
            NIceDb::TUpdate<TxInfo::TxKind>(txKind),
            NIceDb::TUpdate<TxInfo::TxBody>(txBody),
            NIceDb::TUpdate<TxInfo::MaxStep>(maxStep),
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
            ui64 id, const TRowVersion& version,
            const NKikimrTxColumnShard::TSchemaPresetVersionInfo& info)
    {
        TString serialized;
        Y_VERIFY(info.SerializeToString(&serialized));
        db.Table<SchemaPresetVersionInfo>().Key(id, version.Step, version.TxId).Update(
            NIceDb::TUpdate<SchemaPresetVersionInfo::InfoProto>(serialized));
    }

    static void SaveSchemaPresetDropVersion(NIceDb::TNiceDb& db, ui64 id, const TRowVersion& dropVersion) {
        db.Table<SchemaPresetInfo>().Key(id).Update(
            NIceDb::TUpdate<SchemaPresetInfo::DropStep>(dropVersion.Step),
            NIceDb::TUpdate<SchemaPresetInfo::DropTxId>(dropVersion.TxId));
    }

    static void EraseSchemaPresetVersionInfo(NIceDb::TNiceDb& db, ui64 id, const TRowVersion& version) {
        db.Table<SchemaPresetVersionInfo>().Key(id, version.Step, version.TxId).Delete();
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
            ui64 pathId, const TRowVersion& version,
            const NKikimrTxColumnShard::TTableVersionInfo& info)
    {
        TString serialized;
        Y_VERIFY(info.SerializeToString(&serialized));
        db.Table<TableVersionInfo>().Key(pathId, version.Step, version.TxId).Update(
            NIceDb::TUpdate<TableVersionInfo::InfoProto>(serialized));
    }

    static void SaveTableDropVersion(
            NIceDb::TNiceDb& db, ui64 pathId, ui64 dropStep, ui64 dropTxId)
    {
        db.Table<TableInfo>().Key(pathId).Update(
            NIceDb::TUpdate<TableInfo::DropStep>(dropStep),
            NIceDb::TUpdate<TableInfo::DropTxId>(dropTxId));
    }

    static void EraseTableVersionInfo(NIceDb::TNiceDb& db, ui64 pathId, const TRowVersion& version) {
        db.Table<TableVersionInfo>().Key(pathId, version.Step, version.TxId).Delete();
    }

    static void EraseTableInfo(NIceDb::TNiceDb& db, ui64 pathId) {
        db.Table<TableInfo>().Key(pathId).Delete();
    }

    static void SaveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId) {
        NKikimrLongTxService::TLongTxId proto;
        longTxId.ToProto(&proto);
        TString serialized;
        Y_VERIFY(proto.SerializeToString(&serialized));
        db.Table<LongTxWrites>().Key((ui64)writeId).Update(
            NIceDb::TUpdate<LongTxWrites::LongTxId>(serialized),
            NIceDb::TUpdate<LongTxWrites::WritePartId>(writePartId));
    }

    static void EraseLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId) {
        db.Table<LongTxWrites>().Key((ui64)writeId).Delete();
    }

    // InsertTable activities

    static void InsertTable_Upsert(NIceDb::TNiceDb& db, EInsertTableIds recType, const TInsertedData& data) {
        if (data.GetSchemaSnapshot().Valid()) {
            db.Table<InsertTable>().Key((ui8)recType, data.ShardOrPlan, data.WriteTxId, data.PathId, data.DedupId).Update(
                NIceDb::TUpdate<InsertTable::BlobId>(data.BlobId.ToStringLegacy()),
                NIceDb::TUpdate<InsertTable::Meta>(data.Metadata),
                NIceDb::TUpdate<InsertTable::IndexPlanStep>(data.GetSchemaSnapshot().GetPlanStep()),
                NIceDb::TUpdate<InsertTable::IndexTxId>(data.GetSchemaSnapshot().GetTxId())
            );
        } else {
            db.Table<InsertTable>().Key((ui8)recType, data.ShardOrPlan, data.WriteTxId, data.PathId, data.DedupId).Update(
                NIceDb::TUpdate<InsertTable::BlobId>(data.BlobId.ToStringLegacy()),
                NIceDb::TUpdate<InsertTable::Meta>(data.Metadata)
            );
        }
    }

    static void InsertTable_Erase(NIceDb::TNiceDb& db, EInsertTableIds recType, const TInsertedData& data) {
        db.Table<InsertTable>().Key((ui8)recType, data.ShardOrPlan, data.WriteTxId, data.PathId, data.DedupId).Delete();
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
                                 THashMap<TWriteId, TInsertedData>& inserted,
                                 THashMap<ui64, TSet<TInsertedData>>& committed,
                                 THashMap<TWriteId, TInsertedData>& aborted,
                                 const TInstant& loadTime) {
        auto rowset = db.Table<InsertTable>().GreaterOrEqual(0, 0, 0, 0, "").Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            EInsertTableIds recType = (EInsertTableIds)rowset.GetValue<InsertTable::Committed>();
            ui64 shardOrPlan = rowset.GetValue<InsertTable::ShardOrPlan>();
            ui64 writeTxId = rowset.GetValueOrDefault<InsertTable::WriteTxId>();
            ui64 pathId = rowset.GetValue<InsertTable::PathId>();
            TString dedupId = rowset.GetValue<InsertTable::DedupId>();
            TString strBlobId = rowset.GetValue<InsertTable::BlobId>();
            TString metaStr = rowset.GetValue<InsertTable::Meta>();

            std::optional<NOlap::TSnapshot> indexSnapshot;
            if (rowset.HaveValue<InsertTable::IndexPlanStep>()) {
                ui64 indexPlanStep = rowset.GetValue<InsertTable::IndexPlanStep>();
                ui64 indexTxId = rowset.GetValue<InsertTable::IndexTxId>();
                indexSnapshot = NOlap::TSnapshot(indexPlanStep, indexTxId);
            }
            
            TString error;
            NOlap::TUnifiedBlobId blobId = NOlap::TUnifiedBlobId::ParseFromString(strBlobId, dsGroupSelector, error);
            Y_VERIFY(blobId.IsValid(), "Failied to parse blob id: %s", error.c_str());

            TInstant writeTime = loadTime;
            NKikimrTxColumnShard::TLogicalMetadata meta;
            if (meta.ParseFromString(metaStr) && meta.HasDirtyWriteTimeSeconds()) {
                writeTime = TInstant::Seconds(meta.GetDirtyWriteTimeSeconds());
            }

            TInsertedData data(shardOrPlan, writeTxId, pathId, dedupId, blobId, metaStr, writeTime, indexSnapshot);

            switch (recType) {
                case EInsertTableIds::Inserted:
                    inserted.emplace(TWriteId{data.WriteTxId}, std::move(data));
                    break;
                case EInsertTableIds::Committed:
                    committed[data.PathId].emplace(data);
                    break;
                case EInsertTableIds::Aborted:
                    aborted.emplace(TWriteId{data.WriteTxId}, std::move(data));
                    break;
            }

            if (!rowset.Next())
                return false;
        }
        return true;
    }

    // IndexGranules activities

    static void IndexGranules_Write(NIceDb::TNiceDb& db, ui32 index, const NOlap::IColumnEngine& engine,
                                    const TGranuleRecord& row) {
        db.Table<IndexGranules>().Key(index, row.PathId, engine.SerializeMark(row.Mark)).Update(
            NIceDb::TUpdate<IndexGranules::Granule>(row.Granule),
            NIceDb::TUpdate<IndexGranules::PlanStep>(row.GetCreatedAt().GetPlanStep()),
            NIceDb::TUpdate<IndexGranules::TxId>(row.GetCreatedAt().GetTxId())
        );
    }

    static void IndexGranules_Erase(NIceDb::TNiceDb& db, ui32 index, const NOlap::IColumnEngine& engine,
                                    const TGranuleRecord& row) {
        db.Table<IndexGranules>().Key(index, row.PathId, engine.SerializeMark(row.Mark)).Delete();
    }

    static bool IndexGranules_Load(NIceDb::TNiceDb& db, ui32 index, const NOlap::IColumnEngine& engine,
                                   const std::function<void(const TGranuleRecord&)>& callback) {
        auto rowset = db.Table<IndexGranules>().Prefix(index).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui64 pathId = rowset.GetValue<IndexGranules::PathId>();
            TString indexKey = rowset.GetValue<IndexGranules::IndexKey>();
            ui64 granule = rowset.GetValue<IndexGranules::Granule>();
            ui64 planStep = rowset.GetValue<IndexGranules::PlanStep>();
            ui64 txId = rowset.GetValue<IndexGranules::TxId>();

            callback(TGranuleRecord(pathId, granule, NOlap::TSnapshot(planStep, txId), engine.DeserializeMark(indexKey)));

            if (!rowset.Next())
                return false;
        }
        return true;
    }

    // IndexColumns activities

    static void IndexColumns_Write(NIceDb::TNiceDb& db, ui32 index, const TColumnRecord& row) {
        db.Table<IndexColumns>().Key(index, row.Granule, row.ColumnId, row.PlanStep, row.TxId, row.Portion, row.Chunk).Update(
            NIceDb::TUpdate<IndexColumns::XPlanStep>(row.XPlanStep),
            NIceDb::TUpdate<IndexColumns::XTxId>(row.XTxId),
            NIceDb::TUpdate<IndexColumns::Blob>(row.SerializedBlobId()),
            NIceDb::TUpdate<IndexColumns::Metadata>(row.Metadata),
            NIceDb::TUpdate<IndexColumns::Offset>(row.BlobRange.Offset),
            NIceDb::TUpdate<IndexColumns::Size>(row.BlobRange.Size)
        );
    }

    static void IndexColumns_Erase(NIceDb::TNiceDb& db, ui32 index, const TColumnRecord& row) {
        db.Table<IndexColumns>().Key(index, row.Granule, row.ColumnId, row.PlanStep, row.TxId, row.Portion, row.Chunk).Delete();
    }

    static bool IndexColumns_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, ui32 index,
                                  const std::function<void(const TColumnRecord&)>& callback) {
        auto rowset = db.Table<IndexColumns>().Prefix(index).Select();
        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            TColumnRecord row;
            row.Granule = rowset.GetValue<IndexColumns::Granule>();
            row.ColumnId = rowset.GetValue<IndexColumns::ColumnIdx>();
            row.PlanStep = rowset.GetValue<IndexColumns::PlanStep>();
            row.TxId = rowset.GetValue<IndexColumns::TxId>();
            row.Portion = rowset.GetValue<IndexColumns::Portion>();
            row.Chunk = rowset.GetValue<IndexColumns::Chunk>();
            row.XPlanStep = rowset.GetValue<IndexColumns::XPlanStep>();
            row.XTxId = rowset.GetValue<IndexColumns::XTxId>();
            TString strBlobId = rowset.GetValue<IndexColumns::Blob>();
            row.Metadata = rowset.GetValue<IndexColumns::Metadata>();
            row.BlobRange.Offset = rowset.GetValue<IndexColumns::Offset>();
            row.BlobRange.Size = rowset.GetValue<IndexColumns::Size>();

            Y_VERIFY(strBlobId.size() == sizeof(TLogoBlobID), "Size %" PRISZT "  doesn't match TLogoBlobID", strBlobId.size());
            TLogoBlobID logoBlobId((const ui64*)strBlobId.data());
            row.BlobRange.BlobId = NOlap::TUnifiedBlobId(dsGroupSelector->GetGroup(logoBlobId), logoBlobId);

            callback(row);

            if (!rowset.Next())
                return false;
        }
        return true;
    }

    // IndexCounters

    static void IndexCounters_Write(NIceDb::TNiceDb& db, ui32 index, ui32 counterId, ui64 value) {
        db.Table<IndexCounters>().Key(index, counterId).Update(
            NIceDb::TUpdate<IndexCounters::ValueUI64>(value)
        );
    }

    static bool IndexCounters_Load(NIceDb::TNiceDb& db, ui32 index, const std::function<void(ui32 id, ui64 value)>& callback) {
        auto rowset = db.Table<IndexCounters>().Prefix(index).Select();
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
