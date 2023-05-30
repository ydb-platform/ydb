#pragma once

#include "columnshard.h"
#include "columnshard_impl.h"
#include "blob_cache.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NTxUT {

// Private events of different actors reuse the same ES_PRIVATE range
// So in order to capture the right private event we need to check its type via dynamic_cast
template <class TPrivateEvent>
inline TPrivateEvent* TryGetPrivateEvent(TAutoPtr<IEventHandle>& ev) {
    if (ev->GetTypeRewrite() != TPrivateEvent::EventType) {
        return nullptr;
    }
    return dynamic_cast<TPrivateEvent*>(ev->StaticCastAsLocal<IEventBase>());
}

class TTester : public TNonCopyable {
public:
    static constexpr const ui64 FAKE_SCHEMESHARD_TABLET_ID = 4200;

    static void Setup(TTestActorRuntime& runtime);
};

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

struct TTestSchema {
    static inline const TString DefaultTtlColumn = "saved_at";

    struct TStorageTier {
        TString TtlColumn = DefaultTtlColumn;
        std::optional<TDuration> EvictAfter;
        TString Name;
        TString Codec;
        std::optional<int> CompressionLevel;
        std::optional<NKikimrSchemeOp::TS3Settings> S3;

        TStorageTier(const TString& name = {})
            : Name(name)
        {}

        NKikimrSchemeOp::EColumnCodec GetCodecId() const {
            if (Codec == "none") {
                return NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain;
            } else if (Codec == "lz4") {
                return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
            } else if (Codec == "zstd") {
                return NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
            }
            Y_VERIFY(false);
        }

        bool HasCodec() const {
            return !Codec.empty();
        }

        TStorageTier& SetCodec(const TString& codec, std::optional<int> level = {}) {
            Codec = codec;
            if (level) {
                CompressionLevel = *level;
            }
            return *this;
        }

        TStorageTier& SetTtlColumn(const TString& columnName) {
            TtlColumn = columnName;
            return *this;
        }
    };

    struct TTableSpecials : public TStorageTier {
        std::vector<TStorageTier> Tiers;
        bool CompositeMarks = false;

        TTableSpecials() noexcept = default;

        bool HasTiers() const {
            return !Tiers.empty();
        }

        bool HasTtl() const {
            return EvictAfter.has_value();
        }

        TTableSpecials WithCodec(const TString& codec) const {
            TTableSpecials out = *this;
            out.SetCodec(codec);
            return out;
        }

        TTableSpecials WithCompositeMarks(bool composite) const {
            TTableSpecials out = *this;
            out.CompositeMarks = composite;
            return out;
        }

        TTableSpecials& SetTtl(std::optional<TDuration> ttl) {
            EvictAfter = ttl;
            return *this;
        }
    };

    static auto YdbSchema(const std::pair<TString, TTypeInfo>& firstKeyItem = {"timestamp", TTypeInfo(NTypeIds::Timestamp) }) {
        std::vector<std::pair<TString, TTypeInfo>> schema = {
            // PK
            firstKeyItem,
            {"resource_type", TTypeInfo(NTypeIds::Utf8) },
            {"resource_id", TTypeInfo(NTypeIds::Utf8) },
            {"uid", TTypeInfo(NTypeIds::Utf8) },
            {"level", TTypeInfo(NTypeIds::Int32) },
            {"message", TTypeInfo(NTypeIds::Utf8) },
            {"json_payload", TTypeInfo(NTypeIds::Json) },
            {"ingested_at", TTypeInfo(NTypeIds::Timestamp) },
            {"saved_at", TTypeInfo(NTypeIds::Timestamp) },
            {"request_id", TTypeInfo(NTypeIds::Utf8) }
        };
        return schema;
    };

    static auto YdbExoticSchema() {
        std::vector<std::pair<TString, TTypeInfo>> schema = {
            // PK
            {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
            {"resource_type", TTypeInfo(NTypeIds::Utf8) },
            {"resource_id", TTypeInfo(NTypeIds::Utf8) },
            {"uid", TTypeInfo(NTypeIds::Utf8) },
            //
            {"level", TTypeInfo(NTypeIds::Int32) },
            {"message", TTypeInfo(NTypeIds::String4k) },
            {"json_payload", TTypeInfo(NTypeIds::JsonDocument) },
            {"ingested_at", TTypeInfo(NTypeIds::Timestamp) },
            {"saved_at", TTypeInfo(NTypeIds::Timestamp) },
            {"request_id", TTypeInfo(NTypeIds::Yson) }
        };
        return schema;
    };

    static auto YdbPkSchema() {
        std::vector<std::pair<TString, TTypeInfo>> schema = {
            {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
            {"resource_type", TTypeInfo(NTypeIds::Utf8) },
            {"resource_id", TTypeInfo(NTypeIds::Utf8) },
            {"uid", TTypeInfo(NTypeIds::Utf8) }
        };
        return schema;
    }

    static auto YdbAllTypesSchema() {
        std::vector<std::pair<TString, TTypeInfo>> schema = {
            { "ts", TTypeInfo(NTypeIds::Timestamp) },

            { "i8", TTypeInfo(NTypeIds::Int8) },
            { "i16", TTypeInfo(NTypeIds::Int16) },
            { "i32", TTypeInfo(NTypeIds::Int32) },
            { "i64", TTypeInfo(NTypeIds::Int64) },
            { "u8", TTypeInfo(NTypeIds::Uint8) },
            { "u16", TTypeInfo(NTypeIds::Uint16) },
            { "u32", TTypeInfo(NTypeIds::Uint32) },
            { "u64", TTypeInfo(NTypeIds::Uint64) },
            { "float", TTypeInfo(NTypeIds::Float) },
            { "double", TTypeInfo(NTypeIds::Double) },

            { "byte", TTypeInfo(NTypeIds::Byte) },
            //{ "bool", TTypeInfo(NTypeIds::Bool) },
            //{ "decimal", TTypeInfo(NTypeIds::Decimal) },
            //{ "dynum", TTypeInfo(NTypeIds::DyNumber) },

            { "date", TTypeInfo(NTypeIds::Date) },
            { "datetime", TTypeInfo(NTypeIds::Datetime) },
            //{ "interval", TTypeInfo(NTypeIds::Interval) },

            {"text", TTypeInfo(NTypeIds::Text) },
            {"bytes", TTypeInfo(NTypeIds::Bytes) },
            {"yson", TTypeInfo(NTypeIds::Yson) },
            {"json", TTypeInfo(NTypeIds::Json) },
            {"jsondoc", TTypeInfo(NTypeIds::JsonDocument) }
        };
        return schema;
    };

    static NKikimrSchemeOp::TOlapColumnDescription CreateColumn(ui32 id, const TString& name, TTypeInfo type) {
        NKikimrSchemeOp::TOlapColumnDescription col;
        col.SetId(id);
        col.SetName(name);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(type, "");
        col.SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *col.MutableTypeInfo() = *columnType.TypeInfo;
        }
        return col;
    }

    static void InitSchema(const std::vector<std::pair<TString, TTypeInfo>>& columns,
                           const std::vector<std::pair<TString, TTypeInfo>>& pk,
                           const TTableSpecials& specials,
                           NKikimrSchemeOp::TColumnTableSchema* schema)
    {
        schema->SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

        for (ui32 i = 0; i < columns.size(); ++i) {
            *schema->MutableColumns()->Add() = CreateColumn(i + 1, columns[i].first, columns[i].second);
        }

        Y_VERIFY(pk.size() > 0);
        for (auto& column : ExtractNames(pk)) {
            schema->AddKeyColumnNames(column);
        }

        if (specials.HasCodec()) {
            schema->MutableDefaultCompression()->SetCompressionCodec(specials.GetCodecId());
        }
        if (specials.CompressionLevel) {
            schema->MutableDefaultCompression()->SetCompressionLevel(*specials.CompressionLevel);
        }
        schema->SetCompositeMarks(specials.CompositeMarks);
    }

    static void InitTtl(const TTableSpecials& specials, NKikimrSchemeOp::TColumnDataLifeCycle::TTtl* ttl) {
        Y_VERIFY(specials.HasTtl());
        Y_VERIFY(!specials.TtlColumn.empty());
        ttl->SetColumnName(specials.TtlColumn);
        ttl->SetExpireAfterSeconds((*specials.EvictAfter).Seconds());
    }

    static bool InitTiersAndTtl(const TTableSpecials& specials, NKikimrSchemeOp::TColumnDataLifeCycle* ttlSettings) {
        ttlSettings->SetVersion(1);
        if (specials.HasTiers()) {
            ttlSettings->SetUseTiering("Tiering1");
            if (specials.HasTtl()) {
                InitTtl(specials, ttlSettings->MutableEnabled());
            }
            return true;
        } else if (specials.HasTtl()) {
            InitTtl(specials, ttlSettings->MutableEnabled());
            return true;
        }
        return false;
    }

    static TString CreateTableTxBody(ui64 pathId, const std::vector<std::pair<TString, TTypeInfo>>& columns,
        const std::vector<std::pair<TString, TTypeInfo>>& pk,
        const TTableSpecials& specials = {}) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableEnsureTables()->AddTables();
        table->SetPathId(pathId);

        { // preset
            auto* preset = table->MutableSchemaPreset();
            preset->SetId(1);
            preset->SetName("default");

            // schema
            InitSchema(columns, pk, specials, preset->MutableSchema());
        }

        InitTiersAndTtl(specials, table->MutableTtlSettings());

        Cerr << "CreateTable: " << tx << "\n";

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    static TString CreateInitShardTxBody(ui64 pathId, const std::vector<std::pair<TString, TTypeInfo>>& columns,
        const std::vector<std::pair<TString, TTypeInfo>>& pk,
        const TTableSpecials& specials = {}, const TString& ownerPath = "/Root/olap") {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableInitShard()->AddTables();
        tx.MutableInitShard()->SetOwnerPath(ownerPath);
        table->SetPathId(pathId);

        InitSchema(columns, pk, specials, table->MutableSchema());
        InitTiersAndTtl(specials, table->MutableTtlSettings());

        Cerr << "CreateInitShard: " << tx << "\n";

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    static TString CreateStandaloneTableTxBody(ui64 pathId, const std::vector<std::pair<TString, TTypeInfo>>& columns,
                                               const std::vector<std::pair<TString, TTypeInfo>>& pk,
                                               const TTableSpecials& specials = {}) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableEnsureTables()->AddTables();
        table->SetPathId(pathId);

        InitSchema(columns, pk, specials, table->MutableSchema());
        InitTiersAndTtl(specials, table->MutableTtlSettings());

        Cerr << "CreateStandaloneTable: " << tx << "\n";

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    static TString AlterTableTxBody(ui64 pathId, ui32 version, const TTableSpecials& specials) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableAlterTable();
        table->SetPathId(pathId);
        tx.MutableSeqNo()->SetRound(version);

        auto* ttlSettings = table->MutableTtlSettings();
        if (!InitTiersAndTtl(specials, ttlSettings)) {
            ttlSettings->MutableDisabled();
        }

        Cerr << "AlterTable: " << tx << "\n";

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    static TString DropTableTxBody(ui64 pathId, ui32 version) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        tx.MutableDropTable()->SetPathId(pathId);
        tx.MutableSeqNo()->SetRound(version);

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    static NMetadata::NFetcher::ISnapshot::TPtr BuildSnapshot(const TTableSpecials& specials);

    static TString CommitTxBody(ui64 metaShard, const std::vector<ui64>& writeIds) {
        NKikimrTxColumnShard::TCommitTxBody proto;
        if (metaShard) {
            proto.SetTxInitiator(metaShard);
        }
        for (ui64 id : writeIds) {
            proto.AddWriteIds(id);
        }

        TString txBody;
        Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&txBody);
        return txBody;
    }

    static TString TtlTxBody(const std::vector<ui64>& pathIds, TString ttlColumnName, ui64 tsSeconds) {
        NKikimrTxColumnShard::TTtlTxBody proto;
        proto.SetTtlColumnName(ttlColumnName);
        proto.SetUnixTimeSeconds(tsSeconds);
        for (auto& pathId : pathIds) {
            proto.AddPathIds(pathId);
        }

        TString txBody;
        Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&txBody);
        return txBody;
    }

    static std::vector<TString> ExtractNames(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
        std::vector<TString> out;
        out.reserve(columns.size());
        for (auto& col : columns) {
            out.push_back(col.first);
        }
        return out;
    }

    static std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
        std::vector<NScheme::TTypeInfo> types;
        types.reserve(columns.size());
        for (auto& [name, type] : columns) {
            types.push_back(type);
        }
        return types;
    }
};

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap);
void ProvideTieringSnapshot(TTestBasicRuntime& runtime, TActorId& sender, NMetadata::NFetcher::ISnapshot::TPtr snapshot);
void PlanSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap);
bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 writeId, ui64 tableId,
               const TString& data, std::shared_ptr<arrow::Schema> schema = {});
std::optional<ui64> WriteData(TTestBasicRuntime& runtime, TActorId& sender, const NLongTxService::TLongTxId& longTxId,
                              ui64 tableId, const TString& dedupId, const TString& data,
                              std::shared_ptr<arrow::Schema> schema = {});
void ScanIndexStats(TTestBasicRuntime& runtime, TActorId& sender, const std::vector<ui64>& pathIds,
                    NOlap::TSnapshot snap, ui64 scanId = 0);
void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 txId, const std::vector<ui64>& writeIds);
void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 txId, const std::vector<ui64>& writeIds);
void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds);

inline void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, ui64 txId) {
    TSet<ui64> ids;
    ids.insert(txId);
    PlanCommit(runtime, sender, planStep, ids);
}

struct TTestBlobOptions {
    THashSet<TString> NullColumns;
    THashSet<TString> SameValueColumns;
    ui32 SameValue = 42;
};

TCell MakeTestCell(const TTypeInfo& typeInfo, ui32 value, std::vector<TString>& mem);
TString MakeTestBlob(std::pair<ui64, ui64> range, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns,
                     const TTestBlobOptions& options = {});
TSerializedTableRange MakeTestRange(std::pair<ui64, ui64> range, bool inclusiveFrom, bool inclusiveTo,
                                    const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);

                                    
}

namespace NKikimr::NColumnShard {
    class TTableUpdatesBuilder {
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        std::shared_ptr<arrow::Schema> Schema;
        ui32 RowsCount = 0;
    public:
        class TRowBuilder {
            TTableUpdatesBuilder& Owner;
            YDB_READONLY(ui32, Index, 0);
        public:
            TRowBuilder(ui32 index, TTableUpdatesBuilder& owner)
                : Owner(owner)
                , Index(index)
            {}

            template <class T>
            TRowBuilder Add(const T& data) {
                Y_VERIFY(Index < Owner.Builders.size());
                auto dataScalar = arrow::MakeScalar(data);
                auto res = Owner.Builders[Index]->AppendScalar(*dataScalar);
                return TRowBuilder(Index + 1, Owner);
            }

            TRowBuilder AddNull() {
                Y_VERIFY(Index < Owner.Builders.size());
                auto res = Owner.Builders[Index]->AppendNull();
                return TRowBuilder(Index + 1, Owner);
            }
        };

        TTableUpdatesBuilder(std::shared_ptr<arrow::Schema> schema)
            : Schema(schema)
        {
            Builders = NArrow::MakeBuilders(schema);
            Y_VERIFY(Builders.size() == schema->fields().size());
        }

        TRowBuilder AddRow() {
            ++RowsCount;
            return TRowBuilder(0, *this);
        }

        std::shared_ptr<arrow::RecordBatch> BuildArrow() {
            TVector<std::shared_ptr<arrow::Array>> columns;
            columns.reserve(Builders.size());
            for (auto&& builder : Builders) {
                auto arrayDataRes = builder->Finish();
                Y_VERIFY(arrayDataRes.ok());
                columns.push_back(*arrayDataRes);
            }
            return arrow::RecordBatch::Make(Schema, RowsCount, columns);
        }
    };
}
