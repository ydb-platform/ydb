#pragma once

#include "columnshard.h"
#include "columnshard_impl.h"

#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NTxUT {

class TTester : public TNonCopyable {
public:
    static constexpr const ui64 FAKE_SCHEMESHARD_TABLET_ID = 4200;

    static void Setup(TTestActorRuntime& runtime);
};

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;

struct TTestSchema {
    static const constexpr char * DefaultTtlColumn = "saved_at";

    struct TStorageTier {
        TString Name;
        TString Codec;
        std::optional<int> CompressionLevel;
        std::optional<ui32> EvictAfterSeconds;
        TString TtlColumn;
        std::optional<NKikimrSchemeOp::TS3Settings> S3;

        TStorageTier(const TString& name = {})
            : Name(name)
            , TtlColumn(DefaultTtlColumn)
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

        TStorageTier& SetTtl(ui32 seconds, const TString& column = DefaultTtlColumn) {
            EvictAfterSeconds = seconds;
            TtlColumn = column;
            return *this;
        }
    };

    struct TTableSpecials : public TStorageTier {
        std::vector<TStorageTier> Tiers;

        bool HasTiers() const {
            return !Tiers.empty();
        }

        bool HasTtl() const {
            return !HasTiers() && EvictAfterSeconds;
        }

        TTableSpecials WithCodec(const TString& codec) {
            TTableSpecials out = *this;
            out.SetCodec(codec);
            return out;
        }
    };

    static auto YdbSchema() {
        TVector<std::pair<TString, TTypeId>> schema = {
            // PK
            {"timestamp", NTypeIds::Timestamp },
            {"resource_type", NTypeIds::Utf8 },
            {"resource_id", NTypeIds::Utf8 },
            {"uid", NTypeIds::Utf8 },
            //
            {"level", NTypeIds::Int32 },
            {"message", NTypeIds::Utf8 },
            {"json_payload", NTypeIds::Json },
            {"ingested_at", NTypeIds::Timestamp },
            {"saved_at", NTypeIds::Timestamp },
            {"request_id", NTypeIds::Utf8 }
        };
        return schema;
    };

    static auto YdbExoticSchema() {
        TVector<std::pair<TString, TTypeId>> schema = {
            // PK
            {"timestamp", NTypeIds::Timestamp },
            {"resource_type", NTypeIds::Utf8 },
            {"resource_id", NTypeIds::Utf8 },
            {"uid", NTypeIds::Utf8 },
            //
            {"level", NTypeIds::Int32 },
            {"message", NTypeIds::String4k },
            {"json_payload", NTypeIds::JsonDocument },
            {"ingested_at", NTypeIds::Timestamp },
            {"saved_at", NTypeIds::Timestamp },
            {"request_id", NTypeIds::Yson }
        };
        return schema;
    };

    static auto YdbPkSchema() {
        TVector<std::pair<TString, TTypeId>> schema = {
            {"timestamp", NTypeIds::Timestamp },
            {"resource_type", NTypeIds::Utf8 },
            {"resource_id", NTypeIds::Utf8 },
            {"uid", NTypeIds::Utf8 }
        };
        return schema;
    }

    static auto YdbAllTypesSchema() {
        TVector<std::pair<TString, TTypeId>> schema = {
            { "ts", NTypeIds::Timestamp },

            { "i8", NTypeIds::Int8 },
            { "i16", NTypeIds::Int16 },
            { "i32", NTypeIds::Int32 },
            { "i64", NTypeIds::Int64 },
            { "u8", NTypeIds::Uint8 },
            { "u16", NTypeIds::Uint16 },
            { "u32", NTypeIds::Uint32 },
            { "u64", NTypeIds::Uint64 },
            { "float", NTypeIds::Float },
            { "double", NTypeIds::Double },

            { "byte", NTypeIds::Byte },
            //{ "bool", NTypeIds::Bool },
            //{ "decimal", NTypeIds::Decimal },
            //{ "dynum", NTypeIds::DyNumber },

            { "date", NTypeIds::Date },
            { "datetime", NTypeIds::Datetime },
            //{ "interval", NTypeIds::Interval },

            {"text", NTypeIds::Text },
            {"bytes", NTypeIds::Bytes },
            {"yson", NTypeIds::Yson },
            {"json", NTypeIds::Json },
            {"jsondoc", NTypeIds::JsonDocument }
        };
        return schema;
    };

    static NKikimrSchemeOp::TOlapColumnDescription CreateColumn(ui32 id, const TString& name, TTypeId type) {
        NKikimrSchemeOp::TOlapColumnDescription col;
        col.SetId(id);
        col.SetName(name);
        col.SetTypeId(type);
        return col;
    }

    static TString CreateTableTxBody(ui64 pathId, const TVector<std::pair<TString, TTypeId>>& columns,
                                     const TTableSpecials& specials = {}) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableEnsureTables()->AddTables();
        table->SetPathId(pathId);

        { // preset
            auto* preset = table->MutableSchemaPreset();
            preset->SetId(1);
            preset->SetName("default");

            // schema

            auto* schema = preset->MutableSchema();
            schema->SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

            for (ui32 i = 0; i < columns.size(); ++i) {
                *schema->MutableColumns()->Add() = CreateColumn(i + 1, columns[i].first, columns[i].second);
            }

            auto pk = columns;
            Y_VERIFY(pk.size() >= 4);
            pk.resize(4);
            for (auto& column : ExtractNames(pk)) {
                schema->AddKeyColumnNames(column);
            }

            if (specials.HasCodec()) {
                schema->MutableDefaultCompression()->SetCompressionCodec(specials.GetCodecId());
            }
            if (specials.CompressionLevel) {
                schema->MutableDefaultCompression()->SetCompressionLevel(*specials.CompressionLevel);
            }

            for (auto& tier : specials.Tiers) {
                auto* t = schema->AddStorageTiers();
                t->SetName(tier.Name);
                if (tier.HasCodec()) {
                    t->MutableCompression()->SetCompressionCodec(tier.GetCodecId());
                }
                if (tier.CompressionLevel) {
                    t->MutableCompression()->SetCompressionLevel(*tier.CompressionLevel);
                }
                if (tier.S3) {
                    t->MutableObjectStorage()->CopyFrom(*tier.S3);
                }
            }
        }

        if (specials.HasTiers()) {
            auto* ttlSettings = table->MutableTtlSettings();
            ttlSettings->SetVersion(1);
            auto* tiering = ttlSettings->MutableTiering();
            for (auto& tier : specials.Tiers) {
                auto* t = tiering->AddTiers();
                t->SetName(tier.Name);
                t->MutableEviction()->SetColumnName(tier.TtlColumn);
                t->MutableEviction()->SetExpireAfterSeconds(*tier.EvictAfterSeconds);
            }
        } else  if (specials.HasTtl()) {
            auto* ttlSettings = table->MutableTtlSettings();
            ttlSettings->SetVersion(1);
            auto* enable = ttlSettings->MutableEnabled();
            enable->SetColumnName(specials.TtlColumn);
            enable->SetExpireAfterSeconds(*specials.EvictAfterSeconds);
        }

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
        ttlSettings->SetVersion(version);

        if (specials.HasTiers()) {
            auto* tiering = ttlSettings->MutableTiering();
            for (auto& tier : specials.Tiers) {
                auto* t = tiering->AddTiers();
                t->SetName(tier.Name);
                t->MutableEviction()->SetColumnName(tier.TtlColumn);
                t->MutableEviction()->SetExpireAfterSeconds(*tier.EvictAfterSeconds);
            }
        } else if (specials.HasTtl()) {
            auto* enable = ttlSettings->MutableEnabled();
            enable->SetColumnName(specials.TtlColumn);
            enable->SetExpireAfterSeconds(*specials.EvictAfterSeconds);
        } else {
            ttlSettings->MutableDisabled();
        }

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

    static TString CommitTxBody(ui64 metaShard, const TVector<ui64>& writeIds) {
        NKikimrTxColumnShard::TCommitTxBody proto;
        proto.SetTxInitiator(metaShard);
        for (ui64 id : writeIds) {
            proto.AddWriteIds(id);
        }

        TString txBody;
        Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&txBody);
        return txBody;
    }

    static TString TtlTxBody(const TVector<ui64>& pathIds, TString ttlColumnName, ui64 tsSeconds) {
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

    static TVector<TString> ExtractNames(const TVector<std::pair<TString, TTypeId>>& columns) {
        TVector<TString> out;
        out.reserve(columns.size());
        for (auto& col : columns) {
            out.push_back(col.first);
        }
        return out;
    }

    static TVector<TTypeId> ExtractTypes(const TVector<std::pair<TString, TTypeId>>& columns) {
        TVector<TTypeId> types;
        types.reserve(columns.size());
        for (auto& [name, type] : columns) {
            types.push_back(type);
        }
        return types;
    }
};

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap);
void PlanSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap);
bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 writeId, ui64 tableId,
               const TString& data, std::shared_ptr<arrow::Schema> schema = {});
void ScanIndexStats(TTestBasicRuntime& runtime, TActorId& sender, const TVector<ui64>& pathIds,
                    NOlap::TSnapshot snap, ui64 scanId = 0);
void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 metaShard, ui64 txId, const TVector<ui64>& writeIds);
void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds);

inline void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, ui64 txId) {
    TSet<ui64> ids;
    ids.insert(txId);
    PlanCommit(runtime, sender, planStep, ids);
}

TString MakeTestBlob(std::pair<ui64, ui64> range, const TVector<std::pair<TString, TTypeId>>& columns,
                     const THashSet<TString>& nullColumns = {});
TSerializedTableRange MakeTestRange(std::pair<ui64, ui64> range, bool inclusiveFrom, bool inclusiveTo,
                                    const TVector<std::pair<TString, TTypeId>>& columns);

}
