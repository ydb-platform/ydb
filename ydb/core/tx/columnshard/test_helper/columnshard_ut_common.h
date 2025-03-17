#pragma once

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/data_events/common/modification_type.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/services/metadata/abstract/fetcher.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

namespace NKikimr::NOlap {
struct TIndexInfo;
}

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

class TTester: public TNonCopyable {
public:
    static constexpr const ui64 FAKE_SCHEMESHARD_TABLET_ID = 4200;

    static void Setup(TTestActorRuntime& runtime);
};

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

struct TStorageTier {
    static inline TString DefaultTtlColumn = "saved_at";
    TString TtlColumn = DefaultTtlColumn;
    std::optional<TDuration> EvictAfter;
    TString Name;
    TString Codec;
    std::optional<int> CompressionLevel;
    NKikimrSchemeOp::TS3Settings S3 = FakeS3();

    TStorageTier(const TString& name = {})
        : Name(name) {
    }

    TString DebugString() const {
        return TStringBuilder() << "{Column=" << TtlColumn << ";EvictAfter=" << EvictAfter.value_or(TDuration::Zero()) << ";Name=" << Name
                                << ";Codec=" << Codec << "};";
    }

    NKikimrSchemeOp::EColumnCodec GetCodecId() const {
        if (Codec == "none") {
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain;
        } else if (Codec == "lz4") {
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
        } else if (Codec == "zstd") {
            return NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
        }
        Y_ABORT_UNLESS(false);
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

    static NKikimrSchemeOp::TS3Settings FakeS3() {
        const TString bucket = "tiering-test-01";

        NKikimrSchemeOp::TS3Settings s3Config;
        s3Config.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3Config.SetVerifySSL(false);
        s3Config.SetBucket(bucket);
//#define S3_TEST_USAGE
#ifdef S3_TEST_USAGE
        s3Config.SetEndpoint("storage.cloud-preprod.yandex.net");
        s3Config.SetAccessKey("...");
        s3Config.SetSecretKey("...");
        s3Config.SetProxyHost("localhost");
        s3Config.SetProxyPort(8080);
        s3Config.SetProxyScheme(NKikimrSchemeOp::TS3Settings::HTTP);
#else
        s3Config.SetEndpoint("fake.fake");
        s3Config.SetSecretKey("fakeSecret");
#endif
        s3Config.SetRequestTimeoutMs(10000);
        s3Config.SetHttpRequestTimeoutMs(10000);
        s3Config.SetConnectionTimeoutMs(10000);
        return s3Config;
    }
};

struct TTableSpecials: public TStorageTier {
    public:
        std::vector<TStorageTier> Tiers;
        bool WaitEmptyAfter = false;

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

        TTableSpecials& SetTtl(std::optional<TDuration> ttl) {
            EvictAfter = ttl;
            return *this;
        }

        TString DebugString() const {
            auto result = TStringBuilder() << "WaitEmptyAfter=" << WaitEmptyAfter << ";Tiers=";
            for (auto&& tier : Tiers) {
                result << "{" << tier.DebugString() << "}";
            }
            result << ";TTL=" << TStorageTier::DebugString();
            return result;
        }

        TString GetTtlColumn() const {
            for (const auto& tier : Tiers) {
                UNIT_ASSERT_VALUES_EQUAL(tier.TtlColumn, TtlColumn);
            }
            return TtlColumn;
        }

        NKikimrSchemeOp::TColumnDataLifeCycle InitTiersAndTtl() const{
            NKikimrSchemeOp::TColumnDataLifeCycle ttlSettings;
            ttlSettings.SetVersion(1);
            if (!HasTiers() && !HasTtl()) {
                return ttlSettings;
            }
            ttlSettings.MutableEnabled()->SetColumnName(TtlColumn);
            for (const auto& tier : Tiers) {
                UNIT_ASSERT(tier.EvictAfter);
                UNIT_ASSERT_EQUAL(TtlColumn, tier.TtlColumn);
                auto* tierSettings = ttlSettings.MutableEnabled()->AddTiers();
                tierSettings->MutableEvictToExternalStorage()->SetStorage(tier.Name);
                tierSettings->SetApplyAfterSeconds(tier.EvictAfter->Seconds());
            }
            if (HasTtl()) {
                auto* tier = ttlSettings.MutableEnabled()->AddTiers();
                tier->MutableDelete();
                tier->SetApplyAfterSeconds((*EvictAfter).Seconds());
            }
            return ttlSettings;
        }

        THashMap<TString, NColumnShard::NTiers::TTierConfig> BuildSnapshot() const;

};


inline TString TtlTxBody(const std::vector<ui64>& pathIds, TString ttlColumnName, ui64 tsSeconds) {
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

inline std::vector<TString> ExtractNames(const std::vector<NArrow::NTest::TTestColumn>& columns) {
    std::vector<TString> out;
    out.reserve(columns.size());
    for (auto& col : columns) {
        out.push_back(col.GetName());
    }
    return out;
}

inline std::vector<ui32> ExtractIds(const std::vector<NArrow::NTest::TTestColumn>& columns) {
    std::vector<ui32> out;
    out.reserve(columns.size());
    for (auto& col : columns) {
        Y_UNUSED(col);
        out.push_back(out.size() + 1);
    }
    return out;
}

inline std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<NArrow::NTest::TTestColumn>& columns) {
    std::vector<NScheme::TTypeInfo> types;
    types.reserve(columns.size());
    for (auto& i : columns) {
        types.push_back(i.GetType());
    }
    return types;
}

inline TString AlterTableTxBody(ui64 pathId, ui32 version, const TTableSpecials& specials) {
    NKikimrTxColumnShard::TSchemaTxBody tx;
    auto* table = tx.MutableAlterTable();
    table->SetPathId(pathId);
    tx.MutableSeqNo()->SetRound(version);

    *table->MutableTtlSettings() = specials.InitTiersAndTtl();

    Cerr << "AlterTable: " << tx << "\n";

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
    return out;
}

inline TString DropTableTxBody(ui64 pathId, ui32 version) {
    NKikimrTxColumnShard::TSchemaTxBody tx;
    tx.MutableDropTable()->SetPathId(pathId);
    tx.MutableSeqNo()->SetRound(version);

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
    return out;
}


inline TString CommitTxBody(ui64, const std::vector<ui64>& writeIds) {
    NKikimrTxColumnShard::TCommitTxBody proto;
    for (ui64 id : writeIds) {
        proto.AddWriteIds(id);
    }

    TString txBody;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&txBody);
    return txBody;
}



void RefreshTiering(TTestBasicRuntime& runtime, const TActorId& sender);

bool ProposeSchemaTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, NOlap::TSnapshot snap);
void PlanSchemaTx(TTestBasicRuntime& runtime, const TActorId& sender, NOlap::TSnapshot snap);

void PlanWriteTx(TTestBasicRuntime& runtime, const TActorId& sender, NOlap::TSnapshot snap, bool waitResult = true);

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 shardId, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, std::vector<ui64>* writeIds,
    const NEvWrite::EModificationType mType = NEvWrite::EModificationType::Upsert, const ui64 lockId = 1);

bool WriteData(TTestBasicRuntime& runtime, TActorId& sender, const ui64 writeId, const ui64 tableId, const TString& data,
    const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, bool waitResult = true, std::vector<ui64>* writeIds = nullptr,
    const NEvWrite::EModificationType mType = NEvWrite::EModificationType::Upsert, const ui64 lockId = 1);

std::optional<ui64> WriteData(TTestBasicRuntime& runtime, TActorId& sender, const NLongTxService::TLongTxId& longTxId, ui64 tableId,
    const ui64 writePartId, const TString& data, const std::vector<NArrow::NTest::TTestColumn>& ydbSchema,
    const NEvWrite::EModificationType mType = NEvWrite::EModificationType::Upsert);

ui32 WaitWriteResult(TTestBasicRuntime& runtime, ui64 shardId, std::vector<ui64>* writeIds = nullptr);

void ScanIndexStats(TTestBasicRuntime& runtime, TActorId& sender, const std::vector<ui64>& pathIds, NOlap::TSnapshot snap, ui64 scanId = 0);

void ProposeCommitFail(
     TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId = 1);
void ProposeCommit(
    TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId = 1);
void ProposeCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 txId, const std::vector<ui64>& writeIds, const ui64 lockId = 1);

void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 shardId, ui64 planStep, const TSet<ui64>& txIds);
void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, const TSet<ui64>& txIds);

inline void PlanCommit(TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, ui64 txId) {
    TSet<ui64> ids;
    ids.insert(txId);
    PlanCommit(runtime, sender, planStep, ids);
}

void Wakeup(TTestBasicRuntime& runtime, const TActorId& sender, const ui64 shardId);

struct TTestBlobOptions {
    THashSet<TString> NullColumns;
    THashSet<TString> SameValueColumns;
    ui32 SameValue = 42;
};

TCell MakeTestCell(const TTypeInfo& typeInfo, ui32 value, std::vector<TString>& mem);
TString MakeTestBlob(std::pair<ui64, ui64> range, const std::vector<NArrow::NTest::TTestColumn>& columns, const TTestBlobOptions& options = {},
    const std::set<std::string>& notNullColumns = {});
TSerializedTableRange MakeTestRange(
    std::pair<ui64, ui64> range, bool inclusiveFrom, bool inclusiveTo, const std::vector<NArrow::NTest::TTestColumn>& columns);

}   // namespace NKikimr::NTxUT

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
            , Index(index) {
        }

        TRowBuilder Add(const char* data) {
            return Add<std::string>(data);
        }

        template <class TData>
        TRowBuilder Add(const TData& data) {
            Y_ABORT_UNLESS(Index < Owner.Builders.size());
            auto& builder = Owner.Builders[Index];
            auto type = builder->type();

            Y_ABORT_UNLESS(NArrow::SwitchType(type->id(), [&](const auto& t) {
                using TWrap = std::decay_t<decltype(t)>;
                using T = typename TWrap::T;
                using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;

                AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("T", typeid(T).name());

                auto& typedBuilder = static_cast<TBuilder&>(*builder);
                if constexpr (std::is_arithmetic<TData>::value) {
                    if constexpr (arrow::has_c_type<T>::value) {
                        using CType = typename T::c_type;
                        Y_ABORT_UNLESS(typedBuilder.Append((CType)data).ok());
                        return true;
                    }
                }
                if constexpr (std::is_same<TData, std::string>::value) {
                    if constexpr (arrow::has_string_view<T>::value && arrow::is_parameter_free_type<T>::value) {
                        Y_ABORT_UNLESS(typedBuilder.Append(data.data(), data.size()).ok());
                        return true;
                    }
                }

                if constexpr (std::is_same<TData, NYdb::TDecimalValue>::value) {
                    if constexpr (arrow::is_decimal128_type<T>::value) {
                        Y_ABORT_UNLESS(typedBuilder.Append(arrow::Decimal128(data.Hi_, data.Low_)).ok());
                        return true;
                    }
                }
                Y_ABORT("Unknown type combination");
                return false;
            }));
            return TRowBuilder(Index + 1, Owner);
        }

        TRowBuilder AddNull() {
            Y_ABORT_UNLESS(Index < Owner.Builders.size());
            auto res = Owner.Builders[Index]->AppendNull();
            return TRowBuilder(Index + 1, Owner);
        }
    };

    TTableUpdatesBuilder(std::shared_ptr<arrow::Schema> schema)
        : Schema(schema) {
        Builders = NArrow::MakeBuilders(schema);
        Y_ABORT_UNLESS(Builders.size() == schema->fields().size());
    }

    TTableUpdatesBuilder(arrow::Result<std::shared_ptr<arrow::Schema>> schema) {
        UNIT_ASSERT_C(schema.ok(), schema.status().ToString());
        Schema = schema.ValueUnsafe();
        Builders = NArrow::MakeBuilders(Schema);
        Y_ABORT_UNLESS(Builders.size() == Schema->fields().size());
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
            Y_ABORT_UNLESS(arrayDataRes.ok());
            columns.push_back(*arrayDataRes);
        }
        return arrow::RecordBatch::Make(Schema, RowsCount, columns);
    }
};

NOlap::TIndexInfo BuildTableInfo(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, const std::vector<NArrow::NTest::TTestColumn>& key);

struct TTestTableDescription {
    enum EPredefined {
        Default,
        Exotic,
        AllTypes
    };
    const TString StorePath;
    const std::vector<NArrow::NTest::TTestColumn> Columns;
    const THashMap<TString, size_t> ColumnNameToIdx;
    const std::vector<size_t> Pk; //indexes from columns
    TTestTableDescription(std::vector<NArrow::NTest::TTestColumn>&& columns, std::vector<size_t>&& pk, TString&& storePath = "/Root/olapStore")
        : StorePath(std::move(storePath))
        , Columns(std::move(columns))
        , ColumnNameToIdx(GetColumnToIdxMap(Columns))
        , Pk(std::move(pk)){
    }
    TTestTableDescription(const EPredefined predefined, TString&& storePath = "/Root/olapStore")
        : TTestTableDescription(GetPredefinedColumns(predefined), GetPredefinedPkIdxs(predefined), std::move(storePath)) {
    }

    static THashMap<TString, size_t> GetColumnToIdxMap(const std::vector<NArrow::NTest::TTestColumn>& columns) {
        THashMap<TString, size_t> result;
        for (size_t i = 0; i != columns.size(); ++i) {
            result[columns[i].GetName()] = i;
        }
        return result;
    }

    static std::vector<NArrow::NTest::TTestColumn> GetPredefinedColumns(EPredefined predefined);

    static std::vector<size_t> GetPredefinedPkIdxs(EPredefined /* predefined */) {
        return {0, 1, 2, 3};
    }

    bool IsInStore() const {
        return StorePath.size();
    }

    bool IsStandalone() const {
        return !IsInStore();
    }

    NKikimrSchemeOp::TColumnTableSchema InitSchema(const NTxUT::TTableSpecials& specials) const;

    TString CreateTableTxBody(ui64 pathId, const NTxUT::TTableSpecials& spec = {}, ui64 generation = 0) const {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        tx.MutableSeqNo()->SetGeneration(generation);
        auto* table = tx.MutableEnsureTables()->AddTables();
        table->SetPathId(pathId);
        if (IsInStore()) {
            auto* preset = table->MutableSchemaPreset();
            preset->SetId(1);
            preset->SetName("default");
            *preset->MutableSchema() = InitSchema(spec);
        } else {
            *table->MutableSchema() = InitSchema(spec);
        }
        *table->MutableTtlSettings() = spec.InitTiersAndTtl();

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    TString CreateInitShardTxBody(ui64 pathId, const NTxUT::TTableSpecials& spec) const {
        AFL_VERIFY(IsInStore());
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableInitShard()->AddTables();
        tx.MutableInitShard()->SetOwnerPath(StorePath);
        table->SetPathId(pathId);

        *table->MutableSchema() =InitSchema(spec);
        *table->MutableTtlSettings() = spec.InitTiersAndTtl();

        Cerr << "CreateInitShard: " << tx << "\n";

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&out);
        return out;
    }

    std::vector<NArrow::NTest::TTestColumn> GetPkColumns() const {
        std::vector<NArrow::NTest::TTestColumn> result;
        for (const auto& i: Pk) {
            result.push_back(Columns[i]);
        }
        return result;
    } 

    std::vector<ui32> GetColumnIds() const {
        std::vector<ui32> result;
        for (const auto& _: Columns) {
            result.push_back(result.size() + 1);
        }
        return result;
    }

    std::vector<ui32> GetColumnIds(const std::vector<TString>& names) const {
        std::vector<ui32> result;
        for (const auto& name : names) {
            const auto* p = ColumnNameToIdx.FindPtr(name);
            AFL_VERIFY(p);
            result.push_back(*p + 1);
        }
        return result;
    }

    std::vector<TString> GetColumnNames() const {
        std::vector<TString> result;
        for (const auto& c: Columns) {
            result.push_back(c.GetName());
        }
        return result;
    }
};

void SetupSchema(TTestBasicRuntime& runtime, TActorId& sender, ui64 pathId, const TTestTableDescription& table, TString codec = "none");
void SetupSchema(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, const NOlap::TSnapshot& snapshot, bool succeed = true);

void PrepareTablet(
    TTestBasicRuntime& runtime, const ui64 tableId, const TTestTableDescription& tableDescription);
void PrepareTablet(TTestBasicRuntime& runtime, const TString& schemaTxBody, bool succeed);

std::shared_ptr<arrow::RecordBatch> ReadAllAsBatch(
    TTestBasicRuntime& runtime, const ui64 tableId, const NOlap::TSnapshot& snapshot, const std::vector<NArrow::NTest::TTestColumn>& schema);
}   // namespace NKikimr::NColumnShard
