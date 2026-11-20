#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/core/transfer/purecalc.h>

#include <yql/essentials/public/purecalc/common/interface.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>

using namespace NKikimr::NReplication;
using namespace NKikimr::NReplication::NTransfer;
using namespace NYql::NPureCalc;

namespace {

TScheme::TPtr MakeScheme(TVector<TSchemeColumn> columns) {
    auto scheme = std::make_shared<TScheme>();
    scheme->TableColumns = std::move(columns);

    auto addStructColumn = [&](const TString& name, ui32 id, ui32 typeId, bool notNull) {
        NKikimrKqp::TKqpColumnMetadataProto column;
        column.SetName(name);
        column.SetId(id);
        column.SetTypeId(typeId);
        column.SetNotNull(notNull);
        scheme->StructMetadata.push_back(std::move(column));
    };

    addStructColumn(SystemColumns::TargetTable, 0, NKikimr::NScheme::NTypeIds::String, false);
    scheme->TargetTableIndex = 0;

    for (const auto& column : scheme->TableColumns) {
        addStructColumn(column.Name, column.Id, column.PType.GetTypeId(), !column.Nullable);
        scheme->ColumnsMetadata.push_back(scheme->StructMetadata.back());
    }

    return scheme;
}

TString WrapTransferSql(const TString& transformLambda) {
    return TStringBuilder()
        << transformLambda
        << "SELECT * FROM (\n"
        << "  SELECT $__ydb_transfer_lambda(TableRow()) AS " << SystemColumns::Root << " FROM Input\n"
        << ") FLATTEN BY " << SystemColumns::Root << ";\n";
}

TString MakePositionsSql() {
    return WrapTransferSql(R"(
PRAGMA OrderedColumns;
$positions_transformation_lambda = ($msg) -> {
    $j = CAST($msg._data AS Json);
    $opts = Yson::Options(false AS Strict);
    $uint_opts = Yson::Options(true AS AutoConvert);
    return [
        <|
            id_hash: Unwrap(Yson::ConvertToUint64($j.id_hash, $uint_opts)),
            id: Unwrap(Yson::ConvertToString($j.id)),
            pipeline: Unwrap(Yson::ConvertToString($j.pipeline)),
            unix_timestamp: Unwrap(
                CAST(Unwrap(Yson::ConvertToUint64($j.unix_timestamp, $uint_opts)) AS Timestamp)
            ),
            backend_received_timestamp: Unwrap(
                CAST(Unwrap(Yson::ConvertToUint64($j.backend_recieve_unix_timestamp, $uint_opts)) AS Timestamp)
            ),
            topic_partition: $msg._partition,
            topic_offset: $msg._offset,
            lat: Unwrap(Yson::ConvertToDouble($j.lat)),
            lon: Unwrap(Yson::ConvertToDouble($j.lon)),
            speed: Yson::ConvertToDouble(Yson::Lookup($j, "speed", $opts), $opts),
            direction: Yson::ConvertToDouble(Yson::Lookup($j, "direction", $opts), $opts),
            accuracy: Yson::ConvertToDouble(Yson::Lookup($j, "accuracy", $opts), $opts),
            altitude: Yson::ConvertToDouble(Yson::Lookup($j, "altitude", $opts), $opts),
            topic_write_timestamp: $msg._write_timestamp,
        |>
    ];
};
$__ydb_transfer_lambda = $positions_transformation_lambda;
)");
}

TScheme::TPtr MakePositionsScheme() {
    using NKikimr::NScheme::TTypeInfo;
    namespace TypeIds = NKikimr::NScheme::NTypeIds;
    ui32 id = 1;
    auto col = [&](const char* name, ui32 typeId, bool key, bool nullable) {
        return TSchemeColumn{
            .Name = name,
            .Id = id++,
            .PType = TTypeInfo(typeId),
            .KeyColumn = key,
            .Nullable = nullable,
        };
    };
    return MakeScheme({
        col("id_hash", TypeIds::Uint64, true, false),
        col("unix_timestamp", TypeIds::Timestamp, true, false),
        col("id", TypeIds::String, false, false),
        col("pipeline", TypeIds::String, false, false),
        col("backend_received_timestamp", TypeIds::Timestamp, false, false),
        col("topic_partition", TypeIds::Uint32, false, false),
        col("topic_offset", TypeIds::Uint64, false, false),
        col("lat", TypeIds::Double, false, false),
        col("lon", TypeIds::Double, false, false),
        col("speed", TypeIds::Double, false, true),
        col("direction", TypeIds::Double, false, true),
        col("accuracy", TypeIds::Double, false, true),
        col("altitude", TypeIds::Double, false, true),
        col("topic_write_timestamp", TypeIds::Timestamp, false, false),
    });
}

TString MakePositionsJson(size_t i, size_t extraBytes) {
    return TStringBuilder()
        << "{"
        << "\"id_hash\":" << (1000 + i) << ","
        << "\"id\":\"device-" << i << "\","
        << "\"pipeline\":\"prod\","
        << "\"unix_timestamp\":1710000000000000,"
        << "\"backend_recieve_unix_timestamp\":1710000000000001,"
        << "\"lat\":55.75,"
        << "\"lon\":37.62,"
        << "\"speed\":10.5,"
        << "\"direction\":90.0,"
        << "\"accuracy\":5.0,"
        << "\"altitude\":150.0,"
        << "\"extra\":\"" << i << TString(extraBytes, 'x') << "\""
        << "}";
}

class TMessageVectorStream final: public IStream<TMessage*> {
public:
    explicit TMessageVectorStream(TVector<TMessage> data)
        : Data(std::move(data))
    {
    }

    TMessage* Fetch() override {
        return Index < Data.size() ? &Data[Index++] : nullptr;
    }

private:
    TVector<TMessage> Data;
    size_t Index = 0;
};

class TUsedProbeSpec: public TOutputSpecBase {
public:
    TUsedProbeSpec(const TScheme::TPtr& scheme, ui64* usedAtBind)
        : Inner(scheme, MakeOutputSchema(scheme->TableColumns))
        , UsedAtBind(usedAtBind)
    {
    }

    const NYT::TNode& GetSchema() const override {
        return Inner.GetSchema();
    }

    const TMessageOutputSpec& GetInner() const {
        return Inner;
    }

    ui64* GetUsedAtBind() const {
        return UsedAtBind;
    }

private:
    TMessageOutputSpec Inner;
    ui64* UsedAtBind = nullptr;
};

TString JoinValues(const TVector<ui64>& values) {
    TStringBuilder out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i) {
            out << ",";
        }
        out << values[i];
    }
    return TString(out);
}

} // namespace

template <>
struct NYql::NPureCalc::TOutputSpecTraits<TUsedProbeSpec> {
    static const constexpr bool IsPartial = false;
    static const constexpr bool SupportPullListMode = true;

    using TOutputItemType = TOutputMessage*;
    using TPullListReturnType = THolder<IStream<TOutputItemType>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const TUsedProbeSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker)
    {
        *outputSpec.GetUsedAtBind() = worker->GetScopedAlloc().GetUsed();
        return TOutputSpecTraits<TMessageOutputSpec>::ConvertPullListWorkerToOutputType(
            outputSpec.GetInner(),
            std::move(worker));
    }
};

namespace {

THolder<TPullListProgram<TMessageInputSpec, TUsedProbeSpec>> MakePositionsProgram(
    TStringBuf llvmSettings,
    ui64* usedAtBind)
{
    auto options = TProgramFactoryOptions();
    options.SetLLVMSettings(llvmSettings);
    options.SetUseWorkerPool(true);
    auto factory = MakeProgramFactory(options);

    try {
        return factory->MakePullListProgram(
            TMessageInputSpec(),
            TUsedProbeSpec(MakePositionsScheme(), usedAtBind),
            MakePositionsSql(),
            ETranslationMode::SQL);
    } catch (const TCompileError& e) {
        UNIT_FAIL(TStringBuilder() << "compile failed: " << e.GetIssues() << "\nYQL: " << e.GetYql());
    }
    return {};
}

TVector<ui64> ApplyPositions(
    TPullListProgram<TMessageInputSpec, TUsedProbeSpec>& program,
    ui64& usedAtBind,
    size_t applyCount,
    size_t extraBytes,
    bool consumeToEos = true)
{
    TVector<ui64> used;
    used.reserve(applyCount);
    for (size_t i = 0; i < applyCount; ++i) {
        TTopicMessage topicMessage(i + 1, MakePositionsJson(i, extraBytes));
        TMessage input{
            .PartitionId = 0,
            .Message = topicMessage,
        };

        auto stream = program.Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
        used.push_back(usedAtBind);
        UNIT_ASSERT(stream->Fetch());
        if (consumeToEos) {
            UNIT_ASSERT(!stream->Fetch());
        }
        stream.Destroy();
    }
    return used;
}

} // namespace

Y_UNIT_TEST_SUITE(TransferPurecalcMemory) {

Y_UNIT_TEST(JsonYsonUnusedFieldDoesNotGrowMkqlUsed) {
    constexpr size_t ExtraBytes = 256_KB;
    constexpr size_t ApplyCount = 16;
    constexpr size_t WarmupApplies = 3;

    ui64 usedAtBind = 0;
    auto program = MakePositionsProgram("OFF", &usedAtBind);
    const auto used = ApplyPositions(*program, usedAtBind, ApplyCount, ExtraBytes);

    const ui64 warmup = used[WarmupApplies];
    const ui64 last = used.back();
    UNIT_ASSERT_C(
        last < ExtraBytes * 2,
        TStringBuilder()
            << "MKQL used bytes at Apply() bind kept unused JSON/Yson payload on a pooled pull-list worker: warmup="
            << warmup << " last=" << last
            << " extra=" << ExtraBytes
            << " applies=" << ApplyCount
            << " used=" << JoinValues(used));
}

Y_UNIT_TEST(JsonYsonUnusedFieldDoesNotGrowMkqlUsedAfterPartialFetch) {
    constexpr size_t ExtraBytes = 256_KB;
    constexpr size_t ApplyCount = 16;
    constexpr size_t WarmupApplies = 3;

    ui64 usedAtBind = 0;
    auto program = MakePositionsProgram("OFF", &usedAtBind);
    const auto used = ApplyPositions(*program, usedAtBind, ApplyCount, ExtraBytes, /*consumeToEos=*/false);

    const ui64 warmup = used[WarmupApplies];
    const ui64 last = used.back();
    UNIT_ASSERT_C(
        last < ExtraBytes * 2,
        TStringBuilder()
            << "MKQL used bytes at next Apply() bind kept unused JSON/Yson payload after destroying a pull-list stream before EOS: warmup="
            << warmup << " last=" << last
            << " extra=" << ExtraBytes
            << " applies=" << ApplyCount
            << " used=" << JoinValues(used));
}

Y_UNIT_TEST(LlvmManyAppliesDoesNotGrowTypeEnv) {
    constexpr size_t ExtraBytes = 8_KB;
    constexpr size_t ApplyCount = 10000;
    constexpr size_t WarmupApplies = 10;

    ui64 usedAtBind = 0;
    auto program = MakePositionsProgram("ON", &usedAtBind);
    const auto used = ApplyPositions(*program, usedAtBind, ApplyCount, ExtraBytes);

    const ui64 warmup = used[WarmupApplies];
    const ui64 last = used.back();
    const ui64 delta = last > warmup ? last - warmup : 0;
    UNIT_ASSERT_C(
        delta < 64_KB,
        TStringBuilder()
            << "MKQL TypeEnv grew across LLVM Apply() on a pooled pull-list worker: warmup="
            << warmup << " last=" << last << " delta=" << delta
            << " extra=" << ExtraBytes
            << " applies=" << ApplyCount);
}

} // Y_UNIT_TEST_SUITE(TransferPurecalcMemory)
