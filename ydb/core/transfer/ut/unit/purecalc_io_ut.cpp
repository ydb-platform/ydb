#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/core/transfer/purecalc.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/events_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>

#include <yql/essentials/public/purecalc/common/interface.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NKikimr::NReplication;
using namespace NKikimr::NReplication::NTransfer;
using namespace NYql::NPureCalc;
using namespace NYdb::NTopic;

namespace {

TScheme::TPtr MakeIoScheme(bool messageNotNull = false) {
    TAutoPtr<NKikimr::NSchemeCache::TSchemeCacheNavigate> nav(new NKikimr::NSchemeCache::TSchemeCacheNavigate());
    auto& entry = nav->ResultSet.emplace_back();
    entry.Path = {"Root", "Table"};
    entry.Columns[1] = NKikimr::TSysTables::TTableColumnInfo(
        "Key", 1, NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Uint64), {}, 0);
    entry.Columns[2] = NKikimr::TSysTables::TTableColumnInfo(
        "Message", 2, NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Utf8), {}, -1);
    entry.Columns[3] = NKikimr::TSysTables::TTableColumnInfo(
        "Value", 3, NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Int64), {}, -1);
    entry.NotNullColumns.insert("Key");
    if (messageNotNull) {
        entry.NotNullColumns.insert("Message");
    }
    auto scheme = BuildScheme(nav);
    if (messageNotNull) {
        // Keep the YQL type optional so a SQL NULL can reach the runtime NotNull check.
        for (auto& column : scheme->TableColumns) {
            if (column.Name == "Message") {
                column.Nullable = true;
            }
        }
        for (auto& column : scheme->StructMetadata) {
            if (column.GetName() == "Message") {
                column.SetNotNull(true);
            }
        }
    }
    return scheme;
}

TString WrapSql(const TString& lambda) {
    return TStringBuilder()
        << lambda
        << "SELECT * FROM (\n"
        << "  SELECT $__ydb_transfer_lambda(TableRow()) AS " << SystemColumns::Root << " FROM Input\n"
        << ") FLATTEN BY " << SystemColumns::Root << ";\n";
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

THolder<TPullListProgram<TMessageInputSpec, TMessageOutputSpec>> MakeProgram(
    const TScheme::TPtr& scheme,
    const TString& lambda)
{
    auto options = TProgramFactoryOptions();
    options.SetLLVMSettings("OFF");
    auto factory = MakeProgramFactory(options);
    try {
        return factory->MakePullListProgram(
            TMessageInputSpec(),
            TMessageOutputSpec(scheme, MakeOutputSchema(scheme->TableColumns)),
            WrapSql(lambda),
            ETranslationMode::SQL);
    } catch (const TCompileError& e) {
        UNIT_FAIL(TStringBuilder() << "compile failed: " << e.GetIssues() << "\nYQL: " << e.GetYql());
    }
    return {};
}

TTopicMessage MakeRichMessage(const TString& data, const TString& key = {}) {
    auto messageMeta = MakeIntrusive<TMessageMeta>();
    messageMeta->Fields.emplace_back("a", "b");
    if (key) {
        messageMeta->Fields.emplace_back("__key", key);
    }
    TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(
        42,
        "producer",
        7,
        TInstant::MicroSeconds(11),
        TInstant::MicroSeconds(22),
        nullptr,
        messageMeta,
        data.size(),
        "group");
    return TTopicMessage(std::move(info), TString(data));
}

} // namespace

Y_UNIT_TEST_SUITE(TransferPurecalcIo) {

Y_UNIT_TEST(InputFieldsAndOptionalTable) {
    auto scheme = MakeIoScheme();
    auto program = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST($x._data AS Utf8),
            Value: CAST($x._seq_no AS Int64)
        |>
    ];
};
)");
    TTopicMessage topic = MakeRichMessage("hello", "k");
    TMessage input{.PartitionId = 5, .Message = topic};
    auto stream = program->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    auto* row = stream->Fetch();
    UNIT_ASSERT(row);
    UNIT_ASSERT(!row->Table || row->Table->empty());
    UNIT_ASSERT(row->EstimateSize > 0);
    UNIT_ASSERT(!stream->Fetch());

    auto tsProgram = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST($x._producer_id AS Utf8),
            Value: CAST($x._create_timestamp AS Int64)
        |>
    ];
};
)");
    auto tsStream = tsProgram->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    UNIT_ASSERT(tsStream->Fetch());
    UNIT_ASSERT(!tsStream->Fetch());
}

Y_UNIT_TEST(TargetTableAndStringEstimate) {
    auto scheme = MakeIoScheme();
    auto program = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            __ydb_table: "inner/table",
            Key: $x._offset,
            Message: CAST($x._data AS Utf8),
            Value: 1
        |>
    ];
};
)");
    TTopicMessage topic = MakeRichMessage(TString(100, 'x'));
    TMessage input{.PartitionId = 1, .Message = topic};
    auto stream = program->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    auto* row = stream->Fetch();
    UNIT_ASSERT(row);
    UNIT_ASSERT(row->Table);
    UNIT_ASSERT_VALUES_EQUAL(*row->Table, "inner/table");
    UNIT_ASSERT(row->EstimateSize >= 100u);
    UNIT_ASSERT(!stream->Fetch());
}

Y_UNIT_TEST(NullNotNullColumnThrows) {
    auto scheme = MakeIoScheme(true);
    auto program = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST(NULL AS Utf8?),
            Value: 1
        |>
    ];
};
)");
    TTopicMessage topic(0, "hello");
    TMessage input{.PartitionId = 0, .Message = topic};
    auto stream = program->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    UNIT_ASSERT_EXCEPTION_CONTAINS(stream->Fetch(), yexception, "must be non-NULL");
}

Y_UNIT_TEST(MessageWithoutMetaLeavesOptionalKeyEmpty) {
    auto scheme = MakeIoScheme();
    auto program = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST($x._key AS Utf8),
            Value: CAST($x._partition AS Int64)
        |>
    ];
};
)");
    TTopicMessage topic(3, "payload");
    TMessage input{.PartitionId = 9, .Message = topic};
    auto stream = program->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    auto* row = stream->Fetch();
    UNIT_ASSERT(row);
    UNIT_ASSERT(!row->Table);
    UNIT_ASSERT(!stream->Fetch());
}

Y_UNIT_TEST(KeyFromAttributes) {
    auto scheme = MakeIoScheme();
    auto program = MakeProgram(scheme, R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST($x._key AS Utf8),
            Value: CAST($x._seq_no AS Int64)
        |>
    ];
};
)");
    TTopicMessage topic = MakeRichMessage("hello", "from-meta");
    TMessage input{.PartitionId = 0, .Message = topic};
    auto stream = program->Apply(MakeHolder<TMessageVectorStream>(TVector<TMessage>{input}));
    auto* row = stream->Fetch();
    UNIT_ASSERT(row);
    UNIT_ASSERT(!stream->Fetch());
}

} // Y_UNIT_TEST_SUITE(TransferPurecalcIo)
