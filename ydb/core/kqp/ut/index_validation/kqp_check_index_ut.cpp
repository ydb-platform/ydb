#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/json_index/json_index.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <yql/essentials/types/binary_json/write.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#include <util/folder/dirut.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

constexpr TStringBuf Check = "CHECK TABLE `/Root/Docs` INDEX `idx`;";

class TFixture {
public:
    explicit TFixture(bool compact = false, bool smallBatches = false, bool newRbo = false, bool forceSpilling = false)
        : Kikimr(Settings(compact, smallBatches, newRbo, forceSpilling))
        , Client(Kikimr.GetQueryClient())
        , Runtime(*Kikimr.GetTestServer().GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {}

    TExecuteQueryResult Execute(const TString& query) {
        return Kikimr.RunCall([&] {
            return Client.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        });
    }

    void Exec(const TString& query, const TParams& params) {
        const auto result = Runtime.WaitFuture(Client.ExecuteQuery(query, TTxControl::NoTx(), params));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void Exec(const TString& query) {
        const auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TVector<ui64> Shards(const TString& path) {
        return GetTableShards(&Kikimr.GetTestServer(), Sender, path);
    }

    void Corrupt(const TString& path, NKikimrDataEvents::TEvWrite::TOperation::EOperationType operation,
        std::initializer_list<TStringBuf> columns, const std::vector<TCell>& cells, TMaybe<ui64> shard = {})
    {
        if (!shard) {
            const auto shards = Shards(path);
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
            shard = shards.front();
        }
        const auto desc = Kikimr.GetTestClient().Describe(&Runtime, path, Tests::SchemeRoot, true);
        const auto& table = desc.GetPathDescription().GetTable();
        std::vector<ui32> tags;
        for (const auto column : columns) {
            const auto it = FindIf(table.GetColumns(), [&](const auto& item) { return item.GetName() == column; });
            UNIT_ASSERT(it != table.GetColumns().end());
            tags.push_back(it->GetId());
        }
        const TTableId id(desc.GetPathOwnerId(), desc.GetPathId(), table.GetTableSchemaVersion());
        auto request = MakeWriteRequest(std::nullopt, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            operation, id, tags, cells);
        Write(Runtime, Sender, *shard, std::move(request));
    }

    void AssertResult(const TExecuteQueryResult& result, ui64 missing, ui64 extra, ui64 mismatch,
        TStringBuf sampleKind = {}, TStringBuf sampleReason = {}, TStringBuf sampleRole = {},
        std::optional<ui64> sampleLimit = {}, ui64 emptyTokenPostings = 0)
    {
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Status").GetString(),
            missing || extra || mismatch ? "INCONSISTENT" : "OK");
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Missing").GetUint64(), missing);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Extra").GetUint64(), extra);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("ValueMismatch").GetUint64(), mismatch);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("EmptyTokenPostings").GetUint64(), emptyTokenPostings);
        UNIT_ASSERT(parser.ColumnParser("SnapshotStep").GetUint64() > 0);
        auto& sample = parser.ColumnParser("Sample");
        sample.OpenList();
        size_t sampleSize = 0;
        bool found = false;
        while (sample.TryNextListItem()) {
            ++sampleSize;
            TString kind;
            TString reason;
            TString role;
            sample.OpenStruct();
            while (sample.TryNextMember()) {
                if (sample.GetMemberName() == "Kind") {
                    kind = sample.GetString();
                } else if (sample.GetMemberName() == "Reason") {
                    reason = sample.GetString();
                } else if (sample.GetMemberName() == "Role") {
                    role = sample.GetString();
                }
            }
            sample.CloseStruct();
            found |= kind == sampleKind && reason == sampleReason && (sampleRole.empty() || role == sampleRole);
        }
        sample.CloseList();
        UNIT_ASSERT_VALUES_EQUAL(sampleSize, sampleLimit.value_or(Min<ui64>(100, missing + extra + mismatch)));
        if (!sampleKind.empty()) {
            UNIT_ASSERT(found);
        }
        UNIT_ASSERT(!parser.TryNextRow());
    }

    void CreateOrdinary(bool unique) {
        Exec(TStringBuilder() << R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tag Uint64, Data Utf8, Unindexed Utf8, PRIMARY KEY (Id),
                INDEX idx GLOBAL )" << (unique ? "UNIQUE " : "") << R"(ON (Tag) COVER (Data)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        Exec(R"(UPSERT INTO Docs (Id, Tag, Data) VALUES (1, 10, "original"u), (101, NULL, NULL);)");
        UNIT_ASSERT_VALUES_EQUAL(Shards("/Root/Docs").size(), 2);
    }

    void SplitShard(const TString& index, ui64 shard, ui64 border, TStringBuf nextKey = {}) {
        SetSplitMergePartCountLimit(&Runtime, -1);
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());
        auto* operation = request->Record.MutableTransaction()->MutableModifyScheme();
        operation->SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
        auto* split = operation->MutableSplitMergeTablePartitions();
        split->SetTablePath(index);
        split->AddSourceTabletId(shard);
        auto* key = split->AddSplitBoundary()->MutableKeyPrefix();
        key->AddTuple()->MutableOptional()->SetUint64(border);
        if (nextKey) {
            key->AddTuple()->MutableOptional()->SetText(TString(nextKey));
        }
        Runtime.Send(new IEventHandle(MakeTxProxyID(), Sender, request.Release()), 0, true);
        const auto response = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(Sender);
        const auto& proposal = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(proposal.GetStatus(), TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress,
            proposal.ShortDebugString());
        WaitTxNotification(Tests::TServer::TPtr(&Kikimr.GetTestServer()), Sender, proposal.GetTxId());
    }

    TKikimrRunner Kikimr;
    TQueryClient Client;
    TTestActorRuntime& Runtime;
    TActorId Sender;

private:
    static TKikimrSettings Settings(bool compact, bool smallBatches, bool newRbo, bool forceSpilling) {
        NKikimrConfig::TFeatureFlags flags;
        flags.SetEnableJsonIndex(true);
        flags.SetEnableAddUniqueIndex(true);
        flags.SetEnableFulltextIndexPrefix(true);
        flags.SetEnableFulltextIndexRowId(true);
        flags.SetEnableCompactFulltextIndex(compact);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false)
            .SetEnableScriptExecutionOperations(true).SetFeatureFlags(flags);
        settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        if (compact) {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        }
        if (newRbo) {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
            settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        }
        if (smallBatches) {
            auto* reads = settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadsRetrySettings();
            reads->SetMaxShardRetries(100);
            reads->SetMaxShardResolves(100);
            reads->SetMaxTotalRetries(100);
            reads->SetMaxRowsProcessingStreamLookup(2);
            auto* resources = settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager();
            resources->SetChannelBufferSize(64 * 1024);
            resources->SetMinChannelBufferSize(64 * 1024);
        }
        if (forceSpilling) {
            auto* service = settings.AppConfig.MutableTableServiceConfig();
            service->SetEnableQueryServiceSpilling(true);
            auto* resources = service->MutableResourceManager();
            resources->SetMkqlLightProgramMemoryLimit(100);
            resources->SetMkqlHeavyProgramMemoryLimit(300);
            resources->SetSpillingPercent(0.01);
            auto* spilling = service->MutableSpillingServiceConfig()->MutableLocalFileConfig();
            spilling->SetEnable(true);
            spilling->SetRoot("./spilling/");
            MakeDirIfNotExist("./spilling");
        }
        // The fixture uses simulated time, while script result expiration uses wall-clock time.
        settings.AppConfig.MutableQueryServiceConfig()->SetScriptResultsTtlDefaultSeconds(0);
        return settings;
    }
};

struct TSampleField {
    TString Type;
    std::optional<TString> Value;
};

using TSampleFields = THashMap<TString, TSampleField>;

TSampleFields ParseFields(TValueParser& parser) {
    TSampleFields fields;
    parser.OpenList();
    while (parser.TryNextListItem()) {
        TString name;
        TSampleField field;
        parser.OpenStruct();
        while (parser.TryNextMember()) {
            if (parser.GetMemberName() == "Name") {
                name = parser.GetString();
            } else if (parser.GetMemberName() == "Type") {
                field.Type = parser.GetString();
            } else if (parser.GetMemberName() == "Value") {
                field.Value = parser.GetOptionalString();
            }
        }
        parser.CloseStruct();
        UNIT_ASSERT(fields.emplace(std::move(name), std::move(field)).second);
    }
    parser.CloseList();
    return fields;
}

constexpr TStringBuf MappedPostingsPath = "/Root/Docs/idx/indexImplTable";
constexpr TStringBuf RowIdMappingPath = "/Root/Docs/uniq_rowid/indexImplTable";
TString FirstDocumentToken() {
    TString error;
    const auto tokens = NJsonIndex::TokenizeJson(R"({"tag":"a"})", error);
    UNIT_ASSERT_C(error.empty(), error);
    UNIT_ASSERT(!tokens.empty());
    return tokens.front();
}

void CreateMappedJsonIndex(TFixture& fixture) {
    fixture.Exec(R"(
        CREATE TABLE Docs (
            Org Utf8 NOT NULL, Id Uint64 NOT NULL, Tenant Uint64, Payload JsonDocument,
            __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Org, Id)
        ) WITH (PARTITION_AT_KEYS = (("m"u)));
    )");
    fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);");
    fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Tenant, Payload);");
    fixture.Exec(R"(
        UPSERT INTO Docs (Org, Id, Tenant, Payload) VALUES
            ("a"u, 1, 7, JsonDocument('{"tag":"a"}')),
            ("z"u, 2, 8, JsonDocument('{"tag":"b"}'));
    )");
    UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
}

ui64 FirstDocumentRowId(TFixture& fixture) {
    const auto result = fixture.Execute(R"(SELECT __ydb_row_id FROM Docs WHERE Org = "a"u AND Id = 1;)");
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());
    return parser.ColumnParser("__ydb_row_id").GetUint64();
}

void WriteMappedPosting(TFixture& fixture, NKikimrDataEvents::TEvWrite::TOperation::EOperationType operation,
    ui64 tenant, TStringBuf token, ui64 rowId)
{
    fixture.Corrupt(TString(MappedPostingsPath), operation, {"Tenant", "__ydb_token", "__ydb_row_id"},
        {TCell::Make(tenant), TCell(token.data(), token.size()), TCell::Make(rowId)});
}

THashMap<TString, TSampleFields> ReadMappedSampleKeys(const TExecuteQueryResult& result) {
    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());
    auto& samples = parser.ColumnParser("Sample");
    samples.OpenList();
    THashMap<TString, TSampleFields> keys;
    while (samples.TryNextListItem()) {
        TString kind;
        TString role;
        TString reason;
        TSampleFields key;
        samples.OpenStruct();
        while (samples.TryNextMember()) {
            if (samples.GetMemberName() == "Kind") {
                kind = samples.GetString();
            } else if (samples.GetMemberName() == "Role") {
                role = samples.GetString();
            } else if (samples.GetMemberName() == "Reason") {
                reason = samples.GetString();
            } else if (samples.GetMemberName() == "Key") {
                key = ParseFields(samples);
            }
        }
        samples.CloseStruct();
        UNIT_ASSERT(keys.emplace(TStringBuilder() << role << ':' << kind << ':' << reason, std::move(key)).second);
    }
    samples.CloseList();
    return keys;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpCheckIndex) {
    Y_UNIT_TEST(MissingBaseTable) {
        TFixture fixture;
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_SCHEME_ERROR));
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot find table");
    }

    Y_UNIT_TEST(UnknownIndex) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto result = fixture.Execute("CHECK TABLE `/Root/Docs` INDEX unknown;");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_SCHEME_ERROR));
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index not found: unknown");
    }

    Y_UNIT_TEST(IndexNotReady) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        auto indexState = fixture.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(
            [](auto& ev) {
                for (auto& entry : ev->Get()->Request->ResultSet) {
                    for (auto& index : entry.Indexes) {
                        if (index.GetName() == "idx") {
                            index.SetState(NKikimrSchemeOp::EIndexStateNotReady);
                        }
                    }
                }
            });
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_INDEX_IS_NOT_READY));
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index is not ready: idx");
    }

    Y_UNIT_TEST_TWIN(CoverMismatch, Unique) {
        TFixture fixture;
        fixture.CreateOrdinary(Unique);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        const ui64 tag = 10;
        const ui64 id = 1;
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Tag", "Id", "Data"}, {TCell::Make(tag), TCell::Make(id), TCell()});
        const auto result = fixture.Execute(TString(Check));
        fixture.AssertResult(result, 0, 0, 1, "VALUE_MISMATCH");
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto& sample = parser.ColumnParser("Sample");
        sample.OpenList();
        UNIT_ASSERT(sample.TryNextListItem());
        sample.OpenStruct();
        while (sample.TryNextMember()) {
            if (sample.GetMemberName() == "Key") {
                const auto fields = ParseFields(sample);
                UNIT_ASSERT_VALUES_EQUAL(fields.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(fields.at("Tag").Type, "Uint64");
                UNIT_ASSERT_VALUES_EQUAL(fields.at("Tag").Value.value(), "10");
                UNIT_ASSERT_VALUES_EQUAL(fields.at("Id").Value.value(), "1");
            } else if (sample.GetMemberName() == "Expected" || sample.GetMemberName() == "Actual") {
                const bool expected = sample.GetMemberName() == "Expected";
                sample.OpenOptional();
                UNIT_ASSERT(!sample.IsNull());
                const auto fields = ParseFields(sample);
                UNIT_ASSERT_VALUES_EQUAL(fields.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(fields.at("Data").Type, "Utf8");
                if (expected) {
                    UNIT_ASSERT_VALUES_EQUAL(fields.at("Data").Value.value(), "original");
                } else {
                    UNIT_ASSERT(!fields.at("Data").Value);
                }
                sample.CloseOptional();
            }
        }
        sample.CloseStruct();
        sample.CloseList();
    }

    Y_UNIT_TEST_TWIN(JsonCoverMismatch, Unique) {
        TFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tag Uint64, Payload Json, Document JsonDocument,
                PRIMARY KEY (Id),
                INDEX idx GLOBAL )" << (Unique ? "UNIQUE " : "") << R"(ON (Tag) COVER (Payload, Document)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        fixture.Exec(R"(
            UPSERT INTO Docs (Id, Tag, Payload, Document) VALUES
                (1, 10, Json('{"v":1}'), JsonDocument('{"v":1}')),
                (2, 20, Json('{"v":2}'), JsonDocument('{"v":2}')),
                (101, 30, NULL, NULL);
        )");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        constexpr TStringBuf impl = "/Root/Docs/idx/indexImplTable";
        constexpr TStringBuf different = R"({"v":3})";
        constexpr auto upsert = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        fixture.Corrupt(TString(impl), upsert, {"Tag", "Id", "Payload"},
            {TCell::Make(ui64{10}), TCell::Make(ui64{1}), TCell(different.data(), different.size())});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 1, "VALUE_MISMATCH");

        const auto binary = NBinaryJson::SerializeToBinaryJson(different);
        UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binary));
        const auto& document = std::get<NBinaryJson::TBinaryJson>(binary);
        fixture.Corrupt(TString(impl), upsert, {"Tag", "Id", "Document"},
            {TCell::Make(ui64{20}), TCell::Make(ui64{2}), TCell(document.Data(), document.Size())});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 2, "VALUE_MISMATCH");

        fixture.Corrupt(TString(impl), upsert, {"Tag", "Id", "Payload", "Document"},
            {TCell::Make(ui64{10}), TCell::Make(ui64{1}), TCell(), TCell()});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 2, "VALUE_MISMATCH");

        fixture.Corrupt(TString(impl), upsert, {"Tag", "Id", "Payload", "Document"},
            {TCell::Make(ui64{30}), TCell::Make(ui64{101}), TCell(different.data(), different.size()),
                TCell(document.Data(), document.Size())});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 3, "VALUE_MISMATCH");
    }

    Y_UNIT_TEST(SampleDoesNotLimitCounts) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        fixture.Exec(R"(
            $rows = ListMap(ListFromRange(200ul, 305ul), ($id) -> (
                AsStruct($id AS Id, $id AS Tag, "data"u AS Data)
            ));
            UPSERT INTO Docs SELECT * FROM AS_TABLE($rows);
        )");

        std::vector<TCell> cells;
        for (ui64 id = 200; id < 305; ++id) {
            cells.push_back(TCell::Make(id));
            cells.push_back(TCell::Make(id));
            cells.emplace_back();
        }
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Tag", "Id", "Data"}, cells);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 105, "VALUE_MISMATCH");
    }

    Y_UNIT_TEST_TWIN(BatchedLookupsAcrossIndexPartitions, Unique) {
        TFixture fixture(false, true);
        fixture.CreateOrdinary(Unique);
        const TString index = "/Root/Docs/idx/indexImplTable";
        const auto originalShards = fixture.Shards(index);
        UNIT_ASSERT_VALUES_EQUAL(originalShards.size(), 1);
        fixture.SplitShard(index, originalShards.front(), 100);
        const auto shards = fixture.Shards(index);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2);
        fixture.Exec(R"(
            $rows = ListMap(ListFromRange(90ul, 112ul), ($id) -> (
                AsStruct($id AS Id, $id AS Tag, "data"u AS Data)
            ));
            UPSERT INTO Docs SELECT * FROM AS_TABLE($rows);
        )");
        constexpr auto upsert = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        constexpr auto erase = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        fixture.Corrupt(index, upsert, {"Tag", "Id", "Data"},
            {TCell::Make(ui64{10}), TCell::Make(ui64{1}), TCell()}, shards[0]);
        fixture.Corrupt(index, erase, {"Tag", "Id"}, {TCell::Make(ui64{110}), TCell::Make(ui64{110})}, shards[1]);
        fixture.Corrupt(index, upsert, {"Tag", "Id"}, {TCell::Make(ui64{111}), TCell::Make(ui64{400})}, shards[1]);

        THashMap<TActorId, THashMap<ui64, TString>> pointReads;
        THashMap<TString, size_t> batches;
        THashSet<ui64> visited;
        TRowVersion snapshot = TRowVersion::Min();
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() == TEvDataShard::TEvRead::EventType && IsIn(shards, forward->TabletId)) {
                const auto& read = *static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get());
                if (!read.Keys.empty()) {
                    const auto& record = read.Record;
                    UNIT_ASSERT(record.HasSnapshot());
                    const TRowVersion current(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
                    if (snapshot.IsMin()) {
                        snapshot = current;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(current, snapshot);
                    UNIT_ASSERT(read.Keys.size() <= 2);
                    TString batch;
                    for (const auto& key : read.Keys) {
                        batch += key.GetBuffer();
                    }
                    ++batches[batch];
                    pointReads[ev->Sender].emplace(record.GetReadId(), std::move(batch));
                    visited.insert(forward->TabletId);
                }
            }
        });
        std::optional<TString> failed;
        auto results = fixture.Runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            const auto* reads = pointReads.FindPtr(ev->GetRecipientRewrite());
            if (!failed && reads) {
                if (const auto* batch = reads->FindPtr(record.GetReadId())) {
                    record.MutableStatus()->SetCode(Ydb::StatusIds::OVERLOADED);
                    failed = *batch;
                }
            }
        });

        fixture.AssertResult(fixture.Execute(TString(Check)), 1, 1, 1);
        UNIT_ASSERT(failed);
        UNIT_ASSERT_VALUES_EQUAL(batches.at(*failed), 2);
        UNIT_ASSERT(batches.size() > 2);
        UNIT_ASSERT_VALUES_EQUAL(visited.size(), 2);
    }

    Y_UNIT_TEST(PhysicalIndexSplitAfterPartialRead) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const TString index = "/Root/Docs/idx/indexImplTable";
        const auto shards = fixture.Shards(index);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        const auto original = shards.front();
        fixture.Exec(R"(
            UPSERT INTO Docs (Id, Tag, Data) VALUES
                (90, 90, "data"u), (91, 91, "data"u), (100, 100, "data"u), (110, 110, "data"u);
        )");
        constexpr auto upsert = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        constexpr auto erase = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        fixture.Corrupt(index, upsert, {"Tag", "Id", "Data"},
            {TCell::Make(ui64{10}), TCell::Make(ui64{1}), TCell()});
        fixture.Corrupt(index, erase, {"Tag", "Id"}, {TCell::Make(ui64{110}), TCell::Make(ui64{110})});
        fixture.Corrupt(index, upsert, {"Tag", "Id"}, {TCell::Make(ui64{111}), TCell::Make(ui64{400})});

        const auto desc = fixture.Kikimr.GetTestClient().Describe(&fixture.Runtime, index, Tests::SchemeRoot, true);
        THashMap<TActorId, THashSet<ui64>> originalScans;
        THashSet<ui64> scanned;
        TRowVersion snapshot = TRowVersion::Min();
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType) {
                return;
            }
            auto& read = *static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get());
            auto& record = read.Record;
            if (record.GetTableId().GetOwnerId() != desc.GetPathOwnerId()
                || record.GetTableId().GetTableId() != desc.GetPathId())
            {
                return;
            }
            UNIT_ASSERT(record.HasSnapshot());
            const TRowVersion current(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
            if (snapshot.IsMin()) {
                snapshot = current;
            }
            UNIT_ASSERT_VALUES_EQUAL(current, snapshot);
            if (read.Keys.empty()) {
                scanned.insert(forward->TabletId);
                if (forward->TabletId == original) {
                    record.SetMaxRows(1);
                    record.SetMaxRowsInResult(1);
                    originalScans[ev->Sender].insert(record.GetReadId());
                }
            }
        });
        NActors::TBlockEvents<TEvPipeCache::TEvForward> paused(fixture.Runtime, [&](const auto& ev) {
            const auto* forward = ev->Get();
            if (forward->TabletId != original || forward->Ev->Type() != TEvDataShard::TEvReadAck::EventType) {
                return false;
            }
            const auto* reads = originalScans.FindPtr(ev->Sender);
            const auto& ack = static_cast<const TEvDataShard::TEvReadAck&>(*forward->Ev);
            return reads && reads->contains(ack.Record.GetReadId());
        });

        auto running = fixture.Client.ExecuteQuery(TString(Check), TTxControl::NoTx());
        fixture.Runtime.WaitFor("acknowledgment of a partial physical scan", [&] {
            return !paused.empty() || running.HasValue();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!paused.empty(), running.GetValueSync().GetIssues().ToString());
        fixture.SplitShard(index, original, 100);
        const auto children = fixture.Shards(index);
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
        paused.Stop().Unblock();

        const auto result = fixture.Runtime.WaitFuture(running);
        fixture.AssertResult(result, 1, 1, 1);
        for (const auto child : children) {
            UNIT_ASSERT_C(!IsIn(shards, child) && scanned.contains(child), child);
        }
        TResultSetParser report(result.GetResultSet(0));
        UNIT_ASSERT(report.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(report.ColumnParser("SnapshotStep").GetUint64(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(report.ColumnParser("SnapshotTxId").GetUint64(), snapshot.TxId);
    }

    Y_UNIT_TEST(WideCoverValuesDoNotLimitCounts) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const TString value(1024 * 1024, 'x');
        const TString wrong(1024 * 1024, 'y');
        for (ui64 first = 200; first < 230; first += 3) {
            auto params = TParamsBuilder().AddParam("$value").Utf8(value).Build()
                .AddParam("$first").Uint64(first).Build().Build();
            fixture.Exec(R"(
                DECLARE $value AS Utf8;
                DECLARE $first AS Uint64;
                $rows = ListMap(ListFromRange($first, $first + 3ul), ($id) -> (
                    AsStruct($id AS Id, $id AS Tag, $value AS Data)
                ));
                UPSERT INTO Docs SELECT * FROM AS_TABLE($rows);
            )", params);
        }
        for (ui64 id = 200; id < 230; ++id) {
            fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {"Tag", "Id", "Data"}, {TCell::Make(id), TCell::Make(id), TCell(wrong.data(), wrong.size())});
        }
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 30, {}, {}, {}, 0);
    }

    Y_UNIT_TEST_TWIN(EqualPostingCounts, BinaryJson) {
        TFixture fixture;
        const TStringBuf type = BinaryJson ? "JsonDocument" : "Json";
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Payload )" << type << R"(, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Payload)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        fixture.Exec(TStringBuilder() << "UPSERT INTO Docs (Id, Payload) VALUES "
            << "(1, " << type << R"(('{"tag":["a","a"]}')), )"
            << "(101, " << type << R"(('{"tag":"b"}'));)");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        TString error;
        const auto tokens = NJsonIndex::TokenizeJson(R"({"tag":["a","a"]})", error);
        UNIT_ASSERT_C(error.empty(), error);
        UNIT_ASSERT(!tokens.empty());

        const auto count = [&] {
            auto result = fixture.Execute("SELECT COUNT(*) AS Rows FROM `/Root/Docs/idx/indexImplTable`;");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser("Rows").GetUint64();
        };
        const auto before = count();
        const ui64 id = 1;
        constexpr TStringBuf impl = "/Root/Docs/idx/indexImplTable";
        fixture.Corrupt(TString(impl), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
            {"__ydb_token", "Id"}, {TCell(tokens.back().data(), tokens.back().size()), TCell::Make(id)});
        constexpr TStringBuf unexpected = "unexpected-token";
        fixture.Corrupt(TString(impl), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"__ydb_token", "Id"}, {TCell(unexpected.data(), unexpected.size()), TCell::Make(id)});

        UNIT_ASSERT_VALUES_EQUAL(count(), before);
        const auto result = fixture.Execute(TString(Check));
        fixture.AssertResult(result, 1, 1, 0, "EXTRA");
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto& sample = parser.ColumnParser("Sample");
        sample.OpenList();
        THashMap<TString, TString> found;
        while (sample.TryNextListItem()) {
            TString kind;
            TSampleFields key;
            sample.OpenStruct();
            while (sample.TryNextMember()) {
                if (sample.GetMemberName() == "Kind") {
                    kind = sample.GetString();
                } else if (sample.GetMemberName() == "Key") {
                    key = ParseFields(sample);
                }
            }
            sample.CloseStruct();
            UNIT_ASSERT_VALUES_EQUAL(key.at("Id").Value.value(), "1");
            UNIT_ASSERT(found.emplace(kind, key.at("__ydb_token").Value.value()).second);
        }
        sample.CloseList();
        UNIT_ASSERT_VALUES_EQUAL(found.at("MISSING"), tokens.back());
        UNIT_ASSERT_VALUES_EQUAL(found.at("EXTRA"), unexpected);
    }

    Y_UNIT_TEST_TWIN(JsonFanoutDoesNotRepeatDocumentLookups, ExtraPosting) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tenant Uint64, Payload JsonDocument, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Tenant, Payload)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        TStringBuilder json;
        json << "[";
        for (ui64 i = 0; i < 512; ++i) {
            if (i) {
                json << ",";
            }
            json << i;
        }
        json << "]";
        auto params = TParamsBuilder().AddParam("$payload").JsonDocument(json).Build().Build();
        fixture.Exec(R"(
            DECLARE $payload AS JsonDocument;
            UPSERT INTO Docs (Id, Tenant, Payload) VALUES (1, 7, $payload), (101, 8, $payload);
        )", params);

        if (ExtraPosting) {
            constexpr TStringBuf unexpected = "unexpected";
            fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {"Tenant", "__ydb_token", "Id"},
                {TCell::Make(ui64{7}), TCell(unexpected.data(), unexpected.size()), TCell::Make(ui64{1})});
        }

        const auto base = fixture.Shards("/Root/Docs");
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() == TEvDataShard::TEvRead::EventType && IsIn(base, forward->TabletId)) {
                const auto& read = *static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get());
                UNIT_ASSERT_C(read.Keys.empty(), "JSON validation must analyze each document from a scan, including when sampling extras");
            }
        });
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, ExtraPosting ? 1 : 0, 0);
    }

    Y_UNIT_TEST(JsonScanComparisonSpills) {
        TFixture fixture(false, false, false, true);
        fixture.Runtime.SetFastSimulatedTime(false);
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Payload JsonDocument, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Payload)
            ) WITH (PARTITION_AT_KEYS = (1024));
        )");
        const TString payload = TStringBuilder() << "{\"value\":\"" << TString(1024, 'x') << "\"}";
        auto builder = TParamsBuilder();
        auto& rows = builder.AddParam("$rows").BeginList();
        for (ui64 id = 0; id < 2048; ++id) {
            rows.AddListItem().BeginStruct()
                .AddMember("Id").Uint64(id)
                .AddMember("Payload").JsonDocument(payload)
                .EndStruct();
        }
        rows.EndList().Build();
        fixture.Exec(R"(
            DECLARE $rows AS List<Struct<Id: Uint64, Payload: JsonDocument>>;
            UPSERT INTO Docs SELECT * FROM AS_TABLE($rows);
        )", builder.Build());
        TKqpCounters counters(fixture.Runtime.GetAppData().Counters);
        const auto writes = counters.ComputeSpilling.WriteBlobs->Val();
        const auto reads = counters.ComputeSpilling.ReadBlobs->Val();
        const auto result = fixture.Runtime.WaitFuture(fixture.Client.ExecuteQuery(
            TStringBuilder() << "PRAGMA ydb.EnableSpillingNodes=\"WideSort\";" << Check, TTxControl::NoTx(),
            TExecuteQuerySettings().StatsMode(EStatsMode::Full)));
        fixture.AssertResult(result, 0, 0, 0);
        UNIT_ASSERT(result.GetStats());
        const auto ast = result.GetStats()->GetAst().value_or("AST is not available");
        UNIT_ASSERT_C(counters.ComputeSpilling.WriteBlobs->Val() > writes, ast);
        UNIT_ASSERT_C(counters.ComputeSpilling.ReadBlobs->Val() > reads, ast);
    }

    Y_UNIT_TEST_TWIN(EmptyJsonIndex, RowIds) {
        TFixture fixture;
        fixture.Exec(TStringBuilder() << "CREATE TABLE Docs (Id Uint64 NOT NULL, Payload JsonDocument, "
            << (RowIds ? "__ydb_row_id Uint64 NOT NULL, " : "") << "PRIMARY KEY (Id));");
        if (RowIds) {
            fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);");
        }
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Payload);");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
    }

    Y_UNIT_TEST(JsonScanComparisonWithNullKeys) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64, Tenant Uint64, Payload Json, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Tenant, Payload)
            );
        )");
        fixture.Exec(R"(
            UPSERT INTO Docs (Id, Tenant, Payload) VALUES
                (NULL, NULL, Json('{"tag":"a"}')), (2, 7, Json('{"tag":"b"}'));
        )");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        const auto token = FirstDocumentToken();
        constexpr TStringBuf unexpected = "unexpected";
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
            {"Tenant", "__ydb_token", "Id"}, {TCell(), TCell(token.data(), token.size()), TCell()});
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Tenant", "__ydb_token", "Id"}, {TCell(), TCell(unexpected.data(), unexpected.size()), TCell()});
        const auto result = fixture.Execute(TString(Check));
        fixture.AssertResult(result, 1, 1, 0);
        const auto keys = ReadMappedSampleKeys(result);
        UNIT_ASSERT(!keys.at("POSTINGS:MISSING:").at("Id").Value);
        UNIT_ASSERT(!keys.at("POSTINGS:MISSING:").at("Tenant").Value);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("POSTINGS:MISSING:").at("__ydb_token").Value.value(), token);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("POSTINGS:EXTRA:").at("__ydb_token").Value.value(), unexpected);
    }

    Y_UNIT_TEST(PrefixedEmptyTokenPostings) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tenant Uint64, Payload Json, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Tenant, Payload)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        fixture.Exec(R"(
            UPSERT INTO Docs (Id, Tenant, Payload) VALUES
                (1, 7, Json('{"tag":"a"}')), (101, 8, Json('{"tag":"b"}'));
        )");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Tenant", "__ydb_token", "Id"},
            {TCell::Make(ui64{7}), TCell("", 0), TCell::Make(ui64{1}),
             TCell::Make(ui64{8}), TCell("", 0), TCell::Make(ui64{101})});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 2, 0, "EXTRA", "EMPTY_TOKEN_POSTING", {}, {}, 2);
    }

    Y_UNIT_TEST(MappedJsonWithCoveredMapping) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Payload JsonDocument, Other Utf8,
                __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Id)
            );
        )");
        // Seed the base table before building the covered mapping so this test
        // exercises validation independently of index-maintenance DML.
        fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id) COVER (Other);");
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Payload);");
        // Seed physical rows so this regression does not depend on the separate
        // DML and index-build paths for covered row-ID mappings. NULL payloads
        // have no postings; both mapping COVER values still need comparison.
        constexpr auto upsert = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        constexpr ui64 rowId = 101;
        constexpr TStringBuf original = "original";
        fixture.Corrupt("/Root/Docs", upsert, {"Id", "__ydb_row_id", "Other"},
            {TCell::Make(ui64{1}), TCell::Make(rowId), TCell(original.data(), original.size()),
                TCell::Make(ui64{2}), TCell::Make(rowId + 1), TCell()});
        fixture.Corrupt(TString(RowIdMappingPath), upsert, {"__ydb_row_id", "Id", "Other"},
            {TCell::Make(rowId), TCell::Make(ui64{1}), TCell(original.data(), original.size()),
                TCell::Make(rowId + 1), TCell::Make(ui64{2}), TCell()});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        constexpr TStringBuf wrong = "wrong";
        fixture.Corrupt(TString(RowIdMappingPath), upsert,
            {"__ydb_row_id", "Id", "Other"}, {TCell::Make(rowId), TCell::Make(ui64{1}), TCell(wrong.data(), wrong.size())});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 1, "VALUE_MISMATCH", "", "ROW_ID_MAPPING");
    }

    Y_UNIT_TEST(MappedJsonWithPostingPrimaryKey) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Posting Uint64 NOT NULL, Pk Utf8 NOT NULL, Tenant Uint64, Payload JsonDocument,
                __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Posting, Pk)
            );
        )");
        fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);");
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Tenant, Payload);");
        fixture.Exec(R"(UPSERT INTO Docs (Posting, Pk, Tenant, Payload) VALUES (1, "a"u, 7, JsonDocument('{"tag":"a"}'));)");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        const auto row = fixture.Execute("SELECT __ydb_row_id FROM Docs;");
        UNIT_ASSERT_C(row.IsSuccess(), row.GetIssues().ToString());
        TResultSetParser parser(row.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto rowId = parser.ColumnParser("__ydb_row_id").GetUint64();
        const auto token = FirstDocumentToken();
        WriteMappedPosting(fixture, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE, 7, token, rowId);
        WriteMappedPosting(fixture, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, 7, "unexpected", rowId);
        const auto result = fixture.Execute(TString(Check));
        fixture.AssertResult(result, 1, 1, 0);
        const auto keys = ReadMappedSampleKeys(result);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("POSTINGS:MISSING:").at("__ydb_token").Value.value(), token);
        UNIT_ASSERT_VALUES_EQUAL(keys.at("POSTINGS:EXTRA:").at("__ydb_token").Value.value(), "unexpected");
    }

    Y_UNIT_TEST(MappedPostingExtraSamples) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        constexpr auto upsert = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        constexpr auto erase = NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        const auto rowId = FirstDocumentRowId(fixture);
        const auto token = FirstDocumentToken();
        constexpr TStringBuf unexpected = "unexpected-token";
        WriteMappedPosting(fixture, erase, 7, token, rowId);
        WriteMappedPosting(fixture, upsert, 7, unexpected, rowId);

        const auto withValidMap = fixture.Execute(TString(Check));
        fixture.AssertResult(withValidMap, 1, 1, 0);
        const auto validKeys = ReadMappedSampleKeys(withValidMap);
        const auto assertPostingKey = [&](const TSampleFields& key, TStringBuf token) {
            UNIT_ASSERT_VALUES_EQUAL(key.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(key.at("Tenant").Value.value(), "7");
            UNIT_ASSERT_VALUES_EQUAL(key.at("__ydb_token").Value.value(), token);
            UNIT_ASSERT_VALUES_EQUAL(key.at("__ydb_row_id").Value.value(), ToString(rowId));
        };
        assertPostingKey(validKeys.at("POSTINGS:MISSING:"), token);
        assertPostingKey(validKeys.at("POSTINGS:EXTRA:"), unexpected);

        fixture.Corrupt(TString(RowIdMappingPath), erase, {"__ydb_row_id", "Org", "Id"},
            {TCell::Make(rowId), TCell("a", 1), TCell::Make(ui64{1})});
        fixture.Corrupt(TString(RowIdMappingPath), upsert, {"__ydb_row_id", "Org", "Id"},
            {TCell::Make(rowId), TCell("a", 1), TCell::Make(ui64{99})});
        WriteMappedPosting(fixture, upsert, 7, "", rowId);

        const auto withBrokenMap = fixture.Execute(TString(Check));
        fixture.AssertResult(withBrokenMap, 2, 3, 0, "EXTRA", "EMPTY_TOKEN_POSTING", "POSTINGS", 5, 1);
        const auto brokenKeys = ReadMappedSampleKeys(withBrokenMap);
        // The posting scan proves absence directly, even when the mapping is corrupt.
        assertPostingKey(brokenKeys.at("POSTINGS:EXTRA:"), unexpected);
        assertPostingKey(brokenKeys.at("POSTINGS:MISSING:"), token);
        assertPostingKey(brokenKeys.at("POSTINGS:EXTRA:EMPTY_TOKEN_POSTING"), "");
        const auto assertMappingKey = [&](const TSampleFields& key, TStringBuf id) {
            UNIT_ASSERT_VALUES_EQUAL(key.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(key.at("__ydb_row_id").Value.value(), ToString(rowId));
            UNIT_ASSERT_VALUES_EQUAL(key.at("Org").Value.value(), "a");
            UNIT_ASSERT_VALUES_EQUAL(key.at("Id").Value.value(), id);
        };
        assertMappingKey(brokenKeys.at("ROW_ID_MAPPING:MISSING:"), "1");
        assertMappingKey(brokenKeys.at("ROW_ID_MAPPING:EXTRA:"), "99");
    }

    Y_UNIT_TEST_TWIN(MappedJsonSnapshotAcrossRoles, LoseMappingSnapshot) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        const auto rowId = FirstDocumentRowId(fixture);
        // This posting is extra at the old snapshot and valid after the update.
        const auto token = FirstDocumentToken();
        WriteMappedPosting(fixture, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, 9, token, rowId);
        const auto baseShards = fixture.Shards("/Root/Docs");
        const auto postingShards = fixture.Shards(TString(MappedPostingsPath));
        const auto mappingShards = fixture.Shards(TString(RowIdMappingPath));
        const auto baseDescription = fixture.Kikimr.GetTestClient().Describe(&fixture.Runtime, "/Root/Docs", Tests::SchemeRoot, true);
        const auto& columns = baseDescription.GetPathDescription().GetTable().GetColumns();
        const auto payloadColumn = FindIf(columns, [](const auto& column) { return column.GetName() == "Payload"; });
        UNIT_ASSERT(payloadColumn != columns.end());
        const ui32 payloadTag = payloadColumn->GetId();

        const auto sessionResult = fixture.Runtime.WaitFuture(fixture.Client.GetSession());
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        std::optional<TTransaction> updateTx;
        if (!LoseMappingSnapshot) {
            const auto update = fixture.Runtime.WaitFuture(session.ExecuteQuery(R"(
                UPDATE Docs SET Tenant = 9, Payload = JsonDocument('{"tag":"a","other":"updated"}')
                WHERE Org = "a"u AND Id = 1;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW())));
            UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
            updateTx = update.GetTransaction();
            UNIT_ASSERT(updateTx && updateTx->IsActive());
        }

        TRowVersion snapshot = TRowVersion::Min();
        THashSet<ui64> touchedShards;
        THashMap<TActorId, THashMap<ui64, ui64>> baseReads;
        THashSet<ui64> finishedBaseShards;
        bool committed = false;
        bool readMappingByRowId = false;
        bool readBaseByPrimaryKey = false;
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            const ui64 shard = forward->TabletId;
            const bool base = IsIn(baseShards, shard);
            const bool posting = IsIn(postingShards, shard);
            const bool mapping = IsIn(mappingShards, shard);
            if ((!base && !posting && !mapping) || forward->Ev->Type() != TEvDataShard::TEvRead::EventType) {
                return;
            }
            const auto& read = *static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get());
            const auto& record = read.Record;
            UNIT_ASSERT(record.HasSnapshot());
            const auto current = TRowVersion::FromProto(record.GetSnapshot());
            if (snapshot.IsMin()) {
                snapshot = current;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(current, snapshot, "shard " << shard);
            touchedShards.insert(shard);
            if (base && read.Keys.empty()) {
                baseReads[ev->Sender].emplace(record.GetReadId(), shard);
            }
            if (committed && base) {
                if (!read.Keys.empty()) {
                    UNIT_ASSERT_C(!IsIn(record.GetColumns(), payloadTag), "Mapping probes must not reanalyze JSON documents");
                }
                readBaseByPrimaryKey |= AnyOf(read.Keys, [](const auto& key) {
                    const auto cells = key.GetCells();
                    return cells.size() == 2 && cells[0].AsBuf() == "a" && !cells[1].IsNull()
                        && cells[1].template AsValue<ui64>() == 1;
                });
            }
            if (committed && mapping) {
                readMappingByRowId |= AnyOf(read.Ranges, [&](const auto& range) {
                    const auto cells = range.To.GetCells();
                    return cells.size() == 1 && !cells[0].IsNull() && cells[0].template AsValue<ui64>() == rowId;
                });
            }
        });
        auto results = fixture.Runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetFinished()) {
                if (const auto* reads = baseReads.FindPtr(ev->GetRecipientRewrite())) {
                    if (const auto* shard = reads->FindPtr(record.GetReadId())) {
                        finishedBaseShards.insert(*shard);
                    }
                }
            }
        });
        size_t blockedMappingReads = 0;
        size_t blockedPostingReads = 0;
        NActors::TBlockEvents<TEvPipeCache::TEvForward> blockedReads(fixture.Runtime, [&](const auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType) {
                return false;
            }
            if (IsIn(mappingShards, forward->TabletId)) {
                ++blockedMappingReads;
                return true;
            }
            if (!LoseMappingSnapshot && IsIn(postingShards, forward->TabletId)) {
                ++blockedPostingReads;
                return true;
            }
            return false;
        });

        auto running = fixture.Client.ExecuteQuery(TString(Check), TTxControl::NoTx());
        fixture.Runtime.WaitFor("finished base scans and blocked index reads", [&] {
            return running.HasValue() || (finishedBaseShards.size() == baseShards.size()
                && (LoseMappingSnapshot ? blockedMappingReads > 0 : blockedPostingReads > 0));
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!running.HasValue(), running.GetValueSync().GetIssues().ToString());
        if (LoseMappingSnapshot) {
            const auto reader = blockedReads.front()->Sender;
            const auto* forward = blockedReads.front()->Get();
            const auto readId = static_cast<const TEvDataShard::TEvRead*>(forward->Ev.Get())->Record.GetReadId();
            bool snapshotLost = false;
            auto failMappingRead = fixture.Runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
                auto& record = ev->Get()->Record;
                if (ev->GetRecipientRewrite() == reader && record.GetReadId() == readId) {
                    record.MutableStatus()->SetCode(Ydb::StatusIds::PRECONDITION_FAILED);
                    record.MutableStatus()->AddIssues()->set_message("Requested mapping snapshot is unavailable");
                    snapshotLost = true;
                }
            });
            blockedReads.Stop().Unblock();
            const auto result = fixture.Runtime.WaitFuture(running);
            UNIT_ASSERT(snapshotLost);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Requested mapping snapshot is unavailable");
            UNIT_ASSERT(result.GetResultSets().empty());
            return;
        }

        const auto commit = fixture.Runtime.WaitFuture(updateTx->Commit());
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        committed = true;
        blockedReads.Stop().Unblock();
        const auto result = fixture.Runtime.WaitFuture(running);
        requests.Remove();
        results.Remove();

        fixture.AssertResult(result, 0, 1, 0, "EXTRA", "", "POSTINGS");
        for (const auto& shards : {baseShards, postingShards, mappingShards}) {
            UNIT_ASSERT(AllOf(shards, [&](ui64 shard) { return touchedShards.contains(shard); }));
        }
        UNIT_ASSERT(!readMappingByRowId);
        UNIT_ASSERT(readBaseByPrimaryKey);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotStep").GetUint64(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotTxId").GetUint64(), snapshot.TxId);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
    }

    Y_UNIT_TEST(MappingGenerationChangedBeforeReport) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        const auto mappingShards = fixture.Shards(TString(RowIdMappingPath));
        bool readMapping = false;
        auto reads = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            readMapping |= IsIn(mappingShards, forward->TabletId)
                && forward->Ev->Type() == TEvDataShard::TEvRead::EventType;
        });

        bool changed = false;
        auto results = fixture.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            auto& entries = ev->Get()->Request->ResultSet;
            if (!readMapping || !AllOf(entries, [](const auto& entry) {
                    return entry.SyncVersion && entry.ShowPrivatePath
                        && entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                }))
            {
                return;
            }
            for (auto& entry : entries) {
                for (auto& index : entry.Indexes) {
                    if (index.GetName() == "uniq_rowid") {
                        index.SetLocalPathId(index.GetLocalPathId() + 1);
                        changed = true;
                    }
                }
            }
        });

        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT_C(changed, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index schema changed during validation");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(BrokenRowIdMapping) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        const auto rowId = FirstDocumentRowId(fixture);
        const ui64 id = 1;
        const ui64 bogusId = 99;
        const char org = 'a';
        constexpr TStringBuf mapping = "/Root/Docs/uniq_rowid/indexImplTable";
        fixture.Corrupt(TString(mapping), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
            {"__ydb_row_id", "Org", "Id"}, {TCell::Make(rowId), TCell(&org, 1), TCell::Make(id)});
        fixture.Corrupt(TString(mapping), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"__ydb_row_id", "Org", "Id"}, {TCell::Make(rowId), TCell(&org, 1), TCell::Make(bogusId)});
        fixture.AssertResult(fixture.Execute(TString(Check)), 1, 1, 0, "MISSING", "", "ROW_ID_MAPPING");
    }

    Y_UNIT_TEST(DuplicateStoredRowIds) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Org Utf8 NOT NULL, Id Uint64 NOT NULL, Tenant Uint64, Payload JsonDocument,
                __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Org, Id)
            ) WITH (PARTITION_AT_KEYS = (("m"u)));
        )");
        fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);");
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Tenant, Payload);");
        fixture.Exec(R"(
            UPSERT INTO Docs (Org, Id, Tenant, Payload) VALUES
                ("a"u, 1, 7, JsonDocument('{"tag":"a"}')), ("z"u, 101, 7, JsonDocument('{"tag":"a"}'));
        )");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        const auto row = fixture.Execute(R"(SELECT __ydb_row_id FROM Docs WHERE Org = "a"u AND Id = 1;)");
        UNIT_ASSERT_C(row.IsSuccess(), row.GetIssues().ToString());
        TResultSetParser parser(row.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto rowId = parser.ColumnParser("__ydb_row_id").GetUint64();
        const auto shards = fixture.Shards("/Root/Docs");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2);
        fixture.Corrupt("/Root/Docs", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Org", "Id", "__ydb_row_id"}, {TCell("z", 1), TCell::Make(ui64{101}), TCell::Make(rowId)}, shards[1]);
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Condition violated");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Stored document IDs must be unique");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST_TWIN(DuplicateRowIdsWithDistinctPostings, CompleteMapping) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        const auto firstId = FirstDocumentRowId(fixture);
        const auto result = fixture.Execute(R"(SELECT __ydb_row_id FROM Docs WHERE Org = "z"u AND Id = 2;)");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto secondId = parser.ColumnParser("__ydb_row_id").GetUint64();

        fixture.Corrupt("/Root/Docs", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Org", "Id", "__ydb_row_id"}, {TCell("z", 1), TCell::Make(ui64{2}), TCell::Make(firstId)},
            fixture.Shards("/Root/Docs").back());
        if (CompleteMapping) {
            fixture.Corrupt(TString(RowIdMappingPath), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
                {"__ydb_row_id", "Org", "Id"}, {TCell::Make(secondId), TCell("z", 1), TCell::Make(ui64{2})});
            fixture.Corrupt(TString(RowIdMappingPath), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {"__ydb_row_id", "Org", "Id"}, {TCell::Make(firstId), TCell("z", 1), TCell::Make(ui64{2})});
            // The two identical IDs are on opposite sides of an index partition boundary.
            fixture.SplitShard(TString(RowIdMappingPath), fixture.Shards(TString(RowIdMappingPath)).front(), firstId, "m");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Shards(TString(RowIdMappingPath)).size(), 2);
        }
        const auto check = fixture.Execute(TString(Check));
        UNIT_ASSERT(!check.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(check.GetIssues().ToString(), "Stored document IDs must be unique");
        UNIT_ASSERT(check.GetResultSets().empty());
    }

    Y_UNIT_TEST_TWIN(MappedJsonWithBoundedChannels, ExtraMappings) {
        TFixture fixture(false, true);
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Org Utf8 NOT NULL, Payload JsonDocument,
                __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Org)
            );
        )");
        fixture.Exec("ALTER TABLE Docs ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);");
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Payload);");
        auto builder = TParamsBuilder();
        auto& rows = builder.AddParam("$rows").BeginList();
        for (ui64 id = 0; id < 512; ++id) {
            rows.AddListItem().BeginStruct().AddMember("Org")
                .Utf8(TStringBuilder() << id << TString(2048, 'x')).EndStruct();
        }
        rows.EndList().Build();
        fixture.Exec(R"(
            DECLARE $rows AS List<Struct<Org: Utf8>>;
            UPSERT INTO Docs SELECT * FROM AS_TABLE($rows);
        )", builder.Build());
        if (ExtraMappings) {
            constexpr TStringBuf missing = "missing";
            std::vector<TCell> cells;
            for (ui64 id = 0; id < 128; ++id) {
                cells.push_back(TCell::Make(Max<ui64>() - id));
                cells.emplace_back(missing.data(), missing.size());
            }
            fixture.Corrupt(TString(RowIdMappingPath), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                {"__ydb_row_id", "Org"}, cells);
        }
        // The mapping stream exceeds its bounded channel buffers with spilling disabled.
        // A sampler waiting for this same stream's final count would deadlock here.
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, ExtraMappings ? 128 : 0, 0);
    }

    Y_UNIT_TEST(DuplicateMappingRowIdWithUniqueDocuments) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        const auto rowId = FirstDocumentRowId(fixture);
        fixture.Corrupt(TString(RowIdMappingPath), NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"__ydb_row_id", "Org", "Id"}, {TCell::Make(rowId), TCell("z", 1), TCell::Make(ui64{99})});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 1, 0, "EXTRA", "", "ROW_ID_MAPPING");
    }

    Y_UNIT_TEST(DuplicateRowIdInDocumentWithoutPostings) {
        TFixture fixture;
        CreateMappedJsonIndex(fixture);
        fixture.Exec(R"(UPSERT INTO Docs (Org, Id, Tenant, Payload) VALUES ("b"u, 3, NULL, NULL);)");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        const auto rowId = FirstDocumentRowId(fixture);
        fixture.Corrupt("/Root/Docs", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Org", "Id", "__ydb_row_id"}, {TCell("b", 1), TCell::Make(ui64{3}), TCell::Make(rowId)},
            fixture.Shards("/Root/Docs").front());
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Stored document IDs must be unique");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST_TWIN(WriteOnlyIndexBecomesReady, JsonWithRowId) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Org Utf8 NOT NULL, Id Uint64 NOT NULL, Tag Uint64, Payload JsonDocument, Data Utf8,
                PRIMARY KEY (Org, Id)
            ) WITH (PARTITION_AT_KEYS = (("m"u)));
        )");
        fixture.Exec(R"(
            UPSERT INTO Docs (Org, Id, Tag, Payload, Data) VALUES
                ("a"u, 1, 10, JsonDocument('{"tag":"a"}'), "first"u),
                ("z"u, 2, 20, JsonDocument('{"tag":"b"}'), "second"u);
        )");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);

        NActors::TBlockEvents<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction> publish(fixture.Runtime, [](const auto& ev) {
            return AnyOf(ev->Get()->Record.GetTransaction(), [](const auto& operation) {
                return operation.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild
                    && operation.GetApplyIndexBuild().GetIndexName() == "idx";
            });
        });
        const auto indexState = [&] {
            const auto desc = fixture.Kikimr.GetTestClient().Describe(&fixture.Runtime, "/Root/Docs", Tests::SchemeRoot, true);
            UNIT_ASSERT_VALUES_EQUAL_C(desc.GetStatus(), NKikimrScheme::StatusSuccess, desc.ShortDebugString());
            const auto& indexes = desc.GetPathDescription().GetTable().GetTableIndexes();
            const auto index = FindIf(indexes, [](const auto& item) { return item.GetName() == "idx"; });
            UNIT_ASSERT_C(index != indexes.end(), desc.ShortDebugString());
            return index->GetState();
        };
        const auto build = fixture.Client.ExecuteQuery(JsonWithRowId
            ? "ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Payload);"
            : "ALTER TABLE Docs ADD INDEX idx GLOBAL ON (Tag) COVER (Data);", TTxControl::NoTx());
        fixture.Runtime.WaitFor("index build waiting for publication", [&] {
            return !publish.empty() || build.HasValue();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!publish.empty(), build.GetValueSync().GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(indexState(), NKikimrSchemeOp::EIndexStateWriteOnly);
        const auto blocked = fixture.Execute(TString(Check));
        UNIT_ASSERT(!blocked.IsSuccess());
        UNIT_ASSERT(HasIssue(blocked.GetIssues(), NYql::TIssuesIds::KIKIMR_INDEX_IS_NOT_READY));
        UNIT_ASSERT(blocked.GetResultSets().empty());
        publish.Stop().Unblock();

        const auto built = fixture.Runtime.WaitFuture(build);
        UNIT_ASSERT_C(built.IsSuccess(), built.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(indexState(), NKikimrSchemeOp::EIndexStateReady);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        if (JsonWithRowId) {
            const auto rows = fixture.Execute("SELECT COUNT(DISTINCT __ydb_row_id) AS DistinctIds FROM Docs;");
            UNIT_ASSERT_C(rows.IsSuccess(), rows.GetIssues().ToString());
            TResultSetParser parser(rows.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("DistinctIds").GetUint64(), 2);
        }
    }

    Y_UNIT_TEST_TWIN(BaseReadPermissionDoesNotExposeImplementationTables, JsonWithRowId) {
        TFixture fixture;
        fixture.Runtime.GetAppData().FeatureFlags.SetEnableAccessToIndexImplTables(false);
        if (JsonWithRowId) {
            CreateMappedJsonIndex(fixture);
        } else {
            fixture.CreateOrdinary(false);
        }

        constexpr auto Reader = "index_reader@builtin";
        fixture.Runtime.GetAppData().AdministrationAllowedSIDs.emplace_back("root@builtin");
        auto scheme = fixture.Kikimr.GetSchemeClient(TCommonClientSettings().AuthToken("root@builtin"));
        const auto grant = [&](const TString& path, std::vector<std::string> permissions) {
            auto settings = NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                NYdb::NScheme::TPermissions(Reader, permissions));
            const auto result = fixture.Runtime.WaitFuture(scheme.ModifyPermissions(path, settings));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            Tests::TClient::RefreshPathCache(&fixture.Runtime, path);
        };
        grant("/Root", {"ydb.database.connect"});
        auto reader = fixture.Kikimr.GetQueryClient(NQuery::TClientSettings().AuthToken(Reader));
        const auto execute = [&](const TString& query) {
            return fixture.Runtime.WaitFuture(reader.ExecuteQuery(query, TTxControl::NoTx()));
        };

        const auto baseShards = fixture.Shards("/Root/Docs");
        const auto postingShards = fixture.Shards("/Root/Docs/idx/indexImplTable");
        const auto mappingShards = JsonWithRowId ? fixture.Shards(TString(RowIdMappingPath)) : TVector<ui64>{};
        bool readData = false;
        auto watchReads = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            readData |= forward->Ev->Type() == TEvDataShard::TEvRead::EventType
                && (IsIn(baseShards, forward->TabletId) || IsIn(postingShards, forward->TabletId)
                    || IsIn(mappingShards, forward->TabletId));
        });
        const auto denied = execute(TString(Check));
        watchReads.Remove();
        UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::SCHEME_ERROR, denied.GetIssues().ToString());
        UNIT_ASSERT(denied.GetResultSets().empty());
        UNIT_ASSERT(!readData);
        grant("/Root/Docs", {"ydb.granular.describe_schema", "ydb.granular.select_row"});
        const auto base = execute("SELECT COUNT(*) FROM Docs;");
        UNIT_ASSERT_C(base.IsSuccess(), base.GetIssues().ToString());
        fixture.AssertResult(execute(TString(Check)), 0, 0, 0);

        const auto assertPrivate = [&](const TString& query) {
            const auto result = execute(query);
            UNIT_ASSERT_C(!result.IsSuccess(), query);
            UNIT_ASSERT(result.GetResultSets().empty());
        };
        assertPrivate("SELECT COUNT(*) FROM `/Root/Docs/idx/indexImplTable`;");
        if (JsonWithRowId) {
            assertPrivate("SELECT COUNT(*) FROM `/Root/Docs/uniq_rowid/indexImplTable`;");
        }
    }

    Y_UNIT_TEST(StrictNewRbo) {
        TFixture fixture(false, false, true);
        fixture.Exec(R"(
            CREATE TABLE Docs (Id Uint64, Tag Uint64, PRIMARY KEY (Id),
                INDEX idx GLOBAL ON (Tag));
        )");
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
    }

    Y_UNIT_TEST_TWIN(ExecuteScriptPublishesAfterSchemaCheck, DropIndex) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
            {"Tag", "Id"}, {TCell::Make(ui64{10}), TCell::Make(ui64{1})});

        size_t schemaChecks = 0;
        NActors::TBlockEvents<TEvTxProxySchemeCache::TEvNavigateKeySet> finalCheck(fixture.Runtime, [&](const auto& ev) {
            const auto& entries = ev->Get()->Request->ResultSet;
            if (entries.size() != 2 || !AllOf(entries, [](const auto& entry) {
                    return entry.SyncVersion && entry.ShowPrivatePath
                        && entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                }))
            {
                return false;
            }
            return ++schemaChecks == 2;
        });

        const auto operation = fixture.Runtime.WaitFuture(fixture.Client.ExecuteScript(TString(Check)));
        UNIT_ASSERT_C(operation.Status().IsSuccess(), operation.Status().GetIssues().ToString());
        NYdb::NOperation::TOperationClient client(fixture.Kikimr.GetDriver());
        const auto getOperation = [&] {
            return fixture.Runtime.WaitFuture(client.Get<TScriptExecutionOperation>(operation.Id()));
        };
        const auto fetch = [&] {
            return fixture.Runtime.WaitFuture(fixture.Client.FetchScriptResults(operation.Id(), 0));
        };

        // Poll the operation as well as the barrier to surface failures that occur before the final check.
        auto current = getOperation();
        for (size_t attempt = 0; finalCheck.empty() && !current.Ready() && attempt < 50; ++attempt) {
            UNIT_ASSERT_C(current.Status().IsSuccess(), current.Status().GetIssues().ToString());
            current = getOperation();
        }
        UNIT_ASSERT_C(!finalCheck.empty(), "Script did not reach the final schema check: " << current.Status().GetIssues().ToString());
        UNIT_ASSERT_C(!current.Ready(), current.Status().GetIssues().ToString());
        const auto pending = fetch();
        UNIT_ASSERT_VALUES_EQUAL_C(pending.GetStatus(), EStatus::BAD_REQUEST, pending.GetIssues().ToString());
        UNIT_ASSERT(!pending.HasResultSet());

        if (DropIndex) {
            fixture.Exec("ALTER TABLE Docs DROP INDEX idx;");
        }
        finalCheck.Stop().Unblock();

        current = getOperation();
        for (size_t attempt = 0; !current.Ready() && attempt < 50; ++attempt) {
            UNIT_ASSERT_C(current.Status().IsSuccess(), current.Status().GetIssues().ToString());
            current = getOperation();
        }
        UNIT_ASSERT_C(current.Ready(), current.Status().GetIssues().ToString());
        const auto result = fetch();
        if (DropIndex) {
            UNIT_ASSERT_VALUES_EQUAL_C(current.Status().GetStatus(), EStatus::PRECONDITION_FAILED, current.Status().GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(current.Status().GetIssues().ToString(), "Index schema changed during validation");
            UNIT_ASSERT(current.Metadata().ResultSetsMeta.empty());
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(!result.HasResultSet());
            return;
        }

        UNIT_ASSERT_C(current.Status().IsSuccess(), current.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(current.Metadata().ResultSetsMeta.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(current.Metadata().ResultSetsMeta.front().RowsCount, 1);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.HasResultSet());
        UNIT_ASSERT(result.GetNextFetchToken().empty());
        TResultSetParser parser(result.GetResultSet());
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Status").GetString(), "INCONSISTENT");
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Missing").GetUint64(), 1);
        UNIT_ASSERT(parser.ColumnParser("SnapshotStep").GetUint64() > 0);
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST(RejectMixedWrite) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto result = fixture.Execute(TStringBuilder()
            << R"(UPDATE Docs SET Tag = 20 WHERE Id = 1; )" << Check);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        const auto row = fixture.Execute("SELECT Tag FROM Docs WHERE Id = 1;");
        UNIT_ASSERT_C(row.IsSuccess(), row.GetIssues().ToString());
        TResultSetParser parser(row.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Tag").GetOptionalUint64().value(), 10);
    }

    Y_UNIT_TEST_TWIN(UpdateBetweenScans, LoseSnapshot) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto baseShards = fixture.Shards("/Root/Docs");
        const auto indexShards = fixture.Shards("/Root/Docs/idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(indexShards.size(), 1);

        TRowVersion snapshot = TRowVersion::Min();
        THashMap<TActorId, THashMap<ui64, ui64>> baseReads;
        THashSet<ui64> finishedShards;
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType || !IsIn(baseShards, forward->TabletId)) {
                return;
            }
            const auto& record = static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get())->Record;
            UNIT_ASSERT(record.HasSnapshot());
            const TRowVersion current(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
            if (snapshot.IsMin()) {
                snapshot = current;
            }
            UNIT_ASSERT_VALUES_EQUAL(current, snapshot);
            baseReads[ev->Sender].emplace(record.GetReadId(), forward->TabletId);
        });
        auto results = fixture.Runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetFinished()) {
                if (const auto* reads = baseReads.FindPtr(ev->GetRecipientRewrite())) {
                    if (const auto* shard = reads->FindPtr(record.GetReadId())) {
                        finishedShards.insert(*shard);
                    }
                }
            }
        });
        NActors::TBlockEvents<TEvPipeCache::TEvForward> blockedIndex(fixture.Runtime, [&](const auto& ev) {
            const auto* forward = ev->Get();
            return forward->TabletId == indexShards.front() && forward->Ev->Type() == TEvDataShard::TEvRead::EventType;
        });

        auto running = fixture.Client.ExecuteQuery(TString(Check), TTxControl::NoTx());
        fixture.Runtime.WaitFor("base scan complete and index scan blocked", [&] {
            return finishedShards.size() == baseShards.size() && !blockedIndex.empty();
        });
        requests.Remove();
        results.Remove();

        const auto* forward = blockedIndex.front()->Get();
        const auto& indexRead = static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get())->Record;
        UNIT_ASSERT(indexRead.HasSnapshot());
        UNIT_ASSERT_VALUES_EQUAL(indexRead.GetSnapshot().GetStep(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(indexRead.GetSnapshot().GetTxId(), snapshot.TxId);

        if (LoseSnapshot) {
            const auto reader = blockedIndex.front()->Sender;
            const auto readId = indexRead.GetReadId();
            bool snapshotLost = false;
            auto failIndexRead = fixture.Runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
                auto& record = ev->Get()->Record;
                if (ev->GetRecipientRewrite() == reader && record.GetReadId() == readId) {
                    record.MutableStatus()->SetCode(Ydb::StatusIds::PRECONDITION_FAILED);
                    record.MutableStatus()->AddIssues()->set_message("Requested snapshot is unavailable");
                    snapshotLost = true;
                }
            });
            blockedIndex.Stop().Unblock();

            const auto result = fixture.Runtime.WaitFuture(running);
            UNIT_ASSERT(snapshotLost);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Requested snapshot is unavailable");
            UNIT_ASSERT(result.GetResultSets().empty());
            return;
        }

        fixture.Exec(R"(UPDATE Docs SET Tag = 20, Data = "changed"u WHERE Id = 1;)");
        blockedIndex.Stop().Unblock();

        const auto result = fixture.Runtime.WaitFuture(running);
        fixture.AssertResult(result, 0, 0, 0);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotStep").GetUint64(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotTxId").GetUint64(), snapshot.TxId);
    }

    Y_UNIT_TEST_TWIN(IndexDroppedBeforeReport, Streaming) {
        TFixture fixture;
        fixture.CreateOrdinary(false);

        size_t schemaChecks = 0;
        NActors::TBlockEvents<TEvTxProxySchemeCache::TEvNavigateKeySet> finalCheck(fixture.Runtime, [&](const auto& ev) {
            const auto& entries = ev->Get()->Request->ResultSet;
            if (entries.size() != 2 || !AllOf(entries, [](const auto& entry) {
                    return entry.SyncVersion && entry.ShowPrivatePath
                        && entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                }))
            {
                return false;
            }
            return ++schemaChecks == 2;
        });

        if (Streaming) {
            auto stream = fixture.Runtime.WaitFuture(fixture.Client.StreamExecuteQuery(TString(Check), TTxControl::NoTx()));
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            fixture.Runtime.WaitFor("final schema check", [&] { return !finalCheck.empty(); });
            fixture.Exec("ALTER TABLE Docs DROP INDEX idx;");
            finalCheck.Stop().Unblock();

            const auto part = fixture.Runtime.WaitFuture(stream.ReadNext());
            UNIT_ASSERT_VALUES_EQUAL_C(part.GetStatus(), EStatus::PRECONDITION_FAILED, part.GetIssues().ToString());
            UNIT_ASSERT(!part.HasResultSet());
            return;
        }
        auto running = fixture.Client.ExecuteQuery(TString(Check), TTxControl::NoTx());
        fixture.Runtime.WaitFor("final schema check", [&] { return !finalCheck.empty(); });
        fixture.Exec("ALTER TABLE Docs DROP INDEX idx;");
        finalCheck.Stop().Unblock();

        const auto result = fixture.Runtime.WaitFuture(running);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index schema changed during validation");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(RejectMultipleResultsBeforeScanning) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto shards = fixture.Shards("/Root/Docs");
        bool scanned = false;
        auto reads = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            scanned |= forward->Ev->Type() == TEvDataShard::TEvRead::EventType && IsIn(shards, forward->TabletId);
        });

        const auto result = fixture.Execute(TStringBuilder() << Check << " SELECT 1;");
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSets().empty());
        UNIT_ASSERT(!scanned);
    }

    Y_UNIT_TEST(RejectWrongTransactionMode) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto result = fixture.Runtime.WaitFuture(fixture.Client.ExecuteQuery(TString(Check),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "requires a SnapshotRO transaction");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(ReuseSnapshotReadOnlyTransaction) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        const auto sessionResult = fixture.Runtime.WaitFuture(fixture.Client.GetSession());
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto first = fixture.Runtime.WaitFuture(session.ExecuteQuery(TString(Check),
            TTxControl::BeginTx(TTxSettings::SnapshotRO())));
        fixture.AssertResult(first, 0, 0, 0);
        const auto tx = first.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        fixture.Exec(R"(UPDATE Docs SET Tag = 20, Data = "changed"u WHERE Id = 1;)");
        const auto second = fixture.Runtime.WaitFuture(session.ExecuteQuery(TString(Check), TTxControl::Tx(*tx).CommitTx()));
        fixture.AssertResult(second, 0, 0, 0);

        TResultSetParser firstReport(first.GetResultSet(0));
        TResultSetParser secondReport(second.GetResultSet(0));
        UNIT_ASSERT(firstReport.TryNextRow());
        UNIT_ASSERT(secondReport.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(firstReport.ColumnParser("SnapshotStep").GetUint64(), secondReport.ColumnParser("SnapshotStep").GetUint64());
        UNIT_ASSERT_VALUES_EQUAL(firstReport.ColumnParser("SnapshotTxId").GetUint64(), secondReport.ColumnParser("SnapshotTxId").GetUint64());
    }

    Y_UNIT_TEST_TWIN(RejectSnapshotBeforeIndexGeneration, Rebuild) {
        TFixture fixture;
        fixture.Exec("CREATE TABLE Pin (Id Uint64 NOT NULL, PRIMARY KEY (Id));");
        fixture.Exec("UPSERT INTO Pin (Id) VALUES (1);");
        fixture.Exec("CREATE TABLE Docs (Id Uint64 NOT NULL, Tag Utf8, PRIMARY KEY (Id));");
        fixture.Exec(R"(UPSERT INTO Docs (Id, Tag) VALUES (1, "A"u);)");
        if (Rebuild) {
            fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL ON (Tag);");
        }
        const auto sessionResult = fixture.Runtime.WaitFuture(fixture.Client.GetSession());
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto first = fixture.Runtime.WaitFuture(session.ExecuteQuery(
            Rebuild ? TString(Check) : "SELECT * FROM Pin;", TTxControl::BeginTx(TTxSettings::SnapshotRO())));
        UNIT_ASSERT_C(first.IsSuccess(), first.GetIssues().ToString());
        const auto tx = first.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        fixture.Exec(R"(UPDATE Docs SET Tag = "B"u WHERE Id = 1;)");
        if (Rebuild) {
            fixture.Exec("ALTER TABLE Docs DROP INDEX idx;");
        }
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL ON (Tag);");
        // Distinct query text prevents a cached plan for the original index
        // generation from hiding the transaction-snapshot check below.
        fixture.AssertResult(fixture.Execute(TStringBuilder() << "-- current index generation\n" << Check), 0, 0, 0);

        auto shards = fixture.Shards("/Root/Docs");
        const auto indexShards = fixture.Shards("/Root/Docs/idx/indexImplTable");
        shards.insert(shards.end(), indexShards.begin(), indexShards.end());
        size_t reads = 0;
        auto requests = fixture.Runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            reads += forward->Ev->Type() == TEvDataShard::TEvRead::EventType && IsIn(shards, forward->TabletId);
        });
        const auto result = fixture.Runtime.WaitFuture(session.ExecuteQuery(
            TStringBuilder() << "-- original transaction snapshot\n" << Check, TTxControl::Tx(*tx).CommitTx()));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "cannot verify this index generation at the transaction snapshot");
        UNIT_ASSERT(result.GetResultSets().empty());
        UNIT_ASSERT_VALUES_EQUAL(reads, 0);
    }

    Y_UNIT_TEST(IndexPathIdChangedAtSameSchemaVersion) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);
        size_t schemaChecks = 0;
        bool changed = false;
        auto results = fixture.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            auto& entries = ev->Get()->Request->ResultSet;
            if (entries.size() != 2 || !AllOf(entries, [](const auto& entry) {
                    return entry.SyncVersion && entry.ShowPrivatePath
                        && entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                }) || ++schemaChecks != 2)
            {
                return;
            }
            for (auto& entry : entries) {
                for (auto& index : entry.Indexes) {
                    if (index.GetName() == "idx") {
                        // A newly published index can have the same schema version.
                        index.SetLocalPathId(index.GetLocalPathId() + 1);
                        changed = true;
                    }
                }
            }
        });

        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT_C(changed, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index schema changed during validation");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST_TWIN(IndexSchemaLookupFailureIsRetryable, Redirect) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        size_t schemaChecks = 0;
        bool injected = false;
        auto results = fixture.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            auto& entries = ev->Get()->Request->ResultSet;
            if (entries.size() == 2 && AllOf(entries, [](const auto& entry) {
                    return entry.SyncVersion && entry.ShowPrivatePath
                        && entry.RequestType == NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
                }) && ++schemaChecks == 2)
            {
                using TSchemeStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
                entries.front().Status = Redirect ? TSchemeStatus::RedirectLookupError : TSchemeStatus::LookupError;
                injected = true;
            }
        });

        auto client = fixture.Kikimr.GetQueryClient(NQuery::TClientSettings().RetrySettings(TRetryOperationSettings().MaxRetries(0)));
        const auto result = fixture.Runtime.WaitFuture(client.ExecuteQuery(TString(Check), TTxControl::NoTx()));
        UNIT_ASSERT(injected);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAVAILABLE, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST_TWIN(RejectLegacyScripting, Streaming) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        NScripting::TScriptingClient scripting(fixture.Kikimr.GetDriver());
        constexpr TStringBuf diagnostic = "CHECK TABLE INDEX is not supported by ExecuteYqlScript";
        if (Streaming) {
            auto stream = fixture.Runtime.WaitFuture(scripting.StreamExecuteYqlScript(TString(Check)));
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            const auto part = fixture.Runtime.WaitFuture(stream.ReadNext());
            UNIT_ASSERT_VALUES_EQUAL_C(part.GetStatus(), EStatus::UNSUPPORTED, part.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(part.GetIssues().ToString(), diagnostic);
            UNIT_ASSERT(!part.HasPartialResult());
            return;
        }
        const auto result = fixture.Runtime.WaitFuture(scripting.ExecuteYqlScript(TString(Check)));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), diagnostic);
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(RejectSnapshotParameterCollision) {
        TFixture fixture;
        fixture.CreateOrdinary(false);
        auto params = TParamsBuilder()
            .AddParam("%kqp%kqp_index_validation_snapshot_step").Uint64(1).Build()
            .Build();
        const auto result = fixture.Runtime.WaitFuture(fixture.Client.ExecuteQuery(TString(Check),
            TTxControl::NoTx(), std::move(params)));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "reserved for CHECK TABLE INDEX");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(GlobalAsyncUnsupported) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tag Uint64, PRIMARY KEY (Id),
                INDEX idx GLOBAL ASYNC ON (Tag)
            );
        )");
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index validation is not implemented for GlobalAsync");
        UNIT_ASSERT(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(CompactJsonUnsupported) {
        TFixture fixture(true);
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Payload Json, PRIMARY KEY (Id),
                INDEX idx GLOBAL USING json ON (Payload)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        const auto result = fixture.Execute(TString(Check));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index validation is not implemented for GlobalJsonCompact");
    }
}
} // namespace NKikimr::NKqp
