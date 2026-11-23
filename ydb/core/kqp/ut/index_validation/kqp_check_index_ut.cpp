#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/json_index/json_index.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

constexpr TStringBuf Check = "CHECK TABLE `/Root/Docs` INDEX `idx`;";

class TFixture {
public:
    explicit TFixture(bool compact = false)
        : Kikimr(Settings(compact))
        , Client(Kikimr.GetQueryClient())
        , Runtime(*Kikimr.GetTestServer().GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {}

    TExecuteQueryResult Execute(const TString& query) {
        return Kikimr.RunCall([&] {
            return Client.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        });
    }

    void Exec(const TString& query) {
        const auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TVector<ui64> Shards(const TString& path) {
        return GetTableShards(&Kikimr.GetTestServer(), Sender, path);
    }

    void Corrupt(const TString& path, NKikimrDataEvents::TEvWrite::TOperation::EOperationType operation,
        std::initializer_list<TStringBuf> columns, const std::vector<TCell>& cells)
    {
        const auto shards = Shards(path);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
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
        Write(Runtime, Sender, shards.front(), std::move(request));
    }

    void AssertResult(const TExecuteQueryResult& result, ui64 missing, ui64 extra, ui64 mismatch,
        TStringBuf sampleKind = {}, TStringBuf sampleReason = {}, TStringBuf sampleRole = {})
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
        UNIT_ASSERT_VALUES_EQUAL(sampleSize, Min<ui64>(100, missing + extra + mismatch));
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

    TKikimrRunner Kikimr;
    TQueryClient Client;
    TTestActorRuntime& Runtime;
    TActorId Sender;

private:
    static TKikimrSettings Settings(bool compact) {
        NKikimrConfig::TFeatureFlags flags;
        flags.SetEnableJsonIndex(true);
        flags.SetEnableAddUniqueIndex(true);
        flags.SetEnableFulltextIndexPrefix(true);
        flags.SetEnableFulltextIndexRowId(true);
        flags.SetEnableCompactFulltextIndex(compact);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false).SetFeatureFlags(flags);
        settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        if (compact) {
            settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        }
        return settings;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpCheckIndex) {
    Y_UNIT_TEST_TWIN(CoverMismatch, Unique) {
        TFixture fixture;
        fixture.CreateOrdinary(Unique);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        const ui64 tag = 10;
        const ui64 id = 1;
        fixture.Corrupt("/Root/Docs/idx/indexImplTable", NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {"Tag", "Id", "Data"}, {TCell::Make(tag), TCell::Make(id), TCell()});
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 1, "VALUE_MISMATCH");
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
        fixture.AssertResult(fixture.Execute(TString(Check)), 1, 1, 0, "EXTRA");
    }

    Y_UNIT_TEST(PrefixedJsonBackfill) {
        TFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Docs (
                Id Uint64 NOT NULL, Tenant Uint64, Payload Json, PRIMARY KEY (Id)
            ) WITH (PARTITION_AT_KEYS = (100));
        )");
        fixture.Exec(R"(
            UPSERT INTO Docs (Id, Tenant, Payload) VALUES
                (1, 7, Json('{"tag":"a"}')), (101, 8, Json('{"tag":"b"}'));
        )");
        fixture.Exec("ALTER TABLE Docs ADD INDEX idx GLOBAL USING json ON (Tenant, Payload);");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 2, 0, "EXTRA", "EMPTY_TOKEN_POSTING");
    }

    Y_UNIT_TEST(BrokenRowIdMapping) {
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
                ("a"u, 1, 7, JsonDocument('{"tag":"a"}')),
                ("z"u, 2, 8, JsonDocument('{"tag":"b"}'));
        )");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shards("/Root/Docs").size(), 2);
        fixture.AssertResult(fixture.Execute(TString(Check)), 0, 0, 0);

        const auto result = fixture.Execute(R"(SELECT __ydb_row_id FROM Docs WHERE Org = "a"u AND Id = 1;)");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const ui64 rowId = parser.ColumnParser("__ydb_row_id").GetUint64();
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

    Y_UNIT_TEST(UpdateBetweenScans) {
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

        fixture.Exec(R"(UPDATE Docs SET Tag = 20, Data = "changed"u WHERE Id = 1;)");
        blockedIndex.Stop().Unblock();

        const auto result = fixture.Runtime.WaitFuture(running);
        fixture.AssertResult(result, 0, 0, 0);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotStep").GetUint64(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("SnapshotTxId").GetUint64(), snapshot.TxId);
    }

    Y_UNIT_TEST(IndexDroppedBeforeReport) {
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

        auto running = fixture.Client.ExecuteQuery(TString(Check), TTxControl::NoTx());
        fixture.Runtime.WaitFor("final schema check", [&] { return !finalCheck.empty(); });
        fixture.Exec("ALTER TABLE Docs DROP INDEX idx;");
        finalCheck.Stop().Unblock();

        const auto result = fixture.Runtime.WaitFuture(running);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Index schema changed during validation");
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
