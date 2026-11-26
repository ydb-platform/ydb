#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_backup.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <util/generic/scope.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <optional>
#include <utility>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

struct TFixtureOptions {
    bool FeatureFlagEnabled = true;
    bool GeneratedStored = false;
};

NKikimrConfig::TAppConfig DefaultExprAppConfig(const TFixtureOptions& options = {}) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableDefaultFromExpression(options.FeatureFlagEnabled);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(options.GeneratedStored);
    return appConfig;
}

// A volatile expression stays on the write path; multiplying its result by zero
// keeps the value predictable so the tests can assert exact rows
constexpr const char* DefaultExpr = "RandomNumber(1) * 0ul + 42ul";

// Distinct ranges make a draw from an earlier execution detectable without probabilities.
class TScopedEpochRandomProvider {
    class TProvider final : public IRandomProvider {
    public:
        explicit TProvider(TIntrusivePtr<IRandomProvider> original)
            : Original(std::move(original))
        {}

        TGUID GenGuid() noexcept override { return Original->GenGuid(); }
        TGUID GenUuid4() noexcept override { return Original->GenUuid4(); }
        ui64 GenRand() noexcept override {
            return (Epoch.load() << 60) | (Counter.fetch_add(1) & ((ui64(1) << 60) - 1));
        }

        std::atomic<ui64> Epoch{0};

    private:
        TIntrusivePtr<IRandomProvider> Original;
        std::atomic<ui64> Counter{0};
    };

public:
    TScopedEpochRandomProvider()
        : Original(TAppData::RandomProvider)
        , Provider(MakeIntrusive<TProvider>(Original))
    {
        TAppData::RandomProvider = Provider;
    }

    ~TScopedEpochRandomProvider() {
        TAppData::RandomProvider = Original;
    }

    void SetEpoch(ui64 epoch) { Provider->Epoch.store(epoch); }

private:
    TIntrusivePtr<IRandomProvider> Original;
    TIntrusivePtr<TProvider> Provider;
};

class TTestFixture {
    template <typename TCall>
    auto RunCall(TCall&& call) {
        if (Kikimr.GetTestServer().GetRuntime()->IsRealThreads()) {
            return call();
        }
        return Kikimr.RunCall(std::forward<TCall>(call));
    }

    TExecuteQueryResult Execute(const std::string& query) {
        return RunCall([&] { return Session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync(); });
    }

public:
    explicit TTestFixture(const TFixtureOptions& options = {})
        : TTestFixture(TKikimrSettings(DefaultExprAppConfig(options)).SetWithSampleTables(false))
    {
    }

    explicit TTestFixture(const TKikimrSettings& settings)
        : Kikimr(settings)
        , Db(Kikimr.GetQueryClient())
        , Session(RunCall([&] { return Db.GetSession().ExtractValueSync().GetSession(); }))
    {
    }

    void Exec(const std::string& query) {
        auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
    }

    TString Rejects(const std::string& query, const TString& expectedError) {
        auto result = Execute(query);
        UNIT_ASSERT_C(!result.IsSuccess(), "expected the query to be rejected: " << query);
        const TString issues = result.GetIssues().ToString();
        UNIT_ASSERT_STRING_CONTAINS_C(issues, expectedError, "query: " << query);
        return issues;
    }

    void RejectsWithStatus(const std::string& query, EStatus expectedStatus) {
        auto result = Execute(query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, query << "\n" << result.GetIssues().ToString());
    }

    void Check(const std::string& query, const TString& expected) {
        CompareYson(expected, QueryYson(query));
    }

    void CheckUnordered(const std::string& query, const TString& expected) {
        CompareYsonUnordered(expected, QueryYson(query), TStringBuilder() << "unexpected rows for: " << query);
    }

    TString QueryYson(const std::string& query) {
        auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    void CheckEventually(const std::string& query, const TString& expected) {
        TString actual;
        TString issues;
        const TString normalizedExpected = ReformatYson(expected);
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            auto result = Session.ExecuteQuery(
                query, TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).GetValueSync();
            issues = result.GetIssues().ToString();
            if (result.IsSuccess()) {
                actual = FormatResultSetYson(result.GetResultSet(0));
                if (ReformatYson(actual) == normalizedExpected) {
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("eventual query result did not converge: " << query
            << "\nexpected: " << expected << "\nactual: " << actual << "\nissues: " << issues);
    }

    ui64 QueryUint64(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT_C(parser.TryNextRow(), "query returned no rows: " << query);
        return parser.ColumnParser(0).GetUint64();
    }

    TString ExplainAst(const std::string& query) {
        auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto ast = result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.has_value(), "no AST for: " << query);
        return TString(*ast);
    }

    void CheckTargetReadCount(const std::string& query, size_t expected) {
        const TString ast = ExplainAst(query);
        size_t actual = 0;
        for (const TStringBuf callable : {"(KqpCnStreamLookup ", "(KqpCnLookupTable ", "(KqpLookupTable ",
                "(KqpReadTable ", "(KqpReadTableRanges ", "(KqpRowsSourceSettings "}) {
            for (size_t pos = ast.find(callable); pos != TString::npos; pos = ast.find(callable, pos + callable.size())) {
                ++actual;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, "target reads for: " << query << "\nAST:\n" << ast);
    }

    // The column description as SchemeShard persisted it
    NKikimrSchemeOp::TColumnDescription ColumnDesc(const std::string& path, const std::string& column) {
        auto describe = RunCall([&] { return Kikimr.GetTestClient().Ls(TString(path)); });
        const auto& table = describe->Record.GetPathDescription().GetTable();
        for (const auto& col : table.GetColumns()) {
            if (col.GetName() == column) {
                return col;
            }
        }
        UNIT_FAIL("column '" << column << "' not found in " << path);
        return {};
    }

    std::string ShowCreateTable(const std::string& path) {
        const std::string query = "SHOW CREATE TABLE `" + path + "`;";
        auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto ddl = parser.ColumnParser("CreateQuery").GetOptionalUtf8();
        UNIT_ASSERT_C(ddl.has_value(), "SHOW CREATE TABLE returned an empty CreateQuery");
        return *ddl;
    }

    void RestartSchemeShard(const std::string& tablePath) {
        RestartSchemeShardAndSyncCache(*Kikimr.GetTestServer().GetRuntime(), TString(tablePath));
    }

    void DisableExpressionDefaults(const std::string& tablePath) {
        auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableDefaultFromExpression(false);
        }
        RestartSchemeShard(tablePath);
        Session = Db.GetSession().GetValueSync().GetSession();
    }

    TVector<NJson::TJsonValue> ReadChangefeedMessages(size_t expectedCount) {
        return RunCall([&] {
            NTopic::TTopicClient topicClient(Kikimr.GetDriver());
            NTopic::TReadSessionSettings settings;
            settings.ConsumerName("test_consumer");
            settings.AppendTopics(NTopic::TTopicReadSettings().Path("/Root/TestTable/feed"));
            auto reader = topicClient.CreateReadSession(settings);

            TVector<NJson::TJsonValue> messages;
            const auto deadline = TInstant::Now() + TDuration::Seconds(20);
            while (messages.size() < expectedCount) {
                const auto now = TInstant::Now();
                if (now >= deadline || !reader->WaitEvent().Wait(deadline - now)) {
                    break;
                }
                for (auto& event : reader->GetEvents(false)) {
                    if (auto* start = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                        start->Confirm();
                    } else if (auto* data = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        for (const auto& message : data->GetMessages()) {
                            auto& json = messages.emplace_back();
                            UNIT_ASSERT_C(NJson::ReadJsonTree(message.GetData(), &json), message.GetData());
                        }
                        data->Commit();
                    } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                        UNIT_FAIL(closed->GetIssues().ToString());
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(messages.size(), expectedCount, "changefeed message count");
            return messages;
        });
    }

    TKikimrRunner& Runner() {
        return Kikimr;
    }

    NYdb::NQuery::TSession& QuerySession() {
        return Session;
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};

std::string DefaultExprDDL(const std::string& columnDef, const std::string& prefix = "") {
    return prefix + R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            other Int32,
            )" + columnDef + R"(,
            PRIMARY KEY (k)
        );
    )";
}

// The table the value tests use: `v` is filled by DefaultExpr when it is absent from the write
std::string ValueTableDDL() {
    return DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr);
}

void AssertFromExpression(const NKikimrSchemeOp::TColumnDescription& col, const TString& exprText) {
    UNIT_ASSERT_C(col.HasDefaultFromExpression(), "expected an expression default: " << col.ShortDebugString());
    const auto& defaultExpression = col.GetDefaultFromExpression();
    UNIT_ASSERT(defaultExpression.GetStored());
    UNIT_ASSERT(defaultExpression.GetWritable());
    UNIT_ASSERT_VALUES_EQUAL(defaultExpression.GetExprText(), exprText);
    UNIT_ASSERT_VALUES_EQUAL_C(defaultExpression.DependencyColumnNamesSize(), 0u,
        "a DEFAULT expression must not have dependencies: " << defaultExpression.ShortDebugString());
}

enum class ESecondaryIndexKind {
    None,
    Sync,
    Async,
};

std::string DmlMatrixDDL(ESecondaryIndexKind indexKind) {
    TStringBuilder ddl;
    ddl << R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            other Int32,
            v Uint64 DEFAULT )" << DefaultExpr << ",\n"
        << "            PRIMARY KEY (k)";
    switch (indexKind) {
        case ESecondaryIndexKind::None:
            break;
        case ESecondaryIndexKind::Sync:
            ddl << ", INDEX idx_v GLOBAL SYNC ON (v) COVER (other)";
            break;
        case ESecondaryIndexKind::Async:
            ddl << ", INDEX idx_v GLOBAL ASYNC ON (v) COVER (other)";
            break;
    }
    ddl << R"(
        );
    )";
    return ddl;
}

void ExecuteDml(TTestFixture& fixture, const TString& query, const TString& expectedReturning, bool withReturning) {
    if (withReturning) {
        CompareYson(expectedReturning, fixture.QueryYson(query + " RETURNING k, other, v;"));
    } else {
        fixture.Exec(query + ";");
    }
}

void RunDmlMatrix(ESecondaryIndexKind indexKind, bool withReturning) {
    TTestFixture fixture;
    fixture.Exec(DmlMatrixDDL(indexKind));

    ExecuteDml(fixture, "INSERT INTO TestTable (k, other) VALUES (1, 10)", "[[1;[10];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 1;", "[[1;[10];[42u]]]");

    ExecuteDml(fixture, "UPSERT INTO TestTable (k, other) VALUES (2, 20)", "[[2;[20];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 2;", "[[2;[20];[42u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (3, 30, 7ul);");
    ExecuteDml(fixture, "UPSERT INTO TestTable (k, other) VALUES (3, 31)", "[[3;[31];[7u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 3;", "[[3;[31];[7u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (4, 40, 8ul);");
    ExecuteDml(fixture, "REPLACE INTO TestTable (k, other) VALUES (4, 41)", "[[4;[41];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 4;", "[[4;[41];[42u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (5, 50, 9ul);");
    ExecuteDml(fixture, "UPDATE TestTable SET other = 51 WHERE k = 5", "[[5;[51];[9u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 5;", "[[5;[51];[9u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (6, 60, 10ul);");
    ExecuteDml(fixture, "UPDATE TestTable ON (k, other) VALUES (6, 61)", "[[6;[61];[10u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 6;", "[[6;[61];[10u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (7, 70, 11ul);");
    ExecuteDml(fixture, "DELETE FROM TestTable WHERE k = 7", "[[7;[70];[11u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 7;", "[]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (8, 80, 12ul);");
    ExecuteDml(fixture, "DELETE FROM TestTable ON (k) VALUES (8)", "[[8;[80];[12u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 8;", "[]");

    const TString expected = "[[1;[10];[42u]];[2;[20];[42u]];[3;[31];[7u]];[4;[41];[42u]];[5;[51];[9u]];[6;[61];[10u]]]";
    fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", expected);

    if (indexKind != ESecondaryIndexKind::None) {
        const TString expectedIndex = "[[3;[31];[7u]];[5;[51];[9u]];[6;[61];[10u]];[1;[10];[42u]];[2;[20];[42u]];[4;[41];[42u]]]";
        const std::string query = "SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY v, k;";
        if (indexKind == ESecondaryIndexKind::Async) {
            fixture.CheckEventually(query, expectedIndex);
        } else {
            fixture.Check(query, expectedIndex);
        }
    }
}

class TDefaultChangeRaceSessions {
public:
    explicit TDefaultChangeRaceSessions(TKikimrRunner& kikimr)
        : Kikimr(kikimr)
        , DdlDriver(kikimr.GetDriverConfig().SetDiscoveryMode(EDiscoveryMode::Off))
        , WriterDriver(GetWriterConfig())
    {
    }

    ~TDefaultChangeRaceSessions() {
        Kikimr.RunCall([&] {
            WriterDriver.Stop(true);
            DdlDriver.Stop(true);
            return true;
        });
    }

    auto Connect() {
        NQuery::TQueryClient ddlClient(DdlDriver);
        NYdb::NTable::TTableClient writerClient(WriterDriver);
        auto ddl = Kikimr.RunCall([&] { return ddlClient.GetSession().ExtractValueSync(); });
        auto writer = Kikimr.RunCall([&] { return writerClient.CreateSession().ExtractValueSync(); });
        UNIT_ASSERT_C(ddl.IsSuccess(), ddl.GetIssues().ToString());
        UNIT_ASSERT_C(writer.IsSuccess(), writer.GetIssues().ToString());
        const auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        UNIT_ASSERT_STRING_CONTAINS(ddl.GetSession().GetId(), TStringBuilder() << "node_id=" << runtime.GetNodeId(0) << "&");
        UNIT_ASSERT_STRING_CONTAINS(writer.GetSession().GetId(), TStringBuilder() << "node_id=" << runtime.GetNodeId(1) << "&");
        return std::make_pair(ddl.GetSession(), writer.GetSession());
    }

private:
    TDriverConfig GetWriterConfig() {
        const auto port = Ports.GetPort();
        Kikimr.GetTestServer().EnableGRpc(port, 1);
        return Kikimr.GetDriverConfig().SetDiscoveryMode(EDiscoveryMode::Off)
            .SetEndpoint(TStringBuilder() << "localhost:" << port);
    }

    TKikimrRunner& Kikimr;
    TPortManager Ports;
    TDriver DdlDriver;
    TDriver WriterDriver;
};

TKikimrSettings DefaultExprBackupSettings() {
    auto config = DefaultExprAppConfig();
    config.MutableFeatureFlags()->SetEnableBackupService(true);
    return TKikimrSettings(config).SetWithSampleTables(false).SetEnableBackupService(true).SetUseRealThreads(false);
}

using TBackupOperationId = NYdb::TOperation::TOperationId;

template <typename TOperationResult>
void RunBackupCollectionOperation(TTestFixture& fixture, const std::string& query, TBackupOperationId::EKind kind) {
    auto& runtime = *fixture.Runner().GetTestServer().GetRuntime();
    auto result = runtime.WaitFuture(fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx()));
    UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT_C(parser.TryNextRow(), query);
    const std::string id(parser.ColumnParser("operation_id").GetUtf8());
    UNIT_ASSERT_C(!id.empty(), query);

    TBackupOperationId operationId;
    operationId.SetKind(kind);
    operationId.AddOptionalValue("id", id);
    NYdb::NOperation::TOperationClient client(fixture.Runner().GetDriver());
    const auto settings = NYdb::NOperation::TGetOperationSettings().ClientTimeout(TDuration::Seconds(5));
    const auto deadline = TMonotonic::Now() + TDuration::Seconds(30);
    const auto getOperation = [&] { return runtime.WaitFuture(client.Get<TOperationResult>(operationId, settings)); };
    auto operation = getOperation();
    while (operation.Status().IsSuccess() && !operation.Ready() && TMonotonic::Now() < deadline) {
        operation = getOperation();
    }
    UNIT_ASSERT_C(operation.Status().IsSuccess(),
        query << " (" << id << ")\n" << operation.Status().GetStatus() << ": " << operation.Status().GetIssues().ToString());
    UNIT_ASSERT_C(operation.Ready(), query << " did not complete: " << operation.ToString());
}

}   // namespace

Y_UNIT_TEST_SUITE(DefaultExpr) {
    Y_UNIT_TEST(ExpressionIsStoredAndApplied) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(DefaultDialectIsStableAcrossApis, CreateThroughTableApi) {
        TTestFixture fixture;
        const TString expression = "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)";
        const auto ddl = DefaultExprDDL(TStringBuilder() << "v Bool DEFAULT " << expression);
        auto tableClient = fixture.Runner().GetTableClient();
        if (CreateThroughTableApi) {
            auto schemeSession = tableClient.CreateSession().ExtractValueSync().GetSession();
            auto result = schemeSession.ExecuteSchemeQuery(ddl).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            fixture.Exec(ddl);
        }
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);

        // The unmatched value is NULL under the canonical Query dialect. The legacy
        // Table dialect would produce false, even though both fit the nullable column.
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        auto dataSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto dataResult = dataSession.ExecuteDataQuery(
            "INSERT INTO `/Root/TestTable` (k) VALUES (2);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(dataResult.IsSuccess(), dataResult.GetIssues().ToString());

        auto preparedSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = preparedSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k) VALUES ($k);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(3).Build().Build();
        auto preparedResult = prepared.Execute(
            NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(preparedResult.IsSuccess(), preparedResult.GetIssues().ToString());

        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;#];[3;#]]");
    }

    Y_UNIT_TEST(CreateCommitAndWriteNullableDefaultsInScript) {
        TTestFixture fixture;
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                n Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                v Bool DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL),
                PRIMARY KEY (k)
            );
            COMMIT;
            INSERT INTO TestTable (k) VALUES (1);
            COMMIT;
            SELECT k, n, v FROM TestTable ORDER BY k;
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
        CompareYson("[[1;[42u];#]]", FormatResultSetYson(result.GetResultSet(0)));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "n"), DefaultExpr);
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)");

        // Inline CREATE metadata has index descriptions without loaded implementation tables.
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_n GLOBAL SYNC ON (n) COVER (v);");
        fixture.Check("SELECT k, n, v FROM TestTable VIEW idx_n ORDER BY k;", "[[1;[42u];#]]");

        // A separate compilation reloads the persisted metadata, rather than the CREATE cache.
        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, n, v;", "[[2;[42u];#]]");
        fixture.Check("SELECT k, n, v FROM TestTable ORDER BY k;", "[[1;[42u];#];[2;[42u];#]]");
        fixture.Check("SELECT k, n, v FROM TestTable VIEW idx_n ORDER BY k;", "[[1;[42u];#];[2;[42u];#]]");
    }

    Y_UNIT_TEST(NonDeterministicFunction) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("ts Timestamp DEFAULT CurrentUtcTimestamp()"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "ts"), "CurrentUtcTimestamp()");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, ts IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(RandomFunction) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("r Uint64 DEFAULT RandomNumber(1)"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "r"), "RandomNumber(1)");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, r IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(PreparedWritesUseExecutionTimeDefaults) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                ts Timestamp NOT NULL DEFAULT CurrentUtcTimestamp(),
                r Uint64 NOT NULL DEFAULT RandomNumber(1),
                observed_ts Timestamp NOT NULL,
                PRIMARY KEY (k)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "ts"), "CurrentUtcTimestamp()");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "r"), "RandomNumber(1)");

        auto tableClient = fixture.Runner().GetTableClient();
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = tableSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k, observed_ts)
            VALUES ($k, CurrentUtcTimestamp())
            RETURNING k, ts, r, observed_ts;
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        NYdb::NTable::TExecDataQuerySettings settings;
        settings.KeepInQueryCache(true).CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        // Neither the schema nor the prepared handle changes between executions.
        for (i32 key : {1, 2}) {
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            const auto before = TInstant::Now();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
            const auto after = TInstant::Now();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats());
            if (key == 2) {
                UNIT_ASSERT(NYdb::TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache());
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetInt32(), key);
            const auto timestamp = parser.ColumnParser("ts").GetTimestamp();
            UNIT_ASSERT_C(before <= timestamp && timestamp <= after,
                "DEFAULT timestamp outside its execution window: " << before << " <= " << timestamp << " <= " << after);
            // Zero-argument timestamps use the same execution binding in both expressions.
            UNIT_ASSERT_VALUES_EQUAL(timestamp, parser.ColumnParser("observed_ts").GetTimestamp());
            // Require a non-null random value; compare its actual value with storage below.
            parser.ColumnParser("r").GetUint64();
            UNIT_ASSERT(!parser.TryNextRow());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), fixture.QueryYson(
                TStringBuilder() << "SELECT k, ts, r, observed_ts FROM TestTable WHERE k = " << key << ";"));
        }
        fixture.Check("SELECT k FROM TestTable ORDER BY k;", "[[1];[2]]");
    }

    Y_UNIT_TEST(NotNullWithNonNullableExpr) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("ts Timestamp NOT NULL DEFAULT CurrentUtcTimestamp()"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, ts IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(NotNullDefaultOmitted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 NOT NULL DEFAULT " << DefaultExpr));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;42u]]");
    }

    // A generated column is computed from the row and so cannot be a key; a DEFAULT column has no
    // such dependency
    Y_UNIT_TEST(OnPrimaryKey) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL DEFAULT )" << DefaultExpr << R"(,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "id"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (v) VALUES (1);");
        fixture.Check("SELECT id, v FROM TestTable;", "[[42u;[1]]]");
    }

    Y_UNIT_TEST(TtlColumn) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                ts Timestamp DEFAULT CurrentUtcTimestamp(),
                PRIMARY KEY (k)
            ) WITH (TTL = Interval("PT1H") ON ts);
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, ts IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(MultipleDefaultsIndependent) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Uint64 DEFAULT RandomNumber(1) * 0ul + 1ul,
                b Uint64 DEFAULT RandomNumber(2) * 0ul + 2ul,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, a) VALUES (2, 100ul);");
        fixture.Check("SELECT k, a, b FROM TestTable ORDER BY k;", "[[1;[1u];[2u]];[2;[100u];[2u]]]");
    }

    Y_UNIT_TEST(CoercibleType) {
        TTestFixture fixture;
        // The expression is Uint64, the column is Int64
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << DefaultExpr));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");
    }

    Y_UNIT_TEST(NamedNodeRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT RandomNumber(1) * $factor + 42ul", "$factor = 0ul;\n"),
            "Unknown name: $factor");
    }

    Y_UNIT_TEST(UnrelatedPragmaAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(UnrelatedDeclareAccepted) {
        TTestFixture fixture;
        // Unrelated query declarations are not part of the stored expression
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "DECLARE $unused AS Int32;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(LiteralDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Uint32 DEFAULT 5"));

        const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(col.GetDefaultFromLiteral().type().type_id()),
            static_cast<int>(Ydb::Type::UINT32));
        UNIT_ASSERT_VALUES_EQUAL(col.GetDefaultFromLiteral().value().uint32_value(), 5u);

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable").GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
        const auto& proto = NYdb::TProtoAccessor::GetProto(describe.GetTableDescription());
        bool found = false;
        for (const auto& column : proto.columns()) {
            if (column.name() == "v") {
                found = true;
                UNIT_ASSERT_C(column.has_from_literal(), column.ShortDebugString());
                UNIT_ASSERT_VALUES_EQUAL(column.from_literal().value().uint32_value(), 5u);
            }
        }
        UNIT_ASSERT_C(found, "column v not found in DescribeTable");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5u]]]");
    }

    Y_UNIT_TEST_TWIN(ConstantDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT (1 + 2) * 3"));
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int32_value(), 9);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[9]]]");
    }

    Y_UNIT_TEST_TWIN(CoercedConstantDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64 DEFAULT CAST(7 AS Int64) + 2l"));
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 9);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[9]]]");
    }

    Y_UNIT_TEST_TWIN(CoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        for (const auto& [expression, expected] : {
            std::pair{"Abs(-5)", 5},
            std::pair{"ListSum(ListMap(AsList(2, 3), ($x) -> { RETURN $x; }))", 5},
            std::pair{R"(String::LevensteinDistance("a", "b"))", 1}})
        {
            fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << expression));
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), expected);
            fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
            fixture.Check("SELECT k, v FROM TestTable;", TStringBuilder() << "[[1;[" << expected << "]]]");
            fixture.Exec("DROP TABLE TestTable;");
        }
    }

    Y_UNIT_TEST(RuntimeSettingCoercionStaysExpression) {
        TTestFixture fixture;
        for (const auto* expression : {
            "Length(Yql::HostRuntimeSetting('test'))",
            "Length(Yql::UdfRuntimeSetting('String', 'test'))",
            // The ordinary KQP runner does not install the common DQ transform
            // needed to lower CurrentActorId; this case checks its metadata only.
            "Length(Yql::CurrentActorId())"})
        {
            fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << expression));
            AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
            if (TString(expression).Contains("RuntimeSetting")) {
                // An unknown host setting and an absent UDF setting both return NULL.
                fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
                fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
            }
            fixture.Exec("DROP TABLE TestTable;");
        }
    }

    Y_UNIT_TEST(CurrentActorIdDefaultExecutesWithCommonDqTransform) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(false);
        appConfig.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(false);
        appConfig.MutableFeatureFlags()->SetEnableStreamingQueries(false);

        // Proxy bootstrap needs a driver, PQ gateway and credentials even though
        // this test uses only a local table. Keep their owners alive past the runner.
        NYdb::TDriverConfig driverConfig;
        driverConfig.SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
        auto driver = MakeSharedYdbDriverWithStop(std::make_unique<NYdb::TDriver>(driverConfig));
        auto credentialsFactory = NYql::CreateStructuredTokenCredentialsFactory();
        const auto& queryConfig = appConfig.GetQueryServiceConfig();
        auto setupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            nullptr, nullptr, credentialsFactory, nullptr,
            queryConfig.GetS3(), queryConfig.GetGeneric(), queryConfig.GetYt(), nullptr,
            queryConfig.GetSolomon(), nullptr, NYql::NDq::TS3ReadActorFactoryConfig{},
            NYql::CreateCommonDqTaskTransformFactory(), NYql::TPqGatewayConfig{},
            MakePqGatewayFactory(driver, credentialsFactory), nullptr, driver);
        TTestFixture fixture(TKikimrSettings(appConfig)
            .SetWithSampleTables(false)
            .SetFederatedQuerySetupFactory(setupFactory));
        fixture.Exec(DefaultExprDDL("v String NOT NULL DEFAULT Yql::CurrentActorId()"));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "Yql::CurrentActorId()");

        auto db = fixture.Runner().GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            "INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetInt32(), 1);
        const TString actorId(parser.ColumnParser("v").GetString());
        // Parse validates [NodeId:LocalId:Hint] only; the printable form cannot
        // reconstruct actor identity and is not compared with another callable.
        NActors::TActorId parsedShape;
        UNIT_ASSERT_C(parsedShape.Parse(actorId.data(), actorId.size()), actorId);
        UNIT_ASSERT(!parser.TryNextRow());
        CompareYson(FormatResultSetYson(result.GetResultSet(0)),
            fixture.QueryYson("SELECT k, v FROM TestTable;"));
    }

    Y_UNIT_TEST_TWIN(AddCoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64"));
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN added Int64 DEFAULT Abs(-5);");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "added");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 5);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, added FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST_TWIN(SetCoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64"));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT Abs(-5);");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 5);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST_TWIN(LegacyDefaultConstructorsPreserveLiteralMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                negative Int32 DEFAULT -5,
                text Utf8 DEFAULT CAST("hello" AS Utf8),
                amount Decimal(22,9) DEFAULT Decimal("1.11", 22, 9),
                json Json DEFAULT Json('{"k":1}'),
                PRIMARY KEY (k)
            );
        )");
        for (const auto* name : {"negative", "text", "amount", "json"}) {
            const auto column = fixture.ColumnDesc("/Root/TestTable", name);
            UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        }
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, negative, text FROM TestTable;", R"([[1;[-5];["hello"]]])");
        auto result = fixture.Runner().GetQueryClient().ExecuteQuery(
            "SELECT amount, json FROM TestTable;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto amount = parser.ColumnParser("amount").GetOptionalDecimal();
        const auto json = parser.ColumnParser("json").GetOptionalJson();
        UNIT_ASSERT(amount.has_value());
        UNIT_ASSERT(json.has_value());
        UNIT_ASSERT_VALUES_EQUAL(amount->ToString(), "1.11");
        UNIT_ASSERT_VALUES_EQUAL(*json, R"({"k":1})");
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST_TWIN(NullDefault, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT NULL"));
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST(FeatureFlagDisabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Rejects(ValueTableDDL(), "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(CoercibleSourceRequiresFeatureFlag) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << DefaultExpr),
            "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(PersistedDefaultWorksAfterFeatureDisabled) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr));
        fixture.DisableExpressionDefaults("/Root/TestTable");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]]]");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(2);",
            "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(PgDefaultCoercionMatchesAfterReload) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        const TString expression = TStringBuilder() << "ToPg(CAST(" << DefaultExpr << " AS Int32))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v pgint8 DEFAULT " << expression));
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.RestartSchemeShard("/Root/TestTable");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, FromPg(v) FROM TestTable ORDER BY k;", "[[1;[42]];[2;[42]]]");
    }

    Y_UNIT_TEST(PersistedPgDefaultUsesCurrentCompatibilityPolicy) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        const TString expression = TStringBuilder() << "COALESCE(FromPg(ToPg(CAST(" << DefaultExpr << " AS Int32))), 0)";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int32 DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");

        auto& runtime = *fixture.Runner().GetTestServer().GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            NKikimrConfig::TFeatureFlags flags = runtime.GetAppData(node).FeatureFlags;
            flags.SetDisablePgCompatibility(true);
            runtime.GetAppData(node).UpdateRuntimeFlags(flags);
        }
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Rejects("INSERT INTO TestTable (k) VALUES (2);", "PostgreSQL compatibility is disabled");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42]];[2;[7]]]");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
    }

    Y_UNIT_TEST(FeatureFlagDisabledPreservesLiteralDefaultsAndWrites) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32,
                literal Uint64,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 7ul;");
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN added Uint64 DEFAULT 9ul;");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (2);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, literal, added) VALUES (3, 30ul, 40ul);");
        fixture.Exec("UPDATE TestTable SET other = 6 WHERE k = 2;");
        fixture.Exec("DELETE FROM TestTable WHERE k = 3;");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal DROP DEFAULT;");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (4);");

        auto tableClient = fixture.Runner().GetTableClient();
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(5).EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected a missing literal DEFAULT to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: added");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(5)
                    .AddMember("added").OptionalUint64(50)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, other, literal, added FROM TestTable ORDER BY k;",
            "[[1;#;[7u];[9u]];[2;[6];[7u];[9u]];[4;#;#;[9u]];[5;#;#;[50u]]]");
    }

    Y_UNIT_TEST(ColumnReferenceRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT other + 1"), "Column reference");
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT missing + 1"), "Column reference");
    }

    Y_UNIT_TEST(WholeRowReferenceRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT TableRow().other"), "TableRow requires data source");
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT JoinTableRow().other"), "JoinTableRow requires data source");
    }

    Y_UNIT_TEST(SelfReferenceRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT v + 1"), "Column reference");
    }

    Y_UNIT_TEST(AggregateFunctionVariantsRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT SUM(RandomNumber(1))"), "aggregation function");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT COUNT(*)"), "aggregation function");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT ListLength(AGGREGATE_LIST(RandomNumber(1)))"),
            "aggregation function");
    }

    Y_UNIT_TEST(WindowFunctionVariantsRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT RANK() OVER (ORDER BY RandomNumber(1))"),
            "Window and aggregation functions are not allowed");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT NTILE(2) OVER (ORDER BY RandomNumber(1))"),
            "Window and aggregation functions are not allowed");
    }

    Y_UNIT_TEST(SubqueryRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT EXISTS (SELECT k FROM Source)"), "subquery");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT NOT EXISTS (SELECT k FROM Source)"), "subquery");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT ListLength((SELECT COUNT(*) FROM Source))"), "subquery");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT 1 NOT IN (SELECT k FROM Source)"), "subquery");
    }

    Y_UNIT_TEST(NamedExpressionRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT 1 IN $ids", "$ids = (SELECT k FROM Source);\n"),
            "Unknown name: $ids");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT $value + RandomNumber(1)", "$value = 5ul;\n"),
            "Unknown name: $value");
    }

    Y_UNIT_TEST(ParameterRejected) {
        TTestFixture fixture;
        auto db = fixture.Runner().GetQueryClient();
        auto params = NYdb::TParamsBuilder()
            .AddParam("$p").Int32(7).Build()
            .Build();
        auto result = db.ExecuteQuery(
            DefaultExprDDL("v Int32 DEFAULT $p", "DECLARE $p AS Int32;\n"),
            TTxControl::NoTx(), params).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "DEFAULT must not retain a query parameter");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown name: $p");
    }

    Y_UNIT_TEST(NonRowCallableRejected) {
        TTestFixture fixture;
        // Concatenating a random value keeps the expression out of constant folding, so the
        // non-row callable survives to the expression compiler
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT CurrentAuthenticatedUser() || CAST(RandomNumber(1) AS String)"),
            "CurrentAuthenticatedUser");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT TablePath() || CAST(RandomNumber(1) AS String)"),
            "TablePath");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT TableName() || CAST(RandomNumber(1) AS String)"),
            "TableName");
        fixture.Rejects(
            DefaultExprDDL("v Uint64 DEFAULT TableRecordIndex() + RandomNumber(1)"),
            "TableRecord");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT FileContent(\"f\") || CAST(RandomNumber(1) AS String)"),
            "FileContent");
        fixture.Rejects(
            DefaultExprDDL("v Int32 DEFAULT EvaluateExpr(1 + 2) + CAST(RandomNumber(1) AS Int32)"),
            "EvaluateExpr");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT SecureParam(\"token\") || CAST(RandomNumber(1) AS String)"),
            "SecureParam");
    }

    Y_UNIT_TEST(ListHasItemsAcceptsScalarAndRejectsSubquery) {
        TTestFixture fixture;
        const TString expression = "ListHasItems(ListFilter(AsList(RandomNumber(1)), ($x) -> ($x >= 0ul)))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Bool DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[%true]]]");
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects("CREATE TABLE Invalid (k Int32 NOT NULL, v Bool DEFAULT "
            "ListHasItems((SELECT k FROM Source)), PRIMARY KEY (k));", "subquery");
    }

    Y_UNIT_TEST(SingleRowExpressionWithNestedLambdaAccepted) {
        TTestFixture fixture;
        const TString expression = "ListSum(ListMap(AsList(RandomNumber(1), RandomNumber(2)), "
            "($item) -> { RETURN $item * 0ul + 1ul; }))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[2u]]]");
    }

    Y_UNIT_TEST(TypeMismatchRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v String DEFAULT " << DefaultExpr), "type mismatch");
    }

    Y_UNIT_TEST(TypeMismatchReportsDefaultColumnBeforeConversionFailure) {
        TTestFixture fixture;
        const TString issues = fixture.Rejects(
            DefaultExprDDL("v4 Uint32 DEFAULT RandomNumber(2)"),
            "Failed to convert type: Uint64 to Optional<Uint32>");

        const TString context =
            "Default expr v4 type mismatch, expected: Uint32, actual: Uint64";
        const size_t contextPos = issues.find(context);
        const size_t causePos = issues.find("Failed to convert type: Uint64 to Optional<Uint32>");
        UNIT_ASSERT_C(contextPos != TString::npos, issues);
        UNIT_ASSERT_C(contextPos < causePos, issues);
    }

    Y_UNIT_TEST(NotNullOptionalExprRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 NOT NULL DEFAULT CAST(other AS Int32)"), "Column reference");
        fixture.Rejects(DefaultExprDDL("v Uint64 NOT NULL DEFAULT CAST(CAST(RandomNumber(1) AS String) AS Uint64)"),
            "declared NOT NULL");
    }

    Y_UNIT_TEST(JsonExistsDefaultRespectsNullability) {
        TTestFixture fixture;
        const TString expression = R"(JSON_EXISTS(
            IF(RandomNumber(1) * 0ul = 0ul, Json(@@{"key":1}@@), NULL), "$.key" UNKNOWN ON ERROR))";
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v Bool NOT NULL DEFAULT " << expression),
            "is declared NOT NULL");

        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Bool DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[%true]]]");
    }

    Y_UNIT_TEST(NullableOptionalExprAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(
            R"(v Uint64 DEFAULT CAST("invalid" || CAST(RandomNumber(1) AS String) AS Uint64))"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST(SetAndDropNotNullKeepExpressionDefault) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableSetColumnConstraint(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET NOT NULL;");
        const auto required = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT(required.GetNotNull());
        AssertFromExpression(required, DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, v;", "[[2;42u]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP NOT NULL;");
        fixture.RestartSchemeShard("/Root/TestTable");
        const auto nullable = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT(!nullable.GetNotNull());
        AssertFromExpression(nullable, DefaultExpr);
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (3, NULL);");
        fixture.Check("INSERT INTO TestTable (k) VALUES (4) RETURNING k, v;", "[[4;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]];[3;#];[4;[42u]]]");
    }

    Y_UNIT_TEST(DefaultAndGeneratedRejected) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Rejects(DefaultExprDDL(TStringBuilder()
                << "v Uint64 DEFAULT " << DefaultExpr << " GENERATED ALWAYS AS (CAST(k AS Uint64)) STORED"),
            "same time");
    }

    Y_UNIT_TEST(DefaultCannotReadGeneratedColumn) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Rejects(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                g Int32 GENERATED ALWAYS AS (k + 1) STORED,
                v Int32 DEFAULT g,
                PRIMARY KEY (k)
            );
        )", "Column reference");
    }

    Y_UNIT_TEST(DefaultAndSerialRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Serial DEFAULT 5"), "already set");
    }

    Y_UNIT_TEST(OlapRejected) {
        TTestFixture fixture;
        fixture.Rejects(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN);
        )", "column tables");
        fixture.Exec(R"(
            CREATE TABLE TestTable (k Uint64 NOT NULL, v Uint64, PRIMARY KEY (k)) WITH (STORE = COLUMN);
        )");
        fixture.Rejects(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";",
            "Default values are not supported in column tables");
    }

    Y_UNIT_TEST(ExternalTableRejectsExpressionDefault) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(true);
        appConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE EXTERNAL DATA SOURCE ExternalSource WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="my-bucket", AUTH_METHOD="NONE"
            );
        )");
        fixture.Rejects(R"(
            CREATE EXTERNAL TABLE TestTable (
                v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul
            ) WITH (DATA_SOURCE="/Root/ExternalSource", LOCATION="/");
        )", "DEFAULT expressions are not supported in external tables");
        fixture.Exec(R"(
            CREATE EXTERNAL TABLE TestTable (v Uint64)
            WITH (DATA_SOURCE="/Root/ExternalSource", LOCATION="/");
        )");
    }

    Y_UNIT_TEST(AddColumnWithDefaultExprRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ADD COLUMN w Uint64 DEFAULT " << DefaultExpr << ";",
            "Column addition with a DEFAULT expression is not supported");
    }

    Y_UNIT_TEST(AddColumnWithCoercibleDefaultExprRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ADD COLUMN w Int64 DEFAULT " << DefaultExpr << ";",
            "Column addition with a DEFAULT expression is not supported");
    }

    Y_UNIT_TEST(AddColumnWithDefaultLiteralAccepted) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN w Int32 NOT NULL DEFAULT 5;");
        {
            const auto col = fixture.ColumnDesc("/Root/TestTable", "w");
            UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        }

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v, w FROM TestTable ORDER BY k;", "[[1;[42u];5];[2;[42u];5]]");
    }

    Y_UNIT_TEST(OnlyRequiredExpressionDefaultsAreCompiledIntoWriteAst) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                r Uint64 DEFAULT RandomNumber(1),
                ts Timestamp DEFAULT CurrentUtcTimestamp(),
                PRIMARY KEY (k)
            );
        )");

        for (const TStringBuf operation : {"INSERT", "UPSERT", "REPLACE"}) {
            {
                const TString query = TStringBuilder() << operation
                    << R"( INTO TestTable (k, ts) VALUES (1, Timestamp("2020-01-01T00:00:00Z"));)";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(ast.Contains("RandomNumber"),
                    "required DEFAULT expression is absent for: " << query << "\nAST:\n" << ast);
                UNIT_ASSERT_C(!ast.Contains("CurrentUtcTimestamp"),
                    "explicitly supplied DEFAULT expression was compiled for: " << query << "\nAST:\n" << ast);
            }
            {
                const TString query = TStringBuilder() << operation
                    << " INTO TestTable (k, r) VALUES (1, 7ul);";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(!ast.Contains("RandomNumber"),
                    "explicitly supplied DEFAULT expression was compiled for: " << query << "\nAST:\n" << ast);
                UNIT_ASSERT_C(ast.Contains("CurrentUtcTimestamp"),
                    "required DEFAULT expression is absent for: " << query << "\nAST:\n" << ast);
            }
            {
                const TString query = TStringBuilder() << operation
                    << R"( INTO TestTable (k, r, ts) VALUES (1, 7ul, Timestamp("2020-01-01T00:00:00Z"));)";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(!ast.Contains("RandomNumber") && !ast.Contains("CurrentUtcTimestamp"),
                    "unused DEFAULT expressions were compiled for: " << query << "\nAST:\n" << ast);
            }
        }
    }

    Y_UNIT_TEST_TWIN(UpsertPreparesOmittedDefaultKeys, WithIndex) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE LiteralKeyTable (
                k Int32 NOT NULL DEFAULT 1,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k)
            );
            CREATE TABLE CompositeKeyTable (
                k Int32 NOT NULL DEFAULT 1,
                part Uint64 NOT NULL DEFAULT RandomNumber(2) * 0ul + 2ul,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
        )");
        if (WithIndex) {
            fixture.Exec("ALTER TABLE LiteralKeyTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
            fixture.Exec("ALTER TABLE CompositeKeyTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
        }

        UNIT_ASSERT(fixture.ColumnDesc("/Root/LiteralKeyTable", "k").HasDefaultFromLiteral());
        UNIT_ASSERT(fixture.ColumnDesc("/Root/CompositeKeyTable", "k").HasDefaultFromLiteral());
        AssertFromExpression(fixture.ColumnDesc("/Root/CompositeKeyTable", "part"),
            "RandomNumber(2) * 0ul + 2ul");

        fixture.Check("UPSERT INTO LiteralKeyTable (other) VALUES (5) RETURNING k, other, v;",
            "[[1;[5];[42u]]]");
        fixture.Exec("UPSERT INTO LiteralKeyTable (k, v) VALUES (1, 7ul);");
        fixture.Check("UPSERT INTO LiteralKeyTable (other) VALUES (6) RETURNING k, other, v;",
            "[[1;[6];[7u]]]");
        fixture.Check("SELECT k, other, v FROM LiteralKeyTable;", "[[1;[6];[7u]]]");

        fixture.Check("UPSERT INTO CompositeKeyTable (other) VALUES (5) RETURNING k, part, other, v;",
            "[[1;2u;[5];[42u]]]");
        fixture.Exec("UPSERT INTO CompositeKeyTable (k, part, v) VALUES (1, 2ul, 7ul);");
        fixture.Check("UPSERT INTO CompositeKeyTable (other) VALUES (6) RETURNING k, part, other, v;",
            "[[1;2u;[6];[7u]]]");
        fixture.Check("SELECT k, part, other, v FROM CompositeKeyTable;", "[[1;2u;[6];[7u]]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM LiteralKeyTable VIEW idx;", "[[1;[6];[7u]]]");
            fixture.Check("SELECT k, part, other, v FROM CompositeKeyTable VIEW idx;", "[[1;2u;[6];[7u]]]");
        }
    }

    Y_UNIT_TEST(ExpressionSequenceAndLiteralDefaultCombinations) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Serial,
                tag Utf8 NOT NULL,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                PRIMARY KEY (id)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "expr"), DefaultExpr);
        const auto literal = fixture.ColumnDesc("/Root/TestTable", "literal");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        // Exercise every subset of omitted automatic columns: sequence + expression, expression +
        // literal, sequence + literal, all three, each one individually, and none.
        fixture.Exec(R"(INSERT INTO TestTable (tag, literal) VALUES ("a", 101ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag) VALUES (100, "b");)");
        fixture.Exec(R"(INSERT INTO TestTable (tag, expr) VALUES ("c", 103ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (tag) VALUES ("d");)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, expr) VALUES (101, "e", 105ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, literal) VALUES (102, "f", 106ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (tag, expr, literal) VALUES ("g", 107ul, 108ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, expr, literal) VALUES (103, "h", 109ul, 110ul);)");

        fixture.Check("SELECT tag, id, expr, literal FROM TestTable ORDER BY tag;", R"(
            [["a";1;[42u];[101u]];
             ["b";100;[42u];[8u]];
             ["c";2;[103u];[8u]];
             ["d";3;[42u];[8u]];
             ["e";101;[105u];[8u]];
             ["f";102;[42u];[106u]];
             ["g";4;[107u];[108u]];
             ["h";103;[109u];[110u]]]
        )");
    }

    Y_UNIT_TEST_TWIN(DmlMatrixWithoutIndex, WithReturning) {
        RunDmlMatrix(ESecondaryIndexKind::None, WithReturning);
    }

    Y_UNIT_TEST_TWIN(DmlMatrixWithSyncIndex, WithReturning) {
        RunDmlMatrix(ESecondaryIndexKind::Sync, WithReturning);
    }

    Y_UNIT_TEST_TWIN(DmlMatrixWithAsyncIndex, WithReturning) {
        RunDmlMatrix(ESecondaryIndexKind::Async, WithReturning);
    }

    Y_UNIT_TEST(InsertOmittedAndExplicit) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]]]");
    }

    Y_UNIT_TEST(UpsertOmittedNewRow) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    // The materialized DEFAULT value is used only for the insert half of UPSERT: when the row
    // already exists, omitting the column leaves its stored value alone.
    Y_UNIT_TEST(UpsertOmittedExistingRowKeepsValue) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
    }

    Y_UNIT_TEST(UpsertOmittedDefaultsUseWritePath) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32,
                expr Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                literal Uint64 DEFAULT 8ul,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("UPSERT INTO TestTable (k, expr, literal) VALUES (1, NULL, 7ul);");

        const std::string upsert = "UPSERT INTO TestTable (k, other) VALUES (1, 10), (2, 20);";
        fixture.CheckTargetReadCount(upsert, 0);
        auto result = fixture.Runner().GetQueryClient().ExecuteQuery(upsert, TTxControl::NoTx(),
            TExecuteQuerySettings().StatsMode(EStatsMode::Full)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        AssertTableStats(result, "/Root/TestTable", {.ExpectedReads = 0, .ExpectedUpdates = 2});
        fixture.Check("SELECT k, other, expr, literal FROM TestTable ORDER BY k;", "[[1;[10];#;[7u]];[2;[20];[42u];[8u]]]");
    }

    Y_UNIT_TEST_TWIN(NewRboPartialUpsertWithExpressionDefault, EnableFallback) {
        auto appConfig = DefaultExprAppConfig();
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(EnableFallback);
        config->SetEnableIndexStreamWrite(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 0, 7ul);");

        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 10), (2, 20);");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1;[10];[7u]];[2;[20];[42u]]]");
    }

    Y_UNIT_TEST_TWIN(UpsertOmittedKeysWithCompileTimeDefaults, EnableCompileTimeDefaults) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(EnableCompileTimeDefaults);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL DEFAULT 1,
                part Uint64 NOT NULL DEFAULT RandomNumber(2) * 0ul + 2ul,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (other) VALUES (10);");
        fixture.Check("SELECT k, part, other, v FROM TestTable;", "[[1;2u;[10];[42u]]]");
        fixture.Exec("UPSERT INTO TestTable (k, part, v) VALUES (1, 2ul, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (other) VALUES (20);");
        fixture.Check("SELECT k, part, other, v FROM TestTable;", "[[1;2u;[20];[7u]]]");
    }

    Y_UNIT_TEST_TWIN(MixedDefaultsWithIndexedReturning, EnableCompileTimeDefaults) {
        auto appConfig = DefaultExprAppConfig();
        auto* config = appConfig.MutableTableServiceConfig();
        config->SetEnableCompileTimeDefaults(EnableCompileTimeDefaults);
        config->SetEnableIndexStreamWrite(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Serial,
                tag Int32 NOT NULL,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64 DEFAULT 8ul,
                PRIMARY KEY (id),
                INDEX idx_expr GLOBAL SYNC ON (expr) COVER (tag, literal)
            );
        )");

        const TString expected = "[[1;10;[42u];[8u]]]";
        fixture.Check("UPSERT INTO TestTable (tag) VALUES (10) RETURNING id, tag, expr, literal;", expected);
        fixture.Check("SELECT id, tag, expr, literal FROM TestTable;", expected);
        fixture.Check("SELECT id, tag, expr, literal FROM TestTable VIEW idx_expr;", expected);
    }

    Y_UNIT_TEST(ExpressionLiteralAndGeneratedDefaultsStayConsistent) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Uint64,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                generated Uint64 GENERATED ALWAYS AS (
                    COALESCE(other, 0ul) + COALESCE(expr, 0ul) + COALESCE(literal, 0ul)
                ) STORED,
                PRIMARY KEY (k),
                INDEX idx_generated GLOBAL ON (generated) COVER (other, expr, literal)
            ) WITH (
                PARTITION_AT_KEYS = (2)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        const auto literal = fixture.ColumnDesc("/Root/TestTable", "literal");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        fixture.Exec("INSERT INTO TestTable (k, other, expr, literal) VALUES (1, 1ul, 7ul, 9ul);");
        const TString returned = fixture.QueryYson(R"(
            UPSERT INTO TestTable (k, other) VALUES (1, 5ul), (2, 6ul)
            RETURNING k, other, expr, literal, generated;
        )");

        const TString expected = "[[1;[5u];[7u];[9u];[21u]];[2;[6u];[42u];[8u];[56u]]]";
        CompareYsonUnordered(expected, returned);
        fixture.Check("SELECT k, other, expr, literal, generated FROM TestTable ORDER BY k;", expected);
        fixture.Check("SELECT k, other, expr, literal, generated FROM TestTable VIEW idx_generated ORDER BY generated;",
            expected);
    }

    Y_UNIT_TEST(UpsertNotNullGeneratedDependsOnNotNullExpressionDefault) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Int32 NOT NULL,
                a Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 9ul,
                b Uint64,
                g Uint64 NOT NULL GENERATED ALWAYS AS (a + COALESCE(b, 0ul)) STORED,
                PRIMARY KEY (id)
            );
        )");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "a"), "RandomNumber(1) * 0ul + 9ul");

        // A new row uses the materialized DEFAULT dependency: a = 9, g = 9 + 2
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (1, 2);");

        // An existing row keeps its stored dependency instead of replacing it with the DEFAULT
        fixture.Exec("UPSERT INTO TestTable (id, a, b) VALUES (2, 4, 1);");
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (2, 3);");

        fixture.Check("SELECT id, a, b, g FROM TestTable ORDER BY id;",
            "[[1;9u;[2u];11u];[2;4u;[3u];7u]]");
    }

    Y_UNIT_TEST(UpsertGeneratedUsesStoredDependenciesOrDefaults) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32,
                a Uint64 NOT NULL DEFAULT COALESCE(RandomNumber(1) % 1000000000ul, 0ul) + 100ul,
                b Uint64 DEFAULT 8ul,
                g Uint64 NOT NULL GENERATED ALWAYS AS (a + COALESCE(b, 0ul)) STORED,
                PRIMARY KEY (k)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "a"),
            "COALESCE(RandomNumber(1) % 1000000000ul, 0ul) + 100ul");
        UNIT_ASSERT(fixture.ColumnDesc("/Root/TestTable", "b").HasDefaultFromLiteral());
        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (NULL, 7ul, 9ul), (1, 17ul, NULL);");

        const std::string supplied = "UPSERT INTO TestTable (k, a, b) VALUES (3, 10ul, 11ul);";
        fixture.CheckTargetReadCount(supplied, 0);
        fixture.Exec(supplied);
        fixture.Check("SELECT g FROM TestTable WHERE k = 3;", "[[21u]]");

        const std::string omitted = "UPSERT INTO TestTable (k) VALUES (NULL), (1), (2);";
        fixture.CheckTargetReadCount(omitted, 1);
        fixture.Exec(omitted);
        fixture.Check("SELECT a, b, g FROM TestTable WHERE k IS NULL OR k = 1 ORDER BY k;",
            "[[7u;[9u];16u];[17u;#;17u]]");
        fixture.Check("SELECT b, g = a + 8ul FROM TestTable WHERE k = 2;", "[[[8u];%true]]");
    }

    Y_UNIT_TEST(StoredAndVirtualColumnsUseDefaultedValue) {
        auto appConfig = DefaultExprAppConfig(TFixtureOptions{.GeneratedStored = true});
        appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                s Uint64 NOT NULL GENERATED ALWAYS AS (v + 1ul) STORED,
                g Uint64 NOT NULL GENERATED ALWAYS AS (v + 2ul) VIRTUAL,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v) COVER (s)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v, s, g;",
            "[[1;42u;43u;44u]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 50ul;");
        fixture.CheckUnordered("UPSERT INTO TestTable (k) VALUES (1), (2) RETURNING k, v, s, g;",
            "[[1;42u;43u;44u];[2;50u;51u;52u]]");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Check("UPDATE TestTable SET v = 7ul WHERE k = 1 RETURNING k, v, s, g;",
            "[[1;7u;8u;9u]]");
        fixture.Check("SELECT k, v, s, g FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1;7u;8u;9u];[2;50u;51u;52u]]");
    }

    Y_UNIT_TEST_TWIN(FailingDefaultIsEvaluatedBeforeUpsert, WithIndex) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(
            R"(v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"))"));
        if (WithIndex) {
            fixture.Exec("ALTER TABLE TestTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
        }
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 4, 7ul);");
        fixture.Rejects("UPSERT INTO TestTable (k, other) VALUES (1, 5) RETURNING k, other, v;", "default evaluated");
        fixture.Rejects("UPSERT INTO TestTable (k, other) VALUES (2, 6);", "default evaluated");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[4];[7u]]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM TestTable VIEW idx;", "[[1;[4];[7u]]]");
        }
        fixture.Exec("UPSERT INTO TestTable (k, other, v) VALUES (2, 6, 8ul);");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1;[4];[7u]];[2;[6];[8u]]]");
    }

    Y_UNIT_TEST_TWIN(NullKeyUpsertPreservesStoredDefaults, WithIndex) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE NullKeyTable (
                k Int32,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k)
            );
            CREATE TABLE CompositeNullKeyTable (
                k Int32,
                part Int32,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
            CREATE TABLE NullKeyInsertTable (
                k Int32,
                part Int32,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
        )");
        if (WithIndex) {
            for (const TString table : {"NullKeyTable", "CompositeNullKeyTable", "NullKeyInsertTable"}) {
                fixture.Exec(TStringBuilder() << "ALTER TABLE " << table
                    << " ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
            }
        }

        fixture.Exec("INSERT INTO NullKeyTable (k, v) VALUES (NULL, 7ul);");
        fixture.Check("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 5) RETURNING k, other, v;",
            "[[#;[5];[7u]]]");
        fixture.Exec("UPSERT INTO NullKeyTable (k, v) VALUES (NULL, NULL);");
        fixture.Check("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 6) RETURNING k, other, v;",
            "[[#;[6];#]]");
        fixture.Check("SELECT k, other, v FROM NullKeyTable;", "[[#;[6];#]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM NullKeyTable VIEW idx;", "[[#;[6];#]]");
        }
        fixture.Exec("DELETE FROM NullKeyTable WHERE k IS NULL;");
        fixture.Exec("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 7);");
        fixture.Check("SELECT k, other, v FROM NullKeyTable;", "[[#;[7];[42u]]]");
        fixture.Rejects("UPSERT INTO NullKeyTable (other) VALUES (8);", "Missing key column in input: k");

        fixture.Exec("INSERT INTO CompositeNullKeyTable (k, part, v) VALUES (1, NULL, 7ul), (NULL, 1, NULL);");
        const TString existingRows = "[[[1];#;[5];[7u]];[#;[1];[6];#]]";
        fixture.CheckUnordered("UPSERT INTO CompositeNullKeyTable (k, part, other) "
            "VALUES (1, NULL, 5), (NULL, 1, 6) RETURNING k, part, other, v;", existingRows);
        fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable;", existingRows);
        fixture.Exec("UPSERT INTO CompositeNullKeyTable (k, part, other) VALUES (NULL, NULL, 7);");
        const TString allRows = "[[[1];#;[5];[7u]];[#;[1];[6];#];[#;#;[7];[42u]]]";
        fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable;", allRows);
        if (WithIndex) {
            fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable VIEW idx;", allRows);
        }

        const TString insertedRows = "[[#;[1];[10];[42u]];[[1];#;[20];[42u]];[#;#;[30];[42u]]]";
        fixture.CheckUnordered("UPSERT INTO NullKeyInsertTable (k, part, other) "
            "VALUES (NULL, 1, 10), (1, NULL, 20), (NULL, NULL, 30) RETURNING k, part, other, v;", insertedRows);
        fixture.CheckUnordered("SELECT k, part, other, v FROM NullKeyInsertTable;", insertedRows);
        if (WithIndex) {
            fixture.CheckUnordered("SELECT k, part, other, v FROM NullKeyInsertTable VIEW idx;", insertedRows);
        }
    }

    Y_UNIT_TEST(UpsertOmittedLiteralDefaultKeepsExistingRow) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 42ul;");

        const auto literal = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
    }

    Y_UNIT_TEST(UpsertExplicitOverridesDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("UPSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[7u]]]");
    }

    Y_UNIT_TEST(ReplaceOmitted) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(UpdateDoesNotApplyDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPDATE TestTable SET other = 5 WHERE k = 1;");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");

        fixture.Exec("UPDATE TestTable SET v = 8ul WHERE k = 1;");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[8u]]]");

        fixture.Exec("UPDATE TestTable ON (k, other) VALUES (1, 6);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[6];[8u]]]");
    }

    Y_UNIT_TEST(DeleteUnaffected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Exec("DELETE FROM TestTable WHERE k = 1;");
        fixture.Check("SELECT k, v FROM TestTable;", "[[2;[42u]]]");
    }

    Y_UNIT_TEST(MultiRowValuesAndInsertFromSelect) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2), (3);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]];[3;[42u]]]");

        fixture.Exec("INSERT INTO TestTable (k) SELECT k + 10 FROM TestTable;");
        fixture.CheckUnordered("SELECT k, v FROM TestTable WHERE k > 10;",
            "[[11;[42u]];[12;[42u]];[13;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(ReturningSelectSourceForms, EnableStreamWrite) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(ValueTableDDL());
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, other Int32, PRIMARY KEY (k));");
        fixture.Exec("UPSERT INTO Source (k, other) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (4, 7ul), (6, 8ul);");

        fixture.CheckUnordered(R"(
            INSERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (1, 2) RETURNING *;
        )", "[[1;[11];[42u]];[2;[22];[42u]]]");
        fixture.Check(R"(
            INSERT OR REVERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k = 3 RETURNING k;
        )", "[[3]]");
        fixture.CheckUnordered(R"(
            UPSERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (4, 5) RETURNING v, k;
        )", "[[[7u];4];[[42u];5]]");
        fixture.CheckUnordered(R"(
            REPLACE INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (5, 6) RETURNING v;
        )", "[[[42u]];[[42u]]]");
        fixture.Check(R"(
            UPDATE TestTable ON SELECT k, other + 1 AS other FROM Source WHERE k = 4 RETURNING v, other, k;
        )", "[[[7u];[45];4]]");
        fixture.Check(R"(
            DELETE FROM TestTable ON SELECT k FROM Source WHERE k = 2 RETURNING v, k;
        )", "[[[42u];2]]");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;",
            "[[1;[11];[42u]];[3;[33];[42u]];[4;[45];[7u]];[5;[55];[42u]];[6;[66];[42u]]]");
    }

    Y_UNIT_TEST(ParameterizedSelectSourceAppliesDefaults) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul), (4, 8ul);");
        auto params = TParamsBuilder()
            .AddParam("$rows").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(2).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(3).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(4).EndStruct()
            .EndList().Build()
            .Build();
        const auto write = [&](const TString& statement, const TString& expected) {
            auto result = fixture.QuerySession().ExecuteQuery(
                "DECLARE $rows AS List<Struct<k:Int32>>;\n" + statement,
                TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), statement << "\n" << result.GetIssues().ToString());
            CompareYsonUnordered(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        write("INSERT INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k = 1 RETURNING k, v;",
            "[[1;[42u]]]");
        write("UPSERT INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k IN (2, 3) RETURNING k, v;",
            "[[2;[7u]];[3;[42u]]]");
        write("REPLACE INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k = 4 RETURNING k, v;",
            "[[4;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]];[3;[42u]];[4;[42u]]]");
    }

    Y_UNIT_TEST(EmptyWritesDoNotEvaluateDefault) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(R"(v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default ran"))"));
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        const auto params = TParamsBuilder()
            .AddParam("$rows").BeginList().AddListItem().BeginStruct().AddMember("k").Int32(2).EndStruct()
            .EndList().Build()
            .Build();

        for (TStringBuf operation : {"INSERT", "INSERT OR REVERT", "UPSERT", "REPLACE"}) {
            fixture.Check(TStringBuilder() << operation << " INTO TestTable (k) SELECT k FROM Source RETURNING v;", "[]");
            const TString query = TStringBuilder() << "DECLARE $rows AS List<Struct<k:Int32>>; "
                << operation << " INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k < 0 RETURNING v;";
            auto result = fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }

        fixture.Check("UPDATE TestTable SET other = 5 WHERE k = 99 RETURNING v;", "[]");
        fixture.Check("UPDATE TestTable ON (k, other) VALUES (99, 5) RETURNING v;", "[]");
        fixture.Check("DELETE FROM TestTable WHERE k = 99 RETURNING v;", "[]");
        fixture.Check("DELETE FROM TestTable ON (k) VALUES (99) RETURNING v;", "[]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[7u]]]");
    }

    Y_UNIT_TEST(InsertConflictAndRevertPreserveStoredDefaults) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 11, 7ul);");

        fixture.RejectsWithStatus("INSERT INTO TestTable (k, other) VALUES (2, 20), (2, 21);",
            EStatus::PRECONDITION_FAILED);
        fixture.RejectsWithStatus("INSERT INTO TestTable (k, other) VALUES (2, 20), (1, 21);",
            EStatus::PRECONDITION_FAILED);
        fixture.Exec("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (3, 30), (2, 21);");
        fixture.Exec("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (1, 21);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[11];[7u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v;", "[[1;[11];[7u]]]");

        fixture.CheckUnordered("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (3, 30) RETURNING k, v;",
            "[[2;[42u]];[3;[42u]]]");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (4, 40), (4, 41);");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1;[11];[7u]];[2;[20];[42u]];[3;[30];[42u]];[4;[41];[42u]]]");
    }

    Y_UNIT_TEST_TWIN(InteractiveTransactionSeesDefaultAndIndex, Commit) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 10, 7ul);");
        auto& session = fixture.QuerySession();

        auto insert = session.ExecuteQuery("INSERT INTO TestTable (k, other) VALUES (2, 20) RETURNING k, v;",
            TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        CompareYson("[[2;[42u]]]", FormatResultSetYson(insert.GetResultSet(0)));
        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        // The following partial UPSERT must read this uncommitted override.
        auto update = session.ExecuteQuery("UPDATE TestTable SET v = 8ul WHERE k = 2 AND v = 42ul;",
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
        auto upsert = session.ExecuteQuery(R"(
            UPSERT INTO TestTable (k, other) VALUES (1, 11), (2, 21) RETURNING k, other, v;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        const TString expected = "[[1;[11];[7u]];[2;[21];[8u]]]";
        CompareYsonUnordered(expected, FormatResultSetYson(upsert.GetResultSet(0)));

        const std::string indexQuery = "SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;";
        auto read = session.ExecuteQuery(indexQuery, TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(read.GetResultSet(0)));

        if (Commit) {
            auto result = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            auto result = tx->Rollback().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const TString stored = Commit ? expected : "[[1;[10];[7u]]]";
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", stored);
        fixture.Check(indexQuery, stored);
    }

    Y_UNIT_TEST(InteractiveTransactionConflictRollsBackDefaultAndIndex) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        auto& session = fixture.QuerySession();
        auto insert = session.ExecuteQuery("INSERT INTO TestTable (k) VALUES (1);",
            TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto conflict = session.ExecuteQuery("INSERT INTO TestTable (k) VALUES (2), (1);",
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::NOT_FOUND, commit.GetIssues().ToString());
        fixture.Check("SELECT k, v FROM TestTable;", "[]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v;", "[]");
    }

    Y_UNIT_TEST(MultipleStatementsReturnDefaultsAndRollBackOnConflict) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto& session = fixture.QuerySession();
        auto result = session.ExecuteQuery(R"(
            INSERT INTO TestTable (k, other) VALUES (1, 10), (2, 20) RETURNING *;
            UPDATE TestTable SET other = 11 WHERE k = 1 AND v = 42ul RETURNING v, k;
            DELETE FROM TestTable WHERE k = 2 AND v = 42ul RETURNING k;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3u);
        CompareYsonUnordered("[[1;[10];[42u]];[2;[20];[42u]]]", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson("[[[42u];1]]", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson("[[2]]", FormatResultSetYson(result.GetResultSet(2)));

        auto conflict = session.ExecuteQuery(R"(
            INSERT INTO TestTable (k) VALUES (3);
            INSERT INTO TestTable (k) VALUES (1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[11];[42u]]]");
    }

    Y_UNIT_TEST(MultiShardBatchWritesMaintainDefaultedIndex) {
        auto appConfig = DefaultExprAppConfig();
        auto* batch = appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings();
        batch->SetMaxBatchSize(2);
        batch->SetPartitionExecutionLimit(2);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint32 NOT NULL,
                other Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v) COVER (other)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1u, 10, 7ul);");
        fixture.Exec(R"(
            INSERT INTO TestTable (k, other) VALUES (1500000000u, 20), (3500000000u, 30);
        )");
        fixture.CheckUnordered(R"(
            UPSERT INTO TestTable (k, other) VALUES (1u, 11), (2500000000u, 21), (3500000000u, 31)
            RETURNING k, other, v;
        )", "[[1u;11;7u];[2500000000u;21;42u];[3500000000u;31;42u]]");

        fixture.RejectsWithStatus(R"(
            INSERT INTO TestTable (k, other) VALUES (2u, 12), (3500000000u, 32);
        )", EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable WHERE k = 2u;", "[]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE other = 12;", "[]");

        fixture.Exec("BATCH UPDATE TestTable SET v = 43ul WHERE v = 42ul;");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1u;11;7u];[1500000000u;20;43u];[2500000000u;21;43u];[3500000000u;31;43u]]");
        fixture.Exec("BATCH DELETE FROM TestTable WHERE v = 43ul AND other > 20;");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1u;11;7u];[1500000000u;20;43u]]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1u;11;7u];[1500000000u;20;43u]]");
    }

    Y_UNIT_TEST(RandomDefaultWithConstantDependencyIsSharedBetweenRows) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("r Uint64 NOT NULL DEFAULT RandomNumber(1)"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2), (3), (4), (5), (6), (7), (8);");
        UNIT_ASSERT_VALUES_EQUAL(fixture.QueryUint64("SELECT COUNT(*) FROM TestTable;"), 8u);

        UNIT_ASSERT_VALUES_EQUAL_C(fixture.QueryUint64("SELECT COUNT(DISTINCT r) FROM TestTable;"), 1u,
            "RandomNumber() DEFAULT with a constant dependency must be shared between rows of one write");
    }

    Y_UNIT_TEST(UpdateAndDeleteRequireDefaultedKey) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint64 NOT NULL DEFAULT Ensure(RandomNumber(1), false, "key default evaluated"),
                v Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1ul, 1);");

        fixture.Rejects("UPDATE TestTable ON (v) VALUES (2);", "Key columns are not specified");
        fixture.Rejects("DELETE FROM TestTable ON (v) VALUES (2);", "Key columns are not specified");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1u;[1]]]");
    }

    Y_UNIT_TEST(UpsertUsesMaterializedNullDefaultKey) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 DEFAULT CAST("invalid" || CAST(RandomNumber(1) AS String) AS Int32),
                other Int32,
                v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "k"),
            R"(CAST("invalid" || CAST(RandomNumber(1) AS String) AS Int32))");
        fixture.Check("UPSERT INTO TestTable (other) VALUES (5) RETURNING k, other, v;", "[[#;[5];[42u]]]");
        fixture.Exec(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 50ul;
        )");
        fixture.Check("UPSERT INTO TestTable (other) VALUES (6) RETURNING k, other, v;", "[[#;[6];[42u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[#;[6];[42u]]]");
    }

    Y_UNIT_TEST(PrimaryKeyDefaultCollision) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL DEFAULT )" << DefaultExpr << R"(,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (v) VALUES (1);");
        fixture.Rejects("INSERT INTO TestTable (v) VALUES (2);", "Conflict with existing key");

        fixture.Exec("UPSERT INTO TestTable (v) VALUES (3);");
        fixture.Check("SELECT id, v FROM TestTable;", "[[42u;[3]]]");
    }

    Y_UNIT_TEST(Returning) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const TString returned = fixture.QueryYson("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;");
        CompareYson("[[1;[42u]]]", returned);
        CompareYson(returned, fixture.QueryYson("SELECT k, v FROM TestTable;"));
    }

    Y_UNIT_TEST(RandomDefaultConsistentWithReturningAndIndex) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        constexpr TStringBuf columns = "k, a, v, g";
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1),
                g Uint64 GENERATED ALWAYS AS (v) STORED,
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v, g)
            ) WITH (PARTITION_AT_KEYS = (2));
        )");

        const auto checkWrite = [&](const std::string& write, const TString& predicate,
                                    const TString& expectedKeys, ui64 expectedCount) {
            const TString returned = fixture.QueryYson(TStringBuilder() << write << " RETURNING " << columns << ";");
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE " << predicate << ";"));
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT " << columns << " FROM TestTable VIEW idx_a WHERE " << predicate << ";"));
            fixture.Check(TStringBuilder() << "SELECT k, a FROM TestTable WHERE " << predicate << " ORDER BY k;", expectedKeys);
            fixture.Check(TStringBuilder() << "SELECT COUNT(*) FROM TestTable WHERE " << predicate << " AND v = g;",
                TStringBuilder() << "[[" << expectedCount << "u]]");
        };
        checkWrite("INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30)",
            "k <= 3", "[[1;10];[2;20];[3;30]]", 3);
        checkWrite("INSERT INTO TestTable (k, a) SELECT k + 10, a + 100 FROM TestTable",
            "k > 10", "[[11;110];[12;120];[13;130]]", 3);
        const TString existing = fixture.QueryYson(TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE k = 1;");
        checkWrite("UPSERT INTO TestTable (k, a) VALUES (1, 10), (4, 40)",
            "k IN (1, 4)", "[[1;10];[4;40]]", 2);
        CompareYson(existing, fixture.QueryYson(TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE k = 1;"));
        fixture.Check("SELECT k, a FROM TestTable ORDER BY k;",
            "[[1;10];[2;20];[3;30];[4;40];[11;110];[12;120];[13;130]]");
    }

    Y_UNIT_TEST(RandomDefaultKeyConsistentWithReturningAndIndex) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint64 NOT NULL DEFAULT RandomNumber(1),
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(2),
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v)
            );
        )");
        // A constant dependency may be shared between rows, so use a single-row write.
        const TString returned = fixture.QueryYson("INSERT INTO TestTable (a) VALUES (10) RETURNING k, a, v;");
        CompareYson(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable;"));
        CompareYson(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable VIEW idx_a;"));
        fixture.Check("SELECT a FROM TestTable;", "[[10]]");
    }

    Y_UNIT_TEST(StatementTimestampDefaultAcrossShards) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE Source (
                k Int32 NOT NULL,
                PRIMARY KEY (k)
            ) WITH (PARTITION_AT_KEYS = (3, 5));
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                actual Timestamp NOT NULL DEFAULT CurrentUtcTimestamp(),
                expected Timestamp NOT NULL,
                PRIMARY KEY (k)
            ) WITH (PARTITION_AT_KEYS = (3, 5));
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "actual"), "CurrentUtcTimestamp()");
        fixture.Exec("INSERT INTO Source (k) VALUES (1), (2), (3), (4), (5), (6);");
        fixture.Exec("INSERT INTO TestTable (k, expected) SELECT k, CurrentUtcTimestamp() FROM Source;");
        fixture.Check("SELECT COUNT(*), COUNT(DISTINCT actual), COUNT_IF(actual = expected) FROM TestTable;", "[[6u;1u;6u]]");
    }

    Y_UNIT_TEST(RandomDefaultAcrossShardsWithUniqueIndex) {
        TScopedEpochRandomProvider random;
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1),
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL UNIQUE SYNC ON (v)
            ) WITH (PARTITION_AT_KEYS = (2, 3));
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "RandomNumber(1)");

        TStringBuilder expected;
        expected << "[";
        // A constant random dependency can be shared across rows in one statement.
        for (i32 key = 1; key <= 3; ++key) {
            random.SetEpoch(key);
            auto query = TStringBuilder() << "INSERT INTO TestTable (k) VALUES (" << key << ") RETURNING k, v;";
            auto result = fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetInt32(), key);
            const ui64 value = parser.ColumnParser("v").GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(value >> 60, static_cast<ui64>(key));
            UNIT_ASSERT(!parser.TryNextRow());
            if (key != 1) {
                expected << ";";
            }
            expected << "[" << key << ";" << value << "u]";
        }
        expected << "]";
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", expected);
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", expected);
    }

    Y_UNIT_TEST(CachedRandomDefaultConsistentWithReturningAndIndex) {
        TScopedEpochRandomProvider random;
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1),
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "RandomNumber(1)");

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = session.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k, a)
            VALUES ($k, $k * 10), ($k + 1, $k * 10 + 1)
            RETURNING k, a, v;
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        NYdb::NTable::TExecDataQuerySettings settings;
        settings.KeepInQueryCache(true).CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        for (i32 key : {1, 3}) {
            const ui64 epoch = key == 1 ? 1 : 2;
            random.SetEpoch(epoch);
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats());
            if (key == 3) {
                UNIT_ASSERT(NYdb::TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache());
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 2u);
            TResultSetParser draws(result.GetResultSet(0));
            while (draws.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(draws.ColumnParser("v").GetUint64() >> 60, epoch);
            }
            const TString returned = FormatResultSetYson(result.GetResultSet(0));
            const TString predicate = TStringBuilder() << "k IN (" << key << ", " << key + 1 << ")";
            // Each execution's actual draws must reach every consumer unchanged.
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT k, a, v FROM TestTable WHERE " << predicate << ";"));
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT k, a, v FROM TestTable VIEW idx_a WHERE " << predicate << ";"));
        }
        fixture.Check("SELECT k, a FROM TestTable ORDER BY k;", "[[1;10];[2;11];[3;30];[4;31]]");
        CompareYsonUnordered(fixture.QueryYson("SELECT k, a, v FROM TestTable;"),
            fixture.QueryYson("SELECT k, a, v FROM TestTable VIEW idx_a;"));
    }

    Y_UNIT_TEST(ExplicitNullBypassesFailingDefault) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"),
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            R"(Ensure(RandomNumber(1), false, "default evaluated"))");
        fixture.Check("INSERT INTO TestTable (k, v) VALUES (1, NULL) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v;", "[[1;#]]");
        fixture.Check("UPSERT INTO TestTable (k, v) VALUES (2, NULL) RETURNING k, v;", "[[2;#]]");
        for (const auto* write : {
            "UPSERT INTO TestTable (k, v) VALUES (1, NULL)",
            "REPLACE INTO TestTable (k, v) VALUES (1, NULL)",
            "UPDATE TestTable SET v = NULL WHERE k = 1",
            "UPDATE TestTable ON (k, v) VALUES (1, NULL)"})
        {
            fixture.Exec("UPSERT INTO TestTable (k, v) VALUES (1, 7ul);");
            fixture.Check(TStringBuilder() << write << " RETURNING k, v;", "[[1;#]]");
            fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;#]]");
            fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;#];[2;#]]");
        }
    }

    Y_UNIT_TEST(WithSecondaryIndex) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL ON (v)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[1]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 7ul;", "[[2]]");
    }

    Y_UNIT_TEST(SequenceExhaustionRollsBackDefaultedBatch) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Serial,
                other Int32 NOT NULL,
                v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k),
                INDEX idx_other GLOBAL SYNC ON (other) COVER (v)
            );
        )");
        fixture.Check("INSERT INTO TestTable (other) VALUES (10) RETURNING k, other, v;", "[[1;10;[42u]]]");
        // Serial uses an Int32 sequence. Restart at its last two available values.
        fixture.Exec("ALTER SEQUENCE `/Root/TestTable/_serial_column_k` RESTART WITH 2147483646;");

        fixture.Rejects("INSERT INTO TestTable (other) VALUES (20), (30), (40);", "doesn't have any more values available");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1;10;[42u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_other ORDER BY k;", "[[1;10;[42u]]]");
    }

    Y_UNIT_TEST(UniqueIndexRejectsDefaultCollisionsAtomically) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL UNIQUE SYNC ON (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul), (2, 8ul);");

        // The omitted values collide with each other, so neither row may be inserted.
        fixture.RejectsWithStatus("INSERT INTO TestTable (k) VALUES (3), (4);", EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;7u];[2;8u]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;7u];[2;8u]]");

        fixture.Check("INSERT INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;42u]]");
        fixture.RejectsWithStatus("INSERT INTO TestTable (k) VALUES (4);", EStatus::PRECONDITION_FAILED);
        fixture.RejectsWithStatus(
            "UPDATE TestTable SET v = CASE WHEN k = 1 THEN 42ul ELSE 99ul END WHERE k < 3;",
            EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;7u];[2;8u];[3;42u]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;7u];[2;8u];[3;42u]]");
    }

    Y_UNIT_TEST(NullableDefaultTracksUniqueIndexTransitions) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT CAST("invalid" || CAST(RandomNumber(1) AS String) AS Uint64),
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL UNIQUE SYNC ON (v)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v IS NULL ORDER BY k;", "[[1;#];[2;#]]");
        fixture.Exec("UPDATE TestTable SET v = 42ul WHERE k = 1;");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[1;[42u]]]");
        fixture.RejectsWithStatus("INSERT INTO TestTable (k, v) VALUES (3, 42ul);", EStatus::PRECONDITION_FAILED);

        fixture.Check("REPLACE INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[]");
        fixture.Exec("UPDATE TestTable SET v = 42ul WHERE k = 2;");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[2;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]]]");
    }

    Y_UNIT_TEST(CompositeAndCoveringIndexesUseStoredDefaultValues) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                tenant Int32 NOT NULL,
                k Int32 NOT NULL,
                bucket Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (tenant, k),
                INDEX idx_async GLOBAL ASYNC ON (bucket) COVER (v)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (tenant, k, bucket) VALUES (1, 1, 10);");
        fixture.Exec("INSERT INTO TestTable (tenant, k, bucket, v) VALUES (2, 1, 20, 7ul);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 43ul;");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_pair GLOBAL SYNC ON (v, bucket);");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_cover GLOBAL SYNC ON (bucket) COVER (v);");

        const auto checkIndexes = [&](const TString& expected) {
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable ORDER BY tenant, k;", expected);
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_pair ORDER BY tenant, k;", expected);
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_cover ORDER BY tenant, k;", expected);
            fixture.CheckEventually("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_async ORDER BY tenant, k;", expected);
        };
        checkIndexes("[[1;1;10;42u];[2;1;20;7u]]");
        fixture.Exec("UPSERT INTO TestTable (tenant, k, bucket) VALUES (1, 1, 11), (2, 2, 22);");
        fixture.Exec("REPLACE INTO TestTable (tenant, k, bucket) VALUES (2, 1, 21);");
        checkIndexes("[[1;1;11;42u];[2;1;21;43u];[2;2;22;43u]]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_pair WHERE v = 7ul;", "[]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_cover WHERE bucket IN (10, 20);", "[]");
    }

    Y_UNIT_TEST_TWIN(FulltextIndexUsesDefaultValueWithReturning, Compact) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableFulltextIndex(true);
        appConfig.MutableFeatureFlags()->SetEnableCompactFulltextIndex(Compact);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                text String NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, "Cats love naps.", "other"),
                PRIMARY KEY (k),
                INDEX idx_text GLOBAL USING fulltext_plain ON (text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "text"),
            R"(IF(RandomNumber(1) * 0ul = 0ul, "Cats love naps.", "other"))");
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, text;", R"([[1;"Cats love naps."]])");
        fixture.Check(R"(SELECT k, text FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "cats");)",
            R"([[1;"Cats love naps."]])");
        fixture.Check(R"(UPDATE TestTable SET text = "Dogs bark." WHERE k = 1 RETURNING k, text;)",
            R"([[1;"Dogs bark."]])");
        fixture.Check(R"(SELECT k FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "cats");)", "[]");
        fixture.Check(R"(SELECT k FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "dogs");)", "[[1]]");
    }

    Y_UNIT_TEST(SetDefaultExpr) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, v;", "[[2;[42u]]]");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (3, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]];[3;[7u]]]");

        const TString replacement = "RandomNumber(1) * 0ul + 9ul";
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << replacement << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), replacement);
        UNIT_ASSERT_STRING_CONTAINS(fixture.ShowCreateTable("/Root/TestTable"), TStringBuilder() << "DEFAULT " << replacement);
        fixture.Check("INSERT INTO TestTable (k) VALUES (4) RETURNING k, v;", "[[4;[9u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]];[3;[7u]];[4;[9u]]]");
    }

    Y_UNIT_TEST(SetAndDropDefaultsInOneAlter) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                b Uint64,
                c Uint64 DEFAULT RandomNumber(1) * 0ul + 9ul,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Rejects(R"(
            ALTER TABLE TestTable
                ALTER COLUMN a SET DEFAULT RandomNumber(1) * 0ul + 50ul,
                ALTER COLUMN c SET DEFAULT AsList(RandomNumber(1));
        )", "type mismatch");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "a"), DefaultExpr);

        fixture.Exec(R"(
            ALTER TABLE TestTable
                ALTER COLUMN a SET DEFAULT RandomNumber(1) * 0ul + 50ul,
                ALTER COLUMN b SET DEFAULT 8ul,
                ALTER COLUMN c DROP DEFAULT;
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "a"), "RandomNumber(1) * 0ul + 50ul");
        const auto literal = fixture.ColumnDesc("/Root/TestTable", "b");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(literal.GetDefaultFromLiteral().value().uint64_value(), 8u);
        const auto dropped = fixture.ColumnDesc("/Root/TestTable", "c");
        UNIT_ASSERT_C(dropped.GetDefaultValueCase() == NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET,
            dropped.ShortDebugString());
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Check("SELECT k, a, b, c FROM TestTable ORDER BY k;", "[[1;[42u];#;[9u]];[2;[50u];[8u];#]]");
    }

    Y_UNIT_TEST(NotNullColumnRequiresValueAfterDropDefault) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 NOT NULL"));
        fixture.Rejects("INSERT INTO TestTable (k) VALUES (1);", "Missing not null column in input: v");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;42u]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        for (TStringBuf operation : {"INSERT", "REPLACE", "UPSERT"}) {
            fixture.Rejects(TStringBuilder() << operation << " INTO TestTable (k) VALUES (2);",
                "Missing not null column in input: v");
        }
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 9ul;");
        fixture.Check("REPLACE INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;9u]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;42u];[2;7u];[3;9u]]");
    }

    Y_UNIT_TEST_TWIN(SetCoercedDefaultExpr, FeatureEnabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = FeatureEnabled});
        fixture.Exec(DefaultExprDDL("v Int64"));

        const TString query = TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";";
        if (FeatureEnabled) {
            fixture.Exec(query);
            AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
            fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42]]]");
            fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");
        } else {
            fixture.Rejects(query, "DEFAULT expressions are disabled");
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(!column.HasDefaultFromExpression() && !column.HasDefaultFromLiteral(),
                column.ShortDebugString());
        }
    }

    Y_UNIT_TEST(SetDefaultExprOnPrimaryKeyColumn) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN id SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "id"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (v) VALUES (7) RETURNING id, v;", "[[42u;[7]]]");
        fixture.Exec("INSERT INTO TestTable (id, v) VALUES (8ul, 9);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN id DROP DEFAULT;");
        fixture.Rejects("UPSERT INTO TestTable (v) VALUES (11);", "Missing key column in input: id");
        fixture.Exec("UPSERT INTO TestTable (id, v) VALUES (10ul, 11);");
        fixture.Check("SELECT id, v FROM TestTable ORDER BY id;", "[[8u;[9]];[10u;[11]];[42u;[7]]]");
    }

    Y_UNIT_TEST(SetDefaultExprTypeMismatchPreservesDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN missing SET DEFAULT " << DefaultExpr << ";",
            "Column: \"missing\" does not exist");
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT AsList(RandomNumber(1));", "type mismatch");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprOnNotNullColumnRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 NOT NULL DEFAULT 7ul"));

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT IF(RandomNumber(1) = 0ul, NULL, 1ul);",
            "nullable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().uint64_value(), 7u);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;7u]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;7u]]");
    }

    Y_UNIT_TEST(SetDefaultExprWithSqlBindingRejected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects(R"(
            $value = RandomNumber(1);
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT $value;
        )", "DEFAULT");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetDefaultExprSubqueryRejected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT ListLength((SELECT v FROM TestTable LIMIT 1));",
            "subquery");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprInScript) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Bool DEFAULT true"));
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL);
            COMMIT;
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0u);
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)");

        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprInScriptOnNotNullColumnRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Bool NOT NULL DEFAULT true"));
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL);
            COMMIT;
        )").ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "nullable DEFAULT accepted for a NOT NULL column");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "nullable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.ShortDebugString());
        UNIT_ASSERT(column.GetDefaultFromLiteral().value().bool_value());
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;%true]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(SetAndDropDefaultExprAfterRestartWithIndex) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 DEFAULT 7ul"));
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_v GLOBAL SYNC ON (v);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.RestartSchemeShard("/Root/TestTable");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, v;", "[[2;[42u]]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 7ul;", "[[1]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[2]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.RestartSchemeShard("/Root/TestTable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.GetDefaultValueCase() == NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET,
            column.ShortDebugString());
        fixture.Check("INSERT INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;#]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[7u]];[2;[42u]];[3;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;[7u]];[2;[42u]];[3;#]]");
    }

    Y_UNIT_TEST(SetAndDropExpressionDefaultWithAsyncIndexes) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL, other Int32, v Uint64, PRIMARY KEY (k),
                INDEX idx_v GLOBAL ASYNC ON (v) COVER (other),
                INDEX idx_other GLOBAL ASYNC ON (other) COVER (v)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 11), (2, 20);");

        const auto check = [&](const TString& expected) {
            fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", expected);
            fixture.CheckEventually("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;", expected);
            fixture.CheckEventually("SELECT k, other, v FROM TestTable VIEW idx_other ORDER BY k;", expected);
        };
        check("[[1;[11];#];[2;[20];[42u]]]");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (2, 21), (3, 30);");
        check("[[1;[11];#];[2;[21];[42u]];[3;[30];#]]");
        fixture.Exec("REPLACE INTO TestTable (k, other) VALUES (2, 22);");
        check("[[1;[11];#];[2;[22];#];[3;[30];#]]");
    }

    Y_UNIT_TEST(OnlineIndexBuildUsesStoredDefaults) {
        TTestFixture fixture(TKikimrSettings(DefaultExprAppConfig()).SetWithSampleTables(false).SetUseRealThreads(false));
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (2, 20, 7ul), (3, 30, NULL);");
        fixture.Exec(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT Ensure(RandomNumber(1), false, "default ran");
        )");

        auto& kikimr = fixture.Runner();
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.GetQueryClient();
        NActors::TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> backfill(runtime);
        Y_DEFER { backfill.Stop().Unblock(); };
        auto build = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE TestTable ADD INDEX idx_v GLOBAL SYNC ON (v) COVER (other);",
                TTxControl::NoTx());
        });

        runtime.WaitFor("index backfill request", [&] { return !backfill.empty(); });
        // The backfill snapshot predates this write, which must reach the index through live maintenance.
        UNIT_ASSERT(backfill.front()->Get()->Record.HasSnapshotStep());
        UNIT_ASSERT(backfill.front()->Get()->Record.HasSnapshotTxId());
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (4, 40, 9ul);");
        UNIT_ASSERT_C(!build.HasValue() && !build.HasException(), "index build finished with backfill blocked");

        backfill.Stop().Unblock();
        const auto result = runtime.WaitFuture(build);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        fixture.Rejects("INSERT INTO TestTable (k, other) VALUES (5, 50);", "default ran");
        const TString rows = "[[1;[10];[42u]];[2;[20];[7u]];[3;[30];#];[4;[40];[9u]]]";
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", rows);
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;", rows);
    }

    Y_UNIT_TEST(ShardOperationsPreserveStoredDefaults) {
        TTestFixture fixture(TKikimrSettings(DefaultExprAppConfig()).SetWithSampleTables(false).SetUseRealThreads(false));
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k),
                INDEX idx_other GLOBAL SYNC ON (other) COVER (v)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (2, 20, NULL), (3, 30, 7ul);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 43ul;");

        const TString historical = "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]]]";
        const auto checkAfterMaintenance = [&](i32 key, const TString& expected) {
            fixture.Check("SELECT k, other, v FROM TestTable WHERE k <= 3 ORDER BY k;", historical);
            fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_other WHERE k <= 3 ORDER BY k;", historical);
            fixture.Check(TStringBuilder() << "INSERT INTO TestTable (k, other) VALUES (" << key << ", " << key * 10
                << ") RETURNING v;", "[[[43u]]]");
            fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", expected);
            fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_other ORDER BY k;", expected);
        };

        auto& server = fixture.Runner().GetTestServer();
        auto& runtime = *server.GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        const TString tablePath = "/Root/TestTable";
        SetSplitMergePartCountLimit(&runtime, -1);

        const auto description = DescribeTable(&server, sender, tablePath);
        const auto& self = description.GetPathDescription().GetSelf();
        const auto compactShard = [&](ui64 shard) {
            auto request = MakeHolder<TEvDataShard::TEvCompactTable>(self.GetSchemeshardId(), self.GetPathId());
            request->Record.SetCompactBorrowed(true);
            request->Record.SetCompactSinglePartedShards(true);
            runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            const auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK,
                result->Get()->Record.ShortDebugString());
        };

        const auto changePartitions = [&](const TVector<ui64>& sources, std::optional<i32> splitBoundary) {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetExecTimeoutPeriod(Max<ui64>());
            auto& modify = *request->Record.MutableTransaction()->MutableModifyScheme();
            modify.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
            auto& partitions = *modify.MutableSplitMergeTablePartitions();
            partitions.SetTablePath(tablePath);
            for (ui64 tabletId : sources) {
                partitions.AddSourceTabletId(tabletId);
            }
            if (splitBoundary) {
                partitions.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetInt32(*splitBoundary);
            }

            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
            const auto started = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
            const auto& status = started->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress,
                status.ShortDebugString());

            auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(status.GetTxId());
            runtime.SendToPipe(status.GetSchemeShardTabletId(), sender, notify.Release(), 0, GetPipeConfigWithRetries());
            const auto done = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(done->Get()->Record.GetTxId(), status.GetTxId());
        };

        const auto initialShards = GetTableShards(&server, sender, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(initialShards.size(), 1u);
        changePartitions(initialShards, 3);
        const auto splitShards = GetTableShards(&server, sender, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(splitShards.size(), 2u);
        checkAfterMaintenance(4, "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]];[4;[40];[43u]]]");

        // Both split shards initially borrow parts from the original shard. Materialize them before merging.
        for (ui64 shard : splitShards) {
            compactShard(shard);
        }
        changePartitions(splitShards, std::nullopt);
        const auto mergedShards = GetTableShards(&server, sender, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(mergedShards.size(), 1u);
        checkAfterMaintenance(5, "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]];[4;[40];[43u]];[5;[50];[43u]]]");

        const ui64 shard = mergedShards.front();
        const auto oldActor = ResolveTablet(runtime, shard);
        RebootTablet(runtime, shard, sender);
        UNIT_ASSERT_C(ResolveTablet(runtime, shard) != oldActor, "expected a new DataShard actor after restart");
        checkAfterMaintenance(6, "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]];[4;[40];[43u]];[5;[50];[43u]];[6;[60];[43u]]]");

        compactShard(shard);
        checkAfterMaintenance(7, "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]];[4;[40];[43u]];[5;[50];[43u]];[6;[60];[43u]];[7;[70];[43u]]]");
    }

    Y_UNIT_TEST(TtlUsesStoredTimestampsAfterDefaultChange) {
        TTestFixture fixture(TKikimrSettings(DefaultExprAppConfig()).SetWithSampleTables(false).SetUseRealThreads(false));
        auto& server = fixture.Runner().GetTestServer();
        auto& runtime = *server.GetRuntime();
        runtime.SetFastSimulatedTime(false);

        auto& allowErase = runtime.GetAppData().Icb->SchemeShardControls.AllowConditionalEraseOperations;
        TControlBoard::SetValue(false, allowErase);
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));

        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                ts Timestamp DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Timestamp("1970-01-01T00:00:01Z"), Timestamp("1970-01-01T00:00:00Z")),
                PRIMARY KEY (k)
            ) WITH (TTL = Interval("PT1M") ON ts);
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec(R"(
            INSERT INTO TestTable (k, ts) VALUES (2, Timestamp("1970-01-01T01:00:00Z")), (3, NULL);
        )");
        fixture.Exec(R"(
            ALTER TABLE TestTable ALTER COLUMN ts SET DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                Timestamp("1970-01-01T02:00:00Z"), Timestamp("1970-01-01T00:00:00Z"));
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (4);");

        const char* rows = "SELECT k, ts FROM TestTable ORDER BY k;";
        fixture.Check(rows, "[[1;[1000000u]];[2;[3600000000u]];[3;#];[4;[7200000000u]]]");

        const auto shards = GetTableShards(&server, runtime.AllocateEdgeActor(), "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        using TEraseResponse = TEvDataShard::TEvConditionalEraseRowsResponse;
        TDispatchOptions options;
        options.FinalEvents.emplace_back([tabletId = shards.front()](IEventHandle& event) {
            if (event.GetTypeRewrite() != TEraseResponse::EventType) {
                return false;
            }
            const auto& response = event.Get<TEraseResponse>()->Record;
            if (response.GetTabletID() != tabletId) {
                return false;
            }
            const auto status = response.GetStatus();
            using TStatus = TEraseResponse::ProtoRecordType;
            if (status == TStatus::ACCEPTED || status == TStatus::PARTIAL) {
                return false;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(status, TStatus::OK, response.ShortDebugString());
            return true;
        });

        TControlBoard::SetValue(true, allowErase);
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        UNIT_ASSERT_C(runtime.DispatchEvents(options, TDuration::Minutes(5)), "conditional erase did not finish");
        fixture.Check(rows, "[[2;[3600000000u]];[3;#];[4;[7200000000u]]]");
    }

    Y_UNIT_TEST(ChangefeedImagesUseStoredValuesAfterDefaultChange) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 42, 0)"));
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (2, 20, 7), (3, 30, NULL);");

        fixture.Exec("ALTER TABLE TestTable ADD CHANGEFEED feed WITH (MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON');");
        fixture.Exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER test_consumer;");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 50, 0);");
        fixture.Exec("UPDATE TestTable SET other = other + 1 WHERE k IN (1, 2, 3);");
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (4, 40);");

        THashMap<i64, NJson::TJsonValue> messages;
        for (auto& message : fixture.ReadChangefeedMessages(4)) {
            const auto key = message["key"][0].GetInteger();
            UNIT_ASSERT_C(messages.emplace(key, std::move(message)).second, "duplicate CDC key " << key);
        }

        for (const char* image : {"oldImage", "newImage"}) {
            UNIT_ASSERT_VALUES_EQUAL_C(messages.at(1)[image]["v"].GetInteger(), 42, image);
            UNIT_ASSERT_VALUES_EQUAL_C(messages.at(2)[image]["v"].GetInteger(), 7, image);
            const auto& nullImage = messages.at(3)[image];
            UNIT_ASSERT_C(nullImage.Has("v") && nullImage["v"].IsNull(), image);
        }
        UNIT_ASSERT(!messages.at(4).Has("oldImage"));
        UNIT_ASSERT_VALUES_EQUAL(messages.at(4)["newImage"]["v"].GetInteger(), 50);
    }

    Y_UNIT_TEST(ConcurrentDefaultChangesAndWritesAcrossNodes) {
        TTestFixture fixture(TKikimrSettings(DefaultExprAppConfig()).SetWithSampleTables(false)
            .SetNodeCount(2).SetUseRealThreads(false));
        auto& kikimr = fixture.Runner();
        auto& server = kikimr.GetTestServer();
        auto& runtime = *server.GetRuntime();
        runtime.SetFastSimulatedTime(false);

        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32 NOT NULL,
                v Int32 DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 42, 0),
                PRIMARY KEY (k),
                INDEX idx_other GLOBAL SYNC ON (other) COVER (v)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ADD CHANGEFEED feed WITH (MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON');");
        fixture.Exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER test_consumer;");

        TDefaultChangeRaceSessions sessions(kikimr);
        auto [ddl, writer] = sessions.Connect();

        auto prepareResult = kikimr.RunCall([&] {
            return writer.PrepareDataQuery(R"(
                DECLARE $k AS Int32;
                INSERT INTO TestTable (k, other) VALUES ($k, $k * 10) RETURNING k, other, v;
            )").ExtractValueSync();
        });
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        const auto tx = NYdb::NTable::TTxControl::BeginTx().CommitTx();
        std::map<std::pair<i32, i32>, std::optional<i32>> expectedCdc;
        const auto checkReturning = [&](const auto& result, const TString& rows) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYsonUnordered(rows, FormatResultSetYson(result.GetResultSet(0)));
            TResultSetParser parser(result.GetResultSet(0));
            while (parser.TryNextRow()) {
                const auto k = parser.ColumnParser("k").GetInt32();
                const auto other = parser.ColumnParser("other").GetInt32();
                UNIT_ASSERT(expectedCdc.emplace(std::make_pair(k, other), parser.ColumnParser("v").GetOptionalInt32()).second);
            }
        };
        const auto checkPrepared = [&](i32 k, const TString& rows) {
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(k).Build().Build();
            checkReturning(runtime.WaitFuture(prepared.Execute(tx, params)), rows);
        };
        const auto checkWrite = [&](const TString& query, const TString& rows) {
            checkReturning(runtime.WaitFuture(writer.ExecuteDataQuery(query, tx)), rows);
        };
        const auto checkStored = [&](const TString& rows) {
            fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", rows);
            fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_other ORDER BY k;", rows);
        };

        checkPrepared(1, "[[1;10;[42]]]");
        checkWrite("INSERT INTO TestTable (k, other, v) VALUES (2, 20, NULL) RETURNING k, other, v;", "[[2;20;#]]");
        {
            // Keep the DDL pending at SchemeShard while the other node commits the UPSERT.
            NActors::TBlockEvents<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction> proposal(runtime, [](const auto& ev) {
                const auto& record = ev->Get()->Record;
                return record.TransactionSize() == 1 && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable;
            });
            Y_DEFER { proposal.Stop().Unblock(); };
            auto alter = ddl.ExecuteQuery(
                "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 50, 0);", TTxControl::NoTx());
            runtime.WaitFor("SET DEFAULT proposal", [&] { return !proposal.empty(); });
            checkWrite("UPSERT INTO TestTable (k, other) VALUES (1, 11), (2, 21), (3, 30) RETURNING k, other, v;",
                "[[1;11;[42]];[2;21;#];[3;30;[42]]]");
            UNIT_ASSERT(!alter.HasValue() && !alter.HasException());
            proposal.Stop().Unblock();
            const auto result = runtime.WaitFuture(alter);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        checkPrepared(4, "[[4;40;[50]]]");
        checkWrite("UPSERT INTO TestTable (k, other) VALUES (1, 12), (2, 22), (5, 50) RETURNING k, other, v;",
            "[[1;12;[42]];[2;22;#];[5;50;[50]]]");
        checkStored("[[1;12;[42]];[2;22;#];[3;30;[42]];[4;40;[50]];[5;50;[50]]]");
        {
            const TString upsert = "UPSERT INTO TestTable (k, other) VALUES (1, 13), (5, 51), (6, 60) RETURNING k, other, v;";
            NActors::TBlockEvents<TEvKqp::TEvQueryRequest> pendingWrite(runtime, [&](const auto& ev) {
                return ev->Get()->GetQuery() == upsert;
            });
            Y_DEFER { pendingWrite.Stop().Unblock(); };
            auto write = writer.ExecuteDataQuery(upsert, tx);
            runtime.WaitFor("UPSERT request", [&] { return !pendingWrite.empty(); });
            const auto result = runtime.WaitFuture(
                ddl.ExecuteQuery("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;", TTxControl::NoTx()));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(!write.HasValue() && !write.HasException());
            pendingWrite.Stop().Unblock();
            checkReturning(runtime.WaitFuture(write), "[[1;13;[42]];[5;51;[50]];[6;60;#]]");
        }

        checkPrepared(7, "[[7;70;#]]");
        checkWrite("UPSERT INTO TestTable (k, other) VALUES (2, 23), (8, 80) RETURNING k, other, v;", "[[2;23;#];[8;80;#]]");
        checkStored("[[1;13;[42]];[2;23;#];[3;30;[42]];[4;40;[50]];[5;51;[50]];[6;60;#];[7;70;#];[8;80;#]]");

        for (const auto& message : fixture.ReadChangefeedMessages(expectedCdc.size())) {
            const auto& image = message["newImage"];
            UNIT_ASSERT(image.Has("other") && image.Has("v"));
            const auto key = std::make_pair(static_cast<i32>(message["key"][0].GetInteger()),
                static_cast<i32>(image["other"].GetInteger()));
            const auto found = expectedCdc.find(key);
            UNIT_ASSERT_C(found != expectedCdc.end(), "unexpected CDC image for key " << key.first << " other " << key.second);
            if (found->second) {
                UNIT_ASSERT_VALUES_EQUAL(image["v"].GetInteger(), *found->second);
            } else {
                UNIT_ASSERT(image["v"].IsNull());
            }
            expectedCdc.erase(found);
        }
        UNIT_ASSERT(expectedCdc.empty());
    }

    Y_UNIT_TEST_TWIN(CompiledWritesRaceWithDefaultChangesAcrossNodes, PreparedInsert) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false)
            .SetNodeCount(2).SetUseRealThreads(false));
        auto& kikimr = fixture.Runner();
        auto& server = kikimr.GetTestServer();
        auto& runtime = *server.GetRuntime();
        runtime.SetFastSimulatedTime(false);

        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32 NOT NULL,
                v Int32 DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 42, 0),
                PRIMARY KEY (k),
                INDEX idx_other GLOBAL SYNC ON (other) COVER (v)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ADD CHANGEFEED feed WITH (MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON');");
        fixture.Exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER test_consumer;");

        TDefaultChangeRaceSessions sessions(kikimr);
        auto [ddl, writer] = sessions.Connect();
        const auto tx = NYdb::NTable::TTxControl::BeginTx().CommitTx();

        const TString tablePath = "/Root/TestTable";
        const auto sender = runtime.AllocateEdgeActor();
        const auto shards = GetTableShards(&server, sender, tablePath);
        const auto indexShards = GetTableShards(&server, sender, "/Root/TestTable/idx_other/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(indexShards.size(), 1u);

        const auto shardActor = ResolveTablet(runtime, shards.front());
        size_t enqueuedChanges = 0;
        using TEnqueueRecords = NChangeExchange::TEvChangeExchange::TEvEnqueueRecords;
        auto observeCdc = runtime.AddObserver<TEnqueueRecords>([&](auto& ev) {
            if (ev->Sender == shardActor) {
                enqueuedChanges += ev->Get()->Records.size();
            }
        });

        struct TRow {
            i32 Other;
            std::optional<i32> V;

            bool operator==(const TRow&) const = default;
        };
        using TRows = std::map<i32, TRow>;
        struct TChange {
            std::optional<TRow> Old;
            TRow New;
        };
        TRows stored;
        std::map<std::pair<i32, i32>, TChange> changes;

        const auto readRows = [](const auto& result) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
            TRows rows;
            TResultSetParser parser(result.GetResultSet(0));
            while (parser.TryNextRow()) {
                const auto k = parser.ColumnParser("k").GetInt32();
                const auto other = parser.ColumnParser("other").GetInt32();
                const auto value = parser.ColumnParser("v").GetOptionalInt32();
                UNIT_ASSERT_C(rows.emplace(k, TRow{other, value}).second, "duplicate row for key " << k);
            }
            return rows;
        };
        const auto checkStored = [&] {
            for (const char* query : {"SELECT k, other, v FROM TestTable ORDER BY k;",
                    "SELECT k, other, v FROM TestTable VIEW idx_other ORDER BY k;"}) {
                const auto result = runtime.WaitFuture(ddl.ExecuteQuery(query, TTxControl::NoTx()));
                UNIT_ASSERT_C(readRows(result) == stored, query << " returned " << FormatResultSetYson(result.GetResultSet(0)));
            }
            UNIT_ASSERT_VALUES_EQUAL_C(enqueuedChanges, changes.size(), "CDC records enqueued by DataShard");
        };
        const auto checkWrite = [&](const auto& result, const TRows& expectedRows) {
            const auto rows = readRows(result);
            UNIT_ASSERT_C(rows == expectedRows, "unexpected RETURNING rows: " << FormatResultSetYson(result.GetResultSet(0)));
            for (const auto& [k, row] : rows) {
                const auto old = stored.find(k);
                const auto image = old == stored.end() ? std::nullopt : std::optional<TRow>(old->second);
                UNIT_ASSERT(changes.emplace(std::make_pair(k, row.Other), TChange{image, row}).second);
                stored.insert_or_assign(k, row);
            }
            // A volatile transaction can return before DataShard enqueues its CDC records.
            runtime.WaitFor("committed CDC records", [&] { return enqueuedChanges >= changes.size(); }, TDuration::Seconds(30));
            checkStored();
        };
        const auto checkInvalidated = [&](const auto& result) {
            const auto& issues = result.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, issues.ToString());
            UNIT_ASSERT_C(HasIssue(issues, NYql::TIssuesIds::KIKIMR_QUERY_INVALIDATED), issues.ToString());
            UNIT_ASSERT_C(HasIssue(issues, NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH), issues.ToString());
            UNIT_ASSERT(result.GetResultSets().empty());
            checkStored();
        };

        const auto race = [&](const auto& executeWrite, const TString& alter) {
            const auto before = DescribeTable(&server, sender, tablePath);
            const auto& path = before.GetPathDescription().GetSelf();
            const auto version = before.GetPathDescription().GetTable().GetTableSchemaVersion();
            UNIT_ASSERT(version);

            // The write actor has materialized the rows and schema version in TEvWrite.
            // Hold both destination tablets at the pipe cache while the other node runs ALTER.
            using TWrite = NEvents::TDataEvents::TEvWrite;
            NActors::TBlockEvents<TEvPipeCache::TEvForward> blocked(runtime, [&](const auto& ev) {
                const auto& forward = *ev->Get();
                return ev->Sender.NodeId() == runtime.GetNodeId(1)
                    && (forward.TabletId == shards.front() || forward.TabletId == indexShards.front())
                    && forward.Ev->Type() == TWrite::EventType
                    && static_cast<const TWrite&>(*forward.Ev).Record.OperationsSize() > 0;
            });
            Y_DEFER { blocked.Stop().Unblock(); };
            const auto findBaseWrite = [&] {
                return std::find_if(blocked.begin(), blocked.end(), [&](const auto& ev) {
                    return ev->Get()->TabletId == shards.front();
                });
            };
            const auto timeout = TDuration::Seconds(30);
            auto write = executeWrite();
            runtime.WaitFor("compiled write to TestTable", [&] {
                return findBaseWrite() != blocked.end() || write.HasValue() || write.HasException();
            }, timeout);
            const auto base = findBaseWrite();
            UNIT_ASSERT_C(base != blocked.end(), "write completed without a physical write to TestTable");
            for (const auto& operation : static_cast<const TWrite&>(*(*base)->Get()->Ev).Record.GetOperations()) {
                UNIT_ASSERT(operation.HasPayloadIndex());
                UNIT_ASSERT_VALUES_EQUAL(operation.GetTableId().GetOwnerId(), path.GetSchemeshardId());
                UNIT_ASSERT_VALUES_EQUAL(operation.GetTableId().GetTableId(), path.GetPathId());
                UNIT_ASSERT_VALUES_EQUAL(operation.GetTableId().GetSchemaVersion(), version);
            }

            const auto result = runtime.WaitFuture(ddl.ExecuteQuery(alter, TTxControl::NoTx()), timeout);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto after = DescribeTable(&server, sender, tablePath);
            UNIT_ASSERT(after.GetPathDescription().GetTable().GetTableSchemaVersion() > version);
            UNIT_ASSERT_C(!write.HasValue() && !write.HasException(), "write finished before the held rows were delivered");
            blocked.Stop().Unblock();
            return runtime.WaitFuture(write, timeout);
        };

        const TString query = PreparedInsert ? R"(
            DECLARE $k AS Int32;
            INSERT INTO TestTable (k, other) VALUES ($k, $k * 10), ($k + 1, ($k + 1) * 10) RETURNING k, other, v;
        )" : R"(
            DECLARE $k AS Int32;
            DECLARE $phase AS Int32;
            UPSERT INTO TestTable (k, other) VALUES
                (1, 10 + $phase), (2, 20 + $phase), (3, 30 + $phase), ($k, $k * 10), ($k + 1, ($k + 1) * 10)
            RETURNING k, other, v;
        )";
        std::optional<NYdb::NTable::TDataQuery> prepared;
        const auto prepare = [&] {
            if (PreparedInsert) {
                const auto result = runtime.WaitFuture(writer.PrepareDataQuery(query));
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                prepared = result.GetQuery();
            }
        };
        const auto execute = [&](i32 k, i32 phase) {
            NYdb::TParamsBuilder params;
            params.AddParam("$k").Int32(k).Build();
            if (prepared) {
                return prepared->Execute(tx, params.Build());
            }
            params.AddParam("$phase").Int32(phase).Build();
            return writer.ExecuteDataQuery(query, tx, params.Build());
        };
        const auto expectedRows = [&](i32 k, i32 phase, std::optional<i32> value) {
            TRows rows{{k, {k * 10, value}}, {k + 1, {(k + 1) * 10, value}}};
            if (!PreparedInsert) {
                rows.emplace(1, TRow{10 + phase, 7});
                rows.emplace(2, TRow{20 + phase, std::nullopt});
                rows.emplace(3, TRow{30 + phase, 42});
            }
            return rows;
        };

        checkWrite(runtime.WaitFuture(writer.ExecuteDataQuery(
            "INSERT INTO TestTable (k, other, v) VALUES (1, 10, 7), (2, 20, NULL) RETURNING k, other, v;", tx)),
            {{1, {10, 7}}, {2, {20, std::nullopt}}});
        checkWrite(runtime.WaitFuture(writer.ExecuteDataQuery(
            "INSERT INTO TestTable (k, other) VALUES (3, 30) RETURNING k, other, v;", tx)),
            {{3, {30, 42}}});
        prepare();
        if (PreparedInsert) {
            checkWrite(runtime.WaitFuture(execute(4, 0)), expectedRows(4, 0, 42));
        }

        const auto checkTransition = [&](const TString& alter, i32 k, i32 phase, std::optional<i32> newDefault) {
            auto result = race([&] { return execute(k, phase); }, alter);
            if (!result.IsSuccess()) {
                checkInvalidated(result);
                prepare();
                result = runtime.WaitFuture(execute(k, phase));
            }
            // The held write reaches DataShard after ALTER completes, so success requires the current default.
            checkWrite(result, expectedRows(k, phase, newDefault));
        };

        const TString defaultB = "IF(RandomNumber(1) * 0ul = 0ul, 50, 0)";
        checkTransition(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << defaultB << ";", 6, 1, 50);
        AssertFromExpression(fixture.ColumnDesc(tablePath, "v"), defaultB);
        checkTransition("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;", 8, 2, std::nullopt);
        const auto column = fixture.ColumnDesc(tablePath, "v");
        UNIT_ASSERT_C(!column.HasDefaultFromExpression() && !column.HasDefaultFromLiteral(), column.ShortDebugString());

        const auto checkImage = [](const NJson::TJsonValue& image, const TRow& row) {
            UNIT_ASSERT(image.IsMap() && image.Has("other") && image.Has("v"));
            UNIT_ASSERT_VALUES_EQUAL(image.GetMap().size(), 2u);
            UNIT_ASSERT(image["other"].IsInteger());
            UNIT_ASSERT_VALUES_EQUAL(image["other"].GetInteger(), row.Other);
            if (row.V) {
                UNIT_ASSERT(image["v"].IsInteger());
                UNIT_ASSERT_VALUES_EQUAL(image["v"].GetInteger(), *row.V);
            } else {
                UNIT_ASSERT(image["v"].IsNull());
            }
        };
        const auto messages = fixture.ReadChangefeedMessages(changes.size());
        UNIT_ASSERT_VALUES_EQUAL(enqueuedChanges, changes.size());
        for (const auto& message : messages) {
            const auto& key = message["key"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(key.size(), 1u);
            UNIT_ASSERT(key.front().IsInteger());
            const auto& image = message["newImage"];
            UNIT_ASSERT(image.Has("other") && image["other"].IsInteger());
            const auto found = changes.find({static_cast<i32>(key.front().GetInteger()), static_cast<i32>(image["other"].GetInteger())});
            UNIT_ASSERT_C(found != changes.end(), "unexpected CDC image " << message);
            checkImage(image, found->second.New);
            if (found->second.Old) {
                checkImage(message["oldImage"], *found->second.Old);
            } else {
                UNIT_ASSERT(!message.Has("oldImage"));
            }
            changes.erase(found);
        }
        UNIT_ASSERT(changes.empty());
    }

    Y_UNIT_TEST(ChangefeedUsesCurrentExpressionDefault) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Exec("ALTER TABLE TestTable ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'JSON');");
        fixture.Exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER test_consumer;");
        fixture.Exec(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 42, 0);
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "IF(RandomNumber(1) * 0ul = 0ul, 42, 0)");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("REPLACE INTO TestTable (k, other) VALUES (2, 20);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (3, 30);");
        fixture.Exec("REPLACE INTO TestTable (k, other) VALUES (4, 40);");

        THashMap<i64, NJson::TJsonValue> messages;
        for (auto& message : fixture.ReadChangefeedMessages(4)) {
            const auto key = message["key"][0].GetInteger();
            UNIT_ASSERT_C(messages.emplace(key, std::move(message)).second, "duplicate CDC key " << key);
        }
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(messages.at(1)["update"]["v"].GetInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(messages.at(2)["reset"]["v"].GetInteger(), 42);
        UNIT_ASSERT(messages.at(3).Has("update"));
        UNIT_ASSERT(!messages.at(3)["update"].Has("v"));
        UNIT_ASSERT(messages.at(4)["reset"].Has("v"));
        UNIT_ASSERT(messages.at(4)["reset"]["v"].IsNull());
    }

    // Literal defaults retain their metadata kind when replacing an expression.
    Y_UNIT_TEST(SetLiteralDefaultOverExprColumn) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 8ul;");
        {
            const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        }

        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[8u]]]");
    }

    // DROP DEFAULT removes the persisted default. Prepared-plan invalidation is checked below.
    Y_UNIT_TEST_TWIN(DropDefaultAffectsNewWrites, ClearWithNull) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");

        fixture.Exec(ClearWithNull ? "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT NULL;"
                                   : "ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(col.GetDefaultValueCase() == NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET,
            col.ShortDebugString());
        const auto ddl = fixture.ShowCreateTable("/Root/TestTable");
        UNIT_ASSERT_C(ddl.find("DEFAULT") == std::string::npos, ddl);
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;#]]");
    }

    Y_UNIT_TEST_TWIN(PublicAlterClearsExpressionDefault, UseNullLiteral) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                required Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        Ydb::Table::AlterTableRequest request;
        request.set_path("/Root/TestTable");
        for (const char* name : {"v", "required"}) {
            AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", name), DefaultExpr);
            auto& column = *request.add_alter_columns();
            column.set_name(name);
            if constexpr (UseNullLiteral) {
                auto& literal = *column.mutable_from_literal();
                literal.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
                literal.mutable_value()->set_null_flag_value(NProtoBuf::NULL_VALUE);
            } else {
                column.set_empty_default(NProtoBuf::NULL_VALUE);
            }
        }
        using TEvRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse>;
        const auto response = NRpcService::DoLocalRpc<TEvRequest>(std::move(request), "/Root", Nothing(),
            fixture.Runner().GetTestServer().GetRuntime()->GetActorSystem(0)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        const auto nullable = fixture.ColumnDesc("/Root/TestTable", "v");
        const auto required = fixture.ColumnDesc("/Root/TestTable", "required");
        UNIT_ASSERT_C(nullable.GetDefaultValueCase() == NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET,
            nullable.ShortDebugString());
        UNIT_ASSERT_C(required.GetDefaultValueCase() == NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET,
            required.ShortDebugString());
        UNIT_ASSERT(!nullable.GetNotNull());
        UNIT_ASSERT(required.GetNotNull());
        fixture.Rejects("INSERT INTO TestTable (k) VALUES (2);", "Missing not null column in input: required");
        fixture.Exec("INSERT INTO TestTable (k, required) VALUES (2, 7ul);");
        fixture.Check("SELECT k, v, required FROM TestTable ORDER BY k;", "[[1;[42u];42u];[2;#;7u]]");
    }

    Y_UNIT_TEST(FullBackupCollectionRestoresExpressionDefault) {
        TTestFixture fixture(DefaultExprBackupSettings());
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul), (3, NULL);");
        fixture.Exec(R"(
            CREATE BACKUP COLLECTION FullDefaultBackup (TABLE `/Root/TestTable`)
                WITH (STORAGE = 'cluster');
        )");
        RunBackupCollectionOperation<NYdb::NBackup::TFullBackupResponse>(
            fixture, "BACKUP FullDefaultBackup;", TBackupOperationId::FULL_BACKUP);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (4);");
        fixture.Exec("DROP TABLE TestTable;");
        RunBackupCollectionOperation<NYdb::NBackup::TBackupCollectionRestoreResponse>(
            fixture, "RESTORE FullDefaultBackup;", TBackupOperationId::RESTORE);

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        UNIT_ASSERT_STRING_CONTAINS(fixture.ShowCreateTable("/Root/TestTable"), TStringBuilder() << "DEFAULT " << DefaultExpr);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]];[3;#]]");
        fixture.Check("INSERT INTO TestTable (k) VALUES (4) RETURNING k, v;", "[[4;[42u]]]");
    }

    Y_UNIT_TEST(IncrementalBackupCollectionRestoresRowsAndFullSnapshotDefault) {
        TTestFixture fixture(DefaultExprBackupSettings());
        const TString fullSnapshotDefault = "RandomNumber(1) * 0ul + 50ul";
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << fullSnapshotDefault << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), fullSnapshotDefault);

        fixture.Exec(R"(
            CREATE BACKUP COLLECTION IncrementalDefaultBackup (TABLE `/Root/TestTable`)
                WITH (STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true');
        )");
        RunBackupCollectionOperation<NYdb::NBackup::TFullBackupResponse>(
            fixture, "BACKUP IncrementalDefaultBackup;", TBackupOperationId::FULL_BACKUP);

        // The collection locks the table schema. The earlier default change predates the full snapshot.
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 99ul;",
            "Alter schema forbidden: table is part of a backup collection");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (3, 7ul);");

        // The backup rotates its CDC stream, whose name includes a second-resolution timestamp.
        fixture.Runner().GetTestServer().GetRuntime()->AdvanceCurrentTime(TDuration::Seconds(1));
        RunBackupCollectionOperation<NYdb::NBackup::TIncrementalBackupResponse>(
            fixture, "BACKUP IncrementalDefaultBackup INCREMENTAL;", TBackupOperationId::INCREMENTAL_BACKUP);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (4);");
        fixture.Exec("DROP TABLE TestTable;");
        RunBackupCollectionOperation<NYdb::NBackup::TBackupCollectionRestoreResponse>(
            fixture, "RESTORE IncrementalDefaultBackup;", TBackupOperationId::RESTORE);

        // RESTORE copies the full snapshot's schema and replays the incremental row changes.
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), fullSnapshotDefault);
        UNIT_ASSERT_STRING_CONTAINS(fixture.ShowCreateTable("/Root/TestTable"), TStringBuilder() << "DEFAULT " << fullSnapshotDefault);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[50u]];[3;[7u]]]");
        fixture.Check("INSERT INTO TestTable (k) VALUES (4) RETURNING k, v;", "[[4;[50u]]]");
    }

    Y_UNIT_TEST(PreparedInsertAfterTableRecreation) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = session.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k) VALUES ($k);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        const auto executePrepared = [&](i32 key) {
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            return prepared.Execute(NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        };

        const auto originalResult = executePrepared(1);
        UNIT_ASSERT_C(originalResult.IsSuccess(), originalResult.GetIssues().ToString());
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]]]");

        const auto tableId = [&] {
            const auto describe = fixture.Runner().GetTestClient().Ls("/Root/TestTable");
            const auto& path = describe->Record.GetPathDescription().GetSelf();
            UNIT_ASSERT_C(path.GetPathId(), describe->Record.ShortDebugString());
            return path.GetPathId();
        };
        const auto oldTableId = tableId();
        const TString replacement = "RandomNumber(1) * 0ul + 84ul";
        fixture.Exec("DROP TABLE TestTable;");
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << replacement));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), replacement);
        UNIT_ASSERT_VALUES_UNEQUAL(tableId(), oldTableId);

        const auto result = executePrepared(2);
        if (result.IsSuccess()) {
            fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[2;[84u]]]");
        } else {
            const auto status = result.GetStatus();
            const auto& issues = result.GetIssues();
            const bool missingQuery = status == EStatus::NOT_FOUND && issues.ToString().find("Query not found:") != std::string::npos;
            const bool invalidated = status == EStatus::ABORTED && HasIssue(issues, NYql::TIssuesIds::KIKIMR_QUERY_INVALIDATED);
            UNIT_ASSERT_C(missingQuery || invalidated, status << ": " << issues.ToString());
            fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[]");
        }
        fixture.Check("INSERT INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;[84u]]]");
    }

    Y_UNIT_TEST_TWIN(PreparedInsertIsRecompiledAfterDefaultChange, DropDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        auto tableClient = fixture.Runner().GetTableClient();
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = tableSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k) VALUES ($k);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();

        const auto executePrepared = [&](i32 key) {
            auto params = prepared.GetParamsBuilder()
                .AddParam("$k").Int32(key).Build()
                .Build();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        executePrepared(1);
        if (DropDefault) {
            fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(!column.HasDefaultFromExpression() && !column.HasDefaultFromLiteral(), column.ShortDebugString());
        } else {
            fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 8ul;");
        }
        executePrepared(2);

        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;",
            DropDefault ? "[[1;[42u]];[2;#]]" : "[[1;[42u]];[2;[8u]]]");

        const TString replacement = "RandomNumber(1) * 0ul + 9ul";
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << replacement << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), replacement);
        executePrepared(3);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", DropDefault
            ? "[[1;[42u]];[2;#];[3;[9u]]]"
            : "[[1;[42u]];[2;[8u]];[3;[9u]]]");
    }

    Y_UNIT_TEST(CachedWriteStopsUsingReplacedDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        const TString query = R"(
            DECLARE $k AS Int32;
            INSERT INTO TestTable (k) VALUES ($k);
        )";
        const auto settings = NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true)
            .CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);
        const auto fromCache = [&](i32 key) {
            auto params = TParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats().has_value());
            return TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache();
        };

        UNIT_ASSERT(!fromCache(1));
        UNIT_ASSERT(fromCache(2));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 50ul;");
        UNIT_ASSERT(!fromCache(3));
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]];[3;[50u]]]");
    }

    Y_UNIT_TEST(AlterOnGeneratedColumnRejected) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                g Int32 GENERATED ALWAYS AS (k + 1) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ALTER COLUMN g SET DEFAULT " << DefaultExpr << ";",
            "DEFAULT of GENERATED column");
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN g DROP DEFAULT;", "DEFAULT of GENERATED column");
    }

    Y_UNIT_TEST(SetDefaultExprOnSerialColumnRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE TestTable (k Serial, v Int32, PRIMARY KEY (k));");

        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ALTER COLUMN k SET DEFAULT " << DefaultExpr << ";",
            "serial/sequence column");
    }

    Y_UNIT_TEST(ShowCreateTable) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const std::string ddl = fixture.ShowCreateTable("/Root/TestTable");
        UNIT_ASSERT_STRING_CONTAINS(ddl, TStringBuilder() << "DEFAULT " << DefaultExpr);
    }

    Y_UNIT_TEST(ShowCreateTableReplay) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const auto origin = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        const std::string ddl = fixture.ShowCreateTable("/Root/TestTable");

        fixture.Exec("DROP TABLE TestTable;");
        fixture.Exec(ddl);

        const auto replayed = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetExprText(), origin.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetStored(), origin.GetStored());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetWritable(), origin.GetWritable());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(DescribeTableHidesExpression, WithSequenceValue) {
        TTestFixture fixture;
        if constexpr (WithSequenceValue) {
            fixture.Exec(R"(
                CREATE TABLE TestTable (
                    k Serial,
                    v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                    PRIMARY KEY (k)
                );
            )");
        } else {
            fixture.Exec(ValueTableDDL());
        }

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable",
            NYdb::NTable::TDescribeTableSettings().WithSetVal(WithSequenceValue)).GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());

        // Column expressions are not part of the public API: the column describes without a default
        const auto& proto = NYdb::TProtoAccessor::GetProto(describe.GetTableDescription());
        bool found = false;
        for (const auto& column : proto.columns()) {
            if (column.name() != "v") {
                continue;
            }
            found = true;
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(column.default_value_case()),
                static_cast<int>(Ydb::Table::ColumnMeta::DEFAULT_VALUE_NOT_SET), column.ShortDebugString());
        }
        UNIT_ASSERT_C(found, "column v not found in DescribeTable");

        if constexpr (WithSequenceValue) {
            const auto columns = describe.GetTableDescription().GetTableColumns();
            const auto serial = std::find_if(columns.begin(), columns.end(), [](const auto& column) {
                return column.Name == "k";
            });
            UNIT_ASSERT(serial != columns.end());
            UNIT_ASSERT(serial->SequenceDescription);
            UNIT_ASSERT_VALUES_EQUAL(serial->SequenceDescription->Name, "_serial_column_k");
            UNIT_ASSERT(serial->SequenceDescription->StartValue);
            UNIT_ASSERT_VALUES_EQUAL(*serial->SequenceDescription->StartValue, 1);
        }
    }

    Y_UNIT_TEST(DumpDescribeRejectsDefaultExpression) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable",
            NYdb::NTable::TDescribeTableSettings().RequestType("dump")).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::UNSUPPORTED, describe.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(describe.GetIssues().ToString(), "/Root/TestTable");
        UNIT_ASSERT_STRING_CONTAINS(describe.GetIssues().ToString(),
            "exporting tables with DEFAULT expressions is not supported");
    }

    Y_UNIT_TEST(SurvivesSchemeShardRestart) {
        TTestFixture fixture;
        const TString expression = DefaultExpr;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.RestartSchemeShard("/Root/TestTable");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]]]");
    }

    Y_UNIT_TEST(StoredDefaultTypesAfterReload) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                d Decimal(22, 9) NOT NULL DEFAULT
                    IF(RandomNumber(1) * 0ul = 0ul, Decimal("12.34", 22, 9), Decimal("0", 22, 9)),
                s String NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, "bytes", ""),
                u Utf8 NOT NULL DEFAULT Unicode::ToLower(IF(RandomNumber(1) * 0ul = 0ul, "MiXeD"u, ""u)),
                dt Date NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Date("2021-01-01"), Date("2020-01-01")),
                ts Datetime NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Datetime("2021-01-01T01:02:03Z"), Datetime("2020-01-01T00:00:00Z")),
                stamp Timestamp NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Timestamp("2021-01-01T01:02:03.123456Z"), Timestamp("2020-01-01T00:00:00Z")),
                span Interval NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Interval("PT2H"), Interval("PT1H")),
                id Uuid NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Uuid("550e8400-e29b-41d4-a716-446655440000"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe")),
                n DyNumber NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, DyNumber("15.15"), DyNumber("60.5")),
                j JsonDocument NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, JsonDocument("[14]"), JsonDocument("[]")),
                i8 Int8 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Int8("-100"), Int8("0")),
                i16 Int16 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Int16("-200"), Int16("0")),
                i32 Int32 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, -300, 0),
                i64 Int64 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Int64("-400"), Int64("0")),
                u8 Uint8 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Uint8("10"), Uint8("0")),
                u16 Uint16 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Uint16("20"), Uint16("0")),
                u32 Uint32 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 30u, 0u),
                f Float NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 1.5f, 0.0f),
                dbl Double NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, 2.5, 0.0),
                dt32 Date32 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Date32("2021-01-01"), Date32("2020-01-01")),
                ts64 Datetime64 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Datetime64("2021-01-01T00:00:00Z"), Datetime64("2020-01-01T00:00:00Z")),
                stamp64 Timestamp64 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul,
                    Timestamp64("2021-01-01T00:00:00.000000Z"), Timestamp64("2020-01-01T00:00:00.000000Z")),
                span64 Interval64 NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Interval64("P1D"), Interval64("PT1H")),
                json_text Json NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Json("[0]"), Json("[]")),
                yson_text Yson NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, Yson("[1]"), Yson("[]")),
                PRIMARY KEY (k)
            );
        )");
        fixture.RestartSchemeShard("/Root/TestTable");
        for (TStringBuf name : {"d", "s", "u", "dt", "ts", "stamp", "span", "id", "n", "j",
                "i8", "i16", "i32", "i64", "u8", "u16", "u32", "f", "dbl",
                "dt32", "ts64", "stamp64", "span64", "json_text", "yson_text"}) {
            const auto column = fixture.ColumnDesc("/Root/TestTable", TString(name));
            UNIT_ASSERT_C(column.HasDefaultFromExpression(), column.ShortDebugString());
            UNIT_ASSERT(column.GetDefaultFromExpression().GetStored());
            UNIT_ASSERT(column.GetDefaultFromExpression().GetWritable());
        }

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT d, s, u, dt, ts, stamp, span FROM TestTable;",
            R"([["12.34";"bytes";"mixed";18628u;1609462923u;1609462923123456u;7200000000]])");
        fixture.Check("SELECT i8, i16, i32, i64, u8, u16, u32, f, dbl FROM TestTable;",
            "[[-100;-200;-300;-400;10u;20u;30u;1.5;2.5]]");
        fixture.Check("SELECT dt32, ts64, stamp64, span64, json_text, yson_text FROM TestTable;",
            R"([[18628;1609459200;1609459200000000;86400000000;"[0]";"[1]"]])");
        auto result = fixture.QuerySession().ExecuteQuery("SELECT id, n, j FROM TestTable;",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetUuid().ToString(), "550e8400-e29b-41d4-a716-446655440000");
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("n").GetDyNumber(), ".1515e2");
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("j").GetJsonDocument(), "[14]");
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST(CopyAndTruncatePreserveExpressionDefault) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (2, 20, 7ul);");
        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto copy = session.CopyTable("/Root/TestTable", "/Root/Copy").ExtractValueSync();
        UNIT_ASSERT_C(copy.IsSuccess(), copy.GetIssues().ToString());
        AssertFromExpression(fixture.ColumnDesc("/Root/Copy", "v"), DefaultExpr);
        UNIT_ASSERT_STRING_CONTAINS(fixture.ShowCreateTable("/Root/Copy"), TStringBuilder() << "DEFAULT " << DefaultExpr);

        const TString original = "[[1;[10];[42u]];[2;[20];[7u]]]";
        fixture.Check("SELECT k, other, v FROM Copy ORDER BY k;", original);
        fixture.Check("SELECT k, other, v FROM Copy VIEW idx_v ORDER BY k;", original);
        fixture.Exec("TRUNCATE TABLE Copy;");
        fixture.Check("SELECT k, v FROM Copy;", "[]");
        fixture.Check("SELECT k, v FROM Copy VIEW idx_v;", "[]");
        fixture.Check("INSERT INTO Copy (k, other) VALUES (3, 30) RETURNING k, other, v;", "[[3;[30];[42u]]]");
        fixture.Check("SELECT k, other, v FROM Copy VIEW idx_v WHERE v = 42ul;", "[[3;[30];[42u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", original);
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;", original);
    }

    Y_UNIT_TEST_TWIN(StreamExecuteQueryMaterializesDefault, EnableStreamWrite) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        auto client = fixture.Runner().GetQueryClient();
        auto iterator = client.StreamExecuteQuery(R"(
            INSERT INTO TestTable (k, other) VALUES (1, 10) RETURNING k, other, v;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        CompareYson("[[1;[10];[42u]]]", StreamResultToYson(iterator));
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v;", "[[1;[10];[42u]]]");
    }

    Y_UNIT_TEST(ReadsUseStoredValuesAfterDefaultChanges) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k, other) VALUES (1, 10);");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (2, 20, NULL), (3, 30, 7ul);");
        fixture.Exec(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT Ensure(RandomNumber(1), false, "default ran");
        )");
        fixture.RestartSchemeShard("/Root/TestTable");

        fixture.Check("SELECT * FROM TestTable ORDER BY k;", "[[1;[10];[42u]];[2;[20];#];[3;[30];[7u]]]");
        fixture.Check(R"(
            SELECT v, COUNT(*) FROM TestTable WHERE v IS NOT NULL
            GROUP BY v HAVING COUNT(*) = 1u ORDER BY v;
        )", "[[[7u];1u];[[42u];1u]]");
        fixture.Check(R"(
            $rows = SELECT k, v FROM TestTable WHERE v IS NOT NULL;
            SELECT l.k, r.v, ROW_NUMBER() OVER (ORDER BY r.v) AS pos
            FROM $rows AS l INNER JOIN $rows AS r ON l.k = r.k
            ORDER BY pos;
        )", "[[3;[7u];1u];[1;[42u];2u]]");

        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto settings = NYdb::NTable::TReadTableSettings().Ordered().AppendColumns("k").AppendColumns("v");
        auto iterator = session.ReadTable("/Root/TestTable", settings).ExtractValueSync();
        CompareYson("[[[1];[42u]];[[2];#];[[3];[7u]]]", StreamResultToYson(iterator));
    }

    Y_UNIT_TEST(BulkUpsert) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));

        auto tableClient = fixture.Runner().GetTableClient();
        const auto upsertKeyOnly = [&](i32 key) {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(key)
                    .AddMember("other").OptionalInt32(key * 10)
                .EndStruct()
                .EndList();
            return tableClient.BulkUpsert("/Root/TestTable", rows.Build()).ExtractValueSync();
        };

        auto result = upsertKeyOnly(1);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        result = upsertKeyOnly(2);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: v");

        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(2)
                    .AddMember("v").OptionalUint64(7)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        for (i32 key : {2, 3}) {
            result = upsertKeyOnly(key);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1;[10];#];[2;[20];[7u]];[3;[30];#]]");
    }

    Y_UNIT_TEST(BulkUpsertWithMixedExpressionAndLiteralDefaults) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        auto tableClient = fixture.Runner().GetTableClient();
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "expr");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "literal");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(2)
                    .AddMember("expr").OptionalUint64(20)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: literal");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(3)
                    .AddMember("literal").OptionalUint64(30)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: expr");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(4)
                    .AddMember("expr").OptionalUint64(40)
                    .AddMember("literal").OptionalUint64(50)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, expr, literal FROM TestTable;", "[[4;[40u];[50u]]]");
    }
}

}   // namespace NKikimr::NKqp
