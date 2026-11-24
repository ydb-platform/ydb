#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr {
namespace {

using namespace NKqp;
using namespace NGRpcService;
using namespace NRpcService;

constexpr char ReadSql[] = "SELECT Value FROM `/Root/Objects` ORDER BY Key";
constexpr char WriteSql[] = "UPSERT INTO `/Root/Objects` (Key, Value) VALUES (1u, 'new'), (101u, 'new')";

enum class EContinuation {
    None,
    Repeat,
    Omit,
    Change,
};

struct TQueryOptions {
    TString Sql = ReadSql;
    TMaybe<TRowVersion> Version;
    bool Scan = false;
    bool SnapshotRead = true;
    EContinuation Continuation = EContinuation::None;
    TString SessionId;
};

struct TResult {
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    std::vector<NYdb::TResultSet> ResultSets;
};

class TSnapshotQuery: public TQueryBase {
public:
    explicit TSnapshotQuery(TQueryOptions options)
        : TQueryBase(NKikimrServices::KQP_PROXY, options.SessionId, "/Root")
        , Options(std::move(options))
    {
        MvccVersion = Options.Version;
    }

    NThreading::TFuture<TResult> GetResult() const {
        return Result.GetFuture();
    }

private:
    void OnRunQuery() override {
        if (Options.Scan) {
            RunStreamQuery(Options.Sql);
        } else {
            RunDataQuery(Options.Sql, nullptr,
                Options.Continuation == EContinuation::None ? TTxControl::BeginAndCommitTx(Options.SnapshotRead)
                                                            : TTxControl::BeginTx(Options.SnapshotRead));
        }
    }

    void OnStreamResult(NYdb::TResultSet&& result) override {
        Results.emplace_back(std::move(result));
    }

    void OnQueryResult() override {
        if (!Options.Scan) {
            for (auto& result : ResultSets) {
                Results.emplace_back(std::move(result));
            }
        }
        if (++Queries == 1 && Options.Continuation != EContinuation::None) {
            if (Options.Continuation == EContinuation::Omit) {
                MvccVersion = Nothing();
            } else if (Options.Continuation == EContinuation::Change) {
                MvccVersion = MvccVersion->Next();
            }
            RunDataQuery(Options.Sql, nullptr, TTxControl::ContinueAndCommitTx());
            return;
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Result.SetValue(TResult{ status, std::move(issues), std::move(Results) });
    }

    const TQueryOptions Options;
    ui32 Queries = 0;
    std::vector<NYdb::TResultSet> Results;
    NThreading::TPromise<TResult> Result = NThreading::NewPromise<TResult>();
};

struct TEvSnapshotQueryResponse: public TEventLocal<TEvSnapshotQueryResponse, EventSpaceBegin(TEvents::ES_PRIVATE) + 100>, TResult {
    TEvSnapshotQueryResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, std::vector<NYdb::TResultSet>&& results)
        : TResult{ status, std::move(issues), std::move(results) }
    {
    }
};

class TRetrySnapshotQuery: public TQueryBase {
public:
    TRetrySnapshotQuery(TMaybe<TRowVersion> version, TString sql)
        : TQueryBase(NKikimrServices::KQP_PROXY, {}, "/Root")
        , Sql(std::move(sql))
    {
        MvccVersion = version;
    }

private:
    void OnRunQuery() override {
        RunDataQuery(Sql, nullptr, TTxControl::BeginAndCommitTx(true));
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvSnapshotQueryResponse(status, std::move(issues), std::move(ResultSets)));
    }

    const TString Sql;
};

class TSnapshotTestEnv {
public:
    explicit TSnapshotTestEnv(bool sourceRead = true, ui32 nodes = 1, TDuration keepSnapshotTimeout = TDuration::Minutes(5))
        : Kikimr(Settings(sourceRead, nodes, keepSnapshotTimeout))
    {
        Kikimr.RunCall([&] {
            Kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "Objects"
                Columns { Name: "Key", Type: "Uint64" }
                Columns { Name: "Value", Type: "String" }
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
            )");
            return true;
        });
        auto result = Run({ .Sql = "UPSERT INTO `/Root/Objects` (Key, Value) VALUES (1u, 'old'), (101u, 'old')", .SnapshotRead = false });
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToString());
    }

    TTestActorRuntime& Runtime() {
        return *Kikimr.GetTestServer().GetRuntime();
    }

    TResult Run(TQueryOptions options, ui32 node = 0) {
        auto actor = std::make_unique<TSnapshotQuery>(std::move(options));
        auto future = actor->GetResult();
        Runtime().Register(actor.release(), node);
        return Runtime().WaitFuture(future);
    }

    TResult RunWithRetry(TMaybe<TRowVersion> version, ui32 expectedAttempts, const TString& sql = ReadSql) {
        ui32 attempts = 0;
        auto observer = Runtime().AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Recipient == MakeKqpProxyID(Runtime().GetNodeId()) && ev->Get()->GetQuery() == sql) {
                UNIT_ASSERT_C(++attempts <= expectedAttempts, "Unexpected retry of snapshot query");
            }
        });
        const auto edge = Runtime().AllocateEdgeActor();
        using TRetry = TQueryRetryActor<TRetrySnapshotQuery, TEvSnapshotQueryResponse, TMaybe<TRowVersion>, TString>;
        // Deliberately unbounded: a terminal error must stop retries independently of a policy's time/attempt limits.
        auto policy = TQueryRetryActorBase::IRetryPolicy::GetFixedIntervalPolicy(
            TQueryRetryActorBase::Retryable, TDuration::MilliSeconds(1), TDuration::MilliSeconds(1));
        Runtime().Register(new TRetry(edge, policy, version, sql));
        auto response = Runtime().GrabEdgeEventRethrow<TEvSnapshotQueryResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(attempts, expectedAttempts);
        return std::move(static_cast<TResult&>(*response->Get()));
    }

    TRowVersion AcquireSnapshot() {
        const auto edge = Runtime().AllocateEdgeActor();
        SnapshotManager = Runtime().Register(CreateKqpSnapshotManager("/Root", TDuration::Minutes(5)));
        auto request = std::make_unique<TEvKqpSnapshot::TEvCreateSnapshotRequest>(TVector<TTableId>{}, 1);
        // Queue behind the manager's bootstrap instead of delivering synchronously.
        Runtime().Send(new IEventHandle(SnapshotManager, edge, request.release()), 0, true);
        auto response = Runtime().GrabEdgeEventRethrow<TEvKqpSnapshot::TEvCreateSnapshotResponse>(edge);
        UNIT_ASSERT_C(response->Get()->Status == NKikimrIssues::TStatusIds::SUCCESS, response->Get()->Issues.ToString());
        SnapshotHandle = std::move(response->Get()->SnapshotHandle);
        return { response->Get()->Snapshot.Step, response->Get()->Snapshot.TxId };
    }

    void WriteNewValues() {
        auto result = Run({ .Sql = WriteSql, .SnapshotRead = false });
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToString());
    }

    TString CreateSession(ui32 node = 0) {
        using TRpc = TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>;
        auto future = DoLocalRpc<TRpc>(Ydb::Table::CreateSessionRequest(), "/Root", Nothing(), Runtime().GetActorSystem(node), true);
        const auto response = Runtime().WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateSessionResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        return result.session_id();
    }

    Ydb::Table::ExecuteDataQueryResponse ExecuteRaw(
        Ydb::Table::ExecuteDataQueryRequest request, const TMaybe<TString>& version, bool internal = true)
    {
        using TRpc = TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>;
        if (request.session_id().empty()) {
            request.set_session_id(CreateSession());
        }
        TMap<TString, TString> metadata;
        if (version) {
            metadata.emplace(YDB_MVCC_VERSION_HEADER, *version);
        }
        auto future = DoLocalRpc<TRpc>(std::move(request), "/Root", Nothing(), Nothing(), Runtime().GetActorSystem(0), metadata, internal);
        return Runtime().WaitFuture(future);
    }

    static Ydb::Table::ExecuteDataQueryRequest ReadRequest() {
        Ydb::Table::ExecuteDataQueryRequest request;
        request.mutable_query()->set_yql_text(ReadSql);
        request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
        request.mutable_tx_control()->set_commit_tx(true);
        return request;
    }

    TKikimrRunner Kikimr;
    TSnapshotHandle SnapshotHandle;
    TActorId SnapshotManager;

private:
    static TKikimrSettings Settings(bool sourceRead, ui32 nodes, TDuration keepSnapshotTimeout) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(sourceRead);
        return TKikimrSettings(config).SetUseRealThreads(false).SetWithSampleTables(false).SetNodeCount(nodes).SetKeepSnapshotTimeout(
            keepSnapshotTimeout);
    }
};

void AssertValues(const TResult& result, TStringBuf expected, ui32 expectedRows = 2) {
    UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToString());
    ui32 rows = 0;
    for (const auto& set : result.ResultSets) {
        NYdb::TResultSetParser parser(set);
        while (parser.TryNextRow()) {
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Value").GetOptionalString().value(), expected);
            ++rows;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, expectedRows);
}

}   // namespace

Y_UNIT_TEST_SUITE(QueryActorMvccVersion) {
    Y_UNIT_TEST(HistoricalReads) {
        TSnapshotTestEnv env;
        const auto version = env.AcquireSnapshot();
        env.WriteNewValues();
        ui32 creates = 0;
        auto observer = env.Runtime().AddObserver<TEvKqpSnapshot::TEvCreateSnapshotRequest>([&](auto&) {
            ++creates;
        });
        AssertValues(env.Run({ .Version = version }), "old");
        UNIT_ASSERT_VALUES_EQUAL(creates, 0);

        AssertValues(env.Run({}), "new");
    }

    Y_UNIT_TEST(Continuation) {
        TSnapshotTestEnv env;
        const auto version = env.AcquireSnapshot();
        env.WriteNewValues();
        for (auto continuation : { EContinuation::Repeat, EContinuation::Omit }) {
            AssertValues(env.Run({ .Version = version, .Continuation = continuation }), "old", 4);
        }
        const auto changed = env.Run({ .Version = version, .Continuation = EContinuation::Change });
        UNIT_ASSERT_VALUES_EQUAL_C(changed.Status, Ydb::StatusIds::BAD_REQUEST, changed.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(changed.Issues.ToString(), "cannot change the snapshot");
    }

    Y_UNIT_TEST(ForwardedSession) {
        TSnapshotTestEnv env(true, 2);
        const auto session = env.CreateSession(1);
        const auto version = env.AcquireSnapshot();
        env.WriteNewValues();
        bool forwarded = false;
        auto observer = env.Runtime().AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Get()->GetMvccVersion() && ev->Recipient.NodeId() == env.Runtime().GetNodeId(1)) {
                forwarded = true;
            }
        });
        AssertValues(env.Run({ .Version = version, .SessionId = session }, 0), "old");
        UNIT_ASSERT(forwarded);
    }

    Y_UNIT_TEST(InvalidHeadersAndIsolationModes) {
        TSnapshotTestEnv env;
        const auto version = env.AcquireSnapshot();
        for (const TString& value : { TString(), TString("bad"), TString("0:0"), TString("1:18446744073709551616") }) {
            const auto response = env.ExecuteRaw(env.ReadRequest(), value);
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::BAD_REQUEST, response.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(response.DebugString(), "Invalid x-ydb-mvcc-version");
        }
        const auto external = env.ExecuteRaw(env.ReadRequest(), FormatMvccVersion(version), false);
        UNIT_ASSERT_VALUES_EQUAL_C(external.operation().status(), Ydb::StatusIds::BAD_REQUEST, external.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(external.DebugString(), "only supported for internal calls");

        const auto wrongMode = env.Run({ .Version = version, .SnapshotRead = false });
        UNIT_ASSERT_VALUES_EQUAL_C(wrongMode.Status, Ydb::StatusIds::BAD_REQUEST, wrongMode.Issues.ToString());
        for (bool stale : { false, true }) {
            auto request = env.ReadRequest();
            auto* settings = request.mutable_tx_control()->mutable_begin_tx();
            if (stale) {
                settings->mutable_stale_read_only();
            } else {
                settings->mutable_online_read_only();
            }
            const auto response = env.ExecuteRaw(std::move(request), FormatMvccVersion(version));
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::BAD_REQUEST, response.DebugString());
        }

        const auto write = env.Run({ .Sql = WriteSql, .Version = version });
        UNIT_ASSERT_C(write.Status != Ydb::StatusIds::SUCCESS, write.Issues.ToString());
        AssertValues(env.Run({}), "old");
    }

    Y_UNIT_TEST(CannotInitializeExistingTransaction) {
        TSnapshotTestEnv env;
        const auto version = env.AcquireSnapshot();
        auto session = env.Kikimr.RunCall([&] {
            return env.Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        });
        const auto begun = env.Kikimr.RunCall([&] {
            return session.BeginTransaction(NYdb::NTable::TTxSettings::SnapshotRO()).GetValueSync();
        });
        UNIT_ASSERT_C(begun.IsSuccess(), begun.GetIssues().ToString());
        auto request = env.ReadRequest();
        request.set_session_id(session.GetId());
        request.mutable_tx_control()->set_tx_id(begun.GetTransaction().GetId());
        const auto response = env.ExecuteRaw(std::move(request), FormatMvccVersion(version));
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::BAD_REQUEST, response.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(response.DebugString(), "cannot change the snapshot");
    }

    Y_UNIT_TEST(ExternalGrpcRequestsAreRejected) {
        TSnapshotTestEnv env;
        env.Kikimr.RunCall([&] {
            auto client = env.Kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session
                              .ExecuteDataQuery(ReadSql, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SnapshotRO()).CommitTx(),
                                  NYdb::NTable::TExecDataQuerySettings().Header({ { YDB_MVCC_VERSION_HEADER, "1:1" } }))
                              .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::BAD_REQUEST, result.GetIssues().ToString());

            auto stream =
                client
                    .StreamExecuteScanQuery(ReadSql, NYdb::NTable::TStreamExecScanQuerySettings().Header({ { YDB_MVCC_VERSION_HEADER, "1:1" } }))
                    .GetValueSync();
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            auto part = stream.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(part.GetStatus(), NYdb::EStatus::BAD_REQUEST, part.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(part.GetIssues().ToString(), "only supported for internal calls");
            return true;
        });
    }

    Y_UNIT_TEST(UnsupportedActionsAndQueryTypes) {
        TSnapshotTestEnv env;
        const auto session = env.CreateSession();
        const auto edge = env.Runtime().AllocateEdgeActor();
        const std::pair<NKikimrKqp::EQueryAction, NKikimrKqp::EQueryType> requests[] = {
            { NKikimrKqp::QUERY_ACTION_PREPARE, NKikimrKqp::QUERY_TYPE_SQL_DML },
            { NKikimrKqp::QUERY_ACTION_EXPLAIN, NKikimrKqp::QUERY_TYPE_SQL_SCAN },
            { NKikimrKqp::QUERY_ACTION_BEGIN_TX, NKikimrKqp::QUERY_TYPE_UNDEFINED },
            { NKikimrKqp::QUERY_ACTION_COMMIT_TX, NKikimrKqp::QUERY_TYPE_UNDEFINED },
            { NKikimrKqp::QUERY_ACTION_ROLLBACK_TX, NKikimrKqp::QUERY_TYPE_UNDEFINED },
            { NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT },
            { NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY },
            { NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY },
            { NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_SQL_SCAN },
            { NKikimrKqp::QUERY_ACTION_EXECUTE, NKikimrKqp::QUERY_TYPE_AST_SCAN },
        };
        for (const auto& [action, type] : requests) {
            auto request = std::make_unique<TEvKqp::TEvQueryRequest>();
            auto& query = *request->Record.MutableRequest();
            query.SetDatabase("/Root");
            if (action != NKikimrKqp::QUERY_ACTION_EXECUTE) {
                query.SetSessionId(session);
                query.SetKeepSession(true);
            }
            query.SetIsInternalCall(true);
            query.SetMvccVersion("1:1");
            query.SetAction(action);
            query.SetType(type);
            query.SetQuery(ReadSql);
            query.MutableTxControl()->mutable_begin_tx()->mutable_snapshot_read_only();
            env.Runtime().Send(new IEventHandle(MakeKqpProxyID(env.Runtime().GetNodeId()), edge, request.release()));
            auto response = env.Runtime().GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL_C(
                response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::BAD_REQUEST, response->Get()->Record.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.DebugString(), "x-ydb-mvcc-version");
            if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
                UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.DebugString(), "only supported for Table Service data queries");
            }
        }
    }

    Y_UNIT_TEST_TWIN(ScanRejectsExplicitSnapshot, SourceRead) {
        TSnapshotTestEnv env(SourceRead);
        const auto version = env.AcquireSnapshot();
        const auto result = env.Run({ .Version = version, .Scan = true });
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::BAD_REQUEST, result.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "does not support an explicit MVCC snapshot version");
        AssertValues(env.Run({ .Scan = true }), "old");
    }

    Y_UNIT_TEST_TWIN(UnavailableSnapshotDoesNotFallBack, StreamLookup) {
        TSnapshotTestEnv env(true, 1, TDuration::Seconds(1));
        const auto version = env.AcquireSnapshot();
        env.WriteNewValues();
        env.SnapshotHandle.Reset();
        env.Runtime().SimulateSleep(TDuration::Seconds(10));
        const auto edge = env.Runtime().AllocateEdgeActor();
        for (ui64 shard : GetTableShards(&env.Kikimr.GetTestServer(), edge, "/Root/Objects")) {
            env.Runtime().SendToPipe(shard, edge, new TEvDataShard::TEvVacuum(1));
            const auto vacuum = env.Runtime().GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(edge);
            UNIT_ASSERT_C(vacuum->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvVacuumResult::OK, vacuum->Get()->Record.DebugString());
        }
        const TString sql = StreamLookup ? R"(
            SELECT o.Value AS Value
            FROM AS_TABLE(AsList(AsStruct(1ul AS Key), AsStruct(101ul AS Key))) AS k
            INNER JOIN `/Root/Objects` AS o ON k.Key = o.Key
            ORDER BY k.Key;
        )"
                                         : ReadSql;
        ui32 lookupReads = 0;
        auto observer = env.Runtime().AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            if (env.Runtime().FindActorName(ev->Sender).Contains("KQP_STREAM_LOOKUP_ACTOR")) {
                ++lookupReads;
            }
        });
        const auto result = env.Run({ .Sql = sql, .Version = version });
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::PRECONDITION_FAILED, result.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "snapshot");
        UNIT_ASSERT(result.ResultSets.empty());

        const auto retried = env.RunWithRetry(version, 1, sql);
        UNIT_ASSERT_VALUES_EQUAL_C(retried.Status, Ydb::StatusIds::PRECONDITION_FAILED, retried.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(retried.Issues.ToString(), "snapshot");
        UNIT_ASSERT(retried.ResultSets.empty());
        if (StreamLookup) {
            UNIT_ASSERT_GT(lookupReads, 0);
        }
    }

    Y_UNIT_TEST_TWIN(TransientAbortsRemainRetryable, ExplicitSnapshot) {
        TSnapshotTestEnv env;
        const auto version = env.AcquireSnapshot();
        env.WriteNewValues();
        bool injected = false;
        auto observer = env.Runtime().AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            if (!injected && ev->Get()->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                // Ordinary snapshot loss may recover with a fresh snapshot; explicit queries may retry unrelated aborts.
                ev->Get()->Record.MutableStatus()->SetCode(ExplicitSnapshot ? Ydb::StatusIds::ABORTED : Ydb::StatusIds::PRECONDITION_FAILED);
                injected = true;
            }
        });
        AssertValues(env.RunWithRetry(ExplicitSnapshot ? TMaybe<TRowVersion>(version) : Nothing(), 2), ExplicitSnapshot ? "old" : "new");
        UNIT_ASSERT(injected);
    }
}

}   // namespace NKikimr
