#include <ydb/core/base/request_types.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr {
namespace {

using namespace NKqp;
using namespace NGRpcService;
using namespace NRpcService;

constexpr char ReadSql[] = "SELECT SUM(Value) AS Total, COUNT(*) AS Rows FROM `/Root/Objects`";
constexpr ui32 ChecksumHints = TEvDataShard::TEvRead::HINT_BATCH | TEvDataShard::TEvRead::HINT_LOW_PRIORITY;

struct TQueryOptions {
    TMaybe<TString> RequestType = TString(NRequestTypes::ChecksumTable);
    bool Internal = true;
    TDuration Timeout;
    TDuration CancelAfter;
    TString QueryId;
};

struct TResult {
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    std::vector<NYdb::TResultSet> ResultSets;
};

class TChecksumQuery: public TQueryBase {
public:
    explicit TChecksumQuery(TString sessionId)
        : TQueryBase(NKikimrServices::KQP_PROXY, std::move(sessionId), "/Root")
    {
        RequestType = TString(NRequestTypes::ChecksumTable);
    }

    NThreading::TFuture<TResult> GetResult() const {
        return Result.GetFuture();
    }

private:
    void OnRunQuery() override {
        RunDataQuery(ReadSql, nullptr, TTxControl::BeginAndCommitTx(true), TDuration::Seconds(30));
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Result.SetValue(TResult{ status, std::move(issues), std::move(ResultSets) });
    }

    NThreading::TPromise<TResult> Result = NThreading::NewPromise<TResult>();
};

class TTestEnv {
public:
    explicit TTestEnv(bool remote)
        : Kikimr(Settings(remote))
    {
        Kikimr.RunCall([&] {
            Kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "Objects"
                Columns { Name: "Key", Type: "Uint64" }
                Columns { Name: "Value", Type: "Uint64" }
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
            )");
            auto session = Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session
                              .ExecuteDataQuery("UPSERT INTO `/Root/Objects` (Key, Value) VALUES (1u, 10u), (2u, 20u), (101u, 30u)",
                                  NYdb::NTable::TTxControl::BeginTx().CommitTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });
        SessionNode = remote ? 1 : 0;
    }

    TTestActorRuntime& Runtime() {
        return *Kikimr.GetTestServer().GetRuntime();
    }

    TString CreateSession() {
        using TRpc = TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>;
        auto future = DoLocalRpc<TRpc>(Ydb::Table::CreateSessionRequest(), "/Root", Nothing(), Runtime().GetActorSystem(SessionNode), true);
        const auto response = Runtime().WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateSessionResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        return result.session_id();
    }

    NThreading::TFuture<Ydb::Table::ExecuteDataQueryResponse> Start(const TString& sessionId, const TQueryOptions& options = {}) {
        using TRpc = TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>;
        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(sessionId);
        if (options.QueryId) {
            request.mutable_query()->set_id(options.QueryId);
        } else {
            request.mutable_query()->set_yql_text(ReadSql);
        }
        request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();
        request.mutable_tx_control()->set_commit_tx(true);
        request.mutable_query_cache_policy()->set_keep_in_cache(true);
        request.set_collect_stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
        request.mutable_operation_params()->mutable_operation_timeout()->set_seconds(options.Timeout.Seconds());
        request.mutable_operation_params()->mutable_cancel_after()->set_seconds(options.CancelAfter.Seconds());
        return DoLocalRpc<TRpc>(std::move(request), "/Root", Nothing(), options.RequestType, Runtime().GetActorSystem(0), options.Internal);
    }

private:
    static TKikimrSettings Settings(bool remote) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->MutableQueryLimits()->SetDataQueryTimeoutMs(10000);
        return TKikimrSettings(config).SetUseRealThreads(false).SetWithSampleTables(false).SetNodeCount(remote ? 2 : 1);
    }

    TKikimrRunner Kikimr;
    ui32 SessionNode = 0;
};

void AssertAggregate(const NYdb::TResultSet& result) {
    NYdb::TResultSetParser parser(result);
    UNIT_ASSERT(parser.TryNextRow());
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Total").GetOptionalUint64().value(), 60);
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Rows").GetUint64(), 3);
    UNIT_ASSERT(!parser.TryNextRow());
}

Ydb::Table::ExecuteQueryResult AssertAggregate(const Ydb::Table::ExecuteDataQueryResponse& response) {
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
    Ydb::Table::ExecuteQueryResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);
    AssertAggregate(NYdb::TResultSet(result.result_sets(0)));
    return result;
}

}   // namespace

Y_UNIT_TEST_SUITE(QueryActorChecksumTable) {
    Y_UNIT_TEST_TWIN(QueryActorExceedsDefaultTimeout, Remote) {
        TTestEnv env(Remote);
        const auto session = env.CreateSession();
        TBlockEvents<TEvDataShard::TEvRead> reads(env.Runtime());
        auto actor = std::make_unique<TChecksumQuery>(session);
        auto future = actor->GetResult();
        env.Runtime().Register(actor.release());
        env.Runtime().WaitFor("both shard reads", [&] {
            return reads.size() >= 2;
        });
        for (const auto& read : reads) {
            UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.GetHints(), ChecksumHints);
            UNIT_ASSERT(!read->Get()->Record.HasLockTxId());
        }

        env.Runtime().SimulateSleep(TDuration::Seconds(11));
        UNIT_ASSERT(!future.HasValue());
        reads.Stop().Unblock();
        const auto result = env.Runtime().WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::SUCCESS, result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 1);
        AssertAggregate(result.ResultSets.front());
    }

    Y_UNIT_TEST_TWIN(HintsFollowExecutionAndPreparedQueries, Remote) {
        TTestEnv env(Remote);
        const auto session = env.CreateSession();
        ui32 expectedHints = 0;
        ui32 reads = 0;
        auto observer = env.Runtime().AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetHints(), expectedHints);
            ++reads;
        });

        TString queryId;
        for (bool prepared : { false, true }) {
            for (bool checksum : { false, true, false }) {
                expectedHints = checksum ? ChecksumHints : 0;
                reads = 0;
                TQueryOptions options;
                options.RequestType = checksum ? TMaybe<TString>(TString(NRequestTypes::ChecksumTable)) : Nothing();
                options.QueryId = prepared ? queryId : TString();
                const auto result = AssertAggregate(env.Runtime().WaitFuture(env.Start(session, options)));
                UNIT_ASSERT_C(reads >= 2, reads);
                if (queryId) {
                    UNIT_ASSERT(result.query_stats().compilation().from_cache());
                    if (!prepared) {
                        UNIT_ASSERT_VALUES_EQUAL(result.query_meta().id(), queryId);
                    }
                } else {
                    queryId = result.query_meta().id();
                    UNIT_ASSERT(queryId);
                }
            }
        }

        // An external caller cannot select the policy by supplying the same header.
        expectedHints = 0;
        reads = 0;
        AssertAggregate(env.Runtime().WaitFuture(env.Start(session, { .Internal = false })));
        UNIT_ASSERT_C(reads >= 2, reads);
    }

    Y_UNIT_TEST(RetriesPreserveHints) {
        TTestEnv env(false);
        const auto session = env.CreateSession();
        ui32 reads = 0;
        bool retried = false;
        auto readObserver = env.Runtime().AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetHints(), ChecksumHints);
            ++reads;
        });
        auto resultObserver = env.Runtime().AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            if (!retried && ev->Get()->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                ev->Get()->Record.MutableStatus()->SetCode(Ydb::StatusIds::OVERLOADED);
                retried = true;
            }
        });
        AssertAggregate(env.Runtime().WaitFuture(env.Start(session)));
        UNIT_ASSERT(retried);
        UNIT_ASSERT_C(reads > 2, reads);
    }

    Y_UNIT_TEST_TWIN(TimeoutsStillApply, Remote) {
        TTestEnv env(Remote);

        struct TCase {
            TQueryOptions Options;
            TDuration Wait;
            Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::TIMEOUT;
        };

        const TCase cases[] = {
            // Ordinary and external queries still have the configured ceiling.
            { { .RequestType = Nothing(), .Timeout = TDuration::Seconds(30) }, TDuration::Seconds(11) },
            { { .Internal = false, .Timeout = TDuration::Seconds(30) }, TDuration::Seconds(11) },
            // Without an explicit timeout, checksum queries use the default.
            { {}, TDuration::Seconds(11) },
            // Both shorter and longer explicit timeouts are enforced.
            { { .Timeout = TDuration::Seconds(2) }, TDuration::Seconds(3) },
            { { .Timeout = TDuration::Seconds(15) }, TDuration::Seconds(16) },
            { { .Timeout = TDuration::Seconds(30), .CancelAfter = TDuration::Seconds(2) }, TDuration::Seconds(3), Ydb::StatusIds::CANCELLED },
        };
        for (const auto& test : cases) {
            const auto session = env.CreateSession();
            TBlockEvents<TEvDataShard::TEvRead> reads(env.Runtime());
            auto future = env.Start(session, test.Options);
            env.Runtime().WaitFor("blocked read", [&] {
                return !reads.empty();
            });
            env.Runtime().SimulateSleep(test.Wait);
            UNIT_ASSERT_C(future.HasValue(), "Expected query deadline after " << test.Wait);
            const auto response = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), test.Status, response.DebugString());
        }
    }
}   // Y_UNIT_TEST_SUITE(QueryActorChecksumTable)

}   // namespace NKikimr
