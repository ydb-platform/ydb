#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/proxy_service/kqp_session_state.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

namespace NKikimr::NKqp {

namespace {

using namespace NWorkload;
using namespace NYdb;
using namespace NActors;

class TWmSessionUpdaterWrapper : public IWmSessionUpdater {
public:
    TWmSessionUpdaterWrapper(EWmState finalState, std::shared_ptr<IWmSessionUpdater> inner)
        : Inner(inner)
        , FinalState(finalState)
    {}

    void SetUpdater(std::shared_ptr<IWmSessionUpdater> inner) {
        Inner = std::move(inner);
    }

    std::shared_ptr<IWmSessionUpdater> GetUpdater() const {
        return Inner;
    }

    void SetRequestState(EWmState state, TInstant timestamp) override {
        if (state > FinalState) {
            return;
        }

        Inner->SetRequestState(state, timestamp);
    }

    void SetPoolId(TString poolId) override {
        Inner->SetPoolId(poolId);
    }

private:
    std::shared_ptr<IWmSessionUpdater> Inner;
    EWmState FinalState;
};

class TKqpWorkloadProxyActor : public TActorBootstrapped<TKqpWorkloadProxyActor> {
public:
    TKqpWorkloadProxyActor(IWmSessionUpdater::EWmState finalState)
        : FinalState(finalState)
    {}

    void SetWorkloadServiceId(TActorId realServiceId) {
        WorkloadServiceId = realServiceId;
    }

    void Bootstrap(const TActorContext&) {
        Become(&TKqpWorkloadProxyActor::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWorkload::TEvPlaceRequestIntoPool, HandlePlaceRequest);
            default:
                Send(ev->Forward(WorkloadServiceId));
        }
    }

    void HandlePlaceRequest(NWorkload::TEvPlaceRequestIntoPool::TPtr& ev) {
        auto* msg = ev->Get();
        auto m = std::make_shared<TWmSessionUpdaterWrapper>(FinalState, msg->WmSessionUpdater);
        
        auto proxyMsg = new NWorkload::TEvPlaceRequestIntoPool(
            msg->DatabaseId,
            msg->SessionId,
            msg->PoolId,
            msg->UserToken,
            msg->RequestText,
            m
        );

        Send(new IEventHandle(WorkloadServiceId, ev->Sender, proxyMsg));
    }

private:
    TActorId WorkloadServiceId;
    IWmSessionUpdater::EWmState FinalState;
};

class TQuerySessionReader {
public:
    struct Row {
        std::optional<std::string> WmPoolId;
        std::optional<std::string> WmState;
    };

public:
    TQuerySessionReader(TIntrusivePtr<IYdbSetup> ydb) {
        auto Result = ydb->ExecuteQuery(R"(
            SELECT SessionId, WmPoolId, WmState
            FROM `.sys/query_sessions`
            WHERE State = 'EXECUTING'
            ORDER By WmState
        )");

        UNIT_ASSERT_VALUES_EQUAL_C(Result.GetStatus(), NYdb::EStatus::SUCCESS, Result.GetIssues().ToString());

        auto rs = Result.GetResultSet(0);
        auto parser = std::make_unique<NYdb::TResultSetParser>(rs);

        while (parser->TryNextRow()) {
            auto wmState = parser->ColumnParser("WmState").GetOptionalUtf8();
            auto wmPoolId = parser->ColumnParser("WmPoolId").GetOptionalUtf8();

            Results.push_back(Row{.WmPoolId = wmPoolId, .WmState = wmState});
        }
    }

    Row operator[](size_t index) const {
        Y_ENSURE(index < Results.size());
        return Results[index];
    }

    size_t Size() const {
        return Results.size();
    }

private:
    TQueryRunnerResult Result;
    std::vector<Row> Results;
};

class TQuerySessionTestFixture {
public:
    TQuerySessionTestFixture(IWmSessionUpdater::EWmState state, const TString myPoolId, size_t limit = 10) {
        Ydb = TYdbSetupSettings()
            .NodeCount(1)
            .EnableResourcePools(true)
            // turn off to reduce "noise" in a log
            .EnableStreamingQueries(false)
            .ConcurrentQueryLimit(limit)
            .CreateSamplePool(true)
            .PoolId(myPoolId)
            .Create();

        auto& runtime = *Ydb->GetRuntime();
        auto workloadServiceId = MakeKqpWorkloadServiceId(runtime.GetNodeId(0));
        
        auto actor = new TKqpWorkloadProxyActor(state);
        auto newId = runtime.Register(actor);
        auto oldId = runtime.RegisterService(workloadServiceId, newId);
        
        actor->SetWorkloadServiceId(oldId);
    }

    TIntrusivePtr<IYdbSetup> GetYdb() {
        return Ydb;
    }

    TQuerySessionReader GetReader() {
        return TQuerySessionReader(Ydb);
    }

private:
    TIntrusivePtr<IYdbSetup> Ydb;
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceQuerySessions) {
    Y_UNIT_TEST(TestWmStateNone) {
        TQuerySessionTestFixture f(IWmSessionUpdater::NONE, "my_pool");
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        auto r = f.GetReader();

        UNIT_ASSERT_VALUES_EQUAL(r.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmState, "NONE");
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStatePending) {
        TQuerySessionTestFixture f(IWmSessionUpdater::PENDING, "my_pool");
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        auto r = f.GetReader();

        UNIT_ASSERT_VALUES_EQUAL(r.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmState, "PENDING");
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStateDelayed) {
        TQuerySessionTestFixture f(IWmSessionUpdater::DELAYED, "my_pool", 1);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        auto hangingRequest = f.GetYdb()->ExecuteQueryAsync(
            "select 121;",
            myPool.HangUpDuringExecution(true)
        );

        f.GetYdb()->WaitQueryExecution(hangingRequest);

        auto delayedRequest = f.GetYdb()->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, myPool);

        f.GetYdb()->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});
        f.GetYdb()->ContinueQueryExecution(hangingRequest);
        f.GetYdb()->WaitQueryExecution(delayedRequest, TDuration::Seconds(5));

        auto hangingResult = hangingRequest.GetResult();
        auto delayedResult = delayedRequest.GetResult();

        auto r = f.GetReader();

        UNIT_ASSERT_VALUES_EQUAL(r.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmState, "DELAYED");
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStateExited) {
        TQuerySessionTestFixture f(IWmSessionUpdater::EXITED, "my_pool");
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        auto r = f.GetReader();

        UNIT_ASSERT_VALUES_EQUAL(r.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmState, "EXITED");
        UNIT_ASSERT_VALUES_EQUAL(r[0].WmPoolId, "my_pool");
    }

}

}  // namespace NKikimr::NKqp
