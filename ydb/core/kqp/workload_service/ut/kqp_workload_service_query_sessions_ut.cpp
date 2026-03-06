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
        )");

        UNIT_ASSERT_VALUES_EQUAL_C(Result.GetStatus(), NYdb::EStatus::SUCCESS, Result.GetIssues().ToString());

        auto rs = Result.GetResultSet(0);
        Parser = std::make_unique<NYdb::TResultSetParser>(rs);
    }

    size_t TotalRows() const {
        return Result.GetResultSets().size();
    }

    Row Fetch() const {
        Parser->TryNextRow();
        auto wmState = Parser->ColumnParser("WmState").GetOptionalUtf8();
        auto wmPoolId = Parser->ColumnParser("WmPoolId").GetOptionalUtf8();

        return Row{.WmPoolId = wmPoolId, .WmState = wmState};
    }

private:
    TQueryRunnerResult Result;
    std::unique_ptr<NYdb::TResultSetParser> Parser;
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

        auto r = f.GetReader().Fetch();

        UNIT_ASSERT_VALUES_EQUAL(r.WmState, "NONE");
        UNIT_ASSERT_VALUES_EQUAL(r.WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStatePending) {
        TQuerySessionTestFixture f(IWmSessionUpdater::PENDING, "my_pool");
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        auto r = f.GetReader().Fetch();

        UNIT_ASSERT_VALUES_EQUAL(r.WmState, "PENDING");
        UNIT_ASSERT_VALUES_EQUAL(r.WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStateDelayed) {
        TQuerySessionTestFixture f(IWmSessionUpdater::PENDING, "my_pool", 1);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        auto hangingRequest = f.GetYdb()->ExecuteQueryAsync(
            TSampleQueries::TSelect42::Query,
            TQueryRunnerSettings().HangUpDuringExecution(true).PoolId("my_pool")
        );

        f.GetYdb()->WaitQueryExecution(hangingRequest);

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        f.GetYdb()->ContinueQueryExecution(hangingRequest);

        auto r = f.GetReader().Fetch();

        UNIT_ASSERT_VALUES_EQUAL(r.WmState, "DELAYED");
        UNIT_ASSERT_VALUES_EQUAL(r.WmPoolId, "my_pool");
    }

    Y_UNIT_TEST(TestWmStateExited) {
        TQuerySessionTestFixture f(IWmSessionUpdater::EXITED, "my_pool");
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");

        TSampleQueries::TSelect42::CheckResult(
            f.GetYdb()->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

        auto r = f.GetReader().Fetch();

        UNIT_ASSERT_VALUES_EQUAL(r.WmState, "EXITED");
        UNIT_ASSERT_VALUES_EQUAL(r.WmPoolId, "my_pool");
    }

}

}  // namespace NKikimr::NKqp
