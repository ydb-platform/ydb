#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/proxy_service/kqp_session_state.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

namespace NKikimr::NKqp {

namespace {

using namespace NWorkload;
using namespace NYdb;
using namespace NActors;

class TWmSwallowerMock : public IWmSessionUpdater {
public:
    TWmSwallowerMock() 
    {}

    void SetUpdater(std::shared_ptr<IWmSessionUpdater> inner) {
        Inner = std::move(inner);
    }

    std::shared_ptr<IWmSessionUpdater> GetUpdater() const {
        return Inner;
    }

    void SetRequestState(EWmState, TInstant timestamp) override {
        Cerr << "--- Captured state: at " << timestamp << Endl;
    }

    void SetPoolId(TString poolId) override {
        Inner->SetPoolId(poolId);
        Cerr << "--- Set pool Id " << poolId << Endl;
    }

private:
    std::shared_ptr<IWmSessionUpdater> Inner;
};

class TKqpWorkloadProxyActor : public TActorBootstrapped<TKqpWorkloadProxyActor> {
    TActorId WorkloadServiceId;
    std::shared_ptr<TWmSwallowerMock> WmSessionUpdaterMock;

public:
    TKqpWorkloadProxyActor(std::shared_ptr<TWmSwallowerMock> mock)
        : WmSessionUpdaterMock(mock)
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
        WmSessionUpdaterMock->SetUpdater(msg->WmSessionUpdater);
        
        auto proxyMsg = new NWorkload::TEvPlaceRequestIntoPool(
            msg->DatabaseId, msg->SessionId, msg->PoolId,
            msg->UserToken, msg->RequestText, WmSessionUpdaterMock
        );

        Send(new IEventHandle(WorkloadServiceId, ev->Sender, proxyMsg));
    }
};

/*
template<typename R, typename TFetchFunc>
R GetFieldFromResultSet(const Ydb::ResultSet& rs, size_t rowIndex, TFetchFunc&& fnc) {
    NYdb::TResultSetParser parser(rs);
    parser.TryNextRow();

    for (size_t i = 0; i < rowIndex; ++i) {
        parser.TryNextRow();
    }

    return fnc(parser);
}

TString GetWmStateFromResultSet(const Ydb::ResultSet& rs, size_t rowIndex) {
    return GetFieldFromResultSet<TString>(rs, rowIndex, [](NYdb::TResultSetParser& parser) {
        auto opt = parser.ColumnParser("WmState").GetOptionalUtf8();
        return opt ? TString(*opt) : TString();
    });
}

TString GetWMPoolIdFromResultSet(const Ydb::ResultSet& rs, size_t rowIndex) {
    return GetFieldFromResultSet<TString>(rs, rowIndex, [](NYdb::TResultSetParser& parser) {
        auto opt = parser.ColumnParser("WmPoolId").GetOptionalUtf8();
        return opt ? TString(*opt) : TString();
    });
}
*/

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceQuerySessions) {
    Y_UNIT_TEST(TestWmStateInterception) {
        auto ydb = TYdbSetupSettings()
            .NodeCount(1)
            .EnableResourcePools(true)
            // turn off to reduce "noise" in a log
            .EnableStreamingQueries(false)
            .CreateSamplePool(true)
            .PoolId("my_pool")
            .Create();

        auto& runtime = *ydb->GetRuntime();
        auto workloadServiceId = MakeKqpWorkloadServiceId(runtime.GetNodeId(0));
        auto mock = std::make_shared<TWmSwallowerMock>();
        
        auto actor = new TKqpWorkloadProxyActor(mock);
        auto newId = runtime.Register(actor);
        auto oldId = runtime.RegisterService(workloadServiceId, newId);
        
        actor->SetWorkloadServiceId(oldId);

        auto myPoolId = "my_pool";
        auto defPool = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
        auto myPool = TQueryRunnerSettings().PoolId(myPoolId);

        TSampleQueries::TSelect42::CheckResult(
            ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, myPool));

    }
}

}  // namespace NKikimr::NKqp
