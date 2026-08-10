#include <fmt/format.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/services/workload_manager/query_classifier.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/kqp/common/events/events.h>

namespace NKikimr::NWorkloadManager {
namespace {

class TestQueryClassifier : public NWorkloadManager::IQueryClassifier {
public:
    TestQueryClassifier(TPreCompileClassifyResult preClassifyResult, TPostCompileClassifyResult postClassifyResult)
        : PostCompileCalled(false)
        , PreClassifyResult(preClassifyResult)
        , PostClassifyResult(postClassifyResult)
    {}
    
    virtual ~TestQueryClassifier() = default;

    TPreCompileClassifyResult PreCompileClassify() override {
        return PreClassifyResult;
    }

    EState GetState() const override {
        if (std::holds_alternative<TPendingCompilation>(PreClassifyResult)) {
            return !PostCompileCalled ? EState::WaitCompile : EState::PostCompileDone;
        }

        return EState::PreCompileDone;
    }

    TPostCompileClassifyResult PostCompileClassify(const NKqp::TPreparedQueryHolder&, const NKqp::TUserRequestContext&) override {
        PostCompileCalled = true;
        return PostClassifyResult;
    }

private:
    bool PostCompileCalled;
    TPreCompileClassifyResult PreClassifyResult;
    TPostCompileClassifyResult PostClassifyResult;
};

template<typename TPreResult, typename TPostResult>
NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWith(TPreResult preResult, TPostResult postResult) {
    TPortManager tp;
    auto mbusport = tp.GetPort(2134);
    auto settings = Tests::TServerSettings(mbusport);

    settings.SetNodeCount(1);
    settings.SetUseRealThreads(false);

    Tests::TServer server(settings);
    Tests::TClient client(settings);
    auto& runtime = *server.GetRuntime();
    TActorId sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NActors::NLog::PRI_DEBUG);

    auto proxyId = NKqp::MakeKqpProxyID(runtime.GetNodeId(0));
    
    auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvQueryRequest::EventType) {
            // Replace the classifier after proxy set it
            auto* request = ev->Get<NKqp::TEvKqp::TEvQueryRequest>();
            request->SetWmQueryClassifier(std::make_shared<TestQueryClassifier>(preResult, postResult));
        }

        return false;
    };

    runtime.SetEventFilter(captureEvents);

    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
    ev->Record.MutableRequest()->SetQuery("SELECT 1;");

    runtime.Send(new IEventHandle(proxyId, sender, ev.Release()));
    return runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
}

template<typename TPreResult>
NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWithPreClassify(TPreResult preResult) {
    return RunQueryWith(preResult, NWorkloadManager::IQueryClassifier::TBypass{});
}

template<typename TPostResult>
NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWithPostClassify(TPostResult postResult) {
    return RunQueryWith(NWorkloadManager::IQueryClassifier::TPendingCompilation{}, postResult);
}

TString GetErrorMessageFromResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr r) {
    NYql::TIssues issues;
    NYql::IssuesFromMessage( r->Get()->Record.GetResponse().GetQueryIssues(), issues);
    UNIT_ASSERT(issues.Size());
    return issues.ToOneLineString();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpQueryPreClassifier) {
    Y_UNIT_TEST(ShouldBypassOnPreClassify) {
        auto reply = RunQueryWithPreClassify(NWorkloadManager::IQueryClassifier::TBypass());
        auto status = reply->Get()->Record.GetYdbStatus();

        UNIT_ASSERT_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldResolveDefaultOnPreClassify) {
        auto resolve = NWorkloadManager::IQueryClassifier::TResolvedPoolId{
            .PoolId = NResourcePool::DEFAULT_POOL_ID
        };
        auto reply = RunQueryWithPreClassify(resolve);
        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldRejectOnPreClassify) {
        auto reject = NWorkloadManager::IQueryClassifier::TReject{
            .Code = Ydb::StatusIds::ABORTED,
            .Message = "Reject by ShouldRejectOnPreClassify"
        };
        auto reply = RunQueryWithPreClassify(reject);

        UNIT_ASSERT(reply->Get()->Record.GetYdbStatus() == Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(GetErrorMessageFromResponse(reply), reject.Message);
    }
}

Y_UNIT_TEST_SUITE(KqpQueryPostClassifier) {
    Y_UNIT_TEST(ShouldBypassOnPostClassify) {
        auto reply = RunQueryWithPostClassify(NWorkloadManager::IQueryClassifier::TBypass());
        auto status = reply->Get()->Record.GetYdbStatus();

        UNIT_ASSERT_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldResolveDefaultOnPostClassify) {
        auto resolve = NWorkloadManager::IQueryClassifier::TResolvedPoolId{
            .PoolId = NResourcePool::DEFAULT_POOL_ID
        };
        auto reply = RunQueryWithPostClassify(resolve);
        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldRejectOnPostClassify) {
        auto reject = NWorkloadManager::IQueryClassifier::TReject{
            .Code = Ydb::StatusIds::ABORTED,
            .Message = "Rejected by ShouldRejectOnPostClassify"
        };
        auto reply = RunQueryWithPostClassify(reject);

        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(GetErrorMessageFromResponse(reply), reject.Message);
    }
}

} // namespace NKikimr::NWorkloadManager
