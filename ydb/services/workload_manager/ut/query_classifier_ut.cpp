#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/query_classifier.h>

#include <fmt/format.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NWorkloadManager {
namespace {

class TestQueryClassifier: public NWorkloadManager::IQueryClassifier {
public:
    TestQueryClassifier(TPreCompileClassifyResult preClassifyResult, TPostCompileClassifyResult postClassifyResult)
        : PostCompileCalled(false)
        , PreClassifyResult(preClassifyResult)
        , PostClassifyResult(postClassifyResult)
    {
    }

    virtual ~TestQueryClassifier() = default;

    TPreCompileClassifyResult PreCompileClassify(const NKqp::TUserRequestContext&) override {
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

class TTestGateway: public IGateway {
public:
    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString&, TClassifyContext) override {
        ++Calls;
        return std::make_shared<TestQueryClassifier>(PreResult, PostResult);
    }

    ui64 Calls = 0;
    IQueryClassifier::TPreCompileClassifyResult PreResult = IQueryClassifier::TBypass{};
    IQueryClassifier::TPostCompileClassifyResult PostResult = IQueryClassifier::TBypass{};
};

TString GetErrorMessageFromResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr r) {
    NYql::TIssues issues;
    NYql::IssuesFromMessage(r->Get()->Record.GetResponse().GetQueryIssues(), issues);
    UNIT_ASSERT(issues.Size());
    return issues.ToOneLineString();
}

struct TQueryClassifierFixture: public NUnitTest::TBaseFixture {
    TPortManager PortManager;
    Tests::TServerSettings Settings{ PortManager.GetPort(2134) };
    std::unique_ptr<Tests::TServer> Server;
    TTestActorRuntime* Runtime = nullptr;
    std::shared_ptr<TTestGateway> Gateway = std::make_shared<TTestGateway>();
    TActorId ProxyId;
    TActorId Sender;

    void SetUp(NUnitTest::TTestContext&) override {
        Settings.SetNodeCount(1);
        Settings.SetUseRealThreads(false);
        Server = std::make_unique<Tests::TServer>(Settings);
        Runtime = Server->GetRuntime();
        Runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        Runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NActors::NLog::PRI_DEBUG);
        Runtime->GetAppData(0).WorkloadManagerGateway = Gateway;
        ProxyId = NKqp::MakeKqpProxyID(Runtime->GetNodeId(0));
        Sender = Runtime->AllocateEdgeActor();
    }

    NKqp::TEvKqp::TEvQueryResponse::TPtr Send(std::function<void(NKikimrKqp::TQueryRequest&)> fill) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetTimeoutMs(30000);
        fill(*ev->Record.MutableRequest());
        Runtime->Send(new IEventHandle(ProxyId, Sender, ev.Release()));
        auto reply = Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(Sender, TDuration::Seconds(30));
        UNIT_ASSERT(reply);
        return reply;
    }

    NKqp::TEvKqp::TEvQueryResponse::TPtr SendAssertSuccess(std::function<void(NKikimrKqp::TQueryRequest&)> fill) {
        auto reply = Send(std::move(fill));
        const auto& record = reply->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS, record.GetResponse().GetQueryIssues());
        return reply;
    }

    NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWith(
        IQueryClassifier::TPreCompileClassifyResult preResult, IQueryClassifier::TPostCompileClassifyResult postResult) {
        Gateway->PreResult = std::move(preResult);
        Gateway->PostResult = std::move(postResult);
        return Send([](NKikimrKqp::TQueryRequest& request) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetQuery("SELECT 1;");
        });
    }

    NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWithPreClassify(IQueryClassifier::TPreCompileClassifyResult preResult) {
        return RunQueryWith(std::move(preResult), IQueryClassifier::TBypass{});
    }

    NKqp::TEvKqp::TEvQueryResponse::TPtr RunQueryWithPostClassify(IQueryClassifier::TPostCompileClassifyResult postResult) {
        return RunQueryWith(IQueryClassifier::TPendingCompilation{}, std::move(postResult));
    }

    void ExecuteSelect42(std::function<void(NKikimrKqp::TQueryRequest&)> fill = {}) {
        SendAssertSuccess([&](NKikimrKqp::TQueryRequest& request) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetQuery("SELECT 42;");
            if (fill) {
                fill(request);
            }
        });
    }

    void PrepareSelect42(bool warmup) {
        SendAssertSuccess([&](NKikimrKqp::TQueryRequest& request) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.SetQuery("SELECT 42;");
            request.SetIsWarmupCompilation(warmup);
        });
    }

    TString CreateSession() {
        auto request = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        Runtime->Send(new IEventHandle(ProxyId, Sender, request.Release()));
        auto reply = Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvCreateSessionResponse>(Sender, TDuration::Seconds(30));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        return reply->Get()->Record.GetResponse().GetSessionId();
    }
};

}   // anonymous namespace

Y_UNIT_TEST_SUITE(KqpQueryPreClassifier) {
    Y_UNIT_TEST_F(ShouldBypassOnPreClassify, TQueryClassifierFixture) {
        auto reply = RunQueryWithPreClassify(NWorkloadManager::IQueryClassifier::TBypass());
        auto status = reply->Get()->Record.GetYdbStatus();

        UNIT_ASSERT_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_F(ShouldResolveDefaultOnPreClassify, TQueryClassifierFixture) {
        auto resolve = NWorkloadManager::IQueryClassifier::TResolvedPoolId{ .PoolId = NResourcePool::DEFAULT_POOL_ID };
        auto reply = RunQueryWithPreClassify(resolve);
        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_F(ShouldRejectOnPreClassify, TQueryClassifierFixture) {
        auto reject =
            NWorkloadManager::IQueryClassifier::TReject{ .Code = Ydb::StatusIds::ABORTED, .Message = "Reject by ShouldRejectOnPreClassify" };
        auto reply = RunQueryWithPreClassify(reject);

        UNIT_ASSERT(reply->Get()->Record.GetYdbStatus() == Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(GetErrorMessageFromResponse(reply), reject.Message);
    }
}

Y_UNIT_TEST_SUITE(KqpQueryPostClassifier) {
    Y_UNIT_TEST_F(ShouldBypassOnPostClassify, TQueryClassifierFixture) {
        auto reply = RunQueryWithPostClassify(NWorkloadManager::IQueryClassifier::TBypass());
        auto status = reply->Get()->Record.GetYdbStatus();

        UNIT_ASSERT_EQUAL(status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_F(ShouldResolveDefaultOnPostClassify, TQueryClassifierFixture) {
        auto resolve = NWorkloadManager::IQueryClassifier::TResolvedPoolId{ .PoolId = NResourcePool::DEFAULT_POOL_ID };
        auto reply = RunQueryWithPostClassify(resolve);
        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_F(ShouldRejectOnPostClassify, TQueryClassifierFixture) {
        auto reject =
            NWorkloadManager::IQueryClassifier::TReject{ .Code = Ydb::StatusIds::ABORTED, .Message = "Rejected by ShouldRejectOnPostClassify" };
        auto reply = RunQueryWithPostClassify(reject);

        UNIT_ASSERT_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(GetErrorMessageFromResponse(reply), reject.Message);
    }
}

Y_UNIT_TEST_SUITE(KqpQueryClassifierCreation) {
    Y_UNIT_TEST_F(ShouldCreateForUserQuery, TQueryClassifierFixture) {
        const ui64 before = Gateway->Calls;
        ExecuteSelect42();
        UNIT_ASSERT_GT(Gateway->Calls, before);
    }

    Y_UNIT_TEST_F(ShouldSkipInternalCall, TQueryClassifierFixture) {
        const ui64 before = Gateway->Calls;
        ExecuteSelect42([](NKikimrKqp::TQueryRequest& request) {
            request.SetIsInternalCall(true);
        });
        UNIT_ASSERT_VALUES_EQUAL(Gateway->Calls, before);
    }

    Y_UNIT_TEST_F(ShouldSkipWarmupCompilation, TQueryClassifierFixture) {
        const ui64 before = Gateway->Calls;
        PrepareSelect42(/*warmup=*/false);
        const ui64 afterPrepare = Gateway->Calls;
        UNIT_ASSERT_GT(afterPrepare, before);

        PrepareSelect42(/*warmup=*/true);
        UNIT_ASSERT_VALUES_EQUAL(Gateway->Calls, afterPrepare);
    }

    Y_UNIT_TEST_F(ShouldSkipBeginAndCommit, TQueryClassifierFixture) {
        const TString sessionId = CreateSession();
        const ui64 before = Gateway->Calls;

        auto begin = SendAssertSuccess([&](NKikimrKqp::TQueryRequest& request) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
            request.SetSessionId(sessionId);
            request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        });
        const TString txId = begin->Get()->Record.GetResponse().GetTxMeta().id();

        SendAssertSuccess([&](NKikimrKqp::TQueryRequest& request) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
            request.SetSessionId(sessionId);
            request.MutableTxControl()->set_tx_id(txId);
        });

        UNIT_ASSERT_VALUES_EQUAL(Gateway->Calls, before);
    }
}

}   // namespace NKikimr::NWorkloadManager
