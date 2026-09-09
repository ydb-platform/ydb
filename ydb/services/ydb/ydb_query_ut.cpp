#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/shutdown/events.h>
#include <ydb/core/kqp/common/shutdown/state.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NYdbGrpc;
using namespace NTestHelpers;

namespace {

struct TMockMonHttpRequest : NMonitoring::IMonHttpRequest {
    TCgiParameters Params_;

    explicit TMockMonHttpRequest(const TString& params) {
        Params_.Scan(params);
    }

    const TCgiParameters& GetParams() const override { return Params_; }
    IOutputStream& Output() override { Y_ABORT("Not implemented"); }
    HTTP_METHOD GetMethod() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPath() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPathInfo() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetUri() const override { Y_ABORT("Not implemented"); }
    const TCgiParameters& GetPostParams() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPostContent() const override { Y_ABORT("Not implemented"); }
    const THttpHeaders& GetHeaders() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetHeader(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TStringBuf GetCookie(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TString GetRemoteAddr() const override { Y_ABORT("Not implemented"); }
    TString GetServiceTitle() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonPage* GetPage() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override { Y_ABORT("Not implemented"); }
};

} // namespace

Y_UNIT_TEST_SUITE(YdbQueryService) {
    Y_UNIT_TEST(TestCreateAndAttachSession) {
        TKikimrWithGrpcAndRootSchema server;

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);

        UNIT_ASSERT(sessionId);

        for (const auto id : {"", "unknownSesson"}) {
            CheckAttach(clientConfig, id, Ydb::StatusIds::BAD_REQUEST, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);

        {
            // We expect to get reply from KQP proxy
            // and destroy session in the time of destroy stream
            CheckAttach(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);

        {
            // We expect session has been destroyed after stream cancellation.
            // Session destruction by the server is asynchronous relative to stream cancellation,
            // so we retry with backoff until the session is fully closed in KQP proxy.
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
            bool sessionDestroyed = false;
            do {
                bool ok = true;
                CheckAttach(clientConfig, sessionId, Ydb::StatusIds::BAD_SESSION, ok);
                if (ok) {
                    sessionDestroyed = true;
                    break;
                }
                Sleep(TDuration::MilliSeconds(50));
            } while (!sessionDestroyed && TInstant::Now() < deadline);
            UNIT_ASSERT_C(sessionDestroyed, "Query session '" << sessionId
                << "' was not destroyed within the timeout after stream cancellation");
        }

        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(TestAttachTwice) {
        TKikimrWithGrpcAndRootSchema server;

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);

        UNIT_ASSERT(sessionId);

        NYdbGrpc::TGRpcClientLow clientLow;
        auto p = CheckAttach(clientLow, clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);

        CheckAttach(clientConfig, sessionId, Ydb::StatusIds::SESSION_BUSY, allDoneOk);

        p->Cancel();

        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(TestForbidExecuteWithoutAttach) {
        TKikimrWithGrpcAndRootSchema server;

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        TString sessionId = CreateQuerySession(clientConfig);

        UNIT_ASSERT(sessionId);

        NYdbGrpc::TGRpcClientLow clientLow;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        {
            std::unique_ptr<Ydb::Query::V1::QueryService::Stub> stub;
            stub = Ydb::Query::V1::QueryService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Query::ExecuteQueryRequest request;
            request.set_session_id(sessionId);
            request.set_exec_mode(Ydb::Query::EXEC_MODE_EXECUTE);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            request.mutable_query_content()->set_text("SELECT 42");
            Ydb::Query::ExecuteQueryResponsePart response;
            auto reader = stub->ExecuteQuery(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::BAD_REQUEST);
                }
            }
        }
    }

    Y_UNIT_TEST(TestCreateDropAttachSession) {
        TKikimrWithGrpcAndRootSchema server;

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);

        UNIT_ASSERT(sessionId);

        {
            CheckDelete(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);

        {
            // We session has been destroyed by previous call
            CheckAttach(clientConfig, sessionId, Ydb::StatusIds::BAD_SESSION, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(TestCreateAttachAndDropAttachedSession) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_TRACE);
        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_SESSION, NActors::NLog::PRI_TRACE);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);

        UNIT_ASSERT(sessionId);

        NYdbGrpc::TGRpcClientLow clientLow;
        auto p = CheckAttach(clientLow, clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);

        UNIT_ASSERT(allDoneOk);

        {
            CheckDelete(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);

        {
            EnsureSessionClosed(p, Ydb::StatusIds::SUCCESS, allDoneOk);
        }

        p->Cancel();

        UNIT_ASSERT(allDoneOk);

        {
            CheckAttach(clientConfig, sessionId, Ydb::StatusIds::BAD_SESSION, allDoneOk);
        }

        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(TestAttachSessionNodeShutdownHint) {
        TKikimrWithGrpcAndRootSchema server;
        auto* runtime = server.GetRuntime();
        runtime->GetAppData().FeatureFlags.SetEnableNodeShutdownHints(true);
        runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_TRACE);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);
        UNIT_ASSERT(sessionId);

        NYdbGrpc::TGRpcClientLow clientLow;
        auto p = CheckAttach(clientLow, clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
        UNIT_ASSERT(allDoneOk);

        auto shutdownState = MakeIntrusive<NKikimr::NKqp::TKqpShutdownState>();
        auto kqpProxy = NKikimr::NKqp::MakeKqpProxyID(runtime->GetNodeId(0));
        runtime->Send(
            kqpProxy,
            runtime->AllocateEdgeActor(),
            new NKikimr::NKqp::TEvKqp::TEvInitiateShutdownRequest(shutdownState));

        EnsureSessionClosedWithHint(p,
            Ydb::StatusIds::SUCCESS,
            Ydb::Query::SessionState::kNodeShutdown,
            allDoneOk);

        p->Cancel();
        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(TestAttachSessionSessionShutdownHint) {
        TKikimrWithGrpcAndRootSchema server;
        auto* runtime = server.GetRuntime();
        runtime->GetAppData().FeatureFlags.SetEnableNodeShutdownHints(true);
        runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_TRACE);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateQuerySession(clientConfig);
        UNIT_ASSERT(sessionId);

        NYdbGrpc::TGRpcClientLow clientLow;
        auto p = CheckAttach(clientLow, clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
        UNIT_ASSERT(allDoneOk);

        TMockMonHttpRequest monReq("force_shutdown=1");

        auto edgeActor = runtime->AllocateEdgeActor();
        auto kqpProxy = NKikimr::NKqp::MakeKqpProxyID(runtime->GetNodeId(0));
        runtime->Send(kqpProxy, edgeActor, new NActors::NMon::TEvHttpInfo(monReq));
        runtime->GrabEdgeEvent<NActors::NMon::TEvHttpInfoRes>(edgeActor, TDuration::Seconds(5));

        EnsureSessionClosedWithHint(p,
            Ydb::StatusIds::SUCCESS,
            Ydb::Query::SessionState::kSessionShutdown,
            allDoneOk);

        p->Cancel();
        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(ExecuteQueryBuiltInRetrySuccess) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(location));
        NYdb::NQuery::TQueryClient client(driver);

        auto result = client.ExecuteQuery(
            "SELECT 1 AS x;",
            NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);

        driver.Stop(true);
    }

    Y_UNIT_TEST(ExecuteQueryBuiltInRetryDisabled) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(location));
        auto settings = NYdb::NQuery::TClientSettings()
            .RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0));
        NYdb::NQuery::TQueryClient client(driver, settings);

        auto result = client.ExecuteQuery(
            "SELECT 1 AS x;",
            NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        driver.Stop(true);
    }

    Y_UNIT_TEST(ExecuteQueryNoDoubleRetryInRetryQuery) {
        const ui32 outerMaxRetries = 2;
        const ui32 innerMaxRetries = 5;
        const auto outerRetrySettings = NYdb::NRetry::TRetryOperationSettings()
            .MaxRetries(outerMaxRetries)
            .Idempotent(true)
            .FastBackoffSettings(NYdb::NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(50)).Ceiling(2))
            .SlowBackoffSettings(NYdb::NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(50)).Ceiling(2));
        const auto innerRetrySettings = NYdb::NRetry::TRetryOperationSettings()
            .MaxRetries(innerMaxRetries)
            .Idempotent(true)
            .FastBackoffSettings(NYdb::NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(50)).Ceiling(2))
            .SlowBackoffSettings(NYdb::NRetry::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(50)).Ceiling(2));
        const auto executeQuerySettings = NYdb::NQuery::TExecuteQuerySettings()
            .RetrySettings(innerRetrySettings);

        // Use an unreachable endpoint to inject transport failures on every ExecuteQuery attempt.
        TPortManager portManager;
        const ui16 badPort = portManager.GetPort(2136);
        const TString badLocation = TStringBuilder() << "localhost:" << badPort;

        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(badLocation));
        NYdb::NQuery::TQueryClient client(
            driver,
            NYdb::NQuery::TClientSettings().RetrySettings(outerRetrySettings));

        ui32 outerAttempts = 0;
        const auto startedAt = TInstant::Now();
        const auto status = client.RetryQuerySync([&](NYdb::NQuery::TQueryClient& queryClient) {
            ++outerAttempts;
            UNIT_ASSERT(queryClient.GetInRetryOperationContext());
            return queryClient.ExecuteQuery(
                "SELECT 1 AS x;",
                NYdb::NQuery::TTxControl::NoTx(),
                executeQuerySettings).GetValueSync();
        }, outerRetrySettings);
        const auto duration = TInstant::Now() - startedAt;

        UNIT_ASSERT(!status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(outerAttempts, outerMaxRetries + 1);
        // Inner retries are suppressed inside RetryQuerySync. Without that guard, each outer attempt
        // would run up to (innerMaxRetries + 1) ExecuteQuery tries with backoff and take much longer.
        UNIT_ASSERT(duration < TDuration::Seconds(1));

        driver.Stop(true);
    }

}
