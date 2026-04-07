#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>

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
            // We expect session has been destroyed
            CheckAttach(clientConfig, sessionId, Ydb::StatusIds::BAD_SESSION, allDoneOk);
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

}
