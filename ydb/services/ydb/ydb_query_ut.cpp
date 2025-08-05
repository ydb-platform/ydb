#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>

#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NYdbGrpc;
using namespace NTestHelpers;

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

}
