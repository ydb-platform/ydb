#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/draft/ydb_query_v1.grpc.pb.h>

using namespace NYdb;
using namespace NGrpc;

namespace {

TString CreateSession(const NGRpcProxy::TGRpcClientConfig& clientConfig) {
    NGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::CreateSessionRequest request;
    TString sessionId;

    NGrpc::TResponseCallback<Ydb::Query::CreateSessionResponse> responseCb =
        [&sessionId](NGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::CreateSessionResponse&& response) -> void {
            UNIT_ASSERT(!grpcStatus.InternalError);
            UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
            UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(response.session_id() != "");
            sessionId = response.session_id();
    };

    connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncCreateSession);

    return sessionId;
}

using TProcessor = typename NGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>::TPtr;
void CheckAttach(const NGRpcProxy::TGRpcClientConfig& clientConfig, const TString& id,
    const Ydb::StatusIds::StatusCode expected, bool& allDoneOk)
{
    NGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::AttachSessionRequest request;
    request.set_session_id(id);

    auto promise = NThreading::NewPromise<TProcessor>();
    auto cb = [&allDoneOk, promise, expected](TGrpcStatus grpcStatus, TProcessor processor) mutable {
        UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
        auto resp = std::make_shared<Ydb::Query::SessionState>();
        processor->Read(resp.get(), [&allDoneOk, resp, promise, processor, expected](TGrpcStatus grpcStatus) mutable {
            UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
            allDoneOk &= (resp->status() == expected);
            if (!allDoneOk) {
                Cerr << "Got attach response: " << resp->DebugString() << Endl;
            }
            promise.SetValue(processor);
        });
    };

    connection->DoStreamRequest<Ydb::Query::AttachSessionRequest, Ydb::Query::SessionState>(
        request,
        cb,
        &Ydb::Query::V1::QueryService::Stub::AsyncAttachSession);

    auto processor = promise.GetFuture().GetValueSync();
    processor->Cancel();
}

}

Y_UNIT_TEST_SUITE(YdbQueryService) {
    Y_UNIT_TEST(TestCreateAndAttachSession) {
        TKikimrWithGrpcAndRootSchema server;

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = true;

        TString sessionId = CreateSession(clientConfig);

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
}
