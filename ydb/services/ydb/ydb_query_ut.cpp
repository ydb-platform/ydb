#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/draft/ydb_query_v1.grpc.pb.h>

using namespace NYdb;
using namespace NGrpc;

Y_UNIT_TEST_SUITE(YdbQueryService) {
    Y_UNIT_TEST(TestCreateSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = false;

        {
            NGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

            Ydb::Query::CreateSessionRequest request;

            NGrpc::TResponseCallback<Ydb::Query::CreateSessionResponse> responseCb =
                [&allDoneOk](NGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::CreateSessionResponse&& response) -> void {
                    UNIT_ASSERT(!grpcStatus.InternalError);
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    UNIT_ASSERT(response.session_id() != "");
                    allDoneOk = true;
            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncCreateSession);
        }

        UNIT_ASSERT(allDoneOk);
        allDoneOk = false;

        {
            using TProcessor = typename NGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>::TPtr;

            NGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

            Ydb::Query::AttachSessionRequest request;

            auto cb = [&allDoneOk](TGrpcStatus grpcStatus, TProcessor processor) {
                if (grpcStatus.Ok()) {
                    Ydb::Query::SessionState resp;
                    processor->Read(&resp, [&allDoneOk](TGrpcStatus grpcStatus) {
                        allDoneOk = grpcStatus.GRpcStatusCode == grpc::StatusCode::UNIMPLEMENTED;
                    });
                }
            };

            connection->DoStreamRequest<Ydb::Query::AttachSessionRequest, Ydb::Query::SessionState>(
                request,
                cb,
                &Ydb::Query::V1::QueryService::Stub::AsyncAttachSession);
        }

        UNIT_ASSERT(allDoneOk);
    }
}
