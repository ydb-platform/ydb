#include "ut_helpers_query.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <library/cpp/grpc/client/grpc_common.h>
#include <library/cpp/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NGrpc;

namespace NTestHelpers {

TString CreateQuerySession(const TGRpcClientConfig& clientConfig) {
    NGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::CreateSessionRequest request;
    TString sessionId;

    NGrpc::TResponseCallback<Ydb::Query::CreateSessionResponse> responseCb =
        [&sessionId](NGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::CreateSessionResponse&& response) -> void {
            UNIT_ASSERT(!grpcStatus.InternalError);
            UNIT_ASSERT_C(grpcStatus.GRpcStatusCode == 0, grpcStatus.Msg + " " + grpcStatus.Details);
            UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(response.session_id() != "");
            sessionId = response.session_id();
    };

    connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncCreateSession);

    return sessionId;
}

NGrpc::IStreamRequestCtrl::TPtr CheckAttach(NGrpc::TGRpcClientLow& clientLow, const TGRpcClientConfig& clientConfig,
    const TString& id, int code, bool& allDoneOk)
{
    const Ydb::StatusIds::StatusCode expected = static_cast<Ydb::StatusIds::StatusCode>(code);
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::AttachSessionRequest request;
    request.set_session_id(id);

    using TProcessor = typename NGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>::TPtr;
    auto promise = NThreading::NewPromise<TProcessor>();
    auto cb = [&allDoneOk, promise, expected](TGrpcStatus grpcStatus, TProcessor processor) mutable {
        UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
        auto resp = std::make_shared<Ydb::Query::SessionState>();
        processor->Read(resp.get(), [&allDoneOk, resp, promise, processor, expected](TGrpcStatus grpcStatus) mutable {
            UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
            allDoneOk &= (resp->status() == expected);
            if (!allDoneOk) {
                Cerr << "Expected status: " << expected << ", got response: " << resp->DebugString() << Endl;
            }
            promise.SetValue(processor);
        });
    };

    connection->DoStreamRequest<Ydb::Query::AttachSessionRequest, Ydb::Query::SessionState>(
        request,
        cb,
        &Ydb::Query::V1::QueryService::Stub::AsyncAttachSession);

    return promise.GetFuture().GetValueSync();
}

void CheckAttach(const TGRpcClientConfig& clientConfig, const TString& id, int code, bool& allDoneOk) {
    NGrpc::TGRpcClientLow clientLow;
    CheckAttach(clientLow, clientConfig, id, code, allDoneOk)->Cancel();
}

}
