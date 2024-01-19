#include "ut_helpers_query.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/library/grpc/client/grpc_common.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdbGrpc;

namespace NTestHelpers {

TString CreateQuerySession(const TGRpcClientConfig& clientConfig) {
    NYdbGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::CreateSessionRequest request;
    TString sessionId;

    NYdbGrpc::TResponseCallback<Ydb::Query::CreateSessionResponse> responseCb =
        [&sessionId](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::CreateSessionResponse&& response) -> void {
            UNIT_ASSERT(!grpcStatus.InternalError);
            UNIT_ASSERT_C(grpcStatus.GRpcStatusCode == 0, grpcStatus.Msg + " " + grpcStatus.Details);
            UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(response.session_id() != "");
            UNIT_ASSERT(response.node_id() != 0);
            sessionId = response.session_id();
    };

    connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncCreateSession);

    return sessionId;
}

NYdbGrpc::IStreamRequestCtrl::TPtr CheckAttach(NYdbGrpc::TGRpcClientLow& clientLow, const TGRpcClientConfig& clientConfig,
    const TString& id, int code, bool& allDoneOk)
{
    const Ydb::StatusIds::StatusCode expected = static_cast<Ydb::StatusIds::StatusCode>(code);
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::AttachSessionRequest request;
    request.set_session_id(id);

    using TProcessor = typename NYdbGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>::TPtr;
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

void CheckAttach(const TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk) {
    NYdbGrpc::TGRpcClientLow clientLow;
    CheckAttach(clientLow, clientConfig, id, expected, allDoneOk)->Cancel();
}

void CheckDelete(const TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk) {
    NYdbGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::DeleteSessionRequest request;
    request.set_session_id(id);

    NYdbGrpc::TResponseCallback<Ydb::Query::DeleteSessionResponse> responseCb =
        [&allDoneOk, expected](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::DeleteSessionResponse&& response) -> void {
            UNIT_ASSERT(!grpcStatus.InternalError);
            UNIT_ASSERT_C(grpcStatus.GRpcStatusCode == 0, grpcStatus.Msg + " " + grpcStatus.Details);
            allDoneOk &= (response.status() == expected);
            if (!allDoneOk) {
                Cerr << "Expected status: " << expected << ", got response: " << response.DebugString() << Endl;
            }
    };

    connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncDeleteSession);
}

void EnsureSessionClosed(NYdbGrpc::IStreamRequestCtrl::TPtr p, int expected, bool& allDoneOk) {
    using TProcessor = typename NYdbGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>::TPtr;
    TProcessor processor = dynamic_cast<NYdbGrpc::IStreamRequestReadProcessor<Ydb::Query::SessionState>*>(p.Get());
    UNIT_ASSERT(processor);

    auto promise = NThreading::NewPromise<void>();
    auto resp = std::make_shared<Ydb::Query::SessionState>();
    processor->Read(resp.get(), [&allDoneOk, resp, promise, expected](TGrpcStatus grpcStatus) mutable {
        UNIT_ASSERT(grpcStatus.GRpcStatusCode == grpc::StatusCode::OK);
        allDoneOk &= (resp->status() == expected);
        if (!allDoneOk) {
            Cerr << "Expected status: " << expected << ", got response: " << resp->DebugString() << Endl;
        }
        promise.SetValue();
    });
    promise.GetFuture().Wait();
}

}
