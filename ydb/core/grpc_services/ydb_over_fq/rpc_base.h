#pragma once

#include <ydb/core/grpc_services/local_grpc/local_grpc.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService::NYdbOverFq {


template <typename TDerived, typename TReq, typename TResp>
class TRpcBase
    : public TRpcOperationRequestActor<
        TDerived, TGrpcRequestOperationCall<TReq, TResp>>
    , public NLocalGrpc::TCaller {
public:
    using TBase = TRpcOperationRequestActor<TDerived, TGrpcRequestOperationCall<TReq, TResp>>;

    TRpcBase(IRequestOpCtx* request, TActorId grpcProxyId)
        : TBase{request}
        , TCaller{std::move(grpcProxyId)}
    {}

    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::GetProtoRequest;
    using TBase::Request_;

protected:
    // if status is not success, replies error, returns true
    bool HandleFailure(const Ydb::Operations::Operation& operation, std::string_view opName, const TActorContext& ctx) {
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);

        TString errorMsg = TStringBuilder{} << "pseudo " << TDerived::RpcName << " actorId: " << TBase::SelfId().ToString() <<
            " failed to " << opName << " with status: " << Ydb::StatusIds::StatusCode_Name(operation.status());
        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            errorMsg << ", issues: " << issues.ToOneLineString());
        issues.AddIssue(errorMsg);

        TBase::Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, operation.issues(), ctx);
        return true;
    }

    template <typename TGrpcReq>
    void HandleResponse(typename NLocalGrpc::TEvent<typename NLocalGrpc::TCall<TGrpcReq>::TResponse>::TPtr& ev, const TActorContext& ctx) {
        using TCall = NLocalGrpc::TCall<TGrpcReq>;
        using TResponse = typename TCall::TResponse;
        using TResult = typename TCall::TResult;

        const TResponse& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), TResponse::GetDescriptor()->name(), ctx)) [[unlikely]] {
            return;
        }

        TResult result;
        resp.operation().result().UnpackTo(&result);

        static_cast<TDerived*>(this)->Handle(result, ctx);
    }

    template <typename TRequest>
    void MakeLocalCall(TRequest&& req, const TActorContext& ctx) {
        TCaller::MakeLocalCall(std::forward<TRequest>(req), Request_, ctx);
    }
};

} // namespace NKikimr::NGRpcService::NYdbOverFq
