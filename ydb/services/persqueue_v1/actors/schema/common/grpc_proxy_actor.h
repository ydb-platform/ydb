#pragma once

#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcProxy::V1 {

inline NYql::TIssue FillIssue(const TString& errorReason, const size_t errorCode) {
    NYql::TIssue res(NYql::TPosition(), errorReason);
    res.SetCode(errorCode, NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
    return res;
}

inline Ydb::PersQueue::ErrorCode::ErrorCode AsIssueCode(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::SUCCESS:
            return Ydb::PersQueue::ErrorCode::OK;
        case Ydb::StatusIds::UNAUTHORIZED:
            return Ydb::PersQueue::ErrorCode::ACCESS_DENIED;
        default:
            return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
    }
}
    
    

template<class TDerived, class TRequest>
class TGrpcProxyActor : public NGRpcService::TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = NGRpcService::TRpcOperationRequestActor<TDerived, TRequest>;
public:
    TGrpcProxyActor(NGRpcService::IRequestOpCtx* request)
        : NGRpcService::TRpcOperationRequestActor<TDerived, TRequest>(request)
    {
    }

    ~TGrpcProxyActor() = default;

    void Bootstrap(const TActorContext& ctx) {
        NGRpcService::TRpcOperationRequestActor<TDerived, TRequest>::Bootstrap(ctx);

        if (this->Request_->GetSerializedToken().empty()) {
            if (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                      "Unauthenticated access is forbidden, please provide credentials");
            }
        }

        static_cast<TDerived*>(this)->DoAction();
    }

protected:

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
        return this->Request_->GetSerializedToken().empty() ? nullptr : new NACLib::TUserToken(this->Request_->GetSerializedToken());
    }

    void ReplyWithError(Ydb::StatusIds::StatusCode status, const TString& messageText) {
        if (IsDead) {
            return;
        }

        this->Request_->RaiseIssue(FillIssue(messageText, AsIssueCode(status)));
        this->Request_->ReplyWithYdbStatus(status);
        this->Die(this->ActorContext());

        IsDead = true;
    }

    template<class TProtoResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status, const TProtoResult& result) {
        if (IsDead) {
            return;
        }

        this->Request_->SendResult(result, status);
        this->Die(this->ActorContext());

        IsDead = true;
    }

private:
    bool IsDead = false;
};

} // namespace NKikimr::NGRpcProxy::V1
