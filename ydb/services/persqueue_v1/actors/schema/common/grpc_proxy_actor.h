#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

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
            const bool internalRequest = !!dynamic_cast<NGRpcService::IInternalRequestCtx*>(this->Request_.get());
            if (!internalRequest &&
                (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()))
            {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                      "Unauthenticated access is forbidden, please provide credentials");
            }
        }

        if (GetFederatedPathContext()) {
            TopicPath = this->GetProtoRequest()->path();
        } else {
            auto resolved = NGRpcService::ResolveTopicSchemaPath(*this->Request_, this->GetProtoRequest()->path());
            if (resolved.IsFail()) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, resolved.GetErrorMessage());
            }
            TopicPath = resolved.DetachResult().Path;
        }
        static_cast<TDerived*>(this)->DoAction();
    }

protected:
    std::shared_ptr<const NPathAliasing::TPathContext> GetFederatedPathContext() const {
        return this->Request_->HasActivePathRewriting() && !AppData()->PQConfig.GetTopicsAreFirstClassCitizen()
            ? this->Request_->GetPathRewriteSettings().Context : nullptr;
    }

    TString GetLogicalDatabase() const {
        return GetFederatedPathContext() ? this->Request_->GetLogicalDatabaseName().GetOrElse("") : TString{};
    }

    bool ResolveDeadLetterQueue(TString& path) {
        if (path.StartsWith("sqs://") || !this->Request_->HasActivePathRewriting()) {
            return true;
        }
        auto resolved = [&]() -> TConclusion<NPathAliasing::TResolvedSchemaPath> {
            if (GetFederatedPathContext()) {
                auto candidate = NPQ::NNameResolver::ResolveName(GetLogicalDatabase(), path);
                if (!candidate) {
                    return TConclusionStatus::Fail(candidate.error());
                }
                auto resolvedPath = this->Request_->NormalizePath(candidate->Path);
                const auto database = GetDatabase();
                if (resolvedPath.IsSuccess()
                    && resolvedPath->Outcome != NPathAliasing::EPathRewriteOutcome::Rewritten
                    && CanonizePath(GetLogicalDatabase()) == database) {
                    // The legacy federation parser can legitimately navigate
                    // outside PQ's root (for example into LbUserDatabaseRoot).
                    // Keep its original spelling when neither operand changed.
                    return NPathAliasing::TResolvedSchemaPath{path, resolvedPath->Outcome};
                }
                if (resolvedPath.IsSuccess() && !database.empty() && resolvedPath->Path != database
                    && (resolvedPath->Path.size() <= database.size()
                        || !resolvedPath->Path.StartsWith(database)
                        || resolvedPath->Path[database.size()] != '/')) {
                    // The DLQ checker would otherwise interpret an outside
                    // absolute target as a database-relative topic name.
                    return TConclusionStatus::Fail("Rewritten topic path is outside the request database");
                }
                if (resolvedPath.IsSuccess()) {
                    auto finalName = NPQ::NNameResolver::ResolveName(database, resolvedPath->Path);
                    if (!finalName || finalName->Path != resolvedPath->Path) {
                        // The downstream checker/describer still parses a
                        // federation name. Never let it retarget an alias.
                        return TConclusionStatus::Fail("Rewritten topic path is changed by federation naming");
                    }
                }
                return resolvedPath;
            }
            return NGRpcService::ResolveTopicSchemaPath(*this->Request_, path);
        }();
        if (resolved.IsFail()) {
            ReplyWithError(Ydb::StatusIds::BAD_REQUEST, resolved.GetErrorMessage());
            return false;
        }
        path = resolved.DetachResult().Path;
        return true;
    }

    template<class TConsumer>
    bool ResolveConsumerSchemaReferences(TConsumer& consumer) {
        if (consumer.has_shared_consumer_type()
            && consumer.shared_consumer_type().has_dead_letter_policy()
            && consumer.shared_consumer_type().dead_letter_policy().has_move_action()) {
            return ResolveDeadLetterQueue(*consumer.mutable_shared_consumer_type()
                ->mutable_dead_letter_policy()->mutable_move_action()->mutable_dead_letter_queue());
        }
        return true;
    }

    const TString& GetTopicPath() const {
        return TopicPath;
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
        return this->Request_->GetSerializedToken().empty() ? nullptr : new NACLib::TUserToken(this->Request_->GetSerializedToken());
    }

    TString GetDatabase() const {
        return CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));
    }

    void ReplyWithError(Ydb::StatusIds::StatusCode status, const TString& messageText) {
        ReplyWithError(status, messageText, AsIssueCode(status));
    }

    void ReplyWithError(Ydb::StatusIds::StatusCode status, const TString& messageText, Ydb::PersQueue::ErrorCode::ErrorCode issueCode) {
        if (IsDead) {
            return;
        }

        this->Request_->RaiseIssue(FillIssue(messageText, issueCode));
        this->Request_->ReplyWithYdbStatus(status);
        this->Die(this->ActorContext());

        IsDead = true;
    }

    void ReplyWithResult(Ydb::StatusIds::StatusCode status, const google::protobuf::Message& result) {
        if (IsDead) {
            return;
        }

        this->Request_->SendResult(result, status);
        this->Die(this->ActorContext());

        IsDead = true;
    }

private:
    bool IsDead = false;
    TString TopicPath;
};

} // namespace NKikimr::NGRpcProxy::V1
