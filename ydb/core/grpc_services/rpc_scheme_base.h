#pragma once
#include "defs.h"

#include "rpc_deferrable.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr {
namespace NGRpcService {

template <typename TDerived, typename TRequest>
class TRpcSchemeRequestActor : public TRpcOperationRequestActor<TDerived, TRequest> {
protected:
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TRpcSchemeRequestActor(IRequestOpCtx* request)
        : TBase(request) {}

protected:
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            default: TBase::StateFuncBase(ev, ctx);
        }
    }

    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> CreateProposeTransaction() {
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest(new TEvTxUserProxy::TEvProposeTransaction());
        SetAuthToken(proposeRequest, *this->Request_);
        SetDatabase(proposeRequest.get(), *this->Request_);
        SetPeerName(proposeRequest.get(), *this->Request_);
        SetRequestType(proposeRequest.get(), *this->Request_);
        return proposeRequest;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Tablet not available, status: " << (ui32)ev->Get()->Status));
            return this->Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            TStringBuilder() << "Connection to tablet was lost."));
        return this->Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        const TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        auto issueMessage = msg->Record.GetIssues();
        switch (status) {
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete: {
                if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess ||
                    msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists)
                {
                    return this->ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, ctx);
                }
                break;
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress: {
                ui64 schemeShardTabletId = msg->Record.GetSchemeShardTabletId();
                IActor* pipeActor = NTabletPipe::CreateClient(ctx.SelfID, schemeShardTabletId);
                Y_VERIFY(pipeActor);
                SchemePipeActorId_ = ctx.ExecutorThread.RegisterActor(pipeActor);

                auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
                request->Record.SetTxId(msg->Record.GetTxId());
                NTabletPipe::SendData(ctx, SchemePipeActorId_, request.Release());
                return;
            }
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest: {
                return this->ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, issueMessage, ctx);
            }
            case TEvTxUserProxy::TResultStatus::AccessDenied: {
                return this->ReplyWithResult(Ydb::StatusIds::UNAUTHORIZED, issueMessage, ctx);
            }
            case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable: {
                return this->ReplyWithResult(Ydb::StatusIds::UNAVAILABLE, issueMessage, ctx);
            }
            case TEvTxUserProxy::TResultStatus::ResolveError: {
                return this->ReplyWithResult(Ydb::StatusIds::SCHEME_ERROR, issueMessage, ctx);
            }
            case TEvTxUserProxy::TResultStatus::ExecError: {
                switch (msg->Record.GetSchemeShardStatus()) {
                    case NKikimrScheme::EStatus::StatusMultipleModifications: {
                        return this->ReplyWithResult(Ydb::StatusIds::OVERLOADED, issueMessage, ctx);
                    }
                    case NKikimrScheme::EStatus::StatusInvalidParameter: {
                        return this->ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, issueMessage, ctx);
                    }
                    case NKikimrScheme::EStatus::StatusSchemeError:
                    case NKikimrScheme::EStatus::StatusNameConflict:

                    case NKikimrScheme::EStatus::StatusPathDoesNotExist: {
                        return this->ReplyWithResult(Ydb::StatusIds::SCHEME_ERROR, issueMessage, ctx);
                    }
                    case NKikimrScheme::EStatus::StatusQuotaExceeded: {
                        // FIXME: clients may start aggressive retries when receiving 'overloaded'
                        return this->ReplyWithResult(Ydb::StatusIds::OVERLOADED, issueMessage, ctx);
                    }
                    case NKikimrScheme::EStatus::StatusResourceExhausted:
                    case NKikimrScheme::EStatus::StatusPreconditionFailed: {
                        return this->ReplyWithResult(Ydb::StatusIds::PRECONDITION_FAILED, issueMessage, ctx);
                    }
                    default: {
                        return this->ReplyWithResult(Ydb::StatusIds::GENERIC_ERROR, issueMessage, ctx);
                    }
                }
            }
            default: {
                TStringStream str;
                str << "Got unknown TEvProposeTransactionStatus (" << status << ") response from TxProxy";
                const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str.Str());
                auto tmp = issueMessage.Add();
                NYql::IssueToMessage(issue, tmp);
                return this->ReplyWithResult(Ydb::StatusIds::INTERNAL_ERROR, issueMessage, ctx);
            }
        }
        return this->ReplyWithResult(Ydb::StatusIds::INTERNAL_ERROR, issueMessage, ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        NTabletPipe::CloseClient(ctx, SchemePipeActorId_);
        return this->ReplyNotifyTxCompletionResult(ev, ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
    }

    virtual void ReplyNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        return this->Reply(Ydb::StatusIds::SUCCESS, ctx);
    }

private:
    TActorId SchemePipeActorId_;
};

} // namespace NGRpcService
} // namespace NKikimr
