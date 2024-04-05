#pragma once
#include "defs.h"

#include "rpc_deferrable.h"

#include <ydb/core/ydb_convert/tx_proxy_status.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NGRpcService {

template <typename TDerived, typename TRequest>
class TRpcSchemeRequestActor : public TRpcOperationRequestActor<TDerived, TRequest> {
protected:
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TRpcSchemeRequestActor(IRequestOpCtx* request)
        : TBase(request) {}

protected:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            default: TBase::StateFuncBase(ev);
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

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            TStringBuilder() << "Connection to tablet was lost."));
        return this->Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        auto issueMessage = msg->Record.GetIssues();

        Ydb::StatusIds::StatusCode ydbStatus = NKikimr::YdbStatusFromProxyStatus(msg);
        if (!NKikimr::IsTxProxyInProgress(ydbStatus)) {
            return this->Reply(ydbStatus, issueMessage, ctx);
        }

        ui64 schemeShardTabletId = msg->Record.GetSchemeShardTabletId();
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(msg->Record.GetTxId());
        SetSchemeShardId(schemeShardTabletId);
        ForwardToSchemeShard(ctx, std::move(request));
        return;
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        return this->OnNotifyTxCompletionResult(ev, ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
    }

    virtual void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        return this->Reply(Ydb::StatusIds::SUCCESS, ctx);
    }

    void PassAway() override {
        if (SchemePipeActorId_) {
            NTabletPipe::CloseClient(this->SelfId(), SchemePipeActorId_);
        }

        TBase::PassAway();
    }

    void SetSchemeShardId(ui64 schemeShardTabletId) {
        SchemeShardTabletId = schemeShardTabletId;
    }

    template<typename TEv>
    void ForwardToSchemeShard(const TActorContext& ctx, std::unique_ptr<TEv>&& ev) {
        if (!SchemePipeActorId_) {
            Y_ABORT_UNLESS(SchemeShardTabletId);
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {.RetryLimitCount = 3};
            SchemePipeActorId_ = ctx.ExecutorThread.RegisterActor(NTabletPipe::CreateClient(ctx.SelfID, SchemeShardTabletId));
        }

        Y_ABORT_UNLESS(SchemePipeActorId_);
        NTabletPipe::SendData(this->SelfId(), SchemePipeActorId_, ev.release());
    }

private:
    ui64 SchemeShardTabletId = 0;
    TActorId SchemePipeActorId_;
};

} // namespace NKikimr::NGRpcService
