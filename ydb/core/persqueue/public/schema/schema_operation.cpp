#include "schema_operation.h"
#include <ydb/core/ydb_convert/tx_proxy_status.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TSchemaOperationActor: public TBaseActor<TSchemaOperationActor>
                           , public TPipeCacheClient
                           , public TConstantLogPrefix {
    static constexpr size_t MaxWaitTxCompletionRetries = 3;

public:
    TSchemaOperationActor(
        NActors::TActorId parentId,
        const TString& path,
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction>&& operation,
        ui64 cookie)
        : TBaseActor<TSchemaOperationActor>(NKikimrServices::EServiceKikimr::PQ_ALTER_TOPIC)
        , TPipeCacheClient(this)
        , ParentId(parentId)
        , Path(path)
        , Operation(std::move(operation))
        , Cookie(cookie)
    {
    }

    ~TSchemaOperationActor() = default;

    void Bootstrap() {
        DoPropose();
    }

    void PassAway() override {
        TPipeCacheClient::Close();
        TBaseActor<TSchemaOperationActor>::PassAway();
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << ParentId << "[SO]" << SelfId() << "[" << Path << "] ";
    }

    void OnException(const std::exception& exc) override {
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
    }

private:
    void DoPropose() {
        LOG_D("DoPropose");
        Become(&TSchemaOperationActor::ProposeState);
        Send(MakeTxProxyID(), std::move(Operation));
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_D("Handle TEvTxUserProxy::TEvProposeTransactionStatus");

        auto msg = ev->Get();
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());
    
        if (status == TEvTxUserProxy::TResultStatus::ExecError && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPreconditionFailed) {
            return ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                TStringBuilder() << "The '" << Path << "' modification is already in progress");
        }
    
        Ydb::StatusIds::StatusCode ydbStatus = NKikimr::YdbStatusFromProxyStatus(msg);
        if (!NKikimr::IsTxProxyInProgress(ydbStatus)) {
            TString empty;
            const TString* errorMessage = &empty;
            auto issueMessage = msg->Record.GetIssues();
            if (issueMessage.size() > 0 && issueMessage[0].message().size() > 0) {
                errorMessage = &issueMessage[0].message();
            }
            return ReplyErrorAndDie(ydbStatus, TString{*errorMessage});
        }
    
        SchemeShardTabletId = msg->Record.GetSchemeShardTabletId();
        TxId = msg->Record.GetTxId();
    
        DoWaitCompletion();
    }
    
    void HandleOnPropose(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_D("HandleOnPropose TEvPipeCache::TEvDeliveryProblem");
        if (TPipeCacheClient::OnUndelivered(ev)) {
            return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "SchemeShard " << ev->Get()->TabletId << " is unavailable");
        }  
    }

    STFUNC(ProposeState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnPropose);
            sFunc(TEvents::TEvPoison, PassAway);
        }  
    }

private:
    void DoWaitCompletion() {
        LOG_D("DoWaitTxCompletion");
        Become(&TSchemaOperationActor::WaitCompletionState);
    
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        SendToTablet(SchemeShardTabletId, request.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        ReplyOkAndDie();        
    }

    void HandleOnWaitCompletion(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
        OnUndelivered(ev);
        if (++WaitTxCompletionRetries > MaxWaitTxCompletionRetries) {
            return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "SchemeShard " << ev->Get()->TabletId << " is unavailable");
        }
        DoWaitCompletion();
    }

    STFUNC(WaitCompletionState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnWaitCompletion);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        LOG_D("ReplyErrorAndDie: " << errorCode << " " << errorMessage);
        Send(ParentId, new TEvSchemaOperationResponse(errorCode, std::move(errorMessage)), 0, Cookie);
        PassAway();
    }

    void ReplyOkAndDie() {
        LOG_D("ReplyOkAndDie");
        Send(ParentId, new TEvSchemaOperationResponse(), 0, Cookie);
        PassAway();
    }

private:
    const TActorId ParentId;
    const TString Path;
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> Operation;
    const ui64 Cookie;

    ui64 SchemeShardTabletId = 0;
    ui64 TxId = 0;
    size_t WaitTxCompletionRetries = 0;
};

} // namespace

IActor* CreateSchemaOperation(
    NActors::TActorId parentId,
    const TString& path,
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction>&& operation,
    ui64 Cookie
) {
    return new TSchemaOperationActor(parentId, path, std::move(operation), Cookie);
}

} // namespace NKikimr::NPQ::NSchema
