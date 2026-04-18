#include "schema_operation.h"

#include "common.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>
#include <ydb/library/services/services.pb.h>


namespace NKikimr::NPQ::NSchema {

namespace {

class TSchemaOperationActor: public TBaseActor<TSchemaOperationActor>
                           , public TPipeCacheClient
                           , public TConstantLogPrefix {
    static constexpr size_t MaxWaitTxCompletionRetries = 10;

public:
    TSchemaOperationActor(
        NActors::TActorId parentId,
        const TString& path,
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction>&& operation,
        ui64 cookie)
        : TBaseActor<TSchemaOperationActor>(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
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
        return TStringBuilder() << ParentId << "[" << Path << "] ";
    }

    void OnException(const std::exception& exc) override {
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
    }

private:
    void DoPropose() {
        LOG_D("DoPropose retry: " << ProposeBackoff.GetIteration());
        Become(&TSchemaOperationActor::ProposeState);

        auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.CopyFrom(Operation->Record);
        Send(MakeTxProxyID(), std::move(request));
    }

    void RetryPropose() {
        if (ProposeBackoff.HasMore()) {
            Schedule(ProposeBackoff.Next(), new NActors::TEvents::TEvWakeup());
            return;
        }
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to execute operation: SchemeShard is not available");
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        DoPropose();
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        LOG_D("Handle TEvTxUserProxy::TEvProposeTransactionStatus");

        const auto status = ev->Get()->Status();
        const auto& record = ev->Get()->Record;
        const auto ssStatus = record.GetSchemeShardStatus();

        SchemeShardTabletId = record.GetSchemeShardTabletId();
        TxId = status == NTxProxy::TResultStatus::ExecInProgress ? record.GetTxId() : record.GetPathCreateTxId();

        switch (status) {
            case NTxProxy::TResultStatus::ExecComplete:
                [[fallthrough]];
            case NTxProxy::TResultStatus::ExecAlready:
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess || ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    ReplyOkAndDie();
                } else {
                    ReplyError(ssStatus);
                }
                break;
            case NTxProxy::TResultStatus::ProxyShardNotAvailable:
                RetryPropose();
                break;
            case NTxProxy::TResultStatus::ExecError:
                if (ssStatus == NKikimrScheme::EStatus::StatusMultipleModifications) {
                    DoWaitCompletion();
                } else {
                    ReplyError(ssStatus);
                }
                break;
            case NTxProxy::TResultStatus::ExecInProgress:
                DoWaitCompletion();
                break;
            default:
                ReplyError(ssStatus);
                break;
        }
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
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void DoWaitCompletion() {
        LOG_D("DoWaitTxCompletion SchemeShardTabletId: " << SchemeShardTabletId << " TxId: " << TxId);
        Become(&TSchemaOperationActor::WaitCompletionState);

        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        SendToTablet(SchemeShardTabletId, request.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
        LOG_D("Handle TEvSchemeShard::TEvNotifyTxCompletionResult");
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
    void ReplyError(ui32 ssStatus) {
        ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Failed to execute operation: " << NKikimrScheme::EStatus_Name(ssStatus));
    }

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

    TBackoff ProposeBackoff = TBackoff(3, TDuration::MilliSeconds(25), TDuration::Seconds(1));
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
