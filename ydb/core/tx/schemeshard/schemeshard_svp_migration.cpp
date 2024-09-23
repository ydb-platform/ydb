#include "schemeshard_svp_migration.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NSchemeShard {

class TTabletMigrator : public TActorBootstrapped<TTabletMigrator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_TABLET_MIGRATOR;
    }

    TTabletMigrator(ui64 ssTabletId, TActorId ssActorId, std::queue<TMigrationInfo>&& migrations)
        : SSTabletId(ssTabletId)
        , SSActorId(ssActorId)
        , Queue(std::move(migrations))
    {}

    void Bootstrap() {
        Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        Become(&TTabletMigrator::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, Handle);
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle)
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTabletMigrator StateWork unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void RequestTxId() {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - send TEvAllocateTxId"
            << ", working dir " << Current.WorkingDir
            << ", db name: " << Current.DbName
            << ", at schemeshard: " << SSTabletId);

        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void SendModifyScheme(ui64 txId) {
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(txId);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterExtSubDomain);
        modifyScheme.SetWorkingDir(Current.WorkingDir);
        modifyScheme.SetFailOnExist(false);

        auto& modifySubDomain = *modifyScheme.MutableSubDomain();
        modifySubDomain.SetName(Current.DbName);

        if (Current.CreateSVP) {
            modifySubDomain.SetExternalSysViewProcessor(true);
        }
        if (Current.CreateSA) {
            modifySubDomain.SetExternalStatisticsAggregator(true);
        }
        if (Current.CreateBCT) {
            modifySubDomain.SetExternalBackupController(true);
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - send TEvModifySchemeTransaction"
            << ", working dir " << Current.WorkingDir
            << ", db name: " << Current.DbName
            << ", at schemeshard: " << SSTabletId);

        Send(SSActorId, request.Release());
    }

    void SubscribeToCompletion(ui64 txId) {
        auto request = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - send TEvNotifyTxCompletion"
            << ", txId " << txId
            << ", at schemeshard: " << SSTabletId);

        Send(SSActorId, request.Release());
    }

    void StartNextMigration() {
        Current = {};
        if (Queue.empty()) {
            PassAway();
            return;
        }
        Current = std::move(Queue.front());
        Queue.pop();
        RequestTxId();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - start processing migrations"
            << ", queue size: " << Queue.size()
            << ", at schemeshard: " << SSTabletId);

        StartNextMigration();
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        auto txId = ev->Get()->TxId;

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - handle TEvAllocateTxIdResult"
            << ", txId: " << txId
            << ", at schemeshard: " << SSTabletId);

        SendModifyScheme(txId);
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        auto& record = ev->Get()->Record;
        auto status = record.GetStatus();
        auto txId = record.GetTxId();

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - handle TEvModifySchemeTransactionResult"
            << ", status: " << status
            << ", txId: " << txId
            << ", at schemeshard: " << SSTabletId);

        switch (status) {
        case NKikimrScheme::StatusSuccess:
            StartNextMigration();
            break;
        case NKikimrScheme::StatusAccepted:
            SubscribeToCompletion(record.GetTxId());
            break;
        default:
            LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TabletMigrator - migration failed"
                << ", status: " << status
                << ", reason: " << record.GetReason()
                << ", txId: " << txId
                << ", at schemeshard: " << SSTabletId);

            StartNextMigration();
            break;
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - handle TEvNotifyTxCompletionResult"
            << ", txId: " << ev->Get()->Record.GetTxId()
            << ", at schemeshard: " << SSTabletId);

        StartNextMigration();
    }

private:
    const ui64 SSTabletId;
    const TActorId SSActorId;
    std::queue<TMigrationInfo> Queue;
    TMigrationInfo Current;
};

THolder<IActor> CreateTabletMigrator(ui64 ssTabletId, TActorId ssActorId,
    std::queue<TMigrationInfo>&& migrations)
{
    return MakeHolder<TTabletMigrator>(ssTabletId, ssActorId, std::move(migrations));
}

} // namespace NKikimr::NSchemeShard
