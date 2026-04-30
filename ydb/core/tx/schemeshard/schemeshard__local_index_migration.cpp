#include "schemeshard__local_index_migration.h"

#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>


namespace NKikimr::NSchemeShard {

TString TLocalIndexMigrationItem::DebugString() const {
    return TStringBuilder() << "create local index '" << JoinPath({WorkingDir, IndexConfig.GetName()}) << "'";
}

namespace {

// Modeled on TSysViewsRosterUpdate: pre-allocated TxIds, all operations fired in
// parallel, finishes when the awaiting map drains.
class TLocalIndexMigrator : public TActorBootstrapped<TLocalIndexMigrator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_LOCAL_INDEX_MIGRATOR;
    }

    TLocalIndexMigrator(TTabletId selfTabletId, TActorId selfActorId,
                        TVector<std::pair<TTxId, TLocalIndexMigrationItem>>&& items)
        : SelfTabletId(selfTabletId)
        , SelfActorId(selfActorId)
    {
        AwaitingRequests.reserve(items.size());
        for (auto& [txId, item] : items) {
            AwaitingRequests.emplace(txId, std::move(item));
        }
    }

    void Bootstrap() {
        for (const auto& [txId, item] : AwaitingRequests) {
            auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
            auto& record = request->Record;
            record.SetTxId(static_cast<ui64>(txId));

            auto& modifyScheme = *record.AddTransaction();
            modifyScheme.SetWorkingDir(item.WorkingDir);
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTableIndex);
            modifyScheme.SetInternal(true);
            modifyScheme.SetFailOnExist(false);
            *modifyScheme.MutableCreateTableIndex() = item.IndexConfig;

            Send(SelfActorId, request.Release());
        }

        Become(&TLocalIndexMigrator::StateWork);

        if (AwaitingRequests.empty()) {
            PassAway();
        }
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TLocalIndexMigrator unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void Done(TTxId txId) {
        AwaitingRequests.erase(txId);
        if (AwaitingRequests.empty()) {
            PassAway();
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const TTxId txId(record.GetTxId());

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusSuccess:
        case NKikimrScheme::StatusAlreadyExists:
            // StatusAlreadyExists: a concurrent operation already created the scheme object.
            Done(txId);
            break;
        case NKikimrScheme::StatusAccepted:
            Send(SelfActorId, new TEvSchemeShard::TEvNotifyTxCompletion(static_cast<ui64>(txId)));
            break;
        default:
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator at schemeshard: " << SelfTabletId
                << ", failed to " << AwaitingRequests.at(txId).DebugString()
                << ", reason: " << record.GetReason());
            Done(txId);
            break;
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext&) {
        Done(TTxId(ev->Get()->Record.GetTxId()));
    }

private:
    const TTabletId SelfTabletId;
    const TActorId SelfActorId;
    THashMap<TTxId, TLocalIndexMigrationItem> AwaitingRequests;
};

} // anonymous namespace

THolder<IActor> CreateLocalIndexMigrator(TTabletId selfTabletId, TActorId selfActorId,
                                         TVector<std::pair<TTxId, TLocalIndexMigrationItem>>&& items) {
    return MakeHolder<TLocalIndexMigrator>(selfTabletId, selfActorId, std::move(items));
}

} // namespace NKikimr::NSchemeShard
