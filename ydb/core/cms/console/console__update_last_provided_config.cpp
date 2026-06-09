#include "console_configs_manager.h"

namespace NKikimr::NConsole {

class TConfigsManager::TTxUpdateLastProvidedConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxUpdateLastProvidedConfig(TEvConsole::TEvConfigNotificationResponse::TPtr ev,
                                TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Execute(TTransactionContext &txc,
                 const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        auto &rec = Request->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TTxUpdateLastProvidedConfig Execute: " << rec.ShortDebugString());

        Y_ABORT_UNLESS(Self->PendingSubscriptionModifications.IsEmpty());

        auto subscription = Self->SubscriptionIndex.GetSubscription(rec.GetSubscriptionId());
        if (!subscription) {
            LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Config notification response for missing subscription id="
                        << rec.GetSubscriptionId());
            return true;
        }
        if (Request->Cookie != subscription->Cookie) {
            LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Config notification response cookie mismatch for"
                        << " subscription id=" << rec.GetSubscriptionId());
            Y_ABORT_UNLESS(subscription->Subscriber.ServiceId,
                     "%s  ==>  %s",
                     rec.ShortDebugString().c_str(),
                     subscription->ToString().c_str());
            return true;
        }

        Self->PendingSubscriptionModifications.ModifiedLastProvided
            .emplace(subscription->Id, TConfigId(rec.GetConfigId()));

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxUpdateLastProvidedConfig Complete");

        if (!Self->PendingSubscriptionModifications.IsEmpty())
            Self->ApplyPendingSubscriptionModifications(ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvConfigNotificationResponse::TPtr Request;
};

ITransaction *TConfigsManager::CreateTxUpdateLastProvidedConfig(TEvConsole::TEvConfigNotificationResponse::TPtr &ev)
{
    return new TTxUpdateLastProvidedConfig(ev, this);
}

} // namespace NKikimr::NConsole
