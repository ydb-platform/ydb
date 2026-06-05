#include "console_configs_manager.h"

#include <util/random/random.h>

namespace NKikimr::NConsole {

class TConfigsManager::TTxReplaceConfigSubscriptions : public TTransactionBase<TConfigsManager> {
public:
    TTxReplaceConfigSubscriptions(TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr ev,
                                TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code,
               const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Cannot replace subscriptions: " << error);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(error);

        Self->PendingSubscriptionModifications.Clear();

        return true;
    }

    bool Execute(TTransactionContext &txc,
                 const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        auto &rec = Request->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceConfigSubscriptions Execute: " << rec.ShortDebugString());

        Y_ABORT_UNLESS(Self->PendingSubscriptionModifications.IsEmpty());

        Response = new TEvConsole::TEvReplaceConfigSubscriptionsResponse;

        TSubscription::TPtr subscription = new TSubscription(rec.GetSubscription());
        Ydb::StatusIds::StatusCode code;
        TString error;
        if (!Self->MakeNewSubscriptionChecks(subscription, code, error))
            return Error(code, error, ctx);

        for (auto existingSubscription : Self->SubscriptionIndex.GetSubscriptions(subscription->Subscriber)) {
            if (!subscription->Id && subscription->IsEqual(*existingSubscription)) {
                subscription->Id = existingSubscription->Id;
                // For services we assume replacement
                if (subscription->Subscriber.ServiceId) {
                    existingSubscription->Cookie = RandomNumber<ui64>();
                    Self->PendingSubscriptionModifications.ModifiedLastProvided
                        .emplace(existingSubscription->Id, TConfigId());
                    Self->PendingSubscriptionModifications.ModifiedCookies
                        .emplace(existingSubscription->Id, existingSubscription->Cookie);
                }
            } else {
                Self->PendingSubscriptionModifications.RemovedSubscriptions.insert(existingSubscription->Id);
            }
        }

        if (!subscription->Id) {
            subscription->Id = Self->NextSubscriptionId++;
            subscription->Cookie = RandomNumber<ui64>();
            Self->PendingSubscriptionModifications.AddedSubscriptions.push_back(subscription);
        }

        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        Response->Record.SetSubscriptionId(subscription->Id);

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);
        Self->DbUpdateNextSubscriptionId(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceConfigSubscriptions Complete");

        Y_ABORT_UNLESS(Response);
        if (!Self->PendingSubscriptionModifications.IsEmpty()) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(Request->Sender,
                                                         Self->SelfId(),
                                                         Response.Release(), 0,
                                                         Request->Cookie);
            Self->ApplyPendingSubscriptionModifications(ctx, ev);
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Send TEvReplaceConfigSubscriptionsResponse: " << Response->Record.ShortDebugString());
            ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvReplaceConfigSubscriptionsResponse> Response;
};

ITransaction *TConfigsManager::CreateTxReplaceConfigSubscriptions(TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev)
{
    return new TTxReplaceConfigSubscriptions(ev, this);
}

} // namespace NKikimr::NConsole
