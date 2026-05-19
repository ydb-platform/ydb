#include "console_configs_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

class TConfigsManager::TTxRemoveConfigSubscriptions : public TTransactionBase<TConfigsManager> {
public:
    TTxRemoveConfigSubscriptions(TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr ev,
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxRemoveConfigSubscriptions",
            {"Execute", rec.ShortDebugString()});

        Y_ABORT_UNLESS(Self->PendingSubscriptionModifications.IsEmpty());

        Response = new TEvConsole::TEvRemoveConfigSubscriptionsResponse;
        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        TSubscriberId subscriber(rec.GetSubscriber());
        auto &subscriptions = Self->SubscriptionIndex.GetSubscriptions(subscriber);
        if (subscriptions.empty())
            return true;

        for (auto &subscription : subscriptions)
            Self->PendingSubscriptionModifications.RemovedSubscriptions.insert(subscription->Id);

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxRemoveConfigSubscriptions Complete");

        Y_ABORT_UNLESS(Response);
        if (!Self->PendingSubscriptionModifications.IsEmpty()) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(Request->Sender,
                                                         Self->SelfId(),
                                                         Response.Release(), 0,
                                                         Request->Cookie);
            Self->ApplyPendingSubscriptionModifications(ctx, ev);
        } else {
            YDB_LOG_CTX_TRACE(ctx, "Send",
                {"TEvRemoveConfigSubscriptionsResponse", Response->Record.ShortDebugString()});
            ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvRemoveConfigSubscriptionsResponse> Response;
};

ITransaction *TConfigsManager::CreateTxRemoveConfigSubscriptions(TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev)
{
    return new TTxRemoveConfigSubscriptions(ev, this);
}

} // namespace NKikimr::NConsole
