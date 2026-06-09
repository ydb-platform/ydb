#include "console_configs_manager.h"

namespace NKikimr::NConsole {

class TConfigsManager::TTxRemoveConfigSubscription : public TTransactionBase<TConfigsManager> {
public:
    TTxRemoveConfigSubscription(TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr ev,
                                TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code,
               const TString &error,
               const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Cannot remove subscription: " << error);

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
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TTxRemoveConfigSubscription Execute: " << rec.ShortDebugString());

        Y_ABORT_UNLESS(Self->PendingSubscriptionModifications.IsEmpty());

        Response = new TEvConsole::TEvRemoveConfigSubscriptionResponse;

        ui64 id = rec.GetSubscriptionId();
        if (!Self->SubscriptionIndex.GetSubscription(id))
            return Error(Ydb::StatusIds::NOT_FOUND,
                         Sprintf("cannot find subscription %" PRIu64, id), ctx);

        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

        Self->PendingSubscriptionModifications.RemovedSubscriptions.insert(id);

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxRemoveConfigSubscription Complete");

        Y_ABORT_UNLESS(Response);
        if (!Self->PendingSubscriptionModifications.IsEmpty()) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(Request->Sender,
                                                         Self->SelfId(),
                                                         Response.Release(), 0,
                                                         Request->Cookie);
            Self->ApplyPendingSubscriptionModifications(ctx, ev);
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "Send TEvRemoveConfigSubscriptionResponse: " << Response->Record.ShortDebugString());
            ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvRemoveConfigSubscriptionResponse> Response;
};

ITransaction *TConfigsManager::CreateTxRemoveConfigSubscription(TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev)
{
    return new TTxRemoveConfigSubscription(ev, this);
}

} // namespace NKikimr::NConsole
