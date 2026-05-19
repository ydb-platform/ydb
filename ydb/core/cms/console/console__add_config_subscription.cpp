#include "console_configs_manager.h"

#include <util/random/random.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

class TConfigsManager::TTxAddConfigSubscription : public TTransactionBase<TConfigsManager> {
public:
    TTxAddConfigSubscription(TEvConsole::TEvAddConfigSubscriptionRequest::TPtr ev,
                             TConfigsManager *self)
        : TBase(self)
        , Request(std::move(ev))
    {
    }

    bool Error(Ydb::StatusIds::StatusCode code,
               const TString &error,
               const TActorContext &ctx)
    {
        YDB_LOG_CTX_DEBUG(ctx, "Cannot add",
            {"subscription", error});

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxAddConfigSubscription",
            {"Execute", rec.ShortDebugString()});

        Y_ABORT_UNLESS(Self->PendingSubscriptionModifications.IsEmpty());

        Response = new TEvConsole::TEvAddConfigSubscriptionResponse;

        TSubscription::TPtr subscription = new TSubscription(rec.GetSubscription());
        Ydb::StatusIds::StatusCode code;
        TString error;
        if (!Self->MakeNewSubscriptionChecks(subscription, code, error))
            return Error(code, error, ctx);

        // Check if existing subscription should be returned.
        for (auto existingSubscription : Self->SubscriptionIndex.GetSubscriptions(subscription->Subscriber)) {
            if (subscription->IsEqual(*existingSubscription)) {
                YDB_LOG_CTX_DEBUG(ctx, "",
                    {"id", existingSubscription->Id});

                Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
                Response->Record.SetSubscriptionId(existingSubscription->Id);
                return true;
            }
        }

        subscription->Id = Self->NextSubscriptionId++;
        subscription->Cookie = RandomNumber<ui64>();
        Response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        Response->Record.SetSubscriptionId(subscription->Id);

        Self->PendingSubscriptionModifications.AddedSubscriptions.push_back(subscription);

        // Update database.
        Self->DbApplyPendingSubscriptionModifications(txc, ctx);
        Self->DbUpdateNextSubscriptionId(txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxAddConfigSubscription Complete");

        Y_ABORT_UNLESS(Response);
        if (!Self->PendingSubscriptionModifications.IsEmpty()) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(Request->Sender,
                                                         Self->SelfId(),
                                                         Response.Release(), 0,
                                                         Request->Cookie);
            Self->ApplyPendingSubscriptionModifications(ctx, ev);
        } else {
            YDB_LOG_CTX_TRACE(ctx, "Send",
                {"TEvAddConfigSubscriptionResponse", Response->Record.ShortDebugString()});
            ctx.Send(Request->Sender, Response.Release(), 0, Request->Cookie);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvAddConfigSubscriptionRequest::TPtr Request;
    TAutoPtr<TEvConsole::TEvAddConfigSubscriptionResponse> Response;
};

ITransaction *TConfigsManager::CreateTxAddConfigSubscription(TEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev)
{
    return new TTxAddConfigSubscription(ev, this);
}

} // namespace NKikimr::NConsole
