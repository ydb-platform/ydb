#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxUpdateConfigSubscription : public TTransactionBase<TNodeBroker> {
public:
    TTxUpdateConfigSubscription(TNodeBroker *self,
                                TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr event)
        : TBase(self)
        , Event(std::move(event))
        , SubscriptionId(0)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_UPDATE_CONFIG_SUBSCRIPTION; }

    bool Execute(TTransactionContext &txc,
                 const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateConfigSubscription Execute");

        auto &rec = Event->Get()->Record;
        Y_ABORT_UNLESS(rec.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);

        SubscriptionId = rec.GetSubscriptionId();
        Self->DbUpdateConfigSubscription(SubscriptionId, txc);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxUpdateConfigSubscription Complete");

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Using new subscription id=" << SubscriptionId);

        Self->ConfigSubscriptionId = SubscriptionId;

        Self->TxCompleted(0, this, ctx);
    }

private:
    TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr Event;
    ui64 SubscriptionId;
};

ITransaction *TNodeBroker::CreateTxUpdateConfigSubscription(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev)
{
    return new TTxUpdateConfigSubscription(this, std::move(ev));
}

} // NNodeBroker
} // NKikimr
