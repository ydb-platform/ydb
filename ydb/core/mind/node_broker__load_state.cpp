#include "node_broker_impl.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxLoadState : public TTransactionBase<TNodeBroker> {
public:
    TTxLoadState(TNodeBroker *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_LOAD_STATE; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxLoadState Execute");

        DbChanges = Self->Dirty.DbLoadState(txc, ctx);
        return DbChanges.Ready;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxLoadState Complete");
        Self->Execute(Self->CreateTxMigrateState(std::move(DbChanges)));
    }

private:
    TDbChanges DbChanges;
};

ITransaction *TNodeBroker::CreateTxLoadState()
{
    return new TTxLoadState(this);
}

} // NNodeBroker
} // NKikimr
