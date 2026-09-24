#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NODE_BROKER

namespace NKikimr {
namespace NNodeBroker {

class TNodeBroker::TTxInitScheme : public TTransactionBase<TNodeBroker> {
public:
    TTxInitScheme(TNodeBroker *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEME; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Complete");

        Self->Execute(Self->CreateTxLoadState(), ctx);
    }
};

ITransaction *TNodeBroker::CreateTxInitScheme()
{
    return new TTxInitScheme(this);
}

} // NNodeBroker
} // NKikimr
