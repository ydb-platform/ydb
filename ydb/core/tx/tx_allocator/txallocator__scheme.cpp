#include "txallocator_impl.h"

namespace NKikimr {
namespace NTxAllocator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxAllocator::TTxSchema: public TTransactionBase<TTxAllocator> {
    TTxSchema(TSelf *allocator)
        : TBase(allocator)
    {}

    TTxType GetTxType() const override { return TXTYPE_RESERVE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_ALLOCATOR, "tablet# " << Self->TabletID() << " TTxSchema Complete");

        Self->Become(&TSelf::StateWork);
        Self->SignalTabletActive(ctx);
    }
};

ITransaction* TTxAllocator::CreateTxSchema() {
    return new TTxSchema(this);
}

}
}
