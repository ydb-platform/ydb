#include "txallocator_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_ALLOCATOR

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxSchema Complete",
            {"tablet", Self->TabletID()});

        Self->Become(&TSelf::StateWork);
        Self->SignalTabletActive(ctx);
    }
};

ITransaction* TTxAllocator::CreateTxSchema() {
    return new TTxSchema(this);
}

}
}
