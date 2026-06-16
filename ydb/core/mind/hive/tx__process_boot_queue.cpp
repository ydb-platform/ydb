#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxProcessBootQueue : public TTransactionBase<THive> {
    TSideEffects SideEffects;

public:
    TTxProcessBootQueue(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_BOOT_QUEUE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxProcessBootQueue()::Execute");
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        Self->ExecuteProcessBootQueue(db, SideEffects);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxProcessBootQueue()::Complete");
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateProcessBootQueue() {
    return new TTxProcessBootQueue(this);
}

} // NHive
} // NKikimr
