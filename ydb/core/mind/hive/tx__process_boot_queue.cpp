#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxProcessBootQueue : public TTransactionBase<THive> {
public:
    TTxProcessBootQueue(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_BOOT_QUEUE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        BLOG_D("THive::TTxProcessBootQueue()::Execute");
        Self->RunProcessBootQueue();
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxProcessBootQueue()::Complete");
    }
};

ITransaction* THive::CreateProcessBootQueue() {
    return new TTxProcessBootQueue(this);
}

} // NHive
} // NKikimr
