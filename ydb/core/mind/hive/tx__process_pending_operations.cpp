#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxProcessPendingOperations : public TTransactionBase<THive> {
protected:
    std::deque<THolder<IEventHandle>> Events;

public:
    TTxProcessPendingOperations(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_PENDING_OPERATIONS; }

    bool Execute(TTransactionContext&, const TActorContext&) override { 
        BLOG_D("THive::TTxProcessPendingOperations()::Execute");
        for (auto& [owner, pendingCreateTablet] : Self->PendingCreateTablets) {
            THolder<TEvHive::TEvCreateTablet> evCreateTablet(new TEvHive::TEvCreateTablet());
            evCreateTablet->Record = pendingCreateTablet.CreateTablet;
            Events.emplace_back(new IEventHandle(Self->SelfId(), pendingCreateTablet.Sender, evCreateTablet.Release(), 0, pendingCreateTablet.Cookie));
        }
        return true;
    }

    void Complete(const TActorContext&) override { 
        BLOG_D("THive::TTxProcessPendingOperations()::Complete");
        for (THolder<IEventHandle>& event : Events) {
            BLOG_D("THive::TTxProcessPendingOperations(): retry event " << event->Type);
            TlsActivationContext->Send(event.Release());
        }
    }
};

ITransaction* THive::CreateProcessPendingOperations() {
    return new TTxProcessPendingOperations(this);
}

} // NHive
} // NKikimr
