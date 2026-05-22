#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxProcessPendingOperations : public TTransactionBase<THive> {
public:
    TTxProcessPendingOperations(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_PENDING_OPERATIONS; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxProcessPendingOperations()::Execute",
            {"GetLogPrefix", GetLogPrefix()});
        for (auto& [owner, pendingCreateTablet] : Self->PendingCreateTablets) {
            THolder<TEvHive::TEvCreateTablet> evCreateTablet(new TEvHive::TEvCreateTablet());
            evCreateTablet->Record = pendingCreateTablet.CreateTablet;
            YDB_LOG_DEBUG("THive::TTxProcessPendingOperations(): retry CreateTablet",
                {"GetLogPrefix", GetLogPrefix()});
            TlsActivationContext->Send(new IEventHandle(Self->SelfId(), pendingCreateTablet.Sender, evCreateTablet.Release(), 0, pendingCreateTablet.Cookie));
        }
        for (auto& handle : Self->PendingOperations) {
            TlsActivationContext->Send(handle.Release());
        }
        Self->PendingOperations.clear();
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxProcessPendingOperations()::Complete",
            {"GetLogPrefix", GetLogPrefix()});
    }
};

ITransaction* THive::CreateProcessPendingOperations() {
    return new TTxProcessPendingOperations(this);
}

} // NHive
} // NKikimr
