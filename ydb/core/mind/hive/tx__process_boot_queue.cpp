#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
        YDB_LOG_DEBUG("THive::TTxProcessBootQueue()::Execute",
            {"GetLogPrefix", GetLogPrefix()});
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        Self->ExecuteProcessBootQueue(db, SideEffects);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxProcessBootQueue()::Complete",
            {"GetLogPrefix", GetLogPrefix()});
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateProcessBootQueue() {
    return new TTxProcessBootQueue(this);
}

} // NHive
} // NKikimr
