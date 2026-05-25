#include "txallocator_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_ALLOCATOR

namespace NKikimr {
namespace NTxAllocator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxAllocator::TTxReserve: public TTransactionBase<TTxAllocator> {
    TEvTxAllocator::TEvAllocate::TPtr Event;
    ui64 RangeBegin = 0;
    ui64 RangeEnd = 0;
    bool Successed = false;

    TTxReserve(TSelf *self, TEvTxAllocator::TEvAllocate::TPtr &ev)
        : TBase(self)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_RESERVE; }

    bool IsPosibleToAllocate(const ui64 requestedSize) const {
        return TSelf::MaxCapacity - RangeBegin >= requestedSize;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(ctx);

        NIceDb::TNiceDb db(txc.DB);

        auto row = db.Table<Schema::config>().Key(Schema::config::ReservedTo).Select<Schema::config::reservedIds>();
        if (!row.IsReady())
            return false;

        if (row.IsValid())
            RangeBegin = row.GetValue<Schema::config::reservedIds>();

        const ui64 requestedSize = Event->Get()->Record.GetRangeSize();

        if (!IsPosibleToAllocate(requestedSize)) {
            return true;
        }

        RangeEnd = RangeBegin + requestedSize;
        db.Table<Schema::config>().Key(Schema::config::ReservedTo).Update(NIceDb::TUpdate<Schema::config::reservedIds>(RangeEnd));
        Successed = true;
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxReserve Complete Reserved Reserved",
            {"tablet", Self->TabletID()},
            {"Successed", Successed},
            {"from", RangeBegin},
            {"to", RangeEnd});

        if (!Successed) {
            Self->ReplyImposible(Event, ctx);
            return;
        }

        Self->Reply(RangeBegin, RangeEnd, Event, ctx);
    }
};

ITransaction* TTxAllocator::CreateTxReserve(TEvTxAllocator::TEvAllocate::TPtr &ev) {
    return new TTxReserve(this, ev);
}

}
}

