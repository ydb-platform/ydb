#include "cms_impl.h"
#include "scheme.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

class TCms::TTxLogAndSend : public TTransactionBase<TCms> {
public:
    TTxLogAndSend(TCms *self,
                  TEvPrivate::TEvLogAndSend::TPtr &ev)
        : TBase(self)
        , Event(std::move(ev))
    {
    }

    TTxType GetTxType() const override { return TXTYPE_LOG_AND_SEND; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxLogAndSend Execute");

        Self->Logger.DbLogData(Event->Get()->LogData, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxLogAndSend Complete");

        if (Event->Get()->Event)
            ctx.Send(Event->Get()->Event.Release());
    }

private:
    TEvPrivate::TEvLogAndSend::TPtr Event;
};

ITransaction *TCms::CreateTxLogAndSend(TEvPrivate::TEvLogAndSend::TPtr &ev) {
    return new TTxLogAndSend(this, ev);
}

} // namespace NKikimr::NCms
