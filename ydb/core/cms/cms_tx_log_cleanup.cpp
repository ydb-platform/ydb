#include "cms_impl.h"
#include "scheme.h"

namespace NKikimr::NCms {

class TCms::TTxLogCleanup : public TTransactionBase<TCms> {
public:
    TTxLogCleanup(TCms *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_LOG_CLEANUP; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "TTxLogCleanup Execute");

        return Self->Logger.DbCleanupLog(txc, ctx);
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxLogCleanup Complete");
        Self->ScheduleLogCleanup(ctx);
    }

private:
};

ITransaction *TCms::CreateTxLogCleanup() {
    return new TTxLogCleanup(this);
}

} // namespace NKikimr::NCms
