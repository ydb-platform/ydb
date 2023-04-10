#include "cms_impl.h"
#include "scheme.h"

namespace NKikimr::NCms {

class TCms::TTxUpdateDowntimes : public TTransactionBase<TCms> {
public:
    TTxUpdateDowntimes(TCms *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_UPDATE_DOWNTIMES; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "TTxUpdateDowntimes Execute");

        Self->State->Downtimes.DbStoreState(txc, ctx);
        Self->State->Downtimes.CleanupEmpty();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxUpdateDowntimes Complete");
    }
};

ITransaction *TCms::CreateTxUpdateDowntimes() {
    return new TTxUpdateDowntimes(this);
}

} // namespace NKikimr::NCms
