#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxCollect : public TTxBase {
    explicit TTxCollect(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_COLLECT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxCollect::Execute");

        NIceDb::TNiceDb db(txc.DB);

        if (!Self->NodesInFlight.empty() || !Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }
        Self->PersistPartitionResults(db);

        Self->Reset(db, ctx);

        return true;
    }

    void Complete(const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxCollect::Complete");

        Self->ScheduleAggregate();
    }
};

void TSysViewProcessor::Handle(TEvPrivate::TEvCollect::TPtr&) {
    Execute(new TTxCollect(this), TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr
