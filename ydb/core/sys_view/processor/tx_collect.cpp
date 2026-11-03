#include "processor_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxCollect : public TTxBase {
    explicit TTxCollect(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_COLLECT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxCollect::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        if (!Self->NodesInFlight.empty() || !Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }
        Self->PersistPartitionResults(db);

        Self->Reset(db, ctx);

        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxCollect::Complete",
            {"tabletId", Self->TabletID()});

        Self->ScheduleAggregate();
    }
};

void TSysViewProcessor::Handle(TEvPrivate::TEvCollect::TPtr&) {
    Execute(new TTxCollect(this), TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr
