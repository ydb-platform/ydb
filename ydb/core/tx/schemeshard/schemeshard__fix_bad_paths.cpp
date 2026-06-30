#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

#if defined SS_LOG_W
#error log macro redefinition
#endif

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxFixBadPaths : public TTransactionBase<TSchemeShard> {
    explicit TTxFixBadPaths(TSelf *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        for (auto& el : Self->PathsById) {
            TPathId pathId = el.first;
            TPathElement::TPtr pathEl = el.second;

            if (pathEl->Dropped() || !pathEl->NormalState()) {
                continue;
            }

            if (pathEl->CreateTxId == TTxId(0)) {
                pathEl->CreateTxId = TTxId(1);
                Self->PersistCreateTxId(db, pathId, pathEl->CreateTxId);

                YDB_LOG_WARN_CTX(ctx, "Fix CreateTxId,",
                    {"self", Self->TabletID()},
                    {"pathId", pathId});
            }
            if (pathId != Self->RootPathId() && pathEl->StepCreated == InvalidStepId) {
                pathEl->StepCreated = TStepId(1);
                Self->PersistCreateStep(db, pathId, pathEl->StepCreated);

                YDB_LOG_WARN_CTX(ctx, "Fix StepCreated,",
                    {"self", Self->TabletID()},
                    {"pathId", pathId});
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->Execute(Self->CreateTxInitPopulator(TSideEffects::TPublications()), ctx);
    }

}; // TSchemeShard::TTxFixBadPaths

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxFixBadPaths() {
    return new TTxFixBadPaths(this);
}

} // NSchemeShard
} // NKikimr
