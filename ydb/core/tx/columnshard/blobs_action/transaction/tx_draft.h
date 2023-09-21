#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxWriteDraft: public TTransactionBase<TColumnShard> {
private:
    const IWriteController::TPtr WriteController;
public:
    TTxWriteDraft(TColumnShard* self, const IWriteController::TPtr writeController)
        : TBase(self)
        , WriteController(writeController) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        TBlobManagerDb blobManagerDb(txc.DB);
        for (auto&& action : WriteController->GetBlobActions()) {
            action->OnExecuteTxBeforeWrite(*Self, blobManagerDb);
        }
        return true;
    }
    void Complete(const TActorContext& ctx) override {
        for (auto&& action : WriteController->GetBlobActions()) {
            action->OnCompleteTxBeforeWrite(*Self);
        }
        ctx.Register(NColumnShard::CreateWriteActor(Self->TabletID(), WriteController, TInstant::Max()));
    }
    TTxType GetTxType() const override { return TXTYPE_WRITE_DRAFT; }
};

}
