#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxWriteDraft: public TTransactionBase<TColumnShard> {
private:
    bool Completed = false;
    const IWriteController::TPtr WriteController;
public:
    ~TTxWriteDraft() {
        if (!Completed) {
            WriteController->Abort("TTxWriteDraft aborted before complete");
        }
    }

    TTxWriteDraft(TColumnShard* self, const IWriteController::TPtr writeController)
        : TBase(self)
        , WriteController(writeController) {
        AFL_VERIFY(WriteController);
    }

    bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_WRITE_DRAFT; }
};

}
