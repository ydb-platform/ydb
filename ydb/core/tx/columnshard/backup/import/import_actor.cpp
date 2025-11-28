#include "import_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NImport {

class TTxProposeFinish: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    const ui64 TxId;
protected:
    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().FinishProposeOnExecute(TxId, txc);
        return true;
    }
    virtual void Complete(const TActorContext& ctx) override {
        Self->GetProgressTxController().FinishProposeOnComplete(TxId, ctx);
    }
public:
    TTxProposeFinish(NColumnShard::TColumnShard* self, const ui64 txId)
        : TBase(self)
        , TxId(txId) {
    }
};

void TImportActor::OnSessionStateSaved() {
    AFL_VERIFY(ImportSession->IsFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    if (ImportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ImportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

void TImportActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to",
                                                                        (ui32)to);
    Stage = to;
}

void TImportActor::OnTxCompleted(const ui64 /*txId*/) {
    Session->FinishActor();
}

void TImportActor::OnBootstrap(const TActorContext & /*ctx*/) {
    SwitchStage(EStage::Initialization, EStage::WaitSaveCursor);
    SaveSessionProgress();
    Become(&TImportActor::StateFunc);
}

TImportActor::TImportActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter> &adapter)
    : TBase(bgSession, adapter) {
    ImportSession = bgSession->GetLogicAsVerifiedPtr<NImport::TSession>();
}

void TImportActor::OnSessionProgressSaved() {
    SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
    SaveSessionState();
}

} // namespace NKikimr::NOlap::NImport