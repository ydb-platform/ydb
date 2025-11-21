#include "export_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    if (data) {
        CurrentData = NArrow::ToBatch(data);
        CurrentDataBlob = ExportSession->GetTask().GetSerializer()->SerializeFull(CurrentData);
        if (data) {
            Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(CurrentData, ev->Get()->Finished)));
        }
    } else {
        CurrentData = nullptr;
        CurrentDataBlob = "";
        Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(nullptr, true)));
    }
    TOwnedCellVec lastKey = ev->Get()->LastKey;
    AFL_VERIFY(!ExportSession->GetCursor().IsFinished());
    ExportSession->MutableCursor().InitNext(ev->Get()->LastKey, ev->Get()->Finished);
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportError::TPtr& /*ev*/) {
    AFL_VERIFY(false);
}

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

void TActor::OnSessionStateSaved() {
    AFL_VERIFY(ExportSession->IsFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    if (ExportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ExportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

}