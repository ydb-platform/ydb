#include "tx_finish_from_source.h"
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxFinishFromSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Cursor>(Session->SerializeCursorToProto().SerializeAsString()));
    if (Session->GetSourcesInProgressCount() == 0) {
        Finished = true;
        if (Session->GetTransferContext().GetTxId()) {
            Self->GetProgressTxController().FinishProposeOnExecute(*Session->GetTransferContext().GetTxId(), txc);
        }
    }
    return true;
}

void TTxFinishFromSource::DoComplete(const TActorContext& ctx) {
    Session->SendCurrentCursorAck(*Self, SourceTabletId);

    if (Finished) {
        AFL_VERIFY(Session->GetSourcesInProgressCount() == 0);
        if (Session->GetTransferContext().GetTxId()) {
            Self->GetProgressTxController().FinishProposeOnComplete(*Session->GetTransferContext().GetTxId(), ctx);
        }
        NYDBTest::TControllers::GetColumnShardController()->OnDataSharingFinished(Self->TabletID(), Session->GetSessionId());
        Session->Finish(*Self, Self->GetDataLocksManager());
        Session->GetInitiatorController().Finished(Session->GetSessionId());
    }
}

}