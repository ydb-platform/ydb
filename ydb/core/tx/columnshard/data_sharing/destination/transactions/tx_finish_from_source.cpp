#include "tx_finish_from_source.h"

namespace NKikimr::NOlap::NDataSharing {

bool TTxFinishFromSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Cursor>(Session->SerializeCursorToProto().SerializeAsString()));
    return true;
}

void TTxFinishFromSource::DoComplete(const TActorContext& /*ctx*/) {
    Session->SendCurrentCursorAck(*Self, SourceTabletId);
}

}