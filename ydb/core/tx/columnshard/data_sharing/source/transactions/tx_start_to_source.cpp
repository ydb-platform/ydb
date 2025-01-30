#include "tx_start_to_source.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxStartToSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::SourceSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::SourceSessions::Details>(Session->SerializeDataToProto().SerializeAsString()));
    return true;
}

void TTxStartToSource::DoComplete(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("info", "TTxStartToSource::Complete");
    AFL_VERIFY(Sessions->emplace(Session->GetSessionId(), Session).second);
}

}