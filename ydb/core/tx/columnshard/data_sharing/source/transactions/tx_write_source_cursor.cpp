#include "tx_write_source_cursor.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxWriteSourceCursor::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::SourceSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::SourceSessions::CursorDynamic>(Session->GetCursorVerified()->SerializeDynamicToProto().SerializeAsString()));
    return true;
}

void TTxWriteSourceCursor::DoComplete(const TActorContext& /*ctx*/) {
}

}