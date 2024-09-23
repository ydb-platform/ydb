#include "tx_data_ack_to_source.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxDataAckToSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    THashMap<TString, TTabletsByBlob> sharedTabletBlobIds;
    {
        THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> sharedBlobIds;
        auto& index = Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
        for (auto&& [_, i] : Session->GetCursorVerified()->GetPreviousSelected()) {
            for (auto&& portion : i.GetPortions()) {
                portion.FillBlobIdsByStorage(sharedBlobIds, index);
            }
        }
        for (auto&& i : sharedBlobIds) {
            AFL_VERIFY(sharedTabletBlobIds[i.first].Add(Session->GetDestinationTabletId(), i.second));
            sharedTabletBlobIds[i.first].Add(Self->GetStoragesManager()->GetSharedBlobsManager()->GetSelfTabletId(), std::move(i.second));
        }
        Self->GetStoragesManager()->GetSharedBlobsManager()->WriteSharedBlobsDB(txc, sharedTabletBlobIds);
    }

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::SourceSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::SourceSessions::CursorDynamic>(Session->GetCursorVerified()->SerializeDynamicToProto().SerializeAsString()));
    if (!Session->GetCursorVerified()->GetStaticSaved()) {
        db.Table<Schema::SourceSessions>().Key(Session->GetSessionId())
            .Update(NIceDb::TUpdate<Schema::SourceSessions::CursorStatic>(Session->GetCursorVerified()->SerializeStaticToProto().SerializeAsString()));
        Session->GetCursorVerified()->SetStaticSaved(true);
    }
    std::swap(SharedBlobIds, sharedTabletBlobIds);
    return true;
}

void TTxDataAckToSource::DoComplete(const TActorContext& /*ctx*/) {
    Session->ActualizeDestination(*Self, Self->GetDataLocksManager());
}

}