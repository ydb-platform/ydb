#include "tx_data_ack_to_source.h"

namespace NKikimr::NOlap::NDataSharing {

bool TTxDataAckToSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    THashMap<TString, TTabletsByBlob> sharedTabletBlobIds;
    {
        THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> sharedBlobIds;
        for (auto&& [_, i] : Session->GetCursorVerified()->GetPreviousSelected()) {
            for (auto&& portion : i.GetPortions()) {
                portion.FillBlobIdsByStorage(sharedBlobIds);
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
        .Update(NIceDb::TUpdate<Schema::SourceSessions::Cursor>(Session->GetCursorVerified()->SerializeToProto().SerializeAsString()));
    std::swap(SharedBlobIds, sharedTabletBlobIds);
    return true;
}

void TTxDataAckToSource::DoComplete(const TActorContext& /*ctx*/) {
    Self->GetStoragesManager()->GetSharedBlobsManager()->AddSharingBlobs(SharedBlobIds);
    Session->ActualizeDestination();
}

}