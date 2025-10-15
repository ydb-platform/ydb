#include "tx_data_ack_to_source.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxDataAckToSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;
    THashMap<TString, TTabletsByBlob> sharedTabletBlobIds;
    {
        THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> sharedBlobIds;
        auto& index = Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
        for (auto&& [_, i] : Session->GetCursorVerified()->GetPreviousSelected()) {
            for (auto&& portion : i.GetPortions()) {
                portion->FillBlobIdsByStorage(sharedBlobIds, index);
            }
        }
        for (auto&& i : sharedBlobIds) {
            AFL_VERIFY(sharedTabletBlobIds[i.first].Add(Session->GetDestinationTabletId(), i.second));
            sharedTabletBlobIds[i.first].Add(Self->GetStoragesManager()->GetSharedBlobsManager()->GetSelfTabletId(), std::move(i.second));
        }
        Self->GetStoragesManager()->GetSharedBlobsManager()->WriteSharedBlobsDB(txc, sharedTabletBlobIds);
    }

    NIceDb::TNiceDb db(txc.DB);
    Session->SaveCursorToDatabase(db);
    std::swap(SharedBlobIds, sharedTabletBlobIds);
    return true;
}

void TTxDataAckToSource::DoComplete(const TActorContext& /*ctx*/) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("TTxDataAckToSource::DoComplete", "1");

    Session->ActualizeDestination(*Self, Self->GetDataLocksManager());
}

}   // namespace NKikimr::NOlap::NDataSharing
