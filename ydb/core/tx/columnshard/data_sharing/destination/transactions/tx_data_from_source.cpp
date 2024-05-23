#include "tx_data_from_source.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxDataFromSource::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NKikimr::NColumnShard;
    TDbWrapper dbWrapper(txc.DB, nullptr);
    auto& index = Self->TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    {
        ui64* lastPortionPtr = index.GetLastPortionPointer();
        for (auto&& i : PortionsByPathId) {
            auto it = Session->GetPathIds().find(i.first);
            AFL_VERIFY(it != Session->GetPathIds().end());
            i.second.InitPortionIds(lastPortionPtr, it->second);
        }
        dbWrapper.WriteCounter(TColumnEngineForLogs::LAST_PORTION, *lastPortionPtr);
    }
    auto schemaPtr = index.GetVersionedIndex().GetLastSchema();
    THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> sharedBlobIds;
    for (auto&& i : PortionsByPathId) {
        for (auto&& p : i.second.GetPortions()) {
            p.SaveToDatabase(dbWrapper, schemaPtr->GetIndexInfo().GetPKFirstColumnId(), false);
        }
    }
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::DestinationSessions>().Key(Session->GetSessionId())
        .Update(NIceDb::TUpdate<Schema::DestinationSessions::Cursor>(Session->SerializeCursorToProto().SerializeAsString()));
    return true;
}

void TTxDataFromSource::DoComplete(const TActorContext& /*ctx*/) {
    Session->DataReceived(std::move(PortionsByPathId), Self->TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>(), Self->GetStoragesManager()).Validate();
    Session->SendCurrentCursorAck(*Self, SourceTabletId);
}

TTxDataFromSource::TTxDataFromSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session, const THashMap<ui64, NEvents::TPathIdData>& portionsByPathId, const TTabletId sourceTabletId)
    : TBase(self)
    , Session(session)
    , PortionsByPathId(portionsByPathId)
    , SourceTabletId(sourceTabletId)
{
    for (auto&& i : PortionsByPathId) {
        for (ui32 p = 0; p < i.second.GetPortions().size();) {
            if (Session->TryTakePortionBlobs(Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex(), i.second.GetPortions()[p])) {
                ++p;
            } else {
                i.second.MutablePortions()[p] = std::move(i.second.MutablePortions().back());
                i.second.MutablePortions()[p].ResetShardingVersion();
                i.second.MutablePortions().pop_back();
            }
        }
    }
}

}