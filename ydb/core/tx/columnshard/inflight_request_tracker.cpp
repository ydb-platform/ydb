#include "inflight_request_tracker.h"
#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "data_sharing/common/transactions/tx_extension.h"
#include "engines/column_engine.h"
#include "engines/reader/plain_reader/constructor/read_metadata.h"

namespace NKikimr::NColumnShard {

void TInFlightReadsTracker::RemoveInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* index) {
    Y_ABORT_UNLESS(RequestsMeta.contains(cookie), "Unknown request cookie %" PRIu64, cookie);
    const auto& readMetaList = RequestsMeta[cookie];

    for (const auto& readMetaBase : readMetaList) {
        NOlap::NReader::NPlain::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

        if (!readMeta) {
            continue;
        }
        {
            auto it = SnapshotsLive.find(readMeta->GetRequestSnapshot());
            AFL_VERIFY(it != SnapshotsLive.end());
            if (it->second.DelRequest(cookie)) {
                SnapshotsLive.erase(it);
            }
        }

        THashMap<TString, THashSet<NOlap::TUnifiedBlobId>> portionBlobIds;
        for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
            const ui64 portionId = portion->GetPortion();
            AFL_VERIFY(index);
            portion->FillBlobIdsByStorage(portionBlobIds, *index);
            auto it = PortionUseCount.find(portionId);
            Y_ABORT_UNLESS(it != PortionUseCount.end(), "Portion id %" PRIu64 " not found in request %" PRIu64, portionId, cookie);
            if (it->second == 1) {
                PortionUseCount.erase(it);
            } else {
                it->second--;
            }
        }

        for (auto&& i : portionBlobIds) {
            auto storage = StoragesManager->GetOperatorVerified(i.first);
            auto tracker = storage->GetBlobsTracker();
            for (auto& blobId : i.second) {
                tracker->FreeBlob(blobId);
            }
        }

        auto insertStorage = StoragesManager->GetInsertOperator();
        auto tracker = insertStorage->GetBlobsTracker();
        for (const auto& committedBlob : readMeta->CommittedBlobs) {
            tracker->FreeBlob(committedBlob.GetBlobRange().GetBlobId());
        }
    }

    RequestsMeta.erase(cookie);
}

TConclusionStatus TInFlightReadsTracker::AddToInFlightRequest(
    const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* index) {
    RequestsMeta[cookie].push_back(readMetaBase);

    auto readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

    if (!readMeta) {
        return TConclusionStatus::Success();
    }

    auto selectInfo = readMeta->SelectInfo;
    Y_ABORT_UNLESS(selectInfo);
    SelectStatsDelta += selectInfo->Stats();

    THashMap<TString, THashSet<NOlap::TUnifiedBlobId>> portionBlobIds;
    for (const auto& portion : readMeta->SelectInfo->PortionsOrderedPK) {
        const ui64 portionId = portion->GetPortion();
        PortionUseCount[portionId]++;
        AFL_VERIFY(index);
        portion->FillBlobIdsByStorage(portionBlobIds, *index);
    }

    for (auto&& i : portionBlobIds) {
        auto storage = StoragesManager->GetOperatorOptional(i.first);
        if (!storage) {
            return TConclusionStatus::Fail("blobs storage info not ready for '" + i.first + "'");
        }
        auto tracker = storage->GetBlobsTracker();
        for (auto& blobId : i.second) {
            tracker->UseBlob(blobId);
        }
    }

    auto insertStorage = StoragesManager->GetInsertOperator();
    auto tracker = insertStorage->GetBlobsTracker();
    for (const auto& committedBlob : readMeta->CommittedBlobs) {
        tracker->UseBlob(committedBlob.GetBlobRange().GetBlobId());
    }
    return TConclusionStatus::Success();
}

namespace {
class TTransactionSavePersistentSnapshots: public NOlap::NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NOlap::NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard>;
    const std::set<NOlap::TSnapshot> SaveSnapshots;
    const std::set<NOlap::TSnapshot> RemoveSnapshots;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : SaveSnapshots) {
            db.Table<Schema::InFlightSnapshots>().Key(i.GetPlanStep(), i.GetTxId()).Update();
        }
        for (auto&& i : RemoveSnapshots) {
            db.Table<Schema::InFlightSnapshots>().Key(i.GetPlanStep(), i.GetTxId()).Delete();
        }
        return true;
    }

    virtual void DoComplete(const TActorContext& /*ctx*/) override {
    }

public:
    TTransactionSavePersistentSnapshots(
        NColumnShard::TColumnShard* self, std::set<NOlap::TSnapshot>&& saveSnapshots, std::set<NOlap::TSnapshot>&& removeSnapshots)
        : TBase(self)
        , SaveSnapshots(std::move(saveSnapshots))
        , RemoveSnapshots(std::move(removeSnapshots))
    {
        AFL_VERIFY(saveSnapshots.size() || removeSnapshots.size());
    }
};
}   // namespace

std::unique_ptr<NTabletFlatExecutor::ITransaction> TInFlightReadsTracker::Ping(TColumnShard* self, const TDuration critDuration) {
    std::set<NOlap::TSnapshot> snapshotsToSave;
    std::set<NOlap::TSnapshot> snapshotsToFree;
    for (auto&& i : SnapshotsLive) {
        if (i.second.Ping(critDuration)) {
            if (i.second.GetIsLock()) {
                snapshotsToSave.emplace(i.first);
            } else {
                snapshotsToFree.emplace(i.first);
            }
        }
    }
    for (auto&& i : snapshotsToFree) {
        SnapshotsLive.erase(i);
    }
    if (snapshotsToFree.size() || snapshotsToSave.size()) {
        return std::make_unique<TTransactionSavePersistentSnapshots>(self, std::move(snapshotsToSave), std::move(snapshotsToFree));
    } else {
        return nullptr;
    }
    
}

bool TInFlightReadsTracker::LoadFromDatabase(NTable::TDatabase& tableDB) {
    NIceDb::TNiceDb db(tableDB);
    auto rowset = db.Table<Schema::InFlightSnapshots>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const NOlap::TSnapshot snapshot(
            rowset.GetValue<Schema::InFlightSnapshots::PlanStep>(), rowset.GetValue<Schema::InFlightSnapshots::TxId>());
        AFL_VERIFY(SnapshotsLive.emplace(snapshot, TSnapshotLiveInfo::BuildFromDatabase(snapshot)).second);

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

}
