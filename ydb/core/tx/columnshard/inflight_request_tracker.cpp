#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "inflight_request_tracker.h"

#include "data_sharing/common/transactions/tx_extension.h"
#include "engines/column_engine.h"
#include "engines/reader/plain_reader/constructor/read_metadata.h"
#include "hooks/abstract/abstract.h"

namespace NKikimr::NColumnShard {

void TInFlightReadsTracker::RemoveInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* /*index*/, const TInstant now) {
    auto it = RequestsMeta.find(cookie);
    AFL_VERIFY(it != RequestsMeta.end())("cookie", cookie);
    const auto& readMetaList = it->second;

    for (const auto& readMetaBase : readMetaList) {
        {
            auto it = SnapshotsLive.find(readMeta->GetRequestSnapshot());
            AFL_VERIFY(it != SnapshotsLive.end());
            if (it->second.DelRequest(cookie, now)) {
                SnapshotsLive.erase(it);
                Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());
            }
        }

        if (NOlap::NReader::NPlain::TReadMetadata::TConstPtr readMeta =
                std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase)) {
            auto insertStorage = StoragesManager->GetInsertOperator();
            auto tracker = insertStorage->GetBlobsTracker();
            for (const auto& committedBlob : readMeta->CommittedBlobs) {
                tracker->FreeBlob(committedBlob.GetBlobRange().GetBlobId());
            }
        }
    }

    RequestsMeta.erase(cookie);
}

TConclusionStatus TInFlightReadsTracker::AddToInFlightRequest(
    const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* /*index*/) {
    RequestsMeta[cookie].push_back(readMetaBase);

    auto readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

    if (!readMeta) {
        return TConclusionStatus::Success();
    }

    auto selectInfo = readMeta->SelectInfo;
    Y_ABORT_UNLESS(selectInfo);
    SelectStatsDelta += selectInfo->Stats();

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
        , RemoveSnapshots(std::move(removeSnapshots)) {
        AFL_VERIFY(SaveSnapshots.size() || RemoveSnapshots.size());
    }
};
}   // namespace

std::unique_ptr<NTabletFlatExecutor::ITransaction> TInFlightReadsTracker::Ping(
    TColumnShard* self, const TDuration critDuration, const TInstant now) {
    std::set<NOlap::TSnapshot> snapshotsToSave;
    std::set<NOlap::TSnapshot> snapshotsToFree;
    for (auto&& i : SnapshotsLive) {
        if (i.second.Ping(critDuration, now)) {
            if (i.second.GetIsLock()) {
                Counters->OnSnapshotLocked();
                snapshotsToSave.emplace(i.first);
            } else {
                Counters->OnSnapshotUnlocked();
                snapshotsToFree.emplace(i.first);
            }
        }
    }
    for (auto&& i : snapshotsToFree) {
        SnapshotsLive.erase(i);
    }
    if (snapshotsToFree.size() || snapshotsToSave.size()) {
        NYDBTest::TControllers::GetColumnShardController()->OnRequestTracingChanges(snapshotsToSave, snapshotsToFree);
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
    Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());
    return true;
}

NKikimr::TConclusion<ui64> TInFlightReadsTracker::AddInFlightRequest(
    NOlap::NReader::TReadMetadataBase::TConstPtr readMeta, const NOlap::TVersionedIndex* index) {
    const ui64 cookie = NextCookie++;
    auto it = SnapshotsLive.find(readMeta->GetRequestSnapshot());
    if (it == SnapshotsLive.end()) {
        it = SnapshotsLive.emplace(readMeta->GetRequestSnapshot(), TSnapshotLiveInfo::BuildFromRequest(readMeta->GetRequestSnapshot())).first;
        Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());
    }
    it->second.AddRequest(cookie);
    auto status = AddToInFlightRequest(cookie, readMeta, index);
    if (!status) {
        return status;
    }
    return cookie;
}

}   // namespace NKikimr::NColumnShard
