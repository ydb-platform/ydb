#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "inflight_request_tracker.h"

#include "engines/column_engine.h"
#include "engines/reader/plain_reader/constructor/read_metadata.h"
#include "hooks/abstract/abstract.h"
#include "tablet/ext_tx_base.h"

namespace NKikimr::NColumnShard {

NOlap::NReader::TReadMetadataBase::TConstPtr TInFlightReadsTracker::ExtractInFlightRequest(
    ui64 cookie, const NOlap::TVersionedIndex* /*index*/, const TInstant now) {
    auto it = RequestsMeta.find(cookie);
    AFL_VERIFY(it != RequestsMeta.end())("cookie", cookie);
    AFL_VERIFY(ActorIds.erase(cookie));
    const NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase = it->second;

    {
        auto it = SnapshotsLive.find(readMetaBase->GetRequestSnapshot());
        AFL_VERIFY(it != SnapshotsLive.end());
        Y_UNUSED(it->second.DelRequest(cookie, now));
    }
    Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());

    RequestsMeta.erase(cookie);
    return readMetaBase;
}

void TInFlightReadsTracker::AddToInFlightRequest(
    const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* /*index*/) {
    AFL_VERIFY(RequestsMeta.emplace(cookie, readMetaBase).second);

    auto readMeta = std::dynamic_pointer_cast<const NOlap::NReader::NPlain::TReadMetadata>(readMetaBase);

    if (!readMeta) {
        return;
    }
}

namespace {
class TTransactionSavePersistentSnapshots: public TExtendedTransactionBase {
private:
    using TBase = TExtendedTransactionBase;
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
        : TBase(self, "save_persistent_snapshots")
        , SaveSnapshots(std::move(saveSnapshots))
        , RemoveSnapshots(std::move(removeSnapshots)) {
        AFL_VERIFY(SaveSnapshots.size() || RemoveSnapshots.size());
    }
};
}   // namespace

std::unique_ptr<NTabletFlatExecutor::ITransaction> TInFlightReadsTracker::Ping(
    TColumnShard* self, const TDuration stalenessInMem, const TDuration usedSnapshotLivetime, const TInstant now) {
    std::set<NOlap::TSnapshot> snapshotsToSave;
    std::set<NOlap::TSnapshot> snapshotsToFreeInDB;
    std::set<NOlap::TSnapshot> snapshotsToFreeInMem;
    for (auto&& i : SnapshotsLive) {
        if (i.second.IsExpired(usedSnapshotLivetime, now)) {
            if (i.second.GetIsLock()) {
                Counters->OnSnapshotUnlocked();
                snapshotsToFreeInDB.emplace(i.first);
            }
            snapshotsToFreeInMem.emplace(i.first);
        } else if (i.second.CheckToLock(stalenessInMem, usedSnapshotLivetime, now)) {
            Counters->OnSnapshotLocked();
            snapshotsToSave.emplace(i.first);
        }
    }
    for (auto&& i : snapshotsToFreeInMem) {
        SnapshotsLive.erase(i);
    }
    Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());
    if (snapshotsToFreeInDB.size() || snapshotsToSave.size()) {
        NYDBTest::TControllers::GetColumnShardController()->OnRequestTracingChanges(snapshotsToSave, snapshotsToFreeInMem);
        return std::make_unique<TTransactionSavePersistentSnapshots>(self, std::move(snapshotsToSave), std::move(snapshotsToFreeInDB));
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

ui64 TInFlightReadsTracker::AddInFlightRequest(NOlap::NReader::TReadMetadataBase::TConstPtr readMeta, const NOlap::TVersionedIndex* index) {
    const ui64 cookie = NextCookie++;
    auto it = SnapshotsLive.find(readMeta->GetRequestSnapshot());
    if (it == SnapshotsLive.end()) {
        it = SnapshotsLive.emplace(readMeta->GetRequestSnapshot(), TSnapshotLiveInfo::BuildFromRequest(readMeta->GetRequestSnapshot())).first;
        Counters->OnSnapshotsInfo(SnapshotsLive.size(), GetSnapshotToClean());
    }
    it->second.AddRequest(cookie);
    AddToInFlightRequest(cookie, readMeta, index);
    return cookie;
}

}   // namespace NKikimr::NColumnShard
