#include "datashard_impl.h"
#include "hnsw_index_build_actor.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace NKikimr::NDataShard {

namespace {

constexpr ui64 ScanMetadataBytes = 256;

// The executor scan yields between batches and handles page faults without
// making the triggering read/write wait for either scanning or construction.
class THnswSnapshotScan : public NTable::IScan {
public:
    using TFinish = std::function<void(std::vector<std::pair<TString, TString>>,
        std::shared_ptr<void>, ui64, bool)>;

    THnswSnapshotScan(const TUserTable& table, ui32 vectorTag,
            Ydb::Table::VectorIndexSettings settings,
            std::shared_ptr<THnswCacheMemoryTracker> tracker, bool rebuilding, TFinish finish)
        : Range(table.Range)
        , Tags{vectorTag}
        , Settings(std::move(settings))
        , Reservation(std::make_shared<TReservation>(std::move(tracker)))
        , OnFinish(std::move(finish))
        , Rebuilding(rebuilding)
    {
        if (Reservation->Tracker->TryAcquire(ScanMetadataBytes)) {
            Reservation->Bytes = ScanMetadataBytes;
        } else {
            Failed = true;
        }
    }

    TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) override {
        return {Failed ? EScan::Final : EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) override {
        if (seq) {
            return EScan::Final;
        }
        const auto range = Range.ToTableRange();
        lead.To(Tags, range.From, range.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper);
        if (range.To) {
            lead.Until(range.To, range.InclusiveTo);
        }
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
        const auto cells = *row;
        if (cells.empty() || cells[0].IsNull()
                || !THnswIndex::IsValidVector(cells[0].AsBuf(), Settings.vector_dimension())) {
            return EScan::Feed;
        }
        auto serialized = TSerializedCellVec::Serialize(key);
        KeyBytes += serialized.size();
        // Account for both graph storage and the scan/build input buffer.
        const auto required = THnswIndex::EstimateMemoryBytes(Rows.size() + 1,
            Settings.vector_dimension(), Settings.has_hnsw_connectivity() ? Settings.hnsw_connectivity() : 16,
            2 * KeyBytes) + (Rows.size() + 1) * cells[0].Size();
        if (required > Reservation->Bytes) {
            const ui64 additional = required - Reservation->Bytes;
            if (!Reservation->Tracker->TryAcquire(additional)) {
                Failed = true;
                return EScan::Final;
            }
            Reservation->Bytes += additional;
        }
        Rows.emplace_back(std::move(serialized), TString(cells[0].AsBuf()));
        return EScan::Feed;
    }

    EScan Exhausted() override { return EScan::Final; }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        const bool success = !Failed && status == EStatus::Done
            && (Rebuilding || Rows.size() >= GetHnswMinRows(Settings));
        if (!success) {
            Rows.clear();
        }
        OnFinish(std::move(Rows), Reservation, Reservation->Bytes, success && Rebuilding);
        return this; // The executor owns and destroys the scan product.
    }

    void Describe(IOutputStream& out) const override { out << "HNSW snapshot build"; }

private:
    struct TReservation {
        explicit TReservation(std::shared_ptr<THnswCacheMemoryTracker> tracker)
            : Tracker(std::move(tracker)) {}
        ~TReservation() { Tracker->Release(Bytes); }
        std::shared_ptr<THnswCacheMemoryTracker> Tracker;
        ui64 Bytes = 0;
    };
    TSerializedTableRange Range;
    std::vector<ui32> Tags;
    Ydb::Table::VectorIndexSettings Settings;
    std::shared_ptr<TReservation> Reservation;
    TFinish OnFinish;
    std::vector<std::pair<TString, TString>> Rows;
    size_t KeyBytes = 0;
    bool Failed = false;
    bool Rebuilding;
};

} // namespace

void TDataShard::TrackHnswOpenTransactions(ui32 localTid, const NTable::TDatabase& db) {
    auto& entry = HnswIndexCache.at(localTid);
    for (ui64 txId : db.GetOpenTxs(localTid)) {
        if (!entry.Pending.contains(txId) && !entry.UntrackedTransactions.contains(txId)) {
            auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes);
            if (!reservation) {
                InvalidateHnswIndex(localTid);
                DeferHnswIndexBuild(localTid, TDuration::Seconds(5));
                return;
            }
            entry.PendingReservations.emplace(txId, std::move(reservation));
            entry.UntrackedTransactions.insert(txId);
        }
    }
}

TRowVersion TDataShard::GetHnswBuildVersion() const {
    // Include already applied immediate writes, even before their replies.
    // Starting from an old requested snapshot would miss writes predating the journal.
    return Max(GetMvccTxVersion(EMvccTxMode::ReadOnly), SnapshotManager.GetImmediateWriteEdge());
}

void TDataShard::StartHnswSnapshotScan(ui32 localTid, TUserTable::TCPtr table,
        TRowVersion base, TTransactionContext& txc) {
    const ui64 token = GetHnswBuildToken(localTid);
    PromoteImmediatePostExecuteEdges(base, EPromotePostExecuteEdges::RepeatableRead, txc);
    txc.DB.OnRollback([this, localTid, token] {
        if (IsHnswBuildCurrent(localTid, token)) {
            DeferHnswIndexBuild(localTid, TDuration::Zero());
        }
    });
    txc.DB.OnCommit([this, localTid, table = std::move(table), base, token] {
        if (!IsHnswBuildCurrent(localTid, token)) {
            return;
        }
        const auto& entry = HnswIndexCache.at(localTid);
        auto* scan = new THnswSnapshotScan(*table, entry.VectorColumnTag, entry.Settings,
            HnswCacheMemoryTracker, bool(entry.Index),
            [replyTo = SelfId(), localTid, tag = entry.VectorColumnTag, settings = entry.Settings,
                    base, token](auto rows, auto reservation, ui64 bytes, bool allowEmpty) mutable {
                const auto count = rows.size();
                auto* actor = CreateHnswIndexBuildActor(replyTo, localTid, tag, count, settings,
                    std::move(rows), std::move(reservation), bytes, base, token, allowEmpty);
                TActivationContext::Register(actor, TActorId(), TMailboxType::HTSwap, AppData()->BatchPoolId);
            });
        Executor()->QueueScan(localTid, scan, 0, NTabletFlatExecutor::TScanOptions()
            .DisableResourceBroker()
            .SetReadPrio(NTabletFlatExecutor::TScanOptions::EReadPrio::Low)
            .SetReadAhead(0, 512_KB)
            .SetSnapshotRowVersion(base));
    });
}

class TDataShard::TTxRebuildHnswIndex : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxRebuildHnswIndex(TDataShard* self, ui32 tid) : TBase(self), LocalTid(tid) {}
    TTxType GetTxType() const override { return TXTYPE_READ; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto it = Self->HnswIndexCache.find(LocalTid);
        if (it == Self->HnswIndexCache.end() || Self->IsFollower() || Self->IsStopping()) {
            return true;
        }
        auto& entry = it->second;
        entry.RebuildScheduled = false;
        if (entry.Building || !entry.Index || !entry.Changes->Valid
                || !entry.Index->NeedsRebuild(GetHnswRebuildThresholdPercent(entry.Settings))) {
            return true;
        }
        const auto base = Self->GetHnswBuildVersion();
        if (base <= entry.Index->GetBaseVersion() || base < Self->SnapshotManager.GetLowWatermark()
                || base >= Self->Pipeline.GetUnreadableEdge()
                || Self->VolatileTxManager.HasVolatileTxsAtSnapshot(base)
                || TInstant::Now() < entry.NextScanAttemptAt) {
            Retry = true;
            return true;
        }
        for (const auto& [_, table] : Self->TableInfos) {
            if (table->LocalTid == LocalTid
                    && Self->TryStartHnswIndexBuild(LocalTid, entry.VectorColumnTag, entry.Settings, base)) {
                Self->TrackHnswOpenTransactions(LocalTid, txc.DB);
                Self->StartHnswSnapshotScan(LocalTid, table, base, txc);
                break;
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Retry) {
            auto it = Self->HnswIndexCache.find(LocalTid);
            if (it != Self->HnswIndexCache.end()) {
                it->second.RebuildScheduled = true;
                ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRebuildHnswIndex(LocalTid));
            }
        }
    }
private:
    ui32 LocalTid;
    bool Retry = false;
};

void TDataShard::Handle(TEvPrivate::TEvRebuildHnswIndex::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxRebuildHnswIndex(this, ev->Get()->LocalTid), ctx);
}

std::shared_ptr<THnswIndex> TDataShard::GetHnswIndex(ui32 localTid, ui32 vectorColumnTag,
        const Ydb::Table::VectorIndexSettings& settings, bool useCachedHnswParameters,
        TRowVersion readVersion) const {
    const auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end()) {
        return {};
    }
    const auto& entry = it->second;
    if (entry.VectorColumnTag != vectorColumnTag
            || !(AreHnswIndexSettingsCompatible(entry.Settings, settings)
                || (useCachedHnswParameters && entry.Settings.metric() == settings.metric()
                    && entry.Settings.vector_type() == settings.vector_type()
                    && entry.Settings.vector_dimension() == settings.vector_dimension()))) {
        return {};
    }
    if (entry.Index && entry.Index->CanRead(readVersion)) {
        return entry.Index;
    }
    for (auto generation = entry.Retained.rbegin(); generation != entry.Retained.rend(); ++generation) {
        if ((*generation)->CanRead(readVersion)) {
            return *generation;
        }
    }
    return {};
}

bool TDataShard::TryStartHnswIndexBuild(ui32 localTid, ui32 vectorColumnTag,
        const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion) {
    auto& entry = HnswIndexCache[localTid];
    if (entry.Building) {
        return false;
    }
    const bool compatible = entry.VectorColumnTag == vectorColumnTag
        && AreHnswIndexSettingsCompatible(entry.Settings, settings);
    if (!compatible || !entry.Changes->Valid) {
        InvalidateHnswIndex(localTid);
        entry.Changes = std::make_shared<THnswIndexChanges>();
        entry.VectorColumnTag = vectorColumnTag;
        entry.Settings = settings;
    } else if (TInstant::Now() < entry.NextScanAttemptAt) {
        return false;
    }
    entry.Rebuilds += bool(entry.Index);
    entry.Building = true;
    entry.BuildObsolete = false;
    entry.BuildVersion = baseVersion;
    entry.BuildToken = ++NextHnswBuildToken;
    return true;
}

ui64 TDataShard::GetHnswBuildToken(ui32 localTid) const {
    return HnswIndexCache.at(localTid).BuildToken;
}

bool TDataShard::IsHnswBuildCurrent(ui32 localTid, ui64 token) const {
    const auto it = HnswIndexCache.find(localTid);
    return it != HnswIndexCache.end() && it->second.Building
        && it->second.BuildToken == token && !it->second.BuildObsolete
        && it->second.Changes->Valid;
}

void TDataShard::InvalidateHnswIndex(ui32 localTid) {
    auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end()) {
        return;
    }
    auto& entry = it->second;
    // Readers may still own a graph. They must stop using it if its journal
    // no longer receives every write (including eviction on memory pressure).
    entry.Changes->Valid = false;
    entry.Changes = std::make_shared<THnswIndexChanges>();
    entry.Changes->Valid = false;
    entry.Index.reset();
    entry.Retained.clear();
    entry.Generations.clear();
    entry.Pending.clear();
    entry.PendingReservations.clear();
    entry.UntrackedTransactions.clear();
    entry.RowCountAtBuild = 0;
    entry.BuildObsolete = entry.Building;
    entry.NextScanAttemptAt = TInstant::Zero();
}

void TDataShard::InvalidateHnswIndexes() {
    if (HnswCacheMemoryTracker) {
        HnswCacheMemoryTracker->ResetDemand();
    }
    for (const auto& [tid, _] : HnswIndexCache) {
        InvalidateHnswIndex(tid);
    }
}

void TDataShard::PrepareFollowerHnswIndex(ui32 localTid, const NTable::TDatabase& db,
        const TRowVersion& readVersion) {
    auto& entry = HnswIndexCache[localTid];
    const auto counter = db.Head(localTid);
    if (entry.FollowerChangeCounter == counter && entry.Index
            && entry.Index->GetBaseVersion() <= readVersion) {
        // Advancing the repeatable edge without a table mutation does not
        // change any vector. Extend coverage instead of rebuilding copies.
        if (!entry.Index->CanRead(readVersion)) {
            entry.Index->SetSnapshot(entry.Index->GetBaseVersion(), entry.Changes, readVersion);
        }
        entry.FollowerReadVersion = readVersion;
        return;
    }
    if (entry.FollowerChangeCounter != counter || entry.FollowerReadVersion != readVersion) {
        // Freeze coverage when redo changes the table. Existing generations
        // remain useful for the snapshots already covered before that change.
        if (entry.Index) {
            entry.Retained.push_back(std::move(entry.Index));
        }
        entry.BuildObsolete = entry.Building;
        entry.NextScanAttemptAt = TInstant::Zero();
        entry.FollowerChangeCounter = counter;
        entry.FollowerReadVersion = readVersion;
    }
    PruneHnswIndexes();
}

void TDataShard::SetHnswIndex(ui32 localTid, std::shared_ptr<THnswIndex> index,
        std::shared_ptr<void> memoryReservation, ui64 rowCountAtBuild, ui32 vectorColumnTag,
        const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion, ui64 buildToken) {
    if (buildToken && !IsHnswBuildCurrent(localTid, buildToken)) {
        return;
    }
    auto& entry = HnswIndexCache[localTid];
    if (index && HnswCacheMemoryTracker->GetUsed() > GetHnswCacheMemoryLimit()) {
        DeferHnswIndexBuild(localTid, TDuration::Seconds(5));
        ScheduleHnswRebuild(localTid);
        return;
    }
    if (!index) {
        DeferHnswIndexBuild(localTid, TDuration::Seconds(5));
        ScheduleHnswRebuild(localTid);
        return;
    }
    if (!entry.Changes->Valid) {
        entry.Changes = std::make_shared<THnswIndexChanges>();
    }
    index->SetSnapshot(baseVersion, entry.Changes,
        IsFollower() ? baseVersion : TRowVersion::Max());
    Y_ENSURE(memoryReservation, "HNSW index installed without a memory reservation");
    struct TOwnedIndex {
        std::shared_ptr<void> Reservation;
        std::shared_ptr<THnswIndex> Index;
    };
    auto owned = std::make_shared<TOwnedIndex>(TOwnedIndex{std::move(memoryReservation), std::move(index)});
    index = std::shared_ptr<THnswIndex>(owned, owned->Index.get());
    entry.Generations.push_back(index);
    if (entry.Index && entry.Index->GetBaseVersion() > baseVersion) {
        entry.Retained.push_back(std::move(index));
    } else {
        if (entry.Index && entry.Index->GetBaseVersion() != baseVersion) {
            entry.Retained.push_back(std::move(entry.Index));
        }
        entry.Index = std::move(index);
    }
    Sort(entry.Retained, [](const auto& a, const auto& b) {
        return a->GetBaseVersion() < b->GetBaseVersion();
    });
    entry.RowCountAtBuild = rowCountAtBuild;
    entry.VectorColumnTag = vectorColumnTag;
    entry.Settings = settings;
    entry.Building = false;
    entry.BuildObsolete = false;
    entry.NextScanAttemptAt = TInstant::Zero();
    HnswCacheMemoryTracker->ResetDemand();
    PruneHnswIndexes();
    ScheduleHnswRebuild(localTid);
}

void TDataShard::PruneHnswIndexes() {
    const auto low = SnapshotManager.GetLowWatermark();
    for (auto& [tid, entry] : HnswIndexCache) {
        TRowVersion nextBase = entry.Index ? entry.Index->GetBaseVersion() : TRowVersion::Max();
        for (size_t i = entry.Retained.size(); i-- > 0;) {
            const auto base = entry.Retained[i]->GetBaseVersion();
            bool keep = nextBase > low;
            for (const auto& [tableId, table] : TableInfos) {
                if (table->LocalTid == tid) {
                    for (const auto& [key, _] : SnapshotManager.GetSnapshots({GetPathOwnerId(), tableId})) {
                        const TRowVersion version(key.Step, key.TxId);
                        keep |= base <= version && version < nextBase;
                    }
                    break;
                }
            }
            nextBase = base;
            if (!keep) {
                entry.Retained.erase(entry.Retained.begin() + i);
            }
        }
        TRowVersion oldestBase = entry.Building ? entry.BuildVersion : TRowVersion::Max();
        for (auto it = entry.Generations.begin(); it != entry.Generations.end();) {
            if (auto generation = it->lock()) {
                oldestBase = Min(oldestBase, generation->GetBaseVersion());
                ++it;
            } else {
                it = entry.Generations.erase(it);
            }
        }
        if (!oldestBase.IsMax()) {
            entry.Changes->PruneThrough(oldestBase);
        }
    }
}

void TDataShard::ApplyHnswIndexChange(ui32 localTid, TString key, THnswIndexChanges::TVersion change) {
    auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end() || !it->second.Changes->Valid) {
        return;
    }
    auto& entry = it->second;
    if ((entry.Building && change.Version <= entry.BuildVersion)
            || (entry.Index && change.Version <= entry.Index->GetBaseVersion())) {
        // A late commit below the captured base means that base was not stable.
        InvalidateHnswIndex(localTid);
        return;
    }
    entry.Changes->Set(std::move(key), change.Version, std::move(change.Vector),
        std::move(change.MemoryReservation));
    entry.NextScanAttemptAt = TInstant::Zero();
    ScheduleHnswRebuild(localTid);
}

void TDataShard::UpdateHnswIndex(ui32 localTid, NTable::ERowOp rowOp,
        TConstArrayRef<TCell> keyCells, TArrayRef<const NIceDb::TUpdateOp> ops,
        NTable::TDatabase& db, TRowVersion version, ui64 writeTxId) {
    auto it = HnswIndexCache.find(localTid);
    if (it == HnswIndexCache.end() || !it->second.Changes->Valid || !it->second.VectorColumnTag) {
        return;
    }
    const auto changes = it->second.Changes;
    bool affected = rowOp == NTable::ERowOp::Erase;
    bool unsupported = rowOp == NTable::ERowOp::Reset;
    std::optional<TString> vector;
    for (const auto& op : ops) {
        if (op.Tag == it->second.VectorColumnTag && op.Op != NTable::ECellOp::Empty) {
            affected = true;
            if (op.Op == NTable::ECellOp::Reset) {
                unsupported = true; // A schema default may supply the embedding.
            } else if (op.Op == NTable::ECellOp::Set) {
                const auto cell = op.AsCell();
                if (!cell.IsNull() && THnswIndex::IsValidVector(cell.AsBuf(), it->second.Settings.vector_dimension())) {
                    vector = TString(cell.AsBuf());
                }
            }
        }
    }
    if (unsupported) {
        db.OnCommit([this, localTid] { InvalidateHnswIndex(localTid); });
        return;
    }
    TString key;
    std::shared_ptr<void> reservation;
    if (affected) {
        key = TSerializedCellVec::Serialize(keyCells);
        reservation = TryReserveHnswCacheMemory(THnswIndexChanges::EstimateBytes(key, vector ? vector->size() : 0));
        if (!reservation) {
            db.OnCommit([this, localTid] { InvalidateHnswIndex(localTid); });
            return;
        }
    }
    // Captures own the payload/reservation until commit. Database rollback
    // destroys the callback, so retries cannot leak changes into the cache.
    db.OnCommit([this, localTid, changes, affected, writeTxId, key = std::move(key),
            change = THnswIndexChanges::TVersion{version, std::move(vector), std::move(reservation)}]() mutable {
        auto it = HnswIndexCache.find(localTid);
        if (it == HnswIndexCache.end() || it->second.Changes != changes || !changes->Valid) {
            return;
        }
        if (writeTxId) {
            if (!it->second.PendingReservations.contains(writeTxId)) {
                auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes);
                if (!reservation) {
                    InvalidateHnswIndex(localTid);
                    return;
                }
                it->second.PendingReservations.emplace(writeTxId, std::move(reservation));
            }
            auto& pending = it->second.Pending[writeTxId];
            if (affected) {
                pending[std::move(key)] = std::move(change);
            }
        } else if (affected) {
            ApplyHnswIndexChange(localTid, std::move(key), std::move(change));
        }
    });
}

void TDataShard::CommitHnswIndexChanges(ui32 localTid, ui64 txId, TRowVersion version, NTable::TDatabase& db) {
    if (!HnswIndexCache.contains(localTid)) {
        return;
    }
    db.OnCommit([this, localTid, txId, version] {
        auto it = HnswIndexCache.find(localTid);
        if (it == HnswIndexCache.end()) {
            return;
        }
        auto& entry = it->second;
        auto pending = entry.Pending.find(txId);
        if (entry.UntrackedTransactions.erase(txId) || pending == entry.Pending.end()) {
            InvalidateHnswIndex(localTid);
            return;
        }
        auto committed = std::move(pending->second);
        entry.Pending.erase(pending);
        entry.PendingReservations.erase(txId);
        for (auto& [key, change] : committed) {
            change.Version = version;
            ApplyHnswIndexChange(localTid, key, std::move(change));
        }
    });
}

void TDataShard::AbortHnswIndexChanges(ui32 localTid, ui64 txId, NTable::TDatabase& db) {
    if (!HnswIndexCache.contains(localTid)) {
        return;
    }
    db.OnCommit([this, localTid, txId] {
        if (auto it = HnswIndexCache.find(localTid); it != HnswIndexCache.end()) {
            it->second.Pending.erase(txId);
            it->second.PendingReservations.erase(txId);
            it->second.UntrackedTransactions.erase(txId);
        }
    });
}

void TDataShard::ScheduleHnswRebuild(ui32 localTid) {
    if (IsFollower()) {
        return;
    }
    auto& entry = HnswIndexCache.at(localTid);
    if (!entry.Building && !entry.RebuildScheduled && entry.Index
            && entry.Changes->Valid
            && entry.Index->NeedsRebuild(GetHnswRebuildThresholdPercent(entry.Settings))) {
        entry.RebuildScheduled = true;
        Send(SelfId(), new TEvPrivate::TEvRebuildHnswIndex(localTid));
    }
}

} // namespace NKikimr::NDataShard
