#include "../datashard_impl.h"
#include "hnsw_index_build_actor.h"

#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace NKikimr::NDataShard {

    namespace {

        constexpr ui64 ScanMetadataBytes = 256;

        // The executor scan yields between batches and handles page faults without
        // making the triggering read/write wait for either scanning or construction.
        class THnswSnapshotScan: public NTable::IScan {
        public:
            using TFinish = THnswSnapshotScanCallback;

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
                if (cells.empty() || cells[0].IsNull() || !THnswIndex::IsValidVector(cells[0].AsBuf(), Settings.vector_dimension())) {
                    return EScan::Feed;
                }
                auto serialized = TSerializedCellVec::Serialize(key);
                if (serialized.size() > Max<size_t>() - KeyBytes) {
                    Failed = true;
                    return EScan::Final;
                }
                KeyBytes += serialized.size();
                // Account for graph storage, both key buffers and scan input.
                // Saturating graph estimates must not wrap when adding input.
                if (KeyBytes > Max<size_t>() / 2 || Rows.size() == Max<size_t>()) {
                    Failed = true;
                    return EScan::Final;
                }
                const size_t count = Rows.size() + 1;
                const auto graphBytes = THnswIndex::EstimateMemoryBytes(count,
                                                                        Settings.vector_dimension(), Settings.has_m() ? Settings.m() : 16, 2 * KeyBytes);
                if (graphBytes == Max<size_t>() || count > (Max<size_t>() - graphBytes) / cells[0].Size()) {
                    Failed = true;
                    return EScan::Final;
                }
                const auto required = graphBytes + count * cells[0].Size();
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

            EScan Exhausted() override {
                return EScan::Final;
            }

            TAutoPtr<IDestructable> Finish(EStatus status) override {
                THnswSnapshotScanResult result;
                result.Success = !Failed && status == EStatus::Done;
                result.BelowMinRows = result.Success && !Rebuilding && (Rows.empty() || Rows.size() < GetHnswMinRows(Settings));
                result.Success &= !result.BelowMinRows;
                if (result.Success) {
                    result.Rows = std::move(Rows);
                    result.MemoryReservation = Reservation;
                    result.ReservedBytes = Reservation->Bytes;
                    result.AllowEmpty = Rebuilding;
                }
                Rows.clear();
                Reservation.reset();
                OnFinish(std::move(result));
                return this; // The executor owns and destroys the scan product.
            }

            void Describe(IOutputStream& out) const override {
                out << "HNSW snapshot build";
            }

        private:
            struct TReservation {
                explicit TReservation(std::shared_ptr<THnswCacheMemoryTracker> tracker)
                    : Tracker(std::move(tracker))
                {
                }
                ~TReservation() {
                    Tracker->Release(Bytes);
                }
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

    void TDataShard::TrackHnswOpenTransactions(ui32 localTid, const NTable::TDatabase& db, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) { return; }
        auto& entry = *cached;
        for (ui64 txId : db.GetOpenTxs(localTid)) {
            if (!entry.Pending.contains(txId) && !entry.UntrackedTransactions.contains(txId)) {
                auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes);
                if (!reservation) {
                    InvalidateHnswIndex(localTid, indexId);
                    DeferHnswIndexBuild(localTid, TDuration::Seconds(5), indexId);
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
                                           TRowVersion base, TTransactionContext& txc, THnswSnapshotScanCallback finish, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        const ui64 token = GetHnswBuildToken(localTid, indexId);
        if (!finish) {
            const auto* cached = FindHnswIndexEntry(localTid, indexId);
            if (!cached) { return; }
            const auto& entry = *cached;
            finish = [replyTo = SelfId(), localTid, indexId, tag = entry.VectorColumnTag, settings = entry.Settings,
                      base, token](THnswSnapshotScanResult&& result) {
                if (!result.Success) {
                    auto event = MakeHolder<TEvPrivate::TEvHnswIndexBuildResult>();
                    event->LocalTid = localTid;
                    event->IndexId = indexId;
                    event->BuildToken = token;
                    event->BelowMinRows = result.BelowMinRows;
                    event->Error = "Snapshot scan aborted or exceeded its memory budget";
                    TActivationContext::Send(new IEventHandle(replyTo, TActorId(), event.Release()));
                    return;
                }
                const auto count = result.Rows.size();
                auto* actor = CreateHnswIndexBuildActor(replyTo, localTid, tag, count, settings,
                                                        std::move(result.Rows), std::move(result.MemoryReservation), result.ReservedBytes,
                                                        base, token, result.AllowEmpty, indexId);
                TActivationContext::Register(actor, TActorId(), TMailboxType::HTSwap, AppData()->BatchPoolId);
            };
        }
        PromoteImmediatePostExecuteEdges(base, EPromotePostExecuteEdges::RepeatableRead, txc);
        // Exactly one transaction outcome owns completion. Rollback must also
        // wake an eager job that is waiting for its scan to be queued.
        auto completion = std::make_shared<THnswSnapshotScanCallback>(std::move(finish));
        txc.DB.OnRollback([this, localTid, indexId, token, completion] {
            if (IsHnswBuildCurrent(localTid, token, indexId)) {
                DeferHnswIndexBuild(localTid, TDuration::Zero(), indexId);
            }
            (*completion)(THnswSnapshotScanResult{});
        });
        txc.DB.OnCommit([this, localTid, indexId, table = std::move(table), base, token,
                         completion]() mutable {
            // QueueScan may flush memtables and emit redo. Starting it inside
            // DB.Commit would nest those records in the unfinished transaction.
            auto event = MakeHolder<TEvPrivate::TEvStartHnswSnapshotScan>();
            event->LocalTid = localTid;
            event->IndexId = indexId;
            event->Table = std::move(table);
            event->BaseVersion = base;
            event->BuildToken = token;
            event->Finish = std::move(*completion);
            Send(SelfId(), event.Release());
        });
    }

    void TDataShard::Handle(TEvPrivate::TEvStartHnswSnapshotScan::TPtr& ev, const TActorContext&) {
        auto& request = *ev->Get();
        if (IsFollower() || !IsHnswBuildCurrent(request.LocalTid, request.BuildToken, request.IndexId) || !AppData()->FeatureFlags.GetEnableHnswIndex() || IsStopping() || State != TShardState::Ready) {
            request.Finish(THnswSnapshotScanResult{});
            return;
        }
        const auto& entry = *FindHnswIndexEntry(request.LocalTid, request.IndexId);
        auto* scan = new THnswSnapshotScan(*request.Table, entry.VectorColumnTag, entry.Settings,
                                           HnswCacheMemoryTracker, bool(entry.Index), std::move(request.Finish));
        Executor()->QueueScan(request.LocalTid, scan, 0, NTabletFlatExecutor::TScanOptions().DisableResourceBroker().SetReadPrio(NTabletFlatExecutor::TScanOptions::EReadPrio::Low).SetReadAhead(0, 512_KB).SetSnapshotRowVersion(request.BaseVersion));
    }

    class TDataShard::TTxRebuildHnswIndex: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    public:
        TTxRebuildHnswIndex(TDataShard* self, ui32 tid, bool initialBuild, TPathId indexId)
            : TBase(self), LocalTid(tid), IndexId(self->ResolveHnswIndexId(tid, indexId)), InitialBuild(initialBuild) {}

        TTxType GetTxType() const override { return TXTYPE_READ; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (auto* entry = Self->FindHnswIndexEntry(LocalTid, IndexId)) { entry->RebuildScheduled = false; }
            if (Self->IsFollower() || Self->IsStopping() || Self->State != TShardState::Ready
                    || !AppData()->FeatureFlags.GetEnableHnswIndex()) { return true; }
            TUserTable::TCPtr table;
            for (const auto& [_, candidate] : Self->TableInfos) {
                if (candidate->LocalTid == LocalTid) { table = candidate; break; }
            }
            if (!table) { return true; }
            const auto* metadata = table->HnswIndexes.FindPtr(IndexId);
            if (!metadata || !metadata->VectorColumnTag || !table->Columns.contains(metadata->VectorColumnTag)
                    || metadata->Settings.vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) { return true; }
            if (InitialBuild && !Self->GetHnswCacheMemoryLimit()) {
                Retry = !Self->IsHnswCacheMemoryLimitKnown();
                return true;
            }
            auto* cached = Self->FindHnswIndexEntry(LocalTid, IndexId);
            if (!cached) { return true; }
            auto& entry = *cached;
            if (entry.Building || (InitialBuild && !entry.InitialBuildAttemptsLeft)
                    || entry.NextScanAttemptAt == TInstant::Max()) { return true; }
            if (InitialBuild ? bool(entry.Index) : (!entry.Index || !entry.Changes->Valid
                    || !entry.Index->NeedsRebuild(GetHnswDeltaRows(entry.Settings)))) { return true; }
            const auto base = Self->GetHnswBuildVersion();
            if ((entry.Index && base <= entry.Index->GetBaseVersion()) || base < Self->SnapshotManager.GetLowWatermark()
                    || base >= Self->Pipeline.GetUnreadableEdge() || Self->VolatileTxManager.HasVolatileTxsAtSnapshot(base)
                    || AppData()->TimeProvider->Now() < entry.NextScanAttemptAt) { Retry = true; return true; }
            const auto& settings = InitialBuild ? metadata->Settings : entry.Settings;
            if (Self->TryStartHnswIndexBuild(LocalTid, metadata->VectorColumnTag, settings, base, IndexId)) {
                if (InitialBuild) { --entry.InitialBuildAttemptsLeft; }
                Self->TrackHnswOpenTransactions(LocalTid, txc.DB, IndexId);
                if (Self->IsHnswBuildCurrent(LocalTid, Self->GetHnswBuildToken(LocalTid, IndexId), IndexId)) {
                    Self->StartHnswSnapshotScan(LocalTid, table, base, txc, {}, IndexId);
                } else { Retry = true; }
            }
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (Retry) {
                if (auto* entry = Self->FindHnswIndexEntry(LocalTid, IndexId)) {
                    entry->RebuildScheduled = true;
                    ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvRebuildHnswIndex(LocalTid, InitialBuild, IndexId));
                }
            }
        }
    private:
        ui32 LocalTid;
        TPathId IndexId;
        bool InitialBuild;
        bool Retry = false;
    };

    void TDataShard::Handle(TEvPrivate::TEvRebuildHnswIndex::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxRebuildHnswIndex(this, ev->Get()->LocalTid, ev->Get()->InitialBuild, ev->Get()->IndexId), ctx);
    }

    std::shared_ptr<THnswIndex> TDataShard::GetHnswIndex(ui32 localTid, ui32 vectorColumnTag,
                                                         const Ydb::Table::VectorIndexSettings& settings, bool useCachedHnswParameters,
                                                         TRowVersion readVersion, TPathId indexId) const {
        indexId = ResolveHnswIndexId(localTid, indexId);
        if (IsFollower()) {
            return {};
        }
        const auto* it = FindHnswIndexEntry(localTid, indexId);
        if (!it) {
            return {};
        }
        const auto& entry = *it;
        if (entry.VectorColumnTag != vectorColumnTag || !(AreHnswIndexSettingsCompatible(entry.Settings, settings) || (useCachedHnswParameters && entry.Settings.metric() == settings.metric() && entry.Settings.vector_type() == settings.vector_type() && entry.Settings.vector_dimension() == settings.vector_dimension()))) {
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
                                            const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        if (IsFollower() || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return false;
        }
        const auto* metadata = FindHnswIndexInfo(localTid, indexId);
        if (!metadata || metadata->VectorColumnTag != vectorColumnTag) { return false; }
        auto* cached = GetOrCreateHnswIndexEntry(localTid, indexId);
        if (!cached) { return false; }
        auto& entry = *cached;
        if (entry.Building) {
            return false;
        }
        const bool compatible = entry.VectorColumnTag == vectorColumnTag && AreHnswIndexSettingsCompatible(entry.Settings, settings);
        if (!compatible || !entry.Changes->Valid) {
            InvalidateHnswIndex(localTid, indexId);
            entry.Changes = std::make_shared<THnswIndexChanges>();
            entry.VectorColumnTag = vectorColumnTag;
            entry.Settings = settings;
        } else {
            if (entry.NextScanAttemptAt == TInstant::Max() && GetHnswMinRows(entry.Settings) != GetHnswMinRows(settings)) {
                // min_rows does not change graph compatibility, but changing it
                // must re-evaluate a partition previously below the threshold.
                entry.NextScanAttemptAt = TInstant::Zero();
            }
            if (AppData()->TimeProvider->Now() < entry.NextScanAttemptAt) {
                return false;
            }
            entry.Settings = settings;
        }
        entry.Rebuilds += bool(entry.Index);
        entry.Building = true;
        entry.BuildObsolete = false;
        entry.BuildVersion = baseVersion;
        entry.IndexSchemaVersion = FindHnswIndexInfo(localTid, indexId)->SchemaVersion;
        entry.BuildToken = ++NextHnswBuildToken;
        return true;
    }

    ui64 TDataShard::GetHnswBuildToken(ui32 localTid, TPathId indexId) const {
        indexId = ResolveHnswIndexId(localTid, indexId);
        const auto* it = FindHnswIndexEntry(localTid, indexId);
        return !it ? 0 : it->BuildToken;
    }

    bool TDataShard::IsHnswBuildCurrent(ui32 localTid, ui64 token, TPathId indexId) const {
        indexId = ResolveHnswIndexId(localTid, indexId);
        const auto* it = FindHnswIndexEntry(localTid, indexId);
        const auto* metadata = FindHnswIndexInfo(localTid, indexId);
        return it && metadata && it->IndexSchemaVersion == metadata->SchemaVersion && it->Building
            && it->BuildToken == token && !it->BuildObsolete && it->Changes->Valid;
    }

    void TDataShard::InvalidateHnswIndex(ui32 localTid, TPathId indexId) {
        if (auto* entry = FindHnswIndexEntry(localTid, indexId)) { InvalidateHnswIndexEntry(*entry); }
    }

    void TDataShard::InvalidateHnswTable(ui32 localTid) {
        if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
            for (auto& [_, entry] : table->second) { InvalidateHnswIndexEntry(entry); }
        }
    }

    void TDataShard::ReconcileHnswIndexes(const TUserTable& before, const TUserTable& after) {
        if (before.LocalTid != after.LocalTid || before.KeyColumnIds != after.KeyColumnIds
                || before.KeyColumnTypes != after.KeyColumnTypes
                || before.Range.From.GetBuffer() != after.Range.From.GetBuffer()
                || before.Range.To.GetBuffer() != after.Range.To.GetBuffer()
                || before.Range.FromInclusive != after.Range.FromInclusive
                || before.Range.ToInclusive != after.Range.ToInclusive) {
            InvalidateHnswTable(before.LocalTid);
            HnswIndexCache.erase(before.LocalTid);
            return;
        }
        auto table = HnswIndexCache.find(before.LocalTid);
        if (table == HnswIndexCache.end()) { return; }
        for (auto it = table->second.begin(); it != table->second.end();) {
            const auto* oldIndex = before.HnswIndexes.FindPtr(it->first);
            const auto* newIndex = after.HnswIndexes.FindPtr(it->first);
            bool unchanged = oldIndex && newIndex
                && oldIndex->VectorColumnTag == newIndex->VectorColumnTag
                && oldIndex->SchemaVersion == newIndex->SchemaVersion
                && AreHnswIndexSettingsCompatible(oldIndex->Settings, newIndex->Settings)
                && GetHnswMinRows(oldIndex->Settings) == GetHnswMinRows(newIndex->Settings)
                && GetHnswDeltaRows(oldIndex->Settings) == GetHnswDeltaRows(newIndex->Settings);
            if (unchanged) {
                const auto* oldColumn = before.Columns.FindPtr(oldIndex->VectorColumnTag);
                const auto* newColumn = after.Columns.FindPtr(newIndex->VectorColumnTag);
                unchanged = oldColumn && newColumn && oldColumn->Type == newColumn->Type
                    && oldColumn->NotNull == newColumn->NotNull;
            }
            if (unchanged) {
                ++it;
            } else {
                InvalidateHnswIndex(before.LocalTid, it->first);
                it = table->second.erase(it);
            }
        }
    }

    void TDataShard::InvalidateHnswIndexEntry(THnswIndexCacheEntry& entry) {
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
        if (HnswCacheMemoryTracker) { HnswCacheMemoryTracker->ResetDemand(); }
        ForEachHnswIndexEntry([&](ui32, const TPathId&, auto& entry) { InvalidateHnswIndexEntry(entry); });
    }

    void TDataShard::SetHnswIndex(ui32 localTid, std::shared_ptr<THnswIndex> index,
                                  std::shared_ptr<void> memoryReservation, ui64 rowCountAtBuild, ui32 vectorColumnTag,
                                  const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion, ui64 buildToken, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        if (buildToken && !IsHnswBuildCurrent(localTid, buildToken, indexId)) {
            return;
        }
        if (IsFollower() || IsStopping() || State != TShardState::Ready || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndex(localTid, indexId);
            DeferHnswIndexBuild(localTid, TDuration::Zero(), indexId);
            return;
        }
        auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) { return; }
        auto& entry = *cached;
        if (index && HnswCacheMemoryTracker->GetUsed() > GetHnswCacheMemoryLimit()) {
            DeferHnswIndexBuild(localTid, TDuration::Seconds(5), indexId);
            ScheduleHnswRebuild(localTid, indexId);
            return;
        }
        if (!index) {
            DeferHnswIndexBuild(localTid, TDuration::Seconds(5), indexId);
            ScheduleHnswRebuild(localTid, indexId);
            return;
        }
        if (!entry.Changes->Valid) {
            entry.Changes = std::make_shared<THnswIndexChanges>();
        }
        index->SetSnapshot(baseVersion, entry.Changes);
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
        entry.InitialBuildAttemptsLeft = 0;
        entry.RowCountAtBuild = rowCountAtBuild;
        entry.VectorColumnTag = vectorColumnTag;
        entry.Settings = settings;
        entry.Building = false;
        entry.BuildObsolete = false;
        entry.NextScanAttemptAt = TInstant::Zero();
        HnswCacheMemoryTracker->ResetDemand();
        PruneHnswIndexes();
        ScheduleHnswRebuild(localTid, indexId);
    }

    void TDataShard::PruneHnswIndexes() {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            InvalidateHnswIndexes();
            ForEachHnswIndexEntry([&](ui32, const TPathId&, auto& entry) {
                entry.InitialBuildAttemptsLeft = 0;
                entry.RebuildScheduled = false;
            });
            return;
        }
        const auto low = SnapshotManager.GetLowWatermark();
        ForEachHnswIndexEntry([&](ui32 tid, const TPathId&, auto& entry) {
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
        });
    }

    void TDataShard::ApplyHnswIndexChange(ui32 localTid, TString key, THnswIndexChanges::TVersion change, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        auto* it = FindHnswIndexEntry(localTid, indexId);
        if (!it || !it->Changes->Valid) {
            return;
        }
        auto& entry = *it;
        if ((entry.Building && change.Version <= entry.BuildVersion) || (entry.Index && change.Version <= entry.Index->GetBaseVersion())) {
            // A late commit below the captured base means that base was not stable.
            InvalidateHnswIndex(localTid, indexId);
            return;
        }
        entry.Changes->Set(std::move(key), change.Version, std::move(change.Vector),
                           std::move(change.MemoryReservation));
        entry.NextScanAttemptAt = TInstant::Zero();
        ScheduleHnswRebuild(localTid, indexId);
    }

    void TDataShard::UpdateHnswIndex(ui32 localTid, NTable::ERowOp rowOp,
                                     TConstArrayRef<TCell> keyCells, TArrayRef<const NIceDb::TUpdateOp> ops,
                                     NTable::TDatabase& db, TRowVersion version, ui64 writeTxId, TPathId indexId) {
        if (!indexId) {
            if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
                for (const auto& [id, _] : table->second) {
                    UpdateHnswIndex(localTid, rowOp, keyCells, ops, db, version, writeTxId, id);
                }
            }
            return;
        }
        indexId = ResolveHnswIndexId(localTid, indexId);
        auto* it = FindHnswIndexEntry(localTid, indexId);
        if (!it || !it->Changes->Valid || !it->VectorColumnTag) {
            return;
        }
        const auto changes = it->Changes;
        bool affected = rowOp == NTable::ERowOp::Erase;
        bool unsupported = rowOp == NTable::ERowOp::Reset;
        std::optional<TString> vector;
        for (const auto& op : ops) {
            if (op.Tag == it->VectorColumnTag && op.Op != NTable::ECellOp::Empty) {
                affected = true;
                if (op.Op == NTable::ECellOp::Reset) {
                    unsupported = true; // A schema default may supply the embedding.
                } else if (op.Op == NTable::ECellOp::Set) {
                    const auto cell = op.AsCell();
                    if (!cell.IsNull() && THnswIndex::IsValidVector(cell.AsBuf(), it->Settings.vector_dimension())) {
                        vector = TString(cell.AsBuf());
                    }
                }
            }
        }
        if (unsupported) {
            db.OnCommit([this, localTid, indexId, changes] {
                if (auto* cached = FindHnswIndexEntry(localTid, indexId); cached && cached->Changes == changes) {
                    InvalidateHnswIndex(localTid, indexId);
                }
            });
            return;
        }
        TString key;
        std::shared_ptr<void> reservation;
        if (affected) {
            key = TSerializedCellVec::Serialize(keyCells);
            reservation = TryReserveHnswCacheMemory(THnswIndexChanges::EstimateBytes(key, vector ? vector->size() : 0));
            if (!reservation) {
                db.OnCommit([this, localTid, indexId, changes] {
                if (auto* cached = FindHnswIndexEntry(localTid, indexId); cached && cached->Changes == changes) {
                    InvalidateHnswIndex(localTid, indexId);
                }
            });
                return;
            }
        }
        // Captures own the payload/reservation until commit. Database rollback
        // destroys the callback, so retries cannot leak changes into the cache.
        db.OnCommit([this, localTid, indexId, changes, affected, writeTxId, key = std::move(key),
                     change = THnswIndexChanges::TVersion{version, std::move(vector), std::move(reservation)}]() mutable {
            auto* it = FindHnswIndexEntry(localTid, indexId);
            if (!it || it->Changes != changes || !changes->Valid) {
                return;
            }
            if (writeTxId) {
                if (!it->PendingReservations.contains(writeTxId)) {
                    auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes);
                    if (!reservation) {
                        InvalidateHnswIndex(localTid, indexId);
                        return;
                    }
                    it->PendingReservations.emplace(writeTxId, std::move(reservation));
                }
                auto& pending = it->Pending[writeTxId];
                if (affected) {
                    pending[std::move(key)] = std::move(change);
                }
            } else if (affected) {
                ApplyHnswIndexChange(localTid, std::move(key), std::move(change), indexId);
            }
        });
    }

    void TDataShard::CommitHnswIndexChanges(ui32 localTid, ui64 txId, TRowVersion version, NTable::TDatabase& db, TPathId indexId) {
        if (!indexId) {
            if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
                for (const auto& [id, _] : table->second) {
                    CommitHnswIndexChanges(localTid, txId, version, db, id);
                }
            }
            return;
        }
        indexId = ResolveHnswIndexId(localTid, indexId);
        const auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) {
            return;
        }
        db.OnCommit([this, localTid, indexId, changes = cached->Changes, txId, version] {
            auto* it = FindHnswIndexEntry(localTid, indexId);
            if (!it || it->Changes != changes || !changes->Valid) {
                return;
            }
            auto& entry = *it;
            auto pending = entry.Pending.find(txId);
            if (entry.UntrackedTransactions.erase(txId) || pending == entry.Pending.end()) {
                InvalidateHnswIndex(localTid, indexId);
                return;
            }
            auto committed = std::move(pending->second);
            entry.Pending.erase(pending);
            entry.PendingReservations.erase(txId);
            for (auto& [key, change] : committed) {
                change.Version = version;
                ApplyHnswIndexChange(localTid, key, std::move(change), indexId);
            }
        });
    }

    void TDataShard::AbortHnswIndexChanges(ui32 localTid, ui64 txId, NTable::TDatabase& db, TPathId indexId) {
        if (!indexId) {
            if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
                for (const auto& [id, _] : table->second) {
                    AbortHnswIndexChanges(localTid, txId, db, id);
                }
            }
            return;
        }
        indexId = ResolveHnswIndexId(localTid, indexId);
        const auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) {
            return;
        }
        db.OnCommit([this, localTid, indexId, changes = cached->Changes, txId] {
            if (auto* it = FindHnswIndexEntry(localTid, indexId); it && it->Changes == changes) {
                it->Pending.erase(txId);
                it->PendingReservations.erase(txId);
                it->UntrackedTransactions.erase(txId);
            }
        });
    }

    void TDataShard::ScheduleHnswInitialBuilds() {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || IsFollower() || State != TShardState::Ready) { return; }
        for (const auto& [_, table] : TableInfos) {
            for (const auto& [indexId, metadata] : table->HnswIndexes) {
                if (!metadata.VectorColumnTag) { continue; }
                auto* entry = GetOrCreateHnswIndexEntry(table->LocalTid, indexId);
                if (!entry) { continue; }
                if (entry->Index || entry->Building || entry->RebuildScheduled || entry->InitialBuildAttemptsLeft) { continue; }
                entry->InitialBuildAttemptsLeft = 30;
                entry->RebuildScheduled = true;
                Send(SelfId(), new TEvPrivate::TEvRebuildHnswIndex(table->LocalTid, true, indexId));
            }
        }
    }

    void TDataShard::ScheduleHnswRebuild(ui32 localTid, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || IsFollower()) {
            return;
        }
        auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) { return; }
        auto& entry = *cached;
        const bool initialBuild = !entry.Index && entry.InitialBuildAttemptsLeft;
        if (!entry.Building && !entry.RebuildScheduled && entry.NextScanAttemptAt != TInstant::Max() && (initialBuild || (entry.Index && entry.Changes->Valid && entry.Index->NeedsRebuild(GetHnswDeltaRows(entry.Settings))))) {
            entry.RebuildScheduled = true;
            Send(SelfId(), new TEvPrivate::TEvRebuildHnswIndex(localTid, initialBuild, indexId));
        }
    }

} // namespace NKikimr::NDataShard
