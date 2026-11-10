#include <ydb/core/tx/datashard/datashard_impl.h>
#include "hnsw_index_build_actor.h"
#include <ydb/core/tx/datashard/build_index/kmeans_helper.h>

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
                              std::shared_ptr<THnswCacheMemoryTracker> tracker, ui64 owner, bool rebuilding,
                              std::shared_ptr<std::atomic<bool>> cancelled, TFinish finish)
                : Range(table.Range)
                , Tags{vectorTag}
                , Settings(std::move(settings))
                , Reservation(std::make_shared<TReservation>(std::move(tracker), owner))
                , OnFinish(std::move(finish))
                , Rebuilding(rebuilding)
                , Cancelled(std::move(cancelled))
            {
                if (Reservation->Tracker->TryAcquire(ScanMetadataBytes, Reservation->Owner)) {
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
                lead = NKMeans::CreateLeadFrom(Range.ToTableRange());
                lead.SetTags(Tags);
                return EScan::Feed;
            }

            EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
                if (Cancelled && Cancelled->load(std::memory_order_relaxed)) {
                    Failed = true;
                    return EScan::Final;
                }
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
                                                                        Settings.vector_dimension(), Settings.has_m() ? Settings.m() : DefaultHnswM, 2 * KeyBytes);
                if (graphBytes == Max<size_t>() || count > (Max<size_t>() - graphBytes) / cells[0].Size()) {
                    Failed = true;
                    return EScan::Final;
                }
                const auto required = graphBytes + count * cells[0].Size();
                if (required > Reservation->Bytes) {
                    const ui64 additional = required - Reservation->Bytes;
                    if (!Reservation->Tracker->TryAcquire(additional, Reservation->Owner)) {
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
                result.Success = !Failed && status == EStatus::Done
                    && (!Cancelled || !Cancelled->load(std::memory_order_relaxed));
                result.BelowMinRows = result.Success && !Rebuilding && (Rows.empty() || Rows.size() < GetHnswMinRows(Settings));
                result.Success &= !result.BelowMinRows;
                if (result.Success) {
                    result.Rows = std::move(Rows);
                    result.MemoryReservation = Reservation;
                    result.ReservedBytes = Reservation->Bytes;
                    result.AllowEmpty = Rebuilding;
                    result.ReduceReservation = [reservation = Reservation](ui64 bytes) { reservation->ReduceTo(bytes); };
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
                explicit TReservation(std::shared_ptr<THnswCacheMemoryTracker> tracker, ui64 owner)
                    : Tracker(std::move(tracker))
                    , Owner(owner)
                {
                }
                void ReduceTo(ui64 bytes) {
                    const ui64 released = Bytes - Min(Bytes, bytes);
                    Bytes -= released;
                    Tracker->Release(released, Owner);
                }
                ~TReservation() {
                    Tracker->Release(Bytes, Owner);
                }
                std::shared_ptr<THnswCacheMemoryTracker> Tracker;
                ui64 Bytes = 0;
                ui64 Owner = 0;
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
            std::shared_ptr<std::atomic<bool>> Cancelled;
        };

    } // namespace

    void TDataShard::TrackHnswOpenTransactions(ui32 localTid, const NTable::TDatabase& db, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        auto* cached = FindHnswIndexEntry(localTid, indexId);
        if (!cached) { return; }
        auto& entry = *cached;
        if (entry.OpenTransactionsTracked) { return; }
        entry.OpenTransactionsTracked = true;
        for (ui64 txId : db.GetOpenTxs(localTid)) {
            if (!entry.Pending.contains(txId) && !entry.UntrackedTransactions.contains(txId)) {
                auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes, entry.MemoryOwner);
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
            finish = [replyTo = SelfId(), localTid, indexId, token,
                      memoryOwner = entry.MemoryOwner](THnswSnapshotScanResult&& result) {
                auto event = MakeHolder<TEvPrivate::TEvHnswSnapshotFinished>();
                event->LocalTid = localTid;
                event->IndexId = indexId;
                event->BuildToken = token;
                event->MemoryOwner = memoryOwner;
                event->Result = std::move(result);
                TActivationContext::Send(new IEventHandle(replyTo, TActorId(), event.Release()));
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
        auto& entry = *FindHnswIndexEntry(request.LocalTid, request.IndexId);
        auto* scan = new THnswSnapshotScan(*request.Table, entry.VectorColumnTag, entry.Settings,
                                           HnswCacheMemoryTracker, entry.MemoryOwner, bool(entry.Index), entry.CancelBuild, std::move(request.Finish));
        entry.ScanId = Executor()->QueueScan(request.LocalTid, scan, 0, NTabletFlatExecutor::TScanOptions().SetReadPrio(NTabletFlatExecutor::TScanOptions::EReadPrio::Low).SetReadAhead(0, 512_KB).SetSnapshotRowVersion(request.BaseVersion));
    }

    class TDataShard::TTxRebuildHnswIndex: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    public:
        TTxRebuildHnswIndex(TDataShard* self, ui32 tid, bool initialBuild, TPathId indexId, std::optional<TRowVersion> waitingBase)
            : TBase(self), LocalTid(tid), IndexId(self->ResolveHnswIndexId(tid, indexId)), InitialBuild(initialBuild), WaitingBase(waitingBase) {}

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
            auto* cached = Self->FindHnswIndexEntry(LocalTid, IndexId);
            if (!cached) { return true; }
            auto& entry = *cached;
            if (entry.Building || (InitialBuild && !entry.InitialBuildAttemptsLeft)
                    || entry.NextScanAttemptAt == TInstant::Max()) { return true; }
            if (InitialBuild ? bool(entry.Index) : (!entry.Index || !entry.Changes->Valid
                    || !entry.Index->NeedsRebuild(GetHnswDeltaRows(entry.Settings)))) { return true; }
            if (!entry.Changes->Valid) {
                Self->ResetHnswIndexChanges(entry);
                entry.OpenTransactionsTracked = false;
            }
            if (!entry.VectorColumnTag) {
                entry.VectorColumnTag = metadata->VectorColumnTag;
                entry.Settings = metadata->Settings;
            }
            if (AppData()->TimeProvider->Now() < entry.NextScanAttemptAt) { Retry = true; return true; }
            Self->GetHnswCacheMemoryLimit();
            Self->HnswCacheMemoryTracker->Request(entry.RetryMemoryBytes, entry.MemoryOwner);
            if (!Self->GetHnswCacheMemoryLimit()) {
                // Keep a bounded demand while waiting for the first grant.
                if (entry.InitialBuildAttemptsLeft) { --entry.InitialBuildAttemptsLeft; }
                if (!entry.InitialBuildAttemptsLeft && !entry.Index) {
                    Self->DisableHnswIndexBuild(LocalTid, IndexId);
                } else { Retry = true; }
                return true;
            }
            if (entry.EvictedMemoryBytes) {
                const ui64 used = Self->HnswCacheMemoryTracker->GetUsed();
                const ui64 limit = Self->GetHnswCacheMemoryLimit();
                if (used > limit || entry.EvictedMemoryBytes > limit - used) {
                    Self->HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner);
                    if (entry.InitialBuildAttemptsLeft) { --entry.InitialBuildAttemptsLeft; }
                    if (!entry.InitialBuildAttemptsLeft) {
                        Self->DisableHnswIndexBuild(LocalTid, IndexId);
                    } else { Retry = true; }
                    return true;
                }
            }
            Self->TrackHnswOpenTransactions(LocalTid, txc.DB, IndexId);
            if (!entry.Changes->Valid || !entry.UntrackedTransactions.empty()) {
                Self->HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner);
                Retry = true;
                return true;
            }
            const auto base = WaitingBase.value_or(Self->GetHnswBuildVersion());
            if ((entry.Index && base <= entry.Index->GetBaseVersion()) || base < Self->SnapshotManager.GetLowWatermark()
                    || base >= Self->Pipeline.GetUnreadableEdge()) {
                Self->HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner);
                Retry = true;
                return true;
            }
            if (Self->VolatileTxManager.HasVolatileTxsAtSnapshot(base)) {
                Self->HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner);
                WaitingBase = base;
                WaitVolatile = true;
                return true;
            }
            const auto& settings = entry.Settings;
            if (Self->TryStartHnswIndexBuild(LocalTid, metadata->VectorColumnTag, settings, base, IndexId)) {
                if (InitialBuild) { --entry.InitialBuildAttemptsLeft; }
                Self->TrackHnswOpenTransactions(LocalTid, txc.DB, IndexId);
                if (Self->IsHnswBuildCurrent(LocalTid, Self->GetHnswBuildToken(LocalTid, IndexId), IndexId)) {
                    Self->StartHnswSnapshotScan(LocalTid, table, base, txc, {}, IndexId);
                } else {
                    Self->HnswBuildsInFlight.erase(entry.BuildToken);
                    Self->DeferHnswIndexBuild(LocalTid, TDuration::Seconds(1), IndexId);
                    Retry = true;
                }
            }
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (WaitVolatile) {
                auto event = std::make_unique<IEventHandle>(Self->SelfId(), Self->SelfId(),
                    new TEvPrivate::TEvRebuildHnswIndex(LocalTid, InitialBuild, IndexId, WaitingBase));
                if (auto* entry = Self->FindHnswIndexEntry(LocalTid, IndexId)) { entry->RebuildScheduled = true; }
                Self->VolatileTxManager.AttachWaitingSnapshotEvent(*WaitingBase, std::move(event));
                return;
            }
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
        bool WaitVolatile = false;
        std::optional<TRowVersion> WaitingBase;
    };

    void TDataShard::Handle(TEvPrivate::TEvRebuildHnswIndex::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxRebuildHnswIndex(this, ev->Get()->LocalTid, ev->Get()->InitialBuild, ev->Get()->IndexId, ev->Get()->WaitingBase), ctx);
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

    void TDataShard::RequestHnswIndexBuild(ui32 localTid, ui32 vectorColumnTag,
        const Ydb::Table::VectorIndexSettings& settings, TPathId indexId) {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || IsFollower() || IsStopping() || State != TShardState::Ready) { return; }
        const auto* metadata = FindHnswIndexInfo(localTid, indexId);
        if (!metadata || metadata->VectorColumnTag != vectorColumnTag
                || !AreHnswIndexSettingsCompatible(metadata->Settings, settings)) { return; }
        auto* entry = GetOrCreateHnswIndexEntry(localTid, indexId);
        if (!entry || entry->Building || entry->RebuildScheduled || entry->Index) { return; }
        const bool changedThreshold = entry->VectorColumnTag && GetHnswMinRows(entry->Settings) != GetHnswMinRows(settings);
        if (entry->NextScanAttemptAt == TInstant::Max() && !changedThreshold) { return; }
        if (changedThreshold) { entry->NextScanAttemptAt = TInstant::Zero(); }
        entry->VectorColumnTag = vectorColumnTag;
        entry->Settings = settings;
        if (!entry->InitialBuildAttemptsLeft) { entry->InitialBuildAttemptsLeft = 30; }
        ScheduleHnswRebuild(localTid, indexId);
    }

    bool TDataShard::TryStartHnswIndexBuild(ui32 localTid, ui32 vectorColumnTag,
                                            const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        if (IsFollower() || IsStopping() || State != TShardState::Ready || !AppData()->FeatureFlags.GetEnableHnswIndex()) {
            return false;
        }
        const auto* metadata = FindHnswIndexInfo(localTid, indexId);
        if (!metadata || metadata->VectorColumnTag != vectorColumnTag
                || !AreHnswIndexSettingsCompatible(metadata->Settings, settings)) { return false; }
        auto* cached = GetOrCreateHnswIndexEntry(localTid, indexId);
        if (!cached) { return false; }
        auto& entry = *cached;
        if (entry.Building) {
            return false;
        }
        if (entry.EvictedMemoryBytes) {
            const ui64 used = HnswCacheMemoryTracker->GetUsed();
            const ui64 limit = GetHnswCacheMemoryLimit();
            if (used > limit || entry.EvictedMemoryBytes > limit - used) { return false; }
        }
        const bool compatible = entry.VectorColumnTag == vectorColumnTag && AreHnswIndexSettingsCompatible(entry.Settings, settings);
        if (!compatible || !entry.Changes->Valid) {
            InvalidateHnswIndex(localTid, indexId);
            ResetHnswIndexChanges(entry);
            entry.OpenTransactionsTracked = false;
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
        while (!HnswBuildQueue.empty()) {
            const auto* queued = FindHnswIndexEntry(HnswBuildQueue.front().first, HnswBuildQueue.front().second);
            if (queued && queued->NextScanAttemptAt != TInstant::Max()) { break; }
            HnswBuildQueue.erase(HnswBuildQueue.begin());
        }
        const auto identity = std::make_pair(localTid, indexId);
        if (Find(HnswBuildQueue, identity) == HnswBuildQueue.end()) {
            HnswBuildQueue.push_back(identity);
        }
        if (HnswBuildsInFlight.size() >= MaxConcurrentHnswBuilds || HnswBuildQueue.front() != identity) {
            if (!entry.Index && !entry.InitialBuildAttemptsLeft) { entry.InitialBuildAttemptsLeft = 30; }
            if (!entry.RebuildScheduled) {
                entry.RebuildScheduled = true;
                Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvRebuildHnswIndex(localTid, !entry.Index, indexId));
            }
            return false;
        }
        HnswBuildQueue.erase(HnswBuildQueue.begin());
        entry.Rebuilds += bool(entry.Index);
        entry.Building = true;
        entry.BuildVersion = baseVersion;
        entry.BuildReadPin = entry.Changes->PinReadVersion(baseVersion);
        entry.IndexSchemaVersion = FindHnswIndexInfo(localTid, indexId)->SchemaVersion;
        entry.BuildToken = ++NextHnswBuildToken;
        entry.CancelBuild = std::make_shared<std::atomic<bool>>(false);
        entry.EvictedMemoryBytes = 0;
        HnswRetiringMemoryOwners.erase(entry.MemoryOwner);
        HnswBuildsInFlight.insert(entry.BuildToken);
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
            && it->BuildToken == token && it->Changes->Valid;
    }

    void TDataShard::InvalidateHnswIndex(ui32 localTid, TPathId indexId) {
        indexId = ResolveHnswIndexId(localTid, indexId);
        EraseIf(HnswBuildQueue, [&](const auto& queued) { return queued.first == localTid && queued.second == indexId; });
        if (auto* entry = FindHnswIndexEntry(localTid, indexId)) { InvalidateHnswIndexEntry(*entry); }
    }

    void TDataShard::InvalidateHnswTable(ui32 localTid) {
        EraseIf(HnswBuildQueue, [&](const auto& queued) { return queued.first == localTid; });
        if (auto table = HnswIndexCache.find(localTid); table != HnswIndexCache.end()) {
            for (auto& [_, entry] : table->second) { InvalidateHnswIndexEntry(entry); }
        }
    }

    void TDataShard::ReconcileHnswIndexes(const TUserTable& before, const TUserTable& after) {
        if (before.LocalTid != after.LocalTid || before.ShadowTid != after.ShadowTid || before.KeyColumnIds != after.KeyColumnIds
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
                    && oldColumn->TypeMod == newColumn->TypeMod
                    && oldColumn->NotNull == newColumn->NotNull;
            }
            if (unchanged) {
                ++it;
            } else {
                InvalidateHnswIndex(before.LocalTid, it->first);
                table->second.erase(it++);
            }
        }
    }

    void TDataShard::InvalidateHnswIndexEntry(THnswIndexCacheEntry& entry) {
        HnswRetiringMemoryOwners.insert(entry.MemoryOwner);
        if (entry.CancelBuild) { entry.CancelBuild->store(true, std::memory_order_relaxed); }
        if (const ui64 scanId = std::exchange(entry.ScanId, 0)) {
            if (auto* executor = Executor()) {
                executor->CancelScan(entry.LocalTid, scanId);
            }
        }
        if (entry.BuildActor) { Send(entry.BuildActor, new TEvents::TEvPoison()); entry.BuildActor = {}; }
        entry.Building = false;
        entry.BuildReadPin.reset();
        entry.SnapshotReadPins.clear();
        entry.OpenTransactionsTracked = false;
        if (HnswCacheMemoryTracker) { HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner); }
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
        entry.EvictedMemoryBytes = 0;
        entry.NextScanAttemptAt = TInstant::Zero();
    }

    void TDataShard::InvalidateHnswIndexes() {
        if (HnswCacheMemoryTracker) { HnswCacheMemoryTracker->ResetAllDemand(); }
        HnswBuildQueue.clear();
        ForEachHnswIndexEntry([&](ui32, const TPathId&, auto& entry) { InvalidateHnswIndexEntry(entry); });
    }

    void TDataShard::EvictHnswIndexesToLimit(ui64 limit) {
        const auto usage = HnswCacheMemoryTracker->GetUsageByOwner();
        for (auto it = HnswRetiringMemoryOwners.begin(); it != HnswRetiringMemoryOwners.end();) {
            const auto* bytes = usage.FindPtr(*it);
            if (!bytes || !*bytes) {
                HnswRetiringMemoryOwners.erase(it++);
            } else {
                ++it;
            }
        }
        ui64 projectedUsed = 0;
        for (const auto& [owner, bytes] : usage) {
            if (!HnswRetiringMemoryOwners.contains(owner)) { projectedUsed += bytes; }
        }
        ForEachHnswIndexEntry([&](ui32 localTid, const TPathId& indexId, auto& entry) {
            if (projectedUsed <= limit || HnswRetiringMemoryOwners.contains(entry.MemoryOwner)) { return; }
            const auto* bytes = usage.FindPtr(entry.MemoryOwner);
            if (!bytes || !*bytes) { return; }
            // Count the entire retired reservation now, while keeping the real
            // memory charged until every reader and background worker releases it.
            projectedUsed -= *bytes;
            InvalidateHnswIndex(localTid, indexId);
            entry.EvictedMemoryBytes = *bytes;
            entry.InitialBuildAttemptsLeft = 30;
            entry.NextScanAttemptAt = AppData()->TimeProvider->Now() + TDuration::Seconds(1);
        });
    }

    void TDataShard::SetHnswIndex(ui32 localTid, std::shared_ptr<THnswIndex> index,
                                  std::shared_ptr<void> memoryReservation, ui64 rowCountAtBuild, ui32 vectorColumnTag,
                                  const Ydb::Table::VectorIndexSettings& settings, TRowVersion baseVersion, ui64 buildToken, TPathId indexId) {
        HnswBuildsInFlight.erase(buildToken);
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
        if (!index || HnswCacheMemoryTracker->GetUsed() > GetHnswCacheMemoryLimit()) {
            DeferHnswIndexBuild(localTid, TDuration::Seconds(5), indexId);
            ScheduleHnswRebuild(localTid, indexId);
            return;
        }
        if (!entry.Changes->Valid) {
            ResetHnswIndexChanges(entry);
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
        if (entry.Index && entry.Index->GetBaseVersion() != baseVersion) {
            entry.Retained.push_back(std::move(entry.Index));
        }
        entry.Index = std::move(index);
        Sort(entry.Retained, [](const auto& a, const auto& b) {
            return a->GetBaseVersion() < b->GetBaseVersion();
        });
        ++entry.Builds;
        entry.InitialBuildAttemptsLeft = 0;
        entry.RowCountAtBuild = rowCountAtBuild;
        entry.VectorColumnTag = vectorColumnTag;
        entry.Settings = settings;
        entry.Building = false;
        entry.BuildReadPin.reset();
        entry.NextScanAttemptAt = TInstant::Zero();
        HnswCacheMemoryTracker->ResetDemand(entry.MemoryOwner);
        PruneHnswIndexes();
        ScheduleHnswRebuild(localTid, indexId);
    }

    void TDataShard::RefreshHnswSnapshotPins(THnswIndexCacheEntry& entry) {
        TMap<TRowVersion, std::shared_ptr<void>> pins;
        for (const auto& [pathId, table] : TableInfos) {
            if (table->LocalTid != entry.LocalTid) { continue; }
            for (const auto& [key, _] : SnapshotManager.GetSnapshots({GetPathOwnerId(), pathId})) {
                const TRowVersion version(key.Step, key.TxId);
                pins.emplace(version, entry.Changes->PinReadVersion(version));
            }
            break;
        }
        // Acquire replacement leases before releasing the previous set: an old
        // snapshot below the floor must keep its coverage throughout the refresh.
        entry.SnapshotReadPins.swap(pins);
    }

    void TDataShard::ResetHnswIndexChanges(THnswIndexCacheEntry& entry) {
        entry.BuildReadPin.reset();
        entry.SnapshotReadPins.clear();
        entry.Changes = std::make_shared<THnswIndexChanges>();
        RefreshHnswSnapshotPins(entry);
    }

    void TDataShard::UpdateHnswSnapshotPin(const TSnapshotKey& snapshot, bool add) {
        if (HnswIndexCache.empty() || snapshot.OwnerId != GetPathOwnerId()) { return; }
        if (add && !AppData()->FeatureFlags.GetEnableHnswIndex()) { return; }
        const auto table = TableInfos.find(snapshot.PathId);
        if (table == TableInfos.end()) { return; }
        const auto cached = HnswIndexCache.find(table->second->LocalTid);
        if (cached == HnswIndexCache.end()) { return; }
        const TRowVersion version(snapshot.Step, snapshot.TxId);
        for (auto& [_, entry] : cached->second) {
            if (add && entry.Changes->Valid) {
                entry.SnapshotReadPins.emplace(version, entry.Changes->PinReadVersion(version));
            } else {
                entry.SnapshotReadPins.erase(version);
            }
        }
    }

    std::vector<TRowVersion> TDataShard::GetHnswReadVersions(ui32 localTid) const {
        std::vector<TRowVersion> pins;
        for (const auto& [_, reader] : ReadIterators) {
            const auto table = TableInfos.find(reader.PathId.LocalPathId);
            if (table != TableInfos.end() && table->second->LocalTid == localTid && !reader.ReadVersion.IsMax()) {
                pins.push_back(reader.ReadVersion);
            }
        }
        for (const auto& [tableId, table] : TableInfos) {
            if (table->LocalTid != localTid) { continue; }
            for (const auto& [snapshot, _] : SnapshotManager.GetSnapshots({GetPathOwnerId(), tableId})) {
                pins.emplace_back(snapshot.Step, snapshot.TxId);
            }
        }
        return pins;
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
        const auto floor = GetHnswBuildVersion();
        THashMap<ui32, std::vector<TRowVersion>> readVersions;
        ForEachHnswIndexEntry([&](ui32 tid, const TPathId&, auto& entry) {
            if (entry.Changes->Valid) { RefreshHnswSnapshotPins(entry); }
            auto [versions, inserted] = readVersions.try_emplace(tid);
            if (inserted) { versions->second = GetHnswReadVersions(tid); }
            auto pins = versions->second;
            if (entry.Building) { pins.push_back(entry.BuildVersion); }
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
                entry.Changes->Compact(floor, pins);
            } else {
                entry.Changes->Clear();
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
        if (entry.Index || entry.Building) {
            // Readers, stored snapshots and builds pin their versions directly.
            // Advancing the floor is constant time; Set compacts just this key.
            entry.Changes->AdvanceRetentionFloor(GetHnswBuildVersion());
            entry.Changes->Set(std::move(key), change.Version, std::move(change.Vector), std::move(change.MemoryReservation));
        }
        if (entry.NextScanAttemptAt == TInstant::Max()) {
            entry.NextScanAttemptAt = TInstant::Zero();
            if (!entry.Index) { entry.InitialBuildAttemptsLeft = 30; }
        }
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
            reservation = TryReserveHnswCacheMemory(THnswIndexChanges::EstimateBytes(key, vector ? vector->size() : 0), it->MemoryOwner);
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
                    auto reservation = TryReserveHnswCacheMemory(THnswIndexCacheEntry::PendingTransactionBytes, it->MemoryOwner);
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
            if (entry.UntrackedTransactions.erase(txId)) {
                // No scan starts before these pre-journal intents have resolved.
                entry.Pending.erase(txId);
                entry.PendingReservations.erase(txId);
                ScheduleHnswRebuild(localTid, indexId);
                return;
            }
            if (pending == entry.Pending.end()) {
                return; // A lock-only transaction does not change any vector.
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

    std::optional<TDataShard::THnswIndexStats> TDataShard::GetHnswIndexStats(ui32 localTid, TPathId indexId) const {
        const auto* entry = FindHnswIndexEntry(localTid, indexId);
        if (!entry) { return {}; }
        return THnswIndexStats{bool(entry->Index), entry->Building,
            HnswCacheMemoryTracker ? HnswCacheMemoryTracker->GetUsed(entry->MemoryOwner) : 0,
            entry->CacheHits, entry->CacheMisses, entry->Rebuilds, entry->Builds,
            entry->UnsupportedReads, entry->RangeFallbacks, entry->CandidateFallbacks};
    }

    void TDataShard::ScheduleHnswInitialBuilds() {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex() || IsFollower() || State != TShardState::Ready) { return; }
        for (const auto& [_, table] : TableInfos) {
            for (const auto& [indexId, metadata] : table->HnswIndexes) {
                // Reconciliation erases changed definitions. Existing entries
                // retain their thresholds, disabled state and retry budgets.
                if (!metadata.VectorColumnTag || FindHnswIndexEntry(table->LocalTid, indexId)) { continue; }
                auto* entry = GetOrCreateHnswIndexEntry(table->LocalTid, indexId);
                if (!entry) { continue; }
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
