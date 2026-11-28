// The query cache on TKqpCache (ydb/core/kqp/common/compile_cache), selected by EnableCompileCacheV2.
// TKqpQueryCacheV1 in kqp_compile_service.cpp is the fallback until the rollout is over.

#include "kqp_compile_service.h"

#include <ydb/core/kqp/common/compile_cache/kqp_cache.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPILE_SERVICE

namespace NKikimr::NKqp {

namespace {

// TODO(anely-d): make the per-node snapshot budget configurable.
constexpr ui64 SnapshotBudget = 64_MB;

// Mutable only under the cache lock.
struct TQueryCacheEntryV2 {
    TQueryCacheRecord Record;
    // Successful client plan deliveries over the lifetime of this UID.
    ui64 AccessCount = 0;
    TString ReplayMessage;
    TInstant LastReplayTime = TInstant::Zero();
    ui64 BodyBytes = 0;

    ui64 Size() const { return BodyBytes + ReplayMessage.size(); }
    ui64 SnapshotBytes() const { return Record.GetSnapshot() ? BodyBytes : 0; }
};

class TKqpQueryCacheV2 final
    : public TKqpQueryCache
    , private TKqpCache<TKqpQueryCacheV2, TString, TQueryCacheEntryV2>
{
    using TBase = TKqpCache<TKqpQueryCacheV2, TString, TQueryCacheEntryV2>;
    using TEvicted = TBase::TEvicted;
    using TDropped = TBase::TDropped;
    friend TBase;

public:
    // Zero `maxEntries` disables the cache: nothing is stored. V1 keeps one entry instead.
    TKqpQueryCacheV2(size_t maxEntries, TDuration ttl)
        : TBase(maxEntries, ttl)
        , Enabled(maxEntries != 0)
    {}

    bool SupportsSnapshots() const override { return Enabled; }
    bool CountsAccesses() const override { return true; }

    bool Insert(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot, ui64 accesses) override {
        Y_ENSURE(compileResult->Query);
        YQL_ENSURE(compileResult->PreparedQuery);
        if (!Enabled) {
            return false;
        }

        const TInstant now = Now();
        const TString& uid = compileResult->Uid;
        const TKqpQueryId& query = *compileResult->Query;
        auto entry = MakeEntry(compileResult, snapshot, accesses);

        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);

        EraseOtherUidLocked(query, uid, dropped);
        if (snapshot) {
            const auto* previous = FindEntryLocked(uid, false, now, dropped);
            ApplySnapshotBudget(entry, compileResult, previous);
        }
        return InsertEntryLocked(uid, std::move(entry), query, now, dropped);
    }

    void CountSnapshotAccesses(const TQueryCacheSnapshotRecord& source, ui64 accesses) override {
        if (accesses) {
            const TInstant now = Now();
            TDropped dropped;
            TGuard<TAdaptiveLock> guard(Lock);
            if (auto* entry = FindEntryLocked(source.Info->Uid, false, now, dropped)) {
                const auto* current = entry->Record.GetSnapshot();
                if (current && current->Info == source.Info) {
                    entry->AccessCount += accesses;
                }
            }
        }
    }

    void MarkWarmupInsert(const TString& uid) override {
        if (AtomicGet(WarmupWindowClosedAtomic)) {
            return;
        }
        TGuard<TAdaptiveLock> guard(Lock);
        MarkWarmupInsertLocked(uid);
    }

    void AttachReplayMessage(const TString uid, TString replayMessage) override {
        const TInstant now = Now();
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);

        if (auto* entry = FindEntryLocked(uid, false, now, dropped)) {
            CacheBytes = CacheBytes - entry->ReplayMessage.size() + replayMessage.size();
            entry->ReplayMessage = std::move(replayMessage);
            entry->LastReplayTime = now;
        }
    }

    TString ReplayMessageByUid(const TString uid, TDuration timeout) override {
        const TInstant now = Now();
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);

        if (auto* entry = FindEntryLocked(uid, false, now, dropped)) {
            if (entry->LastReplayTime + timeout < now) {
                entry->LastReplayTime = now;
                return entry->ReplayMessage;
            }
        }
        return "";
    }

    TQueryCacheRecord LookupByUid(const TString& uid, bool promote) override {
        const TInstant now = NowFor(promote);
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByUidImpl(uid, promote, now, dropped);
        return entry ? entry->Record : TQueryCacheRecord{};
    }

    TQueryCacheRecord LookupByQuery(const TKqpQueryId& query, bool promote) override {
        const TInstant now = NowFor(promote);
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByQueryImpl(query, promote, now, dropped);
        return entry ? entry->Record : TQueryCacheRecord{};
    }

    bool Replace(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot, ui64 accesses) override {
        Y_ENSURE(compileResult->Query);
        YQL_ENSURE(compileResult->PreparedQuery);

        const TInstant now = Now();
        auto fresh = MakeEntry(compileResult, snapshot, accesses);

        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);

        const bool promote = accesses || !snapshot;
        auto* entry = FindEntryLocked(compileResult->Uid, promote, now, dropped);
        if (!entry) {
            return false;
        }

        const bool sameQuery = entry->Record.GetQuery() == *compileResult->Query;
        if (!sameQuery && entry->Record.GetQuery().DisablePgCompatibility() == compileResult->Query->DisablePgCompatibility()) {
            EraseOtherUidLocked(*compileResult->Query, compileResult->Uid, dropped);
        }
        ApplySnapshotBudget(fresh, compileResult, entry);

        if (sameQuery) {
            entry->AccessCount += accesses;
            const auto* previous = entry->Record.GetSnapshot();
            if (previous && fresh.Record.GetSnapshot() && previous->Snapshot == snapshot) {
                return true; // The same snapshot keeps its original compilation metadata.
            }
            if (!promote) {
                FindEntryLocked(compileResult->Uid, true, now, dropped);
            }
            SnapshotBytes -= entry->SnapshotBytes();
            SnapshotBytes += fresh.SnapshotBytes();
            CacheBytes = CacheBytes - entry->BodyBytes + fresh.BodyBytes;
            entry->BodyBytes = fresh.BodyBytes;
            std::swap(entry->Record, fresh.Record);
            return true;
        }

        // The text changed under the same uid: re-insert so the old mapping is undone through the
        // regular hooks.
        fresh.ReplayMessage = entry->ReplayMessage;
        fresh.LastReplayTime = entry->LastReplayTime;
        fresh.AccessCount += entry->AccessCount;
        InsertEntryLocked(compileResult->Uid, std::move(fresh), *compileResult->Query, now, dropped);
        return true;
    }

    TQueryCacheRecord Lookup(
        const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>& query,
        bool disablePgCompatibility,
        TKqpTempTablesState::TConstPtr tempTablesState,
        bool promote,
        const NACLib::TSID& userSid,
        TIntrusivePtr<TKqpCounters> counters,
        TIntrusivePtr<TKqpDbCounters>& dbCounters,
        const TActorId&,
        const TActorContext&,
        EWarmupAttributionMode warmupAttribution, bool plansOnly) override
    {
        if (!uid) {
            Y_ENSURE(query);
            query->SetDisablePgCompatibility(disablePgCompatibility);
            if (query->UserSid().empty()) {
                query->SetUserSid(userSid);
            } else {
                Y_ENSURE(query->UserSid() == userSid);
            }
        }

        const TInstant now = NowFor(promote);

        TQueryCacheRecord record;
        size_t size;
        ui64 bytes;
        const bool hasTempTables = tempTablesState && !tempTablesState->TempTables.empty();
        // Warmup is not a client use. V1 counts it in the session, so a query no client runs
        // any more can pass from node to node through warmup.
        const bool countsAccess = warmupAttribution != EWarmupAttributionMode::Warmup;
        {
            TDropped dropped;
            TGuard<TAdaptiveLock> guard(Lock);
            auto* entry = uid ? FindByUidImpl(*uid, promote, now, dropped, disablePgCompatibility)
                              : FindByQueryImpl(*query, promote, now, dropped);
            if (entry) {
                record = entry->Record;
                if (countsAccess && !hasTempTables && !record.GetSnapshot() && (!uid || record.GetQuery().UserSid() == userSid)) {
                    ++entry->AccessCount;
                }
            }
            size = SizeLocked();
            bytes = CacheBytes;
        }
        *counters->CompileQueryCacheSize = size;
        *counters->CompileQueryCacheBytes = bytes;
        if (uid) {
            counters->ReportCompileRequestGet(dbCounters);
        }
        if (!record) {
            AccountWarmupLookup(warmupAttribution, nullptr, counters);
            return {};
        }
        if (uid && record.GetQuery().UserSid() != userSid) {
            return {};
        }
        if (record.GetSnapshot()) {
            // The service needs the original UID even when it compiles from scratch.
            return plansOnly ? TQueryCacheRecord{} : record;
        }

        auto plan = record.GetPlan();
        if (HasTempTablesNameClashes(plan, tempTablesState)) {
            return {};
        }
        if (countsAccess && hasTempTables) {
            // The potentially expensive name check above must run outside the cache lock.
            TDropped dropped;
            TGuard<TAdaptiveLock> guard(Lock);
            auto* entry = FindEntryLocked(plan->Uid, false, now, dropped);
            if (entry && entry->Record.GetPlan() == plan) {
                ++entry->AccessCount;
            }
        }
        counters->ReportQueryCacheHit(dbCounters, true);
        AccountWarmupLookup(warmupAttribution, plan, counters);
        return record;
    }

    bool EraseIfCurrent(const TKqpCompileResult::TConstPtr& result) override {
        const TInstant now = NowFor(false);
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByUidImpl(result->Uid, false, now, dropped);
        return entry && entry->Record.GetPlan() == result && EraseLocked(result->Uid, dropped);
    }

    bool EraseByUid(const TString& uid) override {
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseLocked(uid, dropped);
    }

    size_t Size() const override {
        return TBase::Size();
    }

    ui64 Bytes() const override {
        TGuard<TAdaptiveLock> guard(Lock);
        return CacheBytes;
    }

    TVector<TKqpQueryCacheSnapshot::TEntry> GetSnapshot() const override {
        TGuard<TAdaptiveLock> guard(Lock);
        TVector<TKqpQueryCacheSnapshot::TEntry> out;
        out.reserve(SizeLocked());
        ForEachLocked([&](const TString&, const TQueryCacheEntryV2& entry, TInstant touchedAt) {
            if (auto* snapshot = entry.Record.GetSnapshot()) {
                out.push_back({snapshot->Info, touchedAt, entry.AccessCount});
            } else {
                out.push_back({entry.Record.GetPlan(), touchedAt, entry.AccessCount});
            }
        });
        return out;
    }

    size_t EraseExpiredQueries() override {
        return TBase::EraseExpired();
    }

    void Clear() override {
        TBase::Clear();
    }

private:
    static TInstant Now() {
        return TAppData::TimeProvider->Now();
    }

    void OnDropped(const TEvicted& evicted) {
        const auto& entry = evicted.Value;
        CacheBytes -= entry.Size();
        SnapshotBytes -= entry.SnapshotBytes();

        // Only if the text still maps to this uid: a late drop of a replaced entry must not take
        // the live mapping with it.
        auto it = QueryIndex.find(entry.Record.GetQuery());
        if (it != QueryIndex.end() && it->second == evicted.Key) {
            QueryIndex.erase(it);
        }
    }

    void OnCleared() {
        CacheBytes = 0;
        SnapshotBytes = 0;
        QueryIndex.clear();
        ResetWarmupLocked();
    }

    // Builds an entry without the lock: the plan size is the expensive part.
    static TQueryCacheEntryV2 MakeEntry(const TKqpCompileResult::TConstPtr& compileResult,
        const TPreCboSnapshotPtr& snapshot, ui64 accesses) {
        if (snapshot) {
            auto info = std::make_shared<const TQuerySnapshotInfo>(compileResult->Uid, *compileResult->Query,
                compileResult->CompiledAt, compileResult->CompilationDuration, compileResult->Issues, compileResult->CompileMeta);
            return TQueryCacheEntryV2{
                .Record = TQueryCacheSnapshotRecord{std::move(info), snapshot},
                .AccessCount = accesses,
                .BodyBytes = snapshot->ByteSize(),
            };
        }
        return TQueryCacheEntryV2{
            .Record = compileResult,
            .AccessCount = accesses,
            .BodyBytes = compileResult->PreparedQuery->ByteSize(),
        };
    }

    void ApplySnapshotBudget(TQueryCacheEntryV2& candidate, const TKqpCompileResult::TConstPtr& plan,
        const TQueryCacheEntryV2* previous) const
    {
        if (candidate.SnapshotBytes() > SnapshotBudget - SnapshotBytes + (previous ? previous->SnapshotBytes() : 0)) {
            candidate.Record = plan;
            candidate.BodyBytes = plan->PreparedQuery->ByteSize();
        }
    }

    void EraseOtherUidLocked(const TKqpQueryId& query, const TString& uid, TDropped& dropped) {
        if (const TString* oldUid = QueryIndex.FindPtr(query); oldUid && *oldUid != uid) {
            // OnDropped erases the mapping that owns oldUid.
            EraseLocked(TString(*oldUid), dropped);
        }
    }

    // Returns whether inserting this entry evicted another UID to stay within the LRU limit.
    bool InsertEntryLocked(const TString& uid, TQueryCacheEntryV2&& entry, const TKqpQueryId& query,
        TInstant now, TDropped& dropped)
    {
        const ui64 bytes = entry.Size();
        const ui64 snapshotBytes = entry.SnapshotBytes();
        const auto evicted = InsertLocked(uid, std::move(entry), now, dropped);
        CacheBytes += bytes;
        SnapshotBytes += snapshotBytes;
        // PG revalidation may leave two prepared UIDs for the same text.
        QueryIndex.try_emplace(query, uid);
        return evicted != 0;
    }

    TQueryCacheEntryV2* FindByUidImpl(const TString& uid, bool promote, TInstant now, TDropped& dropped, std::optional<bool> mode = {}) {
        // A snapshot lookup only starts a compilation. Refresh its TTL on successful delivery.
        auto* entry = FindEntryLocked(uid, [promote, mode](const TQueryCacheEntryV2& entry) {
            return promote && !entry.Record.GetSnapshot() && (!mode || entry.Record.GetQuery().DisablePgCompatibility() == *mode);
        }, now, dropped);
        return entry && (!mode || entry->Record.GetQuery().DisablePgCompatibility() == *mode) ? entry : nullptr;
    }

    TQueryCacheEntryV2* FindByQueryImpl(const TKqpQueryId& query, bool promote, TInstant now, TDropped& dropped) {
        const TString* uid = QueryIndex.FindPtr(query);
        // The copy is required: a lazy expiry of the entry forgets this very mapping.
        return uid ? FindByUidImpl(TString(*uid), promote, now, dropped) : nullptr;
    }

    void AccountWarmupLookup(EWarmupAttributionMode mode, const TKqpCompileResult::TConstPtr& plan,
        const TIntrusivePtr<TKqpCounters>& counters)
    {
        if (mode == EWarmupAttributionMode::Warmup && plan) {
            MarkWarmupInsert(plan->Uid);
        }
        if (mode != EWarmupAttributionMode::Client || AtomicGet(WarmupWindowClosedAtomic) || !AtomicGet(WarmupHasPendingAtomic)) {
            return;
        }
        const TInstant now = Now();
        {
            TGuard<TAdaptiveLock> guard(Lock);
            if (!RefreshWarmupWindowLocked(now) || (plan && !WarmupPendingHitUids.erase(plan->Uid))) {
                return;
            }
        }
        if (plan) {
            counters->WarmupHitsInWindow->Inc();
            counters->WarmupSavedCompileMs->Add(plan->CompilationDuration.MilliSeconds());
        } else {
            counters->WarmupMissesInWindow->Inc();
        }
    }

    void MarkWarmupInsertLocked(const TString& uid) {
        if (AtomicGet(WarmupWindowClosedAtomic)) {
            return;
        }
        WarmupPendingHitUids.insert(uid);
        AtomicSet(WarmupHasPendingAtomic, 1);
    }

    // Opens the window on the first access with something pending; closes it for good once it has
    // lasted WarmupObservationWindow. Returns whether the window is open.
    bool RefreshWarmupWindowLocked(TInstant now) {
        if (AtomicGet(WarmupWindowClosedAtomic)) {
            return false;
        }
        if (!WarmupWindowStart) {
            if (WarmupPendingHitUids.empty()) {
                return false;
            }
            WarmupWindowStart = now;
            return true;
        }
        if (now - *WarmupWindowStart >= WarmupObservationWindow) {
            AtomicSet(WarmupWindowClosedAtomic, 1);
            WarmupPendingHitUids.clear();
            return false;
        }
        return true;
    }

    void ResetWarmupLocked() {
        WarmupPendingHitUids.clear();
        WarmupWindowStart.reset();
        AtomicSet(WarmupWindowClosedAtomic, 0);
        AtomicSet(WarmupHasPendingAtomic, 0);
    }

    ui64 CacheBytes = 0;
    ui64 SnapshotBytes = 0;
    const bool Enabled;
    // Query text -> uid, one entry per text. Hashing and comparing the key is TKqpQueryId's job.
    THashMap<TKqpQueryId, TString> QueryIndex;

    // Warmup window: guarded by Lock, except the two atomics, which are written under Lock and
    // read without it.
    THashSet<TString> WarmupPendingHitUids;
    std::optional<TInstant> WarmupWindowStart;
    TAtomic WarmupWindowClosedAtomic = 0;
    TAtomic WarmupHasPendingAtomic = 0;
};

} // anonymous namespace

TKqpQueryCachePtr CreateKqpQueryCache(bool compileCacheV2, size_t size, TDuration ttl) {
    if (compileCacheV2) {
        return MakeIntrusive<TKqpQueryCacheV2>(size, ttl);
    }
    return MakeIntrusive<TKqpQueryCacheV1>(size, ttl);
}

} // namespace NKikimr::NKqp
