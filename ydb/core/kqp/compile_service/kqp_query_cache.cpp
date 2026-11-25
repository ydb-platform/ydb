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
    TInstant LastTouched = TInstant::Zero();

    ui64 AccountedBytes = 0;

    ui64 SnapshotBytes() const {
        return Record.GetSnapshot() ? AccountedBytes - ReplayMessage.size() : 0;
    }
};

// Compile-time policy of the query cache: the budget is the entry count (CompileQueryCacheSize);
// bytes are tracked for the cache size sensor only.
struct TQueryCachePolicyV2 {
    static constexpr bool LimitEntries = true;
    static constexpr bool LimitBytes = false;
    static constexpr bool LimitEntryBytes = false;
    static ui64 Size(const TQueryCacheEntryV2& entry) { return entry.AccountedBytes; }
};

// We don't use an RW lock because of cache promotions during lookup: in a benchmark both versions
// (RW spinlock and adaptive W spinlock) showed the same performance. Instead the critical section
// holds only the storage and text index operations; the clock, size accounting, temp-table and SID
// checks, counters and logging happen outside it.
class TKqpQueryCacheV2 final
    : public TKqpQueryCache
    , private TKqpCache<TKqpQueryCacheV2, TString, TQueryCacheEntryV2, TQueryCachePolicyV2>
{
    using TBase = TKqpCache<TKqpQueryCacheV2, TString, TQueryCacheEntryV2, TQueryCachePolicyV2>;
    using TEvicted = TBase::TEvicted;
    using TGraveyard = TBase::TGraveyard;
    friend TBase;

public:
    // Zero `maxEntries` disables the cache: nothing is stored. V1 keeps one entry instead.
    TKqpQueryCacheV2(size_t maxEntries, TDuration ttl)
        : TBase(TCacheLimits{.MaxEntries = maxEntries}, ttl)
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

        // Before the lock: the clock and the plan size.
        const TInstant now = Now();
        const TString& uid = compileResult->Uid;
        const TKqpQueryId& query = *compileResult->Query;
        auto entry = MakeEntry(compileResult, snapshot, accesses, now);
        auto plan = snapshot ? MakeEntry(compileResult, {}, accesses, now) : TQueryCacheEntryV2{};

        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        EraseOtherUidLocked(query, uid, graveyard);
        if (snapshot) {
            const auto* previous = FindEntryLocked(uid, false, now, graveyard);
            if (entry.SnapshotBytes() > SnapshotBudget - SnapshotBytes + (previous ? previous->SnapshotBytes() : 0)) {
                std::swap(entry, plan);
            }
        }
        return InsertEntryLocked(uid, std::move(entry), query, now, graveyard);
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
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        if (auto* entry = FindEntryLocked(uid, false, now, graveyard)) {
            entry->AccountedBytes = entry->AccountedBytes - entry->ReplayMessage.size() + replayMessage.size();
            entry->ReplayMessage = std::move(replayMessage);
            entry->LastReplayTime = now;
            entry->LastTouched = now;
            ReaccountLocked(uid, graveyard);
        }
    }

    TString ReplayMessageByUid(const TString uid, TDuration timeout) override {
        const TInstant now = Now();
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        if (auto* entry = FindEntryLocked(uid, false, now, graveyard)) {
            if (entry->LastReplayTime + timeout < now) {
                entry->LastReplayTime = now;
                entry->LastTouched = now;
                return entry->ReplayMessage;
            }
        }
        return "";
    }

    TQueryCacheRecord LookupByUid(const TString& uid, bool promote) override {
        const TInstant now = NowFor(promote);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByUidImpl(uid, promote, now, graveyard);
        return entry ? entry->Record : TQueryCacheRecord{};
    }

    TQueryCacheRecord LookupByQuery(const TKqpQueryId& query, bool promote) override {
        const TInstant now = NowFor(promote);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByQueryImpl(query, promote, now, graveyard);
        return entry ? entry->Record : TQueryCacheRecord{};
    }

    bool Replace(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot, ui64 accesses) override {
        Y_ENSURE(compileResult->Query);
        YQL_ENSURE(compileResult->PreparedQuery);

        // Before the lock: the clock and the plan size.
        const TInstant now = Now();
        auto fresh = MakeEntry(compileResult, snapshot, accesses, now);
        auto plan = snapshot ? MakeEntry(compileResult, {}, accesses, now) : TQueryCacheEntryV2{};

        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        auto* entry = FindEntryLocked(compileResult->Uid, true, now, graveyard);
        if (!entry) {
            return false;
        }

        const bool sameQuery = entry->Record.GetQuery() == *compileResult->Query;
        if (!sameQuery) {
            EraseOtherUidLocked(*compileResult->Query, compileResult->Uid, graveyard);
        }
        if (snapshot && fresh.SnapshotBytes() > SnapshotBudget - SnapshotBytes + entry->SnapshotBytes()) {
            std::swap(fresh, plan);
        }

        if (sameQuery) {
            entry->AccessCount += accesses;
            entry->LastTouched = now;
            const auto* previous = entry->Record.GetSnapshot();
            if (previous && fresh.Record.GetSnapshot() && previous->Snapshot == snapshot) {
                return true; // The same snapshot keeps its original compilation metadata.
            }
            SnapshotBytes -= entry->SnapshotBytes();
            SnapshotBytes += fresh.SnapshotBytes();
            entry->AccountedBytes = fresh.AccountedBytes + entry->ReplayMessage.size();
            std::swap(entry->Record, fresh.Record);
            ReaccountLocked(compileResult->Uid, graveyard);
            return true;
        }

        // The text changed under the same uid: re-insert so the old mapping is undone through the
        // regular hooks.
        fresh.ReplayMessage = entry->ReplayMessage;
        fresh.LastReplayTime = entry->LastReplayTime;
        fresh.AccessCount += entry->AccessCount;
        fresh.AccountedBytes += fresh.ReplayMessage.size();
        InsertEntryLocked(compileResult->Uid, std::move(fresh), *compileResult->Query, now, graveyard);
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

        // Before the lock: the clock.
        const TInstant now = NowFor(promote);

        TQueryCacheRecord record;
        size_t size;
        ui64 bytes;
        const bool hasTempTables = tempTablesState && !tempTablesState->TempTables.empty();
        // Warmup is not a client use. V1 counts it in the session, so a query no client runs
        // any more can pass from node to node through warmup.
        const bool countsAccess = warmupAttribution != EWarmupAttributionMode::Warmup;
        {
            TGraveyard graveyard;
            TGuard<TAdaptiveLock> guard(Lock);
            auto* entry = uid ? FindByUidImpl(*uid, promote, now, graveyard, disablePgCompatibility)
                              : FindByQueryImpl(*query, promote, now, graveyard);
            if (entry) {
                record = entry->Record;
                if (countsAccess && !hasTempTables && !record.GetSnapshot() && (!uid || record.GetQuery().UserSid() == userSid)) {
                    ++entry->AccessCount;
                }
            }
            size = SizeLocked();
            bytes = TotalBytesLocked();
        }
        *counters->CompileQueryCacheSize = size;
        *counters->CompileQueryCacheBytes = bytes;
        if (uid) {
            counters->ReportCompileRequestGet(dbCounters);
        }
        if (!record) {
            AccountWarmupMiss(warmupAttribution, counters);
            return {};
        }
        if (uid && record.GetQuery().UserSid() != userSid) {
            return {};
        }
        if (record.GetSnapshot()) {
            // A UID-only request may still need the query key for a cold compilation.
            return plansOnly || (hasTempTables && !uid) ? TQueryCacheRecord{} : record;
        }

        auto plan = record.GetPlan();
        if (HasTempTablesNameClashes(plan, tempTablesState)) {
            return {};
        }
        if (countsAccess && hasTempTables) {
            // The potentially expensive name check above must run outside the cache lock.
            TGraveyard graveyard;
            TGuard<TAdaptiveLock> guard(Lock);
            auto* entry = FindEntryLocked(plan->Uid, false, now, graveyard);
            if (entry && entry->Record.GetPlan() == plan) {
                ++entry->AccessCount;
            }
        }
        counters->ReportQueryCacheHit(dbCounters, true);
        AccountWarmupHit(plan, warmupAttribution, counters);
        return record;
    }

    bool EraseIfCurrent(const TKqpCompileResult::TConstPtr& result) override {
        const TInstant now = NowFor(false);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        auto* entry = FindByUidImpl(result->Uid, false, now, graveyard);
        return entry && entry->Record.GetPlan() == result && EraseLocked(result->Uid, graveyard);
    }

    bool EraseByUid(const TString& uid) override {
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseLocked(uid, graveyard);
    }

    size_t Size() const override {
        return TBase::Size();
    }

    ui64 Bytes() const override {
        return TBase::TotalBytes();
    }

    TVector<TKqpQueryCacheSnapshot::TEntry> GetSnapshot() const override {
        TGuard<TAdaptiveLock> guard(Lock);
        TVector<TKqpQueryCacheSnapshot::TEntry> out;
        out.reserve(SizeLocked());
        ForEachLocked([&](const TString&, const TQueryCacheEntryV2& entry) {
            if (auto* snapshot = entry.Record.GetSnapshot()) {
                out.push_back({snapshot->Info, entry.LastTouched, entry.AccessCount});
            } else {
                out.push_back({entry.Record.GetPlan(), entry.LastTouched, entry.AccessCount});
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

    // TKqpCache hooks, called under Lock: OnDropped for every entry that leaves the storage one at
    // a time, OnCleared once after Clear() emptied it.
    void OnDropped(const TEvicted& evicted) {
        const auto& entry = evicted.Value;
        SnapshotBytes -= entry.SnapshotBytes();

        // Only if the text still maps to this uid: a late drop of a replaced entry must not take
        // the live mapping with it.
        auto it = QueryIndex.find(entry.Record.GetQuery());
        if (it != QueryIndex.end() && it->second == evicted.Key) {
            QueryIndex.erase(it);
        }
    }

    void OnCleared() {
        SnapshotBytes = 0;
        QueryIndex.clear();
        ResetWarmupLocked();
    }

    // Builds an entry without the lock: the plan size is the expensive part.
    static TQueryCacheEntryV2 MakeEntry(const TKqpCompileResult::TConstPtr& compileResult,
        const TPreCboSnapshotPtr& snapshot, ui64 accesses, TInstant now) {
        if (snapshot) {
            auto info = std::make_shared<const TQuerySnapshotInfo>(compileResult->Uid, *compileResult->Query,
                compileResult->CompiledAt, compileResult->CompilationDuration, compileResult->Issues, compileResult->CompileMeta);
            return TQueryCacheEntryV2{
                .Record = TQueryCacheSnapshotRecord{std::move(info), snapshot},
                .AccessCount = accesses,
                .LastTouched = now,
                .AccountedBytes = snapshot->ByteSize(),
            };
        }
        return TQueryCacheEntryV2{
            .Record = compileResult,
            .AccessCount = accesses,
            .LastTouched = now,
            .AccountedBytes = compileResult->PreparedQuery->ByteSize(),
        };
    }

    void EraseOtherUidLocked(const TKqpQueryId& query, const TString& uid, TGraveyard& graveyard) {
        if (const TString* oldUid = QueryIndex.FindPtr(query); oldUid && *oldUid != uid) {
            // OnDropped erases the mapping that owns oldUid.
            EraseLocked(TString(*oldUid), graveyard);
        }
    }

    // Returns whether inserting this entry evicted another UID to stay within the LRU limit.
    bool InsertEntryLocked(const TString& uid, TQueryCacheEntryV2&& entry, const TKqpQueryId& query,
        TInstant now, TGraveyard& graveyard)
    {
        const ui64 snapshotBytes = entry.SnapshotBytes();
        const auto result = InsertLocked(uid, std::move(entry), now, graveyard);
        if (result.Admitted) {
            SnapshotBytes += snapshotBytes;
            // Mapped after the insert: dropping the replaced entry under this uid must not forget
            // the mapping of the new one.
            QueryIndex[query] = uid;
        }
        return AnyOf(graveyard.end() - result.Dropped, graveyard.end(),
            [&](const TEvicted& dropped) { return dropped.Key != uid; });
    }

    TQueryCacheEntryV2* FindByUidImpl(const TString& uid, bool promote, TInstant now, TGraveyard& graveyard, std::optional<bool> mode = {}) {
        // A snapshot lookup only starts a compilation. Refresh its TTL on successful delivery.
        auto* entry = FindEntryLocked(uid, [promote, now, mode](TQueryCacheEntryV2& entry) {
            if (!promote || entry.Record.GetSnapshot() || (mode && entry.Record.GetQuery().DisablePgCompatibility() != *mode)) {
                return false;
            }
            entry.LastTouched = now;
            return true;
        }, now, graveyard);
        return entry && (!mode || entry->Record.GetQuery().DisablePgCompatibility() == *mode) ? entry : nullptr;
    }

    TQueryCacheEntryV2* FindByQueryImpl(const TKqpQueryId& query, bool promote, TInstant now, TGraveyard& graveyard) {
        const TString* uid = QueryIndex.FindPtr(query);
        // The copy is required: a lazy expiry of the entry forgets this very mapping.
        return uid ? FindByUidImpl(TString(*uid), promote, now, graveyard) : nullptr;
    }

    // Warmup observation-window accounting: the state machine of V1 (kqp_compile_service.cpp). V1
    // runs it in its Find under the cache lock. V2 releases the lock before the SID and temp-table
    // checks the accounting depends on, so the accounting takes the lock again, and only while the
    // window can still count something.

    // The cheap check, two atomic loads: true once the window is closed or while no warmup uid is
    // pending. Neither the lock nor the clock is touched then.
    bool WarmupIdle() const {
        return AtomicGet(WarmupWindowClosedAtomic) || !AtomicGet(WarmupHasPendingAtomic);
    }

    void AccountWarmupHit(const TKqpCompileResult::TConstPtr& compileResult, EWarmupAttributionMode mode,
        const TIntrusivePtr<TKqpCounters>& counters)
    {
        switch (mode) {
            case EWarmupAttributionMode::None:
                return;
            case EWarmupAttributionMode::Warmup: {
                // Warmup raced a client that already cached this uid -- still
                // mark so a later client hit is attributed.
                if (AtomicGet(WarmupWindowClosedAtomic)) {
                    return;
                }
                TGuard<TAdaptiveLock> guard(Lock);
                MarkWarmupInsertLocked(compileResult->Uid);
                return;
            }
            case EWarmupAttributionMode::Client: {
                if (WarmupIdle()) {
                    return;
                }
                const TInstant now = Now();
                bool consumed = false;
                {
                    TGuard<TAdaptiveLock> guard(Lock);
                    consumed = !WarmupIdle() && RefreshWarmupWindowLocked(now)
                        && WarmupPendingHitUids.erase(compileResult->Uid) != 0;
                }
                if (consumed) {
                    Y_ABORT_UNLESS(counters, "Client warmup attribution requires counters");
                    counters->WarmupHitsInWindow->Inc();
                    counters->WarmupSavedCompileMs->Add(compileResult->CompilationDuration.MilliSeconds());
                }
                return;
            }
        }
    }

    // For true misses only (rejected hits are filtered out by the caller).
    void AccountWarmupMiss(EWarmupAttributionMode mode, const TIntrusivePtr<TKqpCounters>& counters) {
        if (mode != EWarmupAttributionMode::Client || WarmupIdle()) {
            return;
        }
        const TInstant now = Now();
        bool counted = false;
        {
            TGuard<TAdaptiveLock> guard(Lock);
            counted = !WarmupIdle() && RefreshWarmupWindowLocked(now);
        }
        if (counted) {
            Y_ABORT_UNLESS(counters, "Client warmup attribution requires counters");
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
        if (!WarmupWindowOpened) {
            if (WarmupPendingHitUids.empty()) {
                return false;
            }
            WarmupWindowOpened = true;
            WarmupWindowStart = now;
            return true;
        }
        if (now - WarmupWindowStart >= WarmupObservationWindow) {
            WarmupWindowOpened = false;
            AtomicSet(WarmupWindowClosedAtomic, 1);
            WarmupPendingHitUids.clear();
            return false;
        }
        return true;
    }

    void ResetWarmupLocked() {
        WarmupPendingHitUids.clear();
        WarmupWindowStart = TInstant::Zero();
        WarmupWindowOpened = false;
        AtomicSet(WarmupWindowClosedAtomic, 0);
        AtomicSet(WarmupHasPendingAtomic, 0);
    }

    ui64 SnapshotBytes = 0;
    const bool Enabled;
    // Query text -> uid, one entry per text. Hashing and comparing the key is TKqpQueryId's job.
    THashMap<TKqpQueryId, TString> QueryIndex;

    // Warmup window: guarded by Lock, except the two atomics, which are written under Lock and
    // read without it.
    THashSet<TString> WarmupPendingHitUids;
    TInstant WarmupWindowStart = TInstant::Zero();
    bool WarmupWindowOpened = false;
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
