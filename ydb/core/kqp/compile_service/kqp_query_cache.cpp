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

// Drops entry on temp-table clash; returns whether an entry existed before the
// drop so callers can distinguish a rejected hit from a true miss. A copy of the V1 helper in
// kqp_compile_service.cpp, which goes away with V1.
bool RejectOnTempTableClash(
    TKqpCompileResult::TConstPtr& compileResult,
    const TKqpTempTablesState::TConstPtr& tempTablesState)
{
    const bool hadEntry = static_cast<bool>(compileResult);
    if (HasTempTablesNameClashes(compileResult, tempTablesState)) {
        compileResult = nullptr;
    }
    return hadEntry;
}

// Mutable only under the cache lock.
struct TQueryCacheEntryV2 {
    TKqpCompileResult::TConstPtr CompileResult;
    TString ReplayMessage;
    TInstant LastReplayTime = TInstant::Zero();
    TInstant LastTouched = TInstant::Zero();

    ui64 AccountedBytes = 0;    // plan size + replay message, as charged to the budget
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

    bool Insert(const TKqpCompileResult::TConstPtr& compileResult) override {
        Y_ENSURE(compileResult->Query);
        YQL_ENSURE(compileResult->PreparedQuery);
        if (!Enabled) {
            return false;
        }

        // Before the lock: the clock and the plan size.
        const TInstant now = Now();
        const TString& uid = compileResult->Uid;
        const TKqpQueryId& query = *compileResult->Query;
        TQueryCacheEntryV2 entry = MakeEntry(compileResult, now);

        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        size_t dropped = 0;
        // One entry per query text: a new UID for the same text replaces the old entry.
        if (const TString* oldUid = QueryIndex.FindPtr(query); oldUid && *oldUid != uid) {
            // The copy is required: OnDropped forgets this very mapping and invalidates oldUid.
            dropped += EraseLocked(TString(*oldUid), graveyard);
        }
        dropped += InsertEntryLocked(uid, std::move(entry), query, now, graveyard);
        return dropped != 0;
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
            ReaccountLocked(uid, now, graveyard);
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

    TKqpCompileResult::TConstPtr FindByUid(const TString& uid, bool promote) override {
        const TInstant now = NowFor(promote);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByUidImpl(uid, promote, now, graveyard);
    }

    TKqpCompileResult::TConstPtr FindByQuery(const TKqpQueryId& query, bool promote) override {
        const TInstant now = NowFor(promote);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByQueryImpl(query, promote, now, graveyard);
    }

    bool Replace(const TKqpCompileResult::TConstPtr& compileResult) override {
        Y_ENSURE(compileResult->Query);
        YQL_ENSURE(compileResult->PreparedQuery);

        // Before the lock: the clock and the plan size.
        const TInstant now = Now();
        TQueryCacheEntryV2 fresh = MakeEntry(compileResult, now);

        TKqpCompileResult::TConstPtr retired;   // the old plan, freed after the lock is released
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);

        auto* entry = FindEntryLocked(compileResult->Uid, false, now, graveyard);
        if (!entry) {
            return false;
        }

        if (*entry->CompileResult->Query == *compileResult->Query) {
            // Same text: swap the plan in place and re-account its size. The entry keeps its replay
            // message; the recompile counts as a use, so its LRU position and TTL restart.
            entry->AccountedBytes = fresh.AccountedBytes + entry->ReplayMessage.size();
            retired = std::exchange(entry->CompileResult, compileResult);
            entry->LastTouched = now;
            ReaccountLocked(compileResult->Uid, now, graveyard);
            return true;
        }

        // The text changed under the same uid: re-insert so the old mapping is undone through the
        // regular hooks.
        fresh.ReplayMessage = entry->ReplayMessage;
        fresh.LastReplayTime = entry->LastReplayTime;
        fresh.AccountedBytes += fresh.ReplayMessage.size();
        InsertEntryLocked(compileResult->Uid, std::move(fresh), *compileResult->Query, now, graveyard);
        return true;
    }

    TKqpCompileResult::TConstPtr Find(
        const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>& query,
        bool disablePgCompatibility,
        TKqpTempTablesState::TConstPtr tempTablesState,
        bool promote,
        const NACLib::TSID& userSid,
        TIntrusivePtr<TKqpCounters> counters,
        TIntrusivePtr<TKqpDbCounters>& dbCounters,
        const TActorId& sender,
        const TActorContext& ctx,
        EWarmupAttributionMode warmupAttribution) override
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

        TKqpCompileResult::TConstPtr compileResult;
        size_t size = 0;
        ui64 bytes = 0;
        {
            TGraveyard graveyard;
            TGuard<TAdaptiveLock> guard(Lock);
            compileResult = uid
                ? FindByUidImpl(*uid, promote, now, graveyard, disablePgCompatibility)
                : FindByQueryImpl(*query, promote, now, graveyard);
            size = SizeLocked();
            bytes = TotalBytesLocked();
        }

        // After the lock: the result is immutable, the checks and accounting need no cache state.
        *counters->CompileQueryCacheSize = size;
        *counters->CompileQueryCacheBytes = bytes;

        if (uid) {
            counters->ReportCompileRequestGet(dbCounters);
            const bool hadEntry = RejectOnTempTableClash(compileResult, tempTablesState);

            if (compileResult) {
                Y_ENSURE(compileResult->Query);
                if (compileResult->Query->UserSid() == userSid) {
                    counters->ReportQueryCacheHit(dbCounters, true);

                    YDB_LOG_DEBUG_CTX(ctx, "Served query from cache by uid",
                        {"sender", sender},
                        {"queryUid", *uid});

                    AccountWarmupHit(compileResult, warmupAttribution, counters);
                    return compileResult;
                } else {
                    YDB_LOG_NOTICE_CTX(ctx, "Non-matching user sid for query",
                        {"sender", sender},
                        {"queryUid", *uid},
                        {"expectedSid", compileResult->Query->UserSid()},
                        {"actualSid", userSid});
                }
            }

            // hadEntry => uid was present but rejected (SID / TempTables) -- not a true miss.
            if (!hadEntry) {
                AccountWarmupMiss(warmupAttribution, counters);
            }
            return nullptr;
        }

        YDB_LOG_DEBUG_CTX(ctx, "Try to find query by queryId",
            {"queryId", query->SerializeToString()});
        const bool hadEntry = RejectOnTempTableClash(compileResult, tempTablesState);

        if (compileResult) {
            counters->ReportQueryCacheHit(dbCounters, true);
            YDB_LOG_DEBUG_CTX(ctx, "Served query from cache from query text",
                {"sender", sender},
                {"queryUid", compileResult->Uid});

            AccountWarmupHit(compileResult, warmupAttribution, counters);
            return compileResult;
        }

        if (!hadEntry) {
            AccountWarmupMiss(warmupAttribution, counters);
        }

        // note, we don't report cache miss, because it's up to caller to decide what to do:
        // in particular, session actor will go to the compile service, which will actually report the miss.

        return nullptr;
    }

    bool EraseIfCurrent(const TKqpCompileResult::TConstPtr& result) override {
        const TInstant now = NowFor(false);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByUidImpl(result->Uid, false, now, graveyard) == result && EraseLocked(result->Uid, graveyard);
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
            out.push_back({entry.CompileResult, entry.LastTouched});
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
        Y_ABORT_UNLESS(entry.CompileResult && entry.CompileResult->Query);

        // Only if the text still maps to this uid: a late drop of a replaced entry must not take
        // the live mapping with it.
        auto it = QueryIndex.find(*entry.CompileResult->Query);
        if (it != QueryIndex.end() && it->second == evicted.Key) {
            QueryIndex.erase(it);
        }
    }

    void OnCleared() {
        QueryIndex.clear();
        ResetWarmupLocked();
    }

    // Builds an entry without the lock: the plan size is the expensive part.
    static TQueryCacheEntryV2 MakeEntry(const TKqpCompileResult::TConstPtr& compileResult, TInstant now) {
        return TQueryCacheEntryV2{
            .CompileResult = compileResult,
            .LastTouched = now,
            .AccountedBytes = compileResult->PreparedQuery->ByteSize(),
        };
    }

    // Inserts the entry and maps its text to it. An entry the policy rejects is not stored and not
    // mapped; the dropped count is returned either way.
    size_t InsertEntryLocked(const TString& uid, TQueryCacheEntryV2&& entry, const TKqpQueryId& query,
        TInstant now, TGraveyard& graveyard)
    {
        const auto result = InsertLocked(uid, std::move(entry), now, graveyard);
        if (result.Admitted) {
            // Mapped after the insert: dropping the replaced entry under this uid must not forget
            // the mapping of the new one.
            QueryIndex[query] = uid;
        }
        return result.Dropped;
    }

    TKqpCompileResult::TConstPtr FindByUidImpl(const TString& uid, bool promote, TInstant now, TGraveyard& graveyard, std::optional<bool> mode = {}) {
        if (mode) {
            auto* entry = FindEntryLocked(uid, false, now, graveyard);
            if (!entry || entry->CompileResult->Query->DisablePgCompatibility() != *mode) {
                return nullptr;
            }
        }
        auto* entry = FindEntryLocked(uid, promote, now, graveyard);
        if (!entry) {
            return nullptr;
        }
        if (promote) {
            entry->LastTouched = now;
        }
        return entry->CompileResult;
    }

    TKqpCompileResult::TConstPtr FindByQueryImpl(const TKqpQueryId& query, bool promote, TInstant now, TGraveyard& graveyard) {
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
