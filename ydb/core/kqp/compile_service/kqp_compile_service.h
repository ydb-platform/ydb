#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/compile_cache/types.h>

#include <variant>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>

#include <util/system/spinlock.h>

#include <optional>

namespace NKikimr {
namespace NPathAliasing {
class TPathNormalizer;
}

namespace NKqp {

class TKqpDbCounters;

enum class ECompileActorAction {
    COMPILE,
    SPLIT,
};

// How a cache lookup participates in warmup observation-window accounting.
// Default `None` is fail-safe: new call sites don't pollute counters.
enum class EWarmupAttributionMode {
    None,    // no side effects, no counters
    Client,  // consume pending mark on hit (+inc hit/miss counters)
    Warmup,  // mark uid on hit (validation), nothing else
};

// Metadata retained for a resumable query, independently of its optimizer graph.
struct TQuerySnapshotInfo {
    TString Uid;
    TKqpQueryId Query;
    TInstant CompiledAt;
    TDuration Duration;
    NYql::TIssues Issues;
    TMaybe<NJson::TJsonValue> CompileMeta;
};

struct TQueryCacheSnapshotRecord {
    std::shared_ptr<const TQuerySnapshotInfo> Info;
    TPreCboSnapshotPtr Snapshot;
};

class TQueryCacheRecord {
public:
    TQueryCacheRecord() = default;
    TQueryCacheRecord(TKqpCompileResult::TConstPtr plan) : Body(std::move(plan)) {}
    TQueryCacheRecord(TQueryCacheSnapshotRecord snapshot) : Body(std::move(snapshot)) {}

    TKqpCompileResult::TConstPtr GetPlan() const {
        auto* plan = std::get_if<TKqpCompileResult::TConstPtr>(&Body);
        return plan ? *plan : nullptr;
    }

    const TQueryCacheSnapshotRecord* GetSnapshot() const {
        return std::get_if<TQueryCacheSnapshotRecord>(&Body);
    }

    explicit operator bool() const { return GetSnapshot() || GetPlan(); }
    const TKqpQueryId& GetQuery() const {
        if (auto* snapshot = GetSnapshot()) {
            return snapshot->Info->Query;
        }
        return *std::get<TKqpCompileResult::TConstPtr>(Body)->Query;
    }

private:
    std::variant<TKqpCompileResult::TConstPtr, TQueryCacheSnapshotRecord> Body;
};

class TKqpQueryCacheSnapshot {
public:
    struct TEntry {
        std::variant<TKqpCompileResult::TConstPtr, std::shared_ptr<const TQuerySnapshotInfo>> Info;
        TInstant LastTouched;
        std::optional<ui64> AccessCount = {};

        const TString& GetUid() const {
            return std::visit([](const auto& info) -> const TString& { return info->Uid; }, Info);
        }
        void SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to) const;
    };

    struct TUidComparator {
        bool operator()(const TEntry& a, const TEntry& b) const {
            return a.GetUid() < b.GetUid();
        }
    };

    struct TUidLowerBoundComparator {
        bool operator()(const TEntry& entry, const TString& uid) const {
            return entry.GetUid() < uid;
        }
    };

    struct TUidUpperBoundComparator {
        bool operator()(const TString& uid, const TEntry& entry) const {
            return uid < entry.GetUid();
        }
    };

public:
    TKqpQueryCacheSnapshot() = default;
    void Clear();
    void Insert(const TString& uid, const TKqpCompileResult::TConstPtr& compileResult, TInstant now);
    void Erase(const TString& uid);
    void Replace(const TString& uid, const TKqpCompileResult::TConstPtr& newResult);
    void Touch(const TString& uid, TInstant now);
    TVector<TEntry> GetSnapshot() const;
    size_t Size() const noexcept { return Entries_.size(); }
    bool Contains(const TString& uid) const { return Index_.contains(uid); }
private:
    TVector<TEntry> Entries_;
    THashMap<TString, ui32> Index_;
};

// Node-wide cache of compiled queries, shared by the session actors, the compile service and the
// KQP proxy. Thread-safe. Two implementations live behind this interface, selected once at node
// start by the EnableCompileCacheV2 feature flag: TKqpQueryCacheV1 below and the one built on
// TKqpCache (kqp_query_cache.cpp). The interface exists only for the rollout: once V2 is the
// default everywhere, V1 and the interface go away and V2 becomes the concrete TKqpQueryCache.
class TKqpQueryCache: public TThrRefBase {
public:
    // Opens lazily on first non-warmup access with non-empty pending set,
    // closes once after the duration (no timer, checked on access).
    static constexpr TDuration WarmupObservationWindow = TDuration::Minutes(5);

    virtual bool SupportsSnapshots() const { return false; }

    // V2 counts AccessCount itself; for V1 the session counts, as before.
    virtual bool CountsAccesses() const { return false; }

    virtual bool Insert(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot = {}, ui64 accesses = 0) = 0;

    virtual void CountSnapshotAccesses(const TQueryCacheSnapshotRecord&, ui64) {}

    // Idempotent; no-op once the window is closed.
    virtual void MarkWarmupInsert(const TString& uid) = 0;

    virtual void AttachReplayMessage(const TString uid, TString replayMessage) = 0;

    virtual TString ReplayMessageByUid(const TString uid, TDuration timeout) = 0;

    TKqpCompileResult::TConstPtr FindByUid(const TString& uid, bool promote) { return LookupByUid(uid, promote).GetPlan(); }
    virtual TQueryCacheRecord LookupByUid(const TString& uid, bool promote) = 0;

    virtual bool Replace(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot = {}, ui64 accesses = 0) = 0;

    TKqpCompileResult::TConstPtr FindByQuery(const TKqpQueryId& query, bool promote) { return LookupByQuery(query, promote).GetPlan(); }
    virtual TQueryCacheRecord LookupByQuery(const TKqpQueryId& query, bool promote) = 0;

    // find by either uid or query.
    // warmupAttribution defaults to None so new callers don't pollute counters.
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
        EWarmupAttributionMode warmupAttribution = EWarmupAttributionMode::None) {
        return Lookup(uid, query, disablePgCompatibility, tempTablesState, promote, userSid, std::move(counters), dbCounters,
            sender, ctx, warmupAttribution, true).GetPlan();
    }

    virtual TQueryCacheRecord Lookup(
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
        EWarmupAttributionMode warmupAttribution, bool plansOnly = false) = 0;

    virtual bool EraseByUid(const TString& uid) = 0;

    virtual bool EraseIfCurrent(const TKqpCompileResult::TConstPtr& compileResult) = 0;

    virtual size_t Size() const = 0;

    virtual ui64 Bytes() const = 0;

    virtual TVector<TKqpQueryCacheSnapshot::TEntry> GetSnapshot() const = 0;

    virtual size_t EraseExpiredQueries() = 0;

    virtual void Clear() = 0;
};

// Cache is shared between query sessions, compile service and kqp proxy.
// Currently we don't use RW lock, because of cache promotions during lookup:
// in benchmark both versions (RW spinlock and adaptive W spinlock) show same
// performance. Might consider RW lock again in future.
class TKqpQueryCacheV1: public TKqpQueryCache {
public:
    TKqpQueryCacheV1(size_t size, TDuration ttl)
        : List(size)
        , Ttl(ttl) {}

    bool Insert(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot = {}, ui64 accesses = 0) override;

    // Idempotent; no-op once the window is closed.
    void MarkWarmupInsert(const TString& uid) override;

    void AttachReplayMessage(const TString uid, TString replayMessage) override;

    TString ReplayMessageByUid(const TString uid, TDuration timeout) override;

    TQueryCacheRecord LookupByUid(const TString& uid, bool promote) override {
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByUidImpl(uid, promote);
    }

    bool Replace(const TKqpCompileResult::TConstPtr& compileResult, TPreCboSnapshotPtr snapshot = {}, ui64 accesses = 0) override;

    TQueryCacheRecord LookupByQuery(const TKqpQueryId& query, bool promote) override {
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByQueryImpl(query, promote);
    }

    // find by either uid or query.
    // warmupAttribution defaults to None so new callers don't pollute counters.
    TQueryCacheRecord Lookup(
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
        EWarmupAttributionMode warmupAttribution, bool plansOnly = false) override;

    bool EraseByUid(const TString& uid) override {
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseByUidImpl(uid);
    }

    bool EraseIfCurrent(const TKqpCompileResult::TConstPtr& compileResult) override {
        TGuard<TAdaptiveLock> guard(Lock);
        const auto& uid = compileResult->Uid;
        return FindByUidImpl(uid, false) == compileResult && EraseByUidImpl(uid);
    }

    size_t Size() const override {
        TGuard<TAdaptiveLock> guard(Lock);
        return SizeImpl();
    }

    ui64 Bytes() const override {
        TGuard<TAdaptiveLock> guard(Lock);
        return BytesImpl();
    }

    ui64 BytesImpl() const {
        return ByteSize;
    }

    TVector<TKqpQueryCacheSnapshot::TEntry> GetSnapshot() const override;
    size_t EraseExpiredQueries() override;

    void Clear() override;

private:
    TKqpCompileResult::TConstPtr FindByUidImpl(const TString& uid, bool promote, std::optional<bool> disablePgCompatibility = std::nullopt);
    TKqpCompileResult::TConstPtr FindByQueryImpl(const TKqpQueryId& query, bool promote);
    bool EraseByUidImpl(const TString& uid);

    size_t SizeImpl() const {
        return Index.size();
    }

    void EraseQueryIndex(const TKqpCompileResult& compileResult);
    void InsertQuery(const TKqpCompileResult::TConstPtr& compileResult);

    void DecBytes(ui64 bytes) {
        if (bytes > ByteSize) {
            ByteSize = 0;
        } else {
            ByteSize -= bytes;
        }
    }

    void IncBytes(ui64 bytes) {
        ByteSize += bytes;
    }

private:
    struct TCacheEntry {
        TKqpCompileResult::TConstPtr CompileResult;
        TInstant ExpiredAt;
        TString ReplayMessage = "";
        TInstant LastReplayTime = TInstant::Zero();
    };

    using TList = TLRUList<TString, TCacheEntry>;
    using TItem = TList::TItem;

private:
    TList List;
    THashSet<TItem, TItem::THash> Index;
    THashMap<TKqpQueryId, TString, THash<TKqpQueryId>> QueryIndex;
    ui64 ByteSize = 0;
    TDuration Ttl;

    TKqpQueryCacheSnapshot Snapshot;
    TAdaptiveLock Lock;

    // Guarded by Lock (except the two atomics below).
    THashSet<TString> WarmupPendingHitUids;
    TInstant WarmupWindowStart = TInstant::Zero();
    bool WarmupWindowOpened = false;

    // Lock-free fast-path: in steady state (window closed) accounting helpers
    // bail out before touching Lock. Written under Lock, read without it.
    TAtomic WarmupWindowClosedAtomic = 0;
    TAtomic WarmupHasPendingAtomic = 0;

    // Must be called under Lock. May transition open -> closed on expiry.
    bool RefreshWarmupWindowStateImpl();

    bool WarmupAccountingIsNoopFast() const {
        return AtomicGet(WarmupWindowClosedAtomic) || !AtomicGet(WarmupHasPendingAtomic);
    }

    bool TryConsumeWarmupHitImpl(const TString& uid) {
        if (!RefreshWarmupWindowStateImpl()) {
            return false;
        }
        return WarmupPendingHitUids.erase(uid) != 0;
    }
    bool ShouldCountWarmupMissImpl() {
        return RefreshWarmupWindowStateImpl();
    }

    void AccountWarmupHitImpl(
        const TKqpCompileResult::TConstPtr& compileResult,
        EWarmupAttributionMode mode,
        const TIntrusivePtr<TKqpCounters>& counters);

    void AccountWarmupMissImpl(
        EWarmupAttributionMode mode,
        const TIntrusivePtr<TKqpCounters>& counters);
};

using TKqpQueryCachePtr = TIntrusivePtr<TKqpQueryCache>;

// `compileCacheV2` selects the implementation (the EnableCompileCacheV2 feature flag, read once at
// node start); `size` is a number of entries, `ttl == Zero` disables expiry.
TKqpQueryCachePtr CreateKqpQueryCache(bool compileCacheV2, size_t size, TDuration ttl);

bool HasTempTablesNameClashes(
    TKqpCompileResult::TConstPtr compileResult,
    TKqpTempTablesState::TConstPtr tempTablesState,
    bool withSessionId = false);

IActor* CreateKqpCompileService(
    TKqpQueryCachePtr queryCache,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup
    );

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const std::shared_ptr<const NPathAliasing::TPathNormalizer>& pathNormalizer,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& clientAddress,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings, const TMaybe<TString>& applicationName,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, NWilson::TTraceId traceId = {},
    TKqpTempTablesState::TConstPtr tempTablesState = nullptr,
    ECompileActorAction compileAction = ECompileActorAction::COMPILE,
    bool collectFullDiagnostics = false,
    std::shared_ptr<NYql::TExprContext> ctx = nullptr,
    NYql::TExprNode::TPtr expr = nullptr,
    bool usePessimisticLocks = false, TOptimizerInput optimizerInput = {});

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

} // namespace NKqp
} // namespace NKikimr
