#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>

#include <ydb/library/testlib/helpers.h>

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NKqp {

namespace {

// TKqpQueryCache reads TAppData::TimeProvider; the fixture swaps it for a manual clock.
class TManualClock : public ITimeProvider {
public:
    TInstant Now() override {
        return Current;
    }

    void Advance(TDuration delta) {
        Current += delta;
    }

private:
    TInstant Current = TInstant::Seconds(1000);
};

struct TClockFixture {
    TClockFixture()
        : Clock(MakeIntrusive<TManualClock>())
        , Saved(TAppData::TimeProvider)
    {
        TAppData::TimeProvider = Clock;
    }

    ~TClockFixture() {
        TAppData::TimeProvider = Saved;
    }

    TIntrusivePtr<TManualClock> Clock;
    TIntrusivePtr<ITimeProvider> Saved;
};

TKqpQueryId MakeQueryId(const TString& text) {
    return TKqpQueryId("db", "/Root", "", "user@builtin", text,
        TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_DML), nullptr, TGUCSettings());
}

// `planBytes` controls PreparedQuery->ByteSize(), the unit the cache accounts in Bytes().
TKqpCompileResult::TConstPtr MakeResult(const TString& uid, const TString& text, size_t planBytes = 16) {
    auto result = TKqpCompileResult::Make(uid, Ydb::StatusIds::SUCCESS, NYql::TIssues(), ETableReadType::Other,
        TDuration::Zero(), MakeQueryId(text));
    auto* proto = new NKikimrKqp::TPreparedQuery();
    proto->SetText(TString(planBytes, 'x'));
    result->PreparedQuery = std::make_shared<TPreparedQueryHolder>(proto, nullptr, /*noFillTables=*/true);
    return result;
}

ui64 PlanBytes(const TKqpCompileResult::TConstPtr& result) {
    return result->PreparedQuery->ByteSize();
}

TVector<TString> SnapshotUids(const TKqpQueryCache& cache) {
    TVector<TString> uids;
    for (const auto& entry : cache.GetSnapshot()) {
        uids.push_back(entry.GetUid());
    }
    Sort(uids);
    return uids;
}

TInstant LastTouched(const TKqpQueryCache& cache, const TString& uid) {
    for (const auto& entry : cache.GetSnapshot()) {
        if (entry.GetUid() == uid) {
            return entry.LastTouched;
        }
    }
    return TInstant::Zero();
}

// The sys view row of `uid`, built the way the sys view builds it.
NKikimrKqp::TCompileCacheQueryInfo Row(const TKqpQueryCache& cache, const TString& uid) {
    NKikimrKqp::TCompileCacheQueryInfo row;
    for (const auto& entry : cache.GetSnapshot()) {
        if (entry.GetUid() == uid) {
            entry.SerializeTo(&row);
        }
    }
    return row;
}

struct TFakeSnapshot : TPreCboSnapshot {
    explicit TFakeSnapshot(ui64 bytes)
        : Bytes(bytes)
    {}

    ui64 ByteSize() const override {
        return Bytes;
    }

    ui64 Bytes;
};

TPreCboSnapshotPtr MakeSnapshot(ui64 bytes) {
    return std::make_shared<const TFakeSnapshot>(bytes);
}

TKqpTempTablesState::TConstPtr SomeTempTables() {
    auto state = std::make_shared<TKqpTempTablesState>();
    state->TempTables["/Root/tmp"] = {};
    return state;
}

// Lookup needs a TActorContext, so each call runs inside a one-shot actor.
class TCallInActor : public NActors::TActorBootstrapped<TCallInActor> {
public:
    TCallInActor(std::function<void(const TActorContext&)> call, NActors::TActorId done)
        : Call(std::move(call))
        , Done(done)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Call(ctx);
        Send(Done, new NActors::TEvents::TEvWakeup());
        PassAway();
    }

private:
    std::function<void(const TActorContext&)> Call;
    NActors::TActorId Done;
};

// A bare actor runtime that knows the KQP log components: V1 logs through the actor context.
class TKqpLogRuntime : public NActors::TTestActorRuntimeBase {
public:
    TKqpLogRuntime() {
        InitNodes();
        AppendToLogSettings(NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>);
        IsInitialized = true;
    }
};

struct TLookupFixture {

    TQueryCacheRecord ByText(TKqpQueryCache& cache, const TString& text, const TString& userSid,
        bool plansOnly = false, TKqpTempTablesState::TConstPtr tempTables = nullptr,
        EWarmupAttributionMode mode = EWarmupAttributionMode::None)
    {
        return Run(cache, Nothing(), MakeQueryId(text), userSid, plansOnly, std::move(tempTables), mode);
    }

    TQueryCacheRecord ByUid(TKqpQueryCache& cache, const TString& uid, const TString& userSid,
        bool plansOnly = false, TKqpTempTablesState::TConstPtr tempTables = nullptr)
    {
        return Run(cache, uid, Nothing(), userSid, plansOnly, std::move(tempTables));
    }

    TQueryCacheRecord Run(TKqpQueryCache& cache, TMaybe<TString> uid, TMaybe<TKqpQueryId> query,
        const TString& userSid, bool plansOnly, TKqpTempTablesState::TConstPtr tempTables,
        EWarmupAttributionMode mode = EWarmupAttributionMode::None)
    {
        TQueryCacheRecord record;
        const NActors::TActorId done = Runtime.AllocateEdgeActor();
        Runtime.Register(new TCallInActor([&](const TActorContext& ctx) {
            TKqpDbCountersPtr dbCounters;
            record = cache.Lookup(uid, query, /*disablePgCompatibility=*/true, tempTables, /*promote=*/true, userSid, Counters, dbCounters,
                done, ctx, mode, plansOnly);
        }, done));
        Runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);
        return record;
    }

    TKqpLogRuntime Runtime;
    TIntrusivePtr<TKqpCounters> Counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
};

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryCache) {

    Y_UNIT_TEST_TWIN(EntriesAndIndexes, V2) {
        TClockFixture clock;
        auto cacheHolder = CreateKqpQueryCache(V2, /*size=*/2, TDuration::Minutes(1));
        TKqpQueryCache& cache = *cacheHolder;
        auto a = MakeResult("a", "SELECT 1", 10);
        auto b = MakeResult("b", "SELECT 2", 20);
        auto c = MakeResult("c", "SELECT 3", 30);

        // An insert is found by uid and by text, and is accounted in bytes and the listing.
        UNIT_ASSERT(!cache.Insert(a));
        UNIT_ASSERT(!cache.Insert(b));
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), a);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 2"), false), b);
        UNIT_ASSERT(!cache.FindByUid("missing", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 4"), false));
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + PlanBytes(b));
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), (TVector<TString>{"a", "b"}));

        // A promoting lookup moves `a` up and stamps it in the listing, so the overflow evicts `b`
        // together with its text mapping.
        clock.Clock->Advance(TDuration::Seconds(1));
        UNIT_ASSERT(cache.FindByUid("a", /*promote=*/true));
        UNIT_ASSERT_VALUES_EQUAL(LastTouched(cache, "a"), clock.Clock->Now());
        UNIT_ASSERT(cache.Insert(c));
        UNIT_ASSERT(!cache.FindByUid("b", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + PlanBytes(c));
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), (TVector<TString>{"a", "c"}));

        // Erase drops the entry with its text mapping and bytes; a second erase is a no-op.
        UNIT_ASSERT(cache.EraseByUid("c"));
        UNIT_ASSERT(!cache.EraseByUid("c"));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 3"), false));
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a));

        // Clear drops everything and leaves a reusable cache.
        cache.Clear();
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
        UNIT_ASSERT(cache.GetSnapshot().empty());
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 1"), false));
        auto first = MakeResult("u1", "SELECT 1", 10);
        UNIT_ASSERT(!cache.Insert(first));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), first);

        if (!V2) {
            return;   // V1 throws on a second entry for the same text and does not re-account Replace
        }

        // The same text under a new uid replaces the old entry.
        auto second = MakeResult("u2", "SELECT 1", 30);
        UNIT_ASSERT(!cache.Insert(second));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT(!cache.FindByUid("u1", false));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), second);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(second));

        // Reinserting the same uid replaces the entry without losing the text mapping.
        auto third = MakeResult("u2", "SELECT 1", 50);
        UNIT_ASSERT(!cache.Insert(third));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), third);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(third));

        // Replace with the same text swaps the plan in place and re-accounts it; an unknown uid is a no-op.
        auto replaced = MakeResult("u2", "SELECT 1", 70);
        cache.Replace(replaced);
        cache.Replace(MakeResult("missing", "SELECT 2"));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), replaced);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(replaced));

        // Replace with another text under the same uid moves the text mapping.
        auto moved = MakeResult("u2", "SELECT 5", 40);
        cache.Replace(moved);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 1"), false));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 5"), false), moved);
        UNIT_ASSERT_EQUAL(cache.FindByUid("u2", false), moved);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(moved));
    }

    Y_UNIT_TEST_TWIN(ZeroSize, V2) {
        TClockFixture clock;
        auto cacheHolder = CreateKqpQueryCache(V2, /*size=*/0, TDuration::Zero());
        TKqpQueryCache& cache = *cacheHolder;
        cache.Insert(MakeResult("a", "SELECT 1"));
        cache.Insert(MakeResult("b", "SELECT 2"));
        if (V2) {
            // V2 treats a zero size as a disabled cache.
            UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
            UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        } else {
            // V1 keeps one entry: TLRUList never evicts its last item.
            UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
            UNIT_ASSERT(cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        }
    }

    // V2 only: V1 serves expired entries until the sweep and reads the wall clock for replay.
    Y_UNIT_TEST(TtlSweepDropsExpiredAndPromoteExtends) {
        constexpr bool V2 = true;
        TClockFixture clock;
        auto cacheHolder = CreateKqpQueryCache(V2, /*size=*/10, TDuration::Seconds(10));
        TKqpQueryCache& cache = *cacheHolder;

        auto a = MakeResult("a", "SELECT 1");
        auto b = MakeResult("b", "SELECT 2");
        cache.Insert(a);
        cache.Insert(b);

        // A promote at +5s pushes `a` to +15s; `b` still expires at +10s.
        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT(cache.FindByUid("a", /*promote=*/true));
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 0);

        // At +11s `b` is expired: a lookup misses and retires it. The next sweep still reports
        // that expiry, so eviction accounting does not lose it, and reports it only once.
        clock.Clock->Advance(TDuration::Seconds(6));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 2"), false));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a));
        UNIT_ASSERT_EQUAL(SnapshotUids(cache), TVector<TString>{"a"});
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 0);
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), a);

        // At +16s `a` is expired too; this time the sweep gets it first.
        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
    }

    Y_UNIT_TEST_TWIN(AttachingReplayPreservesLruAndTtl, V2) {
        TClockFixture clock;
        auto cache = CreateKqpQueryCache(V2, 2, TDuration::Seconds(10));
        cache->Insert(MakeResult("a", "SELECT a"));
        cache->Insert(MakeResult("b", "SELECT b"));
        clock.Clock->Advance(TDuration::Seconds(5));
        cache->AttachReplayMessage("a", "replay a");
        cache->Insert(MakeResult("c", "SELECT c"));
        UNIT_ASSERT(!cache->FindByUid("a", false));

        const auto touched = LastTouched(*cache, "b");
        cache->AttachReplayMessage("b", "replay b");
        if (V2) {
            UNIT_ASSERT_VALUES_EQUAL(LastTouched(*cache, "b"), touched);
        }
        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(cache->EraseExpiredQueries(), 1);
        UNIT_ASSERT(!cache->FindByUid("b", false));
        UNIT_ASSERT(cache->FindByUid("c", false));
    }

    Y_UNIT_TEST_TWIN(ReplayMessages, V2) {
        TClockFixture clock;
        // The TTL must outlive the replay timeouts below: fetching the replay message does not extend it.
        auto cacheHolder = CreateKqpQueryCache(V2, /*size=*/10, TDuration::Minutes(10));
        TKqpQueryCache& cache = *cacheHolder;

        auto a = MakeResult("a", "SELECT 1");
        cache.Insert(a);

        // The attached message is counted in bytes and handed out at most once per timeout.
        const TString message = "replay-message";
        const auto timeout = TDuration::Seconds(30);
        cache.AttachReplayMessage("a", message);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + message.size());
        UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("missing", timeout), "");
        if (V2) {   // V1 times the replay hand-out by the wall clock, not by TimeProvider
            UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), "");   // just attached
            clock.Clock->Advance(timeout + TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), message);
            UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), "");   // within the timeout again
        }

        // Re-attaching swaps the accounted size, not adds to it.
        const TString shorter = "short";
        cache.AttachReplayMessage("a", shorter);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(a) + shorter.size());

        // Replace keeps the attached message and its accounting alongside the new plan.
        if (V2) {   // V1 does not re-account the plan on Replace
            auto recompiled = MakeResult("a", "SELECT 1", 48);
            cache.Replace(recompiled);
            UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(recompiled) + shorter.size());
            clock.Clock->Advance(timeout + TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(cache.ReplayMessageByUid("a", timeout), shorter);
        }

        // Erase takes the message bytes with the entry.
        UNIT_ASSERT(cache.EraseByUid("a"));
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 0);
    }
}

Y_UNIT_TEST_SUITE(KqpQueryCacheSnapshots) {

    // A snapshot record keeps the query and the sys view fields, never a plan; plan lookups skip it.
    Y_UNIT_TEST(SnapshotRecord) {
        TClockFixture clock;
        auto cacheHolder = CreateKqpQueryCache(/*compileCacheV2=*/true, /*size=*/10, TDuration::Zero());
        TKqpQueryCache& cache = *cacheHolder;
        UNIT_ASSERT(cache.SupportsSnapshots());
        UNIT_ASSERT(cache.CountsAccesses());

        auto compiled = MakeResult("a", "SELECT 1", 40);
        auto snapshot = MakeSnapshot(100);
        cache.Insert(compiled, snapshot, /*accesses=*/1);

        auto record = cache.LookupByUid("a", false);
        UNIT_ASSERT(record.GetSnapshot());
        UNIT_ASSERT(!record.GetPlan());
        UNIT_ASSERT_EQUAL(record.GetSnapshot()->Snapshot, snapshot);
        UNIT_ASSERT_EQUAL(record.GetQuery(), MakeQueryId("SELECT 1"));
        UNIT_ASSERT(cache.LookupByQuery(MakeQueryId("SELECT 1"), false).GetSnapshot());
        UNIT_ASSERT(!cache.FindByUid("a", false));
        UNIT_ASSERT(!cache.FindByQuery(MakeQueryId("SELECT 1"), false));
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 100);

        auto row = Row(cache, "a");
        UNIT_ASSERT_VALUES_EQUAL(row.GetQueryId(), "a");
        UNIT_ASSERT_VALUES_EQUAL(row.GetQuery(), "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(row.GetUserSID(), "user@builtin");
        UNIT_ASSERT_VALUES_EQUAL(row.GetAccessCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(row.GetCompiledQueryAt(), compiled->CompiledAt.MicroSeconds());

        // Plan and snapshot replace each other under the same uid; the counter is kept.
        auto recompiled = MakeResult("a", "SELECT 1", 60);
        cache.Replace(recompiled, nullptr, /*accesses=*/2);
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), recompiled);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(recompiled));
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "a").GetAccessCount(), 3);
        cache.Replace(recompiled, MakeSnapshot(70), /*accesses=*/0);
        UNIT_ASSERT(cache.LookupByUid("a", false).GetSnapshot());
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), 70);

        // One record per text: a plan under a new uid replaces the snapshot, and an erased
        // snapshot takes its text with it.
        auto other = MakeResult("b", "SELECT 1");
        cache.Insert(other, nullptr, 0);
        UNIT_ASSERT(!cache.LookupByUid("a", false));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), other);
        cache.Insert(MakeResult("c", "SELECT 2"), MakeSnapshot(10), 0);
        UNIT_ASSERT(cache.EraseByUid("c"));
        UNIT_ASSERT(!cache.LookupByQuery(MakeQueryId("SELECT 2"), false));
    }

    // Lookup serves sessions (plans only) and the compile service, and counts the accesses.
    Y_UNIT_TEST(LookupFiltersAndCounts) {
        TClockFixture clock;
        TLookupFixture lookup;
        auto cacheHolder = CreateKqpQueryCache(/*compileCacheV2=*/true, /*size=*/10, TDuration::Seconds(10));
        TKqpQueryCache& cache = *cacheHolder;
        cache.Insert(MakeResult("p", "SELECT p"), nullptr, /*accesses=*/1);
        cache.Insert(MakeResult("s", "SELECT s"), MakeSnapshot(10), /*accesses=*/1);
        const TString user = "user@builtin";
        clock.Clock->Advance(TDuration::Seconds(5));

        // A session cannot run a snapshot: for it that is a miss, and not an access.
        UNIT_ASSERT(lookup.ByText(cache, "SELECT p", user, /*plansOnly=*/true).GetPlan());
        UNIT_ASSERT(!lookup.ByText(cache, "SELECT s", user, /*plansOnly=*/true));
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "p").GetAccessCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "s").GetAccessCount(), 1);

        // Retrieving a snapshot by text or UID starts a compilation. Neither lookup delivers a plan.
        UNIT_ASSERT(lookup.ByText(cache, "SELECT s", user).GetSnapshot());
        UNIT_ASSERT(lookup.ByUid(cache, "s", user).GetSnapshot());
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "s").GetAccessCount(), 1);

        // Another user's uid is a miss and is not counted.
        UNIT_ASSERT(!lookup.ByUid(cache, "s", "other@builtin"));
        UNIT_ASSERT(!lookup.ByUid(cache, "p", "other@builtin"));
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "s").GetAccessCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "p").GetAccessCount(), 2);

        // A cold compile in a session with temp tables still needs the original UID.
        auto tempTables = SomeTempTables();
        UNIT_ASSERT(lookup.ByText(cache, "SELECT s", user, false, tempTables).GetSnapshot());
        UNIT_ASSERT(!lookup.ByText(cache, "SELECT s", user, true, tempTables));
        UNIT_ASSERT(lookup.ByUid(cache, "s", user, false, tempTables).GetSnapshot());
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "s").GetAccessCount(), 1);

        // A warmup lookup is not a client use.
        UNIT_ASSERT(lookup.ByText(cache, "SELECT p", user, false, nullptr, EWarmupAttributionMode::Warmup).GetPlan());
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "p").GetAccessCount(), 2);

        // The compile service's own existence checks do not count.
        UNIT_ASSERT(cache.LookupByUid("p", false));
        UNIT_ASSERT(cache.LookupByQuery(MakeQueryId("SELECT p"), false));
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "p").GetAccessCount(), 2);

        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpiredQueries(), 1);
        UNIT_ASSERT(!cache.LookupByUid("s", false));
    }

    Y_UNIT_TEST(UnchangedSnapshotWithoutDeliveriesPreservesTtl) {
        TClockFixture clock;
        auto cache = CreateKqpQueryCache(true, 2, TDuration::Seconds(10));
        const auto plan = MakeResult("s", "SELECT s");
        const auto snapshot = MakeSnapshot(4);
        cache->Insert(plan, snapshot, 1);
        const auto inserted = LastTouched(*cache, "s");
        clock.Clock->Advance(TDuration::Seconds(5));
        cache->Replace(plan, snapshot, 0);
        UNIT_ASSERT_VALUES_EQUAL(LastTouched(*cache, "s"), inserted);
        clock.Clock->Advance(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(cache->EraseExpiredQueries(), 1);

        cache->Insert(plan, snapshot, 1);
        clock.Clock->Advance(TDuration::Seconds(5));
        cache->Replace(plan, MakeSnapshot(4), 0);
        UNIT_ASSERT_VALUES_EQUAL(LastTouched(*cache, "s"), clock.Clock->Now());
    }

    Y_UNIT_TEST(WarmupWindowCountsFirstHitAndSubsequentMisses) {
        TClockFixture clock;
        TLookupFixture lookup;
        auto cache = CreateKqpQueryCache(true, 2, TDuration::Zero());
        auto plan = MakeResult("p", "SELECT p");
        cache->Insert(plan);
        const auto find = [&](const TString& text, EWarmupAttributionMode mode) {
            return lookup.ByText(*cache, text, "user@builtin", false, nullptr, mode);
        };

        using EMode = EWarmupAttributionMode;
        UNIT_ASSERT(!find("miss", EMode::Client));
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupMissesInWindow->Val(), 0);
        UNIT_ASSERT(find("SELECT p", EMode::Warmup));
        UNIT_ASSERT(find("SELECT p", EMode::Client));
        UNIT_ASSERT(find("SELECT p", EMode::Client));
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupHitsInWindow->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupSavedCompileMs->Val(), plan->CompilationDuration.MilliSeconds());
        UNIT_ASSERT(!find("miss", EMode::Client));
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupMissesInWindow->Val(), 1);

        clock.Clock->Advance(TKqpQueryCache::WarmupObservationWindow);
        UNIT_ASSERT(!find("miss", EMode::Client));
        UNIT_ASSERT(find("SELECT p", EMode::Warmup));
        UNIT_ASSERT(find("SELECT p", EMode::Client));
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupHitsInWindow->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(lookup.Counters->WarmupMissesInWindow->Val(), 1);
    }

    // Snapshots have their own byte budget: one that does not fit is stored as its plan.
    Y_UNIT_TEST(SnapshotBudget) {
        TClockFixture clock;
        auto cacheHolder = CreateKqpQueryCache(/*compileCacheV2=*/true, /*size=*/10, TDuration::Zero());
        TKqpQueryCache& cache = *cacheHolder;

        cache.Insert(MakeResult("a", "SELECT 1"), MakeSnapshot(60_MB), 0);
        UNIT_ASSERT(cache.LookupByUid("a", false).GetSnapshot());
        cache.Insert(MakeResult("b", "SELECT 2"), MakeSnapshot(8_MB), 0);
        UNIT_ASSERT(cache.LookupByUid("b", false).GetPlan());

        // A replacement first gives back the share of the snapshot it replaces.
        cache.Replace(MakeResult("a", "SELECT 1"), MakeSnapshot(64_MB), 0);
        UNIT_ASSERT(cache.LookupByUid("a", false).GetSnapshot());

        // A dropped snapshot gives its bytes back.
        UNIT_ASSERT(cache.EraseByUid("a"));
        cache.Insert(MakeResult("c", "SELECT 3"), MakeSnapshot(64_MB), 0);
        UNIT_ASSERT(cache.LookupByUid("c", false).GetSnapshot());
    }

    Y_UNIT_TEST(ReplaceMovesUidToAnOccupiedQueryKey) {
        TClockFixture clock;
        auto cache = CreateKqpQueryCache(true, 2, TDuration::Zero());
        cache->Insert(MakeResult("a", "SELECT 1"), MakeSnapshot(4_MB), 1);
        cache->Insert(MakeResult("b", "SELECT 2"), MakeSnapshot(60_MB), 9);

        auto snapshot = MakeSnapshot(64_MB);
        cache->Replace(MakeResult("a", "SELECT 2"), snapshot, 2);
        UNIT_ASSERT(!cache->LookupByQuery(MakeQueryId("SELECT 1"), false));
        UNIT_ASSERT(!cache->LookupByUid("b", false));
        auto record = cache->LookupByQuery(MakeQueryId("SELECT 2"), false);
        UNIT_ASSERT(record.GetSnapshot());
        UNIT_ASSERT_EQUAL(record.GetSnapshot()->Snapshot, snapshot);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSnapshot()->Info->Uid, "a");
        UNIT_ASSERT_VALUES_EQUAL(Row(*cache, "a").GetAccessCount(), 3);
        UNIT_ASSERT_EQUAL(SnapshotUids(*cache), (TVector<TString>{"a"}));
        UNIT_ASSERT_VALUES_EQUAL(cache->Bytes(), 64_MB);
    }

    // V1 stores plans only and leaves the access count to the session.
    Y_UNIT_TEST(V1IgnoresSnapshotsAndAccesses) {
        TClockFixture clock;
        TLookupFixture lookup;
        auto cacheHolder = CreateKqpQueryCache(/*compileCacheV2=*/false, /*size=*/10, TDuration::Zero());
        TKqpQueryCache& cache = *cacheHolder;
        UNIT_ASSERT(!cache.SupportsSnapshots());
        UNIT_ASSERT(!cache.CountsAccesses());

        auto compiled = MakeResult("a", "SELECT 1");
        cache.Insert(compiled, MakeSnapshot(10), /*accesses=*/5);
        UNIT_ASSERT_EQUAL(cache.FindByUid("a", false), compiled);
        UNIT_ASSERT(lookup.ByText(cache, "SELECT 1", "user@builtin", /*plansOnly=*/true).GetPlan());
        UNIT_ASSERT_VALUES_EQUAL(Row(cache, "a").GetAccessCount(), 0);
    }
}

} // namespace NKikimr::NKqp
