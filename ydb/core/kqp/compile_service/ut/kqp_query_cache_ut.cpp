#include <ydb/core/kqp/compile_service/kqp_compile_service.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

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
        uids.push_back(entry.CompileResult->Uid);
    }
    Sort(uids);
    return uids;
}

TInstant LastTouched(const TKqpQueryCache& cache, const TString& uid) {
    for (const auto& entry : cache.GetSnapshot()) {
        if (entry.CompileResult->Uid == uid) {
            return entry.LastTouched;
        }
    }
    return TInstant::Zero();
}

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
        UNIT_ASSERT(cache.Insert(second));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 1);
        UNIT_ASSERT(!cache.FindByUid("u1", false));
        UNIT_ASSERT_EQUAL(cache.FindByQuery(MakeQueryId("SELECT 1"), false), second);
        UNIT_ASSERT_VALUES_EQUAL(cache.Bytes(), PlanBytes(second));

        // Reinserting the same uid replaces the entry without losing the text mapping.
        auto third = MakeResult("u2", "SELECT 1", 50);
        UNIT_ASSERT(cache.Insert(third));
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

} // namespace NKikimr::NKqp
