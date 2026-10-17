#include <library/cpp/testing/unittest/registar.h>
#include "consumer_collection.h"

#include <atomic>
#include <thread>

namespace NKikimr::NMemory {

namespace {

TActorId Registrant(ui64 localId) {
    return TActorId(1, 0, localId, 0);
}

THashMap<TActorId, ui64> AsMap(const TVector<TConsumerShare>& shares) {
    THashMap<TActorId, ui64> result;
    for (const auto& share : shares) {
        UNIT_ASSERT_C(result.emplace(share.Registrant, share.Bytes).second, "duplicate registrant in shares");
    }
    return result;
}

void AssertReport(const TConsumerReport& report, ui64 used, ui64 demand, ui64 reclaimable) {
    UNIT_ASSERT_VALUES_EQUAL(report.Used, used);
    UNIT_ASSERT_VALUES_EQUAL(report.Demand, demand);
    UNIT_ASSERT_VALUES_EQUAL(report.Reclaimable, reclaimable);
}

}

Y_UNIT_TEST_SUITE(TConsumerCollectionTest) {

Y_UNIT_TEST(SumsRegistrants) {
    TConsumerCollection collection;

    auto c1 = collection.Register(Registrant(1));
    auto c2 = collection.Register(Registrant(2));
    auto c3 = collection.Register(Registrant(3));
    UNIT_ASSERT_VALUES_EQUAL(collection.GetRegistrantsCount(), 3);
    UNIT_ASSERT(c1.Get() != c2.Get() && c2.Get() != c3.Get() && c1.Get() != c3.Get());
    AssertReport(collection.GetTotal(), 0, 0, 0);

    c1->SetReport({.Used = 100, .Demand = 150, .Reclaimable = 40});
    c2->SetReport({.Used = 200, .Demand = 200, .Reclaimable = 0});
    c3->SetReport({.Used = 300, .Demand = 350, .Reclaimable = 300});
    AssertReport(collection.GetTotal(), 600, 700, 340);

    c1->SetReport({.Used = 50, .Demand = 60, .Reclaimable = 10});
    AssertReport(collection.GetTotal(), 550, 610, 310);

    UNIT_ASSERT(collection.Unregister(Registrant(2)));
    UNIT_ASSERT_VALUES_EQUAL(collection.GetRegistrantsCount(), 2);
    AssertReport(collection.GetTotal(), 350, 410, 310);

    UNIT_ASSERT(!collection.Unregister(Registrant(2)));
    AssertReport(collection.GetTotal(), 350, 410, 310);

    // A detached registrant's later reports change nothing
    c2->SetReport({.Used = 999, .Demand = 999, .Reclaimable = 999});
    AssertReport(collection.GetTotal(), 350, 410, 310);
}

Y_UNIT_TEST(ReRegisterHandsOutFreshObject) {
    TConsumerCollection collection;

    auto stale = collection.Register(Registrant(7));
    stale->SetConsumption(1000);
    AssertReport(collection.GetTotal(), 1000, 1000, 0);

    // The comeback gets a new object, so the old pointer no longer reaches the sum
    auto fresh = collection.Register(Registrant(7));
    UNIT_ASSERT(stale.Get() != fresh.Get());
    UNIT_ASSERT_VALUES_EQUAL(collection.GetRegistrantsCount(), 1);
    AssertReport(collection.GetTotal(), 0, 0, 0);

    fresh->SetConsumption(500);
    stale->SetConsumption(2000);
    AssertReport(collection.GetTotal(), 500, 500, 0);
}

Y_UNIT_TEST(ClampsEachEntry) {
    TConsumerCollection collection;

    auto degraded = collection.Register(Registrant(1));
    auto invalid = collection.Register(Registrant(2));

    degraded->SetConsumption(500);
    AssertReport(degraded->GetReport(), 500, 500, 0);

    invalid->SetReport({.Used = 100, .Demand = 50, .Reclaimable = 200});
    AssertReport(invalid->GetReport(), 100, 100, 100);
    AssertReport(collection.GetTotal(), 600, 600, 100);
}

Y_UNIT_TEST(LimitShareSingleRegistrantGetsWholeLimit) {
    TConsumerCollection collection;
    auto only = collection.Register(Registrant(1));

    // A cold single registrant with zero demand still gets the whole limit
    auto shares = AsMap(collection.ComputeLimitShares(1000));
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 1000);

    only->SetReport({.Used = 300, .Demand = 700, .Reclaimable = 0});
    shares = AsMap(collection.ComputeLimitShares(1000));
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 1000);

    // Demand above the limit still yields exactly the limit
    only->SetReport({.Used = 300, .Demand = 5000, .Reclaimable = 0});
    shares = AsMap(collection.ComputeLimitShares(1000));
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 1000);
}

Y_UNIT_TEST(LimitShareSplitsByDemandPlusSurplus) {
    TConsumerCollection collection;
    auto c1 = collection.Register(Registrant(1));
    auto c2 = collection.Register(Registrant(2));

    c1->SetReport({.Used = 100, .Demand = 300, .Reclaimable = 0});
    c2->SetReport({.Used = 200, .Demand = 200, .Reclaimable = 0});

    // Demands 300 + 200 fit into 600: proportional cut plus half of the 100 surplus each
    auto shares = AsMap(collection.ComputeLimitShares(600));
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 350);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(2)), 250);

    // Demands exceed 400: the 25-byte bootstrap slices plus the proportional cut of the remaining 350
    shares = AsMap(collection.ComputeLimitShares(400));
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 235);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(2)), 165);

    // An idle registrant still receives its surplus split so it can start growing
    c1->SetReport({});
    shares = AsMap(collection.ComputeLimitShares(600));
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 200);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(2)), 400);

    // With no demand at all the whole limit splits equally
    c2->SetReport({});
    shares = AsMap(collection.ComputeLimitShares(600));
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(1)), 300);
    UNIT_ASSERT_VALUES_EQUAL(shares.at(Registrant(2)), 300);
}

Y_UNIT_TEST(LimitShareColdRegistrantGrowsOutOfZero) {
    constexpr ui64 Limit = 1'000'000;
    TConsumerCollection collection;
    auto warm = collection.Register(Registrant(1));
    auto cold = collection.Register(Registrant(2));
    warm->SetConsumption(Limit);

    // Both obey their limits and report only Used, the way the existing caches do
    ui64 previous = 0;
    for (int tick = 0; tick < 60; ++tick) {
        auto shares = AsMap(collection.ComputeLimitShares(Limit));
        UNIT_ASSERT_LE(shares.at(Registrant(1)) + shares.at(Registrant(2)), Limit);
        if (tick < 10) {
            UNIT_ASSERT_GT(shares.at(Registrant(2)), previous);
        }
        previous = shares.at(Registrant(2));
        warm->SetConsumption(shares.at(Registrant(1)));
        cold->SetConsumption(shares.at(Registrant(2)));
    }

    // Two saturated caches converge to an equal split instead of freezing the first comer's advantage
    UNIT_ASSERT_GE(previous, Limit * 45 / 100);
    UNIT_ASSERT_LE(previous, Limit * 55 / 100);
}

Y_UNIT_TEST(ReleaseRequests) {
    TConsumerCollection collection;
    auto c1 = collection.Register(Registrant(1));
    auto c2 = collection.Register(Registrant(2));
    auto c3 = collection.Register(Registrant(3));
    c1->SetReport({.Used = 500, .Demand = 500, .Reclaimable = 300});
    c2->SetReport({.Used = 300, .Demand = 300, .Reclaimable = 100});
    c3->SetReport({.Used = 200, .Demand = 200, .Reclaimable = 0});

    UNIT_ASSERT(collection.ComputeReleaseRequests(1000).empty());
    UNIT_ASSERT(collection.ComputeReleaseRequests(1200).empty());

    auto requests = AsMap(collection.ComputeReleaseRequests(800));
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(1)), 150);
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(2)), 50);

    requests = AsMap(collection.ComputeReleaseRequests(600));
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(1)), 300);
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(2)), 100);

    // Excess above the total reclaimable stays capped at each registrant's own Reclaimable
    requests = AsMap(collection.ComputeReleaseRequests(0));
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(1)), 300);
    UNIT_ASSERT_VALUES_EQUAL(requests.at(Registrant(2)), 100);

    c1->SetReport({.Used = 500, .Demand = 500, .Reclaimable = 0});
    c2->SetReport({.Used = 300, .Demand = 300, .Reclaimable = 0});
    UNIT_ASSERT(collection.ComputeReleaseRequests(0).empty());
}

Y_UNIT_TEST(ConcurrentReports) {
    TConsumerCollection collection;

    constexpr size_t ReporterCount = 4;
    constexpr size_t Rounds = 200;
    constexpr ui64 FinalUsed = 3000;

    TVector<TIntrusivePtr<TRegistrantConsumer>> stable;
    for (size_t i = 0; i < ReporterCount; ++i) {
        stable.push_back(collection.Register(Registrant(i + 1)));
    }
    // Replaced victim objects stay alive: the victim writer may still hold one it loaded before a replacement
    TVector<TIntrusivePtr<TRegistrantConsumer>> victims{collection.Register(Registrant(100))};
    std::atomic<TRegistrantConsumer*> victim = victims.back().Get();
    std::atomic<ui64> victimWrites = 0;
    std::atomic<size_t> started = 0;
    std::atomic<bool> stop = false;

    TVector<std::thread> threads;
    for (size_t i = 0; i < ReporterCount; ++i) {
        threads.emplace_back([&, consumer = stable[i]] {
            started.fetch_add(1);
            for (ui64 iteration = 0; !stop.load(); ++iteration) {
                const ui64 used = iteration % FinalUsed;
                consumer->SetReport({.Used = used, .Demand = used + 2, .Reclaimable = used / 3});
            }
            consumer->SetReport({.Used = FinalUsed, .Demand = FinalUsed + 2, .Reclaimable = 0});
        });
    }
    threads.emplace_back([&] {
        started.fetch_add(1);
        for (ui64 iteration = 0; !stop.load(); ++iteration) {
            victim.load()->SetConsumption(iteration % FinalUsed);
            victimWrites.fetch_add(1);
        }
        victim.load()->SetConsumption(FinalUsed);
    });

    while (started.load() < threads.size()) {
        std::this_thread::yield();
    }
    // The writers run for the whole loop; each replacement is written to before the next round
    for (size_t round = 0; round < Rounds; ++round) {
        auto total = collection.GetTotal();
        UNIT_ASSERT_C(total.Used <= (ReporterCount + 1) * FinalUsed, "sum overflow, total.Used = " << total.Used);
        UNIT_ASSERT_C(total.Reclaimable <= total.Used, "clamp broken, total.Reclaimable = " << total.Reclaimable);
        UNIT_ASSERT_C(total.Demand >= total.Used, "clamp broken, total.Demand = " << total.Demand);
        collection.ComputeLimitShares(total.Used / 2);
        collection.ComputeReleaseRequests(total.Used / 2);
        UNIT_ASSERT(collection.Unregister(Registrant(100)));
        victims.push_back(collection.Register(Registrant(100)));
        const ui64 writesBefore = victimWrites.load();
        victim.store(victims.back().Get());
        while (victimWrites.load() < writesBefore + 2) {
            std::this_thread::yield();
        }
    }
    stop.store(true);
    for (auto& thread : threads) {
        thread.join();
    }

    // Every writer ends on a known report into its current object, so the sum is exact
    AssertReport(collection.GetTotal(), (ReporterCount + 1) * FinalUsed, ReporterCount * (FinalUsed + 2) + FinalUsed, 0);
}

}

}
