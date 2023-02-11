#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/actors/helpers/selfping_actor.h>
#include <util/stream/null.h>
#include <util/datetime/cputimer.h>
#include "hive_impl.h"
#include "balancer.h"

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

#ifdef address_sanitizer_enabled
#define SANITIZER_TYPE address
#endif
#ifdef memory_sanitizer_enabled
#define SANITIZER_TYPE memory
#endif
#ifdef thread_sanitizer_enabled
#define SANITIZER_TYPE thread
#endif

using namespace NKikimr;
using namespace NHive;

using duration_nano_t = std::chrono::duration<ui64, std::nano>;
using duration_t = std::chrono::duration<double>;

duration_t GetBasePerformance() {
    duration_nano_t accm{};
    for (int i = 0; i < 1000000; ++i) {
        accm += duration_nano_t(NActors::MeasureTaskDurationNs());
    }
    return std::chrono::duration_cast<duration_t>(accm);
}

static double BASE_PERF = GetBasePerformance().count();

Y_UNIT_TEST_SUITE(THiveImplTest) {
    Y_UNIT_TEST(BootQueueSpeed) {
        TBootQueue bootQueue;
        static constexpr ui64 NUM_TABLETS = 1000000;

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());
        std::unordered_map<ui64, TLeaderTabletInfo> tablets;
        TProfileTimer timer;

        for (ui64 i = 0; i < NUM_TABLETS; ++i) {
            TLeaderTabletInfo& tablet = tablets.emplace(std::piecewise_construct, std::tuple<TTabletId>(i), std::tuple<TTabletId, THive&>(i, hive)).first->second;
            tablet.Weight = RandomNumber<double>();
            bootQueue.AddToBootQueue(tablet);
        }

        double passed = timer.Get().SecondsFloat();
        Ctest << "Create = " << passed << Endl;
#ifndef SANITIZER_TYPE
#ifndef NDEBUG
        UNIT_ASSERT(passed < 3 * BASE_PERF);
#else
        UNIT_ASSERT(passed < 1 * BASE_PERF);
#endif
#endif
        timer.Reset();

        double maxP = 100;

        while (!bootQueue.BootQueue.empty()) {
            auto record = bootQueue.PopFromBootQueue();
            UNIT_ASSERT(record.Priority <= maxP);
            maxP = record.Priority;
            auto itTablet = tablets.find(record.TabletId.first);
            if (itTablet != tablets.end()) {
                bootQueue.AddToWaitQueue(itTablet->second);
            }
        }

        passed = timer.Get().SecondsFloat();
        Ctest << "Process = " << passed << Endl;
#ifndef SANITIZER_TYPE
#ifndef NDEBUG
        UNIT_ASSERT(passed < 10 * BASE_PERF);
#else
        UNIT_ASSERT(passed < 1 * BASE_PERF);
#endif
#endif

        timer.Reset();

        bootQueue.MoveFromWaitQueueToBootQueue();

        passed = timer.Get().SecondsFloat();
        Ctest << "Move = " << passed << Endl;
#ifndef SANITIZER_TYPE
#ifndef NDEBUG
        UNIT_ASSERT(passed < 2 * BASE_PERF);
#else
        UNIT_ASSERT(passed < 0.1 * BASE_PERF);
#endif
#endif
    }

    Y_UNIT_TEST(BalancerSpeedAndDistribution) {
        static constexpr ui64 NUM_TABLETS = 1000000;
        static constexpr ui64 NUM_BUCKETS = 16;

        auto CheckSpeedAndDistribution = [](
            std::unordered_map<ui64, TLeaderTabletInfo>& allTablets,
            std::function<void(std::vector<TTabletInfo*>&)> func) -> void {

            std::vector<TTabletInfo*> tablets;
            for (auto& [id, tab] : allTablets) {
                tablets.emplace_back(&tab);
            }

            TProfileTimer timer;

            func(tablets);

            double passed = timer.Get().SecondsFloat();

            Ctest << "Time=" << passed << Endl;
#ifndef SANITIZER_TYPE
#ifndef NDEBUG
            UNIT_ASSERT(passed < 1 * BASE_PERF);
#else
            UNIT_ASSERT(passed < 0.7 * BASE_PERF);
#endif
#endif
            std::vector<double> buckets(NUM_BUCKETS, 0);
            size_t revs = 0;
            double prev = 0;
            for (size_t n = 0; n < tablets.size(); ++n) {
                buckets[n / (NUM_TABLETS / NUM_BUCKETS)] += tablets[n]->Weight;
                if (n != 0 && tablets[n]->Weight >= prev) {
                    ++revs;
                }
                prev = tablets[n]->Weight;
            }

            Ctest << "Indirection=" << revs * 100 / NUM_TABLETS << "%" << Endl;

            Ctest << "Distribution=";
            for (double v : buckets) {
                Ctest << Sprintf("%.2f", v) << " ";
            }
            Ctest << Endl;

            /*char bars[] = " ▁▂▃▄▅▆▇█";
            double mx = *std::max_element(buckets.begin(), buckets.end());
            for (double v : buckets) {
                Ctest << bars[int(ceil(v / mx)) * 8];
            }
            Ctest << Endl;*/

            /*prev = NUM_TABLETS;
            for (double v : buckets) {
                UNIT_ASSERT(v < prev);
                prev = v;
            }*/

            std::sort(tablets.begin(), tablets.end());
            auto unique_end = std::unique(tablets.begin(), tablets.end());
            Ctest << "Duplicates=" << tablets.end() - unique_end << Endl;

            UNIT_ASSERT(tablets.end() - unique_end == 0);
        };

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());
        std::unordered_map<ui64, TLeaderTabletInfo> allTablets;

        for (ui64 i = 0; i < NUM_TABLETS; ++i) {
            TLeaderTabletInfo& tablet = allTablets.emplace(std::piecewise_construct, std::tuple<TTabletId>(i), std::tuple<TTabletId, THive&>(i, hive)).first->second;
            tablet.Weight = RandomNumber<double>();
        }

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>);

        //Ctest << "HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM" << Endl;
        //CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>);

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>);

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_RANDOM" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>);
    }
}
