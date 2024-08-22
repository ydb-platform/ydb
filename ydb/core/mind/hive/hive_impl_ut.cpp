#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/helpers/selfping_actor.h>
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
            bootQueue.EmplaceToBootQueue(tablet);
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
            auto itTablet = tablets.find(record.TabletId);
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
        UNIT_ASSERT(passed < 2 * BASE_PERF);
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
            std::function<void(std::vector<TTabletInfo*>::iterator, std::vector<TTabletInfo*>::iterator, EResourceToBalance)> func,
            EResourceToBalance resource) -> void {

            std::vector<TTabletInfo*> tablets;
            for (auto& [id, tab] : allTablets) {
                tablets.emplace_back(&tab);
            }

            TProfileTimer timer;

            func(tablets.begin(), tablets.end(), resource);

            double passed = timer.Get().SecondsFloat();

            Ctest << "Time=" << passed << Endl;
#ifndef SANITIZER_TYPE
#ifndef NDEBUG
            UNIT_ASSERT(passed < 1 * BASE_PERF);
#else
            UNIT_ASSERT(passed < 1 * BASE_PERF);
#endif
#endif
            std::vector<double> buckets(NUM_BUCKETS, 0);
            size_t revs = 0;
            double prev = 0;
            for (size_t n = 0; n < tablets.size(); ++n) {
                double weight = tablets[n]->GetWeight(resource);
                buckets[n / (NUM_TABLETS / NUM_BUCKETS)] += weight;
                if (n != 0 && weight >= prev) {
                    ++revs;
                }
                prev = weight;
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
            tablet.GetMutableResourceValues().SetMemory(RandomNumber<double>());
        }

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_HEAVIEST>, EResourceToBalance::Memory);

        //Ctest << "HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM" << Endl;
        //CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_OLD_WEIGHTED_RANDOM>);

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_WEIGHTED_RANDOM>, EResourceToBalance::Memory);

        Ctest << "HIVE_TABLET_BALANCE_STRATEGY_RANDOM" << Endl;
        CheckSpeedAndDistribution(allTablets, BalanceTablets<NKikimrConfig::THiveConfig::HIVE_TABLET_BALANCE_STRATEGY_RANDOM>, EResourceToBalance::Memory);
    }

    Y_UNIT_TEST(TestShortTabletTypes) {
        // This asserts we don't have different tablet types with same short name
        // In a world with constexpr maps this could have been a static_assert...
        UNIT_ASSERT_VALUES_EQUAL(TABLET_TYPE_SHORT_NAMES.size(), TABLET_TYPE_BY_SHORT_NAME.size());
    }

    Y_UNIT_TEST(TestStDev) {
        using TSingleResource = std::tuple<double>;

        TVector<TSingleResource> values(100, 50.0 / 1'000'000);
        values.front() = 51.0 / 1'000'000;

        double stDev1 = std::get<0>(GetStDev(values));

        std::swap(values.front(), values.back());

        double stDev2 = std::get<0>(GetStDev(values));

        double expectedStDev = sqrt(0.9703) / 1'000'000;

        UNIT_ASSERT_DOUBLES_EQUAL(expectedStDev, stDev1, 1e-6);
        UNIT_ASSERT_VALUES_EQUAL(stDev1, stDev2);
    }
}

Y_UNIT_TEST_SUITE(TCutHistoryRestrictions) {
    class TTestHive : public THive {
    public:
        TTestHive(TTabletStorageInfo *info, const TActorId &tablet) : THive(info, tablet) {}

        template<typename F>
        void UpdateConfig(F func) {
            func(ClusterConfig);
            BuildCurrentConfig();
        }
    };

    Y_UNIT_TEST(BasicTest) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetCutHistoryAllowList("DataShard,Coordinator");
            config.SetCutHistoryDenyList("GraphShard");
        });
        UNIT_ASSERT(hive.IsCutHistoryAllowed(TTabletTypes::DataShard));
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::GraphShard));
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::Hive));
    }

    Y_UNIT_TEST(EmptyAllowList) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetCutHistoryAllowList("");
            config.SetCutHistoryDenyList("GraphShard");
        });
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::GraphShard));
        UNIT_ASSERT(hive.IsCutHistoryAllowed(TTabletTypes::Hive));
    }

    Y_UNIT_TEST(EmptyDenyList) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetCutHistoryAllowList("DataShard,Coordinator");
            config.SetCutHistoryDenyList("");
        });
        UNIT_ASSERT(hive.IsCutHistoryAllowed(TTabletTypes::DataShard));
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::GraphShard));
    }

    Y_UNIT_TEST(SameTabletInBothLists) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetCutHistoryAllowList("DataShard,Coordinator");
            config.SetCutHistoryDenyList("SchemeShard,DataShard");
        });
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::DataShard));
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::SchemeShard));
        UNIT_ASSERT(!hive.IsCutHistoryAllowed(TTabletTypes::Hive));
        UNIT_ASSERT(hive.IsCutHistoryAllowed(TTabletTypes::Coordinator));
    }

    Y_UNIT_TEST(BothListsEmpty) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        TTestHive hive(hiveStorage.Get(), TActorId());
        hive.UpdateConfig([](NKikimrConfig::THiveConfig& config) {
            config.SetCutHistoryAllowList("");
            config.SetCutHistoryDenyList("");
        });
        UNIT_ASSERT(hive.IsCutHistoryAllowed(TTabletTypes::DataShard));
    }
}
