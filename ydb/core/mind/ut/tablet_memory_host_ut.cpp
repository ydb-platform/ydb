#include <ydb/core/mind/tablet_memory_host.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NKikimr::NMemory {

namespace {

TActorId MakeExecutor(ui32 id) {
    return TActorId(1, 0, id, 0);
}

TTabletMemoryHost::TReportUpdate Report(ui64 used, ui64 demand, ui64 reclaimable) {
    return {.Used = used, .Demand = demand, .Reclaimable = reclaimable};
}

}

Y_UNIT_TEST_SUITE(TTabletMemoryHost) {

Y_UNIT_TEST(SplitsTheReportIntoStateAndElasticPart) {
    TTabletMemoryHost host;
    const auto first = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40));
    UNIT_ASSERT(first.SumsChanged);
    UNIT_ASSERT(first.NewSlot);
    // The same tablet reporting again is not a new slot
    UNIT_ASSERT(!host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40)).NewSlot);

    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 60u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
    // The elastic demand is what the tablet would hold beyond its state
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 100u);

    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, 60u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Reclaimable, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Used, 40u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Demand, 100u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Reclaimable, 40u);
}

Y_UNIT_TEST(ClampsAReportThatBreaksItsInvariants) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 10, 500));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 100u);
}

Y_UNIT_TEST(AnOmittedFieldKeepsItsLastValue) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40));

    // One metrics tick carried only the used bytes
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Used = 120});
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 80u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
}

Y_UNIT_TEST(ALeavingTabletTakesItsReportOutOfTheSums) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::KeyValue, Report(30, 30, 0));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 90u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 2u);

    UNIT_ASSERT(host.Forget({1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 30u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 1u);

    UNIT_ASSERT(!host.Forget({1, 0}));
    UNIT_ASSERT(host.Forget({2, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 0u);
}

Y_UNIT_TEST(FollowersAndLeadersKeepSeparateSlots) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 0));
    host.SetReport({1, 1}, MakeExecutor(2), TTabletTypes::DataShard, Report(50, 50, 0));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 150u);
}

Y_UNIT_TEST(ElasticLimitWithEqualUsedAndDemand) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 30));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 10));
    // A tablet with no elastic part is not in the split
    host.SetReport({3, 0}, MakeExecutor(3), TTabletTypes::DataShard, Report(100, 100, 0));

    auto shares = host.ApplyElasticLimit(40);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    TMap<TActorId, ui64> byExecutor;
    for (const auto& share : shares) {
        byExecutor[share.Executor] = *share.Bytes;
    }
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(1)], 30u);
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(2)], 10u);

    // Only the tablets whose share changed are reported again
    UNIT_ASSERT(host.ApplyElasticLimit(40).empty());
    UNIT_ASSERT_VALUES_EQUAL(host.ApplyElasticLimit(20).size(), 2u);

    // Nothing elastic left: the shares that were handed out are taken back
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Reclaimable = 0});
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, {.Reclaimable = 0});
    shares = host.ApplyElasticLimit(20);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    for (const auto& share : shares) {
        UNIT_ASSERT(!share.Bytes);
    }
}

Y_UNIT_TEST(InitialZeroElasticLimitIsDelivered) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    const auto shares = host.ApplyElasticLimit(0);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(shares.front().Executor, MakeExecutor(1));
    UNIT_ASSERT(shares.front().Bytes && *shares.front().Bytes == 0u);
    UNIT_ASSERT(host.ApplyElasticLimit(0).empty());
}

Y_UNIT_TEST(ZeroReclaimableRevokesOnlyThatTabletsPreviousShare) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 40));
    UNIT_ASSERT_VALUES_EQUAL(host.ApplyElasticLimit(80).size(), 2u);
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 0));
    const auto shares = host.ApplyElasticLimit(40);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(shares.front().Executor, MakeExecutor(1));
    UNIT_ASSERT(!shares.front().Bytes);
    UNIT_ASSERT(host.ApplyElasticLimit(40).empty());
}

Y_UNIT_TEST(ReplacingExecutorReplaysItsShare) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    UNIT_ASSERT_VALUES_EQUAL(host.ApplyElasticLimit(0).size(), 1u);
    const auto change = host.SetReport({1, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 40));
    UNIT_ASSERT(!change.NewSlot);
    UNIT_ASSERT(!change.SumsChanged);
    UNIT_ASSERT(change.ExecutorChanged);
    const auto shares = host.ApplyElasticLimit(0);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(shares.front().Executor, MakeExecutor(2));
    UNIT_ASSERT(shares.front().Bytes && *shares.front().Bytes == 0u);
}

Y_UNIT_TEST(ElasticSharesDoNotExceedTheLimit) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 3));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 7));
    const auto shares = host.ApplyElasticLimit(Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    UNIT_ASSERT(*shares[0].Bytes <= Max<ui64>() - *shares[1].Bytes);
}

Y_UNIT_TEST(HalfRangeColdDemandsKeepTheirBudgetShares) {
    constexpr ui64 halfRange = ui64(1) << 63;
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(0, halfRange, 0));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(0, halfRange, 0));

    // Check the report before allocation so wrapped demand fails without dividing by zero.
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Demand, Max<ui64>());
    const auto shares = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    for (const auto& share : shares) {
        UNIT_ASSERT(share.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(*share.Bytes, 50u);
    }
    const auto zeroShares = host.ApplyElasticLimit(0);
    UNIT_ASSERT_VALUES_EQUAL(zeroShares.size(), 2u);
    for (const auto& share : zeroShares) {
        UNIT_ASSERT(share.Bytes && *share.Bytes == 0u);
    }
}

Y_UNIT_TEST(OverflowingDemandPreservesTheWeightedBudget) {
    constexpr ui64 quarterRange = ui64(1) << 62;
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(0, 3 * quarterRange, 0));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(0, 2 * quarterRange, 0));
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Demand, Max<ui64>());

    const auto shares = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    TMap<TActorId, ui64> byExecutor;
    for (const auto& share : shares) {
        UNIT_ASSERT(share.Bytes);
        byExecutor[share.Executor] = *share.Bytes;
    }
    // The exact demand ratio is 3:2 even though its total exceeds ui64.
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(1)], 60u);
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(2)], 40u);
    const auto maximumShares = host.ApplyElasticLimit(Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(maximumShares.size(), 2u);
    UNIT_ASSERT(maximumShares[0].Bytes && maximumShares[1].Bytes);
    UNIT_ASSERT(*maximumShares[0].Bytes <= Max<ui64>() - *maximumShares[1].Bytes);
}

Y_UNIT_TEST(SaturatedDemandStillSignalsChangesAndRecoversExactly) {
    constexpr ui64 halfRange = ui64(1) << 63;
    TTabletMemoryHost host;
    for (ui32 i = 1; i <= 3; ++i) {
        host.SetReport({i, 0}, MakeExecutor(i), TTabletTypes::DataShard, Report(0, halfRange, 0));
    }
    TMap<TActorId, ui64> delivered;
    const auto apply = [&] {
        const auto shares = host.ApplyElasticLimit(600);
        for (const auto& share : shares) {
            UNIT_ASSERT(share.Bytes);
            delivered[share.Executor] = *share.Bytes;
        }
        ui64 total = 0;
        for (const auto& [executor, bytes] : delivered) {
            UNIT_ASSERT(bytes <= 600u - total);
            total += bytes;
        }
        return shares.size();
    };
    const auto& sums = host.GetSums();
    // Assert the report first so the original host fails before unsafe allocation.
    UNIT_ASSERT_VALUES_EQUAL(sums.ElasticDemand, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(apply(), 3u);
    for (const auto& [executor, bytes] : delivered) {
        UNIT_ASSERT_VALUES_EQUAL(bytes, 200u);
    }
    const auto before = sums;
    const auto replacement = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Demand = Max<ui64>()});
    UNIT_ASSERT(replacement.SumsChanged);
    UNIT_ASSERT(replacement.SharesChanged);
    UNIT_ASSERT(sums == before);
    UNIT_ASSERT_VALUES_EQUAL(apply(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(1)], 299u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(2)], 150u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(3)], 150u);
    const auto unchanged = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Demand = Max<ui64>()});
    UNIT_ASSERT(!unchanged.SumsChanged);
    UNIT_ASSERT(!unchanged.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(apply(), 0u);

    UNIT_ASSERT(host.Forget({2, 0}));
    delivered.erase(MakeExecutor(2));
    UNIT_ASSERT(sums == before);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(apply(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(1)], 399u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(3)], 200u);
    UNIT_ASSERT(host.Forget({3, 0}));
    delivered.erase(MakeExecutor(3));
    UNIT_ASSERT_VALUES_EQUAL(sums.ElasticDemand, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(apply(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(1)], 600u);
    const auto recovery = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Demand = halfRange + 10});
    UNIT_ASSERT(recovery.SumsChanged && recovery.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(sums.ElasticDemand, halfRange + 10);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Demand, halfRange + 10);
    UNIT_ASSERT_VALUES_EQUAL(apply(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(delivered[MakeExecutor(1)], 600u);
    UNIT_ASSERT(host.Forget({1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(sums.ElasticDemand, 0u);
    UNIT_ASSERT(!host.Forget({1, 0}));
}

Y_UNIT_TEST(RetypingAReportSignalsRemovalAndWithdrawsItsShare) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40));
    const auto initial = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(initial.size(), 1u);
    UNIT_ASSERT(initial.front().Bytes && *initial.front().Bytes == 100u);

    const auto removed = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::KeyValue, Report(0, 0, 0));
    UNIT_ASSERT(removed.SumsChanged);
    UNIT_ASSERT(removed.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 0u);
    const auto withdrawal = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(withdrawal.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(withdrawal.front().Executor, MakeExecutor(1));
    UNIT_ASSERT(!withdrawal.front().Bytes);

    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::KeyValue, Report(100, 160, 40));
    UNIT_ASSERT_VALUES_EQUAL(host.ApplyElasticLimit(100).size(), 1u);
    const auto migrated = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40));
    UNIT_ASSERT(!migrated.SumsChanged);
    UNIT_ASSERT(!migrated.SharesChanged);
    UNIT_ASSERT(host.ApplyElasticLimit(100).empty());
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    host.UpdateCounters(counters);
    const TString dataShard = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::DataShard) << "/";
    const TString keyValue = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::KeyValue) << "/";
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Used")->Val(), 100u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Demand")->Val(), 160u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Reclaimable")->Val(), 40u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Used")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Demand")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Reclaimable")->Val(), 0u);
}

Y_UNIT_TEST(SaturatedStateRecoversAfterReplacementAndRemoval) {
    constexpr ui64 halfRange = ui64(1) << 63;
    TTabletMemoryHost host;
    for (ui32 i = 1; i <= 3; ++i) {
        host.SetReport({i, 0}, MakeExecutor(i), TTabletTypes::DataShard, Report(halfRange, halfRange, 0));
    }
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Demand, Max<ui64>());
    const auto replacement = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard,
        Report(halfRange + 10, halfRange + 10, 0));
    UNIT_ASSERT(replacement.SumsChanged);
    UNIT_ASSERT(!replacement.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, Max<ui64>());
    UNIT_ASSERT(host.Forget({2, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, Max<ui64>());
    UNIT_ASSERT(host.Forget({3, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, halfRange + 10);
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, halfRange + 10);
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Reclaimable, 0u);
}

Y_UNIT_TEST(AnEmptyCacheReceivesBudgetToRecover) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 200, 0));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 100));
    const auto shares = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    for (const auto& share : shares) {
        UNIT_ASSERT(share.Bytes && *share.Bytes == 50u);
    }
    // Changing non-elastic state while preserving elastic demand needs no redistribution.
    const auto update = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(200, 300, 0));
    UNIT_ASSERT(update.SumsChanged);
    UNIT_ASSERT(!update.SharesChanged);
    UNIT_ASSERT(host.ApplyElasticLimit(100).empty());
}

Y_UNIT_TEST(TinyEligibilityOscillationsDoNotRepeatTheFullFanout) {
    constexpr ui64 megabyte = 1 << 20;
    constexpr ui64 limit = 128 * megabyte;
    TTabletMemoryHost host;
    for (ui32 i = 1; i <= 8; ++i) {
        host.SetReport({i, 0}, MakeExecutor(i), TTabletTypes::DataShard, Report(megabyte, 2 * megabyte, 0));
    }
    host.SetReport({9, 0}, MakeExecutor(9), TTabletTypes::DataShard, Report(100, 100, 0));
    TMap<TActorId, ui64> delivered;
    const auto apply = [&](ui64 budget) {
        auto changes = host.ApplyElasticLimit(budget);
        for (const auto& change : changes) {
            delivered[change.Executor] = change.Bytes.value_or(0);
        }
        ui64 total = 0;
        for (const auto& [executor, bytes] : delivered) {
            total += bytes;
        }
        UNIT_ASSERT_C(total <= budget, "delivered allocations exceed the budget");
        return changes;
    };
    UNIT_ASSERT_VALUES_EQUAL(apply(limit).size(), 8u);
    for (ui32 i = 0; i != 64; ++i) {
        host.SetReport({9, 0}, MakeExecutor(9), TTabletTypes::DataShard, Report(100, 101, 0));
        const auto grants = apply(limit);
        // The first entry requires eight small reductions. Later cycles only
        // grant/revoke the entrant; suppressed growth retains the reduced shares.
        UNIT_ASSERT_VALUES_EQUAL(grants.size(), i == 0 ? 9u : 1u);
        host.SetReport({9, 0}, MakeExecutor(9), TTabletTypes::DataShard, Report(100, 100, 0));
        const auto withdrawals = apply(limit);
        UNIT_ASSERT_VALUES_EQUAL(withdrawals.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(withdrawals.front().Executor, MakeExecutor(9));
        UNIT_ASSERT(!withdrawals.front().Bytes);
    }
    // Even a tiny reduction is mandatory for each allocated tablet.
    UNIT_ASSERT_VALUES_EQUAL(apply(limit - 32).size(), 8u);
}

Y_UNIT_TEST(ShareGrowthAccumulatesAgainstTheLastDelivery) {
    constexpr ui64 megabyte = 1 << 20;
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 200, 0));
    const auto initial = host.ApplyElasticLimit(128 * megabyte);
    UNIT_ASSERT_VALUES_EQUAL(initial.size(), 1u);
    UNIT_ASSERT(initial.front().Bytes);
    UNIT_ASSERT_VALUES_EQUAL(*initial.front().Bytes, 128 * megabyte);
    UNIT_ASSERT(host.ApplyElasticLimit(128 * megabyte + megabyte / 2).empty());
    UNIT_ASSERT(host.ApplyElasticLimit(128 * megabyte + megabyte / 4).empty());
    const auto growth = host.ApplyElasticLimit(129 * megabyte);
    UNIT_ASSERT_VALUES_EQUAL(growth.size(), 1u);
    UNIT_ASSERT(growth.front().Bytes);
    UNIT_ASSERT_VALUES_EQUAL(*growth.front().Bytes, 129 * megabyte);
    const auto reduction = host.ApplyElasticLimit(129 * megabyte - 1);
    UNIT_ASSERT_VALUES_EQUAL(reduction.size(), 1u);
    UNIT_ASSERT(reduction.front().Bytes);
    UNIT_ASSERT_VALUES_EQUAL(*reduction.front().Bytes, 129 * megabyte - 1);
    UNIT_ASSERT(host.ApplyElasticLimit(129 * megabyte).empty());
    // A replacement must receive an allocation even if growth was suppressed.
    host.SetReport({1, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 200, 0));
    const auto replay = host.ApplyElasticLimit(129 * megabyte);
    UNIT_ASSERT_VALUES_EQUAL(replay.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(replay.front().Executor, MakeExecutor(2));
    UNIT_ASSERT(replay.front().Bytes);
    UNIT_ASSERT_VALUES_EQUAL(*replay.front().Bytes, 129 * megabyte);
}

Y_UNIT_TEST(SmallAllocationsCanRecoverBelowTheAbsoluteGrowthThreshold) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 200, 0));
    const auto expect = [&](ui64 budget, ui64 value) {
        const auto shares = host.ApplyElasticLimit(budget);
        UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1u);
        UNIT_ASSERT(shares.front().Bytes);
        UNIT_ASSERT_VALUES_EQUAL(*shares.front().Bytes, value);
    };
    expect(0, 0);
    expect(1, 1);
    expect(2, 2);
    expect(1 << 19, 1 << 19); // A meaningful relative increase below 1 MiB.
    UNIT_ASSERT(host.ApplyElasticLimit((1 << 19) + 1).empty());
    expect((1 << 19) - 1, (1 << 19) - 1);
    expect(0, 0);
    expect(1, 1);
}

Y_UNIT_TEST(PerTypeSensorsFollowTheSlots) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 120, 20));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(200, 200, 0));
    host.SetReport({3, 0}, MakeExecutor(3), TTabletTypes::KeyValue, Report(10, 10, 0));

    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    host.UpdateCounters(counters);
    const TString dataShard = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::DataShard) << "/";
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Used")->Val(), 300);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Demand")->Val(), 320);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Reclaimable")->Val(), 20);

    host.Forget({1, 0});
    host.UpdateCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Used")->Val(), 200);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Reclaimable")->Val(), 0);
}

Y_UNIT_TEST(PerTypeSensorsFallToZeroWhenTheLastTabletOfATypeLeaves) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 120, 20));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::KeyValue, Report(50, 50, 10));

    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    host.UpdateCounters(counters);
    const TString keyValue = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::KeyValue) << "/";
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Used")->Val(), 50);

    // The row of a type that lost its last tablet stays, so the sensors are published as 0 instead of freezing
    UNIT_ASSERT(host.Forget({2, 0}));
    host.UpdateCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Used")->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Demand")->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(keyValue + "Reclaimable")->Val(), 0);
}

Y_UNIT_TEST(SaturatedTypeGaugesRecoverAfterReplacementAndRemoval) {
    TTabletMemoryHost host;
    for (ui32 i = 1; i <= 3; ++i) {
        host.SetReport({i, 0}, MakeExecutor(i), TTabletTypes::DataShard,
            Report(Max<ui64>(), Max<ui64>(), Max<ui64>()));
    }
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    const TString dataShard = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::DataShard) << "/";
    const auto expect = [&](const TString& prefix, TAtomicBase used, TAtomicBase demand, TAtomicBase reclaimable) {
        host.UpdateCounters(counters);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), used);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Demand")->Val(), demand);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Reclaimable")->Val(), reclaimable);
    };
    const auto gaugeMax = Max<::NMonitoring::TDeprecatedCounter::TValueBase>();
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Used, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Reclaimable, Max<ui64>());
    expect(dataShard, gaugeMax, gaugeMax, gaugeMax);
    UNIT_ASSERT(host.Forget({2, 0}));
    expect(dataShard, gaugeMax, gaugeMax, gaugeMax);
    const auto replacement = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 120, 20));
    UNIT_ASSERT(replacement.SumsChanged);
    UNIT_ASSERT(replacement.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, Max<ui64>());
    expect(dataShard, gaugeMax, gaugeMax, gaugeMax);
    UNIT_ASSERT(host.Forget({3, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 80u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 20u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 40u);
    expect(dataShard, 100, 120, 20);

    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::KeyValue, Report(200, 240, 40));
    expect(dataShard, 0, 0, 0);
    const TString keyValue = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::KeyValue) << "/";
    expect(keyValue, 200, 240, 40);
}

Y_UNIT_TEST(ClearResetsOverflowedAccountingBeforeNewReports) {
    TTabletMemoryHost host;
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    for (ui32 i = 1; i <= 3; ++i) {
        host.SetReport({i, 0}, MakeExecutor(i), TTabletTypes::KeyValue,
            Report(Max<ui64>(), Max<ui64>(), Max<ui64>()));
    }
    host.UpdateCounters(counters);
    const TString prefix = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::KeyValue) << "/";
    const auto gaugeMax = Max<::NMonitoring::TDeprecatedCounter::TValueBase>();
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), gaugeMax);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Demand")->Val(), gaugeMax);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Reclaimable")->Val(), gaugeMax);
    host.Clear(counters);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 0u);
    UNIT_ASSERT(host.ApplyElasticLimit(100).empty());
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Demand")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Reclaimable")->Val(), 0u);

    const auto update = host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::KeyValue, Report(100, 120, 20));
    UNIT_ASSERT(update.NewSlot && update.SumsChanged && update.SharesChanged);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 80u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 20u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 40u);
    host.UpdateCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), 100u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Demand")->Val(), 120u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Reclaimable")->Val(), 20u);
    const auto shares = host.ApplyElasticLimit(100);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 1u);
    UNIT_ASSERT(shares.front().Bytes && *shares.front().Bytes == 100u);
}

Y_UNIT_TEST(ClearResetsPublishedTypeGauges) {
    TTabletMemoryHost host;
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::KeyValue, Report(100, 120, 20));
    host.UpdateCounters(counters);
    const TString prefix = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::KeyValue) << "/";
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), 100u);
    host.Clear(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Used")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Demand")->Val(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(prefix + "Reclaimable")->Val(), 0u);
}

Y_UNIT_TEST(ClearDropsEverything) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    host.Clear();
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 0u);
    UNIT_ASSERT(host.GetExecutors().empty());
}

}

}
