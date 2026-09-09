#include "hive_impl.h"
#include "ut_common.h"

#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NHive {
namespace {

class TScatterHive : public TTestHive {
public:
    using TTestHive::TTestHive;
    using THive::GetScatterBalancerSettings;
};

class TScatterNode : public TNodeInfo {
public:
    TScatterNode(TNodeId id, THive& hive)
        : TNodeInfo(id, hive)
    {
        Local = TActorId(id, "LOCAL");
        ResourceMaximumValues = {1'000'000, 1'000'000, 1'000'000, 1'000'000};
        std::get<NMetrics::EResource::Memory>(ResourceMaximumValues) = 1'000'000'000;
        SetAlive(true);
    }

    void SetAlive(bool alive) {
        VolatileState = alive ? EVolatileState::Connected : EVolatileState::Disconnected;
    }
};

class TScatterTablet : public TLeaderTabletInfo {
public:
    TScatterTablet(TTabletId id, THive& hive, TNodeInfo& node)
        : TLeaderTabletInfo(id, hive)
    {
        Type = TTabletTypes::Dummy;
        State = ETabletState::ReadyToWork;
        Node = &node;
        NodeId = node.Id;
        VolatileState = EVolatileState::TABLET_VOLATILE_STATE_RUNNING;
    }
};

void SetResource(TResourceRawValues& values, EResourceToBalance resource, i64 value) {
    switch (resource) {
        case EResourceToBalance::Counter: std::get<NMetrics::EResource::Counter>(values) = value; break;
        case EResourceToBalance::CPU: std::get<NMetrics::EResource::CPU>(values) = value; break;
        case EResourceToBalance::Memory: std::get<NMetrics::EResource::Memory>(values) = value * 1'000; break;
        case EResourceToBalance::Network: std::get<NMetrics::EResource::Network>(values) = value; break;
        case EResourceToBalance::ComputeResources: Y_ABORT("A concrete resource is required");
    }
}

void SetResource(TMetrics& values, EResourceToBalance resource, i64 value) {
    switch (resource) {
        case EResourceToBalance::Counter: values.Counter = value; break;
        case EResourceToBalance::CPU: values.CPU = value; break;
        case EResourceToBalance::Memory: values.Memory = value * 1'000; break;
        case EResourceToBalance::Network: values.Network = value; break;
        case EResourceToBalance::ComputeResources: Y_ABORT("A concrete resource is required");
    }
}

struct TScatterFixture {
    static TIntrusivePtr<TTabletStorageInfo> MakeStorage() {
        auto storage = MakeIntrusive<TTabletStorageInfo>();
        storage->TabletType = TTabletTypes::Hive;
        return storage;
    }

    TActorSystemStub ActorSystem;
    TIntrusivePtr<TTabletStorageInfo> Storage = MakeStorage();
    TScatterHive Hive;
    std::vector<std::unique_ptr<TScatterNode>> Nodes;
    std::vector<std::unique_ptr<TScatterTablet>> Tablets;
    const TInstant Now = TInstant::Seconds(10'000);

    TScatterFixture()
        : Hive(Storage.Get(), TActorId())
    {
        Hive.CurrentConfig.SetMinNodeUsageToBalance(.0175);
        Hive.CurrentConfig.SetMinCPUScatterToBalance(.5);
        Hive.CurrentConfig.SetMinMemoryScatterToBalance(.5);
        Hive.CurrentConfig.SetMinNetworkScatterToBalance(.5);
        Hive.CurrentConfig.SetMinCounterScatterToBalance(.5);
        Hive.CurrentConfig.SetMaxResourceCounter(1'000'000);
        Hive.CurrentConfig.SetTabletKickCooldownPeriod(600);
        Hive.CurrentConfig.SetUseTabletUsageEstimate(false);
        Hive.CurrentConfig.SetMaxMovementsOnAutoBalancer(100);
        Hive.CurrentConfig.SetBalancerInflight(4);
        Hive.CurrentConfig.SetContinueAutoBalancer(true);
    }

    TScatterNode& AddNode(EResourceToBalance resource, i64 usage, bool hasTablet = true) {
        Nodes.push_back(std::make_unique<TScatterNode>(Nodes.size() + 1, Hive));
        auto& node = *Nodes.back();
        SetResource(node.ResourceValues, resource, usage);
        if (hasTablet) {
            AddTablet(node, resource);
        }
        return node;
    }

    TScatterTablet& AddTablet(TScatterNode& node, EResourceToBalance resource) {
        Tablets.push_back(std::make_unique<TScatterTablet>(Tablets.size() + 1, Hive, node));
        auto& tablet = *Tablets.back();
        // A compute tablet consumes 1% of capacity, above the metric noise floors.
        // Memory uses a larger capacity so that the same percentages are meaningful.
        SetResource(tablet.GetMutableResourceValues(), resource,
            resource == EResourceToBalance::Counter ? 1 : 10'000);
        auto values = tablet.GetResourceCurrentValues();
        tablet.FilterRawValues(values);
        UNIT_ASSERT(TTabletInfo::ExtractResourceUsage(values, resource) > 0);
        node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING].insert(&tablet);
        return tablet;
    }

    std::optional<TBalancerSettings> Settings() const {
        std::vector<const TNodeInfo*> nodes;
        for (const auto& node : Nodes) {
            nodes.push_back(node.get());
        }
        return Hive.GetScatterBalancerSettings(nodes, Now);
    }
};

void AssertSources(const std::optional<TBalancerSettings>& settings, std::initializer_list<TNodeId> expected) {
    UNIT_ASSERT(settings);
    auto actual = settings->FilterNodeIds;
    std::sort(actual.begin(), actual.end());
    UNIT_ASSERT(actual == std::vector<TNodeId>(expected));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(THiveScatterBalancerTest) {
    Y_UNIT_TEST(SelectsOnlySourcesAboveThresholdFromMovableCohort) {
        TScatterFixture fixture;
        for (i64 percent = 1; percent <= 6; ++percent) {
            fixture.AddNode(EResourceToBalance::CPU, percent * 10'000);
        }
        fixture.Tablets.back()->BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_IGNORE;
        auto settings = fixture.Settings();
        AssertSources(settings, {4, 5});
        UNIT_ASSERT(settings->Type == EBalancerType::ScatterCPU);
        UNIT_ASSERT(settings->ResourceToBalance == EResourceToBalance::CPU);
        UNIT_ASSERT(settings->MinNodeUsage);
        UNIT_ASSERT_DOUBLES_EQUAL(*settings->MinNodeUsage, .035, 1e-12);
        UNIT_ASSERT_VALUES_EQUAL(settings->MaxMovements, 100);
        UNIT_ASSERT_VALUES_EQUAL(settings->MaxInFlight, 4);
        UNIT_ASSERT(settings->RecheckOnFinish);
    }

    Y_UNIT_TEST(UnmovableMaximumCannotTriggerBalancing) {
        TScatterFixture fixture;
        fixture.AddNode(EResourceToBalance::CPU, 30'000);
        fixture.AddNode(EResourceToBalance::CPU, 30'000);
        fixture.AddNode(EResourceToBalance::CPU, 900'000);
        fixture.Tablets.back()->BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_IGNORE;
        UNIT_ASSERT(!fixture.Settings());
    }

    Y_UNIT_TEST(BackgroundLoadWithoutTabletsIsNotAScatterSource) {
        TScatterFixture fixture;
        for (i64 percent = 1; percent <= 5; ++percent) {
            fixture.AddNode(EResourceToBalance::CPU, percent * 10'000);
        }
        auto& background = fixture.AddNode(EResourceToBalance::CPU, 0, false);
        TResourceRawValues total{};
        SetResource(total, EResourceToBalance::CPU, 60'000);
        for (int i = 0; i < 20; ++i) {
            background.AveragedResourceTotalValues.Push(total);
        }
        background.ResourceTotalValues = background.AveragedResourceTotalValues.GetValue();
        UNIT_ASSERT(background.AveragedResourceTotalValues.IsValueStable());
        UNIT_ASSERT_DOUBLES_EQUAL(background.GetNodeUsage(EResourceToBalance::CPU), .06, 1e-12);
        UNIT_ASSERT(!background.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        AssertSources(fixture.Settings(), {4, 5});
    }

    Y_UNIT_TEST(UnmovableMinimumCannotTriggerBalancing) {
        TScatterFixture fixture;
        fixture.AddNode(EResourceToBalance::CPU, 10'000);
        fixture.Tablets.back()->BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_IGNORE;
        fixture.AddNode(EResourceToBalance::CPU, 50'000);
        fixture.AddNode(EResourceToBalance::CPU, 50'000);
        UNIT_ASSERT(!fixture.Settings());
    }

    Y_UNIT_TEST(UnmovableMinimumDoesNotLowerSourceThreshold) {
        TScatterFixture fixture;
        fixture.AddNode(EResourceToBalance::CPU, 10'000);
        fixture.Tablets.back()->BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_IGNORE;
        fixture.AddNode(EResourceToBalance::CPU, 40'000);
        fixture.AddNode(EResourceToBalance::CPU, 50'000);
        fixture.AddNode(EResourceToBalance::CPU, 90'000);
        auto settings = fixture.Settings();
        AssertSources(settings, {4});
        UNIT_ASSERT(settings->ResourceToBalance == EResourceToBalance::CPU);
        UNIT_ASSERT_DOUBLES_EQUAL(*settings->MinNodeUsage, .08, 1e-12);
    }

    Y_UNIT_TEST(IdleReceiverAllowsSingleSourceWithoutBecomingASource) {
        TScatterFixture fixture;
        UNIT_ASSERT(!fixture.Settings());
        fixture.AddNode(EResourceToBalance::CPU, 0, false);
        fixture.AddNode(EResourceToBalance::CPU, 500'000, false);
        UNIT_ASSERT(!fixture.Settings());
        fixture.AddNode(EResourceToBalance::CPU, 900'000);
        AssertSources(fixture.Settings(), {3});
    }

    Y_UNIT_TEST(SingleSourceWithoutIdleReceiversDoesNotTrigger) {
        TScatterFixture fixture;
        fixture.AddNode(EResourceToBalance::CPU, 900'000);
        UNIT_ASSERT(!fixture.Settings());
    }

    Y_UNIT_TEST(BackgroundOnlyScalarUsageIsNotAnIdleComputeReceiver) {
        TScatterFixture fixture;
        fixture.Hive.CurrentConfig.SetMinCounterScatterToBalance(1.);
        for (int i = 0; i < 5; ++i) {
            fixture.AddNode(EResourceToBalance::CPU, 50'000);
        }
        auto& background = fixture.AddNode(EResourceToBalance::CPU, 0, false);
        AssertSources(fixture.Settings(), {1, 2, 3, 4, 5});

        for (int i = 0; i < 20; ++i) {
            background.AveragedNodeTotalUsage.Push(.06);
        }
        background.NodeTotalUsage = background.AveragedNodeTotalUsage.GetValue();
        UNIT_ASSERT(background.AveragedNodeTotalUsage.IsValueStable());
        UNIT_ASSERT_DOUBLES_EQUAL(background.GetNodeUsage(), .06, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(background.GetNodeUsage(EResourceToBalance::CPU), 0., 1e-12);
        UNIT_ASSERT(!background.HasTabletsForBalancer(EResourceToBalance::ComputeResources, fixture.Now));
        // The equal 5% sources must not be compared with external load masquerading as zero CPU.
        UNIT_ASSERT(!fixture.Settings());
    }

    Y_UNIT_TEST(ResourceIdleNodeWithOtherMovableResourceCanReceive) {
        TScatterFixture fixture;
        auto& receiver = fixture.AddNode(EResourceToBalance::Memory, 10'000);
        fixture.AddNode(EResourceToBalance::CPU, 50'000);
        UNIT_ASSERT_DOUBLES_EQUAL(receiver.GetNodeUsage(EResourceToBalance::CPU), 0., 1e-12);
        UNIT_ASSERT(receiver.GetNodeUsage() > 0.);
        UNIT_ASSERT(receiver.HasTabletsForBalancer(EResourceToBalance::ComputeResources, fixture.Now));
        auto settings = fixture.Settings();
        AssertSources(settings, {2});
        UNIT_ASSERT(settings->ResourceToBalance == EResourceToBalance::CPU);
    }

    Y_UNIT_TEST(CounterIdleReceiverMayHaveBackgroundComputeLoad) {
        TScatterFixture fixture;
        auto& receiver = fixture.AddNode(EResourceToBalance::CPU, 800'000, false);
        auto& source = fixture.AddNode(EResourceToBalance::Counter, 3);
        fixture.AddTablet(source, EResourceToBalance::Counter);
        fixture.AddTablet(source, EResourceToBalance::Counter);
        UNIT_ASSERT(receiver.GetNodeUsage() > 0.);
        UNIT_ASSERT(!receiver.HasTabletsForBalancer(EResourceToBalance::ComputeResources, fixture.Now));
        auto settings = fixture.Settings();
        AssertSources(settings, {2});
        UNIT_ASSERT(settings->ResourceToBalance == EResourceToBalance::Counter);
    }

    Y_UNIT_TEST(IdleReceiversMustBeAliveUpAndUnfrozen) {
        for (int unavailableState = 0; unavailableState < 3; ++unavailableState) {
            TScatterFixture fixture;
            fixture.AddNode(EResourceToBalance::CPU, 50'000);
            auto& receiver = fixture.AddNode(EResourceToBalance::CPU, 0, false);
            AssertSources(fixture.Settings(), {1});
            if (unavailableState == 0) {
                receiver.SetAlive(false);
            } else if (unavailableState == 1) {
                receiver.Down = true;
            } else {
                receiver.Freeze = true;
            }
            UNIT_ASSERT(!fixture.Settings());
        }
    }

    Y_UNIT_TEST(NodeEligibilityChecksLivenessAdministrativeStateAndRunningTablets) {
        TScatterFixture fixture;
        auto& node = fixture.AddNode(EResourceToBalance::CPU, 50'000, false);
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        auto& tablet = fixture.AddTablet(node, EResourceToBalance::CPU);
        UNIT_ASSERT(node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        // Group reassignment can retain the RUNNING bucket while the leader is
        // not ready for a restart. Use the same readiness check as the balancer.
        tablet.State = ETabletState::GroupAssignment;
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        tablet.State = ETabletState::ReadyToWork;
        UNIT_ASSERT(node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        node.SetAlive(false);
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        node.SetAlive(true);
        node.Down = true;
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        node.Down = false;
        node.Freeze = true;
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
        node.Freeze = false;
        node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING].erase(&tablet);
        node.Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_STARTING].insert(&tablet);
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
    }

    Y_UNIT_TEST(TabletEligibilityChecksPolicyMetricCooldownAndPinning) {
        TScatterFixture fixture;
        auto& node = fixture.AddNode(EResourceToBalance::CPU, 50'000);
        auto& tablet = *fixture.Tablets.back();
        UNIT_ASSERT(tablet.IsGoodForBalancer(fixture.Now, EResourceToBalance::CPU));
        UNIT_ASSERT(!tablet.IsGoodForBalancer(fixture.Now, EResourceToBalance::Memory));
        tablet.BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_IGNORE;
        UNIT_ASSERT(!tablet.IsGoodForBalancer(fixture.Now, EResourceToBalance::CPU));
        tablet.BalancerPolicy = TTabletInfo::EBalancerPolicy::POLICY_BALANCE;
        tablet.MakeBalancerDecision(fixture.Now);
        UNIT_ASSERT(!tablet.IsGoodForBalancer(fixture.Now + TDuration::Seconds(600), EResourceToBalance::CPU));
        UNIT_ASSERT(tablet.IsGoodForBalancer(fixture.Now + TDuration::Seconds(601), EResourceToBalance::CPU));
        fixture.Hive.CurrentConfig.SetUseTabletUsageEstimate(true);
        fixture.Hive.CurrentConfig.SetTabletImpactToPin(.01);
        fixture.Hive.CurrentConfig.SetTabletImpactShareToPin(.5);
        tablet.SetUsageImpact(.05);
        UNIT_ASSERT(tablet.IsPinnedToNode());
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now + TDuration::Seconds(601)));
    }

    Y_UNIT_TEST(IgnoredTabletTypeIsNotEligible) {
        TScatterFixture fixture;
        auto& node = fixture.AddNode(EResourceToBalance::CPU, 50'000);
        fixture.Hive.UpdateConfig([](auto& config) {
            config.AddBalancerIgnoreTabletTypes(TTabletTypes::Dummy);
        });
        UNIT_ASSERT(!node.HasTabletsForBalancer(EResourceToBalance::CPU, fixture.Now));
    }

    Y_UNIT_TEST(ResourceCohortsAreIndependent) {
        for (auto resource : {EResourceToBalance::CPU, EResourceToBalance::Memory, EResourceToBalance::Network}) {
            TScatterFixture fixture;
            fixture.Hive.CurrentConfig.SetMinCPUScatterToBalance(resource == EResourceToBalance::CPU ? .5 : 1.);
            fixture.Hive.CurrentConfig.SetMinMemoryScatterToBalance(resource == EResourceToBalance::Memory ? .5 : 1.);
            fixture.Hive.CurrentConfig.SetMinNetworkScatterToBalance(resource == EResourceToBalance::Network ? .5 : 1.);
            fixture.AddNode(resource, 10'000);
            fixture.AddNode(resource, 50'000);
            auto other = resource == EResourceToBalance::CPU ? EResourceToBalance::Memory : EResourceToBalance::CPU;
            auto& irrelevant = fixture.AddNode(other, 900'000);
            SetResource(irrelevant.ResourceValues, resource, 900'000);
            auto settings = fixture.Settings();
            AssertSources(settings, {2});
            UNIT_ASSERT(settings->ResourceToBalance == resource);
        }
    }

    Y_UNIT_TEST(CounterHasZeroFloorAndIgnoresDifferenceOfOne) {
        TScatterFixture fixture;
        fixture.Hive.CurrentConfig.SetMinCounterScatterToBalance(.2);
        fixture.AddNode(EResourceToBalance::Counter, 1);
        auto& second = fixture.AddNode(EResourceToBalance::Counter, 2);
        fixture.AddTablet(second, EResourceToBalance::Counter);
        UNIT_ASSERT(!fixture.Settings());
        fixture.AddTablet(second, EResourceToBalance::Counter);
        SetResource(second.ResourceValues, EResourceToBalance::Counter, 3);
        auto settings = fixture.Settings();
        AssertSources(settings, {2});
        UNIT_ASSERT(settings->Type == EBalancerType::ScatterCounter);
        UNIT_ASSERT_DOUBLES_EQUAL(*settings->MinNodeUsage, 1.25e-6, 1e-12);
    }

    Y_UNIT_TEST(SourceThresholdUsesResourceMinimumAboveFloor) {
        for (auto resource : {EResourceToBalance::CPU, EResourceToBalance::Memory, EResourceToBalance::Network}) {
            TScatterFixture fixture;
            fixture.Hive.CurrentConfig.SetMinCounterScatterToBalance(1.);
            fixture.Hive.CurrentConfig.SetMinCPUScatterToBalance(resource == EResourceToBalance::CPU ? .5 : 1.);
            fixture.Hive.CurrentConfig.SetMinMemoryScatterToBalance(resource == EResourceToBalance::Memory ? .5 : 1.);
            fixture.Hive.CurrentConfig.SetMinNetworkScatterToBalance(resource == EResourceToBalance::Network ? .5 : 1.);
            auto& minimum = fixture.AddNode(resource, 40'000);
            fixture.AddNode(resource, 50'000);
            fixture.AddNode(resource, 90'000);
            // Composite node usage must not replace the selected resource minimum.
            const auto other = resource == EResourceToBalance::CPU ? EResourceToBalance::Memory : EResourceToBalance::CPU;
            SetResource(minimum.ResourceValues, other, 500'000);
            auto settings = fixture.Settings();
            AssertSources(settings, {3});
            UNIT_ASSERT(settings->ResourceToBalance == resource);
            UNIT_ASSERT_DOUBLES_EQUAL(*settings->MinNodeUsage, .08, 1e-12);
        }
    }

    Y_UNIT_TEST(SourceThresholdUsesOneMinusScatterAndStrictComparison) {
        for (double scatter : {0., .2, .5, .75}) {
            TScatterFixture fixture;
            fixture.Hive.CurrentConfig.SetMinCPUScatterToBalance(scatter);
            // Every configuration has the same 4% source threshold.
            fixture.Hive.CurrentConfig.SetMinNodeUsageToBalance(.04 * (1. - scatter));
            fixture.AddNode(EResourceToBalance::CPU, 5'000);
            fixture.AddNode(EResourceToBalance::CPU, 30'000);
            fixture.AddNode(EResourceToBalance::CPU, 40'000);
            fixture.AddNode(EResourceToBalance::CPU, 60'000);
            auto settings = fixture.Settings();
            AssertSources(settings, {4});
            UNIT_ASSERT_DOUBLES_EQUAL(*settings->MinNodeUsage, .04, 1e-12);
        }
    }

    Y_UNIT_TEST(ScatterLimitOfOneAndEqualityDoNotTrigger) {
        TScatterFixture fixture;
        fixture.Hive.CurrentConfig.SetMinNodeUsageToBalance(.02);
        fixture.AddNode(EResourceToBalance::CPU, 10'000);
        fixture.AddNode(EResourceToBalance::CPU, 40'000);
        UNIT_ASSERT(!fixture.Settings()); // scatter == .5
        fixture.Hive.CurrentConfig.SetMinCPUScatterToBalance(1.);
        fixture.Hive.CurrentConfig.SetMinNodeUsageToBalance(0.);
        UNIT_ASSERT(!fixture.Settings());
    }

    Y_UNIT_TEST(StaleNodeTotalsDoNotKeepSourcesAboveThreshold) {
        TScatterFixture fixture;
        fixture.AddNode(EResourceToBalance::CPU, 10'000);
        auto& source = fixture.AddNode(EResourceToBalance::CPU, 50'000);
        for (int i = 0; i < 20; ++i) {
            source.AveragedResourceTotalValues.Push(source.ResourceValues);
            source.AveragedNodeTotalUsage.Push(.05);
        }
        source.ResourceTotalValues = source.AveragedResourceTotalValues.GetValue();
        source.NodeTotalUsage = source.AveragedNodeTotalUsage.GetValue();
        UNIT_ASSERT(source.AveragedResourceTotalValues.IsValueStable());
        UNIT_ASSERT(source.AveragedNodeTotalUsage.IsValueStable());
        AssertSources(fixture.Settings(), {2});
        SetResource(source.ResourceValues, EResourceToBalance::CPU, 30'000);
        UNIT_ASSERT_DOUBLES_EQUAL(source.GetNodeUsage(EResourceToBalance::CPU), .05, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(source.GetNodeUsage(), .05, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(source.GetTabletUsage(EResourceToBalance::CPU), .03, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(source.GetTabletUsage(EResourceToBalance::ComputeResources), .03, 1e-12);
        UNIT_ASSERT(!fixture.Settings());
    }
}

} // namespace NKikimr::NHive
