#include "fast_path_service.h"

#include "direct_block_group_mock.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>

#include <algorithm>
#include <thread>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

using EChaosMode = TChaosConfig::TChaosNodeConfig::EChaosMode;

// Records node state changes issued by TFastPathService for one DBG.
class TChaosInjectorControlMock final: public NTransport::IChaosInjectorControl
{
public:
    void DisableNode(ui32 nodeId) override
    {
        DisabledNodes.insert(nodeId);
    }

    void EnableNode(ui32 nodeId) override
    {
        DisabledNodes.erase(nodeId);
    }

    [[nodiscard]] bool IsNodeDisabled(ui32 nodeId) const override
    {
        return DisabledNodes.contains(nodeId);
    }

private:
    TSet<ui32> DisabledNodes;
};

void AssertChaosMode(
    const TFastPathService& service,
    ui32 nodeId,
    ui32 dbgIndex,
    EChaosMode expectedMode)
{
    const auto* config = service.GetChaosConfig().NodeConfigs.FindPtr(
        TChaosConfig::TDbgAndNodeId{
            .NodeId = nodeId,
            .DbgIndex = dbgIndex,
        });
    UNIT_ASSERT(config);
    UNIT_ASSERT(config->Mode == expectedMode);
}

struct TFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters{
        new NMonitoring::TDynamicCounters()};
    TVector<std::shared_ptr<TChaosInjectorControlMock>> ChaosInjectorControls;

    void SetUp(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);

        Runtime = std::make_unique<NActors::TTestActorRuntime>();
        Runtime->Initialize(NActors::TTestActorRuntime::TEgg{
            .App0 = new NKikimr::
                TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
            .Opaque = nullptr,
            .KeyConfigGenerator = nullptr,
            .Icb = {},
            .Dcb = {}});
    }

    std::shared_ptr<TFastPathService> MakeService(ui64 copyRangeBandwidthMbs)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetCopyRangeBandwidthMbs(copyRangeBandwidthMbs);

        TVector<IDirectBlockGroupPtr> directBlockGroups;
        directBlockGroups.reserve(DirectBlockGroupsCount);
        TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls;
        chaosInjectorControls.reserve(DirectBlockGroupsCount);
        ChaosInjectorControls.clear();
        ChaosInjectorControls.reserve(DirectBlockGroupsCount);

        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            directBlockGroups.push_back(
                std::make_shared<TDirectBlockGroupMock>());
            auto control = std::make_shared<TChaosInjectorControlMock>();
            chaosInjectorControls.push_back(control);
            ChaosInjectorControls.push_back(std::move(control));
        }

        return std::make_shared<TFastPathService>(
            Runtime->GetActorSystem(0),
            NActors::TActorId(),
            TDiskDescription{
                .DiskId = "disk-id",
                .TabletId = 100,
                .Generation = 1},
            0,
            DefaultBlockSize,
            std::move(directBlockGroups),
            std::move(chaosInjectorControls),
            TVChunkConfigs{},
            TDirtyMapStateProtos{},
            std::make_shared<TStorageConfig>(std::move(storageServiceConfig)),
            nullptr,
            nullptr,
            Counters);
    }

    static TVector<TDuration> TakeBudgetConcurrently(
        const std::shared_ptr<TFastPathService>& service,
        size_t requestCount)
    {
        TVector<TDuration> delays(requestCount);
        TVector<std::thread> threads;
        threads.reserve(requestCount);

        for (size_t i = 0; i < requestCount; ++i) {
            threads.emplace_back(
                [&, i] {
                    delays[i] =
                        service->TakeVolumeCopyRangeBudget(CopyRangeSize);
                });
        }
        for (auto& thread: threads) {
            thread.join();
        }

        std::sort(delays.begin(), delays.end());
        return delays;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFastPathServiceTest)
{
    Y_UNIT_TEST_F(ShouldNotThrottleCopyRangeWhenDisabled, TFixture)
    {
        auto service = MakeService(0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            service->TakeVolumeCopyRangeBudget(CopyRangeSize));
    }

    Y_UNIT_TEST_F(ShouldShareCopyRangeBudgetAcrossThreads, TFixture)
    {
        auto service = MakeService(2);

        const auto checkBudget = [&]
        {
            const auto delays = TakeBudgetConcurrently(service, 4);
            UNIT_ASSERT_VALUES_EQUAL(4u, delays.size());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), delays[0]);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), delays[1]);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(500), delays[2]);
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), delays[3]);
        };

        // A 2 MB/s bucket allows a 2 MB initial burst shared by all callers.
        checkBudget();

        // The maximum accumulated budget remains one second of bandwidth.
        Runtime->AdvanceCurrentTime(TDuration::Seconds(10));
        checkBudget();
    }

    Y_UNIT_TEST_F(ShouldUpdateChaosNodeConfig, TFixture)
    {
        auto service = MakeService(0);
        UNIT_ASSERT(service->GetChaosConfig().NodeConfigs.empty());

        service->SetNodeChaosMode(42, 0, EChaosMode::Disabled);
        AssertChaosMode(*service, 42, 0, EChaosMode::Disabled);
        UNIT_ASSERT(ChaosInjectorControls[0]->IsNodeDisabled(42));
        UNIT_ASSERT(!ChaosInjectorControls[1]->IsNodeDisabled(42));

        service->SetNodeChaosMode(42, 0, EChaosMode::Enabled);
        AssertChaosMode(*service, 42, 0, EChaosMode::Enabled);
        UNIT_ASSERT(!ChaosInjectorControls[0]->IsNodeDisabled(42));

        service->SetNodeChaosMode(42, 1, EChaosMode::Disabled);
        AssertChaosMode(*service, 42, 1, EChaosMode::Disabled);
        UNIT_ASSERT(ChaosInjectorControls[1]->IsNodeDisabled(42));

        const auto& configs = service->GetChaosConfig().NodeConfigs;
        UNIT_ASSERT_VALUES_EQUAL(2, configs.size());
    }

    Y_UNIT_TEST_F(ShouldUpdateChaosNodeConfigForAllDbgs, TFixture)
    {
        auto service = MakeService(0);

        service->SetNodeChaosMode(42, std::nullopt, EChaosMode::Disabled);
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupsCount,
            service->GetChaosConfig().NodeConfigs.size());
        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            AssertChaosMode(*service, 42, i, EChaosMode::Disabled);
            UNIT_ASSERT(ChaosInjectorControls[i]->IsNodeDisabled(42));
        }

        service->SetNodeChaosMode(42, std::nullopt, EChaosMode::Enabled);
        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            AssertChaosMode(*service, 42, i, EChaosMode::Enabled);
            UNIT_ASSERT(!ChaosInjectorControls[i]->IsNodeDisabled(42));
        }
    }

    Y_UNIT_TEST_F(ShouldIgnoreChaosNodeConfigForMissingDbg, TFixture)
    {
        auto service = MakeService(0);

        service->SetNodeChaosMode(
            42,
            DirectBlockGroupsCount,
            EChaosMode::Disabled);

        UNIT_ASSERT(service->GetChaosConfig().NodeConfigs.empty());
        for (const auto& control: ChaosInjectorControls) {
            UNIT_ASSERT(!control->IsNodeDisabled(42));
        }
    }

    Y_UNIT_TEST_F(ShouldReturnDirectBlockGroupByIndex, TFixture)
    {
        auto service = MakeService(0);

        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            UNIT_ASSERT(service->GetDirectBlockGroup(i));
        }
        UNIT_ASSERT(!service->GetDirectBlockGroup(DirectBlockGroupsCount));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
