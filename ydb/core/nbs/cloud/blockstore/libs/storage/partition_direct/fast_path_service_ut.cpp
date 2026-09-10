#include "fast_path_service.h"

#include "direct_block_group_mock.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/set.h>
#include <util/generic/size_literals.h>

#include <algorithm>
#include <thread>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Reads private region snapshot fields of TFastPathService from tests.
class TFastPathServiceAccessor
{
public:
    // Number of regions in the published snapshot.
    static size_t GetRegionCount(const TFastPathService& service)
    {
        return service.Regions.AtomicLoad()->Regions.size();
    }

    // Region at index, or nullptr if the index is out of range.
    static TRegionPtr GetRegion(const TFastPathService& service, size_t index)
    {
        const auto snapshot = service.Regions.AtomicLoad();
        return index < snapshot->Regions.size() ? snapshot->Regions[index]
                                                : nullptr;
    }
};

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

struct TMakeServiceParams
{
    ui64 CopyRangeBandwidthMbs = 0;
    ui64 BlockCount = 0;
    ui64 VChunkSize = 0;
    ui64 StripeSize = 0;
    bool StubRestoreDBGPBuffers = false;
};

struct TFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters{
        new NMonitoring::TDynamicCounters()};
    TVector<std::shared_ptr<TChaosInjectorControlMock>> ChaosInjectorControls;
    TVector<std::shared_ptr<TDirectBlockGroupMock>> DirectBlockGroups;

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

    void TearDown(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);

        // Regions destroy their vchunks on the DBG executor thread, and a
        // vchunk logs through the runtime's actor system while doing so.
        // Join the executors before the runtime goes away.
        for (const auto& directBlockGroup: DirectBlockGroups) {
            directBlockGroup->GetExecutor()->Stop();
        }
        DirectBlockGroups.clear();
    }

    std::shared_ptr<TFastPathService> MakeService(
        const TMakeServiceParams& params = {})
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetCopyRangeBandwidthMbs(
            params.CopyRangeBandwidthMbs);
        if (params.VChunkSize) {
            storageServiceConfig.SetVChunkSize(params.VChunkSize);
        }
        if (params.StripeSize) {
            storageServiceConfig.SetStripeSize(params.StripeSize);
        }

        TVector<IDirectBlockGroupPtr> directBlockGroups;
        directBlockGroups.reserve(DirectBlockGroupsCount);
        TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls;
        chaosInjectorControls.reserve(DirectBlockGroupsCount);
        ChaosInjectorControls.clear();
        ChaosInjectorControls.reserve(DirectBlockGroupsCount);
        DirectBlockGroups.clear();
        DirectBlockGroups.reserve(DirectBlockGroupsCount);

        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            auto directBlockGroup = std::make_shared<TDirectBlockGroupMock>();
            if (params.StubRestoreDBGPBuffers) {
                directBlockGroup->RestoreDBGPBuffersHandler = [](const auto&...)
                {
                    return NThreading::MakeFuture<TDBGRestoreResponse>(
                        {.Error = MakeError(S_OK)});
                };
            }
            directBlockGroups.push_back(directBlockGroup);
            DirectBlockGroups.push_back(std::move(directBlockGroup));
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
            params.BlockCount,
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
        auto service = MakeService();

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            service->TakeVolumeCopyRangeBudget(CopyRangeSize));
    }

    Y_UNIT_TEST_F(ShouldShareCopyRangeBudgetAcrossThreads, TFixture)
    {
        auto service = MakeService({.CopyRangeBandwidthMbs = 2});

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
        auto service = MakeService();
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
        auto service = MakeService();

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
        auto service = MakeService();

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
        auto service = MakeService();

        for (ui32 i = 0; i < DirectBlockGroupsCount; ++i) {
            UNIT_ASSERT(service->GetDirectBlockGroup(i));
        }
        UNIT_ASSERT(!service->GetDirectBlockGroup(DirectBlockGroupsCount));
    }

    Y_UNIT_TEST_F(ShouldGrowRegionsInPlace, TFixture)
    {
        constexpr ui64 vChunkSize = 32_MB;
        const ui64 blocksPerRegion = RegionSize / DefaultBlockSize;
        auto service = MakeService({
            .BlockCount = blocksPerRegion,
            .VChunkSize = vChunkSize,
            .StripeSize = 512_KB,
            .StubRestoreDBGPBuffers = true,
        });

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        const auto firstRegion =
            TFastPathServiceAccessor::GetRegion(*service, 0);
        UNIT_ASSERT(firstRegion);

        service->Grow(blocksPerRegion * 2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        UNIT_ASSERT_EQUAL(
            firstRegion,
            TFastPathServiceAccessor::GetRegion(*service, 0));
        UNIT_ASSERT(TFastPathServiceAccessor::GetRegion(*service, 1));
        UNIT_ASSERT(
            TFastPathServiceAccessor::GetRegion(*service, 1) != firstRegion);
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerRegion * 2,
            service->GetVolumeConfig()->BlockCount);

        service->Grow(blocksPerRegion * 2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        UNIT_ASSERT_EQUAL(
            firstRegion,
            TFastPathServiceAccessor::GetRegion(*service, 0));

        // Join the region executor threads before the fixture goes away.
        service->Stop().GetValueSync();
    }

    Y_UNIT_TEST_F(ShouldGrowBlockCountWithoutNewRegions, TFixture)
    {
        constexpr ui64 vChunkSize = 32_MB;
        const ui64 blocksPerRegion = RegionSize / DefaultBlockSize;
        auto service = MakeService({
            .BlockCount = blocksPerRegion / 2,
            .VChunkSize = vChunkSize,
            .StripeSize = 512_KB,
            .StubRestoreDBGPBuffers = true,
        });

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        const auto firstRegion =
            TFastPathServiceAccessor::GetRegion(*service, 0);
        UNIT_ASSERT(firstRegion);
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerRegion / 2,
            service->GetVolumeConfig()->BlockCount);

        service->Grow(blocksPerRegion).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        UNIT_ASSERT_EQUAL(
            firstRegion,
            TFastPathServiceAccessor::GetRegion(*service, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerRegion,
            service->GetVolumeConfig()->BlockCount);

        service->Stop().GetValueSync();
    }

    Y_UNIT_TEST_F(ShouldNotPublishGrowAfterStop, TFixture)
    {
        constexpr ui64 vChunkSize = 32_MB;
        const ui64 blocksPerRegion = RegionSize / DefaultBlockSize;
        auto service = MakeService({
            .BlockCount = blocksPerRegion,
            .VChunkSize = vChunkSize,
            .StripeSize = 512_KB,
            .StubRestoreDBGPBuffers = true,
        });

        service->Stop().GetValueSync();
        service->Grow(blocksPerRegion * 2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TFastPathServiceAccessor::GetRegionCount(*service));
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerRegion,
            service->GetVolumeConfig()->BlockCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
