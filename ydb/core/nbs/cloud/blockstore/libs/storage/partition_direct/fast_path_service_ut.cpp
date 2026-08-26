#include "fast_path_service.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <thread>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters{
        new NMonitoring::TDynamicCounters()};

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

    std::shared_ptr<TFastPathService> MakeService(
        ui64 copyRangeBandwidthMbs) const
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetCopyRangeBandwidthMbs(copyRangeBandwidthMbs);

        return std::make_shared<TFastPathService>(
            Runtime->GetActorSystem(0),
            NActors::TActorId(),
            TDiskDescription{
                .DiskId = "disk-id",
                .TabletId = 100,
                .Generation = 1},
            0,
            DefaultBlockSize,
            TVector<IDirectBlockGroupPtr>{},
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
