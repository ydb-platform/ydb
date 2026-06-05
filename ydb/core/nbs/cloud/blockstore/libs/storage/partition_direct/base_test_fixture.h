#pragma once

#include "direct_block_group_mock.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Default vchunk size.
constexpr ui64 DefaultVChunkSize = RegionSize / DirectBlockGroupsCount;

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size);

////////////////////////////////////////////////////////////////////////////////

struct TScheduledTask
{
    TDuration Delay;
    TCallback Callback;
};

struct TBaseFixture: public NUnitTest::TBaseFixture
{
    static constexpr ui32 FixtureVChunkIndex = 100;

    const ui32 BlockSize = DefaultBlockSize;
    const ui64 VChunkBlockCount = DefaultVChunkSize / BlockSize;
    const ui64 BlocksPerCopy = CopyRangeSize / BlockSize;
    const THostIndex FreshDDisk = 1;
    TVChunkConfig VChunkConfig = TVChunkConfig::MakeDefault(
        FixtureVChunkIndex,
        DirectBlockGroupHostCount,
        DefaultPrimaryCount);
    TLogTitle LogTitle{
        GetCycleCount(),
        TLogTitle::TVChunk{
            .DiskId = "disk-id",
            .VChunkIndex = VChunkConfig.GetVChunkIndex()}};

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters{
        new ::NMonitoring::TDynamicCounters()};
    TPartitionDirectServiceMockPtr PartitionDirectService;
    TDirectBlockGroupMockPtr DirectBlockGroup;
    TBlocksDirtyMap DirtyMap{VChunkConfig, BlockSize, VChunkBlockCount};

    TBlockRange64 ExpectedRange;
    TString RangeData;

    TMutex PromisesGuard;
    TVector<TScheduledTask> ScheduledTasks;
    TVector<NThreading::TPromise<TDBGReadBlocksResponse>> ReadPromises;
    TVector<NThreading::TPromise<TDBGWriteBlocksResponse>> WritePromises;
    TVector<NThreading::TPromise<TDBGFlushResponse>> FlushPromises;
    TVector<NThreading::TPromise<TDBGEraseResponse>> ErasePromises;

    virtual void Init();

    TGuardedSgList MakeSgList() const;

    bool WaitScheduledTasks(size_t count, TDuration timeout);
    NThreading::TFuture<void> RunScheduledTasks();

    void SetReadResult(TDBGReadBlocksResponse response, bool async);
    bool WaitReadRequests(size_t count, TDuration timeout);

    void SetWriteResult(TDBGWriteBlocksResponse response, bool async);
    bool WaitWriteRequests(size_t count, TDuration timeout);

    void SetFlushResult(TDBGFlushResponse response, bool async);
    bool WaitFlushRequests(size_t count, TDuration timeout);

    void SetEraseResult(TDBGEraseResponse response, bool async);
    bool WaitEraseRequests(size_t count, TDuration timeout);

    static auto& AccessBlocksDirtyMap(TVChunk& vchunk)
    {
        return vchunk.BlocksDirtyMap;
    }

    static auto& AccessConfig(TVChunk& vchunk)
    {
        return vchunk.VChunkConfig;
    }

private:
    template <typename T>
    void SetResult(
        TVector<NThreading::TPromise<T>>& promises,
        T response,
        bool async);

    template <typename T>
    bool Wait(TVector<T>& items, size_t count, TDuration timeout);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
