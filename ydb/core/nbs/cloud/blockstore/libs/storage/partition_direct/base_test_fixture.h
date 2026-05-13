#pragma once

#include "direct_block_group_mock.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_status.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Default vchunk size.
constexpr ui64 DefaultVChunkSize = RegionSize / DirectBlockGroupsCount;

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size);

////////////////////////////////////////////////////////////////////////////////

struct TBaseFixture: public NUnitTest::TBaseFixture
{
    static constexpr ui32 FixtureVChunkIndex = 100;
    static constexpr size_t FixtureHostCount = 5;
    static constexpr size_t FixturePrimaryCount = 3;

    const ui32 BlockSize = DefaultBlockSize;
    const ui64 BlocksPerCopy = CopyRangeSize / BlockSize;
    const THostIndex FreshDDisk = 1;
    TVChunkConfig VChunkConfig = TVChunkConfig::Make(
        FixtureVChunkIndex,
        FixtureHostCount,
        FixturePrimaryCount);

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TPartitionDirectServiceMockPtr PartitionDirectService;
    TDirectBlockGroupMockPtr DirectBlockGroup;
    TBlocksDirtyMap DirtyMap{
        VChunkConfig,
        BlockSize,
        DefaultVChunkSize / BlockSize};

    TBlockRange64 ExpectedRange;
    TString RangeData;
    TMutex ReadMutex;
    void SetReadResult(TDBGReadBlocksResponse response);
    void ClearReadPromises();
    void AddReadPromise(NThreading::TPromise<TDBGReadBlocksResponse> promise);
    TVector<NThreading::TPromise<TDBGReadBlocksResponse>> ReadPromises;

    NThreading::TPromise<TDBGWriteBlocksResponse> WritePromise =
        NThreading::NewPromise<TDBGWriteBlocksResponse>();

    virtual void Init();

    TGuardedSgList MakeSgList() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
