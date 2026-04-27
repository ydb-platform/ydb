#include "write_request.h"

#include "base_test_fixture.h"
#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TRequestHeaders MakeWriteTestRequestHeaders(
    const TBlockRange64& range,
    ui32 blockSize)
{
    auto volumeConfig = std::make_shared<TVolumeConfig>(TVolumeConfig{
        .DiskId = "disk-1",
        .BlockSize = blockSize,
        .BlockCount = 65536,
        .BlocksPerStripe = 1024,
        .VChunkSize = DefaultVChunkSize});

    return TRequestHeaders{
        .VolumeConfig = std::move(volumeConfig),
        .RequestId = 1,
        .Range = range};
}

THostMask MakeHostMask(std::initializer_list<THostIndex> hosts)
{
    THostMask result;
    for (auto h: hosts) {
        result.Set(h);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteRequestTest)
{
    Y_UNIT_TEST_F(ShouldWrite, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[2].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorAfterTimeout, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("Write request timeout"),
            response.Error.GetMessage());
    }

    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOkWithDirectReplication,
        TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[1].second();

        UNIT_ASSERT_VALUES_EQUAL(5, writePBufferPromises.size());

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        writePBufferPromises[4].SetValue(   // HandOff1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            MakeHostMask({0, 1, 2, 3, 4}),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({2, 3, 4}), response.CompletedWrites);
    }

    Y_UNIT_TEST_F(ShouldNotSendWriteRequestAfterQuorumIsReached, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(100);
        const auto timeout = TDuration::MilliSeconds(10000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue(   // Primary0
            {.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue(   // Primary1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        scheduled[1].second();
        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(E_FAIL, "hedged handoff failed")});

        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2, 3}), response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2}), response.CompletedWrites);
    }

    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOkWithPbReplication,
        TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);
        const auto pbufferReplyTimeout = TDuration::MilliSeconds(500);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TVector<TPromise<TDBGWriteBlocksToManyPBuffersResponse>>
            manyPBufferBatchPromises;

        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            [&](ui32 vChunkIndex,
                TVector<THostIndex> hostIndexes,
                ui64 lsn,
                TBlockRange64 range,
                TDuration replyTimeout,
                const TGuardedSgList& guardedSglist,
                const NWilson::TTraceId& traceId)   //
            -> TFuture<TDBGWriteBlocksToManyPBuffersResponse>
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);
            UNIT_ASSERT_VALUES_EQUAL(pbufferReplyTimeout, replyTimeout);

            if (manyPBufferBatchPromises.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(3u, hostIndexes.size());
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{0}, hostIndexes[0]);
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{1}, hostIndexes[1]);
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{2}, hostIndexes[2]);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(3u, hostIndexes.size());
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{2}, hostIndexes[0]);
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{3}, hostIndexes[1]);
                UNIT_ASSERT_VALUES_EQUAL(THostIndex{4}, hostIndexes[2]);
            }

            auto promise = NewPromise<TDBGWriteBlocksToManyPBuffersResponse>();
            manyPBufferBatchPromises.push_back(std::move(promise));
            return manyPBufferBatchPromises.back().GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithPbReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout,
                pbufferReplyTimeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(1, manyPBufferBatchPromises.size());

        scheduled[1].second();

        UNIT_ASSERT_VALUES_EQUAL(2, manyPBufferBatchPromises.size());

        TDBGWriteBlocksToManyPBuffersResponse hedgeOk;
        hedgeOk.OverallError = MakeError(S_OK);
        hedgeOk.Responses.push_back(
            {.HostIndex = 3, .Error = MakeError(S_OK)});   // HandOff0
        hedgeOk.Responses.push_back(
            {.HostIndex = 4, .Error = MakeError(S_OK)});   // HandOff1
        hedgeOk.Responses.push_back(
            {.HostIndex = 2, .Error = MakeError(S_OK)});   // Primary2

        manyPBufferBatchPromises[1].SetValue(std::move(hedgeOk));

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            MakeHostMask({0, 1, 2, 3, 4}),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({2, 3, 4}), response.CompletedWrites);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
