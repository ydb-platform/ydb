#include "write_request.h"

#include "base_test_fixture.h"
#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"
#include "write_with_pb_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

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

THostMask MakeAllHostsMask()
{
    return MakeHostMask({0, 1, 2, 3, 4});
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
        DirectBlockGroup->Oracle.WriteHedgingDelay = hedgeDelay;
        DirectBlockGroup->Oracle.WriteRequestTimeout = timeout;

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
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
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

        std::optional<TBaseWriteRequestExecutor::TResponse> callbackResult;
        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                LogTitle.GetChild(GetCycleCount()),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId());
        writeRequest->SetReplyCallback(
            [&](TBaseWriteRequestExecutor::TResponse response)
            { callbackResult = std::move(response); });
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[2].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, callbackResult.has_value());
        const auto& response = *callbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorAfterTimeout, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();
        DirectBlockGroup->Oracle.WriteHedgingDelay = hedgeDelay;
        DirectBlockGroup->Oracle.WriteRequestTimeout = timeout;

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
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
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

        std::optional<TBaseWriteRequestExecutor::TResponse> callbackResult;
        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                LogTitle.GetChild(GetCycleCount()),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId());
        writeRequest->SetReplyCallback(
            [&](TBaseWriteRequestExecutor::TResponse response)
            { callbackResult = std::move(response); });
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, callbackResult.has_value());
        const auto& response = *callbackResult;
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
        DirectBlockGroup->Oracle.WriteHedgingDelay = hedgeDelay;
        DirectBlockGroup->Oracle.WriteRequestTimeout = timeout;

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
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
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

        std::optional<TBaseWriteRequestExecutor::TResponse> callbackResult;
        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                LogTitle.GetChild(GetCycleCount()),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId());
        writeRequest->SetReplyCallback(
            [&](TBaseWriteRequestExecutor::TResponse response)
            { callbackResult = std::move(response); });
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[1].second();

        UNIT_ASSERT_VALUES_EQUAL(5, writePBufferPromises.size());

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());

        writePBufferPromises[4].SetValue(   // HandOff1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, callbackResult.has_value());
        const auto& response = *callbackResult;
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
        DirectBlockGroup->Oracle.WriteHedgingDelay = hedgeDelay;
        DirectBlockGroup->Oracle.WriteRequestTimeout = timeout;

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
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
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

        std::optional<TBaseWriteRequestExecutor::TResponse> callbackResult;
        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                LogTitle.GetChild(GetCycleCount()),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId());
        writeRequest->SetReplyCallback(
            [&](TBaseWriteRequestExecutor::TResponse response)
            { callbackResult = std::move(response); });
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue(   // Primary0
            {.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue(   // Primary1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, callbackResult.has_value());

        scheduled[1].second();
        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(E_FAIL, "hedged handoff failed")});

        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        UNIT_ASSERT_VALUES_EQUAL(true, callbackResult.has_value());
        const auto& response = *callbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2, 3}), response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2}), response.CompletedWrites);
    }
}

Y_UNIT_TEST_SUITE(TWriteRequestWithPbReplicationTest)
{
    // @brief we want to sure that base path with no errors works
    Y_UNIT_TEST_F(ShouldBaseSuccessPath, TWriteWithPbTestFixture)
    {
        // make main request ok reply
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerWithImmediateOkResponse();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));

        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());

        const auto& response = *CallbackResult;

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            VChunkConfig.GetDesiredPBuffers(),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(
            VChunkConfig.GetDesiredPBuffers(),
            response.CompletedWrites);
    }

    // @brief we want to sure that in case of hanging main 'multi' request,
    // hedge mechanism will work
    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOk,
        TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        // ManyPBufferCallback is set because hanging handler stored it
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism. It will work with default response's handler
        // from base fixture
        RunScheduledHedge();
        // Reply to all write to PBuffer requests.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // ManyPBufferCallback is still set (main request still pending)
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));
        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);

        // make sure that there were successful hedge requests
        UNIT_ASSERT_EQUAL(true, response.CompletedWrites.Get(THostIndex{3}));
        UNIT_ASSERT_EQUAL(true, response.CompletedWrites.Get(THostIndex{4}));
    }

    // @brief sending main request then hedge requests.
    // main's responses come before hedge and return reply to the caller
    Y_UNIT_TEST_F(ShouldMainPlusHedgeAndReplyFromMain, TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();

        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        // reply from main request
        ManyPBufferCallback(CreateOkResponse());
        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        // but there were sent requests to HO too
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);

        UNIT_ASSERT_EQUAL(
            VChunkConfig.GetDesiredPBuffers(),
            response.CompletedWrites);
    }

    // @brief sending main request then hedge requests.
    // main's responses come partially before hedge responses
    // return reply with mix of main's and hedge replies
    Y_UNIT_TEST_F(ShouldMainPlusHedgeAndReplyWithMix, TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();

        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));

        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());

        // reply is ready
        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, response.CompletedWrites.Count());

        // but there were sent requests to HO too
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);

        UNIT_ASSERT_EQUAL(true, response.CompletedWrites.Get(THostIndex{0}));
        bool atLeastOneHandoffResponded =
            response.CompletedWrites.Get(
                *VChunkConfig.GetSecondaryPBuffers().Nth(0)) ||
            response.CompletedWrites.Get(
                *VChunkConfig.GetSecondaryPBuffers().Nth(1));
        UNIT_ASSERT_EQUAL(true, atLeastOneHandoffResponded);
    }

    // @brief sending main request then hedge requests.
    // main's responses come partially before hedge responses
    // 1 hedge comes with success and 2 with errors
    // sending 1 retry for hedge. Getting reply for it
    // overall reply
    Y_UNIT_TEST_F(ShouldMainPlusHedgeRetry, TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();

        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));

        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[0].SetValue(CreateFailDirectResponse());
        DirectWritePromises[1].SetValue(CreateFailDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        // reply is not ready still
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        // there is retry direct write
        UNIT_ASSERT_VALUES_EQUAL(4, DirectWritePromises.size());
        DirectWritePromises[3].SetValue(CreateOkDirectResponse());

        // reply is ready
        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, response.CompletedWrites.Count());

        // there were sent requests to HO too
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);
    }

    // @brief getting errors on all retry attempts. We should receive an error.
    // Hedge requests are not sent because of existed retries from main path.
    // Retries are failed too.
    Y_UNIT_TEST_F(ShouldNotMainPlusHedgeRetry, TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();
        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        // immediate automatically sent retries
        UNIT_ASSERT_VALUES_EQUAL(2, DirectWritePromises.size());

        // call hedge mechanism
        RunScheduledHedge();
        // there is no hedge direct writes because of existed main's retries
        UNIT_ASSERT_VALUES_EQUAL(2, DirectWritePromises.size());

        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateFailDirectResponse());
        // there are 2 success reply now

        // immediate retry after fail of direct request
        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[2].SetValue(CreateFailDirectResponse());

        // immediate retry after fail of direct request
        UNIT_ASSERT_VALUES_EQUAL(4, DirectWritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());
        DirectWritePromises[3].SetValue(CreateFailDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());

        // reply is ready with error result
        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(2, response.CompletedWrites.Count());

        // there were sent requests all locations
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);
    }

    Y_UNIT_TEST_F(ShouldANotherTryOnOverallError, TWriteWithPbTestFixture)
    {
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();
        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));

        writeRequest->Run();

        ManyPBufferCallback(CreateDBGErrorResponse());
        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            DirectWritePromises.size());   // retry from main
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldWorkWithMultipleResponses, TWriteWithPbTestFixture)
    {
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();
        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));

        writeRequest->Run();

        {
            TDBGWriteBlocksToManyPBuffersResponse partResponse;
            partResponse.OverallError = MakeError(S_OK);
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{1}, .Error = MakeError(S_OK)});
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{2}, .Error = MakeError(S_OK)});

            ManyPBufferCallback(std::move(partResponse));
        }

        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            DirectWritePromises.size());   // retry from main
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());

        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));
        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());
    }

    Y_UNIT_TEST_F(
        ShouldWorkWithMultipleResponsesAndHedge,
        TWriteWithPbTestFixture)
    {
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerHanging();
        DirectBlockGroup->WriteBlocksToPBufferHandler =
            GetDirectWriteHandlerHanging();

        auto writeRequest =
            CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));

        writeRequest->Run();

        //  call hedge mechanism
        RunScheduledHedge();
        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        {
            TDBGWriteBlocksToManyPBuffersResponse partResponse;
            partResponse.OverallError = MakeError(S_OK);
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{0}, .Error = MakeError(S_OK)});
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{2}, .Error = MakeError(S_OK)});

            ManyPBufferCallback(std::move(partResponse));
        }

        UNIT_ASSERT_VALUES_EQUAL(false, CallbackResult.has_value());

        {
            ManyPBufferCallback(CreateOneOkResponse(THostIndex{1}));
        }

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        UNIT_ASSERT_VALUES_EQUAL(true, CallbackResult.has_value());
        const auto& response = *CallbackResult;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());

        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_EQUAL(5, AllCompletedWrites.Count());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
