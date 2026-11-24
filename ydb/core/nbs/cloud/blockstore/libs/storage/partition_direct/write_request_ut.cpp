#include "write_request.h"

#include "write_request_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TRequestHeaders MakeWriteTestRequestHeaders(
    const TBlockRange16& range,
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
        .Range = ConvertRangeSafe<TBlockRange64>(range)};
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
    Y_UNIT_TEST_F(ShouldWrite, TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        DirectWritePromises[0].SetValue({.Error = MakeError(S_OK)});
        DirectWritePromises[1].SetValue({.Error = MakeError(S_OK)});
        DirectWritePromises[2].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorAfterTimeout, TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, Scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(Timeout, Scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(HedgeDelay, Scheduled[1].first);
        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        // Run hedge callback.
        Scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("Write request timeout"),
            response.Error.GetMessage());
    }

    Y_UNIT_TEST_F(
        ShouldNotifyBelatedWriteWithoutQuorum,
        TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        // Run timeout callback: the client gets an error and the quorum is
        // never reached.
        Scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            WriteClient->Response->Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, WriteClient->AllCompletedWrites.Count());

        DirectWritePromises[1].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(1, WriteClient->AllCompletedWrites.Count());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WriteClient->AllCompletedWrites.Get(THostIndex{1}));
    }

    Y_UNIT_TEST_F(
        ShouldNotifyBelatedIndirectWriteWithoutQuorum,
        TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        UNIT_ASSERT(ManyPBufferCallback);

        // Run timeout callback: the client gets an error and the quorum is
        // never reached.
        Scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            WriteClient->Response->Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, WriteClient->AllCompletedWrites.Count());

        ManyPBufferCallback(CreateOneOkResponse(THostIndex{1}));

        UNIT_ASSERT_VALUES_EQUAL(1, WriteClient->AllCompletedWrites.Count());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WriteClient->AllCompletedWrites.Get(THostIndex{1}));
    }

    Y_UNIT_TEST_F(ShouldNotHedgeAfterReply, TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        for (auto& promise: DirectWritePromises) {
            promise.SetValue(CreateOkDirectResponse());
        }

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteClient->Response->Error.GetCode());

        // The hedge fires after the reply: no handoff writes are sent.
        RunScheduledHedge();

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
    }

    Y_UNIT_TEST_F(
        ShouldNotSendAdditionalDirectWritesAfterReply,
        TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(0, DirectWritePromises.size());
        UNIT_ASSERT(ManyPBufferCallback);

        ManyPBufferCallback(CreateOkResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, WriteClient->Response->Error.GetCode());

        // The hedge fires after the reply: no direct writes are sent.
        RunScheduledHedge();

        UNIT_ASSERT_VALUES_EQUAL(0, DirectWritePromises.size());
    }

    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOk,
        TWriteRequestTestFixture)
    {
        Init();

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TPBufferKey pBufferKey,
             TBlockRange16 range,
             const TGuardedSgList& guardedSglist,
             const TBlockChecksums& checksums,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(checksums);

            UNIT_ASSERT_VALUES_EQUAL(
                UserPBufferKey.Print(),
                pBufferKey.Print());
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        Scheduled[1].second();

        UNIT_ASSERT_VALUES_EQUAL(5, writePBufferPromises.size());

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        writePBufferPromises[4].SetValue(   // HandOff1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            MakeHostMask({0, 1, 2, 3, 4}),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({2, 3, 4}), response.CompletedWrites);
    }

    Y_UNIT_TEST_F(
        ShouldNotSendWriteRequestAfterQuorumIsReached,
        TWriteRequestTestFixture)
    {
        Init();

        TMap<THostIndex, TPromise<TDBGWriteBlocksResponse>>
            writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TPBufferKey pBufferKey,
             TBlockRange16 range,
             const TGuardedSgList& guardedSglist,
             const TBlockChecksums& checksums,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(checksums);

            UNIT_ASSERT_VALUES_EQUAL(
                UserPBufferKey.Print(),
                pBufferKey.Print());
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::DirectWrite);
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue(   // Primary0
            {.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue(   // Primary1
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        // Run hedge callback.
        Scheduled[1].second();
        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        writePBufferPromises[2].SetValue(   // Primary2
            {.Error = MakeError(S_OK)});

        writePBufferPromises[3].SetValue(   // HandOff0
            {.Error = MakeError(E_FAIL, "hedged handoff failed")});

        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(4));   // HandOff1

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2, 3}), response.RequestedWrites);
        UNIT_ASSERT_EQUAL(MakeHostMask({0, 1, 2}), response.CompletedWrites);
    }
}

Y_UNIT_TEST_SUITE(TWriteRequestWithPBufferReplicationTest)
{
    // @brief we want to sure that base path with no errors works
    Y_UNIT_TEST_F(ShouldBaseSuccessPath, TWriteRequestTestFixture)
    {
        Init();

        // make main request ok reply
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            GetManyPBuffersHandlerWithImmediateOkResponse();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);

        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());

        const auto& response = *WriteClient->Response;

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
        TWriteRequestTestFixture)
    {
        Init();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        // ManyPBufferCallback is set because hanging handler stored it
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism. It will work with default response's handler
        // from base fixture
        RunScheduledHedge();
        // Reply to all write to PBuffer requests.
        for (auto& promise: DirectWritePromises) {
            promise.SetValue(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)});
        }

        // ManyPBufferCallback is still set (main request still pending)
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));
        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);

        // make sure that there were successful hedge requests
        UNIT_ASSERT_EQUAL(true, response.CompletedWrites.Get(THostIndex{3}));
        UNIT_ASSERT_EQUAL(true, response.CompletedWrites.Get(THostIndex{4}));
    }

    // @brief sending main request then hedge requests.
    // main's responses come before hedge and return reply to the caller
    Y_UNIT_TEST_F(ShouldMainPlusHedgeAndReplyFromMain, TWriteRequestTestFixture)
    {
        Init();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        // reply from main request
        ManyPBufferCallback(CreateOkResponse());
        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
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
    Y_UNIT_TEST_F(ShouldMainPlusHedgeAndReplyWithMix, TWriteRequestTestFixture)
    {
        Init();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());

        // reply is ready
        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
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
    Y_UNIT_TEST_F(ShouldMainPlusHedgeRetry, TWriteRequestTestFixture)
    {
        Init();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[0].SetValue(CreateFailDirectResponse());
        DirectWritePromises[1].SetValue(CreateFailDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        // reply is not ready still
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        // there is retry direct write
        UNIT_ASSERT_VALUES_EQUAL(4, DirectWritePromises.size());
        DirectWritePromises[3].SetValue(CreateOkDirectResponse());

        // reply is ready
        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, response.CompletedWrites.Count());

        // there were sent requests to HO too
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);
    }

    // @brief getting errors on all retry attempts. We should receive an
    // error. Hedge requests are not sent because of existed retries from
    // main path. Retries are failed too.
    Y_UNIT_TEST_F(ShouldNotMainPlusHedgeRetry, TWriteRequestTestFixture)
    {
        Init();

        // prepare and call main request
        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(true, bool(ManyPBufferCallback));

        // partially reply from main request
        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

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
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        DirectWritePromises[3].SetValue(CreateFailDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());

        // reply is ready with error result
        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(2, response.CompletedWrites.Count());

        // there were sent requests all locations
        UNIT_ASSERT_EQUAL(MakeAllHostsMask(), response.RequestedWrites);
    }

    Y_UNIT_TEST_F(ShouldMakeAnotherTryOnOverallError, TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);

        writeRequest->Run();

        ManyPBufferCallback(CreateDBGErrorResponse());
        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            DirectWritePromises.size());   // retry from main
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST(ShouldWaitForBothWritesToTheSameHost)
    {
        for (const bool directFirst: {false, true}) {
            for (const bool lateSuccess: {false, true}) {
                TWriteRequestTestFixture fixture;
                fixture.Init();
                TVector<THostIndex> directHosts;
                auto handler =
                    fixture.DirectBlockGroup->WriteBlocksToPBufferHandler;
                fixture.DirectBlockGroup->WriteBlocksToPBufferHandler =
                    [&directHosts,
                     handler](ui32 vChunkIndex, THostIndex host, auto&&... args)
                {
                    directHosts.push_back(host);
                    return handler(
                        vChunkIndex,
                        host,
                        std::forward<decltype(args)>(args)...);
                };

                auto request = fixture.CreateRequestExecutor(
                    MakeWriteTestRequestHeaders(
                        fixture.Range,
                        fixture.BlockSize),
                    EWriteMode::IndirectWrite);
                request->Run();
                fixture.RunScheduledHedge();
                UNIT_ASSERT_VALUES_EQUAL(3, directHosts.size());

                THostIndex duplicatedHost = InvalidHostIndex;
                for (auto host: directHosts) {
                    if (fixture.VChunkConfig.GetDesiredPBuffers().Get(host)) {
                        duplicatedHost = host;
                    }
                }
                UNIT_ASSERT_VALUES_UNEQUAL(InvalidHostIndex, duplicatedHost);

                if (directFirst) {
                    for (auto& promise: fixture.DirectWritePromises) {
                        promise.SetValue(fixture.CreateOkDirectResponse());
                    }
                } else {
                    fixture.ManyPBufferCallback(fixture.CreateOkResponse());
                }

                UNIT_ASSERT(fixture.WriteClient->Response.has_value());
                const auto& response = *fixture.WriteClient->Response;
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
                UNIT_ASSERT(response.CompletedWrites.Get(duplicatedHost));
                UNIT_ASSERT(!response.AllWritesAnswered.Get(duplicatedHost));
                UNIT_ASSERT(
                    fixture.WriteClient->BelatedAllWritesAnswered.Empty());

                if (directFirst) {
                    auto lateResponse = fixture.CreateOkResponse();
                    if (!lateSuccess) {
                        for (auto& hostResponse: lateResponse.Responses) {
                            hostResponse.Error = MakeError(E_FAIL);
                        }
                    }
                    fixture.ManyPBufferCallback(std::move(lateResponse));
                } else {
                    for (auto& promise: fixture.DirectWritePromises) {
                        promise.SetValue(
                            lateSuccess ? fixture.CreateOkDirectResponse()
                                        : fixture.CreateFailDirectResponse());
                    }
                }

                UNIT_ASSERT(fixture.WriteClient->BelatedAllWritesAnswered.Get(
                    duplicatedHost));
                UNIT_ASSERT(fixture.WriteClient->BelatedCompletedWrites.Get(
                    duplicatedHost));
                UNIT_ASSERT_EQUAL(
                    MakeAllHostsMask(),
                    response.AllWritesAnswered.Include(
                        fixture.WriteClient->BelatedAllWritesAnswered));
                UNIT_ASSERT_VALUES_EQUAL(3, directHosts.size());
            }
        }
    }

    Y_UNIT_TEST_F(
        ShouldReplaceUndeliveredIndirectWithDirect,
        TWriteRequestTestFixture)
    {
        Init();

        THostIndex coordinator = 255;
        TVector<THostIndex> directHosts;

        DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
            [&](ui32,
                THostIndex coordinatorHostIndex,
                THostMask,
                TPBufferKey,
                TBlockRange16,
                TDuration,
                const TGuardedSgList&,
                const TBlockChecksums&,
                const NWilson::TTraceId&,
                IDirectBlockGroup::TWriteBlocksToManyPBuffersCallback callback)
        {
            coordinator = coordinatorHostIndex;
            ManyPBufferCallback = std::move(callback);
        };

        DirectBlockGroup->WriteBlocksToPBufferHandler =
            [&](ui32 vChunkIndex,
                THostIndex hostIndex,
                TPBufferKey,
                TBlockRange16 range,
                const TGuardedSgList&,
                const TBlockChecksums&,
                const NWilson::TTraceId&)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                VChunkConfig.GetVChunkIndex(),
                vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            directHosts.push_back(hostIndex);
            auto response = NewPromise<TDBGWriteBlocksResponse>();
            DirectWritePromises.push_back(response);
            return response.GetFuture();
        };

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        const auto undelivered =
            MakeError(E_FAIL, TString("ERROR ") + UndeliveryErrorMessage);
        TDBGWriteBlocksToManyPBuffersResponse manyResponse;
        const THostIndex desired[] = {0, 1, 2};
        for (auto host: desired) {
            manyResponse.Responses.push_back(
                {.HostIndex = host, .Error = undelivered});
        }
        ManyPBufferCallback(std::move(manyResponse));

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());
        UNIT_ASSERT_VALUES_EQUAL(3, directHosts.size());

        bool wroteOtherDesired = false;
        for (auto host: directHosts) {
            UNIT_ASSERT_VALUES_UNEQUAL(coordinator, host);
            if (host < DefaultPrimaryCount) {
                wroteOtherDesired = true;
            }
        }
        UNIT_ASSERT(wroteOtherDesired);

        for (auto& promise: DirectWritePromises) {
            promise.SetValue(CreateOkDirectResponse());
        }

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& reply = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, reply.Error.GetCode());
        UNIT_ASSERT(!reply.CompletedWrites.Get(coordinator));
        UNIT_ASSERT_VALUES_EQUAL(3, reply.CompletedWrites.Count());
        for (auto host: desired) {
            UNIT_ASSERT(reply.AllWritesAnswered.Get(host));
        }
    }

    Y_UNIT_TEST_F(ShouldWorkWithMultipleResponses, TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        {
            TDBGWriteBlocksToManyPBuffersResponse partResponse;
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{1}, .Error = MakeError(S_OK)});
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{2}, .Error = MakeError(S_OK)});

            ManyPBufferCallback(std::move(partResponse));
        }

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            DirectWritePromises.size());   // retry from main
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());

        ManyPBufferCallback(CreateOneOkResponse(THostIndex{0}));
        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());
    }

    Y_UNIT_TEST_F(
        ShouldWorkWithMultipleResponsesAndHedge,
        TWriteRequestTestFixture)
    {
        Init();

        auto writeRequest = CreateRequestExecutor(
            MakeWriteTestRequestHeaders(Range, BlockSize),
            EWriteMode::IndirectWrite);
        writeRequest->Run();

        //  call hedge mechanism
        RunScheduledHedge();
        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        {
            TDBGWriteBlocksToManyPBuffersResponse partResponse;
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{0}, .Error = MakeError(S_OK)});
            partResponse.Responses.push_back(
                {.HostIndex = THostIndex{2}, .Error = MakeError(S_OK)});

            ManyPBufferCallback(std::move(partResponse));
        }

        UNIT_ASSERT_VALUES_EQUAL(false, WriteClient->Response.has_value());

        {
            ManyPBufferCallback(CreateOneOkResponse(THostIndex{1}));
        }

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());

        UNIT_ASSERT_VALUES_EQUAL(true, WriteClient->Response.has_value());
        const auto& response = *WriteClient->Response;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_EQUAL(3, response.CompletedWrites.Count());

        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());
        DirectWritePromises[2].SetValue(CreateOkDirectResponse());

        UNIT_ASSERT_EQUAL(5, WriteClient->AllCompletedWrites.Count());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
