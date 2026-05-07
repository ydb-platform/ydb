#include "write_with_pb_test_fixture.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

TWriteWithPbTestFixture::TWriteWithPbTestFixture():
    UserLsn{ 123 },
    Range{ TBlockRange64::WithLength(10, 10) },
    HedgeDelay{ TDuration::MilliSeconds(1000) },
    Timeout{ TDuration::MilliSeconds(1000) },
    PBufferReplyTimeout{ TDuration::MilliSeconds(500) }
{
    Init();
    ExpectedRange = Range;
    RangeData = GenerateRandomString(BlockSize * Range.Size());

    ManyPBufferPromise = NewPromise<TDBGWriteBlocksToManyPBuffersResponse>();

    DirectBlockGroup->ScheduleHandler =
        [&](TDuration delay, TCallback callback)
    {
        Scheduled.emplace_back(delay, std::move(callback));
    };
}

TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
TWriteWithPbTestFixture::GetManyPBuffersHandlerWithImmediateOkResponse()
{
    auto result = [&](ui32 vChunkIndex,
                std::vector<ui8> hostIndexes,
                ui64 lsn,
                TBlockRange64 range,
                TDuration replyTimeout,
                const TGuardedSgList& guardedSglist,
                const NWilson::TTraceId& traceId)   //
            -> TFuture<TDBGWriteBlocksToManyPBuffersResponse>
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(UserLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);
            UNIT_ASSERT_VALUES_EQUAL(PBufferReplyTimeout, replyTimeout);

            UNIT_ASSERT_VALUES_EQUAL(3u, hostIndexes.size());
            auto itPB0 =
                std::ranges::find(hostIndexes, VChunkConfig.PrimaryHost0);
            UNIT_ASSERT_VALUES_UNEQUAL(itPB0, hostIndexes.end());

            auto itPB1 =
                std::ranges::find(hostIndexes, VChunkConfig.PrimaryHost1);
            UNIT_ASSERT_VALUES_UNEQUAL(itPB1, hostIndexes.end());

            auto itPB2 =
                std::ranges::find(hostIndexes, VChunkConfig.PrimaryHost2);
            UNIT_ASSERT_VALUES_UNEQUAL(itPB2, hostIndexes.end());

            TDBGWriteBlocksToManyPBuffersResponse okResponse;
            okResponse.OverallError = MakeError(S_OK);
            okResponse.Responses.push_back(
                {.HostIndex = VChunkConfig.PrimaryHost0, .Error = MakeError(S_OK)});
            okResponse.Responses.push_back(
                {.HostIndex = VChunkConfig.PrimaryHost1, .Error = MakeError(S_OK)});
            okResponse.Responses.push_back(
                {.HostIndex = VChunkConfig.PrimaryHost2, .Error = MakeError(S_OK)});

            ManyPBufferPromise.SetValue(std::move(okResponse));
            return ManyPBufferPromise.GetFuture();
        };

    return result;
}

std::shared_ptr<TWriteWithPbReplicationRequestExecutor>
TWriteWithPbTestFixture::CreateRequest(TRequestHeaders headers)
{
    auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
    auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(std::move(headers));
    originalRequest->Sglist = MakeSgList();

    return std::make_shared<TWriteWithPbReplicationRequestExecutor>(
        Runtime->GetActorSystem(0),
        VChunkConfig,
        DirectBlockGroup,
        Range,
        std::move(callContext),
        std::move(originalRequest),
        UserLsn,
        NWilson::TTraceId(),
        HedgeDelay,
        Timeout,
        PBufferReplyTimeout);
}

} // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
