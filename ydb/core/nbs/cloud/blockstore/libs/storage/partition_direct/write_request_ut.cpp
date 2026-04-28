#include "write_request.h"

#include "base_test_fixture.h"
#include "write_with_direct_replication_request.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteRequestTest)
{
    Y_UNIT_TEST_F(ShouldWrite, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);

        Init();

        TCallback hedgeCallback;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, delay);
            hedgeCallback = std::move(callback);
        };

        TMap<ui8, TPromise<TDBGWriteBlocksResponse>> writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
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
            TRequestHeaders{.RequestId = 1, .Range = range});
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
                hedgeDelay);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[2].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
