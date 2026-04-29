#include "read_request.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadRequestTest)
{
    Y_UNIT_TEST_F(ShouldRead, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint =
            DirtyMap.MakeReadHint(range, DDiskReadable(), PBufferActive());
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1, .Range = range});

        auto readRequest = std::make_shared<TReadRequestExecutor>(
            Runtime->GetActorSystem(0),
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
