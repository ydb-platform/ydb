#include "erase_request.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEraseRequestTest)
{
    Y_UNIT_TEST_F(AllOK, TBaseFixture)
    {
        Init();

        THostIndex host = 1;
        TEraseHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 43,
            .Range = TBlockRange64::WithLength(20, 3)});

        auto eraseRequest = std::make_shared<TEraseRequestExecutor>(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            host,
            std::move(hint),
            NWilson::TSpan());

        auto future = eraseRequest->GetFuture();
        eraseRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, ErasePromises.size());
        SetEraseResult(TDBGEraseResponse{.Error = MakeError(S_OK)}, false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(2, response.EraseOk.size());
        UNIT_ASSERT_VALUES_EQUAL(42, response.EraseOk[0]);
        UNIT_ASSERT_VALUES_EQUAL(43, response.EraseOk[1]);
        UNIT_ASSERT_VALUES_EQUAL(0, response.EraseFailed.size());
    }

}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
