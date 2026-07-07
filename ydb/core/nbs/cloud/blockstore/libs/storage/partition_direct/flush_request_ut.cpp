#include "flush_request.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlushRequestTest)
{
    Y_UNIT_TEST_F(AllOK, TBaseFixture)
    {
        Init();

        THostRoute route{.SourceHostIndex = 0, .DestinationHostIndex = 0};
        TFlushHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 43,
            .Range = TBlockRange64::WithLength(20, 3)});

        auto flushRequest = std::make_shared<TFlushRequestExecutor>(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            NWilson::TSpan());

        auto future = flushRequest->GetFuture();
        flushRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, FlushPromises.size());
        SetFlushResult(
            TDBGFlushResponse{.Errors{MakeError(S_OK), MakeError(S_OK)}},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(2, response.FlushOk.size());
        UNIT_ASSERT_VALUES_EQUAL(42, response.FlushOk[0]);
        UNIT_ASSERT_VALUES_EQUAL(43, response.FlushOk[1]);
        UNIT_ASSERT_VALUES_EQUAL(0, response.FlushFailed.size());
        UNIT_ASSERT_EQUAL_C(route, response.Route, response.Route.DebugPrint());
    }

    Y_UNIT_TEST_F(PartialOK, TBaseFixture)
    {
        Init();

        THostRoute route{.SourceHostIndex = 1, .DestinationHostIndex = 2};
        TFlushHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 43,
            .Range = TBlockRange64::WithLength(20, 3)});

        auto flushRequest = std::make_shared<TFlushRequestExecutor>(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            NWilson::TSpan());

        auto future = flushRequest->GetFuture();
        flushRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, FlushPromises.size());
        SetFlushResult(
            TDBGFlushResponse{.Errors{MakeError(S_OK), MakeError(E_FAIL)}},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(1, response.FlushOk.size());
        UNIT_ASSERT_VALUES_EQUAL(42, response.FlushOk[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, response.FlushFailed.size());
        UNIT_ASSERT_VALUES_EQUAL(43, response.FlushFailed[0]);
        UNIT_ASSERT_EQUAL_C(route, response.Route, response.Route.DebugPrint());
    }

    Y_UNIT_TEST_F(AllFailed, TBaseFixture)
    {
        Init();

        THostRoute route{.SourceHostIndex = 1, .DestinationHostIndex = 2};
        TFlushHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 43,
            .Range = TBlockRange64::WithLength(20, 3)});

        auto flushRequest = std::make_shared<TFlushRequestExecutor>(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            NWilson::TSpan());

        auto future = flushRequest->GetFuture();
        flushRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, FlushPromises.size());
        SetFlushResult(
            TDBGFlushResponse{.Errors{MakeError(E_FAIL), MakeError(E_FAIL)}},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(0, response.FlushOk.size());
        UNIT_ASSERT_VALUES_EQUAL(2, response.FlushFailed.size());
        UNIT_ASSERT_VALUES_EQUAL(42, response.FlushFailed[0]);
        UNIT_ASSERT_VALUES_EQUAL(43, response.FlushFailed[1]);
        UNIT_ASSERT_EQUAL_C(route, response.Route, response.Route.DebugPrint());
    }

    Y_UNIT_TEST_F(FailOnTimeout, TBaseFixture)
    {
        Init();
        DirectBlockGroup->Oracle.FlushRequestTimeout = TDuration::Seconds(5);

        THostRoute route{.SourceHostIndex = 1, .DestinationHostIndex = 2};
        TFlushHint hint;
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 42,
            .Range = TBlockRange64::WithLength(10, 3)});
        hint.Segments.push_back(TPBufferSegment{
            .Lsn = 43,
            .Range = TBlockRange64::WithLength(20, 3)});

        auto flushRequest = std::make_shared<TFlushRequestExecutor>(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            NWilson::TSpan());

        auto future = flushRequest->GetFuture();
        flushRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(1, FlushPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroup->Oracle.FlushRequestTimeout,
            ScheduledTasks[0].Delay);
        // Timeout.
        ScheduledTasks[0].Callback();

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(0, response.FlushOk.size());
        UNIT_ASSERT_VALUES_EQUAL(2, response.FlushFailed.size());
        UNIT_ASSERT_VALUES_EQUAL(42, response.FlushFailed[0]);
        UNIT_ASSERT_VALUES_EQUAL(43, response.FlushFailed[1]);
        UNIT_ASSERT_EQUAL_C(route, response.Route, response.Route.DebugPrint());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
