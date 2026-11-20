#include "restore_request.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRestoreRequestTest)
{
    Y_UNIT_TEST_F(ShouldRetryHostWhoseListFailed, TBaseFixture)
    {
        Init();

        struct TListCall
        {
            THostIndex Host = 0;
            TPromise<TListPBufferResponse> Promise;
        };
        TVector<TListCall> calls;

        DirectBlockGroup->ListPBuffersHandler = [&](THostIndex host)
        {
            TListCall call;
            call.Host = host;
            call.Promise = NewPromise<TListPBufferResponse>();
            auto future = call.Promise.GetFuture();
            auto guard = TGuard(PromisesGuard);
            calls.push_back(std::move(call));
            return future;
        };

        auto restore = std::make_shared<TRestoreRequestExecutor>(
            Runtime->GetActorSystem(0),
            DirectBlockGroup);
        auto future = restore->GetFuture();
        restore->Run();

        UNIT_ASSERT_VALUES_EQUAL(DirectBlockGroupHostCount, calls.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        for (auto& call: calls) {
            TListPBufferResponse response;
            response.Error =
                call.Host == 0 ? MakeError(E_FAIL) : MakeError(S_OK);
            call.Promise.SetValue(std::move(response));
        }

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitScheduledTasks(1, TDuration::Seconds(10)));
        RunScheduledTasks().GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount + 1,
            calls.size());
        UNIT_ASSERT_VALUES_EQUAL(THostIndex{0}, calls.back().Host);

        calls.back().Promise.SetValue(
            TListPBufferResponse{.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            response.Meta.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
