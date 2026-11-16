#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/service/service.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 Cookie = 42;

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorServiceTest) {
    Y_UNIT_TEST(ReturnsRetryToWriteHeartbeat) {
        TTestBasicRuntime runtime(1, false);
        SetupTabletServices(runtime);

        const auto serviceId = MakeCdcAggregatorServiceId(runtime.GetNodeId(0));
        runtime.RegisterService(serviceId, runtime.Register(CreateCdcAggregatorService()));

        auto request = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeat>();
        request->RecipientTabletId = MakeTabletID(false, 1);
        request->SourceDataShardId = MakeTabletID(false, 2);
        request->Heartbeat = NPQ::THeartbeat{
            .Version = TRowVersion(100, 0),
            .Data = "{\"resolved\":[100,0]}",
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(serviceId, edge, request.Release(), 0, Cookie));

        auto result = runtime.GrabEdgeEvent<TEvCdcAggregatorService::TEvWriteHeartbeatResult>(edge);
        UNIT_ASSERT_C(result->Get()->Status == TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Retry, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, Cookie);
    }
}

} // namespace NKikimr::NCdcAggregator
