#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 TabletId = MakeTabletID(false, 1);

class TTestEnv {
public:
    TTestEnv()
        : Runtime(MakeHolder<TTestBasicRuntime>(1, false))
    {
        Runtime->SetLogPriority(NKikimrServices::CDC_AGGREGATOR, NLog::PRI_TRACE);
        SetupTabletServices(*Runtime);

        const auto bootstrapper = CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, TTabletTypes::CdcAggregator), &CreateCdcAggregator);
        Runtime->EnableScheduleForActor(bootstrapper);
        WaitTabletBoot();

        Edge = Runtime->AllocateEdgeActor();
    }

    void RebootTablet() {
        ForwardToTablet(*Runtime, TabletId, TActorId(), new TEvents::TEvPoison);
        WaitTabletBoot();
        InvalidateTabletResolverCache(*Runtime, TabletId);
    }

    void ConnectPipe() {
        const auto pipeClient = Runtime->ConnectToPipe(TabletId, Edge, 0, GetPipeConfigWithRetries());
        auto ev = Runtime->GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TabletId, TabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
        Runtime->ClosePipe(pipeClient, Edge, 0);
    }

private:
    void WaitTabletBoot() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime->DispatchEvents(options);
    }

private:
    THolder<TTestBasicRuntime> Runtime;
    TActorId Edge;
};

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorTabletTest) {
    Y_UNIT_TEST(BecomesActive) {
        TTestEnv env;
        env.ConnectPipe();
    }

    Y_UNIT_TEST(BecomesActiveAfterRestart) {
        TTestEnv env;
        env.ConnectPipe();
        env.RebootTablet();
        env.ConnectPipe();
    }
}

} // namespace NKikimr::NCdcAggregator
