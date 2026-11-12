#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 TabletId = MakeTabletID(false, 1);

using TWriteResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;
using TPartitioningAck = NKikimrCdcAggregator::TEvPartitioningChangedAck;
using TDescribeResult = NKikimrCdcAggregator::TEvDescribeTrackingResult;

THolder<TEvCdcAggregator::TEvWriteHeartbeats> MakeWriteHeartbeats(const TVector<std::pair<ui64, ui64>>& heartbeats) {
    auto request = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeats>();
    for (const auto& [sourceDataShardId, step] : heartbeats) {
        auto& heartbeat = *request->Record.AddHeartbeats();
        heartbeat.SetSourceDataShardId(sourceDataShardId);
        NPQ::THeartbeat{
            .Version = TRowVersion(step, 0),
            .Data = TStringBuilder() << "{\"resolved\":[" << step << ",0]}",
        }.Serialize(*heartbeat.MutableHeartbeat());
    }
    return request;
}

THolder<TEvCdcAggregator::TEvPartitioningChanged> MakePartitioningChanged(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds) {
    auto request = MakeHolder<TEvCdcAggregator::TEvPartitioningChanged>();
    request->Record.SetParentDataShardId(parentDataShardId);
    for (const auto childDataShardId : childDataShardIds) {
        request->Record.AddChildDataShardIds(childDataShardId);
    }
    request->Record.SetOperationId(77);
    return request;
}

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

    void SendRequest(THolder<IEventBase> request) {
        Runtime->SendToPipe(TabletId, Edge, request.Release(), 0, GetPipeConfigWithRetries());
    }

    template <typename TEvResult>
    typename TEvResult::TPtr GrabResult() {
        auto ev = Runtime->GrabEdgeEvent<TEvResult>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetOrigin(), TabletId);
        return ev;
    }

    TWriteResult WriteHeartbeats(const TVector<std::pair<ui64, ui64>>& heartbeats) {
        SendRequest(MakeWriteHeartbeats(heartbeats));
        return GrabResult<TEvCdcAggregator::TEvWriteHeartbeatsResult>()->Get()->Record;
    }

    TPartitioningAck PartitioningChanged(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds) {
        SendRequest(MakePartitioningChanged(parentDataShardId, childDataShardIds));
        return GrabResult<TEvCdcAggregator::TEvPartitioningChangedAck>()->Get()->Record;
    }

    TDescribeResult DescribeTracking() {
        SendRequest(MakeHolder<TEvCdcAggregator::TEvDescribeTracking>());
        return Runtime->GrabEdgeEvent<TEvCdcAggregator::TEvDescribeTrackingResult>(Edge)->Get()->Record;
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

    Y_UNIT_TEST(ReturnsNotReadyBeforeConfigure) {
        TTestEnv env;

        const auto writeResult = env.WriteHeartbeats({{1, 100}});
        UNIT_ASSERT_C(writeResult.GetStatus() == TWriteResult::NOT_READY, writeResult.ShortDebugString());

        const auto partitioningAck = env.PartitioningChanged(1, {2, 3});
        UNIT_ASSERT_C(partitioningAck.GetStatus() == TPartitioningAck::NOT_READY, partitioningAck.ShortDebugString());

        const auto describeResult = env.DescribeTracking();
        UNIT_ASSERT_C(!describeResult.HasCurrentHeartbeat(), describeResult.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(describeResult.SourcesSize(), 0, describeResult.ShortDebugString());
    }
}

} // namespace NKikimr::NCdcAggregator
