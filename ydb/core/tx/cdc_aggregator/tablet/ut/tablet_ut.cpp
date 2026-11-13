#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 TabletId = MakeTabletID(false, 1);

using TConfigureResult = NKikimrCdcAggregator::TEvConfigureTrackingResult;
using TWriteResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;
using TPartitioningAck = NKikimrCdcAggregator::TEvPartitioningChangedAck;
using TDescribeResult = NKikimrCdcAggregator::TEvDescribeTrackingResult;

THolder<TEvCdcAggregator::TEvConfigureTracking> MakeConfigureTracking(const TVector<ui64>& sourceDataShardIds, ui32 partitionCount) {
    auto request = MakeHolder<TEvCdcAggregator::TEvConfigureTracking>();
    auto& config = *request->Record.MutableConfig();
    TPathId(1, 2).ToProto(config.MutableStreamPathId());
    config.SetStreamPath("/Root/Table/Stream");
    for (const auto sourceDataShardId : sourceDataShardIds) {
        config.AddSourceDataShardIds(sourceDataShardId);
    }
    for (ui32 partitionId = 0; partitionId < partitionCount; ++partitionId) {
        auto& partition = *config.AddTargetPartitions();
        partition.SetPartitionId(partitionId);
        partition.SetTabletId(MakeTabletID(false, 100 + partitionId / 2));
    }
    return request;
}

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

    TConfigureResult ConfigureTracking(const TVector<ui64>& sourceDataShardIds, ui32 partitionCount = 2) {
        SendRequest(MakeConfigureTracking(sourceDataShardIds, partitionCount));
        return GrabResult<TEvCdcAggregator::TEvConfigureTrackingResult>()->Get()->Record;
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

    TVector<ui64> DescribeSources() {
        const auto describeResult = DescribeTracking();
        TVector<ui64> sources;
        for (const auto& source : describeResult.GetSources()) {
            sources.push_back(source.GetDataShardId());
        }
        return sources;
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

    Y_UNIT_TEST(StoresSourcesOnConfigure) {
        TTestEnv env;

        const auto configureResult = env.ConfigureTracking({2, 1});
        UNIT_ASSERT_C(configureResult.GetStatus() == TConfigureResult::SUCCESS, configureResult.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({1, 2}));

        const auto writeResult = env.WriteHeartbeats({{1, 100}});
        UNIT_ASSERT_C(writeResult.GetStatus() == TWriteResult::NOT_READY, writeResult.ShortDebugString());
    }

    Y_UNIT_TEST(ReturnsConflictWhenConfigDiffers) {
        TTestEnv env;
        env.ConfigureTracking({1, 2});

        const auto sameResult = env.ConfigureTracking({2, 1});
        UNIT_ASSERT_C(sameResult.GetStatus() == TConfigureResult::SUCCESS, sameResult.ShortDebugString());

        const auto otherSourcesResult = env.ConfigureTracking({1, 3});
        UNIT_ASSERT_C(otherSourcesResult.GetStatus() == TConfigureResult::CONFLICT, otherSourcesResult.ShortDebugString());

        const auto otherPartitionsResult = env.ConfigureTracking({1, 2}, 3);
        UNIT_ASSERT_C(otherPartitionsResult.GetStatus() == TConfigureResult::CONFLICT, otherPartitionsResult.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({1, 2}));
    }

    Y_UNIT_TEST(RejectsInvalidConfig) {
        TTestEnv env;

        const auto noSourcesResult = env.ConfigureTracking({});
        UNIT_ASSERT_C(noSourcesResult.GetStatus() == TConfigureResult::BAD_REQUEST, noSourcesResult.ShortDebugString());

        const auto duplicateResult = env.ConfigureTracking({1, 1});
        UNIT_ASSERT_C(duplicateResult.GetStatus() == TConfigureResult::BAD_REQUEST, duplicateResult.ShortDebugString());

        const auto noPartitionsResult = env.ConfigureTracking({1}, 0);
        UNIT_ASSERT_C(noPartitionsResult.GetStatus() == TConfigureResult::BAD_REQUEST, noPartitionsResult.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>());
    }

    Y_UNIT_TEST(ReplacesParentWithChildren) {
        TTestEnv env;
        env.ConfigureTracking({1, 2});

        const auto splitAck = env.PartitioningChanged(1, {3, 4});
        UNIT_ASSERT_C(splitAck.GetStatus() == TPartitioningAck::SUCCESS, splitAck.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({2, 3, 4}));

        const auto repeatedAck = env.PartitioningChanged(1, {3, 4});
        UNIT_ASSERT_C(repeatedAck.GetStatus() == TPartitioningAck::SUCCESS, repeatedAck.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({2, 3, 4}));

        const auto firstMergeAck = env.PartitioningChanged(3, {5});
        UNIT_ASSERT_C(firstMergeAck.GetStatus() == TPartitioningAck::SUCCESS, firstMergeAck.ShortDebugString());
        const auto secondMergeAck = env.PartitioningChanged(4, {5});
        UNIT_ASSERT_C(secondMergeAck.GetStatus() == TPartitioningAck::SUCCESS, secondMergeAck.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({2, 5}));
    }

    Y_UNIT_TEST(RejectsUnknownAndInvalidPartitioningChanges) {
        TTestEnv env;
        env.ConfigureTracking({1, 2});

        const auto unknownAck = env.PartitioningChanged(7, {8});
        UNIT_ASSERT_C(unknownAck.GetStatus() == TPartitioningAck::UNKNOWN_SOURCE, unknownAck.ShortDebugString());

        const auto selfAck = env.PartitioningChanged(1, {1, 3});
        UNIT_ASSERT_C(selfAck.GetStatus() == TPartitioningAck::BAD_REQUEST, selfAck.ShortDebugString());

        const auto noChildrenAck = env.PartitioningChanged(1, {});
        UNIT_ASSERT_C(noChildrenAck.GetStatus() == TPartitioningAck::BAD_REQUEST, noChildrenAck.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({1, 2}));
    }

    Y_UNIT_TEST(KeepsSourcesAfterRestart) {
        TTestEnv env;
        env.ConfigureTracking({1, 2});
        env.PartitioningChanged(1, {3, 4});

        env.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({2, 3, 4}));

        const auto sameResult = env.ConfigureTracking({1, 2});
        UNIT_ASSERT_C(sameResult.GetStatus() == TConfigureResult::SUCCESS, sameResult.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.DescribeSources(), TVector<ui64>({2, 3, 4}));

        const auto otherResult = env.ConfigureTracking({1, 3});
        UNIT_ASSERT_C(otherResult.GetStatus() == TConfigureResult::CONFLICT, otherResult.ShortDebugString());
    }
}

} // namespace NKikimr::NCdcAggregator
