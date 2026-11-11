#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 TabletId = MakeTabletID(false, 1);

using TConfigureResult = NKikimrCdcAggregator::TEvConfigureTrackingResult;
using TWriteResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;
using THeartbeatResult = TWriteResult::THeartbeatResult;
using TPartitioningAck = NKikimrCdcAggregator::TEvPartitioningChangedAck;
using TDescribeResult = NKikimrCdcAggregator::TEvDescribeTrackingResult;

THolder<TEvCdcAggregator::TEvConfigureTracking> MakeConfigureTracking(const TVector<ui64>& sourceDataShardIds) {
    auto request = MakeHolder<TEvCdcAggregator::TEvConfigureTracking>();
    auto& config = *request->Record.MutableConfig();
    TPathId(72075186233409546, 42).ToProto(config.MutableStreamPathId());
    config.SetStreamPath("/Root/table/stream");
    for (const auto sourceDataShardId : sourceDataShardIds) {
        config.AddSourceDataShardIds(sourceDataShardId);
    }
    for (const ui32 partitionId : {0, 1}) {
        auto& partition = *config.AddTargetPartitions();
        partition.SetPartitionId(partitionId);
        partition.SetTabletId(72075186224037900 + partitionId);
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

void AssertWritten(const TWriteResult& result, const TVector<std::pair<ui64, ui64>>& heartbeats) {
    UNIT_ASSERT_C(result.GetStatus() == TWriteResult::SUCCESS, result.ShortDebugString());
    UNIT_ASSERT_VALUES_EQUAL(result.ResultsSize(), heartbeats.size());
    for (size_t i = 0; i < heartbeats.size(); ++i) {
        const auto& heartbeatResult = result.GetResults(i);
        const auto& [sourceDataShardId, step] = heartbeats[i];
        UNIT_ASSERT_VALUES_EQUAL(heartbeatResult.GetSourceDataShardId(), sourceDataShardId);
        UNIT_ASSERT_C(heartbeatResult.GetStatus() == THeartbeatResult::SUCCESS, heartbeatResult.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(heartbeatResult.GetWrittenVersion()), TRowVersion(step, 0));
    }
}

void AssertCurrentHeartbeat(const TDescribeResult& result, std::optional<ui64> step) {
    UNIT_ASSERT_VALUES_EQUAL_C(result.HasCurrentHeartbeat(), step.has_value(), result.ShortDebugString());
    if (step) {
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(result.GetCurrentHeartbeat()), TRowVersion(*step, 0));
    }
}

void AssertSources(const TDescribeResult& result, const TVector<std::pair<ui64, std::optional<ui64>>>& sources) {
    UNIT_ASSERT_VALUES_EQUAL_C(result.SourcesSize(), sources.size(), result.ShortDebugString());
    for (size_t i = 0; i < sources.size(); ++i) {
        const auto& source = result.GetSources(i);
        const auto& [dataShardId, step] = sources[i];
        UNIT_ASSERT_VALUES_EQUAL(source.GetDataShardId(), dataShardId);
        UNIT_ASSERT_VALUES_EQUAL_C(source.HasLastHeartbeat(), step.has_value(), source.ShortDebugString());
        if (step) {
            UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(source.GetLastHeartbeat()), TRowVersion(*step, 0));
        }
    }
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

    TTestActorRuntime& GetRuntime() {
        return *Runtime;
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
    typename TEvResult::TPtr GrabResult(TDuration timeout = TDuration::Max()) {
        auto ev = Runtime->GrabEdgeEvent<TEvResult>(Edge, timeout);
        if (ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetOrigin(), TabletId);
        }
        return ev;
    }

    TConfigureResult ConfigureTracking(const TVector<ui64>& sourceDataShardIds) {
        SendRequest(MakeConfigureTracking(sourceDataShardIds));
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
        AssertCurrentHeartbeat(describeResult, std::nullopt);
        AssertSources(describeResult, {});
    }

    Y_UNIT_TEST(KeepsProgressWhenSameConfigRepeats) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1, 2, 3}).GetStatus() == TConfigureResult::SUCCESS);
        AssertWritten(env.WriteHeartbeats({{1, 100}}), {{1, 100}});

        const auto result = env.ConfigureTracking({3, 2, 1});

        UNIT_ASSERT_C(result.GetStatus() == TConfigureResult::SUCCESS, result.ShortDebugString());
        AssertSources(env.DescribeTracking(), {{1, 100}, {2, std::nullopt}, {3, std::nullopt}});
    }

    Y_UNIT_TEST(ReturnsConflictWhenConfigDiffers) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1, 2, 3}).GetStatus() == TConfigureResult::SUCCESS);

        const auto result = env.ConfigureTracking({1, 2});

        UNIT_ASSERT_C(result.GetStatus() == TConfigureResult::CONFLICT, result.ShortDebugString());
        AssertSources(env.DescribeTracking(), {{1, std::nullopt}, {2, std::nullopt}, {3, std::nullopt}});
    }

    Y_UNIT_TEST(AdvancesCurrentHeartbeatToMinimum) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1, 2, 3}).GetStatus() == TConfigureResult::SUCCESS);

        AssertWritten(env.WriteHeartbeats({{1, 100}, {2, 110}, {3, 120}}), {{1, 100}, {2, 110}, {3, 120}});

        const auto describeResult = env.DescribeTracking();
        AssertCurrentHeartbeat(describeResult, 100);
        AssertSources(describeResult, {{1, 100}, {2, 110}, {3, 120}});
    }

    Y_UNIT_TEST(WaitsForFirstHeartbeatOfEverySource) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1, 2, 3}).GetStatus() == TConfigureResult::SUCCESS);

        AssertWritten(env.WriteHeartbeats({{1, 100}, {2, 110}}), {{1, 100}, {2, 110}});
        AssertCurrentHeartbeat(env.DescribeTracking(), std::nullopt);

        AssertWritten(env.WriteHeartbeats({{3, 120}}), {{3, 120}});
        AssertCurrentHeartbeat(env.DescribeTracking(), 100);
    }

    Y_UNIT_TEST(RepliesAfterCommitAndRestoresState) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1, 2, 3}).GetStatus() == TConfigureResult::SUCCESS);

        TBlockEvents<TEvTablet::TEvCommitResult> blockedCommits(env.GetRuntime());
        env.SendRequest(MakeWriteHeartbeats({{1, 100}, {2, 110}, {3, 120}}));
        env.GetRuntime().WaitFor("blocked commit", [&] { return !blockedCommits.empty(); });
        UNIT_ASSERT(!env.GrabResult<TEvCdcAggregator::TEvWriteHeartbeatsResult>(TDuration::Seconds(1)));

        blockedCommits.Stop();
        env.RebootTablet();

        const auto describeResult = env.DescribeTracking();
        AssertCurrentHeartbeat(describeResult, 100);
        AssertSources(describeResult, {{1, 100}, {2, 110}, {3, 120}});
        AssertWritten(env.WriteHeartbeats({{1, 100}, {2, 110}, {3, 120}}), {{1, 100}, {2, 110}, {3, 120}});
    }

    Y_UNIT_TEST(ReplacesParentWithChildrenOnce) {
        TTestEnv env;
        UNIT_ASSERT(env.ConfigureTracking({1}).GetStatus() == TConfigureResult::SUCCESS);
        AssertWritten(env.WriteHeartbeats({{1, 100}}), {{1, 100}});

        const auto ack = env.PartitioningChanged(1, {2, 3});
        UNIT_ASSERT_C(ack.GetStatus() == TPartitioningAck::SUCCESS, ack.ShortDebugString());
        AssertWritten(env.WriteHeartbeats({{2, 110}}), {{2, 110}});

        env.RebootTablet();

        const auto repeatedAck = env.PartitioningChanged(1, {2, 3});
        UNIT_ASSERT_C(repeatedAck.GetStatus() == TPartitioningAck::SUCCESS, repeatedAck.ShortDebugString());
        auto describeResult = env.DescribeTracking();
        AssertCurrentHeartbeat(describeResult, 100);
        AssertSources(describeResult, {{2, 110}, {3, std::nullopt}});

        const auto lateResult = env.WriteHeartbeats({{1, 120}});
        UNIT_ASSERT_VALUES_EQUAL(lateResult.ResultsSize(), 1);
        UNIT_ASSERT_C(lateResult.GetResults(0).GetStatus() == THeartbeatResult::UNKNOWN_SOURCE, lateResult.ShortDebugString());

        AssertWritten(env.WriteHeartbeats({{3, 130}}), {{3, 130}});
        describeResult = env.DescribeTracking();
        AssertCurrentHeartbeat(describeResult, 110);
        AssertSources(describeResult, {{2, 110}, {3, 130}});
    }
}

} // namespace NKikimr::NCdcAggregator
