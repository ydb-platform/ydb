#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 TabletId = MakeTabletID(false, 1);
constexpr ui64 SchemeShardId = MakeTabletID(false, 2);
constexpr ui64 PersQueueTabletId = MakeTabletID(false, 3);
const TPathId StreamPathId(SchemeShardId, 42);

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
        PipeClient = {};
    }

    void SendRequest(IEventBase* request) {
        if (!PipeClient) {
            PipeClient = Runtime->ConnectToPipe(TabletId, Edge, 0, GetPipeConfigWithRetries());
        }
        Runtime->SendToPipe(PipeClient, Edge, request);
    }

    template <class TResult>
    typename TResult::ProtoRecordType GrabResult() {
        auto ev = Runtime->GrabEdgeEvent<TResult>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetOrigin(), TabletId);
        return ev->Get()->Record;
    }

    template <class TResult>
    typename TResult::ProtoRecordType SendAndGrab(IEventBase* request) {
        SendRequest(request);
        return GrabResult<TResult>();
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
    TActorId PipeClient;
};

TEvCdcAggregator::TEvConfigure* MakeConfigure(const TVector<ui64>& sources, ui64 version = 1) {
    auto* ev = new TEvCdcAggregator::TEvConfigure();
    auto& config = *ev->Record.MutableConfig();
    StreamPathId.ToProto(config.MutableStreamPathId());
    config.SetSchemeShardId(SchemeShardId);
    config.SetConfigVersion(version);
    for (const auto source : sources) {
        config.AddSourceTabletIds(source);
    }
    for (const ui32 partitionId : {0, 1}) {
        auto& partition = *config.AddPartitions();
        partition.SetPartitionId(partitionId);
        partition.SetTabletId(PersQueueTabletId);
    }
    return ev;
}

TEvCdcAggregator::TEvActivate* MakeActivate(ui64 version = 1) {
    auto* ev = new TEvCdcAggregator::TEvActivate();
    StreamPathId.ToProto(ev->Record.MutableStreamPathId());
    ev->Record.SetConfigVersion(version);
    return ev;
}

TEvCdcAggregator::TEvReportDeliveredBatch* MakeReports(const TVector<std::pair<ui64, ui64>>& reports, const TPathId& streamPathId = StreamPathId) {
    auto* ev = new TEvCdcAggregator::TEvReportDeliveredBatch();
    streamPathId.ToProto(ev->Record.MutableStreamPathId());
    for (const auto& [source, step] : reports) {
        auto& report = *ev->Record.AddReports();
        report.SetSourceTabletId(source);
        TRowVersion(step, 0).ToProto(report.MutableDelivered()->MutableVersion());
        report.MutableDelivered()->SetPayload(TStringBuilder() << "{\"resolved\":[" << step << ",0]}");
    }
    return ev;
}

TEvCdcAggregator::TEvReplaceSource* MakeReplace(ui64 parent, const TVector<ui64>& children) {
    auto* ev = new TEvCdcAggregator::TEvReplaceSource();
    StreamPathId.ToProto(ev->Record.MutableStreamPathId());
    ev->Record.SetParentTabletId(parent);
    for (const auto child : children) {
        ev->Record.AddChildTabletIds(child);
    }
    return ev;
}

using TConfigureResult = NKikimrCdcAggregator::TEvConfigureResult;
using TActivateResult = NKikimrCdcAggregator::TEvActivateResult;
using TReportResult = NKikimrCdcAggregator::TEvReportDeliveredBatchResult;
using TReplaceResult = NKikimrCdcAggregator::TEvReplaceSourceResult;

NKikimrCdcAggregator::TEvDescribeStreamResult DescribeStream(TTestEnv& env) {
    return env.SendAndGrab<TEvCdcAggregator::TEvDescribeStreamResult>(new TEvCdcAggregator::TEvDescribeStream());
}

TRowVersion PersistedVersion(const TReportResult& result, size_t index) {
    UNIT_ASSERT_C(result.GetResults(index).GetStatus() == TReportResult::TSourceResult::SUCCESS, result.ShortDebugString());
    return TRowVersion::FromProto(result.GetResults(index).GetPersistedVersion());
}

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorTablet) {

    Y_UNIT_TEST(ShouldAnswerNotReady_WhenReportArrivesBeforeConfiguration) {
        // given
        TTestEnv env;

        // when
        const auto result = env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 100}}));

        // then
        UNIT_ASSERT(result.GetStatus() == TReportResult::NOT_READY);
        UNIT_ASSERT_VALUES_EQUAL(result.ResultsSize(), 0);
    }

    Y_UNIT_TEST(ShouldRestoreStream_WhenTabletRestarts) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2, 3}));
        env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 120}, {2, 100}, {3, 110}}));

        // when
        env.RebootTablet();
        const auto description = DescribeStream(env);
        const auto older = env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{2, 90}}));

        // then
        UNIT_ASSERT_VALUES_EQUAL(description.GetConfig().GetConfigVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(description.SourcesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(description.GetReadyBarrier().GetVersion()), TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(description.GetReadyBarrier().GetPayload(), "{\"resolved\":[100,0]}");
        UNIT_ASSERT_VALUES_EQUAL(PersistedVersion(older, 0), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenSameConfigurationRepeatsAfterRestart) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2}));
        env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 100}}));
        env.RebootTablet();

        // when
        const auto repeated = env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2}));
        const auto conflicting = env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2, 3}, 2));
        const auto description = DescribeStream(env);

        // then
        UNIT_ASSERT(repeated.GetStatus() == TConfigureResult::SUCCESS);
        UNIT_ASSERT(conflicting.GetStatus() == TConfigureResult::CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(conflicting.GetConfigVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(description.GetSources(0).GetSourceTabletId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(description.GetSources(0).GetDelivered()), TRowVersion(100, 0));
        UNIT_ASSERT(!description.GetSources(1).HasDelivered());
    }

    Y_UNIT_TEST(ShouldNotAcknowledge_WhenCommitIsNotWritten) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1}));
        auto& runtime = env.GetRuntime();
        TBlockEvents<TEvBlobStorage::TEvPut> commits(runtime, [](const auto& ev) {
            return ev->Get()->Id.TabletID() == TabletId;
        });
        size_t results = 0;
        auto observer = runtime.AddObserver<TEvCdcAggregator::TEvReportDeliveredBatchResult>([&](auto&) {
            ++results;
        });

        // when
        env.SendRequest(MakeReports({{1, 100}}));
        runtime.WaitFor("blocked commit", [&] { return !commits.empty(); });
        runtime.SimulateSleep(TDuration::Seconds(1));
        const size_t resultsBeforeCommit = results;
        commits.Stop().Unblock();
        const auto result = env.GrabResult<TEvCdcAggregator::TEvReportDeliveredBatchResult>();

        // then
        UNIT_ASSERT_VALUES_EQUAL(resultsBeforeCommit, 0);
        UNIT_ASSERT_VALUES_EQUAL(PersistedVersion(result, 0), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldAcknowledgeRetry_WhenReplyIsLostAfterCommit) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2}));
        auto& runtime = env.GetRuntime();
        TBlockEvents<TEvCdcAggregator::TEvReportDeliveredBatchResult> lostReplies(runtime);
        env.SendRequest(MakeReports({{1, 100}}));
        runtime.WaitFor("lost reply", [&] { return !lostReplies.empty(); });
        lostReplies.Stop();
        lostReplies.clear();

        // when
        env.RebootTablet();
        const auto retry = env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 100}}));

        // then
        UNIT_ASSERT_VALUES_EQUAL(PersistedVersion(retry, 0), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldKeepReplacement_WhenTabletRestarts) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1, 2}));
        env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 100}, {2, 110}}));
        const auto replaced = env.SendAndGrab<TEvCdcAggregator::TEvReplaceSourceResult>(MakeReplace(1, {4, 3}));
        env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{3, 130}}));

        // when
        env.RebootTablet();
        const auto repeated = env.SendAndGrab<TEvCdcAggregator::TEvReplaceSourceResult>(MakeReplace(1, {3, 4}));
        const auto lateParent = env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(MakeReports({{1, 200}}));
        const auto description = DescribeStream(env);

        // then
        UNIT_ASSERT(replaced.GetStatus() == TReplaceResult::SUCCESS);
        UNIT_ASSERT(repeated.GetStatus() == TReplaceResult::SUCCESS);
        UNIT_ASSERT(lateParent.GetResults(0).GetStatus() == TReportResult::TSourceResult::RETIRED_SOURCE);
        UNIT_ASSERT_VALUES_EQUAL(description.SourcesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(description.GetSources(1).GetDelivered()), TRowVersion(130, 0));
        UNIT_ASSERT(!description.GetSources(2).HasDelivered());
        UNIT_ASSERT_VALUES_EQUAL(description.RetiredSourceTabletIdsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(description.GetReadyBarrier().GetVersion()), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldRejectRequests_WhenStreamDiffers) {
        // given
        TTestEnv env;
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1}));

        // when
        const auto result = env.SendAndGrab<TEvCdcAggregator::TEvReportDeliveredBatchResult>(
            MakeReports({{1, 100}}, TPathId(SchemeShardId, 43)));

        // then
        UNIT_ASSERT(result.GetStatus() == TReportResult::WRONG_STREAM);
    }

    Y_UNIT_TEST(ShouldStayActive_WhenTabletRestarts) {
        // given
        TTestEnv env;
        const auto notReady = env.SendAndGrab<TEvCdcAggregator::TEvActivateResult>(MakeActivate());
        env.SendAndGrab<TEvCdcAggregator::TEvConfigureResult>(MakeConfigure({1}));
        const auto wrongVersion = env.SendAndGrab<TEvCdcAggregator::TEvActivateResult>(MakeActivate(2));
        const auto activated = env.SendAndGrab<TEvCdcAggregator::TEvActivateResult>(MakeActivate());

        // when
        env.RebootTablet();
        const auto description = DescribeStream(env);

        // then
        UNIT_ASSERT(notReady.GetStatus() == TActivateResult::NOT_READY);
        UNIT_ASSERT(wrongVersion.GetStatus() == TActivateResult::CONFLICT);
        UNIT_ASSERT(activated.GetStatus() == TActivateResult::SUCCESS);
        UNIT_ASSERT(description.GetActive());
    }
}

} // namespace NKikimr::NCdcAggregator
