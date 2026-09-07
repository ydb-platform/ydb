#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/query_metrics_limits.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/sys_view/service/query_interval.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {
namespace {

class TQueryMetricsEnv {
public:
    using TRequest = TEvSysView::TEvGetIntervalMetricsRequest::TPtr;
    using TEntries = TVector<NKikimrSysView::TQueryMetricsEntry>;

    static constexpr TDuration CollectionInterval = TDuration::Seconds(5);
    const ui64 TabletId = MakeTabletID(false, 1);
    const TString Database = "/Root/Test";
    TTestBasicRuntime Runtime;
    TActorId Edge;
    TActorId Processor;
    TVector<TActorId> NodeEdges;
    TVector<TQueryInterval> NodeMetrics;
    TInstant IntervalEnd;

    explicit TQueryMetricsEnv(ui32 nodes = 1,
        TInstant start = TInstant::Seconds(1'800'001'800))
        : Runtime(nodes)
        , NodeMetrics(nodes)
        , IntervalEnd(start)
    {
        TAppPrepare app;
        app.SetEnablePersistentQueryStats(true);
        app.SetEnableDbCounters(false);
        SetupTabletServices(Runtime, &app, /* mockDisk */ true);
        Runtime.UpdateCurrentTime(start);
        Runtime.SetScheduledLimit(1'000);
        Runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_DEBUG);
        Edge = Runtime.AllocateEdgeActor();
        for (ui32 node = 0; node < nodes; ++node) {
            // The real service uses wall-clock time. Stop its timers before
            // replacing it with a controllable responder in simulated time.
            const auto serviceId = MakeSysViewServiceID(Runtime.GetNodeId(node));
            Runtime.Send(new IEventHandle(Runtime.GetLocalServiceId(serviceId, node),
                Edge, new TEvents::TEvPoison), node, /* viaActorSystem */ true);
            NodeEdges.push_back(Runtime.AllocateEdgeActor(node));
            Runtime.RegisterService(serviceId, NodeEdges.back(), node);
        }

        const auto bootstrapper = CreateTestBootstrapper(Runtime,
            CreateTestTabletInfo(TabletId, TTabletTypes::SysViewProcessor),
            &CreateSysViewProcessorForTests);
        Runtime.EnableScheduleForActor(bootstrapper);
        TDispatchOptions boot;
        boot.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
        UNIT_ASSERT(Runtime.DispatchEvents(boot, TDuration::Seconds(10)));
        Processor = ResolveTablet(Runtime, TabletId);
        Sync();
    }

    // The configure acknowledgement is emitted from Complete, after the
    // preceding writes have committed. It also provides a readiness barrier.
    void Sync() {
        auto request = MakeHolder<TEvSysView::TEvConfigureProcessor>();
        request->Record.SetDatabase(Database);
        Runtime.Send(new IEventHandle(Processor, Edge, request.Release()));
        Runtime.GrabEdgeEvent<TEvSubDomain::TEvConfigureStatus>(Edge);
    }

    void Reboot() {
        Sync();
        RebootTablet(Runtime, TabletId, Edge);
        Processor = ResolveTablet(Runtime, TabletId);
        Sync();
    }

    void NextInterval() {
        IntervalEnd += CollectionInterval;
        // Execute the scheduled reset, but stop before aggregation of the new
        // interval. Only simulated time advances; no wall-clock sleeps.
        const auto collectTime = IntervalEnd + TDuration::Seconds(1);
        UNIT_ASSERT_LT(Runtime.GetCurrentTime(), collectTime);
        Runtime.SimulateSleep(collectTime - Runtime.GetCurrentTime());
        for (auto& metrics : NodeMetrics) {
            metrics.Clear();
        }
        Sync();
    }

    void Add(ui32 node, ui64 hash, ui64 cpu, const TString& text = "query",
        ui64 durationMs = 1, ui64 readRows = 0, ui64 brokenLocks = 0)
    {
        auto stats = std::make_shared<NKikimrSysView::TQueryStats>();
        stats->SetQueryTextHash(hash);
        stats->SetQueryText(text);
        stats->SetTotalCpuTimeUs(cpu);
        stats->SetDurationMs(durationMs);
        stats->MutableStats()->SetReadRows(readRows);
        stats->SetLocksBrokenAsBreaker(brokenLocks);
        NodeMetrics.at(node).Add(std::move(stats));
    }

    void Submit(ui32 node) {
        auto summary = MakeHolder<TEvSysView::TEvIntervalQuerySummary>();
        auto& record = summary->Record;
        record.SetDatabase(Database);
        record.SetNodeId(Runtime.GetNodeId(node));
        record.SetIntervalEndUs(IntervalEnd.MicroSeconds());
        NodeMetrics.at(node).FillSummary(*record.MutableMetrics());
        record.SetQueryMetricsTotalCpuTimeUs(NodeMetrics.at(node).GetTotalCpuTimeUs());
        record.SetQueryMetricsRetainedCpuTimeUs(NodeMetrics.at(node).GetRetainedCpuTimeUs());
        Runtime.Send(new IEventHandle(Processor, NodeEdges.at(node), summary.Release()));
    }

    TRequest TakeRequest(ui32 node) {
        auto request = Runtime.GrabEdgeEvent<TEvSysView::TEvGetIntervalMetricsRequest>(
            NodeEdges.at(node), CollectionInterval);
        UNIT_ASSERT(request);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetIntervalEndUs(),
            IntervalEnd.MicroSeconds());
        return request;
    }

    void Reply(ui32 node, const TRequest& request) {
        auto response = MakeHolder<TEvSysView::TEvGetIntervalMetricsResponse>();
        response->Record.SetIntervalEndUs(request->Get()->Record.GetIntervalEndUs());
        NodeMetrics.at(node).FillMetrics(request->Get()->Record, response->Record);
        Runtime.Send(new IEventHandle(request->Sender, NodeEdges.at(node),
            response.Release(), 0, request->Cookie));
    }

    void Fail(ui32 node, const TRequest& request, bool disconnect = false) {
        THolder<IEventBase> failure;
        if (disconnect) {
            failure = MakeHolder<TEvInterconnect::TEvNodeDisconnected>(Runtime.GetNodeId(node));
        } else {
            failure = MakeHolder<TEvents::TEvUndelivered>(
                TEvSysView::TEvGetIntervalMetricsRequest::EventType,
                TEvents::TEvUndelivered::Disconnected);
        }
        Runtime.Send(new IEventHandle(request->Sender, NodeEdges.at(node),
            failure.Release(), 0, request->Cookie));
    }

    void Collect(ui32 node = 0) {
        Submit(node);
        Reply(node, TakeRequest(node));
        Sync();
    }

    TEntries Read(NKikimrSysView::EStatsType type = NKikimrSysView::METRICS_ONE_HOUR) {
        auto request = MakeHolder<TEvSysView::TEvGetQueryMetricsRequest>();
        request->Record.SetType(type);
        Runtime.Send(new IEventHandle(Processor, Edge, request.Release()));
        auto response = Runtime.GrabEdgeEvent<TEvSysView::TEvGetQueryMetricsResponse>(Edge);
        UNIT_ASSERT(!response->Get()->Record.GetOverloaded());
        UNIT_ASSERT(response->Get()->Record.GetLastBatch());
        const auto& entries = response->Get()->Record.GetEntries();
        return {entries.begin(), entries.end()};
    }
};

const NKikimrSysView::TQueryMetricsEntry& FindQuery(
    const TQueryMetricsEnv::TEntries& entries, ui64 hash)
{
    auto it = std::find_if(entries.begin(), entries.end(), [hash](const auto& entry) {
        return entry.GetMetrics().GetQueryTextHash() == hash;
    });
    UNIT_ASSERT_C(it != entries.end(), "Missing query hash " << hash);
    return *it;
}

void TestStaleFailure(bool disconnect) {
    TQueryMetricsEnv env(2);
    env.Add(1, 42, 10);
    env.Submit(1);
    const auto oldRequest = env.TakeRequest(1);
    env.NextInterval(); // The old request times out.

    env.Add(1, 42, 20);
    env.Submit(1);
    const auto newRequest = env.TakeRequest(1);
    env.Fail(1, oldRequest, disconnect);
    env.Sync();
    env.Reply(1, newRequest);
    env.Sync();

    const auto entries = env.Read();
    UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCpuTimeUs().GetSum(), 20);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryMetricsProcessorTest) {
    Y_UNIT_TEST(CollectsBeyondMinuteTopAfterReboot) {
        TQueryMetricsEnv env;
        constexpr ui64 recurringHash = NQueryMetricsLimits::MetricsFetchCount;

        auto fillInterval = [&](ui64 offset) {
            for (ui64 i = 1; i < NQueryMetricsLimits::MetricsFetchCount; ++i) {
                env.Add(0, offset + i, 11, "one-off");
            }
            env.Add(0, recurringHash, 10, "recurring");
            env.Collect();
        };
        fillInterval(0);
        auto first = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(first.size(), NQueryMetricsLimits::OneHourResultCount);
        for (const auto& entry : first) {
            UNIT_ASSERT_UNEQUAL(entry.GetMetrics().GetQueryTextHash(), recurringHash);
        }

        env.Reboot();
        env.NextInterval();
        fillInterval(NQueryMetricsLimits::MetricsFetchCount);
        const auto hour = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(hour.size(), NQueryMetricsLimits::OneHourResultCount);
        const auto& recurring = FindQuery(hour, recurringHash);
        UNIT_ASSERT_VALUES_EQUAL(recurring.GetKey().GetRank(), 1);
        UNIT_ASSERT_VALUES_EQUAL(recurring.GetQueryText(), "recurring");
        UNIT_ASSERT_VALUES_EQUAL(recurring.GetMetrics().GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(recurring.GetMetrics().GetCpuTimeUs().GetSum(), 20);

        const auto minutes = env.Read(NKikimrSysView::METRICS_ONE_MINUTE);
        UNIT_ASSERT_VALUES_EQUAL(minutes.size(), 2 * NQueryMetricsLimits::OneMinuteResultCount);
        for (const auto& entry : minutes) {
            UNIT_ASSERT_LE(entry.GetKey().GetRank(), NQueryMetricsLimits::OneMinuteResultCount);
            UNIT_ASSERT_UNEQUAL(entry.GetMetrics().GetQueryTextHash(), recurringHash);
        }
    }

    Y_UNIT_TEST(TextAfterCandidateFailure) {
        TQueryMetricsEnv env(2);
        for (ui64 hash = 1; hash <= NQueryMetricsLimits::OneMinuteResultCount; ++hash) {
            env.Add(1, hash, 20, "failed leader");
        }
        env.Add(0, 91'003, 10, "surviving candidate");
        env.Submit(0);
        env.Submit(1);
        auto good = env.TakeRequest(0);
        auto failed = env.TakeRequest(1);
        env.Reply(0, good);
        env.Fail(1, failed);
        env.Sync();

        for (auto type : {NKikimrSysView::METRICS_ONE_MINUTE, NKikimrSysView::METRICS_ONE_HOUR}) {
            const auto entries = env.Read(type);
            UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetQueryText(), "surviving candidate");
            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCpuTimeUs().GetSum(), 10);
        }
    }

    Y_UNIT_TEST(AggregatesMetrics) {
        TQueryMetricsEnv env;
        env.Add(0, 42, 10, "aggregate", 1, 100, 2);
        env.Collect();
        env.NextInterval();
        env.Add(0, 42, 20, "aggregate", 3, 200, 4);
        env.Collect();

        const auto entries = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
        const auto& metrics = entries[0].GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetSum(), 30);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetMin(), 10);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetMax(), 20);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetDurationUs().GetSum(), 4'000);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetReadRows().GetSum(), 300);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetLocksBrokenAsBreaker(), 6);
    }

    Y_UNIT_TEST_TWIN(FailureAndDuplicateAfterReboot, disconnect) {
        TQueryMetricsEnv env(2);
        env.Add(0, 42, 10);
        env.Add(1, 42, 20);
        env.Submit(0);
        env.Submit(1);
        auto good = env.TakeRequest(0);
        auto failed = env.TakeRequest(1);
        env.Reply(0, good);
        env.Fail(1, failed, disconnect);
        env.Fail(1, failed, disconnect);
        env.Sync();
        env.Reboot();

        const auto entries = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCpuTimeUs().GetSum(), 10);
    }

    Y_UNIT_TEST(TimeoutAndReboot) {
        TQueryMetricsEnv env(2);
        env.Add(0, 42, 10);
        env.Add(1, 42, 20);
        env.Submit(0);
        env.Submit(1);
        env.Reply(0, env.TakeRequest(0));
        env.TakeRequest(1); // Do not respond; finalize the partial result at the deadline.
        env.Sync();
        env.NextInterval();
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(FindQuery(env.Read(), 42).GetMetrics().GetCount(), 1);

        env.Add(0, 42, 30);
        env.Add(1, 42, 30);
        env.Submit(0);
        env.Submit(1);
        env.Reply(0, env.TakeRequest(0));
        env.Reply(1, env.TakeRequest(1));
        env.Sync();
        const auto entries = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(FindQuery(entries, 42).GetMetrics().GetCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(FindQuery(entries, 42).GetMetrics().GetCpuTimeUs().GetSum(), 70);
    }

    Y_UNIT_TEST(RebootWithPendingRequest) {
        TQueryMetricsEnv env(2);
        env.Add(0, 42, 10);
        env.Add(1, 42, 20);
        env.Submit(0);
        env.Submit(1);
        env.Reply(0, env.TakeRequest(0));
        const auto oldRequest = env.TakeRequest(1);
        env.Sync();
        env.Reboot();
        const auto retriedRequest = env.TakeRequest(1);
        UNIT_ASSERT_UNEQUAL(oldRequest->Sender, retriedRequest->Sender);
        env.Fail(1, oldRequest);
        env.Reply(1, retriedRequest);
        env.Sync();

        const auto entries = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCpuTimeUs().GetSum(), 30);
    }

    Y_UNIT_TEST(IgnoresStaleDeliveryFailure) {
        TestStaleFailure(false);
    }

    Y_UNIT_TEST(IgnoresStaleDisconnect) {
        TestStaleFailure(true);
    }

    Y_UNIT_TEST(HourBoundaryAndReboot) {
        const auto hourEnd = TInstant::Seconds(1'800'000'000);
        TQueryMetricsEnv env(1, hourEnd);
        env.Add(0, 42, 10);
        env.Collect();
        env.Reboot();
        env.NextInterval();
        env.Add(0, 42, 20);
        env.Collect();
        env.Reboot();

        const auto entries = env.Read();
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetKey().GetIntervalEndUs(), hourEnd.MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(entries[1].GetKey().GetIntervalEndUs(),
            (hourEnd + TDuration::Hours(1)).MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(entries[0].GetMetrics().GetCpuTimeUs().GetSum(), 10);
        UNIT_ASSERT_VALUES_EQUAL(entries[1].GetMetrics().GetCpuTimeUs().GetSum(), 20);
        for (const auto& entry : entries) {
            UNIT_ASSERT_VALUES_EQUAL(entry.GetMetrics().GetCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(entry.GetKey().GetRank(), 1);
        }
    }
}

} // namespace NKikimr::NSysView
