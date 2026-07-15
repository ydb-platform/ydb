#include <ydb/core/quoter/ut_helpers.h>
#include <ydb/core/kesus/tablet/events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

#include <cstdio>

namespace NKikimr {

using namespace testing;

namespace {

size_t GetCurrentRSSKB() {
    try {
        TFileInput f("/proc/self/status");
        TString line;
        while (f.ReadLine(line)) {
            size_t rss = 0;
            if (sscanf(line.c_str(), "VmRSS: %zu", &rss) == 1) {
                return rss;
            }
        }
    } catch (...) {
    }
    return 0;
}

void PrintRSS(const TString& label, size_t currentKB, size_t baseKB) {
    Cerr << label << ": " << currentKB << " KB";
    if (baseKB > 0) {
        i64 delta = static_cast<i64>(currentKB) - static_cast<i64>(baseKB);
        Cerr << " (" << (delta >= 0 ? "+" : "") << delta << " KB, "
             << (delta >= 0 ? "+" : "") << (delta / 1024) << " MB)";
    }
    Cerr << Endl;
}

void DrainEvents(TTestActorRuntime& runtime) {
    TDispatchOptions opts;
    opts.CustomFinalCondition = [] { return true; };
    runtime.DispatchEvents(opts, TDuration::Seconds(10));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(QuoterMemoryTest) {

Y_UNIT_TEST(MeasureProxyMemoryWith100kResources) {
    constexpr size_t ResourceCount = 100'000;

    Cerr << Endl << "=== Quoter Proxy Memory Test: " << ResourceCount << " resources ===" << Endl << Endl;

    const size_t rss0 = GetCurrentRSSKB();
    PrintRSS("Baseline", rss0, 0);

    // ---- Setup ----
    TKesusProxyTestSetup setup;
    setup.GetRuntime().SetLogPriority(NKikimrServices::KESUS_TABLET, NActors::NLog::PRI_WARN);
    setup.GetRuntime().SetLogPriority(NKikimrServices::QUOTER_SERVICE, NActors::NLog::PRI_WARN);
    setup.GetRuntime().SetLogPriority(NKikimrServices::QUOTER_PROXY, NActors::NLog::PRI_WARN);

    auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();

    ui64 nextResId = 1;
    EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
        .WillRepeatedly(Invoke([pipe, &nextResId](
            const NKikimrKesus::TEvSubscribeOnResources& req, ui64 cookie) {
            NKikimrKesus::TEvSubscribeOnResourcesResult result;
            result.SetProtocolVersion(1);
            for (size_t i = 0; i < static_cast<size_t>(req.ResourcesSize()); ++i) {
                auto* res = result.AddResults();
                res->SetResourceId(nextResId++);
                res->MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
                auto* props = res->MutableEffectiveProps();
                props->SetResourcePath(req.GetResources(i).GetResourcePath());
                props->MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(100);
            }
            pipe->SendSubscribeOnResourceResult(result, cookie);
        }));

    EXPECT_CALL(*pipe, OnUpdateConsumptionState(_, _)).Times(AnyNumber());

    setup.WaitConnected();

    const size_t rss1 = GetCurrentRSSKB();
    PrintRSS("After setup", rss1, rss0);

    // ---- Phase 2: Register resources ----
    Cerr << Endl << "--- Phase 2: Registering " << ResourceCount << " resources ---" << Endl;
    for (size_t i = 0; i < ResourceCount; ++i) {
        setup.SendProxyRequest(TStringBuilder() << "resource/" << i);
    }

    const size_t rss2 = GetCurrentRSSKB();
    PrintRSS("After enqueue (before subscribe dispatch)", rss2, rss1);

    // ---- Phase 3: Drain remaining events ----
    Cerr << Endl << "--- Phase 3: Draining remaining events ---" << Endl;
    DrainEvents(setup.GetRuntime());

    const size_t rss3 = GetCurrentRSSKB();
    PrintRSS("All resources subscribed", rss3, rss1);
    if (rss3 > rss1) {
        Cerr << "  Per-resource cost (incl mailbox events): ~"
             << ((rss3 - rss1) * 1024 / ResourceCount) << " bytes" << Endl;
    }

    // ---- Phase 4: Batch allocation (amount only, no props) ----
    Cerr << Endl << "--- Phase 4: Batch allocation (amount only) ---" << Endl;
    {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
        }
        Cerr << "  Input protobuf size: " << ev->Record.ByteSizeLong() << " bytes ("
             << ev->Record.ByteSizeLong() / 1024 / 1024 << " MB)" << Endl;
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
    }
    DrainEvents(setup.GetRuntime());

    const size_t rss4 = GetCurrentRSSKB();
    PrintRSS("After batch alloc (no props)", rss4, rss3);

    // ---- Phase 5: Batch allocation WITH props change ----
    Cerr << Endl << "--- Phase 5: Batch allocation with props change ---" << Endl;
    {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
            resInfo->MutableEffectiveProps()
                ->MutableHierarchicalDRRResourceConfig()
                ->SetMaxUnitsPerSecond(200);
        }
        Cerr << "  Input protobuf size: " << ev->Record.ByteSizeLong() << " bytes ("
             << ev->Record.ByteSizeLong() / 1024 / 1024 << " MB)" << Endl;
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
    }
    DrainEvents(setup.GetRuntime());

    const size_t rss5 = GetCurrentRSSKB();
    PrintRSS("After batch alloc with props", rss5, rss4);

    // ---- Phase 6: Accounting enabled (small collect period) ----
    Cerr << Endl << "--- Phase 6: Accounting enabled (CollectPeriodSec=1) ---" << Endl;
    {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
            auto* props = resInfo->MutableEffectiveProps();
            props->MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(200);
            auto* acc = props->MutableAccountingConfig();
            acc->SetEnabled(true);
            acc->SetReportPeriodMs(1000);
            acc->SetCollectPeriodSec(1);
        }
        const size_t vecEntries = 1 * 100;
        Cerr << "  TTimeSeriesVec per resource: " << vecEntries << " doubles = "
             << (vecEntries * sizeof(double)) << " bytes" << Endl;
        Cerr << "  Expected total: ~" << (ResourceCount * vecEntries * sizeof(double) / 1024 / 1024)
             << " MB" << Endl;
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
    }
    DrainEvents(setup.GetRuntime());

    const size_t rss6 = GetCurrentRSSKB();
    PrintRSS("After accounting enabled (1s)", rss6, rss5);

    // ---- Phase 6b: Replicated bucket mode ----
    Cerr << Endl << "--- Phase 6b: Replicated bucket mode ---" << Endl;
    {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
            auto* props = resInfo->MutableEffectiveProps();
            auto* drrCfg = props->MutableHierarchicalDRRResourceConfig();
            drrCfg->SetMaxUnitsPerSecond(200);
            // Enable replicated bucket
            auto* replBucket = drrCfg->MutableReplicatedBucket();
            replBucket->SetReportIntervalMs(1000);
            // Disable accounting to isolate replication cost
            props->MutableAccountingConfig()->SetEnabled(false);
        }
        Cerr << "  ReportHistory per resource: MaxReportHistory=120 * 16 bytes = 1920 bytes" << Endl;
        Cerr << "  Expected total: ~" << (ResourceCount * 1920 / 1024 / 1024) << " MB" << Endl;
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
    }
    DrainEvents(setup.GetRuntime());

    const size_t rss6b = GetCurrentRSSKB();
    PrintRSS("After replicated bucket enabled", rss6b, rss6);

    // ---- Phase 6c: Simulate replication report cycles to fill ReportHistory ----
    Cerr << Endl << "--- Phase 6c: Filling ReportHistory deques (send stats to trigger reports) ---" << Endl;
    const size_t rss6c_start = GetCurrentRSSKB();
    // Send ProxyStats to trigger replication report accumulation.
    // Each stats cycle adds one entry to ReportHistory per resource (up to MaxReportHistory=120).
    for (int cycle = 1; cycle <= 10; ++cycle) {
        TDeque<TEvQuota::TProxyStat> stats;
        for (size_t i = 0; i < ResourceCount; ++i) {
            stats.emplace_back(i + 1, 1, 1.0, TTimeSeriesMap<double>(), 0, 0, 0, 0);
        }
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), setup.GetEdgeActor(),
                new TEvQuota::TEvProxyStats(std::move(stats))),
            0, true);
        // Advance time past ReplicationReportPeriod (1000ms)
        setup.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(2));
        DrainEvents(setup.GetRuntime());

        const size_t rssNow = GetCurrentRSSKB();
        const i64 delta = static_cast<i64>(rssNow) - static_cast<i64>(rss6c_start);
        Cerr << "  Report cycle " << cycle << ": RSS = " << rssNow << " KB ("
             << (delta >= 0 ? "+" : "") << delta << " KB)" << Endl;
    }
    const size_t rss6c = GetCurrentRSSKB();
    PrintRSS("After 10 replication report cycles", rss6c, rss6b);

    // ---- Phase 7a: Repeated cycles with REAL resource IDs (ProxyUpdate generated) ----
    Cerr << Endl << "--- Phase 7a: Repeated cycles with real resource IDs (10x) ---" << Endl;
    const size_t rss7a_start = GetCurrentRSSKB();
    for (int cycle = 1; cycle <= 10; ++cycle) {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
        }
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
        DrainEvents(setup.GetRuntime());

        const size_t rssNow = GetCurrentRSSKB();
        const i64 delta = static_cast<i64>(rssNow) - static_cast<i64>(rss7a_start);
        Cerr << "  Cycle " << cycle << ": RSS = " << rssNow << " KB ("
             << (delta >= 0 ? "+" : "") << delta << " KB)" << Endl;
    }
    const size_t rss7a = GetCurrentRSSKB();

    // ---- Phase 7b: Repeated cycles with FAKE resource IDs (no ProxyUpdate) ----
    Cerr << Endl << "--- Phase 7b: Repeated cycles with fake resource IDs (10x) ---" << Endl;
    const size_t rss7b_start = GetCurrentRSSKB();
    for (int cycle = 1; cycle <= 10; ++cycle) {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(900'000 + i + 1); // fake IDs → nothing found
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
        }
        setup.GetRuntime().Send(
            new IEventHandle(setup.GetKesusProxyId(), pipe->GetSelfID(), ev.release()),
            0, true);
        DrainEvents(setup.GetRuntime());

        const size_t rssNow = GetCurrentRSSKB();
        const i64 delta = static_cast<i64>(rssNow) - static_cast<i64>(rss7b_start);
        Cerr << "  Cycle " << cycle << ": RSS = " << rssNow << " KB ("
             << (delta >= 0 ? "+" : "") << delta << " KB)" << Endl;
    }
    const size_t rss7b = GetCurrentRSSKB();

    // ---- Phase 7c: Repeated cycles with NO event at all (just alloc/free protobuf) ----
    Cerr << Endl << "--- Phase 7c: Just alloc/free 100k-entry protobuf (10x, no send) ---" << Endl;
    const size_t rss7c_start = GetCurrentRSSKB();
    for (int cycle = 1; cycle <= 10; ++cycle) {
        auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
        for (size_t i = 0; i < ResourceCount; ++i) {
            auto* resInfo = ev->Record.AddResourcesInfo();
            resInfo->SetResourceId(i + 1);
            resInfo->SetAmount(10.0);
            resInfo->MutableStateNotification()->SetStatus(Ydb::StatusIds::SUCCESS);
        }
        // Don't send — just let ev go out of scope and be freed
        const size_t rssNow = GetCurrentRSSKB();
        const i64 delta = static_cast<i64>(rssNow) - static_cast<i64>(rss7c_start);
        Cerr << "  Cycle " << cycle << ": RSS = " << rssNow << " KB ("
             << (delta >= 0 ? "+" : "") << delta << " KB)" << Endl;
    }
    const size_t rss7c = GetCurrentRSSKB();

    Cerr << Endl;
    Cerr << "  7a (real IDs, send+process): +"
         << (rss7a > rss7a_start ? rss7a - rss7a_start : 0) << " KB" << Endl;
    Cerr << "  7b (fake IDs, send+process): +"
         << (rss7b > rss7b_start ? rss7b - rss7b_start : 0) << " KB" << Endl;
    Cerr << "  7c (alloc/free only, no send): +"
         << (rss7c > rss7c_start ? rss7c - rss7c_start : 0) << " KB" << Endl;
    Cerr << "  If 7a≈7b≈7c → allocator retains freed memory (not a leak)." << Endl;
    Cerr << "  If 7a>7b>7c → real accumulation in proxy/mailbox." << Endl;

    const size_t rss7 = rss7c;

    // ---- Projected costs ----
    Cerr << Endl << "--- Projected costs (not executed) ---" << Endl;
    for (size_t collectSec : {60, 300}) {
        const size_t vecEntries = collectSec * 100;
        const size_t perResource = vecEntries * sizeof(double);
        const size_t totalMB = ResourceCount * perResource / 1024 / 1024;
        Cerr << "  CollectPeriodSec=" << collectSec << ": " << vecEntries
             << " doubles/resource = " << perResource << " bytes/resource"
             << " => " << totalMB << " MB total (" << totalMB / 1024 << " GB)" << Endl;
    }

    // ====== SUMMARY ======
    Cerr << Endl;
    Cerr << "============================================" << Endl;
    Cerr << "  MEMORY SUMMARY (" << ResourceCount << " resources)" << Endl;
    Cerr << "============================================" << Endl;
    Cerr << "Baseline:                    " << rss0 << " KB" << Endl;
    Cerr << "Setup overhead:              +" << (rss1 - rss0) << " KB" << Endl;
    Cerr << "Resource registration:       +"
         << (rss3 > rss1 ? rss3 - rss1 : 0) << " KB"
         << " (~" << ((rss3 > rss1) ? (rss3 - rss1) * 1024 / ResourceCount : 0)
         << " bytes/res)" << Endl;
    Cerr << "Batch alloc (no props):      +"
         << (rss4 > rss3 ? rss4 - rss3 : 0) << " KB" << Endl;
    Cerr << "Batch alloc (props change):  +"
         << (rss5 > rss4 ? rss5 - rss4 : 0) << " KB" << Endl;
    Cerr << "Accounting (1s collect):     +"
         << (rss6 > rss5 ? rss6 - rss5 : 0) << " KB" << Endl;
    Cerr << "Replicated bucket:           +"
         << (rss6b > rss6 ? rss6b - rss6 : 0) << " KB" << Endl;
    Cerr << "Replication reports (10x):   +"
         << (rss6c > rss6b ? rss6c - rss6b : 0) << " KB" << Endl;
    Cerr << "10 cycles (real IDs):         +"
         << (rss7a > rss7a_start ? rss7a - rss7a_start : 0) << " KB (unprocessed events)" << Endl;
    Cerr << "10 cycles (fake IDs):         +"
         << (rss7b > rss7b_start ? rss7b - rss7b_start : 0) << " KB (no events)" << Endl;
    Cerr << "--------------------------------------------" << Endl;
    Cerr << "TOTAL:                       " << rss7 << " KB ("
         << (rss7 > rss0 ? (rss7 - rss0) / 1024 : 0) << " MB from baseline)" << Endl;
    Cerr << "============================================" << Endl;
}

Y_UNIT_TEST(MeasureKesusTabletMemory) {
    constexpr size_t ResourceCount = 10'000; // Fewer resources — full server is slower

    Cerr << Endl << "=== Kesus Tablet Memory Test: " << ResourceCount
         << " resources (extrapolate x10 for 100k) ===" << Endl << Endl;

    const size_t rss0 = GetCurrentRSSKB();
    PrintRSS("Baseline", rss0, 0);

    // ---- Setup full server ----
    TKesusQuoterTestSetup setup(false); // don't auto-run (we'll customize first)
    setup.RunServer();

    setup.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KESUS_TABLET, NActors::NLog::PRI_WARN);
    setup.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::QUOTER_SERVICE, NActors::NLog::PRI_WARN);
    setup.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::QUOTER_PROXY, NActors::NLog::PRI_WARN);

    const size_t rss1 = GetCurrentRSSKB();
    PrintRSS("After server setup", rss1, rss0);

    // ---- Phase 2: Create resources on Kesus ----
    Cerr << Endl << "--- Phase 2: Creating " << ResourceCount << " resources on Kesus ---" << Endl;

    const auto& kesusPath = TKesusQuoterTestSetup::DEFAULT_KESUS_PATH;

    // Create root resource (already created by setup, create a new tree)
    NKikimrKesus::THierarchicalDRRResourceConfig rootCfg;
    rootCfg.SetMaxUnitsPerSecond(1'000'000);
    setup.CreateKesusResource(kesusPath, "memleak_root", rootCfg);

    NKikimrKesus::THierarchicalDRRResourceConfig childCfg;
    childCfg.SetMaxUnitsPerSecond(100);

    for (size_t i = 0; i < ResourceCount; ++i) {
        setup.CreateKesusResource(kesusPath,
            TStringBuilder() << "memleak_root/" << i, childCfg);
        if ((i + 1) % 1000 == 0) {
            Cerr << "  Created " << (i + 1) << " / " << ResourceCount << " resources" << Endl;
        }
    }

    const size_t rss2 = GetCurrentRSSKB();
    PrintRSS("After creating resources", rss2, rss1);
    if (rss2 > rss1) {
        Cerr << "  Per-resource cost on tablet: ~"
             << ((rss2 - rss1) * 1024 / ResourceCount) << " bytes" << Endl;
        Cerr << "  Extrapolated 100k: ~"
             << ((rss2 - rss1) * 10 / 1024) << " MB" << Endl;
    }

    // ---- Phase 3: Establish sessions via quota requests ----
    Cerr << Endl << "--- Phase 3: Establishing sessions (" << ResourceCount << " quota requests) ---" << Endl;

    for (size_t i = 0; i < ResourceCount; ++i) {
        setup.SendGetQuotaRequest(kesusPath,
            TStringBuilder() << "memleak_root/" << i, 1);
    }
    Cerr << "  All requests sent, waiting for answers..." << Endl;
    for (size_t i = 0; i < ResourceCount; ++i) {
        auto answer = setup.WaitGetQuotaAnswer();
        UNIT_ASSERT_VALUES_EQUAL_C(answer->Result, TEvQuota::TEvClearance::EResult::Success,
            "Failed at resource " << i);
        if ((i + 1) % 1000 == 0) {
            Cerr << "  Got " << (i + 1) << " / " << ResourceCount << " answers" << Endl;
        }
    }

    const size_t rss3 = GetCurrentRSSKB();
    PrintRSS("After establishing sessions", rss3, rss2);
    if (rss3 > rss2) {
        Cerr << "  Per-session cost: ~"
             << ((rss3 - rss2) * 1024 / ResourceCount) << " bytes" << Endl;
        Cerr << "  Extrapolated 100k: ~"
             << ((rss3 - rss2) * 10 / 1024) << " MB" << Endl;
    }

    // ---- Phase 4: Update root resource (triggers OnUpdateResourceProps cascade) ----
    Cerr << Endl << "--- Phase 4: Updating root resource (cascade to " << ResourceCount << " children) ---" << Endl;

    // Get tablet ID so we can send update directly
    TTestActorRuntime* runtime = setup.GetServer().GetRuntime();
    {
        TAutoPtr<NMsgBusProxy::TBusResponse> resp = setup.GetClient().Ls(kesusPath);
        const ui64 tabletId = resp->Record.GetPathDescription().GetKesus().GetKesusTabletId();

        auto request = MakeHolder<NKesus::TEvKesus::TEvUpdateQuoterResource>();
        request->Record.MutableResource()->SetResourcePath("memleak_root");
        request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig()
            ->SetMaxUnitsPerSecond(2'000'000); // change speed to trigger effective props change

        ForwardToTablet(*runtime, tabletId, setup.GetEdgeActor(), request.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        auto* result = runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvUpdateQuoterResourceResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
    }

    const size_t rss4 = GetCurrentRSSKB();
    PrintRSS("After root update (cascade)", rss4, rss3);
    if (rss4 > rss3) {
        Cerr << "  Cascade overhead: " << (rss4 - rss3) << " KB" << Endl;
        Cerr << "  Per-resource cascade cost: ~"
             << ((rss4 - rss3) * 1024 / ResourceCount) << " bytes" << Endl;
        Cerr << "  Extrapolated 100k: ~"
             << ((rss4 - rss3) * 10 / 1024) << " MB" << Endl;
    }

    // ---- Phase 5: Repeated root updates ----
    Cerr << Endl << "--- Phase 5: Repeated root updates (5x, check accumulation) ---" << Endl;
    const size_t rssBeforeUpdates = GetCurrentRSSKB();
    {
        TAutoPtr<NMsgBusProxy::TBusResponse> resp = setup.GetClient().Ls(kesusPath);
        const ui64 tabletId = resp->Record.GetPathDescription().GetKesus().GetKesusTabletId();

        for (int cycle = 1; cycle <= 5; ++cycle) {
            auto request = MakeHolder<NKesus::TEvKesus::TEvUpdateQuoterResource>();
            request->Record.MutableResource()->SetResourcePath("memleak_root");
            request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig()
                ->SetMaxUnitsPerSecond(1'000'000.0 + cycle * 100'000.0);

            ForwardToTablet(*runtime, tabletId, setup.GetEdgeActor(), request.Release(), 0);

            TAutoPtr<IEventHandle> handle;
            runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvUpdateQuoterResourceResult>(handle);

            const size_t rssNow = GetCurrentRSSKB();
            const i64 delta = static_cast<i64>(rssNow) - static_cast<i64>(rssBeforeUpdates);
            Cerr << "  Update " << cycle << ": RSS = " << rssNow << " KB ("
                 << (delta >= 0 ? "+" : "") << delta << " KB)" << Endl;
        }
    }
    const size_t rss5 = GetCurrentRSSKB();

    // ---- Phase 6: Create resources with accounting enabled ----
    Cerr << Endl << "--- Phase 6: Creating " << ResourceCount << " resources WITH accounting ---" << Endl;
    {
        NKikimrKesus::THierarchicalDRRResourceConfig accRootCfg;
        accRootCfg.SetMaxUnitsPerSecond(1'000'000);
        setup.CreateKesusResource(kesusPath, "acc_root", accRootCfg);

        TAutoPtr<NMsgBusProxy::TBusResponse> resp = setup.GetClient().Ls(kesusPath);
        const ui64 tabletId = resp->Record.GetPathDescription().GetKesus().GetKesusTabletId();

        for (size_t i = 0; i < ResourceCount; ++i) {
            auto request = MakeHolder<NKesus::TEvKesus::TEvAddQuoterResource>();
            auto* res = request->Record.MutableResource();
            res->SetResourcePath(TStringBuilder() << "acc_root/" << i);
            res->MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(100);
            res->MutableAccountingConfig()->SetEnabled(true);
            res->MutableAccountingConfig()->SetReportPeriodMs(1000);
            res->MutableAccountingConfig()->SetCollectPeriodSec(1);

            ForwardToTablet(*runtime, tabletId, setup.GetEdgeActor(), request.Release(), 0);
            TAutoPtr<IEventHandle> handle;
            runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvAddQuoterResourceResult>(handle);

            if ((i + 1) % 1000 == 0) {
                Cerr << "  Created " << (i + 1) << " / " << ResourceCount
                     << " accounting resources" << Endl;
            }
        }
    }

    const size_t rss6 = GetCurrentRSSKB();
    PrintRSS("After creating accounting resources", rss6, rss5);
    if (rss6 > rss5) {
        Cerr << "  Per-resource accounting overhead on tablet: ~"
             << ((rss6 - rss5) * 1024 / ResourceCount) << " bytes" << Endl;
        Cerr << "  Extrapolated 100k: ~"
             << ((rss6 - rss5) * 10 / 1024) << " MB" << Endl;
    }

    // ---- Phase 7: Create resources with replicated bucket ----
    Cerr << Endl << "--- Phase 7: Creating " << ResourceCount << " resources WITH replicated bucket ---" << Endl;
    {
        NKikimrKesus::THierarchicalDRRResourceConfig replRootCfg;
        replRootCfg.SetMaxUnitsPerSecond(1'000'000);
        setup.CreateKesusResource(kesusPath, "repl_root", replRootCfg);

        NKikimrKesus::THierarchicalDRRResourceConfig replChildCfg;
        replChildCfg.SetMaxUnitsPerSecond(100);
        replChildCfg.MutableReplicatedBucket()->SetReportIntervalMs(1000);

        for (size_t i = 0; i < ResourceCount; ++i) {
            setup.CreateKesusResource(kesusPath,
                TStringBuilder() << "repl_root/" << i, replChildCfg);
            if ((i + 1) % 1000 == 0) {
                Cerr << "  Created " << (i + 1) << " / " << ResourceCount
                     << " replicated resources" << Endl;
            }
        }
    }

    const size_t rss7 = GetCurrentRSSKB();
    PrintRSS("After creating replicated bucket resources", rss7, rss6);
    if (rss7 > rss6) {
        Cerr << "  Per-resource replicated bucket overhead on tablet: ~"
             << ((rss7 - rss6) * 1024 / ResourceCount) << " bytes" << Endl;
        Cerr << "  Extrapolated 100k: ~"
             << ((rss7 - rss6) * 10 / 1024) << " MB" << Endl;
    }

    // ====== SUMMARY ======
    Cerr << Endl;
    Cerr << "============================================" << Endl;
    Cerr << "  KESUS TABLET MEMORY SUMMARY (" << ResourceCount << " resources)" << Endl;
    Cerr << "  (multiply by 10 for 100k estimate)" << Endl;
    Cerr << "============================================" << Endl;
    Cerr << "Baseline:                    " << rss0 << " KB" << Endl;
    Cerr << "Server setup:                +" << (rss1 - rss0) << " KB" << Endl;
    Cerr << "Resource creation:           +"
         << (rss2 > rss1 ? rss2 - rss1 : 0) << " KB"
         << " (~" << ((rss2 > rss1) ? (rss2 - rss1) * 1024 / ResourceCount : 0)
         << " bytes/res)" << Endl;
    Cerr << "Session establishment:       +"
         << (rss3 > rss2 ? rss3 - rss2 : 0) << " KB"
         << " (~" << ((rss3 > rss2) ? (rss3 - rss2) * 1024 / ResourceCount : 0)
         << " bytes/session)" << Endl;
    Cerr << "Root update cascade:         +"
         << (rss4 > rss3 ? rss4 - rss3 : 0) << " KB" << Endl;
    Cerr << "5 repeated updates:          +"
         << (rss5 > rss4 ? rss5 - rss4 : 0) << " KB" << Endl;
    Cerr << "Accounting resources:        +"
         << (rss6 > rss5 ? rss6 - rss5 : 0) << " KB"
         << " (~" << ((rss6 > rss5) ? (rss6 - rss5) * 1024 / ResourceCount : 0)
         << " bytes/res)" << Endl;
    Cerr << "Replicated bucket resources: +"
         << (rss7 > rss6 ? rss7 - rss6 : 0) << " KB"
         << " (~" << ((rss7 > rss6) ? (rss7 - rss6) * 1024 / ResourceCount : 0)
         << " bytes/res)" << Endl;
    Cerr << "--------------------------------------------" << Endl;
    Cerr << "TOTAL:                       " << rss7 << " KB ("
         << (rss7 > rss0 ? (rss7 - rss0) / 1024 : 0) << " MB from baseline)" << Endl;
    Cerr << "Extrapolated 100k TOTAL:     ~"
         << ((rss7 > rss1 ? rss7 - rss1 : 0) * 10 / 1024) << " MB" << Endl;
    Cerr << "============================================" << Endl;
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr
