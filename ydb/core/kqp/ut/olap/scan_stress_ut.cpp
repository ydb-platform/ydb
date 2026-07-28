#include "helpers/local.h"
#include "helpers/writer.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <thread>

namespace NKikimr::NKqp {

namespace {

// Drain the stream ignoring per-query failures: the point of the stress is to catch
// crashes/sanitizer findings in the server, queries may legally fail (e.g. by memory limit).
ui64 DrainStream(NYdb::NTable::TScanQueryPartIterator& it, std::atomic<ui64>& memoryLimitErrors, TAdaptiveLock& lock, TString& lastError) {
    ui64 rows = 0;
    while (true) {
        auto part = it.ReadNext().GetValueSync();
        if (!part.IsSuccess()) {
            if (part.EOS()) {
                break;
            }
            const TString issues = part.GetIssues().ToString();
            if (issues.Contains("memory limit")) {
                ++memoryLimitErrors;
            } else {
                with_lock (lock) {
                    lastError = TStringBuilder() << part.GetStatus() << ": " << issues;
                }
            }
            break;
        }
        if (part.HasResultSet()) {
            rows += part.GetResultSet().RowsCount();
        }
    }
    return rows;
}

void RunScanStress(TKikimrRunner& kikimr, const ui32 writeIterations, const bool expectSuccess) {
    auto tableClient = kikimr.GetTableClient();

    // expectSuccess mode: 20x the same PK range -> maximum portion overlap, dedup stress.
    // failure mode: distinct ranges -> scans retain tens of MB in TopSort against a tiny quota.
    for (ui32 i = 0; i < 20; ++i) {
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000 + (expectSuccess ? 0 : i * 1000), 1000);
    }

    const std::vector<TString> queries = {
        R"(
            --!syntax_v1
            SELECT SUM(`level`), COUNT(*), MAX(`timestamp`)
            FROM `/Root/olapStore/olapTable`
            WHERE `timestamp` >= CAST(3000000 AS Timestamp) AND `level` >= 0
        )",
        R"(
            --!syntax_v1
            PRAGMA ydb.DisableBlockExecution;
            SELECT `uid`, `message`
            FROM `/Root/olapStore/olapTable`
            ORDER BY `message` DESC LIMIT 20000
        )",
        R"(
            --!syntax_v1
            SELECT a.`resource_id`, COUNT(*), MAX(b.`message`)
            FROM `/Root/olapStore/olapTable` AS a
            INNER JOIN `/Root/olapStore/olapTable` AS b ON a.`uid` = b.`uid`
            GROUP BY a.`resource_id` ORDER BY a.`resource_id` LIMIT 10
        )",
    };

    std::atomic<bool> stop{ false };
    std::atomic<ui64> failedQueries{ 0 };
    std::atomic<ui64> memoryLimitErrors{ 0 };
    TAdaptiveLock lock;
    TString lastError;
    std::vector<std::thread> readers;
    for (ui32 t = 0; t < 4; ++t) {
        readers.emplace_back([&, t]() {
            const TString query = queries[t % queries.size()];
            while (!stop.load()) {
                auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();
                if (!it.IsSuccess()) {
                    ++failedQueries;
                    continue;
                }
                if (!DrainStream(it, memoryLimitErrors, lock, lastError)) {
                    ++failedQueries;
                }
            }
        });
    }

    for (ui32 i = 0; i < writeIterations; ++i) {
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
    }

    stop = true;
    for (auto& th : readers) {
        th.join();
    }
    Cerr << "SCAN_STRESS: failedQueries=" << failedQueries.load() << " memoryLimitErrors=" << memoryLimitErrors.load()
         << " lastError=" << lastError << Endl;
    if (expectSuccess) {
        UNIT_ASSERT_VALUES_EQUAL_C(failedQueries.load(), 0, lastError);
    } else {
        UNIT_ASSERT_C(memoryLimitErrors.load() > 0, "no memory limit errors triggered; lastError: " << lastError);
    }
}

}   // namespace

// Reproduction for https://github.com/ydb-platform/ydb/issues/47942:
// SIGSEGV in TAccessorsCollection::RemainOnly / TProjectionProcessor on scans with
// aggregation over many overlapping portions (duplicate filtering active) under
// concurrent writes. Run under TSAN/ASAN to detect the underlying race.
Y_UNIT_TEST_SUITE(KqpOlapScanStress) {
    Y_UNIT_TEST(AggregationWithOverlapsAndConcurrentWrites) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        TLocalHelper(kikimr).CreateTestOlapTable();
        RunScanStress(kikimr, 50, true);
    }

    // Same workload with a tiny MKQL memory limit: scan compute actors constantly hit
    // TMemoryLimitExceededException mid-stream — the trigger for the use-after-poison
    // teardown from https://github.com/ydb-platform/ydb/issues/40326.
    Y_UNIT_TEST(AggregationWithMkqlMemoryLimitExceptions) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(128_KB);
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(128_KB);
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetQueryMemoryLimit(64_MB);
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(128_KB);
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(128_KB);
        {
            // Bound extra mkql quota grants via the resource broker: RequestExtraMemory ->
            // AllocateResources -> SubmitTaskInstant("kqp_query") is denied above the queue limit.
            auto& rb = *appCfg.MutableResourceBrokerConfig();
            auto* queue = rb.AddQueues();
            queue->SetName("queue_default");
            queue->SetWeight(5);
            queue->MutableLimit()->AddResource(4);
            queue->MutableLimit()->AddResource(1'000'000'000);
            queue = rb.AddQueues();
            queue->SetName("queue_kqp_resource_manager");
            queue->SetWeight(20);
            queue->MutableLimit()->AddResource(4);
            queue->MutableLimit()->AddResource(64_MB);
            auto* task = rb.AddTasks();
            task->SetName("unknown");
            task->SetQueueName("queue_default");
            task->SetDefaultDuration(TDuration::Seconds(5).GetValue());
            task = rb.AddTasks();
            task->SetName(NLocalDb::KqpResourceManagerTaskName);
            task->SetQueueName("queue_kqp_resource_manager");
            task->SetDefaultDuration(TDuration::Seconds(5).GetValue());
            rb.MutableResourceLimit()->AddResource(10);
            rb.MutableResourceLimit()->AddResource(1'000'000'000);
        }
        auto settings = TKikimrSettings(appCfg).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        TLocalHelper(kikimr).CreateTestOlapTable();
        RunScanStress(kikimr, 50, false);
    }
}

}   // namespace NKikimr::NKqp
