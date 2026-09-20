#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/general_cache/usage/service.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

// Requests to the tablet only go out on a cache miss, but this event is sent for every accessor lookup, so
// it is the one point where a reader can be reliably stopped before it touches data.
using TPortionAccessorsCache = NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>;
using TEvAskPortionAccessors = NGeneralCache::NPublic::TEvents<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::TEvAskData;

void SleepUntil(TTestActorRuntime& runtime, const TInstant deadline) {
    const auto now = runtime.GetCurrentTime();
    if (deadline > now) {
        runtime.SimulateSleep(deadline - now);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(KqpOlapScanCleanup) {
    // The test implements the case1 from this diagram https://github.com/ydb-platform/ydb/issues/49434#issuecomment-5277352717
    Y_UNIT_TEST(ScanThenCleanupDoesNotCrash) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Cleanup);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));

        // Intercepting events requires the test to drive the dispatch loop, so every SDK call goes through
        // RunCall/RunInThreadPool while the main thread keeps the actor system running.
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        // The shard collects nothing younger than the oldest registered snapshot minus these margins, and the
        // defaults put that floor further into the past than the test runs.
        auto& longTxConfig = *settings.AppConfig.MutableLongTxServiceConfig();
        longTxConfig.SetLocalSnapshotPromotionTimeSeconds(1);
        longTxConfig.SetMaxClockSkewMs(1000);
        longTxConfig.SetSnapshotsRegistryUpdateIntervalSeconds(1);
        longTxConfig.SetSnapshotsExchangeIntervalSeconds(1);
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        // 1. Create the table
        kikimr.RunCall([&] {
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            const auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnTable` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                )
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )")
                                    .GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            return true;
        });

        using namespace NYdb::NQuery;
        auto queryClient = kikimr.GetQueryClient();
        auto session1 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });
    
        // 2. Get a snapshot for tx1
        // The snapshot is acquired by the first query of the transaction, not by the session. The table is
        // empty at that point.
        std::optional<TTransaction> tx1;
        {
            const auto result = kikimr.RunCall([&] {
                return session1.ExecuteQuery("SELECT 1;", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);
            UNIT_ASSERT(tx1->IsActive());
        }

        auto session2 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });

        // 3. Write two portions, the first is intersecting with tx1, the second is not
        {
            const auto insertResult = kikimr.RunCall([&] {
                return session2
                    .ExecuteQuery(R"(
                        INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (5u, "five");
                    )",
                        TTxControl::BeginTx(TTxSettings::SerializableRW()))
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
            const auto tx2 = insertResult.GetTransaction();
            UNIT_ASSERT(tx2);

            const auto commitResult = kikimr.RunCall([&] {
                return session2
                    .ExecuteQuery(R"(
                        INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (500u, "five hundred");
                    )",
                        TTxControl::Tx(*tx2).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
        }

        auto session3 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });
        const auto countActivePortions = [&] {
            const auto result = kikimr.RunCall([&] {
                return session3
                    .ExecuteQuery(R"(
                        SELECT COUNT(*) AS Portions
                        FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
                        WHERE Activity == 1;
                    )",
                        TTxControl::NoTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto parser = result.GetResultSetParser(0);
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser("Portions").GetUint64();
        };
        UNIT_ASSERT_VALUES_EQUAL(countActivePortions(), 2);

        // 4. Compact the portions, so they have remove snapshot now
        TInstant compactedAt;
        {
            // The controller forces compaction regardless of the portion count. The originals get a remove
            // snapshot but stay on disk until cleanup, which is still switched off.
            const i64 compactionsBefore = csController->GetCompactionFinishedCounter().Val();
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
            runtime.WaitFor(
                "compaction", [&] { return csController->GetCompactionFinishedCounter().Val() > compactionsBefore; }, TDuration::Seconds(30));
            compactedAt = runtime.GetCurrentTime();
            UNIT_ASSERT_VALUES_EQUAL(countActivePortions(), 1);
        }

        // Park the scan after it has built its read metadata, which is where the lock is taken, and before it
        // reads anything: the accessor lookup is the first thing in between. Only scan requests are held,
        // holding the background ones would deadlock the cleanup this test waits for.
        const auto accessorsCacheId = TPortionAccessorsCache::MakeServiceId(runtime.GetNodeId(0));
        NActors::TBlockEvents<TEvAskPortionAccessors> heldScanAccessors(runtime, [accessorsCacheId](const auto& ev) {
            // Match the original recipient: the service id is rewritten to the concrete actor before the
            // observer sees the event.
            return ev->Recipient == accessorsCacheId && ev->Get()->GetConsumer() == NOlap::NBlobOperations::EConsumer::SCAN;
        });

        // 5. tx1 reads the table
        // it plans to read only the already removed portion with key 5, just for conflict detection
        auto selectFuture = kikimr.RunInThreadPool([&] {
            return session1
                .ExecuteQuery(R"(
                    SELECT Key, Value FROM `/Root/ColumnTable` WHERE Key BETWEEN 1 AND 10 ORDER BY Key;
                )",
                    TTxControl::Tx(*tx1))
                .ExtractValueSync();
        });

        runtime.WaitFor(
            "scan looks portion accessors up", [&] { return !heldScanAccessors.empty() || selectFuture.HasValue(); }, TDuration::Seconds(30));
        if (heldScanAccessors.empty()) {
            const auto earlyResult = selectFuture.GetValue();
            UNIT_ASSERT_C(false, "select finished without looking portion accessors up: " << earlyResult.GetStatus() << " "
                                                                                          << earlyResult.GetIssues().ToString());
        }

        // The registry floor lags real time by the promotion and clock skew margins, so the portions stay
        // uncollectable until the clock moves past them for reasons that have nothing to do with the lock.
        // Wait that out first, otherwise the lock proves nothing.
        SleepUntil(runtime, compactedAt + TDuration::Seconds(10));

        // 6. Cleanup happens, after the scan takes the portion for reading, but before the scan actually reads it.
        // Cleanup collects the second portion (with key 500), but leaves the first one, because the scan took a lock for it.
        {
            const i64 cleanupsBefore = csController->GetCleaningFinishedCounter().Val();
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Cleanup);
            runtime.WaitFor(
                "cleanup", [&] { return csController->GetCleaningFinishedCounter().Val() > cleanupsBefore; }, TDuration::Seconds(30));
        }

        heldScanAccessors.Stop().Unblock();

        // 7. The scan reads the data and detects conflicts
        const auto selectResult = runtime.WaitFuture(selectFuture);
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        CompareYson("[]", FormatResultSetYson(selectResult.GetResultSet(0)));

        // 8. tx1 tries to write some data and commit, but fails because the scan detected a conflict with the first portion (key 5)
        const auto commitResult = kikimr.RunCall([&] {
            return session1
                .ExecuteQuery(R"(
                    INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (7u, "seven");
                )",
                    TTxControl::Tx(*tx1).CommitTx())
                .ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), NYdb::EStatus::ABORTED, commitResult.GetIssues().ToString());
    }

    // The test implements the case2 from this diagram https://github.com/ydb-platform/ydb/issues/49434#issuecomment-5277352717
    Y_UNIT_TEST(CleanupThenScanDoesNotMissConflict) {
        // enable when this is fixed https://github.com/ydb-platform/ydb/issues/49908
        return;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Cleanup);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        auto& longTxConfig = *settings.AppConfig.MutableLongTxServiceConfig();
        longTxConfig.SetLocalSnapshotPromotionTimeSeconds(1);
        longTxConfig.SetMaxClockSkewMs(1000);
        longTxConfig.SetSnapshotsRegistryUpdateIntervalSeconds(1);
        longTxConfig.SetSnapshotsExchangeIntervalSeconds(1);
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        // 1. Create the table
        kikimr.RunCall([&] {
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            const auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnTable` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                )
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )")
                                    .GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            return true;
        });

        using namespace NYdb::NQuery;
        auto queryClient = kikimr.GetQueryClient();
        auto session1 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });

        // 2. Get a snapshot for tx1. The table is empty at that point.
        std::optional<TTransaction> tx1;
        {
            const auto result = kikimr.RunCall([&] {
                return session1.ExecuteQuery("SELECT 1;", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            tx1 = result.GetTransaction();
            UNIT_ASSERT(tx1);
            UNIT_ASSERT(tx1->IsActive());
        }

        auto session2 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });

        // 3. Write two portions so compaction can merge them. Only key 5 intersects the range tx1 reads.
        {
            const auto insertResult = kikimr.RunCall([&] {
                return session2
                    .ExecuteQuery(R"(
                        INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (5u, "five");
                    )",
                        TTxControl::BeginTx(TTxSettings::SerializableRW()))
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
            const auto tx2 = insertResult.GetTransaction();
            UNIT_ASSERT(tx2);

            const auto commitResult = kikimr.RunCall([&] {
                return session2
                    .ExecuteQuery(R"(
                        INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (500u, "five hundred");
                    )",
                        TTxControl::Tx(*tx2).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
        }

        auto session3 = kikimr.RunCall([&] { return queryClient.GetSession().GetValueSync().GetSession(); });
        const auto countActivePortions = [&] {
            const auto result = kikimr.RunCall([&] {
                return session3
                    .ExecuteQuery(R"(
                        SELECT COUNT(*) AS Portions
                        FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
                        WHERE Activity == 1;
                    )",
                        TTxControl::NoTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto parser = result.GetResultSetParser(0);
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser("Portions").GetUint64();
        };
        UNIT_ASSERT_VALUES_EQUAL(countActivePortions(), 2);

        // 4. Compact the portions, so they have a remove snapshot now
        TInstant compactedAt;
        {
            const i64 compactionsBefore = csController->GetCompactionFinishedCounter().Val();
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
            runtime.WaitFor(
                "compaction", [&] { return csController->GetCompactionFinishedCounter().Val() > compactionsBefore; }, TDuration::Seconds(30));
            compactedAt = runtime.GetCurrentTime();
            UNIT_ASSERT_VALUES_EQUAL(countActivePortions(), 1);
        }

        // The registry floor lags real time by the promotion and clock skew margins.
        SleepUntil(runtime, compactedAt + TDuration::Seconds(10));

        // 5. Cleanup runs before tx1 reads. Nobody holds a scan lock, so both originals can be collected.
        {
            const i64 cleanupsBefore = csController->GetCleaningFinishedCounter().Val();
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Cleanup);
            runtime.WaitFor(
                "cleanup", [&] { return csController->GetCleaningFinishedCounter().Val() > cleanupsBefore; }, TDuration::Seconds(30));
        }

        // 6. tx1 reads the range that insert2 wrote into. The table was empty at its snapshot.
        {
            const auto selectResult = kikimr.RunCall([&] {
                return session1
                    .ExecuteQuery(R"(
                        SELECT Key, Value FROM `/Root/ColumnTable` WHERE Key BETWEEN 1 AND 10 ORDER BY Key;
                    )",
                        TTxControl::Tx(*tx1))
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(selectResult.GetResultSet(0)));
        }

        // 7. tx1 tries to write and commit. Somebody wrote into the range it read, so this must be ABORTED.
        const auto commitResult = kikimr.RunCall([&] {
            return session1
                .ExecuteQuery(R"(
                    INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (7u, "seven");
                )",
                    TTxControl::Tx(*tx1).CommitTx())
                .ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), NYdb::EStatus::ABORTED, commitResult.GetIssues().ToString());
    }
}

}   // namespace NKikimr::NKqp
