#include "ut_scheme_change_records_helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecordsFull;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSchemaTests) {
    Y_UNIT_TEST(SchemeChangeRecordsTableExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto entries = ReadSchemeChangeRecords(runtime);
        Y_UNUSED(entries);
    }

    Y_UNIT_TEST(NoRecordsCreatedWithoutSubscribers) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.empty(),
            "No records should be created without subscribers, got " << entries.size());
    }

    Y_UNIT_TEST(RecordsCreatedAfterSubscriberRegistered) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create T1 without subscriber -- no record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Register subscriber
        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Create T2 with subscriber -- record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool foundT1 = false;
        bool foundT2 = false;
        for (const auto& e : entries) {
            if (e.Body.HasCreateTable()) {
                const auto& name = e.Body.GetCreateTable().GetName();
                if (name == "T1") foundT1 = true;
                if (name == "T2") foundT2 = true;
            }
        }
        UNIT_ASSERT_C(!foundT1, "T1 record should not exist (created before subscriber)");
        UNIT_ASSERT_C(foundT2, "T2 record should exist (created after subscriber)");
    }

    Y_UNIT_TEST(CreateTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.TxId, (ui64)txId);
                UNIT_ASSERT_VALUES_EQUAL(e.Path, "/MyRoot");
                UNIT_ASSERT(e.Order > 0);
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(AlterTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 alterCount = 0;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                ++alterCount;
            }
        }
        UNIT_ASSERT_C(alterCount >= 1, "ALTER TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(DropTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTable
                && e.Body.GetDrop().GetName() == "Table1") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DROP TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(OrdersAreMonotonicAcrossOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        for (size_t i = 1; i < entries.size(); ++i) {
            UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                "Orders must be strictly monotonic");
        }
    }

    Y_UNIT_TEST(OverflowRejectsNewOperations) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 2;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.size() >= baseline.size() + 2, "Expected at least baseline+2 entries");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(AckFreesOverflowCapacityImmediately) {
        // Verifies the post-Phase-2 invariant: overflow check uses
        // (NextSchemeChangeOrder - MinSubscriberOrder), so an ack
        // restores capacity immediately without waiting for background cleanup.
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 2;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // At capacity: next op rejected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3a"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // Ack everything (without manually firing background cleanup)
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!entries.empty());
        ui64 lastOrder = entries.back().Order;
        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "test:sub", lastOrder, ackHandle);

        // Capacity must be free immediately after ack (overflow check is
        // based on unacked range, not on row count in SchemeChangeRecords).
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3b"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RaisingLimitViaConfigUnblocksOperations) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(baseline.size() + 2);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(baseline.size() + 10);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(LoweringLimitViaConfigBlocksOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(1);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T3"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});
    }

    Y_UNIT_TEST(PlanStepIsRecordedForCoordinatedOps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(!entries.empty());

        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = true;
                UNIT_ASSERT_C(e.PlanStep > 0,
                    "CreateTable should have a valid PlanStep, got: " << e.PlanStep);
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(PlanStepIsRecordedForAlterTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool sawAlter = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                sawAlter = true;
                UNIT_ASSERT_C(e.PlanStep > 0,
                    "AlterTable Table1 should have a valid PlanStep, got: " << e.PlanStep);
            }
        }
        UNIT_ASSERT(sawAlter);
    }

    Y_UNIT_TEST(PlanStepMonotonicAcrossOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        ui64 prevPlanStep = 0;
        for (const auto& e : entries) {
            UNIT_ASSERT_C(e.PlanStep >= prevPlanStep,
                "PlanStep should be monotonically non-decreasing: prev=" << prevPlanStep
                    << " current=" << e.PlanStep << " path=" << e.Path);
            prevPlanStep = e.PlanStep;
        }

        for (size_t i = 1; i < entries.size(); ++i) {
            const auto& prev = entries[i-1];
            const auto& curr = entries[i];
            if (curr.PlanStep != prev.PlanStep || curr.TxId != prev.TxId) {
                bool planStepTxIdOrdering = std::tie(curr.PlanStep, curr.TxId) > std::tie(prev.PlanStep, prev.TxId);
                UNIT_ASSERT_C(planStepTxIdOrdering,
                    "(PlanStep, TxId) ordering must match Order ordering:"
                        << " prev=(" << prev.PlanStep << "," << prev.TxId << ") order=" << prev.Order
                        << " curr=(" << curr.PlanStep << "," << curr.TxId << ") order=" << curr.Order);
            }
        }
    }

    Y_UNIT_TEST(MkDirWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMkDir
                && e.Body.GetMkDir().GetName() == "DirA") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "MkDir should produce a scheme change record");
    }

    Y_UNIT_TEST(WatermarkDoesNotRegressAfterAllOpsComplete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto result = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT(!result.Entries.empty());
        // Clients rely on WatermarkPlanStep being monotonic: once reported at
        // some value, it must not drop below it even when TxInFlight empties.
        UNIT_ASSERT_C(result.WatermarkPlanStep > 0,
            "WatermarkPlanStep must not regress to 0 after an op completes, got: "
                << result.WatermarkPlanStep);
    }

    Y_UNIT_TEST(WatermarkSurvivesTabletReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "reboot:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto before = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(before.WatermarkPlanStep > 0,
            "Pre-reboot watermark should be > 0, got: " << before.WatermarkPlanStep);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto after = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(after.WatermarkPlanStep >= before.WatermarkPlanStep,
            "Post-reboot watermark must not regress: before="
                << before.WatermarkPlanStep << " after=" << after.WatermarkPlanStep);
    }

    Y_UNIT_TEST(WatermarkReflectsInFlightPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TVector<THolder<IEventHandle>> heldEvents;
        ui64 firstTxId = txId + 1;
        bool captured = false;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvSchemaChanged) {
                auto* msg = ev->Get<TEvDataShard::TEvSchemaChanged>();
                if (msg->Record.GetTxId() == firstTxId) {
                    captured = true;
                    heldEvents.push_back(THolder<IEventHandle>(ev.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");

        {
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&]() { return captured; };
            runtime.DispatchEvents(opts);
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Read notification log -- Table2 should be present, Table1 still in-flight
        auto result = ReadSchemeChangeRecordsFull(runtime);

        UNIT_ASSERT_C(result.WatermarkPlanStep > 0,
            "WatermarkPlanStep should be > 0 while Table1 is still in-flight, got: "
                << result.WatermarkPlanStep);

        for (auto& ev : heldEvents) {
            runtime.Send(ev.Release());
        }
        heldEvents.clear();
        env.TestWaitNotification(runtime, firstTxId);

        auto result2 = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(result2.WatermarkPlanStep >= result.WatermarkPlanStep,
            "WatermarkPlanStep must not regress after all ops complete: had "
                << result.WatermarkPlanStep << ", now " << result2.WatermarkPlanStep);
    }

    Y_UNIT_TEST(CreateTableWithIndexProducesSingleParentRecord) {
        // Under parent-level persistence, a multi-part DDL emits exactly one
        // record carrying the user-level body — index descriptions live
        // inside it and the target cluster re-runs decomposition on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 parentCount = 0;
        bool haveMain = false;
        bool haveIndex = false;
        for (const auto& e : entries) {
            if (e.TxId != (ui64)txId) continue;
            UNIT_ASSERT_VALUES_EQUAL(
                (ui32)e.Body.GetOperationType(),
                (ui32)NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
            ++parentCount;
            const auto& ct = e.Body.GetCreateIndexedTable();
            if (ct.GetTableDescription().GetName() == "Main") haveMain = true;
            for (const auto& idx : ct.GetIndexDescription()) {
                if (idx.GetName() == "IdxByValue") haveIndex = true;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(parentCount, 1u,
            "Expected exactly 1 parent-level record, got " << parentCount);
        UNIT_ASSERT_C(haveMain, "Parent record must carry the main table description");
        UNIT_ASSERT_C(haveIndex, "Parent record must carry the index description");
    }

    Y_UNIT_TEST(AutoMkDirBodyPreservedOnParent) {
        // Auto-mkdirs are an internal decomposition detail. Under parent-
        // level persistence, only the user's original CreateTable body
        // (with its "A/B/C/Leaf" path) is persisted; the target cluster
        // regenerates the MkDir chain on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "A/B/C/Leaf"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool foundLeaf = false;
        for (const auto& e : entries) {
            if (e.TxId != (ui64)txId) continue;
            UNIT_ASSERT_VALUES_EQUAL(
                (ui32)e.Body.GetOperationType(),
                (ui32)NKikimrSchemeOp::ESchemeOpCreateTable);
            if (e.Body.GetCreateTable().GetName() == "A/B/C/Leaf") {
                foundLeaf = true;
            }
        }
        UNIT_ASSERT_C(foundLeaf, "User-level CreateTable body must preserve the original path");
    }

    Y_UNIT_TEST(ParentBodyCarriesFullSubDescriptions) {
        // Every CreateTableIndex/CreateTable sub-description the decomposer
        // would ever need must live inside the one parent body — otherwise
        // replay cannot reproduce the DDL on the target.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "body:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const NKikimrSchemeOp::TIndexedTableCreationConfig* ct = nullptr;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId && e.Body.HasCreateIndexedTable()) {
                ct = &e.Body.GetCreateIndexedTable();
                break;
            }
        }
        UNIT_ASSERT(ct);
        UNIT_ASSERT_VALUES_EQUAL(ct->GetTableDescription().GetName(), "Main");
        UNIT_ASSERT_VALUES_EQUAL(ct->IndexDescriptionSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ct->GetIndexDescription(0).GetName(), "IdxByValue");
    }

    Y_UNIT_TEST(MultiPartDDLIsReconstructibleBySubscriber) {
        // Models cross-cluster replication of a complex multi-index DDL.
        // Target: base table + 2 regular indexes + 1 vector KMeans tree index.
        // Under parent-level persistence the outbox emits exactly ONE record
        // carrying the user-level ESchemeOpCreateIndexedTable body; all
        // index descriptions (including vector KMeans settings) live inside
        // it. Replay on the target cluster is a single submission via
        // TEvReplaySchemeChangeRecord with WorkingDir prefix rewritten from
        // "/MyRoot" to "/MyRoot/Replay"; the target's SS re-runs decomposition
        // and produces the full impl-table tree automatically.

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "replay:sub", regHandle);

        const ui64 indexedTxId = ++txId;
        TestCreateIndexedTable(runtime, indexedTxId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "id"       Type: "Uint64" }
                Columns { Name: "value"    Type: "Utf8"   }
                Columns { Name: "other"    Type: "Uint32" }
                Columns { Name: "embedding" Type: "String" }
                Columns { Name: "covered"  Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
            IndexDescription {
                Name: "IdxByOther"
                KeyColumnNames: ["other"]
            }
            IndexDescription {
                Name: "IdxByEmbedding"
                KeyColumnNames: ["embedding"]
                DataColumnNames: ["covered"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription: {
                    Settings: {
                        settings: {
                            metric: DISTANCE_COSINE
                            vector_type: VECTOR_TYPE_FLOAT
                            vector_dimension: 1024
                        }
                        clusters: 4
                        levels: 5
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, indexedTxId);

        // Exactly one parent record for the whole DDL.
        auto result = ReadSchemeChangeRecordsFull(runtime);
        const NKikimrSchemeOp::TModifyScheme* parentBody = nullptr;
        for (const auto& e : result.Entries) {
            if (e.TxId == indexedTxId) {
                UNIT_ASSERT_C(!parentBody,
                    "Expected exactly one record for the indexed-table DDL");
                parentBody = &e.Body;
            }
        }
        UNIT_ASSERT(parentBody);
        UNIT_ASSERT_VALUES_EQUAL(
            (ui32)parentBody->GetOperationType(),
            (ui32)NKikimrSchemeOp::ESchemeOpCreateIndexedTable);

        // Type-specific settings (e.g. vector KMeans params) survive the
        // roundtrip through persistence.
        const auto& ct = parentBody->GetCreateIndexedTable();
        UNIT_ASSERT_VALUES_EQUAL(ct.IndexDescriptionSize(), 3u);
        bool sawVectorIdx = false;
        for (const auto& idx : ct.GetIndexDescription()) {
            if (idx.GetType() == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree) {
                UNIT_ASSERT(idx.HasVectorIndexKmeansTreeDescription());
                UNIT_ASSERT_VALUES_EQUAL(
                    idx.GetVectorIndexKmeansTreeDescription()
                        .GetSettings().Getclusters(), 4u);
                sawVectorIdx = true;
            }
        }
        UNIT_ASSERT(sawVectorIdx);

        // === Single-submission replay ===
        TestMkDir(runtime, ++txId, "/MyRoot", "Replay");
        env.TestWaitNotification(runtime, txId);

        TString serialized;
        UNIT_ASSERT(parentBody->SerializeToString(&serialized));

        TAutoPtr<IEventHandle> handle;
        const ui64 replayTxId = ++txId;
        auto* reply = ReplaySchemeChangeRecord(
            runtime, serialized, replayTxId, "/MyRoot", "/MyRoot/Replay", handle);
        UNIT_ASSERT_VALUES_EQUAL_C(
            (ui32)reply->GetStatus(),
            (ui32)NKikimrScheme::StatusAccepted,
            "Replay of parent CREATE TABLE WITH 3 INDEXES must succeed; reason: "
                << reply->GetReason());
        env.TestWaitNotification(runtime, replayTxId);

        // Target SS regenerated every impl table on its own.
        TestDescribeResult(
            DescribePath(runtime, "/MyRoot/Replay/Main"),
            {NLs::PathExist});
        TestDescribeResult(
            DescribePrivatePath(runtime, "/MyRoot/Replay/Main/IdxByValue"),
            {NLs::PathExist, NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal)});
        TestDescribeResult(
            DescribePrivatePath(runtime, "/MyRoot/Replay/Main/IdxByEmbedding"),
            {NLs::PathExist,
             NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree)});
        TestDescribeResult(
            DescribePrivatePath(runtime,
                "/MyRoot/Replay/Main/IdxByEmbedding/indexImplLevelTable"),
            {NLs::PathExist});
        TestDescribeResult(
            DescribePrivatePath(runtime,
                "/MyRoot/Replay/Main/IdxByEmbedding/indexImplPostingTable"),
            {NLs::PathExist});
    }

    Y_UNIT_TEST(FetchBodiesReturnsOnlyRequestedSparseOrders) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bodies:sub", regHandle);

        for (int i = 1; i <= 10; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "bodies:sub", 0, 1000, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 10u);

        TVector<ui64> allOrders;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            allOrders.push_back(fetch->Record.GetEntries(i).GetOrder());
        }

        TVector<ui64> requested = {allOrders[1], allOrders[4], allOrders[7]};

        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, "bodies:sub", requested, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.EntriesSize(), requested.size());

        THashSet<ui64> requestedSet(requested.begin(), requested.end());
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            const auto& e = bodies->Record.GetEntries(i);
            UNIT_ASSERT_C(requestedSet.contains(e.GetOrder()),
                "FetchBodies returned unrequested order " << e.GetOrder());
            UNIT_ASSERT_C(!e.GetBody().empty(),
                "FetchBodies returned empty body for order " << e.GetOrder());
            NKikimrSchemeOp::TModifyScheme body;
            UNIT_ASSERT(body.ParseFromString(e.GetBody()));
            UNIT_ASSERT_VALUES_EQUAL((ui32)body.GetOperationType(), (ui32)NKikimrSchemeOp::ESchemeOpCreateTable);
        }
    }

    Y_UNIT_TEST(AckWithLargeBacklogDrainsAcrossMultipleTxs) {
        // Bounded cleanup: a single Ack tx deletes at most
        // SchemeChangeCleanupBatchSize rows; the rest drains via a chain
        // of follow-up TTxSchemeChangeRecordsCleanup txs kicked off from
        // Complete(). Ack reply must return with correct LastAckedOrder
        // before the drain completes.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backlog:sub", regHandle);

        // Small cap so we can trigger continuation without creating 1000+ records.
        schemeshard->SchemeChangeCleanupBatchSize = 3;

        constexpr int kRecords = 10;
        for (int i = 0; i < kRecords; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "B%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "backlog:sub", 0, 1000, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= kRecords);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        // Measure only the chain triggered by the single Ack below.
        schemeshard->SchemeChangeCleanupTxCount = 0;

        TAutoPtr<IEventHandle> ackHandle;
        auto* ackRes = AckSchemeChangeRecords(runtime, "backlog:sub", latest, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)ackRes->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ackRes->Record.GetLastAckedOrder(), latest);

        for (int i = 0; i < 50; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT_C(schemeshard->SchemeChangeCleanupTxCount >= 3,
            "Expected >=3 continuation cleanup txs for " << kRecords
            << " records at batch size 3, got " << schemeshard->SchemeChangeCleanupTxCount);
    }

    Y_UNIT_TEST(PersistsNextSchemeChangeOrderOncePerBatch) {
        // Under parent-level persistence a multi-part DDL emits one record,
        // so the NextSchemeChangeOrder sysparam is persisted once per batch
        // by construction. This test locks in both properties.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "batchpersist:sub", regHandle);

        schemeshard->NextSchemeChangeOrderPersistCount = 0;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Main"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "IdxByValue"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        size_t thisTxRecords = 0;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId) ++thisTxRecords;
        }
        UNIT_ASSERT_VALUES_EQUAL(thisTxRecords, 1u);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrderPersistCount, 1u,
            "Expected one NextSchemeChangeOrder persist for a batch, got "
            << schemeshard->NextSchemeChangeOrderPersistCount);
    }

    Y_UNIT_TEST(FetchBodiesUnorderedAndDuplicateRequestedOrdersHandled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bodies:sub2", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "bodies:sub2", 0, 1000, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 5u);

        TVector<ui64> all;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            all.push_back(fetch->Record.GetEntries(i).GetOrder());
        }

        TVector<ui64> requested = {all[3], all[0], all[3], all[2]};

        TAutoPtr<IEventHandle> bodiesHandle;
        auto* bodies = FetchSchemeChangeRecordBodies(runtime, "bodies:sub2", requested, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)bodies->Record.GetStatus(),
            (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        THashSet<ui64> returned;
        for (size_t i = 0; i < static_cast<size_t>(bodies->Record.EntriesSize()); ++i) {
            returned.insert(bodies->Record.GetEntries(i).GetOrder());
        }
        // Dedup expected: each requested order returned at most once
        UNIT_ASSERT_C(returned.contains(all[0]), "order " << all[0] << " missing");
        UNIT_ASSERT_C(returned.contains(all[2]), "order " << all[2] << " missing");
        UNIT_ASSERT_C(returned.contains(all[3]), "order " << all[3] << " missing");
        UNIT_ASSERT_VALUES_EQUAL(returned.size(), 3u);
    }
}
