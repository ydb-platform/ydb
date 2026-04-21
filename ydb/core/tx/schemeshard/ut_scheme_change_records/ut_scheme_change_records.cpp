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
            if (e.Path == "T1") foundT1 = true;
            if (e.Path == "T2") foundT2 = true;
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
            if (e.OperationType == (ui32)TTxState::TxCreateTable && e.Path == "Table1") {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.TxId, (ui64)txId);
                UNIT_ASSERT_VALUES_EQUAL(e.ObjectType, (ui32)NKikimrSchemeOp::EPathTypeTable);
                UNIT_ASSERT_VALUES_EQUAL(e.Status, (ui32)NKikimrScheme::StatusSuccess);
                UNIT_ASSERT(e.Order > 0);
                UNIT_ASSERT_C(e.Body.HasOperationType(), "Body should contain operation description");
                UNIT_ASSERT_VALUES_EQUAL((ui32)e.Body.GetOperationType(), (ui32)NKikimrSchemeOp::ESchemeOpCreateTable);
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
            if (e.OperationType == (ui32)TTxState::TxAlterTable && e.Path == "Table1") {
                ++alterCount;
                UNIT_ASSERT_C(e.Body.HasOperationType(), "Body should contain operation description for ALTER");
                UNIT_ASSERT_VALUES_EQUAL((ui32)e.Body.GetOperationType(), (ui32)NKikimrSchemeOp::ESchemeOpAlterTable);
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
            if (e.OperationType == (ui32)TTxState::TxDropTable && e.Path == "Table1") {
                found = true;
                UNIT_ASSERT_C(e.Body.HasOperationType(), "Body should contain operation description for DROP");
                UNIT_ASSERT_VALUES_EQUAL((ui32)e.Body.GetOperationType(), (ui32)NKikimrSchemeOp::ESchemeOpDropTable);
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
            if (e.Path == "Table1" && e.OperationType == (ui32)TTxState::TxCreateTable) {
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
        for (const auto& e : entries) {
            if (e.Path == "Table1") {
                UNIT_ASSERT_C(e.PlanStep > 0,
                    "Table1 entry (opType=" << e.OperationType << ") should have a valid PlanStep, got: " << e.PlanStep);
            }
        }
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
            if (e.Path == "DirA") {
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

    Y_UNIT_TEST(CreateTableWithIndexProducesMultipleRecords) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // CREATE TABLE WITH GLOBAL INDEX produces multiple parts:
        //  Parts[0] = CreateTable (main)
        //  Parts[1] = CreateTableIndex
        //  Parts[2] = CreateTable (impl table)
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
        // Multi-part: main table + index + impl table = at least 3 records
        // associated with this txId.
        ui32 mainTableCount = 0;
        ui32 indexCount = 0;
        ui32 implTableCount = 0;
        for (const auto& e : entries) {
            if (e.TxId != (ui64)txId) continue;
            if (e.OperationType == (ui32)TTxState::TxCreateTable) {
                if (e.Path == "Main") ++mainTableCount;
                else ++implTableCount;
            } else if (e.OperationType == (ui32)TTxState::TxCreateTableIndex) {
                ++indexCount;
            }
        }
        UNIT_ASSERT_C(mainTableCount >= 1,
            "Expected at least 1 main table record for txId=" << txId << ", got " << mainTableCount);
        UNIT_ASSERT_C(indexCount >= 1,
            "Expected at least 1 index record for txId=" << txId << ", got " << indexCount);
        UNIT_ASSERT_C(implTableCount >= 1,
            "Expected at least 1 impl table record for txId=" << txId << ", got " << implTableCount);
    }

    Y_UNIT_TEST(AutoMkDirProducesMkDirRecords) {
        // When CREATE TABLE targets a path whose ancestors don't exist,
        // schemeshard auto-generates MkDir parts. Each part should produce
        // its own scheme change record.
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

        auto entries = ReadSchemeChangeRecordsFull(runtime);
        bool foundA = false, foundB = false, foundC = false, foundLeaf = false;
        for (const auto& e : entries.Entries) {
            if (e.TxId != (ui64)txId) continue;
            if (e.OperationType == (ui32)TTxState::TxMkDir) {
                if (e.Path == "A") foundA = true;
                else if (e.Path == "B") foundB = true;
                else if (e.Path == "C") foundC = true;
            } else if (e.OperationType == (ui32)TTxState::TxCreateTable && e.Path == "Leaf") {
                foundLeaf = true;
            }
        }
        UNIT_ASSERT_C(foundA && foundB && foundC,
            "Expected MkDir records for A, B, C auto-generated by CREATE TABLE");
        UNIT_ASSERT_C(foundLeaf, "Expected CreateTable record for Leaf");
    }

    Y_UNIT_TEST(EachPartRecordHasOwnBody) {
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

        auto entries = ReadSchemeChangeRecordsFull(runtime);
        bool haveCreateTableBody = false;
        bool haveCreateIndexBody = false;
        for (const auto& e : entries.Entries) {
            if (e.TxId != (ui64)txId) continue;
            if (!e.Body.HasOperationType()) continue;
            auto op = e.Body.GetOperationType();
            if (op == NKikimrSchemeOp::ESchemeOpCreateTable) haveCreateTableBody = true;
            if (op == NKikimrSchemeOp::ESchemeOpCreateTableIndex) haveCreateIndexBody = true;
        }
        UNIT_ASSERT_C(haveCreateTableBody, "At least one record must have CreateTable body");
        UNIT_ASSERT_C(haveCreateIndexBody, "At least one record must have CreateTableIndex body");
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
        // A multi-part DDL must persist the NextSchemeChangeOrder sysparam
        // once for the whole batch, not once per emitted record.
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

        // Test env setup produces bookkeeping writes; reset before the target DDL.
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

        // The DDL has >= 3 parts → >= 3 records → pre-fix would persist >=3 times.
        auto entries = ReadSchemeChangeRecords(runtime);
        size_t thisTxRecords = 0;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId) ++thisTxRecords;
        }
        UNIT_ASSERT_C(thisTxRecords >= 3,
            "Indexed-table DDL should emit at least 3 records, got " << thisTxRecords);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrderPersistCount, 1u,
            "Expected one NextSchemeChangeOrder persist for a multi-part DDL, got "
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
