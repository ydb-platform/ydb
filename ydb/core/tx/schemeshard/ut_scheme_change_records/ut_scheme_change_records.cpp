#include "ut_scheme_change_records_helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/schemeshard_counters.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/string/printf.h>
#include <util/string/join.h>

#include <functional>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecordsFull;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecordsFromTable;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSchemaTests) {
    // The table reader must be a faithful substitute for the protocol reader
    // before the audit tests are moved onto it, or the migration silently
    // weakens what they check. Drives a multi-target op so Targets/SourcePaths
    // and the table-143 body join are both exercised.
    Y_UNIT_TEST(TableReaderMatchesProtocolReader) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "parity:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src"
                DstPath: "/MyRoot/Dst"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto viaTable = ReadSchemeChangeRecordsFromTable(runtime);
        auto viaProtocol = ReadSchemeChangeRecords(runtime);

        UNIT_ASSERT_C(!viaTable.empty(), "table reader returned nothing");
        UNIT_ASSERT_VALUES_EQUAL_C(viaTable.size(), viaProtocol.size(),
            "readers disagree on record count");

        for (size_t i = 0; i < viaTable.size(); ++i) {
            const auto& t = viaTable[i];
            const auto& p = viaProtocol[i];
            UNIT_ASSERT_VALUES_EQUAL(t.Order, p.Order);
            UNIT_ASSERT_VALUES_EQUAL(t.TxId, p.TxId);
            UNIT_ASSERT_VALUES_EQUAL(t.PathOwnerId, p.PathOwnerId);
            UNIT_ASSERT_VALUES_EQUAL(t.PathLocalId, p.PathLocalId);
            UNIT_ASSERT_VALUES_EQUAL(t.ObjectType, p.ObjectType);
            UNIT_ASSERT_VALUES_EQUAL(t.Status, p.Status);
            UNIT_ASSERT_VALUES_EQUAL(t.SchemaVersion, p.SchemaVersion);
            UNIT_ASSERT_VALUES_EQUAL(t.PlanStep, p.PlanStep);
            UNIT_ASSERT_VALUES_EQUAL(t.PositionKind, p.PositionKind);
            UNIT_ASSERT_VALUES_EQUAL(t.UserSID, p.UserSID);
            UNIT_ASSERT_VALUES_EQUAL_C(AllTargetPaths(t), AllTargetPaths(p),
                "target paths disagree at order " << t.Order);
            UNIT_ASSERT_VALUES_EQUAL_C(t.Targets.size(), p.Targets.size(),
                "target count disagrees at order " << t.Order);
            for (size_t j = 0; j < t.Targets.size(); ++j) {
                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", t.Targets[j].SourcePaths),
                                         JoinSeq(",", p.Targets[j].SourcePaths));
            }
            // The body is what audit tests select on, so parity here is the
            // load-bearing part of the migration.
            UNIT_ASSERT_VALUES_EQUAL_C(
                (ui32)t.Body.GetOperationType(), (ui32)p.Body.GetOperationType(),
                "body OperationType disagrees at order " << t.Order);
        }

        // A record carrying SourcePaths must actually be present, or the
        // SourcePaths comparison above passed vacuously.
        bool sawSourcePaths = false;
        for (const auto& e : viaTable) {
            for (const auto& target : e.Targets) {
                if (!target.SourcePaths.empty()) {
                    sawSourcePaths = true;
                }
            }
        }
        UNIT_ASSERT_C(sawSourcePaths,
            "expected at least one record carrying SourcePaths (the consistent copy)");
    }

    Y_UNIT_TEST(SchemeChangeRecordsTableExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        auto entries = ReadSchemeChangeRecords(runtime);
        Y_UNUSED(entries);
    }

    Y_UNIT_TEST(NoRecordsCreatedWithoutSubscribers) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(e.Targets[0].Path, "Table1");
                UNIT_ASSERT(e.Order > 0);
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found in notification log");
    }

    Y_UNIT_TEST(AlterTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(alterCount >= 1, "ALTER TABLE entry not found in notification log");
    }

    // UserSID must record who issued the DDL, not who owns the target object.
    // A ModifyACL changes the owner without the owner ever issuing anything;
    // a later ALTER by a different (here: anonymous-token) issuer must not be
    // attributed to that owner.
    Y_UNIT_TEST(UserSIDRecordsIssuerNotOwner) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        // Positive companion: the owner is genuinely changed to someone who
        // never issues the later ALTER, so a later match can't be coincidence.
        TestModifyACL(runtime, ++txId, "/MyRoot", "Table1", "", "bob@builtin");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* alterEntry = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable
                && e.Body.GetAlterTable().GetName() == "Table1") {
                alterEntry = &e;
            }
        }
        UNIT_ASSERT_C(alterEntry, "ALTER TABLE entry not found in notification log");
        // The test issues the ALTER with no user token, so the true issuer
        // SID is empty -- never "bob@builtin", the object's owner.
        UNIT_ASSERT_VALUES_EQUAL_C(alterEntry->UserSID, "",
            "UserSID must reflect the DDL issuer (empty, no token supplied here), "
            "not the target's owner (\"bob@builtin\"); got \"" << alterEntry->UserSID << "\"");
    }

    // TCreateCdcStream's target is nested under TableName/StreamDescription.Name:
    // the record must carry the changefeed itself, not the table or the working dir.
    Y_UNIT_TEST(CdcStreamRecordDoesNotImpersonateParentDirectory) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateCdcStream) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CreateCdcStream must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_VALUES_UNEQUAL_C(found->Targets[0].Path, "Table1",
            "the target must be the changefeed, not its parent table");
        UNIT_ASSERT_VALUES_UNEQUAL_C(found->Targets[0].Path, "",
            "the target must be the changefeed, not the working directory");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "Table1/Stream1",
            "expected the changefeed's own path, got \"" << found->Targets[0].Path << "\"");
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
    }

    // TDropCdcStream.StreamName is repeated, so one op can retire N changefeeds:
    // each must get its own target, not be collapsed into a single entry.
    Y_UNIT_TEST(DropCdcStreamRecordsEveryStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream2"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // showPrivate: a changefeed is not a common-sense path, so the default
        // describe refuses it before any identity can be read.
        const auto stream1Version = ExtractPathVersion(
            DescribePath(runtime, "/MyRoot/Table1/Stream1", false, false, true));
        const ui64 droppedOwnerId = stream1Version.PathId.OwnerId;
        const ui64 droppedLocalId = stream1Version.PathId.LocalPathId;

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamName: "Stream1"
            StreamName: "Stream2"
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "a multi-stream drop must resolve every target, not bump the path-missing counter");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropCdcStream) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropCdcStream must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 2u,
            "dropping two changefeeds must record two targets, got " << AllTargetPaths(*found));

        THashSet<TString> paths;
        for (const auto& target : found->Targets) {
            UNIT_ASSERT_C(target.SourcePaths.empty(),
                "a drop has no source, SourcePaths must be empty for " << target.Path);
            paths.insert(target.Path);
        }
        UNIT_ASSERT_C(paths.contains("Table1/Stream1"), "missing Table1/Stream1 in " << AllTargetPaths(*found));
        UNIT_ASSERT_C(paths.contains("Table1/Stream2"), "missing Table1/Stream2 in " << AllTargetPaths(*found));
        // The streams are gone by finalize time, so the first target's identity is
        // checked against the one captured before the drop, not a live re-resolve.
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId,
            "recorded PathOwnerId must match the first dropped changefeed's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId,
            "recorded PathLocalId must match the first dropped changefeed's identity");
    }

    Y_UNIT_TEST(DropTableWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        opts.EnableSchemeChangeRecords(true);
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
        // Overflow check uses (NextSchemeChangeOrder - MinSubscriberOrder), so
        // an ack restores capacity immediately without waiting for cleanup.
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
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

        // Capacity must be free immediately after ack: overflow check is based
        // on unacked range, not row count.
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
        opts.EnableSchemeChangeRecords(true);
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
                    << " current=" << e.PlanStep << " path=" << AllTargetPaths(e));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
                break;
            }
        }
        UNIT_ASSERT_C(found, "MkDir should produce a scheme change record");
    }

    Y_UNIT_TEST(WatermarkDoesNotRegressAfterAllOpsComplete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        // ClosedThroughPlanStep must be monotonic even when TxInFlight empties.
        UNIT_ASSERT_C(result.ClosedThroughPlanStep > 0,
            "ClosedThroughPlanStep must not regress to 0 after an op completes, got: "
                << result.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(WatermarkSurvivesTabletReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        UNIT_ASSERT_C(before.ClosedThroughPlanStep > 0,
            "Pre-reboot watermark should be > 0, got: " << before.ClosedThroughPlanStep);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto after = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(after.ClosedThroughPlanStep >= before.ClosedThroughPlanStep,
            "Post-reboot watermark must not regress: before="
                << before.ClosedThroughPlanStep << " after=" << after.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(WatermarkReflectsInFlightPlanStep) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        // Table2 should be present, Table1 still in-flight.
        auto result = ReadSchemeChangeRecordsFull(runtime);

        UNIT_ASSERT_C(result.ClosedThroughPlanStep > 0,
            "ClosedThroughPlanStep should be > 0 while Table1 is still in-flight, got: "
                << result.ClosedThroughPlanStep);

        for (auto& ev : heldEvents) {
            runtime.Send(ev.Release());
        }
        heldEvents.clear();
        env.TestWaitNotification(runtime, firstTxId);

        auto result2 = ReadSchemeChangeRecordsFull(runtime);
        UNIT_ASSERT_C(result2.ClosedThroughPlanStep >= result.ClosedThroughPlanStep,
            "ClosedThroughPlanStep must not regress after all ops complete: had "
                << result.ClosedThroughPlanStep << ", now " << result2.ClosedThroughPlanStep);
    }

    Y_UNIT_TEST(CreateTableWithIndexProducesSingleParentRecord) {
        // A multi-part DDL emits exactly one record, carrying the TModifyScheme
        // as it arrived in the request; the target cluster re-runs decomposition
        // on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        // Only the user's original CreateTable body is persisted; the target
        // cluster regenerates the auto-mkdir chain on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        UNIT_ASSERT_C(foundLeaf, "The persisted CreateTable body must preserve the path as requested");
    }

    Y_UNIT_TEST(ParentBodyCarriesFullSubDescriptions) {
        // Every sub-description the decomposer needs must live inside the one
        // parent body, or replay cannot reproduce the DDL on the target.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

    Y_UNIT_TEST(FetchBodiesReturnsOnlyRequestedSparseOrders) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        // A single Ack tx deletes at most SchemeChangeCleanupBatchSize rows;
        // the rest drains via follow-up cleanup txs. Ack reply must return
        // with the correct LastAckedOrder before the drain completes.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
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

    // Bounding the fetch scan by NextSchemeChangeOrder must not change which
    // records are returned. This does NOT prove the precharge cost bound --
    // charge counters stay at 0 in this in-memory test regardless of range
    // width, so the cost effect is unobservable here. It only proves the
    // upper bound is not off-by-one at the boundary or under a maxCount cap.
    Y_UNIT_TEST(FetchUpperBoundDoesNotChangeResults) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "bound:sub", regHandle);

        constexpr int kRecords = 10;
        for (int i = 0; i < kRecords; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Bound%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        // Ground truth: a fetch wide enough to cover everything at once. The
        // boundary/capped cases below are checked against this, not against
        // an independent oracle -- ReadSchemeChangeRecords is itself built on
        // this same fetch path, so it cannot serve as an independent check.
        TAutoPtr<IEventHandle> fullHandle;
        auto* full = FetchSchemeChangeRecords(runtime, "bound:sub", 0, 1000, fullHandle);
        UNIT_ASSERT_C(full->Record.EntriesSize() >= kRecords,
            "precondition: the unbounded fetch must see all " << kRecords << " records");
        TVector<ui64> allOrders;
        ui64 maxOrder = 0;
        for (size_t i = 0; i < static_cast<size_t>(full->Record.EntriesSize()); ++i) {
            const ui64 order = full->Record.GetEntries(i).GetOrder();
            allOrders.push_back(order);
            maxOrder = Max(maxOrder, order);
        }

        // Boundary case: AfterOrder pinned one below the true max, so the
        // query's GreaterOrEqual/LessOrEqual window collapses to exactly the
        // last row. An off-by-one bound would return zero entries here.
        TAutoPtr<IEventHandle> boundaryHandle;
        auto* boundary = FetchSchemeChangeRecords(runtime, "bound:sub", maxOrder - 1, 1000, boundaryHandle);
        UNIT_ASSERT_VALUES_EQUAL_C(boundary->Record.EntriesSize(), 1,
            "the last record must still be reachable right at the upper bound");
        UNIT_ASSERT_VALUES_EQUAL(boundary->Record.GetEntries(0).GetOrder(), maxOrder);

        // Count-capped case: maxCount stops the scan before the bound would.
        // Same leading records as the unbounded fetch, in the same order.
        TAutoPtr<IEventHandle> cappedHandle;
        auto* capped = FetchSchemeChangeRecords(runtime, "bound:sub", 0, 3, cappedHandle);
        UNIT_ASSERT_VALUES_EQUAL(capped->Record.EntriesSize(), 3);
        UNIT_ASSERT_C(capped->Record.GetHasMore(), "capped fetch must report more entries remain");
        for (int i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(capped->Record.GetEntries(i).GetOrder(), allOrders[i]);
        }
    }

    Y_UNIT_TEST(PersistsNextSchemeChangeOrderOncePerBatch) {
        // A multi-part DDL emits one record, so NextSchemeChangeOrder is
        // persisted once per batch by construction.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
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
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

    Y_UNIT_TEST(ZeroMaxSchemeChangeRecordsIsIgnored) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "zero:sub", regHandle);

        const ui64 before = schemeshard->MaxSchemeChangeRecords;
        UNIT_ASSERT_C(before > 0, "precondition: the default cap must be non-zero");

        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableSchemeShardConfig()->SetMaxSchemeChangeRecords(0);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->MaxSchemeChangeRecords, before,
            "a zero cap rejects every DDL including after a force-advance, so it "
            "must be ignored rather than applied");

        // The consequence that matters: DDL is still accepted.
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DisabledByDefaultEmitsNoRecords) {
        // ReadSchemeChangeRecords itself registers a temp subscriber, which is
        // refused while disabled; use the order counter as a disabled-safe oracle.
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        // Default options: the flag is off unless a test opts in.
        TTestEnv env(runtime, TTestEnvOptions(), ssFactory);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrder, 0u,
            "no outbox rows should be reserved while the feature is disabled");
    }

    // Positive companion to DisabledByDefaultEmitsNoRecords: the identical DDL
    // with the flag on does produce a row, so the disabled case can't pass
    // vacuously because DDL itself failed.
    Y_UNIT_TEST(EnabledByOptInEmitsRecord) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        UNIT_ASSERT_C(!entries.empty(), "expected at least one outbox row with the feature enabled");
    }

    Y_UNIT_TEST(DisabledRefusesRegistration) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> regHandle;
        auto* result = RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, regHandle);
        UNIT_ASSERT_C(!result->Record.GetReason().empty(),
            "a disabled-feature refusal must carry a clear reason, not a silent failure");
    }

    Y_UNIT_TEST(DisabledNeverBlocksDdl) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true), ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        auto baseline = ReadSchemeChangeRecords(runtime);
        schemeshard->MaxSchemeChangeRecords = baseline.size() + 1;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Seed the outbox to the cap: this DDL must be refused while enabled.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // The kill switch: disabling must rescue a cluster wedged by a full outbox.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(false);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(FlagFlipTakesEffectWithoutReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Disabled: registration is refused.
        TAutoPtr<IEventHandle> regHandle1;
        RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_INVALID_REQUEST, regHandle1);

        // Flip live, on the same running tablet -- no reboot.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);

        TAutoPtr<IEventHandle> regHandle2;
        RegisterSubscriber(runtime, "test:sub", regHandle2);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(!entries.empty(),
            "record emission must follow the flag flip without a tablet restart");
    }

    Y_UNIT_TEST(ReEnablingResumesWithoutReportingLoss) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Mid-stream: fetch but don't ack past the tail, so the subscriber's
        // cursor sits in the middle of the retained log.
        TAutoPtr<IEventHandle> fetchHandle;
        FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);

        // Disable, then re-enable: the cursor and its record window must survive.
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(false);
        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);

        TAutoPtr<IEventHandle> regHandle2;
        auto* result = RegisterSubscriberExpect(runtime, "test:sub",
            NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS, regHandle2);
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)result->Record.GetState(),
            (ui32)NKikimrSchemeShard::TSchemeChangeSubscriberState::STATE_READY,
            "a subscriber that was mid-stream across a disable/enable cycle "
            "must not be reported as STATE_LOST");
    }

    // Cleanup is only ever enqueued from the Complete() hooks of outbox
    // transactions, and those are gated on the flag. Suppress the scheduled
    // continuation so the self-chain cannot drain on its own; only the
    // disable path (ApplyConsoleConfigs) may rescue it.
    Y_UNIT_TEST(DisablingDrainsAckedRecords) {
        TSchemeShard* schemeshard = nullptr;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts, ssFactory);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Small cap so the Ack's own tx cannot finish the drain in one shot.
        schemeshard->SchemeChangeCleanupBatchSize = 1;

        constexpr int kRecords = 3;
        for (int i = 0; i < kRecords; ++i) {
            TestMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir%d", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        auto before = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!before.empty(), "precondition: the record must exist before acking");

        // Suppress the scheduled continuation before acking, so the batch-size
        // limited remainder cannot drain via the ordinary self-chain.
        TVector<THolder<IEventHandle>> suppressed;
        auto prevObserver = SetSuppressObserver(runtime, suppressed,
            TEvPrivate::TEvSchemeChangeRecordsCleanup::EventType);

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "test:sub", latest, ackHandle);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        auto stillPresent = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!stillPresent.empty(),
            "precondition: the continuation must be suppressed, so acked "
            "records are not yet swept");

        // Stop suppressing, but deliberately drop the already-suppressed
        // continuation instead of replaying it: the drain below must come
        // from a freshly enqueued cleanup, not from that stale one.
        runtime.SetObserverFunc(prevObserver);
        suppressed.clear();

        // Disabling must kick the drain despite the dropped continuation.
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableSchemeChangeRecords(false);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);
        auto after = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(after.empty(),
            "disabling the flag must self-start a drain of already-acked records");
    }

    // Scope limit: disabling must not bypass GetMinSubscriberOrder. An
    // unacked record stays retained so re-enabling resumes losslessly.
    Y_UNIT_TEST(DisablingRetainsUnackedRecords) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto* fetch = FetchSchemeChangeRecords(runtime, "test:sub", 0, 1000, fetchHandle);
        ui64 latest = 0;
        for (size_t i = 0; i < static_cast<size_t>(fetch->Record.EntriesSize()); ++i) {
            latest = Max(latest, fetch->Record.GetEntries(i).GetOrder());
        }

        auto before = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!before.empty(), "precondition: the record must exist before disabling");

        // No ack: the subscriber's cursor still sits below this record.
        {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableSchemeChangeRecords(false);
            SetConfig(runtime, TTestTxConfig::SchemeShard, std::move(request));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        runtime.GetAppData().FeatureFlags.SetEnableSchemeChangeRecords(true);
        auto after = ProbeRecordOrdersPresent(runtime, "test:sub", {latest});
        UNIT_ASSERT_C(!after.empty(),
            "an unacked record must survive disabling: the retention floor "
            "must not be bypassed");
    }

    // (a) FinalizeSchemeChangeRecord resolves the target via TPath::Resolve but
    // only writes PathOwnerId/PathLocalId back -- Path itself keeps the
    // propose-time synthesis. A WorkingDir with a trailing slash still
    // canonicalizes on resolve, so the two differ and the record must carry
    // the canonical one.
    Y_UNIT_TEST(RecordCarriesCanonicalResolvedPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Trailing slash: propose-time string concatenation would leave a
        // double slash, while TPath::Resolve canonicalizes it away.
        TestCreateTable(runtime, ++txId, "/MyRoot/", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_C(!found->Targets[0].Path.Contains("//"),
            "Path must be the canonical resolved path, not a raw concatenation; got \"" << found->Targets[0].Path << "\"");
    }

    // (b) + (c) A stored path must be relative to the database root, and the
    // relative remainder must be the full path to the object -- a path
    // truncated to empty must not pass.
    Y_UNIT_TEST(RecordPathIsRelativeToDatabaseRoot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
            Name: "Nested"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Nested") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_C(!found->Targets[0].Path.StartsWith("/MyRoot"),
            "Path must not carry the source database prefix; got \"" << found->Targets[0].Path << "\"");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "DirA/Nested",
            "Path must be the full remainder relative to the database root, "
            "not truncated to empty or to just the leaf name");
    }

    // No fallback, ever: across a varied corpus of path-bearing ops, nothing may be
    // refused for a missing path and every record must name the object it touched.
    Y_UNIT_TEST(PathBearingOpNeverStoresAnApproximatePath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        ui64 checkedThrough = 0;
        ui32 checkedRecords = 0;
        // Checked after every op, before a later drop or rename can invalidate an
        // already-correct path. targetsStillExist is false once the op removed them.
        auto verifyNewRecords = [&](bool targetsStillExist) {
            const auto entries = ReadSchemeChangeRecordsFromTable(runtime);
            ui64 maxOrder = checkedThrough;
            for (const auto& e : entries) {
                maxOrder = Max(maxOrder, e.Order);
                if (e.Order <= checkedThrough) {
                    continue;
                }
                // Bootstrap system views (.sys/*) are recorded too; not this test's DDL.
                if (AnyPathContains(e, ".sys/")) {
                    continue;
                }
                const TString opName = NKikimrSchemeOp::EOperationType_Name(e.Body.GetOperationType());
                // An empty path is the database root itself, per RelativeToDomain's
                // contract; the resolve below rejects it for anything else.
                UNIT_ASSERT_C(!e.Targets.empty(), opName << " recorded no target at all");
                ++checkedRecords;
                if (!targetsStillExist) {
                    continue;
                }
                for (size_t i = 0; i < e.Targets.size(); ++i) {
                    AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e, i);
                }
            }
            checkedThrough = maxOrder;
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing"), 0u,
                "no op in this corpus may fail to resolve a path");
        };

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "IndexedTable1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ByValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream2"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamName: "Stream1"
            Disable {}
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomain1"
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        // Two streams in one op: the repeated StreamName must yield two targets.
        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamName: "Stream1"
            StreamName: "Stream2"
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(false);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "MoveSrc"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        TestMoveTable(runtime, ++txId, "/MyRoot/MoveSrc", "/MyRoot/MoveDst");
        env.TestWaitNotification(runtime, txId);
        verifyNewRecords(true);

        UNIT_ASSERT_C(checkedRecords >= 10,
            "the corpus must actually have produced records to check, got " << checkedRecords);
    }

    // Positive companion to PathBearingOpNeverStoresAnApproximatePath: the
    // pathless allowlist is empty today, so an ordinary op must still produce
    // its record -- the invariant above must not be satisfiable by refusing
    // everything.
    Y_UNIT_TEST(OrdinaryOpsStillProduceRecordsWhenAllowlistIsEmpty) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "PlainTable"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "PlainTable") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "an ordinary path-bearing op must still produce a record");
    }

    // CreateConsistentCopyTables carries N targets in one repeated
    // TCopyTableConfig field, each pairing a destination with its own source.
    // The record carries N (Path, SourcePaths) targets per record (one record
    // per TModifyScheme in the request, since Body is the atomic replay unit),
    // so this propose must succeed and the record must list every copy as a
    // target with both its destination Path and its source populated.
    Y_UNIT_TEST(ConsistentCopyTablesRecordsAllTargetsWithSources) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Src2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestConsistentCopyTables(runtime, ++txId, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src1"
                DstPath: "/MyRoot/Dst1"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Src2"
                DstPath: "/MyRoot/Dst2"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "a resolvable multi-target copy must not bump the path-missing counter");

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CreateConsistentCopyTables entry not found -- the propose must not be refused");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 2u,
            "record must list all N copy targets, got " << found->Targets.size());

        THashMap<TString, TString> dstToSrc;
        for (const auto& target : found->Targets) {
            UNIT_ASSERT_VALUES_EQUAL_C(target.SourcePaths.size(), 1u,
                "each copy target must have exactly one source, got "
                    << target.SourcePaths.size() << " for " << target.Path);
            dstToSrc[target.Path] = target.SourcePaths[0];
        }
        UNIT_ASSERT_C(dstToSrc.contains("Dst1"), "missing Dst1 in " << AllTargetPaths(*found));
        UNIT_ASSERT_C(dstToSrc.contains("Dst2"), "missing Dst2 in " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst1"], "Src1",
            "Dst1's target must record Src1 as its source");
        UNIT_ASSERT_VALUES_EQUAL_C(dstToSrc["Dst2"], "Src2",
            "Dst2's target must record Src2 as its source");
        // Every one of the N targets must resolve, not just the first (the
        // extractor only captures PathId for target[0], so [0] gets the full
        // PathId cross-check and [1] gets a plain resolve check).
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 0);
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 1);
    }

    // Guards against the repeated field silently turning every record into a
    // list of one-or-more by accident: a plain single-target op must still
    // yield exactly one target, with an empty SourcePaths (a create has no
    // source, it is a repeated field so "empty" is the correct assertion,
    // not "unset").
    Y_UNIT_TEST(SingleTargetOpRecordsExactlyOneTarget) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable
                && e.Body.GetCreateTable().GetName() == "Table1") {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "CREATE TABLE entry not found");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u,
            "a single-target op must yield exactly one target entry, got " << found->Targets.size());
        UNIT_ASSERT_VALUES_EQUAL(found->Targets[0].Path, "Table1");
        UNIT_ASSERT_C(found->Targets[0].SourcePaths.empty(),
            "a plain create has no source, SourcePaths must be empty");
    }

    // TMove carries SrcPath/DstPath, no `Name` field at all, so a plain
    // reflection-based lookup for "Name" finds nothing and the propose was
    // being refused outright (see git history / diagnosis notes). A rename
    // must be accepted and must record the object's new location (DstPath,
    // since that is where a consumer would look the object up afterwards)
    // as Path, and its old location as the sole entry in SourcePaths.
    Y_UNIT_TEST(MoveRecordsDestinationAndSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "MoveSrc"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/MoveSrc", "/MyRoot/MoveDst");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveTable) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "MOVE TABLE entry not found -- the propose must not be refused");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "MoveDst",
            "Path must be the object's new (destination) location, database-relative");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].SourcePaths.size(), 1u,
            "a rename has exactly one source, got " << found->Targets[0].SourcePaths.size());
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].SourcePaths[0], "MoveSrc",
            "SourcePaths must record the object's old (source) location, database-relative");
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
    }

    // Altering the database root itself: WorkingDir="/", Name="MyRoot"
    // targets /MyRoot, which TTestEnv sets up as the domain root -- so
    // RelativeToDomain collapses the resolved path down to the empty string,
    // per its documented contract (a domain root's PathString() equals the
    // domain prefix). This must succeed and must not carry an approximate
    // or non-empty path.
    Y_UNIT_TEST(AlterDatabaseRootItselfStoresEmptyPath) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        runtime.GetAppData().FeatureFlags.SetEnableAlterDatabase(true);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterSubDomain(runtime, ++txId, "/", R"(
            Name: "MyRoot"
            SchemeLimits {
                MaxPaths: 1000
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterSubDomain) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ALTER SUBDOMAIN(root) entry not found -- altering the root must not be refused");
        UNIT_ASSERT_VALUES_EQUAL(found->Targets.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "",
            "the database root's own path, relative to itself, must be the empty string");
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
    }

    // CreateIndexedTable already has a special case in
    // ExtractSchemeChangeTargetName (returns TableDescription.Name directly),
    // bypassing the reflection loop entirely. Confirm it actually produces
    // exactly one record and that the stored path is the table, not an
    // index impl table synthesized during the same tx.
    Y_UNIT_TEST(CreateIndexedTableWritesExactlyOneRecordForTheTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "IndexedTable1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "byValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecords(runtime);
        ui32 indexedTableRecords = 0;
        TVector<TString> storedPaths;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable) {
                ++indexedTableRecords;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                storedPaths.push_back(e.Targets[0].Path);
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(indexedTableRecords, 1u,
            "CreateIndexedTable must write exactly one record for the whole requested tx");
        UNIT_ASSERT_VALUES_EQUAL_C(storedPaths[0], "IndexedTable1",
            "the stored path must be the table itself, not an index impl table");
    }
}

namespace {

// Drivers for the shapes whose recorded target is not a plain WorkingDir/Name.
// Each runs under a nested working dir, where a prefix mistake cannot hide.
struct TPathShapeCase {
    TString Name;
    NKikimrSchemeOp::EOperationType OpType;
    TVector<TString> ExpectedPaths;
    std::function<void(TTestActorRuntime&, TTestEnv&, ui64&)> Drive;
};

const TVector<TPathShapeCase>& GetPathShapeCases() {
    static const TVector<TPathShapeCase> cases = {
        // The record must carry the whole multi-segment path the user asked for,
        // not just the leaf the auto-mkdir chain ends on.
        {"CreateTable auto-mkdir", NKikimrSchemeOp::ESchemeOpCreateTable, {"Dir1/A/B/Leaf"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "A/B/Leaf"
                 Columns { Name: "key" Type: "Uint64" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
         }},

        // The target is nested one level inside TableDescription, and the op also
        // synthesizes index impl tables that must not become the target.
        {"CreateIndexedTable", NKikimrSchemeOp::ESchemeOpCreateIndexedTable, {"Dir1/Table1"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateIndexedTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 TableDescription {
                     Name: "Table1"
                     Columns { Name: "key" Type: "Uint64" }
                     Columns { Name: "value" Type: "Utf8" }
                     KeyColumnNames: ["key"]
                 }
                 IndexDescription {
                     Name: "Index1"
                     KeyColumnNames: ["value"]
                     Type: EIndexTypeGlobal
                 }
             )");
             env.TestWaitNotification(runtime, txId);
         }},

        // WorkingDir/TableName/StreamDescription.Name: two levels below the working
        // dir, so neither a direct-child lookup nor the reflection walk finds it.
        {"CreateCdcStream", NKikimrSchemeOp::ESchemeOpCreateCdcStream, {"Dir1/Table1/Stream1"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "Table1"
                 Columns { Name: "key"   Type: "Uint64" }
                 Columns { Name: "value" Type: "Utf8" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestCreateCdcStream(runtime, ++txId, "/MyRoot/Dir1", R"(
                 TableName: "Table1"
                 StreamDescription {
                     Name: "Stream1"
                     Mode: ECdcStreamModeKeysOnly
                     Format: ECdcStreamFormatProto
                 }
             )");
             env.TestWaitNotification(runtime, txId);
         }},

        // WorkingDir/TableName/StreamName, and no field literally named Name.
        {"AlterCdcStream", NKikimrSchemeOp::ESchemeOpAlterCdcStream, {"Dir1/Table1/Stream1"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "Table1"
                 Columns { Name: "key"   Type: "Uint64" }
                 Columns { Name: "value" Type: "Utf8" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestCreateCdcStream(runtime, ++txId, "/MyRoot/Dir1", R"(
                 TableName: "Table1"
                 StreamDescription {
                     Name: "Stream1"
                     Mode: ECdcStreamModeKeysOnly
                     Format: ECdcStreamFormatProto
                 }
             )");
             env.TestWaitNotification(runtime, txId);
             TestAlterCdcStream(runtime, ++txId, "/MyRoot/Dir1", R"(
                 TableName: "Table1"
                 StreamName: "Stream1"
                 Disable {}
             )");
             env.TestWaitNotification(runtime, txId);
         }},

        // A rename records the destination, so the path a consumer looks up after
        // the op is the new one, not the vanished source.
        {"MoveTable", NKikimrSchemeOp::ESchemeOpMoveTable, {"Dir1/MoveDst"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "MoveSrc"
                 Columns { Name: "key" Type: "Uint64" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestMoveTable(runtime, ++txId, "/MyRoot/Dir1/MoveSrc", "/MyRoot/Dir1/MoveDst");
             env.TestWaitNotification(runtime, txId);
         }},

        // TMoveIndex.SrcPath/DstPath are index names relative to TablePath, not
        // absolute paths -- treating them as absolute resolved to nothing.
        {"MoveIndex", NKikimrSchemeOp::ESchemeOpMoveIndex, {"Dir1/Table1/Index2"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateIndexedTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 TableDescription {
                     Name: "Table1"
                     Columns { Name: "key" Type: "Uint64" }
                     Columns { Name: "value" Type: "Utf8" }
                     KeyColumnNames: ["key"]
                 }
                 IndexDescription {
                     Name: "Index1"
                     KeyColumnNames: ["value"]
                     Type: EIndexTypeGlobal
                 }
             )");
             env.TestWaitNotification(runtime, txId);
             TestMoveIndex(runtime, ++txId, "/MyRoot/Dir1/Table1", "Index1", "Index2", false);
             env.TestWaitNotification(runtime, txId);
         }},

        // N destinations in one op: each must get its own target, in request order.
        {"CreateConsistentCopyTables", NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables,
         {"Dir1/Dst1", "Dir1/Dst2"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "Src1"
                 Columns { Name: "key" Type: "Uint64" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "Src2"
                 Columns { Name: "key" Type: "Uint64" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestConsistentCopyTables(runtime, ++txId, "/MyRoot/Dir1", R"(
                 CopyTableDescriptions {
                     SrcPath: "/MyRoot/Dir1/Src1"
                     DstPath: "/MyRoot/Dir1/Dst1"
                 }
                 CopyTableDescriptions {
                     SrcPath: "/MyRoot/Dir1/Src2"
                     DstPath: "/MyRoot/Dir1/Dst2"
                 }
             )");
             env.TestWaitNotification(runtime, txId);
         }},

        // TTruncateTable names its target TableName, not Name.
        {"TruncateTable", NKikimrSchemeOp::ESchemeOpTruncateTable, {"Dir1/Table1"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestCreateTable(runtime, ++txId, "/MyRoot/Dir1", R"(
                 Name: "Table1"
                 Columns { Name: "key"   Type: "Uint64" }
                 Columns { Name: "value" Type: "Utf8" }
                 KeyColumnNames: ["key"]
             )");
             env.TestWaitNotification(runtime, txId);
             TestTruncateTable(runtime, ++txId, "/MyRoot/Dir1", "Table1");
             env.TestWaitNotification(runtime, txId);
         }},

        // TAlterUserAttributes names its target PathName, not Name.
        {"AlterUserAttributes", NKikimrSchemeOp::ESchemeOpAlterUserAttributes, {"Dir1/Sub"},
         [](TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
             TestMkDir(runtime, ++txId, "/MyRoot/Dir1", "Sub");
             env.TestWaitNotification(runtime, txId);
             TestUserAttrs(runtime, ++txId, "/MyRoot/Dir1", "Sub",
                 AlterUserAttrs({{"attr1", "value1"}}));
             env.TestWaitNotification(runtime, txId);
         }},
    };
    return cases;
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsPathCorrectnessTests) {
    // A resolvable-but-wrong path is the failure mode here: the recorded path must
    // resolve to the very object the operation touched, not to a plausible sibling.
    Y_UNIT_TEST(RecordedPathResolvesToTouchedObject) {
        for (const auto& testCase : GetPathShapeCases()) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
            ui64 txId = 100;

            // Cleanup retires records no subscriber still needs, so one must be
            // registered for the driver's record to survive to the read below.
            TAutoPtr<IEventHandle> regHandle;
            RegisterSubscriber(runtime, "test:sub", regHandle);

            TestMkDir(runtime, ++txId, "/MyRoot", "Dir1");
            env.TestWaitNotification(runtime, txId);

            testCase.Drive(runtime, env, txId);

            UNIT_ASSERT_VALUES_EQUAL_C(
                GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing"), 0u,
                testCase.Name << ": the driver must not fail to resolve a path");

            auto entries = ReadSchemeChangeRecordsFromTable(runtime);
            const TSchemeChangeRecordEntry* found = nullptr;
            for (const auto& e : entries) {
                if (e.Body.GetOperationType() == testCase.OpType) {
                    found = &e;
                }
            }
            UNIT_ASSERT_C(found, testCase.Name << ": no record for "
                << NKikimrSchemeOp::EOperationType_Name(testCase.OpType));
            UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), testCase.ExpectedPaths.size(),
                testCase.Name << ": target count mismatch, got " << AllTargetPaths(*found));
            for (size_t i = 0; i < testCase.ExpectedPaths.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[i].Path, testCase.ExpectedPaths[i],
                    testCase.Name << ": target " << i << " path mismatch, got "
                    << AllTargetPaths(*found));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, i);
            }
        }
    }
}

// Drives suspected-outage op types against the target extractors.
Y_UNIT_TEST_SUITE(TSchemeChangeRecordsOperationAuditTests) {
    Y_UNIT_TEST(AlterUserAttributesRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", "SubDir");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestUserAttrs(runtime, ++txId, "/MyRoot", "SubDir",
            AlterUserAttrs({{"attr1", "value1"}}));
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "AlterUserAttributes must resolve a path; TAlterUserAttributes.PathName is not "
            "found by the generic reflection walk (it looks for a field literally named Name)");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterUserAttributes) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_C(AnyPathContains(e, "SubDir"),
                    "expected the target to be SubDir, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterUserAttributes must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterTableByPathIdRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 tablePathId = DescribePath(runtime, "/MyRoot/Table1").GetPathId();
        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // An id-addressed alter names no path at all: WorkingDir is a placeholder
        // and TTableDescription.Name is unset.
        TStringBuilder schema;
        schema << "Id_Deprecated: " << tablePathId << Endl
               << R"(Columns { Name: "added" Type: "Uint32" })";
        TestAlterTable(runtime, ++txId, "not used", schema);
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "an alter addressed by path id must resolve a path");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTable) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "an id-addressed alter must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateWithUserAttributesRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // A create carries TAlterUserAttributes as a side payload with PathName
        // unset; the target is still named by the create's own payload.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"attr1", "value1"}}));
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "CreateTable with user attributes must resolve a path, not lose its target to the "
            "empty AlterUserAttributes.PathName");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTable) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateTable with user attributes must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterCdcStreamRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamDescription {
                Name: "Stream1"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // TAlterCdcStream{TableName,StreamName} has no field literally named Name, so the
        // generic reflection walk cannot find it; the CDC extractor names it explicitly.
        TestAlterCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            StreamName: "Stream1"
            Disable {}
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "AlterCdcStream must resolve a path, not bump SchemeChangePathMissing: "
            "counter went " << rejectedBefore << " -> " << rejectedAfter);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterCdcStream) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(e.Targets.size(), 1u);
                UNIT_ASSERT_C(AnyPathContains(e, "Stream1"),
                    "expected the target to be the changefeed, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterCdcStream must produce a scheme change record");
    }

    // DropCdcStream resolves every stream in its repeated StreamName field, so its
    // propose is no longer refused; see DropCdcStreamRecordsEveryStream.

    Y_UNIT_TEST(MoveIndexRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index1"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: ExtractSchemeChangeMoveTarget used to treat TMoveIndex.SrcPath/
        // DstPath as absolute paths, but they are relative index names under
        // TablePath (confirmed via mainTablePath.Child(srcIndex) in
        // index/operation_move_index.cpp), so TPath::Resolve used to fail and
        // refuse the propose.
        TestMoveIndex(runtime, ++txId, "/MyRoot/Table1", "Index1", "Index2", false);
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "MoveIndex must resolve a path");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpMoveIndex) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Index2"),
                    "expected the target to be the new index name, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "MoveIndex must produce a scheme change record");
    }

    Y_UNIT_TEST(UpgradeSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
        )");
        env.TestWaitNotification(runtime, txId);
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Already safe: TUpgradeSubDomain.Name is a direct scalar Name field,
        // found by the generic reflection walk. Kept as a regression guard.
        TestUpgradeSubDomain(runtime, ++txId, "/MyRoot", "USD1");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "UpgradeSubDomain must resolve a path");
        // Not verified via ReadSchemeChangeRecords here: registering a second
        // subscriber (its internal read-only helper) is refused once the
        // domain has been upgraded to serve transactions on its own, since
        // scheme change subscribers require a single transaction-supporting
        // domain. The counter check above is the load-bearing assertion.
    }

    Y_UNIT_TEST(TruncateTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: TTruncateTable.TableName has no field literally named Name,
        // so the generic reflection walk used to miss it and refuse the propose.
        TestTruncateTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "TruncateTable must resolve a path");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpTruncateTable) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "TruncateTable must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateContinuousBackupRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
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

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: TCreateContinuousBackup.TableName (and the sibling Alter/Drop
        // messages) have no field literally named Name, so the generic
        // reflection walk used to miss them and refuse the propose.
        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "CreateContinuousBackup must resolve a path");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateContinuousBackup) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateContinuousBackup must produce a scheme change record");
    }

    Y_UNIT_TEST(RestoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Restore targets an existing table (it restores INTO it), unlike
        // Create; the target must already exist for the propose to get past
        // path resolution at all.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // TRestoreTask names its target TableName, like TTruncateTable/
        // TCreateContinuousBackup before their extractor fixes -- not yet
        // special-cased here. Driving Restore to full completion needs a
        // real backup at the FS/S3 path (see ut_restore's TS3Mock rig); with
        // no such fixture the data phase hangs, so this only checks the
        // propose-time behavior (the same shape the outbox's own check
        // observes) rather than the post-finalize resolve. That is still a
        // real, meaningful assertion: before this fix, the propose itself
        // was refused -- see the crash this test produced before the fix
        // (Restore's AbortPropose is also an unconditional Y_ABORT stub, so
        // the refusal took the tablet down rather than cleanly failing).
        TestRestore(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            FSSettings {
                BasePath: "/tmp"
                Path: "restore"
            }
        )");

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "Restore must resolve a path");

        // The op never completes here, so the row is still the propose-time one:
        // its path is assertable, its PathId is not yet written back.
        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore) {
                found = &e;
            }
        }
        UNIT_ASSERT_C(found, "Restore must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u,
            "Restore targets one table, got " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "Table1",
            "the target must be the restored-into table, not its working directory");
    }

    Y_UNIT_TEST(AlterLoginRecordsAPath) {
        // TAlterLogin's target (the user/group being altered) is nested
        // inside a oneof (e.g. CreateUser.User), never a top-level scalar
        // Name/PathName/TableName field -- the generic reflection walk and
        // every existing special case both miss it.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        const auto hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "alice", hashes.HashedPassword);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "AlterLogin must resolve a path");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterLogin) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "AlterLogin must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u, "AlterLogin");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "",
            "AlterLogin has no path-bearing target -- it must be attributed to the "
            "database root itself, relative to which the root's own path is empty");
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found);
    }

    // The Drop.Id branch in ResolveSchemeChangeTargets (a
    // TModifyScheme with Drop.Id set and Drop.Name/WorkingDir unset) is read
    // but was never exercised by a test. Its only production producer is
    // ydb/core/tx/replication/controller/dst_remover.cpp:46. TestForceDropUnsafe
    // drives this exact shape: DROP_BY_PATH_ID_HELPERS builds TDrop with only
    // Id set, no Name, no WorkingDir.
    Y_UNIT_TEST(DropByIdRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirToDropById");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/DirToDropById"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestForceDropUnsafe(runtime, ++txId, droppedLocalId);
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore,
            "Drop.Id (Drop.Name/WorkingDir unset) must resolve a path by PathId");

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpForceDropUnsafe) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ForceDropUnsafe (Drop.Id) must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u, "ForceDropUnsafe");
        UNIT_ASSERT_C(AnyPathContains(*found, "DirToDropById"),
            "expected the target to be the dropped directory, got: " << AllTargetPaths(*found));
        // The object is gone by finalize time, so the record must carry the
        // resolved-at-propose PathId of the object the drop actually
        // targeted -- assert against the identity captured before the drop,
        // not a live re-resolve (which would correctly fail post-drop).
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId,
            "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId,
            "recorded PathLocalId must match the dropped object's identity, not some other path");
    }

    Y_UNIT_TEST(CreatePQGroupRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "PQGroup1"),
                    "expected the target to be PQGroup1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreatePQGroup must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSubDomain) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "USD1"),
                    "expected the target to be USD1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSubDomain must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateRtmrVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateRtmrVolume(runtime, ++txId, "/MyRoot", R"(
            Name: "rtmr1"
            PartitionsCount: 0
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateRtmrVolume) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "rtmr1"),
                    "expected the target to be rtmr1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateRtmrVolume must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateBlockStoreVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume1");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "BSVolume1"),
                    "expected the target to be BSVolume1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateBlockStoreVolume must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateKesusRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateKesus(runtime, ++txId, "/MyRoot", R"(Name: "Kesus1")");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateKesus) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Kesus1"),
                    "expected the target to be Kesus1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateKesus must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSolomonVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon1"
            PartitionCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSolomonVolume) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Solomon1"),
                    "expected the target to be Solomon1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSolomon must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateFileStoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        NKikimrSchemeOp::TFileStoreDescription descr;
        descr.SetName("FileStore1");
        auto& config = *descr.MutableConfig();
        config.SetBlockSize(4096);
        config.SetBlocksCount(4096);
        config.SetFileSystemId("FileStore1");
        config.SetCloudId("cloud");
        config.SetFolderId("folder");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateFileStore) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "FileStore1"),
                    "expected the target to be FileStore1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateFileStore must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateColumnStoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore1"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnStore) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "OlapStore1"),
                    "expected the target to be OlapStore1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateOlapStore must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateColumnTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ColumnTable1"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnTable) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ColumnTable1"),
                    "expected the target to be ColumnTable1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateColumnTable must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateExtSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "ExtSubDomain1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExtSubDomain) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ExtSubDomain1"),
                    "expected the target to be ExtSubDomain1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateExtSubDomain must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateExternalTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).RunFakeConfigDispatcher(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalDataSource1"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalTable1"
            SourceType: "General"
            DataSourcePath: "/MyRoot/ExternalDataSource1"
            Location: "/"
            Columns { Name: "key" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExternalTable) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ExternalTable1"),
                    "expected the target to be ExternalTable1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateExternalTable must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateExternalDataSourceRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).RunFakeConfigDispatcher(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalDataSource2"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExternalDataSource) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ExternalDataSource2"),
                    "expected the target to be ExternalDataSource2, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateExternalDataSource must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateViewRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
            Name: "View1"
            QueryText: "Some query"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateView) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "View1"),
                    "expected the target to be View1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateView must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateResourcePoolRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);

        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
            Name: "ResourcePool1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateResourcePool) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ResourcePool1"),
                    "expected the target to be ResourcePool1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateResourcePool must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateBackupCollectionRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).EnableBackupService(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "BackupCollection1"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateBackupCollection) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "BackupCollection1"),
                    "expected the target to be BackupCollection1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateBackupCollection must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSysViewRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys", R"(
            Name: "sys_view_1"
            Type: EPartitionStats
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSysView) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "sys_view_1"),
                    "expected the target to be sys_view_1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSysView must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSecretRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSecret(runtime, ++txId, "/MyRoot", R"(
            Name: "Secret1"
            Value: "value1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSecret) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Secret1"),
                    "expected the target to be Secret1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSecret must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateStreamingQueryRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
            Name: "StreamingQuery1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateStreamingQuery) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "StreamingQuery1"),
                    "expected the target to be StreamingQuery1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateStreamingQuery must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateTestShardSetRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("TestShardSet1"));
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTestShardSet) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "TestShardSet1"),
                    "expected the target to be TestShardSet1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateTestShardSet must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSequenceRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "Sequence1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSequence) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Sequence1"),
                    "expected the target to be Sequence1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSequence must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateLockRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestLock(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateLock must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterPersQueueGroupRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup1"
            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 20}}
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "PQGroup1"),
                    "expected the target to be PQGroup1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterPQGroup must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterBlockStoreVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume1");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        NKikimrSchemeOp::TBlockStoreVolumeDescription alterDescr;
        alterDescr.SetName("BSVolume1");
        auto& alterVc = *alterDescr.MutableVolumeConfig();
        alterVc.SetVersion(1);
        alterVc.AddPartitions()->SetBlockCount(16);
        alterVc.AddPartitions()->SetBlockCount(16);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", alterDescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "BSVolume1"),
                    "expected the target to be BSVolume1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterBlockStoreVolume must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterKesusRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateKesus(runtime, ++txId, "/MyRoot", R"(Name: "Kesus1")");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterKesus(runtime, ++txId, "/MyRoot", R"(
            Name: "Kesus1"
            Config { self_check_period_millis: 3000 }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterKesus) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Kesus1"),
                    "expected the target to be Kesus1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterKesus must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterExtSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "ExtSubDomain1")");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Not verified via ReadSchemeChangeRecords here, same constraint as
        // UpgradeSubDomainRecordsAPath: ExternalSchemeShard:true turns this
        // SchemeShard into one no longer serving a single transaction-
        // supporting domain, so registering a second (internal, read-only)
        // subscriber is refused. The counter check is the load-bearing
        // assertion -- it fires before the domain state changes.
        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "ExtSubDomain1"
            ExternalSchemeShard: true
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "name_USER_0_kind_hdd-1"
                Kind: "pool-kind-1"
            }
            StoragePools {
                Name: "name_USER_0_kind_hdd-2"
                Kind: "pool-kind-2"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "AlterExtSubDomain must resolve a path");
    }

    Y_UNIT_TEST(AlterSolomonVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon1"
            PartitionCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        // Alter without any actual partition/channel change: a real alter needs
        // reflecting the just-created shard layout, which this test does not
        // need -- only that the outbox resolves the target before whatever
        // status the op itself returns.
        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        TestAlterSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon1"
            ChannelProfileId: 3
        )", {NKikimrScheme::StatusInvalidParameter});
        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "AlterSolomon must resolve a path");
    }

    Y_UNIT_TEST(AlterColumnStoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore1"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore1"
            AlterSchemaPresets {
                Name: "default"
                AlterSchema {
                    AddColumns { Name: "comment" Type: "Utf8" }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterColumnStore) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "OlapStore1"),
                    "expected the target to be OlapStore1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterOlapStore must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterColumnTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ColumnTable1"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ColumnTable1"
            UpsertMultiColumnStatistics { Name: "s1" ColumnNames: "data" Types: COUNT_MIN_SKETCH }
        )", {NKikimrScheme::StatusSuccess});
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterColumnTable) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ColumnTable1"),
                    "expected the target to be ColumnTable1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterColumnTable must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterSequenceRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSequence(runtime, ++txId, "/MyRoot", R"(Name: "Sequence1")");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterSequence(runtime, ++txId, "/MyRoot", R"(
            Name: "Sequence1"
            Increment: 2
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterSequence) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Sequence1"),
                    "expected the target to be Sequence1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterSequence must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterResourcePoolRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);
        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
            Name: "ResourcePool1"
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
            Name: "ResourcePool1"
            Properties {
                Properties {
                    key: "concurrent_query_limit",
                    value: "20"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterResourcePool) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "ResourcePool1"),
                    "expected the target to be ResourcePool1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterResourcePool must produce a scheme change record");
    }

    // For the drop-family tests below the object is gone by finalize time, so
    // each of these captures the
    // touched object's identity BEFORE the drop (like DropByIdRecordsAPath)
    // and asserts the record's PathOwnerId/PathLocalId against that captured
    // identity, rather than re-resolving a path that no longer exists.

    Y_UNIT_TEST(DropTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Table1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTable) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropTable must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Table1"),
            "expected the target to be Table1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropPersQueueGroupRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/PQGroup1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "PQGroup1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropPersQueueGroup) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropPQGroup must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "PQGroup1"),
            "expected the target to be PQGroup1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "USD1")");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/USD1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropSubDomain(runtime, ++txId, "/MyRoot", "USD1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropSubDomain) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropSubDomain must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "USD1"),
            "expected the target to be USD1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropKesusRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateKesus(runtime, ++txId, "/MyRoot", R"(Name: "Kesus1")");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Kesus1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropKesus(runtime, ++txId, "/MyRoot", "Kesus1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropKesus) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropKesus must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Kesus1"),
            "expected the target to be Kesus1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropSolomonVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon1"
            PartitionCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Solomon1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropSolomon(runtime, ++txId, "/MyRoot", "Solomon1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropSolomonVolume) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropSolomon must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Solomon1"),
            "expected the target to be Solomon1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(ForceDropSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "USD1")");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/USD1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestForceDropSubDomain(runtime, ++txId, "/MyRoot", "USD1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpForceDropSubDomain) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ForceDropSubDomain must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "USD1"),
            "expected the target to be USD1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(ForceDropExtSubDomainRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "ExtSubDomain1")");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/ExtSubDomain1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "ExtSubDomain1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "ForceDropExtSubDomain must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "ExtSubDomain1"),
            "expected the target to be ExtSubDomain1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropFileStoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        NKikimrSchemeOp::TFileStoreDescription descr;
        descr.SetName("FileStore1");
        auto& config = *descr.MutableConfig();
        config.SetBlockSize(4096);
        config.SetBlocksCount(4096);
        config.SetFileSystemId("FileStore1");
        config.SetCloudId("cloud");
        config.SetFolderId("folder");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        TestCreateFileStore(runtime, ++txId, "/MyRoot", descr.DebugString());
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/FileStore1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropFileStore(runtime, ++txId, "/MyRoot", "FileStore1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropFileStore) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropFileStore must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "FileStore1"),
            "expected the target to be FileStore1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropColumnStoreRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore1"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/OlapStore1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropOlapStore(runtime, ++txId, "/MyRoot", "OlapStore1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnStore) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropOlapStore must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "OlapStore1"),
            "expected the target to be OlapStore1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropColumnTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ColumnTable1"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/ColumnTable1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropColumnTable(runtime, ++txId, "/MyRoot", "ColumnTable1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnTable) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropColumnTable must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "ColumnTable1"),
            "expected the target to be ColumnTable1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropExternalTableRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).RunFakeConfigDispatcher(true));
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalDataSource1"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalTable1"
            SourceType: "General"
            DataSourcePath: "/MyRoot/ExternalDataSource1"
            Location: "/"
            Columns { Name: "key" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/ExternalTable1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropExternalTable(runtime, ++txId, "/MyRoot", "ExternalTable1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropExternalTable) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropExternalTable must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "ExternalTable1"),
            "expected the target to be ExternalTable1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropExternalDataSourceRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).RunFakeConfigDispatcher(true));
        ui64 txId = 100;

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalDataSource1"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/ExternalDataSource1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropExternalDataSource(runtime, ++txId, "/MyRoot", "ExternalDataSource1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropExternalDataSource) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropExternalDataSource must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "ExternalDataSource1"),
            "expected the target to be ExternalDataSource1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropViewRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
            Name: "View1"
            QueryText: "Some query"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/View1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropView(runtime, ++txId, "/MyRoot", "View1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropView) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropView must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "View1"),
            "expected the target to be View1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropContinuousBackupRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Table1"));
        const ui64 tableOwnerId = pathVersion.PathId.OwnerId;
        const ui64 tableLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropContinuousBackup) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropContinuousBackup must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Table1"),
            "expected the target to be Table1, got: " << AllTargetPaths(*found));
        // DropContinuousBackup's target is the table it was created on -- the
        // table itself is not dropped, so the ordinary live-resolve
        // assertion would also work here, but the pre-op-captured identity
        // is used for symmetry with the other Drop* rows in this suite.
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, tableOwnerId, "recorded PathOwnerId must match Table1's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, tableLocalId, "recorded PathLocalId must match Table1's identity");
    }

    Y_UNIT_TEST(DropResourcePoolRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".metadata/workload_manager/pools");
        env.TestWaitNotification(runtime, txId);
        TestCreateResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", R"(
            Name: "ResourcePool1"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/.metadata/workload_manager/pools/ResourcePool1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropResourcePool(runtime, ++txId, "/MyRoot/.metadata/workload_manager/pools", "ResourcePool1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropResourcePool) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropResourcePool must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "ResourcePool1"),
            "expected the target to be ResourcePool1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropBackupCollectionRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).EnableBackupService(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "BackupCollection1"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/.backups/collections/BackupCollection1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(Name: "BackupCollection1")");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropBackupCollection) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropBackupCollection must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "BackupCollection1"),
            "expected the target to be BackupCollection1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropSysViewRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys", R"(
            Name: "sys_view_1"
            Type: EPartitionStats
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/.sys/sys_view_1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropSysView(runtime, ++txId, "/MyRoot/.sys", "sys_view_1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropSysView) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropSysView must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "sys_view_1"),
            "expected the target to be sys_view_1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropSecretRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateSecret(runtime, ++txId, "/MyRoot", R"(
            Name: "Secret1"
            Value: "value1"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Secret1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropSecret(runtime, ++txId, "/MyRoot", "Secret1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropSecret) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropSecret must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Secret1"),
            "expected the target to be Secret1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropStreamingQueryRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
            Name: "StreamingQuery1"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/StreamingQuery1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropStreamingQuery(runtime, ++txId, "/MyRoot", "StreamingQuery1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropStreamingQuery) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropStreamingQuery must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "StreamingQuery1"),
            "expected the target to be StreamingQuery1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropTestShardSetRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTestShardSet(runtime, ++txId, "/MyRoot", CreateTestShardSetConfig("TestShardSet1"));
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/TestShardSet1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropTestShardSet(runtime, ++txId, "/MyRoot", "TestShardSet1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTestShardSet) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropTestShardSet must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "TestShardSet1"),
            "expected the target to be TestShardSet1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropTableIndexRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index1"
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Table1/Index1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            IndexName: "Index1"
        )");
        env.TestWaitNotification(runtime, txId);

        // TestDropTableIndex issues ESchemeOpDropIndex as the top-level
        // OperationType (see GENERIC_HELPERS(DropTableIndex, ...ESchemeOpDropIndex,
        // ...) in ut_helpers/helpers.cpp) -- ESchemeOpDropTableIndex is a
        // distinct, apparently-unissued enum value.
        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropIndex) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropTableIndex must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Index1"),
            "expected the target to be Index1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped index's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped index's identity");
    }

    Y_UNIT_TEST(DropLockRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestLock(runtime, ++txId, "/MyRoot", "Table1");
        const ui64 lockId = txId;
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestUnlock(runtime, ++txId, lockId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "DropLock (unlock) must produce a scheme change record");
    }

    Y_UNIT_TEST(RmDirRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirToRemove");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/DirToRemove"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestRmDir(runtime, ++txId, "/MyRoot", "DirToRemove");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpRmDir) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "RmDir must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "DirToRemove"),
            "expected the target to be DirToRemove, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the removed dir's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the removed dir's identity");
    }

    Y_UNIT_TEST(ModifyACLRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestModifyACL(runtime, ++txId, "/MyRoot", "Table1", "", "bob@builtin");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpModifyACL) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "ModifyACL must produce a scheme change record");
    }

    // ESchemeOpCreateIndexBuild IS issued as a top-level
    // TModifyScheme.OperationType by TIndexBuilder (InitiateIndexBuild.Table,
    // an absolute path) -- ConstructParts then decomposes it into
    // TCreateTableIndex/TInitializeBuildIndex/... sub-ops. Before the
    // extractor fix below, InitiateIndexBuild.Table was not recognized by
    // any special case or the generic Name-field walk, so the propose was
    // refused. That refusal is worse than a clean StatusInvalidParameter:
    // AbortOperationPropose unconditionally calls AbortPropose on every
    // already-proposed part in the same request, including the index's
    // impl-table TCreateTable sub-op -- whose AbortPropose is a Y_ABORT
    // stub ("no AbortPropose for TCreateTable") -- so the refusal crashed
    // the tablet (SIGABRT) instead of failing cleanly. VERBATIM crash seen
    // before the fix: "VERIFY failed: no AbortPropose for TCreateTable" at
    // schemeshard__operation_create_table.cpp:814, reached via
    // AbortOperationPropose <- CheckSchemeChangeRecordHasPath rejecting
    // InitiateIndexBuild. Fixed by resolving InitiateIndexBuild.Table (and
    // CancelIndexBuild.TablePath, same shape) directly in
    // ResolveSchemeChangeTargets -- see schemeshard__scheme_change_records.cpp.
    Y_UNIT_TEST(CreateIndexBuildRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "index" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table1",
            TBuildIndexConfig{"Index1", NKikimrSchemeOp::EIndexTypeGlobal, {"index"}, {}, {}});
        const ui64 buildIndexId = txId;
        env.TestWaitNotification(runtime, buildIndexId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexBuild) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateIndexBuild must produce a scheme change record");
    }

    Y_UNIT_TEST(CancelIndexBuildRecordsAPath) {
        // Same fix as CreateIndexBuildRecordsAPath: CancelIndexBuild.TablePath
        // is an absolute path, resolved directly now instead of refused.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "index" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table1",
            TBuildIndexConfig{"Index1", NKikimrSchemeOp::EIndexTypeGlobal, {"index"}, {}, {}});
        const ui64 buildIndexId = txId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
        env.TestWaitNotification(runtime, buildIndexId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Table1"),
                    "expected the target to be Table1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CancelIndexBuild must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateReplicationRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateReplication) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Replication1"),
                    "expected the target to be Replication1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateReplication must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterReplicationRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            State { Paused {} }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterReplication) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Replication1"),
                    "expected the target to be Replication1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterReplication must produce a scheme change record");
    }

    Y_UNIT_TEST(DropReplicationRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Replication1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropReplication) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropReplication must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Replication1"),
            "expected the target to be Replication1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropReplicationCascadeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Replication1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropReplicationCascade(runtime, ++txId, "/MyRoot", "Replication1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropReplicationCascade) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropReplicationCascade must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Replication1"),
            "expected the target to be Replication1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(CreateTransferRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateTransfer) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Transfer1"),
                    "expected the target to be Transfer1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateTransfer must produce a scheme change record");
    }

    Y_UNIT_TEST(AlterTransferRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAlterTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            State { Paused {} }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterTransfer) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "Transfer1"),
                    "expected the target to be Transfer1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AlterTransfer must produce a scheme change record");
    }

    Y_UNIT_TEST(DropTransferRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Transfer1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropTransfer(runtime, ++txId, "/MyRoot", "Transfer1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTransfer) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropTransfer must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Transfer1"),
            "expected the target to be Transfer1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(DropTransferCascadeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true).InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTransfer(runtime, ++txId, "/MyRoot", R"(
            Name: "Transfer1"
            Config {
              TransferSpecific {
                Target {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/Transfer1"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestDropTransferCascade(runtime, ++txId, "/MyRoot", "Transfer1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTransferCascade) {
                found = &e;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DropTransferCascade must produce a scheme change record");
        UNIT_ASSERT_C(AnyPathContains(*found, "Transfer1"),
            "expected the target to be Transfer1, got: " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId, "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId, "recorded PathLocalId must match the dropped object's identity");
    }

    Y_UNIT_TEST(BackupRecordsAPath) {
        // Same shape as RestoreRecordsAPath above: driving Backup to full
        // completion needs a real/mocked S3 endpoint (see ut_backup's
        // TS3Mock rig); without it the data phase hangs, so this only
        // checks the propose-time path resolution -- the same check the
        // outbox itself performs -- not the post-finalize resolve.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        TestBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            S3Settings {
                Endpoint: "localhost:1"
                Scheme: HTTP
            }
        )");

        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "Backup must resolve a path");

        // The op never completes here, so the row is still the propose-time one:
        // its path is assertable, its PathId is not yet written back.
        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        const TSchemeChangeRecordEntry* found = nullptr;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup) {
                found = &e;
            }
        }
        UNIT_ASSERT_C(found, "Backup must produce a scheme change record");
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets.size(), 1u,
            "Backup targets one table, got " << AllTargetPaths(*found));
        UNIT_ASSERT_VALUES_EQUAL_C(found->Targets[0].Path, "Table1",
            "the target must be the backed-up table, not its working directory");
    }
    Y_UNIT_TEST(AssignBlockStoreVolumeRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume1");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume1", "Owner123");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume) {
                found = true;
                UNIT_ASSERT_C(AnyPathContains(e, "BSVolume1"),
                    "expected the target to be BSVolume1, got: " << AllTargetPaths(e));
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "AssignBlockStoreVolume must produce a scheme change record");
    }

    Y_UNIT_TEST(UpgradeSubDomainDecisionRecordsAPath) {
        // Same registration constraint as UpgradeSubDomainRecordsAPath: once
        // the domain is upgraded to serve transactions on its own, a second
        // subscriber registration is refused, so this asserts via the
        // rejection counter (propose-time resolution), not a live read.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        // Must register before AlterSubDomain(Coordinators/Mediators): that
        // alone already breaks single-domain-serving, and RegisterSubscriber
        // itself would then be refused (not just a later ReadSchemeChangeRecords
        // call) -- same ordering constraint as UpgradeSubDomainRecordsAPath.
        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub", regHandle);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "USD1")");
        env.TestWaitNotification(runtime, txId);
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "USD1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )");
        env.TestWaitNotification(runtime, txId);

        TestUpgradeSubDomain(runtime, ++txId, "/MyRoot", "USD1");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        TestUpgradeSubDomainDecision(runtime, ++txId, "/MyRoot", "USD1", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);
        const ui64 rejectedAfter = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");
        UNIT_ASSERT_VALUES_EQUAL_C(rejectedAfter, rejectedBefore, "UpgradeSubDomainDecision must resolve a path");
    }
}

namespace {

// An alter addressed by path id ignores WorkingDir, so the op succeeds while the
// outbox is left with a name under a directory that does not exist.
ui64 ProposeAlterWithUnresolvableTarget(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table1"
        Columns { Name: "key"   Type: "Uint64" }
        Columns { Name: "value" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);

    const ui64 tablePathId = DescribePath(runtime, "/MyRoot/Table1").GetPathId();

    TStringBuilder schema;
    schema << "Id_Deprecated: " << tablePathId << Endl
           << R"(Name: "Table1")" << Endl
           << R"(Columns { Name: "added" Type: "Uint32" })";
    TestAlterTable(runtime, ++txId, "/MyRoot/NoSuchDir", schema);
    env.TestWaitNotification(runtime, txId);

    return tablePathId;
}

}  // anonymous namespace

// Proves the tally can be non-zero at all, and that it survives a restart.
Y_UNIT_TEST_SUITE(TSchemeChangeRecordsPathMissingTallyTests) {
    Y_UNIT_TEST(TallyObservesAnUnresolvableTarget) {
        // These two tests deliberately break the invariant the corpus mode asserts.
        if (SchemeChangeCorpusEnabled()) {
            return;
        }
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        auto& tally = TSchemeChangePathMissingTally::Instance();
        UNIT_ASSERT_VALUES_EQUAL_C(tally.Total(runtime), 0u, "nothing has failed to resolve yet");

        ProposeAlterWithUnresolvableTarget(runtime, env, txId);

        UNIT_ASSERT_VALUES_EQUAL_C(tally.Total(runtime), 1u,
            "an alter whose target names a non-existent directory must be counted as unresolved");
    }

    Y_UNIT_TEST(TallySurvivesASchemeShardReboot) {
        if (SchemeChangeCorpusEnabled()) {
            return;
        }
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        auto& tally = TSchemeChangePathMissingTally::Instance();
        ProposeAlterWithUnresolvableTarget(runtime, env, txId);
        UNIT_ASSERT_VALUES_EQUAL(tally.Total(runtime), 1u);

        RebootSchemeShard(runtime);

        UNIT_ASSERT_VALUES_EQUAL_C(
            GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing"), 0u,
            "the raw tablet counter must have restarted at zero, otherwise this proves nothing");
        UNIT_ASSERT_VALUES_EQUAL_C(tally.Total(runtime), 1u,
            "the tally must carry the pre-reboot increment across the restart");
    }
}
