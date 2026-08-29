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
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecordsFromTable;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSchemaTests) {
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

    Y_UNIT_TEST(CreateTableWritesLogEntry) {
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // UserSID must record who issued the DDL, not who owns the target: a
    // ModifyACL changes the owner without that owner issuing anything.
    Y_UNIT_TEST(UserSIDRecordsIssuerNotOwner) {
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

        // Positive companion: the owner is genuinely changed to someone who
        // never issues the later ALTER, so a later match can't be coincidence.
        TestModifyACL(runtime, ++txId, "/MyRoot", "Table1", "", "bob@builtin");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        for (size_t i = 1; i < entries.size(); ++i) {
            UNIT_ASSERT_C(entries[i].Order > entries[i-1].Order,
                "Orders must be strictly monotonic");
        }
    }

    Y_UNIT_TEST(PlanStepIsRecordedForCoordinatedOps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        UNIT_ASSERT(entries.size() >= 2);

        ui64 prevPlanStep = 0;
        for (const auto& e : entries) {
            // Bootstrap system views (.sys/*) are recorded too now that the
            // gate is the flag alone; they are not this test's DDL.
            if (AnyPathContains(e, ".sys/")) {
                continue;
            }
            UNIT_ASSERT_C(e.PlanStep >= prevPlanStep,
                "PlanStep should be monotonically non-decreasing: prev=" << prevPlanStep
                    << " current=" << e.PlanStep << " path=" << AllTargetPaths(e));
            prevPlanStep = e.PlanStep;
        }

        for (size_t i = 1; i < entries.size(); ++i) {
            const auto& prev = entries[i-1];
            const auto& curr = entries[i];
            // Order is the stream's only total order, and it is what cursors,
            // acks and retention are defined on.
            UNIT_ASSERT_C(curr.Order > prev.Order,
                "Order must strictly increase: prev=" << prev.Order << " curr=" << curr.Order);
            UNIT_ASSERT_C(curr.PlanStep != 0,
                "a finalised record must carry a PlanStep, order=" << curr.Order);
            // (PlanStep, TxId) is deliberately not asserted co-monotonic with Order: Order
            // is allocated at propose, PlanStep by the coordinator at plan time.
        }
    }

    Y_UNIT_TEST(MkDirWritesLogEntry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    Y_UNIT_TEST(CreateTableWithIndexProducesSingleParentRecord) {
        // A multi-part DDL emits exactly one record, carrying the TModifyScheme
        // as it arrived in the request; the target cluster re-runs decomposition
        // on replay.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "A/B/C/Leaf"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        size_t thisTxRecords = 0;
        for (const auto& e : entries) {
            if (e.TxId == (ui64)txId) ++thisTxRecords;
        }
        UNIT_ASSERT_VALUES_EQUAL(thisTxRecords, 1u);

        UNIT_ASSERT_VALUES_EQUAL_C(schemeshard->NextSchemeChangeOrderPersistCount, 1u,
            "Expected one NextSchemeChangeOrder persist for a batch, got "
            << schemeshard->NextSchemeChangeOrderPersistCount);
    }

    // Positive companion to DisabledByDefaultEmitsNoRecords: the identical DDL
    // with the flag on does produce a row, so the disabled case cannot pass vacuously.
    Y_UNIT_TEST(EnabledByOptInEmitsRecord) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        UNIT_ASSERT_C(!entries.empty(), "expected at least one outbox row with the feature enabled");
    }

    // Finalize resolves the target but writes back only PathOwnerId/PathLocalId;
    // Path keeps the propose-time synthesis, which must already be canonical.
    Y_UNIT_TEST(RecordCarriesCanonicalResolvedPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        // Trailing slash: propose-time string concatenation would leave a
        // double slash, while TPath::Resolve canonicalizes it away.
        TestCreateTable(runtime, ++txId, "/MyRoot/", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // A stored path must be relative to the database root and must be the full
    // path to the object -- a path truncated to empty must not pass.
    Y_UNIT_TEST(RecordPathIsRelativeToDatabaseRoot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
            Name: "Nested"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // The pathless allowlist is empty today, so an ordinary op must still produce
    // its record -- the invariant above must not be satisfiable by refusing everything.
    Y_UNIT_TEST(OrdinaryOpsStillProduceRecordsWhenAllowlistIsEmpty) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "PlainTable"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // One record per TModifyScheme in the request (Body is the atomic replay unit),
    // so a copy of N tables must list all N as targets, each with its own
    // destination and source.
    Y_UNIT_TEST(ConsistentCopyTablesRecordsAllTargetsWithSources) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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
        // Every one of the N targets must resolve; only target[0] carries a captured
        // PathId, so [1] gets a plain resolve check.
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 0);
        AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", *found, 1);
    }

    // A plain single-target op must still yield exactly one target, with SourcePaths
    // empty -- it is a repeated field, so "empty" is the assertion, not "unset".
    Y_UNIT_TEST(SingleTargetOpRecordsExactlyOneTarget) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // A rename records the object's new location as Path (where a consumer looks it
    // up afterwards) and its old location as the sole SourcePaths entry.
    Y_UNIT_TEST(MoveRecordsDestinationAndSource) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "MoveSrc"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/MoveSrc", "/MyRoot/MoveDst");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // The domain root's path relative to its own domain is the empty string, per
    // RelativeToDomain's contract -- that empty path is correct, not approximate.
    Y_UNIT_TEST(AlterDatabaseRootItselfStoresEmptyPath) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.EnableSchemeChangeRecords(true);
        TTestEnv env(runtime, opts);
        runtime.GetAppData().FeatureFlags.SetEnableAlterDatabase(true);
        ui64 txId = 100;

        TestAlterSubDomain(runtime, ++txId, "/", R"(
            Name: "MyRoot"
            SchemeLimits {
                MaxPaths: 1000
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    // Exactly one record for the whole requested TModifyScheme, and its path is the
    // table -- not an index impl table synthesized during the same tx.
    Y_UNIT_TEST(CreateIndexedTableWritesExactlyOneRecordForTheTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
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

    Y_UNIT_TEST(MoveIndexRecordsAPath) {
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
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: TMoveIndex.SrcPath/DstPath are index names relative to TablePath, not
        // absolute paths, so TPath::Resolve used to fail and refuse the propose.
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
    }

    Y_UNIT_TEST(TruncateTableRecordsAPath) {
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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: TCreateContinuousBackup.TableName has no field literally named Name,
        // so the generic reflection walk used to miss it and refuse the propose.
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

        // Restore targets an existing table (it restores INTO it), so the target must
        // already exist for the propose to get past path resolution.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const ui64 rejectedBefore = GetCumulativeCounter(runtime, "SchemeShard/SchemeChangePathMissing");

        // Fixed: TRestoreTask names its target TableName. Full completion needs a real
        // backup fixture, so this checks propose-time resolution only.
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
        // TAlterLogin's target is nested inside a oneof (e.g. CreateUser.User), never
        // a top-level scalar Name/PathName/TableName field.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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

    // Exercises the Drop.Id branch of ResolveSchemeChangeTargets (Drop.Id set,
    // Drop.Name/WorkingDir unset), as produced by replication's dst_remover.
    Y_UNIT_TEST(DropByIdRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirToDropById");
        env.TestWaitNotification(runtime, txId);

        const auto pathVersion = ExtractPathVersion(DescribePath(runtime, "/MyRoot/DirToDropById"));
        const ui64 droppedOwnerId = pathVersion.PathId.OwnerId;
        const ui64 droppedLocalId = pathVersion.PathId.LocalPathId;

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
        // The object is gone by finalize time, so assert against the identity captured
        // before the drop rather than a live re-resolve.
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathOwnerId, droppedOwnerId,
            "recorded PathOwnerId must match the dropped object's owner");
        UNIT_ASSERT_VALUES_EQUAL_C(found->PathLocalId, droppedLocalId,
            "recorded PathLocalId must match the dropped object's identity, not some other path");
    }

    Y_UNIT_TEST(CreatePQGroupRecordsAPath) {
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

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys", R"(
            Name: "sys_view_1"
            Type: EPartitionStats
        )");
        env.TestWaitNotification(runtime, txId);

        auto entries = ReadSchemeChangeRecordsFromTable(runtime);
        bool found = false;
        for (const auto& e : entries) {
            if (e.Body.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateSysView
                        && AnyPathContains(e, "sys_view_1")) {
                found = true;
                AssertRecordedPathResolvesToTouchedObject(runtime, "/MyRoot", e);
            }
        }
        UNIT_ASSERT_C(found, "CreateSysView must produce a scheme change record");
    }

    Y_UNIT_TEST(CreateSecretRecordsAPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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

        // Alter with no actual partition/channel change: only that the outbox resolves
        // the target matters here, not the status the op itself returns.
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

    // The drop-family tests below capture the touched object's identity before the
    // drop and assert against it, since the path no longer resolves afterwards.
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
        // The target table is not itself dropped, but the pre-op-captured identity is
        // used for symmetry with the other Drop* tests in this suite.
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

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            IndexName: "Index1"
        )");
        env.TestWaitNotification(runtime, txId);

        // TestDropTableIndex issues ESchemeOpDropIndex as the top-level OperationType;
        // ESchemeOpDropTableIndex is a distinct, apparently-unissued enum value.
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

    // CreateIndexBuild is a top-level op whose target is InitiateIndexBuild.Table,
    // an absolute path; an unresolved refusal used to crash the tablet on abort.
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
        // Driving Backup to completion needs a real S3 endpoint, so this checks only
        // propose-time path resolution, not the post-finalize resolve.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

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
        // Upgrading the domain blocks a live read here, so this asserts propose-time
        // resolution via the rejection counter instead.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSchemeChangeRecords(true));
        ui64 txId = 100;

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
