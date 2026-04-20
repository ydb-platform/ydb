#include "ut_scheme_change_records_helpers.h"

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;
using NSchemeChangeRecordTestHelpers::ReadSchemeChangeRecords;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsSubscriberTests) {
    Y_UNIT_TEST(MockBackupSubscriberEndToEnd) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backup:collection:1", regHandle);

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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "backup:collection:1", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);

        // Should have at least 4 entries (CREATE, ALTER, CREATE, DROP)
        UNIT_ASSERT_C(fetch->Record.EntriesSize() >= 4,
            "Expected >= 4 entries, got " << fetch->Record.EntriesSize());

        // Verify sequence is monotonic
        for (int i = 1; i < (int)fetch->Record.EntriesSize(); ++i) {
            UNIT_ASSERT(fetch->Record.GetEntries(i).GetSequenceId() >
                        fetch->Record.GetEntries(i-1).GetSequenceId());
        }

        // ACK all
        ui64 lastSeq = fetch->Record.GetLastSequenceId();
        TAutoPtr<IEventHandle> ackHandle;
        auto ack = AckSchemeChangeRecords(runtime, "backup:collection:1", lastSeq, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL(ack->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);

        // Fetch again - should be empty
        TAutoPtr<IEventHandle> fetch2Handle;
        auto fetch2 = FetchSchemeChangeRecords(runtime, "backup:collection:1", lastSeq, 100, fetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL(fetch2->Record.EntriesSize(), 0);
        UNIT_ASSERT(!fetch2->Record.GetHasMore());
    }

    Y_UNIT_TEST(MockBackupSubscriberPaginatedFetch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "backup:sub", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        ui64 cursor = 0;
        ui32 totalFetched = 0;
        while (true) {
            TAutoPtr<IEventHandle> fetchHandle;
            auto fetch = FetchSchemeChangeRecords(runtime, "backup:sub", cursor, 2, fetchHandle);
            UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
            if (fetch->Record.EntriesSize() == 0) {
                break;
            }
            totalFetched += fetch->Record.EntriesSize();
            cursor = fetch->Record.GetLastSequenceId();
            TAutoPtr<IEventHandle> ackHandle;
            AckSchemeChangeRecords(runtime, "backup:sub", cursor, ackHandle);
            if (!fetch->Record.GetHasMore()) {
                break;
            }
        }

        UNIT_ASSERT(totalFetched >= 5);
    }

    Y_UNIT_TEST(TwoSubscribersIndependentConsumption) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> reg1Handle, reg2Handle;
        RegisterSubscriber(runtime, "backup:collection:1", reg1Handle);
        RegisterSubscriber(runtime, "audit:system", reg2Handle);

        for (int i = 1; i <= 3; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        // backup fetches and acks all
        TAutoPtr<IEventHandle> backupFetchHandle;
        auto backupFetch = FetchSchemeChangeRecords(runtime, "backup:collection:1", 0, 100, backupFetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(backupFetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(backupFetch->Record.EntriesSize() >= 3);
        ui64 backupLastSeq = backupFetch->Record.GetLastSequenceId();
        TAutoPtr<IEventHandle> backupAckHandle;
        AckSchemeChangeRecords(runtime, "backup:collection:1", backupLastSeq, backupAckHandle);

        // audit fetches but only acks first entry
        TAutoPtr<IEventHandle> auditFetchHandle;
        auto auditFetch = FetchSchemeChangeRecords(runtime, "audit:system", 0, 100, auditFetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(auditFetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(auditFetch->Record.EntriesSize() >= 3);
        ui64 auditFirstSeq = auditFetch->Record.GetEntries(0).GetSequenceId();
        TAutoPtr<IEventHandle> auditAckHandle;
        AckSchemeChangeRecords(runtime, "audit:system", auditFirstSeq, auditAckHandle);

        // backup should have nothing new
        TAutoPtr<IEventHandle> backupFetch2Handle;
        auto backupFetch2 = FetchSchemeChangeRecords(runtime, "backup:collection:1", backupLastSeq, 100, backupFetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL(backupFetch2->Record.EntriesSize(), 0);

        // audit should still have remaining entries
        TAutoPtr<IEventHandle> auditFetch2Handle;
        auto auditFetch2 = FetchSchemeChangeRecords(runtime, "audit:system", auditFirstSeq, 100, auditFetch2Handle);
        UNIT_ASSERT(auditFetch2->Record.EntriesSize() > 0);
    }

    Y_UNIT_TEST(ForceAdvanceSubscriber) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "stuck:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> advHandle;
        auto result = ForceAdvanceSubscriber(runtime, "stuck:sub", advHandle);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(result->Record.GetNewCursor() > 0);

        // Fetch should return empty (cursor is at tail)
        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "stuck:sub", result->Record.GetNewCursor(), 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 0);
    }

    Y_UNIT_TEST(ForceAdvanceUnknownSubscriberReturnsError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> advHandle;
        auto result = ForceAdvanceSubscriber(runtime, "nonexistent:sub", advHandle);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), (ui32)NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(FetchReturnsMetadataWithoutBody) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "meta:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "meta:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 1);
        for (int i = 0; i < (int)fetch->Record.EntriesSize(); ++i) {
            const auto& entry = fetch->Record.GetEntries(i);
            UNIT_ASSERT_C(entry.GetBodySize() > 0,
                "Metadata should include non-zero BodySize for entry " << entry.GetSequenceId());
        }
    }

    Y_UNIT_TEST(FetchBodiesReturnsRequestedBodies) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "body:sub", regHandle);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "body:sub", 0, 100, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 1);

        TVector<ui64> orders;
        for (int i = 0; i < (int)fetch->Record.EntriesSize(); ++i) {
            orders.push_back(fetch->Record.GetEntries(i).GetSequenceId());
        }

        TAutoPtr<IEventHandle> bodiesHandle;
        auto bodies = FetchSchemeChangeRecordBodies(runtime, "body:sub", orders, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.EntriesSize(), orders.size());
        for (int i = 0; i < (int)bodies->Record.EntriesSize(); ++i) {
            UNIT_ASSERT(!bodies->Record.GetEntries(i).GetBody().empty());
        }
    }

    Y_UNIT_TEST(FetchBodiesUnregisteredSubscriberRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TVector<ui64> orders = {1, 2, 3};
        TAutoPtr<IEventHandle> bodiesHandle;
        auto bodies = FetchSchemeChangeRecordBodies(runtime, "ghost:sub", orders, bodiesHandle);
        UNIT_ASSERT_VALUES_EQUAL(bodies->Record.GetStatus(), (ui32)NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(AckDeletesAckedRecordsInline) {
        // Phase 3 invariant: Ack tx deletes records (up to ReactiveCleanupCap)
        // in the same transaction. No manual wakeup / background sweep needed.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "cleanup:sub", regHandle);

        for (int i = 1; i <= 3; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "cleanup:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 3);
        ui64 lastSeq = fetch->Record.GetLastSequenceId();

        TAutoPtr<IEventHandle> ackHandle;
        AckSchemeChangeRecords(runtime, "cleanup:sub", lastSeq, ackHandle);

        // No manual TEvWakeupToRunSchemeChangeRecordsCleanup forwarded:
        // records must be gone from the table as a side effect of the ack.
        auto entries = ReadSchemeChangeRecords(runtime);
        UNIT_ASSERT_C(entries.empty(),
            "Records should be deleted inline by ack, got " << entries.size());
    }
}
