#include "ut_scheme_change_records_helpers.h"

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeChangeRecordTestHelpers;

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsProtocolTests) {
    Y_UNIT_TEST(RegisterSubscriberCreatesEmptyCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> handle;
        auto result = RegisterSubscriber(runtime, "test:sub:1", handle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetCurrentOrder(), 0u);
    }

    Y_UNIT_TEST(RegisterSubscriberIdempotent) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> h1;
        auto r1 = RegisterSubscriber(runtime, "test:sub:1", h1);
        UNIT_ASSERT_VALUES_EQUAL((ui32)r1->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        TAutoPtr<IEventHandle> h2;
        auto r2 = RegisterSubscriber(runtime, "test:sub:1", h2);
        UNIT_ASSERT_VALUES_EQUAL((ui32)r2->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
    }

    Y_UNIT_TEST(FetchReturnsEntriesAfterCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

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

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 2);
    }

    Y_UNIT_TEST(FetchRespectsMaxCount) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 2, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 2);
        UNIT_ASSERT(fetch->Record.GetHasMore());
    }

    Y_UNIT_TEST(AckAdvancesCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

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

        TAutoPtr<IEventHandle> fetch1Handle;
        auto fetch1 = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetch1Handle);
        UNIT_ASSERT(fetch1->Record.EntriesSize() >= 2);

        ui64 firstOrder = fetch1->Record.GetEntries(0).GetOrder();
        TAutoPtr<IEventHandle> ackHandle;
        auto ack = AckSchemeChangeRecords(runtime, "test:sub:1", firstOrder, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)ack->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ack->Record.GetLastAckedOrder(), firstOrder);

        TAutoPtr<IEventHandle> fetch2Handle;
        auto fetch2 = FetchSchemeChangeRecords(runtime, "test:sub:1", firstOrder, 100, fetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch2->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);
        UNIT_ASSERT(fetch2->Record.EntriesSize() < fetch1->Record.EntriesSize());
    }

    Y_UNIT_TEST(FetchForUnknownSubscriberReturnsError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "nonexistent:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
    }

    Y_UNIT_TEST(MultipleSubscribersIndependentCursors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> reg1Handle, reg2Handle;
        RegisterSubscriber(runtime, "sub1", reg1Handle);
        RegisterSubscriber(runtime, "sub2", reg2Handle);

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

        TAutoPtr<IEventHandle> fetch1Handle, fetch2Handle;
        auto fetch1 = FetchSchemeChangeRecords(runtime, "sub1", 0, 100, fetch1Handle);
        auto fetch2 = FetchSchemeChangeRecords(runtime, "sub2", 0, 100, fetch2Handle);

        UNIT_ASSERT_VALUES_EQUAL(fetch1->Record.EntriesSize(), fetch2->Record.EntriesSize());

        ui64 lastOrder = fetch1->Record.EntriesSize() > 0
            ? fetch1->Record.GetEntries(fetch1->Record.EntriesSize() - 1).GetOrder()
            : 0;
        ui64 firstOrder = fetch1->Record.GetEntries(0).GetOrder();
        TAutoPtr<IEventHandle> ack1Handle, ack2Handle;
        AckSchemeChangeRecords(runtime, "sub1", lastOrder, ack1Handle);
        AckSchemeChangeRecords(runtime, "sub2", firstOrder, ack2Handle);

        TAutoPtr<IEventHandle> fetch1bHandle, fetch2bHandle;
        auto fetch1b = FetchSchemeChangeRecords(runtime, "sub1", lastOrder, 100, fetch1bHandle);
        auto fetch2b = FetchSchemeChangeRecords(runtime, "sub2", firstOrder, 100, fetch2bHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch1b->Record.EntriesSize(), 0);
        UNIT_ASSERT(fetch2b->Record.EntriesSize() > 0);
    }

    Y_UNIT_TEST(UnregisterSubscriberRemovesSubscriber) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

        TAutoPtr<IEventHandle> unregHandle;
        auto unregResult = UnregisterSubscriber(runtime, "test:sub:1", unregHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)unregResult->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_SUCCESS);

        // Fetch should fail -- subscriber no longer exists
        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)fetch->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
    }

    Y_UNIT_TEST(UnregisterLastSubscriberStopsRecordCreation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

        // Create T1 with subscriber -- record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetchHandle);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 1);

        // Unregister
        TAutoPtr<IEventHandle> unregHandle;
        UnregisterSubscriber(runtime, "test:sub:1", unregHandle);

        // Create T2 without subscriber -- no record expected
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "T2"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Re-register to check
        TAutoPtr<IEventHandle> reg2Handle;
        RegisterSubscriber(runtime, "test:sub:2", reg2Handle);

        TAutoPtr<IEventHandle> fetch2Handle;
        auto fetch2 = FetchSchemeChangeRecords(runtime, "test:sub:2", 0, 100, fetch2Handle);
        // Unregistering the last subscriber drops all retained records:
        // GetMinSubscriberOrder() collapses to NextSchemeChangeOrder when
        // Subscribers is empty, so the inline cleanup deletes everything.
        // A fresh subscriber starts with an empty log; NextSchemeChangeOrder
        // is preserved so future orders never collide with old ones.
        for (int i = 0; i < (int)fetch2->Record.EntriesSize(); ++i) {
            const auto& e = fetch2->Record.GetEntries(i);
            UNIT_ASSERT_C(e.GetPath() != "T1",
                "T1 record must be dropped when the last subscriber unregisters");
            UNIT_ASSERT_C(e.GetPath() != "T2",
                "T2 record must not exist (created after last subscriber unregistered)");
        }
    }

    Y_UNIT_TEST(UnregisterNonexistentSubscriberReturnsError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> unregHandle;
        auto result = UnregisterSubscriber(runtime, "nonexistent:sub", unregHandle);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result->Record.GetStatus(), (ui32)NKikimrSchemeShard::TSchemeChangeRecordsStatus::STATUS_NOT_REGISTERED);
    }
}
