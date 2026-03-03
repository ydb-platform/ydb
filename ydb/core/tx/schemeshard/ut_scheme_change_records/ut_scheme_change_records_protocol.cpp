#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

TEvSchemeShard::TEvRegisterSubscriberResult* RegisterSubscriber(
    TTestBasicRuntime& runtime, const TString& subscriberId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvRegisterSubscriber>();
    req->Record.SetSubscriberId(subscriberId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvRegisterSubscriberResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

TEvSchemeShard::TEvFetchSchemeChangeRecordsResult* FetchSchemeChangeRecords(
    TTestBasicRuntime& runtime, const TString& subscriberId, ui64 afterSeqId, ui32 maxCount,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvFetchSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetAfterSequenceId(afterSeqId);
    req->Record.SetMaxCount(maxCount);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFetchSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

TEvSchemeShard::TEvAckSchemeChangeRecordsResult* AckSchemeChangeRecords(
    TTestBasicRuntime& runtime, const TString& subscriberId, ui64 upToSeqId,
    TAutoPtr<IEventHandle>& handle)
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<TEvSchemeShard::TEvAckSchemeChangeRecords>();
    req->Record.SetSubscriberId(subscriberId);
    req->Record.SetUpToSequenceId(upToSeqId);
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, req.Release());
    auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvAckSchemeChangeRecordsResult>(handle);
    UNIT_ASSERT(result);
    return result;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeChangeRecordsProtocolTests) {
    Y_UNIT_TEST(RegisterSubscriberCreatesEmptyCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> handle;
        auto result = RegisterSubscriber(runtime, "test:sub:1", handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetCurrentSequenceId(), 0u);
    }

    Y_UNIT_TEST(RegisterSubscriberIdempotent) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> h1;
        auto r1 = RegisterSubscriber(runtime, "test:sub:1", h1);
        UNIT_ASSERT_VALUES_EQUAL(r1->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);

        TAutoPtr<IEventHandle> h2;
        auto r2 = RegisterSubscriber(runtime, "test:sub:1", h2);
        UNIT_ASSERT_VALUES_EQUAL(r2->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
    }

    Y_UNIT_TEST(FetchReturnsEntriesAfterCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);
        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(fetch->Record.EntriesSize() >= 2);
    }

    Y_UNIT_TEST(FetchRespectsMaxCount) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        for (int i = 1; i <= 5; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "T%d"
                Columns { Name: "key" Type: "Uint64" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);
        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 2, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.EntriesSize(), 2);
        UNIT_ASSERT(fetch->Record.GetHasMore());
    }

    Y_UNIT_TEST(AckAdvancesCursor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        TAutoPtr<IEventHandle> regHandle;
        RegisterSubscriber(runtime, "test:sub:1", regHandle);

        TAutoPtr<IEventHandle> fetch1Handle;
        auto fetch1 = FetchSchemeChangeRecords(runtime, "test:sub:1", 0, 100, fetch1Handle);
        UNIT_ASSERT(fetch1->Record.EntriesSize() >= 2);

        ui64 firstSeqId = fetch1->Record.GetEntries(0).GetSequenceId();
        TAutoPtr<IEventHandle> ackHandle;
        auto ack = AckSchemeChangeRecords(runtime, "test:sub:1", firstSeqId, ackHandle);
        UNIT_ASSERT_VALUES_EQUAL(ack->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(ack->Record.GetNewCursor(), firstSeqId);

        TAutoPtr<IEventHandle> fetch2Handle;
        auto fetch2 = FetchSchemeChangeRecords(runtime, "test:sub:1", firstSeqId, 100, fetch2Handle);
        UNIT_ASSERT_VALUES_EQUAL(fetch2->Record.GetStatus(), (ui32)NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(fetch2->Record.EntriesSize() < fetch1->Record.EntriesSize());
    }

    Y_UNIT_TEST(FetchForUnknownSubscriberReturnsError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TAutoPtr<IEventHandle> fetchHandle;
        auto fetch = FetchSchemeChangeRecords(runtime, "nonexistent:sub", 0, 100, fetchHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Record.GetStatus(), (ui32)NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(MultipleSubscribersIndependentCursors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        TAutoPtr<IEventHandle> reg1Handle, reg2Handle;
        RegisterSubscriber(runtime, "sub1", reg1Handle);
        RegisterSubscriber(runtime, "sub2", reg2Handle);

        TAutoPtr<IEventHandle> fetch1Handle, fetch2Handle;
        auto fetch1 = FetchSchemeChangeRecords(runtime, "sub1", 0, 100, fetch1Handle);
        auto fetch2 = FetchSchemeChangeRecords(runtime, "sub2", 0, 100, fetch2Handle);

        UNIT_ASSERT_VALUES_EQUAL(fetch1->Record.EntriesSize(), fetch2->Record.EntriesSize());

        ui64 lastSeq = fetch1->Record.GetLastSequenceId();
        ui64 firstSeq = fetch1->Record.GetEntries(0).GetSequenceId();
        TAutoPtr<IEventHandle> ack1Handle, ack2Handle;
        AckSchemeChangeRecords(runtime, "sub1", lastSeq, ack1Handle);
        AckSchemeChangeRecords(runtime, "sub2", firstSeq, ack2Handle);

        TAutoPtr<IEventHandle> fetch1bHandle, fetch2bHandle;
        auto fetch1b = FetchSchemeChangeRecords(runtime, "sub1", lastSeq, 100, fetch1bHandle);
        auto fetch2b = FetchSchemeChangeRecords(runtime, "sub2", firstSeq, 100, fetch2bHandle);
        UNIT_ASSERT_VALUES_EQUAL(fetch1b->Record.EntriesSize(), 0);
        UNIT_ASSERT(fetch2b->Record.EntriesSize() > 0);
    }
}
