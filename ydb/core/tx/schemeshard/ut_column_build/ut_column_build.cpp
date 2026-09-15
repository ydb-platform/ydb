#include <ydb/core/metering/metering.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(ColumnBuildTest) {
    Y_UNIT_TEST(ValidDefaultValue) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(1111);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::SUCCESS);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_C(listing.EntriesSize() == 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_C(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(DoNotRestoreDeletedRows) {
        TTestBasicRuntime runtime(1, false);
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint64" }
              Columns { Name: "index"   Type: "Uint64" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint64 '%u ) ) ) )
                    (let row   '( '('index (Uint64 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        auto fnDeleteRow = [&] (ui64 tabletId, ui32 key, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint64 '%u ) ) ) )
                    (return (AsList (EraseRow '__user__%s key ) ))
                )
            )", key, table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool enabledCapture = true;
        TVector<TAutoPtr<IEventHandle>> delayedUpsertRows;
        auto grab = [&delayedUpsertRows, &enabledCapture](TAutoPtr<IEventHandle>& ev) -> auto {
            if (enabledCapture && ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvUploadRowsRequest::EventType) {
                delayedUpsertRows.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedUpsertRows](IEventHandle&) {
            return delayedUpsertRows.size() > 0;
        });

        runtime.SetObserverFunc(grab);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.DispatchEvents(opts);
        UNIT_ASSERT_C(delayedUpsertRows.size() > 0, "not captured several events");

        for (ui32 delta = 0; delta < 50; ++delta) {
            fnDeleteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, "Table");
        }

        for (const auto& ev: delayedUpsertRows) {
            runtime.Send(ev);
        }

        enabledCapture = false;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});

        for (ui32 delta = 50; delta < 101; ++delta) {
            UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::FakeHiveTablets + 6, "__user__Table", "key", 1 + delta));
        }

        for (ui32 delta = 0; delta < 50; ++delta) {
            UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::FakeHiveTablets + 6, "__user__Table", "key", 1 + delta));
        }
    }

    Y_UNIT_TEST(BaseCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TStringBuilder meteringMessages;
        auto grabMeteringMessage = [&meteringMessages](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->Type == NMetering::TEvMetering::TEvWriteMeteringJson::EventType) {
                auto *msg = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>();
                meteringMessages << msg->MeteringJson;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(grabMeteringMessage);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::SUCCESS);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {}, {"key"}, true)});
/*
        const TString meteringData = R"({"usage":{"start":0,"quantity":179,"finish":0,"unit":"request_unit","type":"delta"},"tags":{},"id":"106-72075186233409549-2-101-1818-101-1818","cloud_id":"CLOUD_ID_VAL","source_wt":0,"source_id":"sless-docapi-ydb-ss","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.requests.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";

        UNIT_ASSERT_NO_DIFF(meteringMessages, meteringData + "\n");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table/index1", true, true, true),
                           {NLs::PathExist,
                            NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});

        TestForgetBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        Y_ASSERT(listing.EntriesSize() == 0);
*/
    }

    Y_UNIT_TEST(Cancelling) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        ui64 buildIndexId = txId;

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        TestCancelBuildIndex(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());

        blocker.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Rejecting) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, value.c_str(), table);
            NKikimrMiniKQL::TResult result;

            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, "abcd", "Table");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"})});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        // Set invalid default value to get rejected
        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::JSON);
        columnDefaultValue.mutable_value()->set_text_value("{not json]");

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        blocker.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Locking_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexTx = ++txId;
        AsyncBuildColumn(runtime, buildIndexTx, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Drop the table BEFORE locking stage
        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", "Table");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathNotExist});

        blocker.Stop().Unblock();

        {
            TAutoPtr<IEventHandle> handle;
            TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL_C(event->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST, event->Record.DebugString());
        }
    }

    Y_UNIT_TEST(Locking_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state because Initiating not started
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to skip DroppingColumns state because AlterMainTable failed
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Lock operation does not know about DEFAULT columns, so reject will be from AlterMainTable
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(AlterMainTable_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        // Try to add column that already exists
        const TString columnName = "value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto listing = TestListBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB");
        UNIT_ASSERT_VALUES_EQUAL_C(listing.EntriesSize(), 1, listing.DebugString());

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(AlterMainTable_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction(0);
            return tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating not started
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to skip DroppingColumns state because AlterMainTable failed
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Initiating_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnBuild) {
                auto& op = *modifyScheme.MutableInitiateColumnBuild();
                // set invalid table name to fail the operation
                op.SetTable("");
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(5),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Initiating_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateColumnBuild;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to skip Rejection_Applying state if Initiating failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(5),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Filling_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime, [&](auto& ev) {
            auto& record = ev->Get()->Record;
            record.SetTargetName("");
            return false;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> applyingBlocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();
        applyingBlocker.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was no Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Filling_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime, [&](auto&) {
            return true;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        // We have to execute Applying (by Rejection_Applying) because Initiating was successful
        UNIT_ASSERT_C(rejectionApplying, "There was no Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(descr.DebugString(), "Adding columns with defaults is disabled");
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Applying_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                auto& op = *modifyScheme.MutableApplyIndexBuild();
                // set invalid snapshot tx id to fail the operation
                op.SetSnapshotTxId(ui64(InvalidTxId));
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state if Applying failed
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Applying_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Applying operation cannot be rejected because it have to finalize the index building
            // If it failed, Rejection_Applying will be skipped and the table will not be available
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        };

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Unlocking_Failed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // First drop lock belongs to Unlocking stage, second to Rejection_Unlocking
        bool firstLock = true;
        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (firstLock && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock) {
                auto& op = *modifyScheme.MutableLockConfig();
                // set invalid name to fail the operation
                op.SetName("");
                firstLock = false;
            } else if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, ++txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        blocker.Stop().Unblock();

        // We have to skip Rejection_Applying state because Applying was successful
        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        // We have to execute DroppingColumns (by Rejection_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(rejectionDroppingColumns, "There was no Rejection_DroppingColumns state");

        auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
        UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, descr.DebugString());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Unlocking_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropLock;
        });

        bool rejectionApplying = false;
        bool rejectionDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rejectionDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                rejectionDroppingColumns = true;
            } else if (rejectionDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild) {
                rejectionApplying = true;
            }
            return false;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        AsyncBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        rejectionDetector.Stop().Unblock();

        UNIT_ASSERT_C(!rejectionApplying, "There was Rejection_Applying state");
        UNIT_ASSERT_C(!rejectionDroppingColumns, "There was Rejection_DroppingColumns state");

        {
            // Unlock operation does not know about DEFAULT columns, so no rejection will occur
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }

    Y_UNIT_TEST(Cancellation_DroppingColumns_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> cancellationDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexId = ++txId;
        AsyncBuildColumn(runtime, buildIndexId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto request = CreateCancelBuildIndexRequest(++txId, "/MyRoot/ServerLessDB", buildIndexId);
        ForwardToTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor(), request);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);
        cancellationDetector.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was no Cancellation_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        {
            // If operation is cancelled, it does not matter if the flag is disabled or enabled
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(Cancellation_Applying_DisableFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "index", "value"}, {}, {"key"}, true)});

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        bool cancellationApplying = false;
        bool cancellationDroppingColumns = false;

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> cancellationDetector(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            if (modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild) {
                cancellationDroppingColumns = true;
            } else if (cancellationDroppingColumns && modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild) {
                cancellationApplying = true;
            }
            return false;
        });

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> blocker(runtime, [&](auto& ev) {
            auto& modifyScheme = *ev->Get()->Record.MutableTransaction(0);
            return modifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCancelIndexBuild;
        });

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        ui64 buildIndexId = ++txId;
        AsyncBuildColumn(runtime, buildIndexId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);

        auto request = CreateCancelBuildIndexRequest(++txId, "/MyRoot/ServerLessDB", buildIndexId);
        ForwardToTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor(), request);

        runtime.WaitFor("block", [&]{ return blocker.size(); });

        // Disable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(false);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildIndexId, tenantSchemeShard);
        cancellationDetector.Stop().Unblock();

        // We have to execute Applying (by Cancellation_Applying) because Initiating was successful
        UNIT_ASSERT_C(cancellationApplying, "There was no Cancellation_Applying state");
        // We have to execute DroppingColumns (by Cancellation_DroppingColumns) because AlterMainTable was successful
        UNIT_ASSERT_C(cancellationDroppingColumns, "There was no Cancellation_DroppingColumns state");

        {
            // If operation is cancelled, it does not matter if the flag is disabled or enabled
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", buildIndexId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(7),
                            NLs::CheckColumns("Table", {"key", "index", "value", columnName}, {columnName}, {"key"}, true)});
    }

    Y_UNIT_TEST(DisabledAndEnabledFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(false));

        ui64 txId = 100;
        ui64 tenantSchemeShard = 0;

        TestCreateServerLessDb(runtime, env, txId, tenantSchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
            Name: "Table"
            Columns { Name: "key"     Type: "Uint32" }
            Columns { Name: "value"   Type: "Utf8"   }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        const TString columnName = "default_value";
        Ydb::TypedValue columnDefaultValue;
        columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT64);
        columnDefaultValue.mutable_value()->set_uint64_value(10);

        TestBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});

        // Enable the flag
        runtime.GetAppData().FeatureFlags.SetEnableAddColumsWithDefaults(true);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestBuildColumn(runtime, txId, tenantSchemeShard, "/MyRoot/ServerLessDB", "/MyRoot/ServerLessDB/Table", columnName, columnDefaultValue);
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        {
            auto descr = TestGetBuildIndex(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, descr.DebugString());
        }

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(6),
                            NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
    }
}
