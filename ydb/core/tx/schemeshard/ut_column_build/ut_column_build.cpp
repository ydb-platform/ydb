#include <ydb/core/metering/metering.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/public/lib/value/value.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

class TColumnBuildFixture {
public:
    TColumnBuildFixture()
        : Env(Runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true))
    {
        TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 100 } } } }
        )");
        Env.TestWaitNotification(Runtime, TxId);
    }

    auto CreateBuild(const TString& name, const Ydb::TypedValue& literal, bool notNull) {
        auto request = CreateBuildColumnRequest(++TxId, "/MyRoot", "/MyRoot/Table", name, literal);
        request->Record.MutableSettings()->mutable_column_build_operation()->mutable_column(0)->SetNotNull(notNull);
        const auto sender = Runtime.AllocateEdgeActor();
        ForwardToTablet(Runtime, TTestTxConfig::SchemeShard, sender, request.release());
        const auto response = Runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
        return response->Get()->Record;
    }

    auto FinishBuild(ui64 id) {
        Env.TestWaitNotification(Runtime, id);
        return TestGetBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot", id);
    }

    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
};

}

Y_UNIT_TEST_SUITE(ColumnBuildTest) {
    Y_UNIT_TEST_FLAG(NullDefaultNonOptionalDirectRequest, Populated) {
        TColumnBuildFixture fixture;
        auto& runtime = fixture.Runtime;
        if (Populated) {
            WriteRow(runtime, ++fixture.TxId, "/MyRoot/Table", 1, 101, "existing");
        }
        const auto before = DescribePath(runtime, "/MyRoot/Table");
        Ydb::TypedValue literal;
        literal.mutable_type()->set_type_id(Ydb::Type::UINT32);
        literal.mutable_value()->set_null_flag_value(NProtoBuf::NULL_VALUE);
        const auto rejected = fixture.CreateBuild("extra", literal, false);
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), Ydb::StatusIds::BAD_REQUEST, rejected.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(rejected.DebugString(), "NULL");
        UNIT_ASSERT_STRING_CONTAINS(rejected.DebugString(), "extra");
        const auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
            GetBuildIndexRequest("/MyRoot", fixture.TxId));
        const auto build = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvGetResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(build->Get()->Record.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED,
            build->Get()->Record.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(build->Get()->Record.DebugString(), "not found");
        const auto after = DescribePath(runtime, "/MyRoot/Table");
        TestDescribeResult(after, {NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
        UNIT_ASSERT_VALUES_EQUAL(after.GetPathDescription().GetTable().GetTableSchemaVersion(),
            before.GetPathDescription().GetTable().GetTableSchemaVersion());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table"), Populated ? 1 : 0);
        if (Populated) {
            const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
            const auto data = ReadTable(runtime, shards[1], "Table", {"key"}, {"key", "value"});
            const auto rows = NClient::TValue::Create(data)["Result"]["List"];
            UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(rows[0]["key"]), 101);
            UNIT_ASSERT_VALUES_EQUAL(TString(rows[0]["value"]), "existing");
        }
        literal.mutable_value()->set_uint32_value(42);
        const auto created = fixture.CreateBuild("extra", literal, false);
        UNIT_ASSERT_VALUES_EQUAL_C(created.GetStatus(), Ydb::StatusIds::SUCCESS, created.DebugString());
        const auto finished = fixture.FinishBuild(fixture.TxId);
        UNIT_ASSERT_VALUES_EQUAL_C(finished.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            finished.DebugString());
        const auto description = DescribePath(runtime, "/MyRoot/Table");
        TestDescribeResult(description, {NLs::CheckColumns("Table", {"key", "value", "extra"}, {}, {"key"}, true)});
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.DebugString());
            if (column.GetName() == "extra") {
                UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().uint32_value(), 42);
                UNIT_ASSERT_C(!column.GetNotNull(), column.DebugString());
            }
        }
    }

    Y_UNIT_TEST_FLAG(NullDefaultNotNullDirectRequest, Populated) {
        TColumnBuildFixture fixture;
        auto& runtime = fixture.Runtime;
        if (Populated) {
            WriteRow(runtime, ++fixture.TxId, "/MyRoot/Table", 1, 101, "existing");
        }
        const auto schemaVersion = DescribePath(runtime, "/MyRoot/Table")
            .GetPathDescription().GetTable().GetTableSchemaVersion();

        for (const bool optional : {false, true}) {
            Ydb::TypedValue literal;
            if (optional) {
                literal.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
            } else {
                literal.mutable_type()->set_type_id(Ydb::Type::UINT32);
            }
            literal.mutable_value()->set_null_flag_value(NProtoBuf::NULL_VALUE);
            const auto result = fixture.CreateBuild("required", literal, true);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), Ydb::StatusIds::BAD_REQUEST, result.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "not null");
            UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "required");

            const auto sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender,
                GetBuildIndexRequest("/MyRoot", fixture.TxId));
            const auto build = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvGetResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(build->Get()->Record.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED,
                build->Get()->Record.DebugString());
            UNIT_ASSERT_STRING_CONTAINS(build->Get()->Record.DebugString(), "not found");
            const auto description = DescribePath(runtime, "/MyRoot/Table");
            TestDescribeResult(description, {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetTable().GetTableSchemaVersion(), schemaVersion);
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table"), Populated ? 1 : 0);
        }

        WriteRow(runtime, ++fixture.TxId, "/MyRoot/Table", 1, 102, "after rejection");
        Ydb::TypedValue literal;
        literal.mutable_type()->set_type_id(Ydb::Type::UINT32);
        literal.mutable_value()->set_uint32_value(42);
        const auto create = fixture.CreateBuild("required", literal, true);
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), Ydb::StatusIds::SUCCESS, create.DebugString());
        const auto result = fixture.FinishBuild(fixture.TxId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            result.DebugString());
        TestCheckColumnsNotNull(runtime, "/MyRoot/Table", {{"key", false}, {"value", false}, {"required", true}});
        const auto description = DescribePath(runtime, "/MyRoot/Table");
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.DebugString());
        }

        TVector<TCell> cells{TCell::Make(ui32(1)), TCell(TStringBuf("explicit")), TCell::Make(ui32(42))};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++fixture.TxId, "/MyRoot/Table", 0,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, {1, 2, 3}, TSerializedCellMatrix(cells, 1, 3), true);
        cells = {TCell::Make(ui32(2)), TCell(TStringBuf("invalid")), TCell()};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++fixture.TxId, "/MyRoot/Table", 0,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, {1, 2, 3}, TSerializedCellMatrix(cells, 1, 3), false);
        cells = {TCell::Make(ui32(3)), TCell(TStringBuf("omitted"))};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++fixture.TxId, "/MyRoot/Table", 0,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, {1, 2}, TSerializedCellMatrix(cells, 1, 2), false);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table"), Populated ? 3 : 2);
    }

    Y_UNIT_TEST_FLAG(NullDefaultBackfillRecovery, RebootSchemeShard) {
        TColumnBuildFixture fixture;
        auto& runtime = fixture.Runtime;
        WriteRow(runtime, ++fixture.TxId, "/MyRoot/Table", 0, 1, "first");
        WriteRow(runtime, ++fixture.TxId, "/MyRoot/Table", 1, 101, "second");
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2);
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(runtime);
        Ydb::TypedValue literal;
        literal.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        literal.mutable_value()->set_null_flag_value(NProtoBuf::NULL_VALUE);
        const auto created = fixture.CreateBuild("extra", literal, false);
        UNIT_ASSERT_VALUES_EQUAL_C(created.GetStatus(), Ydb::StatusIds::SUCCESS, created.DebugString());
        const ui64 buildId = fixture.TxId;
        runtime.WaitFor("column backfill upload", [&] { return !uploads.empty(); });
        if (RebootSchemeShard) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        } else {
            for (const auto shard : shards) {
                RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
            }
        }
        uploads.Stop().Unblock();
        const auto finished = fixture.FinishBuild(buildId);
        UNIT_ASSERT_VALUES_EQUAL_C(finished.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            finished.DebugString());
        const auto description = DescribePath(runtime, "/MyRoot/Table");
        TestDescribeResult(description, {NLs::CheckColumns("Table", {"key", "value", "extra"}, {}, {"key"}, true)});
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.DebugString());
            if (column.GetName() == "extra") {
                UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
                UNIT_ASSERT_C(column.GetDefaultFromLiteral().value().has_null_flag_value(), column.DebugString());
                UNIT_ASSERT_C(!column.GetNotNull(), column.DebugString());
            }
        }
        for (size_t i = 0; i < shards.size(); ++i) {
            const auto data = ReadTable(runtime, shards[i], "Table", {"key"}, {"key", "value", "extra"});
            const auto rows = NClient::TValue::Create(data)["Result"]["List"];
            UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(rows[0]["key"]), i ? 101 : 1);
            UNIT_ASSERT_VALUES_EQUAL(TString(rows[0]["value"]), i ? "second" : "first");
            UNIT_ASSERT_C(rows[0]["extra"].IsNull(), data.DebugString());
        }
        TVector<TCell> cells{TCell::Make(ui32(2)), TCell(TStringBuf("after reboot")), TCell()};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++fixture.TxId, "/MyRoot/Table", 0,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, {1, 2, 3}, TSerializedCellMatrix(cells, 1, 3), true);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table"), 3);
    }

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
