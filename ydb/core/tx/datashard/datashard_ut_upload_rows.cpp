#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

using TRows = TVector<std::pair<TSerializedCellVec, TString>>;
using TRowTypes = TVector<std::pair<TString, Ydb::Type>>;

static void DoStartUploadTestRows(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        const TString& database,
        const TString& tableName,
        Ydb::Type::PrimitiveTypeId typeId,
        TBackoff backoff = TBackoff(0))
{
    auto& runtime = *server->GetRuntime();

    std::shared_ptr<TRows> rows(new TRows);
    auto types = std::make_shared<TRowTypes>();
    Ydb::Type type;
    type.set_type_id(typeId);
    types->emplace_back("key", type);
    types->emplace_back("value", type);
    for (ui32 i = 0; i < 32; i++) {
        auto key = TVector<TCell>{TCell::Make(1 << i)};
        auto value = TVector<TCell>{TCell::Make(i)};
        TSerializedCellVec serializedKey(key);
        TString serializedValue = TSerializedCellVec::Serialize(value);
        rows->emplace_back(serializedKey, serializedValue);
    }

    auto actor = NTxProxy::CreateUploadRowsInternal(sender, database, tableName, types, rows, NTxProxy::EUploadRowsMode::Normal, false, false, false, 0, backoff);
    runtime.Register(actor);
}

static void DoWaitUploadTestRows(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        Ydb::StatusIds::StatusCode expected)
{
    auto& runtime = *server->GetRuntime();

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, expected);
}

static void DoUploadTestRows(Tests::TServer::TPtr server, const TActorId& sender,
                             const TString& database, const TString& tableName,
                             Ydb::Type::PrimitiveTypeId typeId, Ydb::StatusIds::StatusCode expected)
{
    DoStartUploadTestRows(server, sender, database, tableName, typeId);
    DoWaitUploadTestRows(server, sender, expected);
}

static TActorId DoStartUploadRows(
        TTestActorRuntime& runtime,
        const TString& database,
        const TString& tableName,
        const std::vector<std::pair<ui32, ui32>>& data,
        NTxProxy::EUploadRowsMode mode = NTxProxy::EUploadRowsMode::Normal)
{
    auto sender = runtime.AllocateEdgeActor();

    auto types = std::make_shared<TRowTypes>();
    Ydb::Type type;
    type.set_type_id(Ydb::Type::UINT32);
    types->emplace_back("key", type);
    types->emplace_back("value", type);

    auto rows = std::make_shared<TRows>();
    for (const auto& kv : data) {
        auto key = TVector<TCell>{TCell::Make(kv.first)};
        auto value = TVector<TCell>{TCell::Make(kv.second)};
        TSerializedCellVec serializedKey(key);
        TString serializedValue = TSerializedCellVec::Serialize(value);
        rows->emplace_back(serializedKey, serializedValue);
    }

    auto actor = NTxProxy::CreateUploadRowsInternal(
            sender,
            database,
            tableName,
            std::move(types),
            std::move(rows),
            mode);
    runtime.Register(actor);

    return sender;
}

static void DoWaitUploadRows(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
{
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, expected);
}

static void DoUploadRows(
        TTestActorRuntime& runtime,
        const TString& database,
        const TString& tableName,
        const std::vector<std::pair<ui32, ui32>>& data,
        NTxProxy::EUploadRowsMode mode = NTxProxy::EUploadRowsMode::Normal,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
{
    auto sender = DoStartUploadRows(runtime, database, tableName, data, mode);
    DoWaitUploadRows(runtime, sender, expected);
}

// Install the internal hidden schema independently of the SQL ALTER compiler.
// SchemeShard sees an ordinary nullable column, allowing explicit test values.
static void GeneratedFillTest(bool nullResult, bool compactAndReboot, bool concurrent = false) {
    TPortManager pm;
    TServerSettings settings(pm.GetPort(2134));
    settings.SetDomainName("Root").SetUseRealThreads(false);
    if (compactAndReboot) {
        settings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(0);
    }
    TServer::TPtr server = new TServer(settings);
    auto& runtime = *server->GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);
    auto tableOptions = TShardedTableOptions().Shards(1);
    if (compactAndReboot) {
        tableOptions.ExecutorCacheSize(1);
    }
    CreateShardedTable(server, sender, "/Root", "table-1", tableOptions);
    ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40);");
    const auto beforeAlter = ResolveTableId(server, sender, "/Root/table-1");
    auto schemaObserver = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& event) {
        auto& record = event->Get()->Record;
        if (record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
            return;
        }
        NKikimrTxDataShard::TFlatSchemeTransaction transaction;
        UNIT_ASSERT(transaction.ParseFromString(record.GetTxBody()));
        if (!transaction.HasAlterTable()) {
            return;
        }
        for (auto& column : *transaction.MutableAlterTable()->MutableColumns()) {
            if (column.GetName() == "g") {
                column.SetIsBuildInProgress(true);
                auto* expression = column.MutableDefaultFromExpression();
                expression->SetStored(true);
                expression->SetExprText("42");
            }
        }
        record.SetTxBody(transaction.SerializeAsString());
    });
    ExecSQL(server, sender, "ALTER TABLE `/Root/table-1` ADD COLUMN g Int64;", false);
    schemaObserver.Remove();
    ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, g) VALUES (2, NULL), (3, 99); DELETE FROM `/Root/table-1` WHERE key = 4;");
    const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
    const ui64 shard = GetTableShards(server, sender, "/Root/table-1").at(0);
    const auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"});
    auto request = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
    auto& record = request->Record;
    record.SetId(12345);
    record.SetTabletId(shard);
    record.SetOwnerId(tableId.PathId.OwnerId);
    record.SetPathId(tableId.PathId.LocalPathId);
    record.SetDatabaseName("/Root");
    record.SetTargetName("/Root/table-1");
    record.SetSeqNoGeneration(1);
    record.SetSeqNoRound(1);
    record.SetSnapshotStep(snapshot.Step);
    record.SetSnapshotTxId(snapshot.TxId);
    record.MutableScanSettings()->SetMaxBatchRows(1);
    auto* column = record.MutableColumnBuildSettings()->add_column();
    column->SetColumnName("g");
    auto* descriptor = column->mutable_generated_from_expression();
    descriptor->SetExprText("42");
    descriptor->SetSourceOwnerId(tableId.PathId.OwnerId);
    descriptor->SetSourcePathId(tableId.PathId.LocalPathId);
    descriptor->SetSourceSchemaVersion(beforeAlter.SchemaVersion);
    descriptor->MutableOutputType()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::INT64);
    {
        using namespace NMiniKQL;
        const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment environment(alloc);
        TProgramBuilder builder(environment, *registry);
        const auto value = nullResult ? builder.NewEmptyOptionalDataLiteral(NUdf::TDataType<i64>::Id)
            : builder.NewOptional(builder.NewDataLiteral<i64>(42));
        descriptor->SetProgram(SerializeRuntimeNode(value, environment));
        descriptor->SetProgramFormatVersion(1);
        descriptor->SetMiniKqlRuntimeVersion(RuntimeVersion.Value());
        descriptor->SetLanguageVersion(NYql::MinLangVersion);
        descriptor->SetRuntimeSettings(NYql::SerializeRuntimeSettingsToString(*NYql::MakeRuntimeSettings()));
    }
    NKikimrTxDataShard::TEvUploadRowsRequest committedRequest;
    THolder<IEventHandle> lostAck;
    THolder<IEventHandle> pendingUpload;
    TString foregroundState;
    auto uploadObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto& event) {
        if (event->Get()->Record.HasGeneratedColumnFillContext()) {
            committedRequest = event->Get()->Record;
            if (concurrent && !pendingUpload) {
                pendingUpload.Reset(event.Release());
            }
        }
    });
    auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& event) {
        if (committedRequest.HasGeneratedColumnFillContext() && !lostAck) {
            lostAck.Reset(event.Release());
        }
    });
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
    if (concurrent) {
        runtime.WaitFor("generated fill upload paused", [&] { return bool(pendingUpload); });
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, g) VALUES (1, NULL); DELETE FROM `/Root/table-1` WHERE key = 3; UPSERT INTO `/Root/table-1` (key, value, g) VALUES (3, 300, 123);");
        foregroundState = ReadShardedTable(server, "/Root/table-1");
        uploadObserver.Remove();
        runtime.Send(pendingUpload.Release(), 0, true);
    }
    runtime.WaitFor("generated fill committed without acknowledgement", [&] { return bool(lostAck); });
    responseObserver.Remove();
    if (!concurrent) {
        uploadObserver.Remove();
    }
    const TString stored = ReadShardedTable(server, "/Root/table-1");
    if (concurrent) {
        UNIT_ASSERT_VALUES_EQUAL(stored, foregroundState);
    } else {
        // This read helper formats nullable cells as an empty Maybe. The
        // non-NULL duplicate below proves the NULL was physically materialized:
        // an unwritten target would accept 777 after compaction and reboot.
        UNIT_ASSERT_STRING_CONTAINS(stored, nullResult ? "g = (empty maybe)" : "g = 42");
        UNIT_ASSERT_STRING_CONTAINS(stored, "g = 99");
    }
    if (compactAndReboot) {
        CompactTable(runtime, shard, tableId);
        RebootTablet(runtime, shard, sender);
    }
    // Redelivery may contain changed candidate values: it must never overwrite
    // the committed result, explicit NULL, another value, or resurrect key 4.
    auto duplicate = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
    duplicate->Record = committedRequest;
    duplicate->Record.ClearRows();
    const i64 candidate = 777;
    for (ui32 key = 1; key <= 4; ++key) {
        auto* row = duplicate->Record.AddRows();
        row->SetKeyColumns(TSerializedCellVec::Serialize({TCell::Make(key)}));
        row->SetValueColumns(TSerializedCellVec::Serialize({TCell::Make(candidate)}));
    }
    THolder<IEventHandle> pageFaultRequest;
    bool receivedFillResponse = false;
    auto faultObserver = runtime.AddObserver<NSharedCache::TEvRequest>([&](auto& event) {
        if (compactAndReboot && !pageFaultRequest &&
            event->Cookie == static_cast<ui64>(NSharedCache::ERequestTypeCookie::Transaction))
        {
            pageFaultRequest.Reset(event.Release());
        }
    });
    auto fillResponseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto&) {
        receivedFillResponse = true;
    });
    runtime.SendToPipe(shard, sender, duplicate.Release(), 0, GetPipeConfigWithRetries());
    if (compactAndReboot) {
        runtime.WaitFor("conditional fill page fault", [&] { return bool(pageFaultRequest) || receivedFillResponse; });
        UNIT_ASSERT_C(pageFaultRequest, "Cold conditional target select did not request a page");
        UNIT_ASSERT(!receivedFillResponse);
        faultObserver.Remove();
        runtime.Send(pageFaultRequest.Release(), 0, true);
    } else {
        faultObserver.Remove();
    }
    auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(sender);
    fillResponseObserver.Remove();
    UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::OK), reply->Get()->Record.GetErrorDescription());
    UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), stored);
    {
        auto raw = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        raw->Record = committedRequest;
        raw->Record.ClearGeneratedColumnFillContext();
        runtime.SendToPipe(shard, sender, raw.Release(), 0, GetPipeConfigWithRetries());
        auto rejected = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(rejected->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::SCHEME_ERROR));
    }
    for (int mutation = 0; mutation < 4; ++mutation) {
        auto forged = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        forged->Record = committedRequest;
        auto* context = forged->Record.MutableGeneratedColumnFillContext();
        if (mutation == 0) context->SetTargetColumnId(999);
        if (mutation == 1) context->SetSnapshotTxId(context->GetSnapshotTxId() + 1);
        if (mutation == 2) context->SetSeqNoRound(0);
        if (mutation == 3) context->SetProgramFingerprint("replaced");
        runtime.SendToPipe(shard, sender, forged.Release(), 0, GetPipeConfigWithRetries());
        auto rejected = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(rejected->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::SCHEME_CHANGED));
    }
    UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), stored);
}

} // namespace

Y_UNIT_TEST_SUITE(TTxDataShardUploadRows) {
    Y_UNIT_TEST(GeneratedFillConcurrentNullAndDeleteReinsert) {
        GeneratedFillTest(false, false, true);
    }

    Y_UNIT_TEST(GeneratedFillOnlyUnwritten) {
        GeneratedFillTest(false, false);
    }

    Y_UNIT_TEST(GeneratedFillNullSurvivesCompaction) {
        GeneratedFillTest(true, true);
    }

    Y_UNIT_TEST(GeneratedFillRetryAfterPageFault) {
        GeneratedFillTest(false, true);
    }


    Y_UNIT_TEST(TestUploadRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 4, false);

        DoUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::SUCCESS);

        DoUploadTestRows(server, sender, "/Root", "/Root/table-doesnt-exist", Ydb::Type::UINT32, Ydb::StatusIds::SCHEME_ERROR);

        DoUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::INT32, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(TestUploadRowsDropColumnRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 4, false);

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        // Capture all upload rows requests
        TVector<THolder<IEventHandle>> uploadRequests;

        auto observerHolder = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&uploadRequests](auto& ev) {
            Cerr << "... captured TEvUploadRowsRequest" << Endl;
            uploadRequests.emplace_back(ev.Release());
        });

        DoStartUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32);

        waitFor([&]{ return uploadRequests.size() >= 3; }, "TEvUploadRowsRequest");
        observerHolder.Remove();

        ui64 dropTxId = AsyncAlterDropColumn(server, "/Root", "table-1", "value");
        WaitTxNotification(server, dropTxId);

        for (auto& ev : uploadRequests) {
            runtime.Send(ev.Release(), 0, true);
        }

        DoWaitUploadTestRows(server, sender, Ydb::StatusIds::GENERIC_ERROR);
    }

    Y_UNIT_TEST(TestUploadRowsLocks) {
        NKikimrConfig::TAppConfig appConfig;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetAppConfig(appConfig)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        TString sessionId = CreateSessionRPC(runtime);

        // Begin transaction reading key 3
        TString txId;
        {
            auto result = KqpSimpleBegin(runtime, sessionId, txId,
                "SELECT value FROM `/Root/table-1` WHERE key = 3");
            UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 300 } }");
        }

        // Do some upserts using UploadRows (overwrites key 3)
        DoUploadRows(runtime, "/Root", "/Root/table-1", {
            { 2, 20 },
            { 3, 30 },
            { 4, 40 },
        });

        // Commit transaction and perform some writes (must result in transaction locks invalidated)
        {
            auto result = KqpSimpleCommit(runtime, sessionId, txId,
                "UPSERT INTO `/Root/table-1` (key, value) VALUES (6, 600);");
            UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
        }
    }


    Y_UNIT_TEST(TestUploadShadowRowsShadowData) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get(), EShadowDataMode::Enabled);

        // Apply some blind operations on an incomplete table
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` ON (key) VALUES (5), (6), (8);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key) VALUES (6), (7), (10);");

        // Write shadow data: keys from 1 to 9 historically had value=key*10
        {
            auto types = std::make_shared<TRowTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<TRows>();
            for (ui32 i = 1; i <= 9; i++) {
                auto key = TVector<TCell>{TCell::Make(ui32(i))};
                auto value = TVector<TCell>{TCell::Make(ui32(i * 10))};
                TSerializedCellVec serializedKey(key);
                TString serializedValue = TSerializedCellVec::Serialize(value);
                rows->emplace_back(serializedKey, serializedValue);
            }
            auto actor = NTxProxy::CreateUploadRowsInternal(
                    sender,
                    "/Root",
                    "/Root/table-1",
                    std::move(types),
                    std::move(rows),
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Writes to shadow data should not be visible yet
        auto data = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(data,
                "key = 1, value = 100\n"
                "key = 3, value = 300\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = (empty maybe)\n"
                "key = 10, value = (empty maybe)\n");

        // Alter table: disable shadow data and change compaction policy
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server,
            AsyncAlterAndDisableShadow(server, "/Root", "table-1", policy.Get()));

        // Shadow data must be visible now
        auto data2 = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(data2,
                "key = 1, value = 100\n"
                "key = 2, value = 20\n"
                "key = 3, value = 300\n"
                "key = 4, value = 40\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = 70\n"
                "key = 9, value = 90\n"
                "key = 10, value = (empty maybe)\n");
    }

    Y_UNIT_TEST(TestUploadShadowRowsShadowDataSplitThenPublish) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get(), EShadowDataMode::Enabled);

        // Apply some blind operations on an incomplete table
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` ON (key) VALUES (5), (6), (8);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key) VALUES (6), (7), (10);");

        // Write shadow data: keys from 1 to 9 historically had value=key*10
        {
            auto types = std::make_shared<TRowTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<TRows>();
            for (ui32 i = 1; i <= 9; i++) {
                auto key = TVector<TCell>{TCell::Make(ui32(i))};
                auto value = TVector<TCell>{TCell::Make(ui32(i * 10))};
                TSerializedCellVec serializedKey(key);
                TString serializedValue = TSerializedCellVec::Serialize(value);
                rows->emplace_back(serializedKey, serializedValue);
            }
            auto actor = NTxProxy::CreateUploadRowsInternal(
                    sender,
                    "/Root",
                    "/Root/table-1",
                    std::move(types),
                    std::move(rows),
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Writes to shadow data should not be visible yet
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 3, value = 300\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = (empty maybe)\n"
                "key = 10, value = (empty maybe)\n");

        // Split shard at key 6
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 1);
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 6);
            WaitTxNotification(server, senderSplit, txId);
            tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 2);
        }

        // Writes to shadow data should still not be visible
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 3, value = 300\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = (empty maybe)\n"
                "key = 10, value = (empty maybe)\n");

        // Alter table: disable shadow data and change compaction policy
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server,
            AsyncAlterAndDisableShadow(server, "/Root", "table-1", policy.Get()));

        // Shadow data must be visible now
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 2, value = 20\n"
                "key = 3, value = 300\n"
                "key = 4, value = 40\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = 70\n"
                "key = 9, value = 90\n"
                "key = 10, value = (empty maybe)\n");
    }

    Y_UNIT_TEST(TestUploadShadowRowsShadowDataPublishThenSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get(), EShadowDataMode::Enabled);

        // Apply some blind operations on an incomplete table
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` ON (key) VALUES (5), (6), (8);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key) VALUES (6), (7), (10);");

        // Write shadow data: keys from 1 to 9 historically had value=key*10
        {
            auto types = std::make_shared<TRowTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<TRows>();
            for (ui32 i = 1; i <= 9; i++) {
                auto key = TVector<TCell>{TCell::Make(ui32(i))};
                auto value = TVector<TCell>{TCell::Make(ui32(i * 10))};
                TSerializedCellVec serializedKey(key);
                TString serializedValue = TSerializedCellVec::Serialize(value);
                rows->emplace_back(serializedKey, serializedValue);
            }
            auto actor = NTxProxy::CreateUploadRowsInternal(
                    sender,
                    "/Root",
                    "/Root/table-1",
                    std::move(types),
                    std::move(rows),
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Writes to shadow data should not be visible yet
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 3, value = 300\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = (empty maybe)\n"
                "key = 10, value = (empty maybe)\n");

        // Alter table: disable shadow data and change compaction policy
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server,
            AsyncAlterAndDisableShadow(server, "/Root", "table-1", policy.Get()));

        // Shadow data must be visible now
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 2, value = 20\n"
                "key = 3, value = 300\n"
                "key = 4, value = 40\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = 70\n"
                "key = 9, value = 90\n"
                "key = 10, value = (empty maybe)\n");

        // Split shard at key 6
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 1);
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 6);
            WaitTxNotification(server, senderSplit, txId);
            tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 2);
        }

        // Shadow data must still be visible
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 2, value = 20\n"
                "key = 3, value = 300\n"
                "key = 4, value = 40\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = 70\n"
                "key = 9, value = 90\n"
                "key = 10, value = (empty maybe)\n");
    }

    Y_UNIT_TEST(TestUploadShadowRowsShadowDataAlterSplitThenPublish) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get(), EShadowDataMode::Enabled);

        // Apply some blind operations on an incomplete table
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` ON (key) VALUES (5), (6), (8);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key) VALUES (6), (7), (10);");

        // Write shadow data: keys from 1 to 9 historically had value=key*10
        {
            auto types = std::make_shared<TRowTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<TRows>();
            for (ui32 i = 1; i <= 9; i++) {
                auto key = TVector<TCell>{TCell::Make(ui32(i))};
                auto value = TVector<TCell>{TCell::Make(ui32(i * 10))};
                TSerializedCellVec serializedKey(key);
                TString serializedValue = TSerializedCellVec::Serialize(value);
                rows->emplace_back(serializedKey, serializedValue);
            }
            auto actor = NTxProxy::CreateUploadRowsInternal(
                    sender,
                    "/Root",
                    "/Root/table-1",
                    std::move(types),
                    std::move(rows),
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Writes to shadow data should not be visible yet
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 3, value = 300\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = (empty maybe)\n"
                "key = 10, value = (empty maybe)\n");

        // Alter table: add extra column
        WaitTxNotification(server,
            AsyncAlterAddExtraColumn(server, "/Root", "table-1"));

        // Split shard at key 6
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 1);
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 6);
            WaitTxNotification(server, senderSplit, txId);
            tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == 2);
        }

        // Write shadow data: keys from 1 to 9 historically had extra=key*10
        {
            auto types = std::make_shared<TRowTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("extra", type);

            auto rows = std::make_shared<TRows>();
            for (ui32 i = 1; i <= 9; i++) {
                auto key = TVector<TCell>{TCell::Make(ui32(i))};
                auto extra = TVector<TCell>{TCell::Make(ui32(i * 10))};
                TSerializedCellVec serializedKey(key);
                TString serializedExtra = TSerializedCellVec::Serialize(extra);
                rows->emplace_back(serializedKey, serializedExtra);
            }
            auto actor = NTxProxy::CreateUploadRowsInternal(
                    sender,
                    "/Root",
                    "/Root/table-1",
                    std::move(types),
                    std::move(rows),
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Writes to shadow data should still not be visible
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100, extra = (empty maybe)\n"
                "key = 3, value = 300, extra = (empty maybe)\n"
                "key = 6, value = (empty maybe), extra = (empty maybe)\n"
                "key = 7, value = (empty maybe), extra = (empty maybe)\n"
                "key = 10, value = (empty maybe), extra = (empty maybe)\n");

        // Alter table: disable shadow data and change compaction policy
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server,
            AsyncAlterAndDisableShadow(server, "/Root", "table-1", policy.Get()));

        // Shadow data must be visible now
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100, extra = 10\n"
                "key = 2, value = 20, extra = 20\n"
                "key = 3, value = 300, extra = 30\n"
                "key = 4, value = 40, extra = 40\n"
                "key = 6, value = (empty maybe), extra = (empty maybe)\n"
                "key = 7, value = 70, extra = 70\n"
                "key = 9, value = 90, extra = 90\n"
                "key = 10, value = (empty maybe), extra = (empty maybe)\n");
    }

    Y_UNIT_TEST(UploadRowsToReplicatedTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions().Replicated(true));

        DoUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::GENERIC_ERROR);
    }

    Y_UNIT_TEST(RetryUploadRowsToShard) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::RPC_REQUEST, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto [shards, _] = CreateShardedTable(server, sender, "/Root", "table-1", 2, false);

        TVector<THolder<IEventHandle>> blockedEnqueueRecords;
        TVector<TActorId> requestedTablets;
        auto prevObserverFunc = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvUploadRowsRequest) {
                requestedTablets.push_back(ev->Recipient);
            } else if (ev->GetTypeRewrite() == TEvDataShard::EvUploadRowsResponse) {
                if (blockedEnqueueRecords.size() < 2) {
                    blockedEnqueueRecords.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP; // drop a message for one datashard
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto splitShard = [&](size_t shardId, size_t splitKey) {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(shardId), splitKey);
            WaitTxNotification(server, senderSplit, txId);
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            UNIT_ASSERT(tablets.size() == shards.size() + 1);
        };


        DoStartUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32, TBackoff(5));

        splitShard(0, 5);

        ui64 minTabletId = Max<ui64>();
        for (auto& ev : blockedEnqueueRecords) {
            auto shardResponse = ev->Get<TEvDataShard::TEvUploadRowsResponse>();
            minTabletId = std::min(minTabletId, shardResponse->Record.GetTabletID());
        }

        for (auto& ev : blockedEnqueueRecords) {
            auto shardResponse = ev->Get<TEvDataShard::TEvUploadRowsResponse>();
            if (shardResponse->Record.GetTabletID() == minTabletId) {
                auto response = MakeHolder<TEvDataShard::TEvUploadRowsResponse>();
                response->Record.SetStatus(NKikimrTxDataShard::TError::WRONG_SHARD_STATE);
                response->Record.SetTabletID(shardResponse->Record.GetTabletID());
                runtime.Send(ev->Recipient, ev->Sender, response.Release());
            } else {
                runtime.Send(ev.Release(), 0, true);
            }
        }

        DoWaitUploadTestRows(server, sender, Ydb::StatusIds::SUCCESS);
        // Must receive 4 events:
        // - splitted shard (event was blocked and unswered with status WRONG_SHARD_STATE)
        // - other shard existed after create table
        // - two shards created after split
        UNIT_ASSERT_VALUES_EQUAL(requestedTablets.size(), 4);
        THashSet<TActorId> requestedTabletsSet(requestedTablets.begin(), requestedTablets.end());
        UNIT_ASSERT_VALUES_EQUAL_C(requestedTabletsSet.size(), 4, JoinRange(",", requestedTabletsSet.begin(), requestedTabletsSet.end()));

        auto data = KqpSimpleExec(runtime, Q_(R"(
            SELECT COUNT(*) FROM `/Root/table-1`
        )"));
        // DoStartUploadTestRows wrote 32 rows
        UNIT_ASSERT_VALUES_EQUAL(data, "{ items { uint64_value: 32 } }");
    }

    void DoShouldRejectOnChangeQueueOverflow(bool overloadSubscribe, bool withBackoff = false) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetChangesQueueItemsLimit(1);

        TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::RPC_REQUEST, NLog::PRI_DEBUG);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Indexes({
                TShardedTableOptions::TIndex{
                    "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
                }
            })
        );

        TVector<ui32> observedUploadStatus;
        TVector<THolder<IEventHandle>> blockedEnqueueRecords;

        auto observerRequestHandler = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&overloadSubscribe](auto& ev) {
            if (!overloadSubscribe) {
                ev->Get()->Record.ClearOverloadSubscribe();
            }
        });

        auto observerResponseHandler = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&observedUploadStatus](auto& ev) {
            observedUploadStatus.push_back(ev->Get()->Record.GetStatus());
        });

        auto prevObserverFunc = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvEnqueueRecords) {
                blockedEnqueueRecords.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        DoUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT(!observedUploadStatus.empty());
        UNIT_ASSERT(observedUploadStatus.back() == NKikimrTxDataShard::TError::OK);
        observedUploadStatus.clear();

        if (!overloadSubscribe && !withBackoff) {
            DoUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::OVERLOADED);
            return;
        }

        TVector<THolder<TEvTxUserProxy::TEvUploadRowsResponse>> responses;
        auto responseAwaiter = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxUserProxy::TEvUploadRowsResponse::EventType: {
                    auto msg = ev->Release<TEvTxUserProxy::TEvUploadRowsResponse>();
                    responses.push_back(std::move(msg));
                    break;
                }
            }
        }));

        TBackoff backoff = withBackoff ? TBackoff(3, TDuration::Seconds(1)) : TBackoff(0);
        DoStartUploadTestRows(server, responseAwaiter, "/Root", "/Root/table-1", Ydb::Type::UINT32, backoff);

        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!blockedEnqueueRecords.empty());
        UNIT_ASSERT(!observedUploadStatus.empty());
        UNIT_ASSERT(observedUploadStatus.back() == NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
        observedUploadStatus.clear();
        UNIT_ASSERT(responses.empty());

        observerRequestHandler.Remove();
        observerResponseHandler.Remove();
        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : blockedEnqueueRecords) {
            runtime.Send(ev.Release(), 0, true);
        }
        blockedEnqueueRecords.clear();

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        waitFor([&]{ return !responses.empty(); }, "upload rows response");

        UNIT_ASSERT_VALUES_EQUAL(responses.back()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldRejectOnChangeQueueOverflow) {
        DoShouldRejectOnChangeQueueOverflow(false);
    }

    Y_UNIT_TEST(ShouldRejectOnChangeQueueOverflowAndRetry) {
        DoShouldRejectOnChangeQueueOverflow(true);
    }

    Y_UNIT_TEST(ShouldRejectOnChangeQueueOverflowAndRetryOnRetryableError) {
        DoShouldRejectOnChangeQueueOverflow(false, true);
    }

    Y_UNIT_TEST(BulkUpsertDuringAddIndexRaceCorruption) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TShardedTableOptions opts;
        CreateShardedTable(server, sender, "/Root", "table-1", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 2), (3, 4);");

        std::vector<std::unique_ptr<IEventHandle>> bulkUpserts;
        auto captureBulkUpserts = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>(
            [&](TEvDataShard::TEvUploadRowsRequest::TPtr& ev) {
                bulkUpserts.emplace_back(ev.Release());
            });

        // Start writing to key 5 using bulk upsert
        NThreading::TFuture<Ydb::Table::BulkUpsertResponse> bulkUpsertFuture;
        {
            Ydb::Table::BulkUpsertRequest request;
            request.set_table("/Root/table-1");
            auto* r = request.mutable_rows();

            auto* reqRowType = r->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
            auto* reqKeyType = reqRowType->add_members();
            reqKeyType->set_name("key");
            reqKeyType->mutable_type()->set_type_id(Ydb::Type::UINT32);
            auto* reqValueType = reqRowType->add_members();
            reqValueType->set_name("value");
            reqValueType->mutable_type()->set_type_id(Ydb::Type::UINT32);

            auto* reqRows = r->mutable_value();
            auto* row1 = reqRows->add_items();
            row1->add_items()->set_uint32_value(5);
            row1->add_items()->set_uint32_value(6);

            using TEvBulkUpsertRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
                Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
            bulkUpsertFuture = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));
        }

        WaitFor(runtime, [&]{ return bulkUpserts.size() > 0; }, "captured bulk upsert");
        UNIT_ASSERT_VALUES_EQUAL(bulkUpserts.size(), 1u);
        captureBulkUpserts.Remove();

        Cerr << "... creating a by_value index" << Endl;
        WaitTxNotification(server, sender,
            AsyncAlterAddIndex(server, "/Root", "/Root/table-1",
                TShardedTableOptions::TIndex{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobal}));
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Unblock the captured bulk upsert
        for (auto& ev : bulkUpserts) {
            runtime.Send(ev.release(), 0, true);
        }
        bulkUpserts.clear();

        // Wait for the bulk upsert to finish
        Cerr << "... waiting for bulk upsert to finish" << Endl;
        auto response = AwaitResponse(runtime, std::move(bulkUpsertFuture));
        Cerr << "... bulk upsert finished with status " << response.operation().status() << Endl;

        // Whether bulk upsert succeeds or not we shouldn't get a corrupted index (bug KIKIMR-20765)
        auto data1 = KqpSimpleExec(runtime, Q_(R"(
            SELECT key, value FROM `/Root/table-1` ORDER BY key
        )"));
        auto data2 = KqpSimpleExec(runtime, Q_(R"(
            SELECT key, value FROM `/Root/table-1` VIEW by_value ORDER BY key
        )"));
        UNIT_ASSERT_VALUES_EQUAL(data1, data2);
    }
}

} // namespace NKikimr
