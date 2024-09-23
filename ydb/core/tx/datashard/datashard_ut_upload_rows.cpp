#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <library/cpp/testing/unittest/registar.h>

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
        const TString& tableName,
        Ydb::Type::PrimitiveTypeId typeId)
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

    auto actor = NTxProxy::CreateUploadRowsInternal(sender, tableName, types, rows);
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
                             const TString& tableName, Ydb::Type::PrimitiveTypeId typeId,
                             Ydb::StatusIds::StatusCode expected)
{
    DoStartUploadTestRows(server, sender, tableName, typeId);
    DoWaitUploadTestRows(server, sender, expected);
}

static TActorId DoStartUploadRows(
        TTestActorRuntime& runtime,
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
        const TString& tableName,
        const std::vector<std::pair<ui32, ui32>>& data,
        NTxProxy::EUploadRowsMode mode = NTxProxy::EUploadRowsMode::Normal,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
{
    auto sender = DoStartUploadRows(runtime, tableName, data, mode);
    DoWaitUploadRows(runtime, sender, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(TTxDataShardUploadRows) {

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

        DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::SUCCESS);

        DoUploadTestRows(server, sender, "/Root/table-doesnt-exist", Ydb::Type::UINT32, Ydb::StatusIds::SCHEME_ERROR);

        DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::INT32, Ydb::StatusIds::SCHEME_ERROR);
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

        DoStartUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32);

        waitFor([&]{ return uploadRequests.size() >= 3; }, "TEvUploadRowsRequest");
        observerHolder.Remove();

        ui64 dropTxId = AsyncAlterDropColumn(server, "/Root", "table-1", "value");
        WaitTxNotification(server, dropTxId);

        for (auto& ev : uploadRequests) {
            runtime.Send(ev.Release(), 0, true);
        }

        DoWaitUploadTestRows(server, sender, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST_TWIN(TestUploadRowsLocks, StreamLookup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(StreamLookup);

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
        DoUploadRows(runtime, "/Root/table-1", {
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

    Y_UNIT_TEST(TestUploadShadowRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get());

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
                    "/Root/table-1",
                    std::move(types),
                    rows,
                    NTxProxy::EUploadRowsMode::WriteToTableShadow);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        auto data = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(data,
                "key = 1, value = 100\n"
                "key = 2, value = 20\n"
                "key = 3, value = 300\n"
                "key = 4, value = 40\n"
                "key = 6, value = (empty maybe)\n"
                "key = 7, value = 70\n"
                "key = 9, value = 90\n"
                "key = 10, value = (empty maybe)\n");
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

        DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::GENERIC_ERROR);
    }

    void DoShouldRejectOnChangeQueueOverflow(bool overloadSubscribe) {
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

        DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT(!observedUploadStatus.empty());
        UNIT_ASSERT(observedUploadStatus.back() == NKikimrTxDataShard::TError::OK);
        observedUploadStatus.clear();

        if (!overloadSubscribe) {
            DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::OVERLOADED);
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

        DoStartUploadTestRows(server, responseAwaiter, "/Root/table-1", Ydb::Type::UINT32);

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
