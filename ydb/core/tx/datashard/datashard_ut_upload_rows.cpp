#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_exchange.h>
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
        NTxProxy::EUploadRowsMode mode = NTxProxy::EUploadRowsMode::Normal,
        TBackoff backoff = TBackoff(0))
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
            mode, false, false, false, 0, backoff);
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

        // SCHEME_CHANGED remains GENERIC_ERROR even while other shard replies are pending.
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

    Y_UNIT_TEST_QUAD(UploadRowsLosesCommittedReplyOnReboot, MultipleShards, WithRetries) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", MultipleShards ? 2 : 1, false);

        size_t requests = 0;
        auto requestObserver = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                ++requests;
            }
        });
        TVector<TEvDataShard::TEvUploadRowsResponse::TPtr> responses;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
            responses.emplace_back(ev.Release());
        });

        std::vector<std::pair<ui32, ui32>> rows{{1, 10}};
        if (MultipleShards) {
            rows.emplace_back(Max<ui32>(), 20);
        }
        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", rows,
            NTxProxy::EUploadRowsMode::Normal, TBackoff(WithRetries ? 1 : 0, TDuration::MilliSeconds(1)));
        WaitFor(runtime, [&]{ return responses.size() == rows.size(); }, "committed upload responses");
        responseObserver.Remove();

        if (MultipleShards) {
            const auto uploader = responses[0]->Recipient;
            const auto completedShard = responses[0]->Get()->Record.GetTabletID();
            UNIT_ASSERT(completedShard != responses[1]->Get()->Record.GetTabletID());
            bool unlinked = false;
            auto unlinkObserver = runtime.AddObserver<TEvPipeCache::TEvUnlink>([&](auto& ev) {
                if (ev->Sender == uploader && ev->Get()->TabletId == completedShard) {
                    unlinked = true;
                }
            });
            runtime.Send(responses[0].Release(), 0, true);
            WaitFor(runtime, [&]{ return unlinked; }, "successful shard response processed");
        }

        // Lose a committed reply through a real pipe disconnect, with or without another completed shard.
        const auto disconnectedShard = responses.back()->Get()->Record.GetTabletID();
        RebootTablet(runtime, disconnectedShard, sender);
        DoWaitUploadRows(runtime, uploadSender, Ydb::StatusIds::UNDETERMINED);

        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), MultipleShards
            ? "key = 1, value = 10\nkey = 4294967295, value = 20\n"
            : "key = 1, value = 10\n");
        // Even with retries available, an unknown outcome must not resend the write.
        UNIT_ASSERT_VALUES_EQUAL(requests, rows.size());
    }

    Y_UNIT_TEST_TWIN(UploadRowsNotDelivered, WithRetries) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        size_t requests = 0;
        auto requestObserver = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() != TEvDataShard::TEvUploadRowsRequest::EventType) {
                return;
            }
            if (++requests == 1) {
                // Model a guaranteed delivery failure by dropping the request before the pipe cache.
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvPipeCache::TEvDeliveryProblem(ev->Get()->TabletId, /* notDelivered */ true)));
                ev.Reset();
            }
        });

        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", {{1, 10}},
            NTxProxy::EUploadRowsMode::Normal, TBackoff(WithRetries ? 1 : 0, TDuration::MilliSeconds(1)));
        DoWaitUploadRows(runtime, uploadSender, WithRetries ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), WithRetries ? "key = 1, value = 10\n" : "");
        UNIT_ASSERT_VALUES_EQUAL(requests, WithRetries ? 2u : 1u);
    }

    Y_UNIT_TEST(UploadRowsIgnoresDeliveryProblemAfterReply) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);

        TVector<TEvDataShard::TEvUploadRowsResponse::TPtr> responses;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
            responses.emplace_back(ev.Release());
        });

        DoStartUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32);
        WaitFor(runtime, [&]{ return responses.size() == 2; }, "upload rows responses");
        responseObserver.Remove();

        const auto uploader = responses[0]->Recipient;
        const auto shardId = responses[0]->Get()->Record.GetTabletID();
        TEvPipeCache::TEvDeliveryProblem::TPtr deliveryProblem;
        auto deliveryObserver = runtime.AddObserver<TEvPipeCache::TEvDeliveryProblem>([&](auto& ev) {
            if (ev->Recipient == uploader && ev->Get()->TabletId == shardId) {
                UNIT_ASSERT(!ev->Get()->NotDelivered);
                deliveryProblem.Reset(ev.Release());
            }
        });
        RebootTablet(runtime, shardId, sender);
        WaitFor(runtime, [&]{ return bool(deliveryProblem); }, "queued pipe disconnect");
        deliveryObserver.Remove();

        bool unlinked = false;
        auto unlinkObserver = runtime.AddObserver<TEvPipeCache::TEvUnlink>([&](auto& ev) {
            if (ev->Sender == uploader && ev->Get()->TabletId == shardId) {
                unlinked = true;
            }
        });
        runtime.Send(responses[0].Release(), 0, true);
        WaitFor(runtime, [&]{ return unlinked; }, "completed shard unlinked");

        // A queued disconnect from the completed shard must not fail the remaining upload.
        runtime.Send(deliveryProblem.Release(), 0, true);
        runtime.Send(responses[1].Release(), 0, true);
        DoWaitUploadTestRows(server, sender, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_TWIN(UploadRowsNotDeliveredAfterOtherShardWrite, OtherReplyReceived) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 2, false);

        TVector<TEvPipeCache::TEvForward::TPtr> requests;
        auto requestObserver = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                requests.emplace_back(ev.Release());
            }
        });
        TVector<TEvDataShard::TEvUploadRowsResponse::TPtr> responses;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
            responses.emplace_back(ev.Release());
        });

        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", {
            {1, 10},
            {Max<ui32>(), 20},
        });
        WaitFor(runtime, [&]{ return requests.size() == 2; }, "upload requests to both shards");
        requestObserver.Remove();

        const auto uploader = requests[0]->Sender;
        const auto failedShard = requests[0]->Get()->TabletId;
        const auto writtenShard = requests[1]->Get()->TabletId;
        // The first request is never delivered. The second shard commits its rows.
        runtime.Send(requests[1].Release(), 0, true);
        WaitFor(runtime, [&]{ return responses.size() == 1; }, "successful shard response");
        responseObserver.Remove();

        if (OtherReplyReceived) {
            bool unlinked = false;
            auto unlinkObserver = runtime.AddObserver<TEvPipeCache::TEvUnlink>([&](auto& ev) {
                if (ev->Sender == uploader && ev->Get()->TabletId == writtenShard) {
                    unlinked = true;
                }
            });
            runtime.Send(responses[0].Release(), 0, true);
            WaitFor(runtime, [&]{ return unlinked; }, "successful shard response processed");
        }

        runtime.Send(new IEventHandle(uploader, sender,
            new TEvPipeCache::TEvDeliveryProblem(failedShard, /* notDelivered */ true)));

        // Both an acknowledged write and a still pending response require UNDETERMINED.
        DoWaitUploadRows(runtime, uploadSender, Ydb::StatusIds::UNDETERMINED);
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleExec(runtime, "SELECT COUNT(*) FROM `/Root/table-1`"),
            "{ items { uint64_value: 1 } }");
    }

    Y_UNIT_TEST_TWIN(UploadRowsOverloadedAfterOtherShardWrite, OtherReplyReceived) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetChangesQueueItemsLimit(1);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Shards(2)
            .Indexes({
                TShardedTableOptions::TIndex{
                    "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
                }
            })
        );

        TVector<THolder<IEventHandle>> blockedChanges;
        auto changesObserver = runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>([&](auto& ev) {
            blockedChanges.emplace_back(ev.Release());
        });
        // Fill the change queue on the first shard only.
        DoUploadRows(runtime, "/Root", "/Root/table-1", {{1, 10}});
        WaitFor(runtime, [&]{ return !blockedChanges.empty(); }, "blocked index changes");

        size_t requests = 0;
        auto requestObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto& ev) {
            ++requests;
            ev->Get()->Record.ClearOverloadSubscribe();
        });
        TEvDataShard::TEvUploadRowsResponse::TPtr success;
        TEvDataShard::TEvUploadRowsResponse::TPtr overloaded;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK) {
                UNIT_ASSERT(!success);
                success.Reset(ev.Release());
            } else {
                UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
                UNIT_ASSERT(!overloaded);
                overloaded.Reset(ev.Release());
            }
        });

        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", {
            {1, 20},
            {Max<ui32>(), 30},
        });
        WaitFor(runtime, [&]{ return bool(success) && bool(overloaded); }, "successful and overloaded shard responses");
        responseObserver.Remove();
        UNIT_ASSERT(success->Get()->Record.GetTabletID() != overloaded->Get()->Record.GetTabletID());

        if (OtherReplyReceived) {
            const auto uploader = success->Recipient;
            const auto writtenShard = success->Get()->Record.GetTabletID();
            bool unlinked = false;
            auto unlinkObserver = runtime.AddObserver<TEvPipeCache::TEvUnlink>([&](auto& ev) {
                if (ev->Sender == uploader && ev->Get()->TabletId == writtenShard) {
                    unlinked = true;
                }
            });
            runtime.Send(success.Release(), 0, true);
            WaitFor(runtime, [&]{ return unlinked; }, "successful shard response processed");
        }

        // The rejection must not permit a retry of the whole batch, even if OK is still pending.
        runtime.Send(overloaded.Release(), 0, true);
        DoWaitUploadRows(runtime, uploadSender, Ydb::StatusIds::UNDETERMINED);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
            "key = 1, value = 10\nkey = 4294967295, value = 30\n");
        UNIT_ASSERT_VALUES_EQUAL(requests, 2u);
    }

    Y_UNIT_TEST_QUAD(UploadRowsRetryAfterOverload, Disconnect, LoseRetryResponse) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetChangesQueueItemsLimit(1);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Indexes({
                TShardedTableOptions::TIndex{
                    "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
                }
            })
        );
        const auto shardId = GetTableShards(server, sender, "/Root/table-1").at(0);

        TVector<THolder<IEventHandle>> blockedChanges;
        auto changesObserver = runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>([&](auto& ev) {
            blockedChanges.emplace_back(ev.Release());
        });
        DoUploadRows(runtime, "/Root", "/Root/table-1", {{1, 10}});
        WaitFor(runtime, [&]{ return !blockedChanges.empty(); }, "blocked index changes");

        size_t requests = 0;
        auto requestObserver = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                ++requests;
            }
        });
        size_t rejections = 0;
        TEvDataShard::TEvUploadRowsResponse::TPtr rejected;
        TEvDataShard::TEvUploadRowsResponse::TPtr committed;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::SHARD_IS_BLOCKED) {
                UNIT_ASSERT_VALUES_EQUAL(++rejections, 1u);
                UNIT_ASSERT(ev->Get()->Record.HasOverloadSubscribed());
                rejected.Reset(ev.Release());
            } else {
                UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
                if (LoseRetryResponse) {
                    UNIT_ASSERT(!committed);
                    committed.Reset(ev.Release());
                }
            }
        });

        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", {{1, 20}},
            NTxProxy::EUploadRowsMode::Normal, TBackoff(1, TDuration::MilliSeconds(1)));
        WaitFor(runtime, [&]{ return bool(rejected); }, "overload rejection from the shard");
        const auto uploader = rejected->Recipient;
        const auto seqNo = rejected->Get()->Record.GetOverloadSubscribed();

        TEvDataShard::TEvOverloadReady::TPtr ready;
        auto readyObserver = runtime.AddObserver<TEvDataShard::TEvOverloadReady>([&](auto& ev) {
            if (ev->Recipient == uploader) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTabletID(), shardId);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSeqNo(), seqNo);
                ready.Reset(ev.Release());
            }
        });
        responseObserver.Remove();
        runtime.Send(rejected.Release(), 0, true);
        changesObserver.Remove();
        for (auto& ev : blockedChanges) {
            runtime.Send(ev.Release(), 0, true);
        }
        blockedChanges.clear();
        WaitFor(runtime, [&]{ return bool(ready); }, "change queue drained and shard ready");
        readyObserver.Remove();
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 10\n");
        UNIT_ASSERT_VALUES_EQUAL(requests, 1u);

        auto retryResponseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
            if (LoseRetryResponse) {
                UNIT_ASSERT(!committed);
                committed.Reset(ev.Release());
            }
        });

        if (Disconnect) {
            // The real rejection is known; losing the subscription must still allow a retry.
            TEvPipeCache::TEvDeliveryProblem::TPtr deliveryProblem;
            auto deliveryObserver = runtime.AddObserver<TEvPipeCache::TEvDeliveryProblem>([&](auto& ev) {
                if (ev->Recipient == uploader && ev->Get()->TabletId == shardId) {
                    UNIT_ASSERT(!ev->Get()->NotDelivered);
                    deliveryProblem.Reset(ev.Release());
                }
            });
            RebootTablet(runtime, shardId, sender);
            WaitFor(runtime, [&]{ return bool(deliveryProblem); }, "pipe disconnected after rejection");
            // Let the shard finish recovery before allowing the uploader's retry.
            UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 10\n");
            deliveryObserver.Remove();
            runtime.Send(deliveryProblem.Release(), 0, true);
        } else {
            runtime.Send(ready.Release(), 0, true);
        }

        if (LoseRetryResponse) {
            WaitFor(runtime, [&]{ return bool(committed); }, "committed retry response");
            retryResponseObserver.Remove();
            // The retried write is a new uncertain operation, unlike the rejected first attempt.
            RebootTablet(runtime, shardId, sender);
        }
        DoWaitUploadRows(runtime, uploadSender,
            LoseRetryResponse ? Ydb::StatusIds::UNDETERMINED : Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 20\n");
        UNIT_ASSERT_VALUES_EQUAL(requests, 2u);
    }

    Y_UNIT_TEST(UploadRowsTimeoutWithPendingWrite) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        TEvDataShard::TEvUploadRowsResponse::TPtr response;
        auto responseObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK);
            response.Reset(ev.Release());
        });
        const auto uploadSender = DoStartUploadRows(runtime, "/Root", "/Root/table-1", {{1, 10}});
        WaitFor(runtime, [&]{ return bool(response); }, "committed upload response");
        responseObserver.Remove();

        // Expiring the deadline keeps TIMEOUT even if the write committed and its reply was lost.
        runtime.Send(new IEventHandle(response->Recipient, sender, new TEvents::TEvWakeup()));
        DoWaitUploadRows(runtime, uploadSender, Ydb::StatusIds::TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 10\n");
    }

    Y_UNIT_TEST_TWIN(UploadRowsPipeCacheUndelivered, Unsure) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        auto requestObserver = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvents::TEvUndelivered(TEvPipeCache::TEvForward::EventType,
                        TEvents::TEvUndelivered::ReasonActorUnknown, Unsure), 0, ev->Cookie));
                ev.Reset();
            }
        });

        DoStartUploadTestRows(server, sender, "/Root", "/Root/table-1", Ydb::Type::UINT32);
        DoWaitUploadTestRows(server, sender, Ydb::StatusIds::INTERNAL_ERROR);
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
