#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/erase_rows.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

static TActorId DoStartEraseRows(
        TTestActorRuntime& runtime,
        const TString& database,
        const TString& tableName,
        const std::vector<ui32>& keys,
        bool writeToTableShadow = false)
{
    auto sender = runtime.AllocateEdgeActor();

    auto serializedKeys = std::make_shared<NTxProxy::TEraseKeys>();
    for (ui32 key : keys) {
        serializedKeys->emplace_back(TVector<TCell>{ TCell::Make(key) });
    }

    auto actor = NTxProxy::CreateEraseRowsInternal(
            sender,
            database,
            tableName,
            std::move(serializedKeys),
            writeToTableShadow);
    runtime.Register(actor);

    return sender;
}

static void DoWaitEraseRows(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
{
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvEraseRowsResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, expected);
}

static void DoEraseRows(
        TTestActorRuntime& runtime,
        const TString& database,
        const TString& tableName,
        const std::vector<ui32>& keys,
        bool writeToTableShadow = false,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
{
    auto sender = DoStartEraseRows(runtime, database, tableName, keys, writeToTableShadow);
    DoWaitEraseRows(runtime, sender, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(TTxDataShardEraseRowsProxy) {

    Y_UNIT_TEST(TestEraseRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 4, false);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (2, 200), (3, 300);");

        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 100\n"
                "key = 2, value = 200\n"
                "key = 3, value = 300\n");

        DoEraseRows(runtime, "/Root", "/Root/table-1", { 1, 3 });

        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 2, value = 200\n");

        // Erasing from an unknown table fails
        DoEraseRows(runtime, "/Root", "/Root/table-doesnt-exist", { 1 }, false, Ydb::StatusIds::SCHEME_ERROR);

        // Erasing an empty key set is a no-op success
        DoEraseRows(runtime, "/Root", "/Root/table-1", {}, false, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestEraseShadowRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false, policy.Get(), EShadowDataMode::Enabled);

        // Write shadow data: keys from 1 to 5 historically had value = key*10
        {
            auto types = std::make_shared<NTxProxy::TUploadTypes>();
            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<NTxProxy::TUploadRows>();
            for (ui32 i = 1; i <= 5; i++) {
                rows->emplace_back(
                    TSerializedCellVec(TVector<TCell>{ TCell::Make(i) }),
                    TSerializedCellVec::Serialize(TVector<TCell>{ TCell::Make(i * 10) }));
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

        // Erase shadow keys 2 and 4
        DoEraseRows(runtime, "/Root", "/Root/table-1", { 2, 4 }, true);

        // Publish shadow data
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server,
            AsyncAlterAndDisableShadow(server, "/Root", "table-1", policy.Get()));

        // Erased keys must be gone, the rest of the shadow data must be visible
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                "key = 1, value = 10\n"
                "key = 3, value = 30\n"
                "key = 5, value = 50\n");
    }

}

} // namespace NKikimr
