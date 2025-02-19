#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace Tests;

Y_UNIT_TEST_SUITE(DataCleanup) {

    static const TString DeletedShortValue("Some_value");
    static const TString DeletedLongValue(size_t(100 * 1024), 't');
    static const TString PresentShortValue("Some_other_value");
    static const TString PresentLongValue(size_t(100 * 1024), 'r');

    bool BlobStorageContains(const TVector<TServerSettings::TProxyDSPtr>& proxyDSs, const TString& value) {
        for (const auto& proxyDS : proxyDSs) {
            for (const auto& [id, blob] : proxyDS->AllMyBlobs()) {
                if (!blob.DoNotKeep && blob.Buffer.ConvertToString().Contains(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    auto SetupWithTable() {
        TVector<TServerSettings::TProxyDSPtr> proxyDSs {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(2181038080)),
        };
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetKeepSnapshotTimeout(TDuration::Seconds(1))
            .SetProxyDSMocks(proxyDSs);

        Tests::TServer::TPtr server = MakeIntrusive<TServer>(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Columns({
                {"key",   "Uint32", true,  false},
                {"value", "Utf8",   false, false}
            });
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(1))}, {TCell(DeletedShortValue)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(2))}, {TCell(PresentLongValue)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(3))}, {TCell(PresentShortValue)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(4))}, {TCell(DeletedLongValue)}
        );

        auto compactionResult = CompactTable(runtime, shards.at(0), tableId, true);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);

        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedShortValue));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentLongValue));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentShortValue));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedLongValue));

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");

        SimulateSleep(runtime, TDuration::Seconds(2));

        return std::make_tuple(server, sender, shards, proxyDSs);
    }

    void CheckResultEvent(const TEvDataShard::TEvForceDataCleanupResult& ev, ui64 tabletId, ui64 generation) {
        UNIT_ASSERT_EQUAL(ev.Record.GetStatus(), NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetDataCleanupGeneration(), generation);
    }

    void CheckTableData(Tests::TServer::TPtr server, const TVector<TServerSettings::TProxyDSPtr>& proxyDSs) {
        auto result = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_EQUAL(result,
            "key = 2, value = " + PresentLongValue + "\n"
            "key = 3, value = " + PresentShortValue + "\n"
        );
        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedShortValue));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentLongValue));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentShortValue));
        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedLongValue));
    }

    Y_UNIT_TEST(ForceDataCleanup) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedDataCleanupGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedDataCleanupGeneration);
        };

        cleanupAndCheck(24);
        cleanupAndCheck(24);
        cleanupAndCheck(25);

        CheckTableData(server, proxyDSs);
    }

    Y_UNIT_TEST(MultipleDataCleanups) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 expectedGenFirst = 42;
        ui64 expectedGenLast = 43;
        auto request1 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenLast);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        CheckTableData(server, proxyDSs);
    }

    Y_UNIT_TEST(MultipleDataCleanupsWithOldGenerations) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 expectedGenFirst = 42;
        ui64 expectedGenOld = 10;
        auto request1 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedGenOld);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        CheckTableData(server, proxyDSs);
    }

    Y_UNIT_TEST(ForceDataCleanupWithRestart) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable();
        auto& runtime = *server->GetRuntime();

        ui64 cleanupGeneration = 33;
        ui64 oldGeneration = 10;
        ui64 olderGeneration = 5;

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(cleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(oldGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        // restart tablet
        SendViaPipeCache(runtime, tableShards.at(0), sender, std::make_unique<TEvents::TEvPoison>());
        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(olderGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            // more recent generation should be persisted
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        CheckTableData(server, proxyDSs);
    }
}

} // namespace NKikimr
