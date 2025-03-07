#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace Tests;

Y_UNIT_TEST_SUITE(DataCleanup) {

    static const TString DeletedSubkey1("Subkey1");
    static const TString PresentSubkey2("Subkey2");
    static const TString PresentSubkey3("Subkey3");
    static const TString DeletedSubkey4("Subkey4");

    static const TString DeletedShortValue1("_Some_value_1_");
    static const TString PresentLongValue2(size_t(100 * 1024), 'r');
    static const TString PresentShortValue3("_Some_value_3_");
    static const TString DeletedLongValue4(size_t(100 * 1024), 't');

    int CountBlobsWithSubstring(ui64 tabletId, const TVector<TServerSettings::TProxyDSPtr>& proxyDSs, const TString& substring) {
        int res = 0;
        for (const auto& proxyDS : proxyDSs) {
            for (const auto& [id, blob] : proxyDS->AllMyBlobs()) {
                if (id.TabletID() == tabletId && !blob.DoNotKeep && blob.Buffer.ConvertToString().Contains(substring)) {
                    ++res;
                }
            }
        }
        return res;
    }

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

    auto SetupWithTable(bool withCompaction) {
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
                {"key",    "Uint32", true,  false},
                {"subkey", "String", true,  false},
                {"value",  "Utf8",   false, false}
            });
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(1)), TCell(DeletedSubkey1)}, {TCell(DeletedShortValue1)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(2)), TCell(PresentSubkey2)}, {TCell(PresentLongValue2)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(3)), TCell(PresentSubkey3)}, {TCell(PresentShortValue3)}
        );
        UploadRows(runtime, "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(4)), TCell(DeletedSubkey4)}, {TCell(DeletedLongValue4)}
        );

        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedSubkey1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentSubkey2));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentSubkey3));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedSubkey4));

        // short values inlined in log
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedShortValue1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentShortValue3));

        if (withCompaction) {
            auto compactionResult = CompactTable(runtime, shards.at(0), tableId, true);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);

            // uncompressed long values should be present only after compaction
            UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentLongValue2));
            UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedLongValue4));
        } else {
            // before compaction long values persisted in log only in compressed format
            UNIT_ASSERT(!BlobStorageContains(proxyDSs, PresentLongValue2));
            UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedLongValue4));
        }

        return std::make_tuple(server, sender, shards, proxyDSs);
    }

    void CheckResultEvent(const TEvDataShard::TEvForceDataCleanupResult& ev, ui64 tabletId, ui64 generation) {
        UNIT_ASSERT_EQUAL(ev.Record.GetStatus(), NKikimrTxDataShard::TEvForceDataCleanupResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetDataCleanupGeneration(), generation);
    }

    void CheckTableData(Tests::TServer::TPtr server, const TVector<TServerSettings::TProxyDSPtr>& proxyDSs, const TString& table) {
        auto result = ReadShardedTable(server, table);
        UNIT_ASSERT_VALUES_EQUAL(result,
            "key = 2, subkey = " + PresentSubkey2 + ", value = " + PresentLongValue2 + "\n"
            "key = 3, subkey = " + PresentSubkey3 + ", value = " + PresentShortValue3 + "\n"
        );

        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedSubkey1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentSubkey2));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentSubkey3));
        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedSubkey4));

        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedShortValue1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentLongValue2));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, PresentShortValue3));
        UNIT_ASSERT(!BlobStorageContains(proxyDSs, DeletedLongValue4));
    }

    Y_UNIT_TEST(ForceDataCleanup) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedDataCleanupGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedDataCleanupGeneration);
        };

        cleanupAndCheck(24);
        cleanupAndCheck(24);
        cleanupAndCheck(25);

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }


    Y_UNIT_TEST(ForceDataCleanupWithoutCompaction) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(false);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedDataCleanupGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedDataCleanupGeneration);
        };

        cleanupAndCheck(24);

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(MultipleDataCleanups) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

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

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(MultipleDataCleanupsWithOldGenerations) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

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

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(ForceDataCleanupWithRestart) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

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

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(OutReadSetsCleanedAfterCopyTable) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        UNIT_ASSERT_VALUES_EQUAL(CountBlobsWithSubstring(tableShards.at(0), proxyDSs, DeletedSubkey1), 3); // in log + after compaction: part switch in log and sst

        size_t readSetsWithDeletedSubkey1 = 0;
        auto prevObserver = runtime.SetObserverFunc([&readSetsWithDeletedSubkey1](TAutoPtr<IEventHandle> &ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    if (msg->Record.SerializeAsString().Contains(DeletedSubkey1)) {
                        ++readSetsWithDeletedSubkey1;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto txIdCopy = AsyncCreateCopyTable(server, sender, "/Root", "table-2", "/Root/table-1");
        WaitTxNotification(server, sender, txIdCopy);
        auto table2Shards = GetTableShards(server, sender, "/Root/table-2");
        auto table2Id = ResolveTableId(server, sender, "/Root/table-2");

        UNIT_ASSERT_VALUES_EQUAL(readSetsWithDeletedSubkey1, 1);
        UNIT_ASSERT_VALUES_EQUAL(CountBlobsWithSubstring(tableShards.at(0), proxyDSs, DeletedSubkey1), 4); // + outreadset
        UNIT_ASSERT_VALUES_EQUAL(CountBlobsWithSubstring(table2Shards.at(0), proxyDSs, DeletedSubkey1), 1); // in log

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-2` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(CountBlobsWithSubstring(tableShards.at(0), proxyDSs, DeletedSubkey1), 5); // + deletion in log
        UNIT_ASSERT_VALUES_EQUAL(CountBlobsWithSubstring(table2Shards.at(0), proxyDSs, DeletedSubkey1), 2); // + deletion in log

        auto cleanupAndCheck = [&runtime, &sender](ui64 tabletId, ui64 expectedDataCleanupGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvForceDataCleanup>(expectedDataCleanupGeneration);

            runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvForceDataCleanupResult>(sender);
            CheckResultEvent(*ev->Get(), tabletId, expectedDataCleanupGeneration);
        };

        cleanupAndCheck(table2Shards.at(0), 24);
        cleanupAndCheck(tableShards.at(0), 24);

        CheckTableData(server, proxyDSs, "/Root/table-1");
        CheckTableData(server, proxyDSs, "/Root/table-2");
    }
}

} // namespace NKikimr
