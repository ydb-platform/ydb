#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/testlib/storage_helpers.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <memory>

namespace NKikimr {

using namespace Tests;

Y_UNIT_TEST_SUITE(Vacuum) {

    static const TString DeletedSubkey1("Subkey1");
    static const TString PresentSubkey2("Subkey2");
    static const TString PresentSubkey3("Subkey3");
    static const TString DeletedSubkey4("Subkey4");

    static const TString DeletedShortValue1("_Some_value_1_");
    static const TString PresentLongValue2(size_t(100 * 1024), 'r');
    static const TString PresentShortValue3("_Some_value_3_");
    static const TString DeletedLongValue4(size_t(100 * 1024), 't');

    auto SetupWithTable(bool withCompaction, bool snapshotRetention = false) {
        TVector<TIntrusivePtr<NFake::TProxyDS>> proxyDSs {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(2181038080)),
        };
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetKeepSnapshotTimeout(TDuration::Seconds(1))
            .SetProxyDSMocks(proxyDSs);
        NKikimrConfig::TAppConfig app;
        if (snapshotRetention) {
            app.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
            app.MutableFeatureFlags()->SetEnableDataShardSnapshotLocking(true);
            // Publish registry views explicitly, preserving the default
            // promotion/skew margin that exceeds this test's MVCC timeout.
            app.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(3600);
            serverSettings.SetAppConfig(app);
        }

        Tests::TServer::TPtr server = MakeIntrusive<TServer>(serverSettings);
        auto& runtime = *server->GetRuntime();
        if (snapshotRetention) {
            runtime.GetAppData(0).LongTxServiceConfig = app.GetLongTxServiceConfig();
        }
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        if (snapshotRetention) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(true);
            runtime.GetAppData(0).SnapshotRegistryHolder->Set(nullptr);
        }

        auto opts = TShardedTableOptions()
            .Columns({
                {"key",    "Uint32", true,  false},
                {"subkey", "String", true,  false},
                {"value",  "Utf8",   false, false}
            });
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        UploadRows(runtime, "/Root", "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(1)), TCell(DeletedSubkey1)}, {TCell(DeletedShortValue1)}
        );
        UploadRows(runtime, "/Root", "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(2)), TCell(PresentSubkey2)}, {TCell(PresentLongValue2)}
        );
        UploadRows(runtime, "/Root", "/Root/table-1",
            {{"key", Ydb::Type::UINT32}, {"subkey", Ydb::Type::STRING}, {"value", Ydb::Type::UTF8}},
            {TCell::Make(ui32(3)), TCell(PresentSubkey3)}, {TCell(PresentShortValue3)}
        );
        UploadRows(runtime, "/Root", "/Root/table-1",
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

    void CheckResultEvent(const TEvDataShard::TEvVacuumResult& ev, ui64 tabletId, ui64 generation) {
        UNIT_ASSERT_EQUAL(ev.Record.GetStatus(), NKikimrTxDataShard::TEvVacuumResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ev.Record.GetVacuumGeneration(), generation);
    }

    void VacuumAndCheck(TTestActorRuntime& runtime, TActorId sender, ui64 tabletId, ui64 generation) {
        auto request = MakeHolder<TEvDataShard::TEvVacuum>(generation);
        runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());
        const auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
        CheckResultEvent(*ev->Get(), tabletId, generation);
    }

    void PublishSnapshotRegistry(TTestActorRuntime& runtime, const TTableId& tableId,
        const TVector<TRowVersion>& snapshots = {})
    {
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetOldestCollectionTime(runtime.GetCurrentTime());
        builder->SetSnapshotBorder(TRowVersion::Max());
        for (const auto& snapshot : snapshots) {
            builder->AddSnapshot({tableId}, snapshot);
        }
        runtime.GetAppData(0).SnapshotRegistryHolder->Set(std::move(*builder).Build());
    }

    void AdvanceSnapshotRetentionWindow(TTestActorRuntime& runtime) {
        const auto& config = runtime.GetAppData(0).LongTxServiceConfig;
        const auto margin = TDuration::Seconds(config.GetLocalSnapshotPromotionTimeSeconds())
            + TDuration::MilliSeconds(config.GetMaxClockSkewMs());
        runtime.SimulateSleep(margin + TDuration::Seconds(2));
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> ReadSnapshotKeys(Tests::TServer::TPtr server,
        TActorId sender, ui64 tabletId, const TTableId& tableId, const TRowVersion& snapshot,
        ui64 maxRows = Max<ui64>())
    {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        record.SetReadId(1);
        record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
        record.AddColumns(1);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        record.SetMaxRows(maxRows);
        snapshot.ToProto(record.MutableSnapshot());
        AddFullRangeQuery(*request);
        return SendRead(server, tabletId, request.release(), sender);
    }

    void CheckDeletedRowsRetained(const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs) {
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedSubkey1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedSubkey4));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedShortValue1));
        UNIT_ASSERT(BlobStorageContains(proxyDSs, DeletedLongValue4));
    }

    void CheckTableData(Tests::TServer::TPtr server, const TVector<TIntrusivePtr<NFake::TProxyDS>>& proxyDSs, const TString& table) {
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

    Y_UNIT_TEST(Vacuum) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedVacuumGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(expectedVacuumGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedVacuumGeneration);
        };

        cleanupAndCheck(24);
        cleanupAndCheck(24);
        cleanupAndCheck(25);

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST_TWIN(VacuumPreservesRetainedSnapshot, LocalIterator) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true, true);
        auto& runtime = *server->GetRuntime();
        const auto shard = tableShards.at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const auto snapshot = AcquireReadSnapshot(runtime, "/Root");
        std::unique_ptr<TEvDataShard::TEvReadResult> first;
        if (LocalIterator) {
            first = ReadSnapshotKeys(server, sender, shard, tableId, snapshot, 1);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(first.get()), "1\n");
            UNIT_ASSERT(!first->Record.GetFinished());
        }

        runtime.SimulateSleep(TDuration::Seconds(1));
        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        AdvanceSnapshotRetentionWindow(runtime);
        PublishSnapshotRegistry(runtime, tableId,
            LocalIterator ? TVector<TRowVersion>{} : TVector<TRowVersion>{snapshot});

        // The ordinary MVCC window and the registry freshness margin have
        // expired. Only the explicit registration or acquired iterator remains.
        VacuumAndCheck(runtime, sender, shard, 24);
        CheckDeletedRowsRetained(proxyDSs);

        if (LocalIterator) {
            auto ack = MakeHolder<TEvDataShard::TEvReadAck>();
            ack->Record.SetReadId(first->Record.GetReadId());
            ack->Record.SetSeqNo(first->Record.GetSeqNo());
            ack->Record.SetMaxRows(100);
            ack->Record.SetMaxBytes(Max<ui32>());
            runtime.SendToPipe(shard, sender, ack.Release());
            const auto rest = WaitReadResult(server);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(rest.get()), "2\n3\n4\n");
            UNIT_ASSERT(rest->Record.GetFinished());
        } else {
            const auto retained = ReadSnapshotKeys(server, sender, shard, tableId, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(FormatIntReadResult(retained.get()), "1\n2\n3\n4\n");
            UNIT_ASSERT(retained->Record.GetFinished());
        }

        // A completed generation is idempotent. Reclaiming history released
        // after it completed requires a new generation.
        PublishSnapshotRegistry(runtime, tableId);
        VacuumAndCheck(runtime, sender, shard, 25);
        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST_TWIN(VacuumPreservesHistoryUntilRegistryRefresh, MissingRegistry) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true, true);
        auto& runtime = *server->GetRuntime();
        const auto shard = tableShards.at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        if (!MissingRegistry) {
            // Freeze an empty registry at a time when the soon-to-be-deleted
            // data is still inside the default 35-second freshness window.
            PublishSnapshotRegistry(runtime, tableId);
        }

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        AdvanceSnapshotRetentionWindow(runtime);
        VacuumAndCheck(runtime, sender, shard, 24);
        CheckDeletedRowsRetained(proxyDSs);

        // Wall-clock time alone cannot replace fresh registry knowledge.
        // Once it arrives, the next Vacuum can physically remove the rows.
        PublishSnapshotRegistry(runtime, tableId);
        VacuumAndCheck(runtime, sender, shard, 25);
        CheckTableData(server, proxyDSs, "/Root/table-1");
    }


    Y_UNIT_TEST(VacuumWithoutCompaction) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(false);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        auto cleanupAndCheck = [&runtime, &sender, &tableShards](ui64 expectedVacuumGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(expectedVacuumGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedVacuumGeneration);
        };

        cleanupAndCheck(24);

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(MultipleVacuums) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        ui64 expectedGenFirst = 42;
        ui64 expectedGenLast = 43;
        auto request1 = MakeHolder<TEvDataShard::TEvVacuum>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvVacuum>(expectedGenLast);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenLast);
        }

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(MultipleVacuumsWithOldGenerations) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        ui64 expectedGenFirst = 42;
        ui64 expectedGenOld = 10;
        auto request1 = MakeHolder<TEvDataShard::TEvVacuum>(expectedGenFirst);
        auto request2 = MakeHolder<TEvDataShard::TEvVacuum>(expectedGenOld);

        runtime.SendToPipe(tableShards.at(0), sender, request1.Release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tableShards.at(0), sender, request2.Release(), 0, GetPipeConfigWithRetries());

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), expectedGenFirst);
        }

        CheckTableData(server, proxyDSs, "/Root/table-1");
    }

    Y_UNIT_TEST(VacuumWithRestart) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        ui64 cleanupGeneration = 33;
        ui64 oldGeneration = 10;
        ui64 olderGeneration = 5;

        {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(cleanupGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(oldGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), cleanupGeneration);
        }

        // restart tablet
        SendViaPipeCache(runtime, tableShards.at(0), sender, std::make_unique<TEvents::TEvPoison>());
        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(olderGeneration);

            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
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

        auto cleanupAndCheck = [&runtime, &sender](ui64 tabletId, ui64 expectedVacuumGeneration) {
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(expectedVacuumGeneration);

            runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tabletId, expectedVacuumGeneration);
        };

        cleanupAndCheck(table2Shards.at(0), 24);
        cleanupAndCheck(tableShards.at(0), 24);

        CheckTableData(server, proxyDSs, "/Root/table-1");
        CheckTableData(server, proxyDSs, "/Root/table-2");
    }

    Y_UNIT_TEST(BorrowerDataCleanedAfterCopyTable) {
        auto [server, sender, tableShards, proxyDSs] = SetupWithTable(true);
        auto& runtime = *server->GetRuntime();

        auto txIdCopy = AsyncCreateCopyTable(server, sender, "/Root", "table-2", "/Root/table-1");
        WaitTxNotification(server, sender, txIdCopy);
        auto table2Shards = GetTableShards(server, sender, "/Root/table-2");
        auto table2Id = ResolveTableId(server, sender, "/Root/table-2");

        ExecSQL(server, sender, "DELETE FROM `/Root/table-1` WHERE key IN (1, 4);");
        ExecSQL(server, sender, "DELETE FROM `/Root/table-2` WHERE key IN (1, 4);");
        SimulateSleep(runtime, TDuration::Seconds(2));

        ui64 vacuumGeneration = 24;
        {
            // cleanup for the first table should be failed due to borrowed parts
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(vacuumGeneration);
            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvVacuumResult::BORROWED);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTabletId(), tableShards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetVacuumGeneration(), vacuumGeneration);
        }
        {
            // cleanup for the second table
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(vacuumGeneration);
            runtime.SendToPipe(table2Shards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), table2Shards.at(0), vacuumGeneration);
        }
        {
            // next cleanup for the first table should succeed after compaction of the second table
            ++vacuumGeneration;
            auto request = MakeHolder<TEvDataShard::TEvVacuum>(vacuumGeneration);
            runtime.SendToPipe(tableShards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvVacuumResult>(sender);
            CheckResultEvent(*ev->Get(), tableShards.at(0), vacuumGeneration);
        }

        CheckTableData(server, proxyDSs, "/Root/table-1");
        CheckTableData(server, proxyDSs, "/Root/table-2");
    }
}

} // namespace NKikimr
