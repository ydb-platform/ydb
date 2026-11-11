#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"
#include "datashard_user_table.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

namespace {

NKikimrTxDataShard::TEvCompactTableResult CompactTable(
        Tests::TServer::TPtr server,
        NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
        ui64 tabletId,
        ui64 ownerId,
        ui64 cookie)
{
    return CompactTable(*server->GetRuntime(), tabletId, TTableId(ownerId, userTable.GetPathId()), false, cookie);
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardCompaction) {
    Y_UNIT_TEST(ZstdColumnFamilyCodec) {
        NKikimrSchemeOp::TFamilyDescription description;
        description.SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);
        TUserTable::TUserFamily family(description);
        UNIT_ASSERT(family.Codec == NTable::NPage::ECodec::ZSTD);
        UNIT_ASSERT(!family.ColumnCodecLevel.Defined());
        UNIT_ASSERT_VALUES_EQUAL(family.GetCodecLevel(), 1);

        description.SetColumnCodecLevel(7);
        family.Update(description);
        UNIT_ASSERT_VALUES_EQUAL(family.GetCodecLevel(), 7);

        description.SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
        description.ClearColumnCodecLevel();
        family.Update(description);
        UNIT_ASSERT(family.Codec == NTable::NPage::ECodec::LZ4);
        UNIT_ASSERT(!family.ColumnCodecLevel.Defined());
        description.SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);
        family.Update(description);
        UNIT_ASSERT(family.Codec == NTable::NPage::ECodec::ZSTD);
        UNIT_ASSERT_VALUES_EQUAL(family.GetCodecLevel(), 1);
        description.ClearColumnCodec();
        description.SetColumnCodecLevel(0);
        family.Update(description);
        UNIT_ASSERT(family.Codec == NTable::NPage::ECodec::ZSTD);
        UNIT_ASSERT_VALUES_EQUAL(family.GetCodecLevel(), 0);
        description.SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);

        NKikimrSchemeOp::TTableDescription schema;
        schema.SetName("table-1");
        schema.SetPath("/Root/table-1");
        schema.MutablePartitionConfig()->AddColumnFamilies()->CopyFrom(description);
        const TUserTable original(1, schema, 0);
        NKikimrSchemeOp::TTableDescription delta;
        delta.SetName("table-1");
        const TUserTable altered(original, delta);
        NKikimrSchemeOp::TTableDescription persisted;
        altered.GetSchema(persisted);
        const auto& persistedFamily = persisted.GetPartitionConfig().GetColumnFamilies(0);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(persistedFamily.GetColumnCodec()),
            static_cast<int>(NKikimrSchemeOp::ColumnCodecZSTD));
        UNIT_ASSERT(persistedFamily.HasColumnCodecLevel());
        UNIT_ASSERT_VALUES_EQUAL(persistedFamily.GetColumnCodecLevel(), 0);
        const TUserTable restored(1, persisted, 0);
        UNIT_ASSERT(restored.Families.at(0).Codec == NTable::NPage::ECodec::ZSTD);
        UNIT_ASSERT_VALUES_EQUAL(restored.Families.at(0).GetCodecLevel(), 0);
    }

    Y_UNIT_TEST_TWIN(ZstdColumnFamilyCompaction, DefaultFamily) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        runtime.GetAppData().FeatureFlags.SetEnableRowTableZstdCompression(true);
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        const TString familyName = DefaultFamily ? "default" : "archive";
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "String", false, false, familyName},
            })
            .Families({{.Name = familyName, .ColumnCodec = NKikimrSchemeOp::ColumnCodecZSTD}}));
        const ui64 shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        const TString value(1024, 'x');
        ui32 key = 0;
        TString expected;
        const TVector<TShardedTableOptions::TFamily> changes = {
            {.Name = familyName, .ColumnCodec = NKikimrSchemeOp::ColumnCodecZSTD},
            {.Name = familyName, .ColumnCodecLevel = 7},
            {.Name = familyName, .ColumnCodecLevel = 0},
            {.Name = familyName, .ColumnCodec = NKikimrSchemeOp::ColumnCodecLZ4},
            {.Name = familyName, .ColumnCodec = NKikimrSchemeOp::ColumnCodecZSTD},
        };
        for (const auto& change : changes) {
            WaitTxNotification(server, sender, AsyncAlterColumnFamily(server, "/Root", "table-1", change));
            if (key) {
                UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), expected);
            }
            ExecSQL(server, sender, TStringBuilder() << "UPSERT INTO `/Root/table-1` (key, value) VALUES ("
                << ++key << ", '" << value << "');");
            expected = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_STRING_CONTAINS(expected, value);

            const auto result = CompactTable(runtime, shard, tableId, false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
            UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), expected);
            RebootTablet(runtime, shard, sender);
            UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), expected);
        }
    }

    Y_UNIT_TEST(ShouldCompact) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId, 42u);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
    }

    Y_UNIT_TEST(ShouldNotCompactWhenBorrowed) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1UL);
        const auto originalShard = shards.at(0);

        SetSplitMergePartCountLimit(&runtime, -1);
        ui64 txId = AsyncSplitTable(server, sender, "/Root/table-1", shards.at(0), 3);
        WaitTxNotification(server, sender, txId);

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT(shards.size() > 1);

        for (auto shard: shards) {
            auto [tables, ownerId] = GetTables(server, shard);
            auto compactionResult = CompactTable(server, tables["table-1"], shard, ownerId, 43u);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::BORROWED);
        }

        {
            auto [tables, ownerId] = GetTables(server, originalShard);
            // try to compact original table (should be inactive now)
            auto compactionResult = CompactTable(server, tables["table-1"], originalShard, ownerId, 21u);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::FAILED);
        }
    }

    Y_UNIT_TEST(ShouldNotCompactWhenCopyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto txIdCopy = AsyncCreateCopyTable(server, sender, "/Root", "table-2", "/Root/table-1");
        WaitTxNotification(server, sender, txIdCopy);

        {
            auto shards = GetTableShards(server, sender, "/Root/table-1");

            auto [tables, ownerId] = GetTables(server, shards.at(0));
            auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId, 33u);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::LOANED);
        }

        {
            auto shards = GetTableShards(server, sender, "/Root/table-2");

            auto [tables, ownerId] = GetTables(server, shards.at(0));
            auto compactionResult = CompactTable(server, tables["table-2"], shards.at(0), ownerId, 55u);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::BORROWED);
        }
    }

    Y_UNIT_TEST(ShouldNotCompactEmptyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId, 134u);
        UNIT_ASSERT(compactionResult.GetStatus() == NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED);
    }

    Y_UNIT_TEST(ShouldNotCompactSecondTime) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId, 11u);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);

        compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId, 13u);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED);
    }
}

} // namespace NKikimr
