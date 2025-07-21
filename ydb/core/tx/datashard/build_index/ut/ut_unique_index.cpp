#include "defs.h"
#include "ut_helpers.h"

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

static const TString kTable = "/Root/TestTable";
static const TString kTable2 = "/Root/TestTable-2";

static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
    std::function<void(NKikimrTxDataShard::TEvValidateUniqueIndexRequest&)> setupRequest,
    TString expectedError, bool expectedErrorSubstring = false)
{
    TVector<ui64> datashards = GetTableShards(server, sender, kTable);
    TTableId tableId = ResolveTableId(server, sender, kTable);

    TStringBuilder data;
    TString err;
    UNIT_ASSERT(datashards.size() == 1);

    auto ev = std::make_unique<TEvDataShard::TEvValidateUniqueIndexRequest>();
    NKikimrTxDataShard::TEvValidateUniqueIndexRequest& rec = ev->Record;
    rec.SetId(1);

    rec.SetTabletId(datashards[0]);
    rec.SetOwnerId(tableId.PathId.OwnerId);
    rec.SetPathId(tableId.PathId.LocalPathId);

    rec.AddIndexColumns("key");
    rec.AddIndexColumns("value");

    setupRequest(rec);

    TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = DoBadRequest<TEvDataShard::TEvValidateUniqueIndexResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring);
    UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetTabletId(), datashards[0]);

    // Stats
    // Invalid parameters => we scanned nothing
    UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadRows(), 0);
    UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadBytes(), 0);
    UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 0);
    UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
}

Y_UNIT_TEST_SUITE(TTxDataShardValidateUniqueIndexScan) {
    static TEvDataShard::TEvValidateUniqueIndexResponse::TPtr MakeScanRequest(
        Tests::TServer::TPtr server,
        TActorId sender,
        std::pair<ui64, ui64>& seqNo,
        const TString& tablePath = kTable,
        const std::vector<TString>& columns = {"key", "value"},
        NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::DONE,
        const TString& expectedErrorSubstring = {})
    {
        TVector<ui64> datashards = GetTableShards(server, sender, tablePath);
        TTableId tableId = ResolveTableId(server, sender, tablePath);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);
        ui64 tabletId = datashards[0];

        auto ev = new TEvDataShard::TEvValidateUniqueIndexRequest;
        NKikimrTxDataShard::TEvValidateUniqueIndexRequest& rec = ev->Record;
        rec.SetId(1);

        rec.SetTabletId(datashards[0]);
        rec.SetOwnerId(tableId.PathId.OwnerId);
        rec.SetPathId(tableId.PathId.LocalPathId);

        rec.SetSeqNoGeneration(seqNo.first);
        rec.SetSeqNoRound(seqNo.second++);

        for (const TString& col : columns) {
            rec.AddIndexColumns(col);
        }

        auto& runtime = *server->GetRuntime();
        runtime.SendToPipe(tabletId, sender, ev, 0, GetPipeConfigWithRetries());

        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateUniqueIndexResponse>(sender);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(reply->Get()->Record.GetIssues(), issues);

        auto status = reply->Get()->Record.GetStatus();
        UNIT_ASSERT_VALUES_EQUAL_C(status, expectedStatus, issues.ToOneLineString());

        if (expectedErrorSubstring) {
            UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), expectedErrorSubstring);
        }

        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetTabletId(), datashards[0]);

        // Stats
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadRows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadBytes(), 0);

        return reply;
    }

    static TString PrintRow(const TString& serialized, const std::vector<NScheme::TTypeInfo>& expectedTypes) {
        TSerializedCellVec key(serialized);
        auto cells = key.GetCells();

        UNIT_ASSERT_VALUES_EQUAL(cells.size(), expectedTypes.size());

        // Print key as tuple
        TStringBuilder keyStr;
        keyStr << "(";
        for (size_t i = 0; i < cells.size(); ++i) {
            if (i > 0) {
                keyStr << ", ";
            }
            DbgPrintValue(keyStr, cells[i], expectedTypes[i]);
        }
        keyStr << ")";

        return std::move(keyStr);
    }

    static void AssertFirstIndexKey(
        const TEvDataShard::TEvValidateUniqueIndexResponse::TPtr& reply,
        const std::optional<TString>& expectedKey,
        const std::vector<NScheme::TTypeInfo>& expectedTypes = {NScheme::NTypeIds::Uint32, NScheme::NTypeIds::Uint32})
    {
        const auto& record = reply->Get()->Record;
        if (!expectedKey) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetFirstIndexKey().size(), 0);
            return;
        }

        UNIT_ASSERT_STRINGS_EQUAL(PrintRow(record.GetFirstIndexKey(), expectedTypes), *expectedKey);
    }

    static void AssertLastIndexKey(
        const TEvDataShard::TEvValidateUniqueIndexResponse::TPtr& reply,
        const std::optional<TString>& expectedKey,
        const std::vector<NScheme::TTypeInfo>& expectedTypes = {NScheme::NTypeIds::Uint32, NScheme::NTypeIds::Uint32})
    {
        const auto& record = reply->Get()->Record;
        if (!expectedKey) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetLastIndexKey().size(), 0);
            return;
        }

        UNIT_ASSERT_STRINGS_EQUAL(PrintRow(record.GetLastIndexKey(), expectedTypes), *expectedKey);
    }

    Y_UNIT_TEST(BadRequest) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "TestTable", 1, false);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvValidateUniqueIndexRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvValidateUniqueIndexRequest& request) {
            request.SetPathId(0);
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvValidateUniqueIndexRequest& request) {
            request.ClearIndexColumns();
        }, "{ <main>: Error: Empty index columns list }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvValidateUniqueIndexRequest& request) {
            request.AddIndexColumns("some");
        }, "{ <main>: Error: Unknown index column: some }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvValidateUniqueIndexRequest& request) {
            request.ClearIndexColumns();
            request.SetPathId(0);
        }, "[ { <main>: Error: Unknown table id: 0 } { <main>: Error: Empty index columns list } ]");
    }

    Y_UNIT_TEST(RunScan) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .EnableOutOfOrder(false);
        CreateShardedTable(server, sender, "/Root", "TestTable", opts);

        std::pair<ui64, ui64> seqNo = {42, 42};

        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo);
            AssertFirstIndexKey(reply, std::nullopt);
            AssertLastIndexKey(reply, std::nullopt);

            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 0);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }

        // Upsert unique values
        // We perform uniqueness check for usual table, because we don't need anything specific for index
        // Just specify to usual shards what to check
        ExecSQL(server, sender, "UPSERT INTO `/Root/TestTable` (key, value) VALUES (3, 300);");
        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo);
            AssertFirstIndexKey(reply, "(3, 300)");
            AssertLastIndexKey(reply, "(3, 300)");
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 1);
            UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }

        ExecSQL(server, sender, "UPSERT INTO `/Root/TestTable` (key, value) VALUES (1, 100), (3, 300), (5, 500);");
        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo);
            AssertFirstIndexKey(reply, "(1, 100)");
            AssertLastIndexKey(reply, "(5, 500)");
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 3);
            UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }


        auto opts2 = TShardedTableOptions()
            .EnableOutOfOrder(false)
            .Columns({
                {"key_part1", "Int64", true, false},
                {"key_part2", "String", true, false},
                {"key_part3", "String", true, false},
            });
        CreateShardedTable(server, sender, "/Root", "TestTable-2", opts2);

        std::vector<TString> indexColumns2 = {"key_part1", "key_part2"};
        std::vector<NScheme::TTypeInfo> indexTypes2 = { NScheme::NTypeIds::Int64, NScheme::NTypeIds::String };

        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo, kTable2, indexColumns2);
            AssertFirstIndexKey(reply, std::nullopt, indexTypes2);
            AssertLastIndexKey(reply, std::nullopt, indexTypes2);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 0);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }

        ExecSQL(server, sender, "UPSERT INTO `/Root/TestTable-2` (key_part1, key_part2, key_part3) VALUES (1, '1', 'one'), (1, '1', 'two');");
        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo, kTable2,
                indexColumns2, NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, "Duplicate key found: (key_part1=1, key_part2=1)");

            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 2);
            UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }

        // Handle NULLs
        ExecSQL(server, sender, "DELETE FROM `/Root/TestTable-2`;");
        ExecSQL(server, sender, "UPSERT INTO `/Root/TestTable-2` (key_part1, key_part2, key_part3) VALUES (1, '1', 'one'), (1, NULL, NULL);");
        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo, kTable2, indexColumns2);
            AssertFirstIndexKey(reply, "(1, NULL)", indexTypes2);
            AssertLastIndexKey(reply, "(1, 1)", indexTypes2);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 2);
            UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }

        // Two NULLs are not equal to each other
        // So unique index with such keys is OK
        ExecSQL(server, sender, "UPSERT INTO `/Root/TestTable-2` (key_part1, key_part2, key_part3) VALUES (NULL, '1', 'one'), (NULL, NULL, 'one'), (NULL, NULL, NULL);");
        {
            TEvDataShard::TEvValidateUniqueIndexResponse::TPtr reply = MakeScanRequest(server, sender, seqNo, kTable2, indexColumns2);
            AssertFirstIndexKey(reply, "(NULL, NULL)", indexTypes2);
            AssertLastIndexKey(reply, "(1, 1)", indexTypes2);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetReadRows(), 5);
            UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetReadBytes(), 0);
        }
    }
}

} // namespace NKikimr
