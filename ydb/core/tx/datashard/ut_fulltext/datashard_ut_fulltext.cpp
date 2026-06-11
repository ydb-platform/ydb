#include "datashard.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/fulltext.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using NTableIndex::NFulltext::TGen;

Y_UNIT_TEST_SUITE(DataShardFulltext) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(TPortManager& pm, std::optional<TServerSettings> serverSettings = {}) {
        if (!serverSettings) {
            serverSettings.emplace(pm.GetPort(2134));
            serverSettings->SetDomainName("Root").SetUseRealThreads(false);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings.value());
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    TString ToSegment(const TVector<std::pair<ui64, ui32>>& docIds, bool withFreq = false, bool sign = false) {
        NFulltext::TDeltaWriter wr;
        wr.Reset(withFreq, sign);
        for (size_t i = 0; i < docIds.size(); i++) {
            wr.Add(docIds[i].first, docIds[i].second);
        }
        auto buf = wr.GetBuf();
        return TString((const char*)buf.data(), buf.size());
    }

    TVector<TCell> ToInsert(TConstArrayRef<TString> segs, bool Uint32) {
        TVector<TCell> cells(segs.size() * 5);
        for (size_t i = 0; i < segs.size(); i++) {
            cells[i * 5 + 0] = TCell("red", 3);
            cells[i * 5 + 1] = Uint32 ? TCell::Make((ui32)0) : TCell::Make((ui64)0);
            cells[i * 5 + 2] = TCell::Make((TGen)0);
            cells[i * 5 + 3] = TCell::Make(true);
            cells[i * 5 + 4] = TCell(segs[i]);
        }
        return cells;
    }

    void DoTestInsert(bool useInt32, bool useSigned, bool withFreq) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);
        serverSettings.FeatureFlags.SetEnableAccessToIndexImplTables(true);
        serverSettings.FeatureFlags.SetEnableCompactFulltextIndex(true);
        auto [runtime, server, sender] = TestCreateServer(pm, serverSettings);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, Sprintf(R"(
                CREATE TABLE `/Root/table` (pk %s, text string, PRIMARY KEY (pk));
            )", (useSigned ? (useInt32 ? "int32" : "int64") : (useInt32 ? "uint32" : "uint64"))), "/Root"),
            "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, Sprintf(R"(
                ALTER TABLE `/Root/table` ADD INDEX ft_idx GLOBAL USING %s ON (text)
                WITH (tokenizer=standard, use_filter_lowercase=true);
            )", withFreq ? "fulltext_relevance" : "fulltext_plain"), "/Root"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/ft_idx/indexImplTable");
        auto shards = GetTableShards(server, sender, "/Root/table/ft_idx/indexImplTable");
        ui64 txId = 100;
        const ui64 maxKey = (useSigned ? (useInt32 ? INT32_MAX : INT64_MAX) : (useInt32 ? UINT32_MAX : UINT64_MAX));
        const TString maxKeyStr = std::to_string(maxKey);

        Cout << "========= Initial writes =========\n";
        TVector<TString> segs;
        {
            // 1, 2, 3 or -32 -31 -30
            // 100, 200, 201, 203 or -20, -10, 201, 203
            // 10, 20, 30
            segs.push_back(useSigned
                ? ToSegment({{-32, 1}, {-31, 2}, {-30, 1}}, withFreq, useSigned)
                : ToSegment({{1, 1}, {2, 2}, {3, 1}}, withFreq, useSigned));
            segs.push_back(useSigned
                ? ToSegment({{-20, 2}, {-10, 3}, {201, 1}, {203, 1}}, withFreq, useSigned)
                : ToSegment({{100, 2}, {200, 3}, {201, 1}, {203, 1}}, withFreq, useSigned));
            segs.push_back(ToSegment({{10, 2}, {20, 1}, {30, 2}}, withFreq, useSigned));
            TVector<TCell> cells = ToInsert(segs, useInt32);
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            Cout << "Data: " << tableState << "\n";
            TString expectedState = "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX - 2 * gFulltextSegmentPenalty - 4 - 3) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[2]) + "\n" +
                "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX - gFulltextSegmentPenalty - 4) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[1]) + "\n" +
                "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[0]) + "\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }

        Cout << "========= Writes with compaction =========\n";
        {
            gFulltextMaxDelta = 10;
            gFulltextMaxSegment = 3;
            Y_DEFER {
                gFulltextMaxDelta = 10000;
                gFulltextMaxSegment = 10000;
            };

            // del 100, 200
            // list becomes: 1 2 3 10 20 30 201 203
            // -> split into 1 2 3 + 10 20 30 + 201 203
            // add 50, 60
            // added as a new segment
            // -> 1 2 3 [max=9] + 10 20 30 [max=200] + 50 60 [max=200] + 201 203 [max=UINT64_MAX]
            segs.push_back(useSigned
                ? ToSegment({{-20, 2}, {-10, 3}}, withFreq, useSigned)
                : ToSegment({{100, 2}, {200, 3}}, withFreq, useSigned));
            segs.push_back(ToSegment({{50, 1}, {60, 2}}, withFreq, useSigned));
            TVector<TCell> cells = ToInsert(TConstArrayRef<TString>(segs).Slice(3), useInt32);
            cells[3] = TCell::Make(false);
            txId++;
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Read table =========\n";
        {
            segs = {
                segs[0], // { 1 2 3 }
                segs[4], // { 50 60 }
                segs[2], // { 10 20 30 }
                ToSegment({{201, 1}, {203, 1}}, withFreq, useSigned), // { 201 203 }
            };
            auto tableState = ReadTable(server, shards, tableId);
            Cout << "Data: " << tableState << "\n";
            TString expectedState =
                // { 1 2 3 } max = 9 gen = max
                "__ydb_token = red, __ydb_max_id = 9, __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[0]) + "\n" +
                // { 50 60 } max = 200 gen = max-seg
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = " +
                std::to_string(UINT32_MAX - gFulltextSegmentPenalty - 2) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[1]) + "\n" +
                // { 10 20 30 } max = 200 gen = max
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[2]) + "\n" +
                // { 201 203 } max = max gen = max
                "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[3]) + "\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }

        Cout << "========= Longer list split into multiple segments =========\n";
        {
            // add 15 25 202 300 400 500 600
            // inserted as two new segments: 15 25 + 300 400 500 600
            // (no compaction)
            segs.push_back(ToSegment({{15, 1}, {25, 2}, {202, 1}, {300, 1}, {400, 2}, {500, 1}, {600, 1}}, withFreq, useSigned));
            TVector<TCell> cells = ToInsert(TConstArrayRef<TString>(segs).Slice(4), useInt32);
            txId++;
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Read table =========\n";
        {
            segs.pop_back();
            segs.push_back(ToSegment({{15, 1}, {25, 2}}, withFreq, useSigned));
            segs.push_back(ToSegment({{202, 1}, {300, 1}, {400, 2}, {500, 1}, {600, 1}}, withFreq, useSigned));
            auto tableState = ReadTable(server, shards, tableId);
            Cout << "Data: " << tableState << "\n";
            TString expectedState =
                // { 1 2 3 } max = 9 gen = max
                "__ydb_token = red, __ydb_max_id = 9, __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[0]) + "\n" +
                // new: { 15, 25 } max = 200 gen = max-seg-seg2
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = " +
                std::to_string(UINT32_MAX - 2 * gFulltextSegmentPenalty - 2 - 2) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[4]) + "\n" +
                // { 50 60 } max = 200 gen = max-seg
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = " +
                std::to_string(UINT32_MAX - gFulltextSegmentPenalty - 2) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[1]) + "\n" +
                // { 10 20 30 } max = 200 gen = max
                "__ydb_token = red, __ydb_max_id = 200, __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[2]) + "\n" +
                // new: { 202, 300, 400, 500, 600 } max = max gen = max-seg
                "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX - gFulltextSegmentPenalty - 5) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[5]) + "\n" +
                // { 201 203 } max = max gen = max
                "__ydb_token = red, __ydb_max_id = " + maxKeyStr + ", __ydb_generation = " +
                std::to_string(UINT32_MAX) +
                ", __ydb_added = true, __ydb_segment = " + EscapeC(segs[3]) + "\n";
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedState);
        }
    }

    Y_UNIT_TEST_QUAD(InsertUnsigned, Uint32, WithFreq) {
        DoTestInsert(Uint32, false, WithFreq);
    }

    Y_UNIT_TEST_QUAD(InsertSigned, Uint32, WithFreq) {
        DoTestInsert(Uint32, true, WithFreq);
    }

    Y_UNIT_TEST(InsertNoSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);
        serverSettings.FeatureFlags.SetEnableAccessToIndexImplTables(true);
        serverSettings.FeatureFlags.SetEnableCompactFulltextIndex(true);
        auto [runtime, server, sender] = TestCreateServer(pm, serverSettings);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (pk uint64, text string, PRIMARY KEY (pk));
            )"),
            "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                ALTER TABLE `/Root/table` ADD INDEX ft_idx GLOBAL USING fulltext_plain ON (text)
                WITH (tokenizer=standard, use_filter_lowercase=true);
            )", "/Root"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/ft_idx/indexImplTable");
        auto shards = GetTableShards(server, sender, "/Root/table/ft_idx/indexImplTable");
        ui64 txId = 100;

        Cout << "========= Initial writes =========\n";
        {
            // 1, 2, 3
            // 100, 200, 201, 203
            // 10, 20, 30
            TVector<TCell> cells(15);
            cells[0] = TCell("red", 3);
            cells[1] = TCell::Make((ui64)0);
            cells[2] = TCell::Make((TGen)0);
            cells[3] = TCell::Make(true);
            cells[4] = TCell("\x01\x01\x01", 3);
            cells[5] = TCell("red", 3);
            cells[6] = TCell::Make((ui64)0);
            cells[7] = TCell::Make((TGen)0);
            cells[8] = TCell::Make(true);
            cells[9] = TCell("dd\x01\x02", 4);
            cells[10] = TCell("red", 3);
            cells[11] = TCell::Make((ui64)0);
            cells[12] = TCell::Make((TGen)0);
            cells[13] = TCell::Make(true);
            cells[14] = TCell("\x0A\x0A\x0A", 3);
            Write(runtime, sender, shards[0], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Split index impl table on a token =========\n";
        {
            SetSplitMergePartCountLimit(server->GetRuntime(), -1);
            NKikimrMiniKQL::TValue token;
            token.SetBytes(TString("red"));
            NKikimrMiniKQL::TValue maxId;
            maxId.SetUint64(200);
            TVector<NKikimrMiniKQL::TValue> splitKey;
            splitKey.push_back(std::move(token));
            splitKey.push_back(std::move(maxId));
            auto splitTxId = AsyncSplitTable(server, sender, "/Root/table/ft_idx/indexImplTable", shards[0], std::move(splitKey));
            WaitTxNotification(server, sender, splitTxId);

            // Re-resolve shards
            shards = GetTableShards(server, sender, "/Root/table/ft_idx/indexImplTable");
            UNIT_ASSERT_C(shards.size() == 2, "Expected 2 shards after split, got " << shards.size());
            Cout << "Shard 0: " << shards[0] << ", Shard 1: " << shards[1] << "\n";
        }

        Cout << "========= Insert `red` into any of the shards: should fail =========\n";
        for (int i = 0; i < 2; i++) {
            TVector<TCell> cells(5);
            cells[0] = TCell("red", 3);
            cells[1] = TCell::Make((ui64)0);
            cells[2] = TCell::Make((TGen)0);
            cells[3] = TCell::Make(true);
            cells[4] = TCell("\x32", 1);
            txId++;
            Write(runtime, sender, shards[i], MakeWriteRequest(txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId, {1, 2, 3, 4, 5}, cells, 0), NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardFulltext)
} // namespace NKikimr
