#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"
#include "read_iterator.h"

#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <algorithm>
#include <map>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

namespace {

using TCellVec = std::vector<TCell>;

void CreateTable(Tests::TServer::TPtr server,
                 TActorId sender,
                 const TString &root,
                 const TString &name,
                 bool withFollower = false,
                 ui64 shardCount = 1)
{
    TVector<TShardedTableOptions::TColumn> columns = {
        {"key1", "Uint32", true, false},
        {"key2", "Uint32", true, false},
        {"key3", "Uint32", true, false},
        {"value", "Uint32", false, false}
    };

    auto opts = TShardedTableOptions()
        .Shards(shardCount)
        .Columns(columns);

    if (withFollower)
        opts.Followers(1);

    CreateShardedTable(server, sender, root, name, opts);
}

void CreateMoviesTable(Tests::TServer::TPtr server,
                       TActorId sender,
                       const TString &root,
                       const TString &name)
{
    TVector<TShardedTableOptions::TColumn> columns = {
        {"id", "Uint32", true, false},
        {"title", "String", false, false},
        {"rating", "Uint32", false, false}
    };

    auto opts = TShardedTableOptions()
        .Shards(1)
        .Columns(columns);

    CreateShardedTable(server, sender, root, name, opts);
}

struct TRowWriter : public NArrow::IRowWriter {
    std::vector<TOwnedCellVec> Rows;

    TRowWriter() = default;

    void AddRow(const TConstArrayRef<TCell> &cells) override {
        Rows.emplace_back(cells);
    }
};

std::vector<TOwnedCellVec> GetRows(
    const TVector<std::pair<TString, NScheme::TTypeInfo>>& batchSchema,
    const TEvDataShard::TEvReadResult& result)
{
    UNIT_ASSERT(result.GetArrowBatch());

    // TODO: use schema from ArrowBatch
    TRowWriter writer;
    NArrow::TArrowToYdbConverter converter(batchSchema, writer);

    TString error;
    UNIT_ASSERT(converter.Process(*result.GetArrowBatch(), error));

    return std::move(writer.Rows);
}

void CheckRow(
    const TConstArrayRef<TCell>& row,
    const TCellVec& gold,
    const std::vector<NScheme::TTypeInfoOrder>& goldTypes)
{
    UNIT_ASSERT_VALUES_EQUAL(row.size(), gold.size());
    for (size_t i: xrange(row.size())) {
        int cmp = CompareTypedCells(row[i], gold[i], goldTypes[i]);
        UNIT_ASSERT_VALUES_EQUAL(cmp, 0);
    }
}

template <typename TCellVecType>
void CheckRows(
    const std::vector<TCellVecType>& rows,
    const std::vector<TCellVec>& gold,
    const std::vector<NScheme::TTypeInfoOrder>& goldTypes)
{
    UNIT_ASSERT_VALUES_EQUAL(rows.size(), gold.size());
    for (size_t i: xrange(rows.size())) {
        CheckRow(rows[i], gold[i], goldTypes);
    }
}

void CheckResultCellVec(
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TEvDataShard::TEvReadResult& result,
    const std::vector<TCellVec>& gold,
    const std::vector<NScheme::TTypeInfoOrder>& goldTypes,
    std::vector<NTable::TTag> columns = {})
{
    Y_UNUSED(userTable);
    Y_UNUSED(columns);

    UNIT_ASSERT(!gold.empty());

    auto nrows = result.GetRowsCount();
    TVector<TConstArrayRef<TCell>> rows;
    rows.reserve(nrows);
    for (size_t i = 0; i < nrows; ++i) {
        rows.emplace_back(result.GetCells(i));
    }

    UNIT_ASSERT(!rows.empty());
    CheckRows(rows, gold, goldTypes);
}

void CheckResultArrow(
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TEvDataShard::TEvReadResult& result,
    const std::vector<TCellVec>& gold,
    const std::vector<NScheme::TTypeInfoOrder>& goldTypes,
    std::vector<NTable::TTag> columns = {})
{
    UNIT_ASSERT(!gold.empty());
    UNIT_ASSERT(result.GetArrowBatch());

    TVector<std::pair<TString, NScheme::TTypeInfo>> batchSchema;
    const auto& description = userTable.GetDescription();
    if (columns.empty()) {
        batchSchema.reserve(description.ColumnsSize());
        for (const auto& column: description.GetColumns()) {
            batchSchema.emplace_back(column.GetName(), column.GetTypeId());
        }
    } else {
        std::map<NTable::TTag, std::pair<TString, ui32>> colsMap;
        for (const auto& column: description.GetColumns()) {
            colsMap[column.GetId()] = std::make_pair(column.GetName(), column.GetTypeId());
        }
        batchSchema.reserve(columns.size());
        for (auto tag: columns) {
            const auto& col = colsMap[tag];
            batchSchema.emplace_back(col.first, col.second);
        }
    }

    auto rows = GetRows(batchSchema, result);
    CheckRows(rows, gold, goldTypes);
}

void CheckResult(
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TEvDataShard::TEvReadResult& result,
    const std::vector<TCellVec>& gold,
    const std::vector<NScheme::TTypeInfoOrder>& goldTypes,
    std::vector<NTable::TTag> columns = {})
{
    const auto& record = result.Record;

    if (record.GetStatus().IssuesSize()) {
        TStringStream ss;
        for (const auto& issue: record.GetStatus().GetIssues()) {
            ss << "issue: " << issue;
        }
        Cerr << "Request with issues: " << ss.Str() << Endl;
    }

    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
    if (gold.size()) {
        switch (record.GetResultFormat()) {
        case NKikimrTxDataShard::ARROW:
            CheckResultArrow(userTable, result, gold, goldTypes, columns);
            break;
        case NKikimrTxDataShard::CELLVEC:
            CheckResultCellVec(userTable, result, gold, goldTypes, columns);
            break;
        default:
            UNIT_ASSERT(false);
        }
    } else {
        UNIT_ASSERT(!result.GetArrowBatch() && result.GetRowsCount() == 0);
    }
}

void CheckResult(
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TEvDataShard::TEvReadResult& result,
    const std::vector<std::vector<ui32>>& gold,
    std::vector<NTable::TTag> columns = {})
{
    std::vector<NScheme::TTypeInfoOrder> types;
    if (!gold.empty() && !gold[0].empty()) {
        types.reserve(gold[0].size());
        for (auto i: xrange(gold[0].size())) {
            Y_UNUSED(i);
            types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
        }
    }

    std::vector<TCellVec> goldCells;
    goldCells.reserve(gold.size());
    for (const auto& row: gold) {
        TCellVec cells;
        cells.reserve(row.size());
        for (auto item: row) {
            cells.push_back(TCell::Make(item));
        }
        goldCells.emplace_back(std::move(cells));
    }

    CheckResult(userTable, result, goldCells, types, columns);
}

template <typename TKeyType>
TVector<TCell> ToCells(const std::vector<TKeyType>& keys) {
    TVector<TCell> cells;
    for (auto& key: keys) {
        cells.emplace_back(TCell::Make(key));
    }
    return cells;
}

void AddKeyQuery(
    TEvDataShard::TEvRead& request,
    const std::vector<ui32>& keys)
{
    // convertion is ugly, but for tests is OK
    auto cells = ToCells(keys);
    auto buf = TSerializedCellVec::Serialize(cells);
    request.Keys.emplace_back(buf);
}

template <typename TCellType>
void AddRangeQuery(
    TEvDataShard::TEvRead& request,
    std::vector<TCellType> from,
    bool fromInclusive,
    std::vector<TCellType> to,
    bool toInclusive)
{
    auto fromCells = ToCells(from);
    auto toCells = ToCells(to);

    // convertion is ugly, but for tests is OK
    auto fromBuf = TSerializedCellVec::Serialize(fromCells);
    auto toBuf = TSerializedCellVec::Serialize(toCells);

    request.Ranges.emplace_back(fromBuf, toBuf, fromInclusive, toInclusive);
}

struct TTableInfo {
    TString Name;

    ui64 TabletId;
    ui64 OwnerId;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;

    TActorId ClientId;
};

struct TTestHelper {
    explicit TTestHelper(bool withFollower = false) {
        WithFollower = withFollower;
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        init(serverSettings);
    }

    explicit TTestHelper(const TServerSettings& serverSettings, ui64 shardCount = 1, bool withFollower = false) {
        WithFollower = withFollower;
        ShardCount = shardCount;
        init(serverSettings);
    }

    void init(const TServerSettings& serverSettings) {
        Server = new TServer(serverSettings);

        auto &runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);

        InitRoot(Server, Sender);

        {
            auto& table1 = Tables["table-1"];
            table1.Name = "table-1";
            CreateTable(Server, Sender, "/Root", "table-1", WithFollower, ShardCount);
            ExecSQL(Server, Sender, R"(
                UPSERT INTO `/Root/table-1`
                (key1, key2, key3, value)
                VALUES
                (1, 1, 1, 100),
                (3, 3, 3, 300),
                (5, 5, 5, 500),
                (8, 0, 0, 800),
                (8, 0, 1, 801),
                (8, 1, 0, 802),
                (8, 1, 1, 803),
                (11, 11, 11, 1111);
            )");

            auto shards = GetTableShards(Server, Sender, "/Root/table-1");
            table1.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table1.TabletId);
            table1.OwnerId = ownerId;
            table1.UserTable = tables["table-1"];

            table1.ClientId = runtime.ConnectToPipe(table1.TabletId, Sender, 0, GetTestPipeConfig());
        }

        {
            auto& table2 = Tables["movies"];
            table2.Name = "movies";
            CreateMoviesTable(Server, Sender, "/Root", "movies");
            ExecSQL(Server, Sender, R"(
                UPSERT INTO `/Root/movies`
                (id, title, rating)
                VALUES
                (1, "I Robot", 10),
                (2, "I Am Legend", 9),
                (3, "Hard die", 8);
            )");

            auto shards = GetTableShards(Server, Sender, "/Root/movies");
            table2.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table2.TabletId);
            table2.OwnerId = ownerId;
            table2.UserTable = tables["movies"];

            table2.ClientId = runtime.ConnectToPipe(table2.TabletId, Sender, 0, GetTestPipeConfig());
        }

        {
            auto& table3 = Tables["table-1-many"];
            table3.Name = "table-1-many";
            CreateTable(Server, Sender, "/Root", "table-1-many", WithFollower, ShardCount);

            auto shards = GetTableShards(Server, Sender, "/Root/table-1-many");
            table3.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table3.TabletId);
            table3.OwnerId = ownerId;
            table3.UserTable = tables["table-1-many"];

            table3.ClientId = runtime.ConnectToPipe(table3.TabletId, Sender, 0, GetTestPipeConfig());
        }
    }

    void UpsertMany(ui32 startRow, ui32 rowCount) {
        auto &runtime = *Server->GetRuntime();
        const auto& table = Tables["table-1-many"];
        auto endRow = startRow + rowCount;

        for (ui32 key = startRow; key < endRow;) {
            auto request = std::make_unique<TEvDataShard::TEvUploadRowsRequest>();
            auto& record = request->Record;
            record.SetTableId(table.UserTable.GetPathId());

            auto& rowScheme = *record.MutableRowScheme();

            const auto& description = table.UserTable.GetDescription();
            std::set<ui32> keyColumns(
                description.GetKeyColumnIds().begin(),
                description.GetKeyColumnIds().end());

            for (const auto& column: description.GetColumns()) {
                if (keyColumns.contains(column.GetId()))
                    continue;
                rowScheme.AddValueColumnIds(column.GetId());
            }

            for (auto column: keyColumns) {
                rowScheme.AddKeyColumnIds(column);
            }

            for (size_t i = 0; i < 1000 && key < endRow; ++i) {
                TVector<TCell> keys;
                keys.reserve(keyColumns.size());
                for (size_t i = 0; i < keyColumns.size(); ++i) {
                    keys.emplace_back(TCell::Make(key));
                }

                TVector<TCell> values;
                for (size_t i = 0; i < description.ColumnsSize() - keyColumns.size(); ++i) {
                    values.emplace_back(TCell::Make(key)); // key intentionally as value
                }

                auto& row = *record.AddRows();
                row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
                row.SetValueColumns(TSerializedCellVec::Serialize(values));

                ++key;
            }

            runtime.SendToPipe(
                table.TabletId,
                Sender,
                request.release(),
                0,
                GetTestPipeConfig(),
                table.ClientId);

            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(handle);
            UNIT_ASSERT(handle);
            auto event = handle->Release<TEvDataShard::TEvUploadRowsResponse>();
            UNIT_ASSERT(event->Record.GetStatus() == 0);
        }
    }

    void SplitTable1() {
        auto& table1 = Tables["table-1"];
        SetSplitMergePartCountLimit(Server->GetRuntime(), -1);
        ui64 txId = AsyncSplitTable(Server, Sender, "/Root/table-1", table1.TabletId, 5);
        WaitTxNotification(Server, Sender, txId);
    }

    std::unique_ptr<TEvDataShard::TEvRead> GetBaseReadRequest(
        const TString& tableName,
        ui64 readId,
        NKikimrTxDataShard::EScanDataFormat format = NKikimrTxDataShard::ARROW,
        const TRowVersion& snapshot = {})
    {
        const auto& table = Tables[tableName];

        std::unique_ptr<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(table.OwnerId);
        record.MutableTableId()->SetTableId(table.UserTable.GetPathId());

        const auto& description = table.UserTable.GetDescription();
        for (const auto& column: description.GetColumns()) {
            record.AddColumns(column.GetId());
        }

        record.MutableTableId()->SetSchemaVersion(description.GetTableSchemaVersion());

        TRowVersion readVersion;
        if (!snapshot) {
            readVersion = CreateVolatileSnapshot(
                Server,
                {"/Root/movies", "/Root/table-1"},
                TDuration::Hours(1));
        } else {
            readVersion = snapshot;
        }

        record.MutableSnapshot()->SetStep(readVersion.Step);
        record.MutableSnapshot()->SetTxId(readVersion.TxId);

        record.SetResultFormat(format);

        return request;
    }

    std::unique_ptr<TEvDataShard::TEvRead> GetUserTablesRequest(
        const TString& tableName,
        ui64 localTid,
        ui64 readId)
    {
        const auto& table = Tables[tableName];

        std::unique_ptr<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.SetReadId(readId);

        record.MutableTableId()->SetOwnerId(table.TabletId);
        record.MutableTableId()->SetTableId(localTid);

        record.AddColumns(1);
        record.AddColumns(2);

        record.SetResultFormat(NKikimrTxDataShard::CELLVEC);

        return request;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> WaitReadResult(TDuration timeout = TDuration::Max()) {
        auto &runtime = *Server->GetRuntime();
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(handle, timeout);
        if (!handle) {
            return nullptr;
        }
        auto event = handle->Release<TEvDataShard::TEvReadResult>();
        return std::unique_ptr<TEvDataShard::TEvReadResult>(event.Release());
    }

    void SendReadAsync(
        const TString& tableName,
        TEvDataShard::TEvRead* request,
        ui32 node = 0,
        TActorId sender = {})
    {
        if (!sender) {
            sender = Sender;
        }

        const auto& table = Tables[tableName];
        auto &runtime = *Server->GetRuntime();
        runtime.SendToPipe(
            table.TabletId,
            sender,
            request,
            node,
            GetTestPipeConfig(),
            table.ClientId);
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(
        const TString& tableName,
        TEvDataShard::TEvRead* request,
        ui32 node = 0,
        TActorId sender = {},
        TDuration timeout = TDuration::Max())
    {
        SendReadAsync(tableName, request, node, sender);

        return WaitReadResult(timeout);
    }

    void SendReadAck(
        const TString& tableName,
        const NKikimrTxDataShard::TEvReadResult& readResult,
        ui64 rows,
        ui64 bytes,
        ui32 node = 0,
        TActorId sender = {})
    {
        if (!sender) {
            sender = Sender;
        }

        const auto& table = Tables[tableName];
        auto* request = new TEvDataShard::TEvReadAck();
        request->Record.SetReadId(readResult.GetReadId());
        request->Record.SetSeqNo(readResult.GetSeqNo());
        request->Record.SetMaxRows(rows);
        request->Record.SetMaxBytes(bytes);

        auto &runtime = *Server->GetRuntime();
        runtime.SendToPipe(
            table.TabletId,
            sender,
            request,
            node,
            GetTestPipeConfig(),
            table.ClientId);
    }

    void SendCancel(const TString& tableName, ui64 readId) {
        const auto& table = Tables[tableName];
        auto* request = new TEvDataShard::TEvReadCancel();
        request->Record.SetReadId(readId);

        auto &runtime = *Server->GetRuntime();
        runtime.SendToPipe(
            table.TabletId,
            Sender,
            request,
            0,
            GetTestPipeConfig(),
            table.ClientId);
    }

    void CheckLockValid(const TString& tableName, ui64 readId, const std::vector<ui32>& key, ui64 lockTxId) {
        auto request = GetBaseReadRequest(tableName, readId);
        request->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request, key);

        auto readResult = SendRead(tableName, request.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.BrokenTxLocksSize(), 0);
    }

    void CheckLockBroken(
        const TString& tableName,
        ui64 readId,
        const std::vector<ui32>& key,
        ui64 lockTxId,
        const TEvDataShard::TEvReadResult& prevResult)
    {
        auto request = GetBaseReadRequest(tableName, readId);
        request->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request, key);

        auto readResult = SendRead(tableName, request.release());

        const NKikimrTxDataShard::TLock* prevLock;
        if (prevResult.Record.TxLocksSize()) {
            prevLock = &prevResult.Record.GetTxLocks(0);
        } else {
            prevLock = &prevResult.Record.GetBrokenTxLocks(0);
        }

        const NKikimrTxDataShard::TLock* newLock;
        if (readResult->Record.TxLocksSize()) {
            newLock = &readResult->Record.GetTxLocks(0);
        } else {
            newLock = &readResult->Record.GetBrokenTxLocks(0);
        }

        UNIT_ASSERT(newLock && prevLock);
        UNIT_ASSERT_VALUES_EQUAL(newLock->GetLockId(), prevLock->GetLockId());
        UNIT_ASSERT(newLock->GetCounter() != prevLock->GetCounter()
            || newLock->GetGeneration() != prevLock->GetGeneration());
    }

    void TestChunkRead(ui32 chunkSize, ui32 rowCount, ui32 ranges = 1, ui32 limit = Max<ui32>()) {
        UpsertMany(1, rowCount);

        auto request = GetBaseReadRequest("table-1-many", 1, NKikimrTxDataShard::CELLVEC, TRowVersion::Max());
        request->Record.ClearSnapshot();

        ui32 base = 1;
        for (ui32 i = 0; i < ranges; ++i) {
            ui32 count = rowCount / ranges;
            if (i < (rowCount % ranges)) {
                ++count;
            }
            AddRangeQuery<ui32>(
                *request,
                {base, 1, 1},
                true,
                {base + count - 1, Max<ui32>(), Max<ui32>()},
                true
            );
            base += count;
        }

        request->Record.SetMaxRowsInResult(chunkSize);
        if (limit != Max<ui32>()) {
            request->Record.SetTotalRowsLimit(limit);
        }

        auto readResult = SendRead("table-1-many", request.release());
        UNIT_ASSERT(readResult);

        ui32 rowsRead = readResult->GetRowsCount();
        UNIT_ASSERT(rowsRead > 0);

        while (!readResult->Record.GetFinished()) {
            readResult = WaitReadResult();
            UNIT_ASSERT(readResult);
            ui32 count = readResult->GetRowsCount();
            UNIT_ASSERT_C(count > 0 || readResult->Record.GetFinished(), "Unexpected empty intermediate result");
            rowsRead += count;
        }

        UNIT_ASSERT_VALUES_EQUAL(rowsRead, Min(rowCount, limit));
    }

    struct THangedReturn {
        ui64 LastPlanStep = 0;
        TVector<THolder<IEventHandle>> ReadSets;
    };

    THangedReturn HangWithTransactionWaitingRS(ui64 shardCount, bool finalUpserts = true) {
        THangedReturn result;

        auto& runtime = *Server->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::MINIKQL_ENGINE, NActors::NLog::PRI_DEBUG);

        CreateTable(Server, Sender, "/Root", "table-2", false, shardCount);
        ExecSQL(Server, Sender, R"(
            UPSERT INTO `/Root/table-2`
            (key1, key2, key3, value)
            VALUES
            (1, 1, 1, 1000),
            (3, 3, 3, 3000),
            (5, 5, 5, 5000),
            (8, 0, 0, 8000),
            (8, 0, 1, 8010),
            (8, 1, 0, 8020),
            (8, 1, 1, 8030),
            (11, 11, 11, 11110);
        )");

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                Server->GetRuntime()->DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        bool capturePlanStep = true;
        bool dropRS = true;

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvTxProcessing::EvPlanStep: {
                    if (capturePlanStep) {
                        auto planMessage = event->Get<TEvTxProcessing::TEvPlanStep>();
                        result.LastPlanStep = planMessage->Record.GetStep();
                    }
                    break;
                }
                case TEvTxProcessing::EvReadSet: {
                    if (dropRS) {
                        result.ReadSets.push_back(std::move(event));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = Server->GetRuntime()->SetObserverFunc(captureEvents);

        capturePlanStep = true;

        // Send SQL request which should hang due to lost RS
        // We will capture its planstep
        SendSQL(
            Server,
            Sender,
            "UPSERT INTO `/Root/table-1` (key1, key2, key3, value) SELECT key1, key2, key3, value FROM `/Root/table-2`");

        waitFor([&]{ return result.LastPlanStep != 0; }, "intercepted TEvPlanStep");
        capturePlanStep = false;

        if (finalUpserts) {
            // With mvcc (or a better dependency tracking) the read below may start out-of-order,
            // because transactions above are stuck before performing any writes. Make sure it's
            // forced to wait for above transactions by commiting a write that is guaranteed
            // to "happen" after transactions above.
            SendSQL(Server, Sender, (R"(
                UPSERT INTO `/Root/table-1` (key1, key2, key3, value) VALUES (11, 11, 11, 11234);
                UPSERT INTO `/Root/table-2` (key1, key2, key3, value) VALUES (11, 11, 11, 112345);
            )"));
        }

        waitFor([&]{ return result.ReadSets.size() == 1; }, "intercepted RS");

        // restore original observer (note we used lambda function and stack variables)
        Server->GetRuntime()->SetObserverFunc(prevObserverFunc);

        return result;
    }

    NTabletPipe::TClientConfig GetTestPipeConfig() {
        auto config = GetPipeConfigWithRetries();
        if (WithFollower)
            config.ForceFollower = true;
        return config;
    }

    NKikimrTabletBase::TEvGetCountersResponse GetCounters(
        const TString& tableName,
        ui32 node = 0,
        TActorId sender = {})
    {
        if (!sender) {
            sender = Sender;
        }

        const auto& table = Tables[tableName];
        auto &runtime = *Server->GetRuntime();
        runtime.SendToPipe(
            table.TabletId,
            sender,
            new TEvTablet::TEvGetCounters,
            node,
            GetTestPipeConfig(),
            table.ClientId);

        auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>(sender);

        UNIT_ASSERT(ev);
        return ev->Get()->Record;
    }

    ui64 GetSimpleCounter(
        const TString& tableName,
        const TString& name,
        ui32 node = 0)
    {
        const auto counters = GetCounters(tableName, node);
        for (const auto& counter : counters.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (name != counter.GetName()) {
                continue;
            }

            return counter.GetValue();
        }

        UNIT_ASSERT_C(false, "Counter not found: " << name);
        return 0; // unreachable
    }

public:
    bool WithFollower = false;
    ui64 ShardCount = 1;
    Tests::TServer::TPtr Server;
    TActorId Sender;

    THashMap<TString, TTableInfo> Tables;
};

void TestReadKey(NKikimrTxDataShard::EScanDataFormat format, bool withFollower = false) {
    TTestHelper helper(withFollower);

    for (ui32 k: {1, 3, 5}) {
        auto request = helper.GetBaseReadRequest("table-1", 1, format);
        AddKeyQuery(*request, {k, k, k});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {{k, k, k, k * 100}});
    }
}

void TestReadRangeInclusiveEnds(NKikimrTxDataShard::EScanDataFormat format) {
    TTestHelper helper;

    auto request = helper.GetBaseReadRequest("table-1", 1, format);
    AddRangeQuery<ui32>(
        *request,
        {1, 1, 1},
        true,
        {5, 5, 5},
        true
    );

    auto readResult = helper.SendRead("table-1", request.release());
    CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
        {1, 1, 1, 100},
        {3, 3, 3, 300},
        {5, 5, 5, 500},
    });
}

void TestReadRangeMovies(NKikimrTxDataShard::EScanDataFormat format) {
    // test just to check if non-trivial type like string is properly replied
    TTestHelper helper;

    auto request = helper.GetBaseReadRequest("movies", 1, format);
    AddRangeQuery<ui32>(
        *request,
        {1},
        true,
        {100},
        true
    );

    TString s1 = "I Robot";
    TString s2 = "I Am Legend";
    TString s3 = "Hard die";

    auto readResult = helper.SendRead("movies", request.release());
    CheckResult(helper.Tables["movies"].UserTable, *readResult,
    {
        {TCell::Make(1u), TCell(s1.data(), s1.size()), TCell::Make(10u)},
        {TCell::Make(2u), TCell(s2.data(), s2.size()), TCell::Make(9u)},
        {TCell::Make(3u), TCell(s3.data(), s3.size()), TCell::Make(8u)}
    },
    {
        NScheme::TTypeIdOrder(NScheme::NTypeIds::Uint32),
        NScheme::TTypeIdOrder(NScheme::NTypeIds::String),
        NScheme::TTypeIdOrder(NScheme::NTypeIds::Uint32)
    });
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIterator) {
    Y_UNIT_TEST(ShouldReadKeyCellVec) {
        TestReadKey(NKikimrTxDataShard::CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadKeyArrow) {
        TestReadKey(NKikimrTxDataShard::ARROW);
    }

    Y_UNIT_TEST(ShouldReadRangeCellVec) {
        TestReadRangeMovies(NKikimrTxDataShard::CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadRangeArrow) {
        TestReadRangeMovies(NKikimrTxDataShard::ARROW);
    }

    Y_UNIT_TEST(ShouldReadKeyOnlyValueColumn) {
        TTestHelper helper;

        for (ui32 k: {1, 3, 5}) {
            auto request = helper.GetBaseReadRequest("table-1", 1);
            AddKeyQuery(*request, {k, k, k});
            request->Record.ClearColumns();

            const auto& description = helper.Tables["table-1"].UserTable.GetDescription();
            std::vector<ui32> keyColumns(
                description.GetKeyColumnIds().begin(),
                description.GetKeyColumnIds().end());

            for (const auto& column: description.GetColumns()) {
                auto it = std::find(keyColumns.begin(), keyColumns.end(), column.GetId());
                if (it != keyColumns.end())
                    continue;
                request->Record.AddColumns(column.GetId());
            }

            std::vector<NTable::TTag> columns(
                request->Record.GetColumns().begin(),
                request->Record.GetColumns().end());

            auto readResult = helper.SendRead("table-1", request.release());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {{k * 100}}, columns);
        }
    }

    Y_UNIT_TEST(ShouldReadKeyValueColumnAndSomeKeyColumn) {
        TTestHelper helper;

        for (ui32 k: {1, 3, 5}) {
            auto request = helper.GetBaseReadRequest("table-1", 1);
            AddKeyQuery(*request, {k, k, k});
            request->Record.ClearColumns();

            const auto& description = helper.Tables["table-1"].UserTable.GetDescription();
            std::vector<ui32> keyColumns(
                description.GetKeyColumnIds().begin(),
                description.GetKeyColumnIds().end());

            for (const auto& column: description.GetColumns()) {
                auto it = std::find(keyColumns.begin(), keyColumns.end(), column.GetId());
                if (it != keyColumns.end())
                    continue;
                request->Record.AddColumns(column.GetId());
            }

            request->Record.AddColumns(keyColumns[0]);

            std::vector<ui32> columns(
                request->Record.GetColumns().begin(),
                request->Record.GetColumns().end());

            auto readResult = helper.SendRead("table-1", request.release());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {{k * 100, k}}, columns);
        }
    }

    Y_UNIT_TEST(ShouldReadNoColumnsKeysRequestCellVec) {
        // KIKIMR-16897: no columns mean we want to calc row count
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::CELLVEC);
        request->Record.ClearColumns();
        AddKeyQuery(*request, {3, 3, 3});
        AddKeyQuery(*request, {1, 1, 1});
        AddKeyQuery(*request, {5, 5, 5});

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            std::vector<ui32>(),
            std::vector<ui32>(),
            std::vector<ui32>(),
        });
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 3UL);
    }

    Y_UNIT_TEST(ShouldReadNoColumnsKeysRequestArrow) {
        // KIKIMR-16897: no columns mean we want to calc row count
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW);
        request->Record.ClearColumns();
        AddKeyQuery(*request, {3, 3, 3});
        AddKeyQuery(*request, {1, 1, 1});
        AddKeyQuery(*request, {5, 5, 5});

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 3UL);
        UNIT_ASSERT(readResult->GetArrowBatch());

        auto batch = readResult->GetArrowBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3UL);
    }

    Y_UNIT_TEST(ShouldReadNoColumnsRangeRequestCellVec) {
        // KIKIMR-16897: no columns mean we want to calc row count
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::CELLVEC);
        request->Record.ClearColumns();
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            std::vector<ui32>(),
            std::vector<ui32>(),
            std::vector<ui32>(),
        });
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 3UL);
    }

    Y_UNIT_TEST(ShouldReadNoColumnsRangeRequestArrow) {
        // KIKIMR-16897: no columns mean we want to calc row count
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW);
        request->Record.ClearColumns();
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 3UL);
        UNIT_ASSERT(readResult->GetArrowBatch());

        auto batch = readResult->GetArrowBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3UL);
    }

    Y_UNIT_TEST(ShouldReadNonExistingKey) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request, {2, 2, 2});

        auto readResult = helper.SendRead("table-1", request.release());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
        });
    }

    Y_UNIT_TEST(ShouldReadMultipleKeys) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request, {3, 3, 3});
        AddKeyQuery(*request, {1, 1, 1});
        AddKeyQuery(*request, {5, 5, 5});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
            {1, 1, 1, 100},
            {5, 5, 5, 500},
        });
    }

    Y_UNIT_TEST(ShouldReverseReadMultipleKeys) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request, {3, 3, 3});
        AddKeyQuery(*request, {1, 1, 1});
        AddKeyQuery(*request, {5, 5, 5});
        request->Record.SetReverse(true);

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {5, 5, 5, 500},
            {1, 1, 1, 100},
            {3, 3, 3, 300},
        });
    }

    Y_UNIT_TEST(ShouldReadMultipleKeysOneByOne) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {3, 3, 3, 300}
        });

        const auto& record1 = readResult1->Record;
        UNIT_ASSERT(!record1.GetLimitReached());
        UNIT_ASSERT(record1.HasSeqNo());
        //UNIT_ASSERT(!record1.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record1.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record1.GetSeqNo(), 1UL);
        // TODO: check continuation token

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100}
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);
        // TODO: check continuation token

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {5, 5, 5, 500}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 2);

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(!record3.GetLimitReached());
        UNIT_ASSERT(record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);
        // TODO: check continuation token
    }

    Y_UNIT_TEST(ShouldReverseReadMultipleKeysOneByOne) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);
        request1->Record.SetReverse(true);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {5, 5, 5, 500}
        });

        const auto& record1 = readResult1->Record;
        UNIT_ASSERT(!record1.GetLimitReached());
        UNIT_ASSERT(record1.HasSeqNo());
        //UNIT_ASSERT(!record1.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record1.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record1.GetSeqNo(), 1UL);
        // TODO: check continuation token

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100}
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);
        // TODO: check continuation token

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {3, 3, 3, 300}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 2);

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(!record3.GetLimitReached());
        UNIT_ASSERT(record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);
        // TODO: check continuation token
    }

    Y_UNIT_TEST(ShouldHandleReadAck) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        for (size_t i = 0; i < 8; ++i) {
            AddKeyQuery(*request1, {1, 1, 1});
        }

        // limit quota
        request1->Record.SetMaxRows(1);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);

        helper.SendReadAck("table-1", readResult1->Record, 3, 10000);

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100},
            {1, 1, 1, 100},
            {1, 1, 1, 100}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 1);

        helper.SendReadAck("table-1", readResult2->Record, 100, 10000);

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {1, 1, 1, 100},
            {1, 1, 1, 100},
            {1, 1, 1, 100},
            {1, 1, 1, 100}
        });

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 2);
    }

    Y_UNIT_TEST(ShouldHandleOutOfOrderReadAck) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        for (size_t i = 0; i < 8; ++i) {
            AddKeyQuery(*request1, {1, 1, 1});
        }

        // limit quota
        request1->Record.SetMaxRows(3);
        request1->Record.SetMaxRowsInResult(1);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(!readResult1->Record.GetLimitReached());

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(!readResult2->Record.GetLimitReached());

        auto readResult3 = helper.WaitReadResult();
        UNIT_ASSERT(readResult3->Record.GetLimitReached()); // quota is empty now

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 2);

        helper.SendReadAck("table-1", readResult3->Record, 1, 10000);

        // since it's a test this one will be delivered the second and should be ignored
        helper.SendReadAck("table-1", readResult2->Record, 10, 10000);

        auto readResult4 = helper.WaitReadResult();
        UNIT_ASSERT(readResult4);
        UNIT_ASSERT(readResult4->Record.GetLimitReached()); // quota is empty now

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 3);

        auto readResult5 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult5);
        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 3);

        helper.SendReadAck("table-1", readResult4->Record, 1, 10000);
        auto readResult6 = helper.WaitReadResult();
        UNIT_ASSERT(readResult6);
        UNIT_ASSERT(readResult6->Record.GetLimitReached()); // quota is empty now
        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 4);
    }

    Y_UNIT_TEST(ShouldReverseReadMultipleRanges) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {1, 0, 0},
            true,
            {5, 5, 5},
            true
        );
        AddRangeQuery<ui32>(
            *request,
            {8, 1, 1},
            true,
            {11, 11, 11},
            true
        );

        request->Record.SetReverse(true);

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {11, 11, 11, 1111},
            {8, 1, 1, 803},
            {5, 5, 5, 500},
            {3, 3, 3, 300},
            {1, 1, 1, 100},
        });
    }

    Y_UNIT_TEST(ShouldReverseReadMultipleRangesOneByOneWithAcks) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {1, 0, 0},
            true,
            {5, 5, 5},
            true
        );
        AddRangeQuery<ui32>(
            *request,
            {8, 1, 1},
            true,
            {11, 11, 11},
            true
        );

        request->Record.SetReverse(true);
        request->Record.SetMaxRows(1);

        std::vector<std::vector<ui32>> gold = {
            {11, 11, 11, 1111},
            {8, 1, 1, 803},
            {5, 5, 5, 500},
            {3, 3, 3, 300},
            {1, 1, 1, 100},
        };

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            gold[0]
        });

        for (size_t i = 1; i < gold.size(); ++i) {
            helper.SendReadAck("table-1", readResult->Record, 1, 10000);
            readResult = helper.WaitReadResult();
            UNIT_ASSERT(readResult);
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                gold[i]
            });
        }
    }

    Y_UNIT_TEST(ShouldRangeReadReverseLeftInclusive) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetReverse(true);
        AddRangeQuery<ui32>(
            *request1,
            {8, 0, 0},
            true,
            {11, 11, 11},
            true
        );

        // limit quota (enough to read all rows)
        request1->Record.SetMaxRows(8);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(readResult1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->GetRowsCount(), 5);
        UNIT_ASSERT(readResult1->Record.GetFinished());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {11, 11, 11, 1111},
            {8, 1, 1, 803},
            {8, 1, 0, 802},
            {8, 0, 1, 801},
            {8, 0, 0, 800}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);
    }

    Y_UNIT_TEST(ShouldRangeReadReverseLeftNonInclusive) {
        // Regression test for KIKIMR-17253
        // Version with no ACK: only reverse and left not inclusive like in ReadContinue

        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetReverse(true);
        AddRangeQuery<ui32>(
            *request1,
            {8, 0, 0},
            false,
            {11, 11, 11},
            true
        );

        // limit quota (enough to read all rows)
        request1->Record.SetMaxRows(8);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(readResult1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->GetRowsCount(), 4);
        UNIT_ASSERT(readResult1->Record.GetFinished());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {11, 11, 11, 1111},
            {8, 1, 1, 803},
            {8, 1, 0, 802},
            {8, 0, 1, 801},
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);
    }

    Y_UNIT_TEST(ShouldHandleReadAckWhenExhaustedRangeRead) {
        // Regression test for KIKIMR-17253

        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {11, 11, 11},
            true
        );

        // limit quota
        request1->Record.SetMaxRows(5);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(readResult1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->GetRowsCount(), 5);
        UNIT_ASSERT(!readResult1->Record.GetFinished());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
            {8, 0, 0, 800},
            {8, 0, 1, 801},
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);

        helper.SendReadAck("table-1", readResult1->Record, 8, 10000);

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(readResult2);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->GetRowsCount(), 3);
        UNIT_ASSERT(readResult2->Record.GetFinished());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {8, 1, 0, 802},
            {8, 1, 1, 803},
            {11, 11, 11, 1111}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 1);
    }

    Y_UNIT_TEST(ShouldHandleReadAckWhenExhaustedRangeReadReverse) {
        // Regression test for KIKIMR-17253

        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetReverse(true);
        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {11, 11, 11},
            true
        );

        // limit quota
        request1->Record.SetMaxRows(5);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(readResult1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->GetRowsCount(), 5);
        UNIT_ASSERT(!readResult1->Record.GetFinished());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {11, 11, 11, 1111},
            {8, 1, 1, 803},
            {8, 1, 0, 802},
            {8, 0, 1, 801},
            {8, 0, 0, 800}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);

        helper.SendReadAck("table-1", readResult1->Record, 8, 10000);

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(readResult2);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->GetRowsCount(), 3);
        UNIT_ASSERT(readResult2->Record.GetFinished());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {5, 5, 5, 500},
            {3, 3, 3, 300},
            {1, 1, 1, 100}
        });

        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 1);
    }

    Y_UNIT_TEST(ShouldNotReadAfterCancel) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        for (size_t i = 0; i < 8; ++i) {
            AddKeyQuery(*request1, {1, 1, 1});
        }

        // limit quota
        request1->Record.SetMaxRows(1);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100}
        });

        helper.SendCancel("table-1", 1);
        helper.SendReadAck("table-1", readResult1->Record, 3, 10000);

        auto readResult2 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult2);
        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);
    }

    Y_UNIT_TEST(ShouldForbidDuplicatedReadId) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRows(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        auto request2 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request2, {3, 3, 3});
        auto readResult2 = helper.SendRead("table-1", request2.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetStatus().GetCode(), Ydb::StatusIds::ALREADY_EXISTS);
    }

    Y_UNIT_TEST(ShouldReadRangeInclusiveEndsCellVec) {
        TestReadRangeInclusiveEnds(NKikimrTxDataShard::CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadRangeInclusiveEndsArrow) {
        TestReadRangeInclusiveEnds(NKikimrTxDataShard::ARROW);
    }

    Y_UNIT_TEST(ShouldReadRangeReverse) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        request->Record.SetReverse(true);
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {5, 5, 5, 500},
            {3, 3, 3, 300},
            {1, 1, 1, 100},
        });
    }

    Y_UNIT_TEST(ShouldReadRangeInclusiveEndsMissingLeftRight) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {2, 2, 2},
            true,
            {7, 7, 7},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });
    }

    Y_UNIT_TEST(ShouldReadRangeNonInclusiveEnds) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            false,
            {5, 5, 5},
            false
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
        });
    }

    Y_UNIT_TEST(ShouldReadRangeLeftInclusive) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            true,
            {5, 5, 5},
            false
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
        });
    }

    Y_UNIT_TEST(ShouldReadRangeRightInclusive) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {1, 1, 1},
            false,
            {5, 5, 5},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });
    }

    Y_UNIT_TEST(ShouldReadNotExistingRange) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request,
            {100, 1, 1},
            true,
            {200, 5, 5},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
        });
    }

    Y_UNIT_TEST(ShouldReadRangeOneByOne) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );
        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {1, 1, 1},
            true
        );

        request1->Record.SetMaxRowsInResult(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
        });

        const auto& record1 = readResult1->Record;
        UNIT_ASSERT(!record1.GetLimitReached());
        UNIT_ASSERT(record1.HasSeqNo());
        UNIT_ASSERT(!record1.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record1.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record1.GetSeqNo(), 1UL);

        // TODO: check continuation token
 #if 0
        UNIT_ASSERT_VALUES_EQUAL(readResult1.GetFirstUnprocessedQuery(), 0UL);

        UNIT_ASSERT(readResult1.HasLastProcessedKey());
        TOwnedCellVec lastKey1(
            TSerializedCellVec(readResult1.GetLastProcessedKey()).GetCells());
        CheckRow(lastKey1, {1, 1, 1});
#endif

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {3, 3, 3, 300},
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);

        // TODO: check continuation token
#if 0
        UNIT_ASSERT_VALUES_EQUAL(readResult2.GetFirstUnprocessedQuery(), 0UL);

        UNIT_ASSERT(readResult2.HasLastProcessedKey());
        TOwnedCellVec lastKey2(
            TSerializedCellVec(readResult2.GetLastProcessedKey()).GetCells());
        CheckRow(lastKey2, {3, 3, 3});
#endif

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {5, 5, 5, 500}
        });

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(!record3.GetLimitReached());
        UNIT_ASSERT(!record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);

        // TODO: check continuation token
#if 0
        UNIT_ASSERT_VALUES_EQUAL(readResult3.GetFirstUnprocessedQuery(), 1UL);
        UNIT_ASSERT(!readResult3.HasLastProcessedKey());
#endif

        auto readResult4 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult4, {
            {1, 1, 1, 100}
        });

        const auto& record4 = readResult4->Record;
        UNIT_ASSERT(!record4.GetLimitReached());
        UNIT_ASSERT(!record4.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record4.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record4.GetSeqNo(), 4UL);
        // TODO: check continuation token

        auto readResult5 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult5, {
        });

        const auto& record5 = readResult5->Record;
        UNIT_ASSERT(!record5.GetLimitReached());
        UNIT_ASSERT(record5.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record5.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record5.GetSeqNo(), 5UL);
        // TODO: check no continuation token
    }

    Y_UNIT_TEST(ShouldReadRangeChunk1_100) {
        TTestHelper helper;
        helper.TestChunkRead(1, 100);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk1) {
        TTestHelper helper;
        helper.TestChunkRead(1, 1000);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk2) {
        TTestHelper helper;
        helper.TestChunkRead(2, 1000);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk3) {
        TTestHelper helper;
        helper.TestChunkRead(3, 1000);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk5) {
        TTestHelper helper;
        helper.TestChunkRead(5, 1000);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk7) {
        TTestHelper helper;
        helper.TestChunkRead(7, 1000);
    }

    Y_UNIT_TEST(ShouldReadRangeChunk100) {
        TTestHelper helper;
        helper.TestChunkRead(99, 10000);
    }

    Y_UNIT_TEST(ShouldLimitReadRangeChunk1Limit100) {
        TTestHelper helper;
        helper.TestChunkRead(1, 1000, 1, 100);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit98) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 98);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit99) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 99);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit100) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 100);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit101) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 101);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit198) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 198);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk99Limit900) {
        TTestHelper helper;
        helper.TestChunkRead(99, 1000, 10, 900);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk100Limit900) {
        TTestHelper helper;
        helper.TestChunkRead(100, 1000, 10, 900);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk100Limit1000) {
        TTestHelper helper;
        helper.TestChunkRead(100, 1000, 10, 1000);
    }

    Y_UNIT_TEST(ShouldLimitRead10RangesChunk100Limit1001) {
        TTestHelper helper;
        helper.TestChunkRead(100, 1000, 10, 1001);
    }

    Y_UNIT_TEST(ShouldReadKeyPrefix1) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddKeyQuery(*request, {8});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803}
        });
    }

    Y_UNIT_TEST(ShouldReadKeyPrefix2) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddKeyQuery(*request, {8, 0});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
        });
    }

    Y_UNIT_TEST(ShouldReadKeyPrefix3) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddKeyQuery(*request, {8, 1, 0});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 1, 0, 802},
        });
    }

    Y_UNIT_TEST(ShouldReadRangePrefix1) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddRangeQuery<ui32>(
            *request,
            {8},
            true,
            {9},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803}
        });
    }

    Y_UNIT_TEST(ShouldReadRangePrefix2) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddRangeQuery<ui32>(
            *request,
            {8},
            true,
            {9},
            false
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803}
        });
    }

    Y_UNIT_TEST(ShouldReadRangePrefix3) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddRangeQuery<ui32>(
            *request,
            {8},
            true,
            {8},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803}
        });
    }

    Y_UNIT_TEST(ShouldReadRangePrefix4) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddRangeQuery<ui32>(
            *request,
            {8},
            true,
            {8},
            false
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {});
    }

    Y_UNIT_TEST(ShouldReadRangePrefix5) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);

        AddRangeQuery<ui32>(
            *request,
            {8, 1},
            true,
            {9},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {8, 1, 0, 802},
            {8, 1, 1, 803}
        });
    }

    Y_UNIT_TEST(ShouldFailUknownColumns) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request, {1, 1, 1});

        request->Record.AddColumns(0xDEADBEAF);

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ShouldFailWrongSchema) {
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request, {1, 1, 1});

        request->Record.MutableTableId()->SetSchemaVersion(0xDEADBEAF);

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ShouldFailReadNextAfterSchemeChange) {
        TTestHelper helper;

        bool shouldDrop = true;
        TAutoPtr<IEventHandle> continueEvent;

        // capture original observer func by setting dummy one
        auto& runtime = *helper.Server->GetRuntime();

        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue: {
                if (shouldDrop) {
                    continueEvent = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            default:
                return originalObserver(runtime, ev);
            }
        });

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});

        request1->Record.SetMaxRowsInResult(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        auto txId = AsyncAlterAddExtraColumn(helper.Server, "/Root", "table-1");
        WaitTxNotification(helper.Server, helper.Sender, txId);

        // now allow to continue read
        shouldDrop = false;
        TAutoPtr<TEvDataShard::TEvReadContinue> request = continueEvent->Release<TEvDataShard::TEvReadContinue>();
        UNIT_ASSERT_VALUES_EQUAL(request->ReadId, 1UL);

        const auto& table = helper.Tables["table-1"];
        runtime.SendToPipe(
            table.TabletId,
            helper.Sender,
            request.Release(),
            0,
            GetPipeConfigWithRetries(),
            table.ClientId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvDataShard::EvReadContinue, 1);
        runtime.DispatchEvents(options);

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(readResult2);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetSeqNo(), readResult1->Record.GetSeqNo() + 1);
    }

    Y_UNIT_TEST(ShouldFailReadNextAfterSchemeChangeExhausted) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        request1->Record.SetMaxRows(1); // will wait for ack

        auto readResult1 = helper.SendRead("table-1", request1.release());

        auto txId = AsyncAlterAddExtraColumn(helper.Server, "/Root", "table-1");
        WaitTxNotification(helper.Server, helper.Sender, txId);

        helper.SendReadAck("table-1", readResult1->Record, 3, 10000);

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT(readResult2->Record.HasReadId());
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetReadId(), readResult1->Record.GetReadId());

        // try to make one more read using this iterator
        helper.SendReadAck("table-1", readResult1->Record, 3, 10000);
        auto readResult3 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult3);
    }

    Y_UNIT_TEST(ShouldReceiveErrorAfterSplit) {
        TTestHelper helper;

        bool shouldDrop = true;
        TAutoPtr<IEventHandle> continueEvent;

        // capture original observer func by setting dummy one
        auto& runtime = *helper.Server->GetRuntime();

        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue: {
                if (shouldDrop) {
                    continueEvent = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            default:
                return originalObserver(runtime, ev);
            }
        });

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});

        request1->Record.SetMaxRowsInResult(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(continueEvent);

        helper.SplitTable1();

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(readResult2);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetSeqNo(), readResult1->Record.GetSeqNo() + 1);

        // now allow to continue read and check we don't get extra read result with error
        shouldDrop = false;
        TAutoPtr<TEvDataShard::TEvReadContinue> request = continueEvent->Release<TEvDataShard::TEvReadContinue>();
        UNIT_ASSERT_VALUES_EQUAL(request->ReadId, 1UL);

        const auto& table = helper.Tables["table-1"];
        runtime.SendToPipe(
            table.TabletId,
            helper.Sender,
            request.Release(),
            0,
            GetPipeConfigWithRetries(),
            table.ClientId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvDataShard::EvReadContinue, 1);
        runtime.DispatchEvents(options);

        auto readResult3 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult3);
    }

    Y_UNIT_TEST(ShouldReceiveErrorAfterSplitWhenExhausted) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});

        // set quota so that DS hangs waiting for ACK
        request1->Record.SetMaxRows(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        helper.SplitTable1();

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT(readResult2);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.GetSeqNo(), readResult1->Record.GetSeqNo() + 1);
    }

    Y_UNIT_TEST(NoErrorOnFinalACK) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});

        auto readResult1 = helper.SendRead("table-1", request1.release());
        UNIT_ASSERT(readResult1);
        UNIT_ASSERT(readResult1->Record.GetFinished());

        helper.SendReadAck("table-1", readResult1->Record, 300, 10000);

        auto readResult2 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult2);
    }

    Y_UNIT_TEST(ShouldReadFromFollower) {
        TestReadKey(NKikimrTxDataShard::CELLVEC, true);
    }

    Y_UNIT_TEST(ShouldNotReadMvccFromFollower) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount, true);

        TRowVersion someVersion = TRowVersion(10000, Max<ui64>());
        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, someVersion);
        AddKeyQuery(*request, {3, 3, 3});
        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::UNSUPPORTED);
    }

    Y_UNIT_TEST(ShouldReadHeadFromFollower) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount, true);

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, TRowVersion::Max());
        request->Record.ClearSnapshot();
        AddKeyQuery(*request, {3, 3, 3});
        auto readResult = helper.SendRead("table-1", request.release());

        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
        });
    }

    Y_UNIT_TEST(ShouldStopWhenNodeDisconnected) {
        const ui32 nodeCount = 2;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(nodeCount);


        TTestHelper helper(serverSettings);
        auto &runtime = *helper.Server->GetRuntime();

        ui32 node = 0;

        ui32 continueCounter = 0;
        bool connectedFromDifferentNode = false;
        ui32 serverConnectedCount = 0;
        runtime.SetObserverFunc([&continueCounter, &connectedFromDifferentNode, &serverConnectedCount](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue:
                ++continueCounter;
                break;
            case TEvTabletPipe::EvServerConnected: {
                auto* typedEvent = dynamic_cast<TEvTabletPipe::TEvServerConnected*>(ev->GetBase());
                ++serverConnectedCount;
                if (typedEvent->ClientId.NodeId() != typedEvent->ServerId.NodeId()) {
                    connectedFromDifferentNode = true;
                }
                break;
            }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                helper.Server->GetRuntime()->DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        auto& table = helper.Tables["table-1"];

        auto sender = runtime.AllocateEdgeActor(node);

        // we need to connect from another node
        table.ClientId = runtime.ConnectToPipe(table.TabletId, sender, node, GetPipeConfigWithRetries());
        UNIT_ASSERT(table.ClientId);

        waitFor([&]{ return serverConnectedCount != 0; }, "intercepted EvServerConnected");
        if (!connectedFromDifferentNode) {
            ++node;
            table.ClientId = runtime.ConnectToPipe(table.TabletId, sender, node, GetPipeConfigWithRetries());
            UNIT_ASSERT(table.ClientId);
        }
        UNIT_ASSERT(connectedFromDifferentNode);

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});

        request1->Record.SetMaxRows(1); // set quota so that DS hangs waiting for ACK

        auto readResult1 = helper.SendRead("table-1", request1.release(), node, sender);

        auto exhaustedCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsExhaustedCount", node);
        auto iteratorsCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount", node);
        UNIT_ASSERT_VALUES_EQUAL(exhaustedCount, 1UL);
        UNIT_ASSERT_VALUES_EQUAL(iteratorsCount, 1UL);

        runtime.DisconnectNodes(0, 1);
        table.ClientId = runtime.ConnectToPipe(table.TabletId, sender, node, GetPipeConfigWithRetries());

        exhaustedCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsExhaustedCount", 0);
        while (exhaustedCount != 0) {
            SimulateSleep(helper.Server, TDuration::Seconds(1));
            exhaustedCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsExhaustedCount", 0);
        }

        iteratorsCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount", node);
        UNIT_ASSERT_VALUES_EQUAL(iteratorsCount, 0UL);
    }

    Y_UNIT_TEST(ShouldReadFromHead) {
        // read from HEAD when there is no conflicting operation
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, TRowVersion::Max());
        request->Record.ClearSnapshot();
        AddKeyQuery(*request, {3, 3, 3});

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        UNIT_ASSERT(!readResult->Record.HasSnapshot());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            {3, 3, 3, 300},
        });
    }

    Y_UNIT_TEST(ShouldReadFromHeadWithConflict) {
        // Similar to ShouldReadFromHead, but there is conflicting hanged operation.
        // We will read all at once thus should not block

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount);

        auto hangedInfo = helper.HangWithTransactionWaitingRS(shardCount, false);

        {
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, TRowVersion::Max());
            request->Record.ClearSnapshot();
            AddKeyQuery(*request, {3, 3, 3});
            AddKeyQuery(*request, {1, 1, 1});
            AddKeyQuery(*request, {5, 5, 5});

            auto readResult = helper.SendRead(
                "table-1",
                request.release(),
                0,
                helper.Sender,
                TDuration::MilliSeconds(100));
            UNIT_ASSERT(readResult); // read is not blocked by conflicts!
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            UNIT_ASSERT(!record.HasSnapshot());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 300},
                {1, 1, 1, 100},
                {5, 5, 5, 500}
            });
        }

        // Don't catch RS any more and send caught ones to proceed with upserts.
        auto& runtime = *helper.Server->GetRuntime();
        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        for (auto &rs : hangedInfo.ReadSets)
            runtime.Send(rs.Release());

        // Wait for upsert to finish.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsTxResultComplete(), 1);
            runtime.DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(ShouldReadFromHeadToMvccWithConflict) {
        // Similar to ShouldProperlyOrderConflictingTransactionsMvcc, but we read HEAD
        //
        // In this test HEAD read waits conflicting transaction: first time we read from HEAD and
        // notice that result it not full. Then restart after conflicting operation finishes

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount);

        auto hangedInfo = helper.HangWithTransactionWaitingRS(shardCount, false);

        {
            // now read HEAD
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, TRowVersion::Max());
            request->Record.ClearSnapshot();
            AddKeyQuery(*request, {3, 3, 3});
            AddKeyQuery(*request, {1, 1, 1});
            AddKeyQuery(*request, {3, 3, 3});
            AddKeyQuery(*request, {1, 1, 1});
            AddKeyQuery(*request, {5, 5, 5});
            AddKeyQuery(*request, {11, 11, 11});

            // intentionally 2: we check that between Read restart Reader's state is reset.
            // Because of implementation we always read 1
            request->Record.SetMaxRowsInResult(2);

            auto readResult = helper.SendRead(
                "table-1",
                request.release(),
                0,
                helper.Sender,
                TDuration::MilliSeconds(100));
            UNIT_ASSERT(!readResult); // read is blocked by conflicts
        }

        // Don't catch RS any more and send caught ones to proceed with upserts.
        auto& runtime = *helper.Server->GetRuntime();
        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        for (auto &rs : hangedInfo.ReadSets)
            runtime.Send(rs.Release());

        // Wait for upsert to finish.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsTxResultComplete(), 1);
            runtime.DispatchEvents(options);
        }

        {
            // get1
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(!record.HasFinished());
            UNIT_ASSERT(record.HasSnapshot());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 3000},
                {1, 1, 1, 1000}
            });
        }

        {
            // get2
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(!record.HasFinished());
            UNIT_ASSERT(record.HasSnapshot());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 3000},
                {1, 1, 1, 1000}
            });
        }

        {
            // get3
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            UNIT_ASSERT(record.HasSnapshot());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {5, 5, 5, 5000},
                {11, 11, 11, 11110}
            });
        }
    }

    Y_UNIT_TEST(ShouldProperlyOrderConflictingTransactionsMvcc) {
        // 1. Start read-write multishard transaction: readset will be blocked
        // to hang transaction. Write is the key we want to read.
        // 2a. Check that we can read prior blocked step.
        // 2b. Do MVCC read of the key, which hanging transaction tries to write. MVCC must wait
        // for the hanging transaction.
        // 3. Finish hanging write.
        // 4. MVCC read must finish, do another MVCC read of same version for sanity check
        // that read is repeatable.
        // 5. Read prior data again

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount);

        auto hangedInfo = helper.HangWithTransactionWaitingRS(shardCount);
        auto hangedStep = hangedInfo.LastPlanStep;

        // 2a: read prior data
        {
            auto oldVersion = TRowVersion(hangedStep - 1, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, oldVersion);
            AddKeyQuery(*request, {3, 3, 3});

            auto readResult = helper.SendRead("table-1", request.release());
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 300}
            });
        }

        // 2b-1 (key): try to read hanged step, note that we have hanged write to the same key
        {
            auto oldVersion = TRowVersion(hangedStep, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, oldVersion);
            AddKeyQuery(*request, {3, 3, 3});

            auto readResult = helper.SendRead(
                "table-1",
                request.release(),
                0,
                helper.Sender,
                TDuration::MilliSeconds(100));
            UNIT_ASSERT(!readResult); // read is blocked by conflicts
        }

        // 2b-2 (range): try to read hanged step, note that we have hanged write to the same key
        {
            auto oldVersion = TRowVersion(hangedStep, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 2, NKikimrTxDataShard::ARROW, oldVersion);

            AddRangeQuery<ui32>(
                *request,
                {1, 1, 1},
                true,
                {5, 5, 5},
                true
            );

            auto readResult = helper.SendRead(
                "table-1",
                request.release(),
                0,
                helper.Sender,
                TDuration::MilliSeconds(100));
            UNIT_ASSERT(!readResult); // read is blocked by conflicts
        }

        // 2b-3 (key prefix, equals to range): try to read hanged step, note that we have hanged write to the same key
        {
            auto oldVersion = TRowVersion(hangedStep, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 3, NKikimrTxDataShard::ARROW, oldVersion);
            AddKeyQuery(*request, {3});

            auto readResult = helper.SendRead(
                "table-1",
                request.release(),
                0,
                helper.Sender,
                TDuration::MilliSeconds(100));
            UNIT_ASSERT(!readResult); // read is blocked by conflicts
        }

        // 3. Don't catch RS any more and send caught ones to proceed with upserts.
        auto& runtime = *helper.Server->GetRuntime();
        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        for (auto &rs : hangedInfo.ReadSets)
            runtime.Send(rs.Release());

        // Wait for upserts and immediate tx to finish.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsTxResultComplete(), 3);
            runtime.DispatchEvents(options);
        }

        // read 2b-1 should finish now
        {
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 3000}
            });
        }

        // read 2b-2 should finish now
        {
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {1, 1, 1, 1000},
                {3, 3, 3, 3000},
                {5, 5, 5, 5000}
            });
        }

        // read 2b-3 should finish now
        {
            auto readResult = helper.WaitReadResult();
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 3000}
            });
        }

        // 4: try to read hanged step again
        {
            auto oldVersion = TRowVersion(hangedStep, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 4, NKikimrTxDataShard::ARROW, oldVersion);
            AddKeyQuery(*request, {3, 3, 3});

            auto readResult = helper.SendRead("table-1", request.release());
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 3000}
            });
        }

        // 5: read prior data again
        {
            auto oldVersion = TRowVersion(hangedStep - 1, Max<ui64>());
            auto request = helper.GetBaseReadRequest("table-1", 5, NKikimrTxDataShard::ARROW, oldVersion);
            AddKeyQuery(*request, {3, 3, 3});

            auto readResult = helper.SendRead("table-1", request.release());
            const auto& record = readResult->Record;
            UNIT_ASSERT(record.HasFinished());
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                {3, 3, 3, 300}
            });
        }
    }

    Y_UNIT_TEST(ShouldReturnMvccSnapshotFromFuture) {
        // checks that when snapshot is in future, we wait for it

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TTestHelper helper(serverSettings);

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                helper.Server->GetRuntime()->DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        bool captureTimecast = false;
        bool captureWaitNotify = false;

        TRowVersion snapshot = TRowVersion::Min();
        ui64 lastStep = 0;
        ui64 waitPlanStep = 0;
        ui64 notifyPlanStep = 0;

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvMediatorTimecast::EvUpdate: {
                    if (captureTimecast) {
                        auto update = event->Get<TEvMediatorTimecast::TEvUpdate>();
                        lastStep = update->Record.GetTimeBarrier();
                        Cerr << "---- dropped EvUpdate ----" << Endl;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvWaitPlanStep: {
                    if (captureWaitNotify) {
                        auto waitEvent = event->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                        waitPlanStep = waitEvent->PlanStep;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvNotifyPlanStep: {
                    if (captureWaitNotify) {
                        auto notifyEvent = event->Get<TEvMediatorTimecast::TEvNotifyPlanStep>();
                        notifyPlanStep = notifyEvent->PlanStep;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = helper.Server->GetRuntime()->SetObserverFunc(captureEvents);

        // check transaction waits for proper plan step
        captureTimecast = true;

        // note that we need this to capture snapshot version
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (3, 3, 3, 300);
        )");

        waitFor([&]{ return lastStep != 0; }, "intercepted TEvUpdate");

        captureTimecast = false;
        captureWaitNotify = true;

        // future snapshot
        snapshot = TRowVersion(lastStep + 1000, Max<ui64>());

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, snapshot);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        waitFor([&]{ return notifyPlanStep != 0; }, "intercepted TEvNotifyPlanStep");
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(notifyPlanStep, snapshot.Step);

        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {3, 3, 3, 300}
        });

        const auto& record1 = readResult1->Record;
        UNIT_ASSERT(!record1.GetLimitReached());
        UNIT_ASSERT(record1.HasSeqNo());
        UNIT_ASSERT(!record1.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record1.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record1.GetSeqNo(), 1UL);

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100}
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {5, 5, 5, 500}
        });

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(!record3.GetLimitReached());
        UNIT_ASSERT(record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);
    }

    Y_UNIT_TEST(ShouldCancelMvccSnapshotFromFuture) {
        // checks that when snapshot is in the future, we can cancel it

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TTestHelper helper(serverSettings);

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                helper.Server->GetRuntime()->DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        bool captureTimecast = false;
        bool captureWaitNotify = false;

        TRowVersion snapshot = TRowVersion::Min();
        ui64 lastStep = 0;
        ui64 waitPlanStep = 0;
        ui64 notifyPlanStep = 0;
        size_t readResults = 0;

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvMediatorTimecast::EvUpdate: {
                    if (captureTimecast) {
                        auto update = event->Get<TEvMediatorTimecast::TEvUpdate>();
                        lastStep = update->Record.GetTimeBarrier();
                        Cerr << "---- dropped EvUpdate ----" << Endl;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvWaitPlanStep: {
                    if (captureWaitNotify) {
                        auto waitEvent = event->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                        waitPlanStep = waitEvent->PlanStep;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvNotifyPlanStep: {
                    if (captureWaitNotify) {
                        auto notifyEvent = event->Get<TEvMediatorTimecast::TEvNotifyPlanStep>();
                        notifyPlanStep = notifyEvent->PlanStep;
                    }
                    break;
                }
                case TEvDataShard::EvReadResult: {
                    ++readResults;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = helper.Server->GetRuntime()->SetObserverFunc(captureEvents);

        // check transaction waits for proper plan step
        captureTimecast = true;

        // note that we need this to capture snapshot version
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (3, 3, 3, 300);
        )");

        waitFor([&]{ return lastStep != 0; }, "intercepted TEvUpdate");

        captureTimecast = false;
        captureWaitNotify = true;

        // future snapshot
        snapshot = TRowVersion(lastStep + 1000, Max<ui64>());

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, snapshot);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);

        helper.SendReadAsync("table-1", request1.release());

        waitFor([&]{ return waitPlanStep != 0; }, "intercepted TEvWaitPlanStep");
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(notifyPlanStep, 0);

        helper.SendCancel("table-1", 1);

        waitFor([&]{ return notifyPlanStep != 0; }, "intercepted TEvNotifyPlanStep");
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(notifyPlanStep, snapshot.Step);

        SimulateSleep(helper.Server, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(readResults, 0);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKey) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {1, 1, 1});

        auto readResult1 = helper.SendRead("table-1", request1.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);

        // breaks lock obtained above
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (1, 1, 1, 101);
        )");

        // we use request2 to obtain same lock as in request1 to check it
        auto request2 = helper.GetBaseReadRequest("table-1", 1);
        request2->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request2, {1, 1, 1});

        auto readResult2 = helper.SendRead("table-1", request2.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRange) {
        // upsert into "left border -1 " and to the "right border + 1" - lock not broken
        // upsert inside range - broken
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(
            *request1,
            {3, 3, 3},
            true,
            {8, 0, 1},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());

        {
            // upsert to the left and check that lock is not broken
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/table-1`
                (key1, key2, key3, value)
                VALUES
                (1, 1, 1, 101);
            )");

            helper.CheckLockValid("table-1", 2, {11, 11, 11}, lockTxId);
        }

        {
            // upsert to the right and check that lock is not broken
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/table-1`
                (key1, key2, key3, value)
                VALUES
                (8, 1, 0, 802);
            )");

            helper.CheckLockValid("table-1", 2, {11, 11, 11}, lockTxId);
        }

        // breaks lock
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (4, 4, 4, 400);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips) {
        // If we read in v1, write in v2, then write breaks lock.
        // Because of out of order execution, v2 can happen before v1
        // and we should properly handle it in DS to break lock.
        // Similar to ShouldReturnBrokenLockWhenReadKeyWithContinueInvisibleRowSkips,
        // but lock is broken during the first iteration.

        TTestHelper helper;

        auto readVersion = CreateVolatileSnapshot(
            helper.Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

        // write new data above snapshot
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (4, 4, 4, 4444);
        )");

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);

        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });

        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 1);

        helper.CheckLockBroken("table-1", 10, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips2) {
        // Almost the same as ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips:
        // 1. tx1: read some **non-existing** range1
        // 2. tx2: upsert into range2 > range1 range and commit.
        // 3. tx1: read range2 -> lock should be broken

        TTestHelper helper;

        auto readVersion = CreateVolatileSnapshot(
            helper.Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(
            *request1,
            {100, 0, 0},
            true,
            {200, 0, 0},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {});
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);

        // write new data above snapshot
        ExecSQL(helper.Server, helper.Sender, R"(
            SELECT * FROM `/Root/table-1` WHERE key1 == 300;
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (300, 0, 0, 3000);
        )");

        auto request2 = helper.GetBaseReadRequest("table-1", 2, NKikimrTxDataShard::ARROW, readVersion);
        request2->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(
            *request2,
            {300, 0, 0},
            true,
            {300, 0, 0},
            true
        );

        auto readResult2 = helper.SendRead("table-1", request2.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1);
        helper.CheckLockBroken("table-1", 10, {300, 0, 0}, lockTxId, *readResult2);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRangeLeftBorder) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(
            *request1,
            {3, 3, 3},
            true,
            {8, 0, 1},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());

        // breaks lock
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (3, 3, 3, 0xdead);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRangeRightBorder) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(
            *request1,
            {3, 3, 3},
            true,
            {8, 0, 1},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());

        // breaks lock
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (8, 0, 1, 0xdead);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKeyPrefix) {
        // upsert into "left border -1 " and to the "right border + 1" - lock not broken
        // upsert inside range - broken
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead("table-1", request1.release());

        {
            // upsert to the left and check that lock is not broken
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/table-1`
                (key1, key2, key3, value)
                VALUES
                (5, 5, 5, 555);
            )");

            helper.CheckLockValid("table-1", 2, {11, 11, 11}, lockTxId);
        }

        {
            // upsert to the right and check that lock is not broken
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/table-1`
                (key1, key2, key3, value)
                VALUES
                (9, 0, 0, 900);
            )");

            helper.CheckLockValid("table-1", 2, {11, 11, 11}, lockTxId);
        }

        // breaks lock obtained above
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (8, 1, 1, 8000);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKeyPrefixLeftBorder) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead("table-1", request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (8, 0, 0, 8000);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKeyPrefixRightBorder) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead("table-1", request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (8, 1, 1, 8000);
        )");

        helper.CheckLockBroken("table-1", 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKeyWithContinue) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRows(1);
        request1->Record.SetLockTxId(lockTxId);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (1, 1, 1, 1000);
        )");

        helper.SendReadAck("table-1", readResult1->Record, 3, 10000);
        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1UL);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadKeyWithContinueInvisibleRowSkips) {
        // If we read in v1, write in v2, then write breaks lock.
        // Because of out of order execution, v2 can happen before v1
        // and we should properly handle it in DS to break lock.

        TTestHelper helper;

        auto readVersion = CreateVolatileSnapshot(
            helper.Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

        // write new data above snapshot
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-1`
            (key1, key2, key3, value)
            VALUES
            (4, 4, 4, 4444);
        )");

        const ui64 lockTxId = 1011121314;

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrTxDataShard::ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);
        request1->Record.SetMaxRows(1); // set quota so that DS hangs waiting for ACK

        AddRangeQuery<ui32>(
            *request1,
            {1, 1, 1},
            true,
            {5, 5, 5},
            true
        );

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
        });

        // we had read only key=1, so didn't see invisible key=4
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);

        helper.SendReadAck("table-1", readResult1->Record, 100, 10000);
        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });

        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1UL);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());

        helper.CheckLockBroken("table-1", 10, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(HandlePersistentSnapshotGoneInContinue) {
        // TODO
    }

    Y_UNIT_TEST(HandleMvccGoneInContinue) {
        // TODO
    }
};

Y_UNIT_TEST_SUITE(DataShardReadIteratorSysTables) {
    Y_UNIT_TEST(ShouldRead) {
        TTestHelper helper;

        auto request = helper.GetUserTablesRequest("table-1", 2, 1);
        AddRangeQuery<ui64>(
            *request,
            {Min<ui64>(),},
            true,
            {Max<ui64>(),},
            true
        );

        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 1UL);

        const auto& cells = readResult->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells[0].AsValue<ui64>(), helper.Tables["table-1"].UserTable.GetPathId());
    }

    Y_UNIT_TEST(ShouldNotReadUserTableUsingLocalTid) {
        TTestHelper helper;

        auto request = helper.GetUserTablesRequest("table-1", 2, 1);
        AddRangeQuery<ui64>(
            *request,
            {Min<ui64>(),},
            true,
            {Max<ui64>(),},
            true
        );

        auto localId = helper.Tables["table-1"].UserTable.GetLocalId();
        UNIT_ASSERT(localId >= 1000);
        request->Record.MutableTableId()->SetTableId(localId);

        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldForbidSchemaVersion) {
        TTestHelper helper;

        auto request = helper.GetUserTablesRequest("table-1", 2, 1);
        AddRangeQuery<ui64>(
            *request,
            {Min<ui64>(),},
            true,
            {Max<ui64>(),},
            true
        );

        request->Record.MutableTableId()->SetSchemaVersion(1111);

        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldNotAllowArrow) {
        TTestHelper helper;

        auto request = helper.GetUserTablesRequest("table-1", 2, 1);
        AddRangeQuery<ui64>(
            *request,
            {Min<ui64>(),},
            true,
            {Max<ui64>(),},
            true
        );

        request->Record.SetResultFormat(NKikimrTxDataShard::ARROW);

        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
    }
};

Y_UNIT_TEST_SUITE(DataShardReadIteratorState) {
    Y_UNIT_TEST(ShouldCalculateQuota) {
        NDataShard::TReadIteratorState state({}, false, {});
        state.Quota.Rows = 100;
        state.Quota.Bytes = 1000;
        state.ConsumeSeqNo(10, 100); // seqno1
        state.ConsumeSeqNo(30, 200); // seqno2
        state.ConsumeSeqNo(40, 300); // seqno3

        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 0UL);
        UNIT_ASSERT_VALUES_EQUAL(state.SeqNo, 3UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 20UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 400UL);

        state.UpQuota(2, 200, 1000);
        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 2UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 160UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 700UL);

        state.ConsumeSeqNo(10, 100);    // seqno4
        state.ConsumeSeqNo(20, 200);    // seqno5
        state.ConsumeSeqNo(10, 50);     // seqno6
        state.ConsumeSeqNo(2000, 2000); // seqno7

        state.UpQuota(4, 5000, 5000);
        UNIT_ASSERT_VALUES_EQUAL(state.SeqNo, 7UL);
        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 4UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 2970UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 2750);
        UNIT_ASSERT(state.State == NDataShard::TReadIteratorState::EState::Executing);

        state.UpQuota(5, 100, 100);
        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 5UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 0UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 0UL);
        UNIT_ASSERT(state.State == NDataShard::TReadIteratorState::EState::Exhausted);

        state.UpQuota(6, 10, 10);
        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 6UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 0UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 0UL);
        UNIT_ASSERT(state.State == NDataShard::TReadIteratorState::EState::Exhausted);

        state.UpQuota(7, 11, 131729);
        UNIT_ASSERT_VALUES_EQUAL(state.LastAckSeqNo, 7UL);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Rows, 11);
        UNIT_ASSERT_VALUES_EQUAL(state.Quota.Bytes, 131729);
        UNIT_ASSERT(state.State == NDataShard::TReadIteratorState::EState::Executing);
    }
};

Y_UNIT_TEST_SUITE(DataShardReadIteratorPageFaults) {
    Y_UNIT_TEST(CancelPageFaultedReadThenDropTable) {
        TPortManager pm;
        NFake::TCaches caches;
        caches.Shared = 1 /* bytes */;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetCacheParams(caches);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);
        // runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto opts = TShardedTableOptions()
                .Shards(1)
                .ExecutorCacheSize(1 /* byte */)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)"));
        SimulateSleep(runtime, TDuration::Seconds(1));

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        CompactTable(runtime, shard1, tableId1, false);
        RebootTablet(runtime, shard1, sender);
        SimulateSleep(runtime, TDuration::Seconds(1));

        size_t observedReadResults = 0;
        bool captureCacheRequests = true;
        std::vector<std::unique_ptr<IEventHandle>> capturedCacheRequests;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvReadResult::EventType: {
                    auto* msg = ev->Get<TEvDataShard::TEvReadResult>();
                    Cerr << "... observed TEvReadResult:\n" << msg->ToString() << Endl;
                    observedReadResults++;
                    break;
                }
                case NSharedCache::TEvRequest::EventType: {
                    if (captureCacheRequests) {
                        Cerr << "... captured TEvRequest" << Endl;
                        capturedCacheRequests.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        auto readSender = runtime.AllocateEdgeActor();
        auto tabletPipe = runtime.ConnectToPipe(shard1, readSender, 0, NTabletPipe::TClientConfig());
        {
            auto request = std::make_unique<TEvDataShard::TEvRead>();
            request->Record.SetReadId(1);
            request->Record.MutableTableId()->SetOwnerId(tableId1.PathId.OwnerId);
            request->Record.MutableTableId()->SetTableId(tableId1.PathId.LocalPathId);
            request->Record.MutableTableId()->SetSchemaVersion(tableId1.SchemaVersion);
            request->Record.AddColumns(1);
            request->Record.AddColumns(2);
            request->Ranges.emplace_back(TOwnedCellVec(), true, TOwnedCellVec(), true);
            runtime.SendToPipe(tabletPipe, readSender, request.release());
        }

        WaitFor(runtime, [&]() { return capturedCacheRequests.size() > 0 || observedReadResults > 0; }, "shared cache request");
        UNIT_ASSERT_C(capturedCacheRequests.size() > 0, "cache request was not captured");

        {
            auto request = std::make_unique<TEvDataShard::TEvReadCancel>();
            request->Record.SetReadId(1);
            runtime.SendToPipe(tabletPipe, readSender, request.release());
        }
        SimulateSleep(runtime, TDuration::Seconds(1));

        captureCacheRequests = false;
        for (auto& ev : capturedCacheRequests) {
            runtime.Send(ev.release(), 0, true);
        }

        // We should be able to drop table
        WaitTxNotification(server, AsyncDropTable(server, sender, "/Root", "table-1"));
    }
}

} // namespace NKikimr
