#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"
#include "datashard_failpoints.h"
#include "datashard_impl.h"
#include "read_iterator.h"

#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_tli.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/memory_controller/consumer_collection.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/protos/query_stats.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <util/system/env.h>
#include <util/string/hex.h>
#include <sys/resource.h>
#include <thread>

#include <algorithm>
#include <map>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

using TCellVec = std::vector<TCell>;

TVector<TShardedTableOptions::TColumn> GetColumns() {
    TVector<TShardedTableOptions::TColumn> columns = {
        {"key1", "Uint32", true, false},
        {"key2", "Uint32", true, false},
        {"key3", "Uint32", true, false},
        {"value", "Uint32", false, false}};

    return columns;
}

TVector<TShardedTableOptions::TColumn> GetMoviesColumns() {
    TVector<TShardedTableOptions::TColumn> columns = {
        {"id", "Uint32", true, false},
        {"title", "String", false, false},
        {"rating", "Uint32", false, false}};

    return columns;
}

std::tuple<TVector<ui64>, TTableId> CreateTable(Tests::TServer::TPtr server,
                 TActorId sender,
                 const TString &root,
                 const TString &name,
                 bool withFollower = false,
                 ui64 shardCount = 1)
{
    auto opts = TShardedTableOptions()
        .Shards(shardCount)
        .Columns(GetColumns());

    if (withFollower)
        opts.Followers(1);

    return CreateShardedTable(server, sender, root, name, opts);
}

std::tuple<TVector<ui64>, TTableId> CreateMoviesTable(Tests::TServer::TPtr server,
                       TActorId sender,
                       const TString &root,
                       const TString &name)
{
    auto opts = TShardedTableOptions()
        .Columns(GetMoviesColumns());

    return CreateShardedTable(server, sender, root, name, opts);
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
        case NKikimrDataEvents::FORMAT_ARROW:
            CheckResultArrow(userTable, result, gold, goldTypes, columns);
            break;
        case NKikimrDataEvents::FORMAT_CELLVEC:
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

void CheckContinuationToken(
    const TEvDataShard::TEvReadResult& result,
    ui32 firstUprocessedQuery,
    const std::vector<ui32>& gold)
{
    UNIT_ASSERT(result.Record.HasContinuationToken());

    NKikimrTxDataShard::TReadContinuationToken readToken;
    UNIT_ASSERT(readToken.ParseFromString(result.Record.GetContinuationToken()));
    UNIT_ASSERT(readToken.HasFirstUnprocessedQuery());
    UNIT_ASSERT_VALUES_EQUAL(readToken.GetFirstUnprocessedQuery(), firstUprocessedQuery);

    if (gold.empty())
        return;

    UNIT_ASSERT(readToken.HasLastProcessedKey());

    std::vector<NScheme::TTypeInfoOrder> types;
    types.reserve(gold.size());
    for (auto i: xrange(gold.size())) {
        Y_UNUSED(i);
        types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
    }

    TCellVec goldRow;
    for (const auto& item: gold) {
        goldRow.push_back(TCell::Make(item));
    }

    TSerializedCellVec lastKey(readToken.GetLastProcessedKey());
    CheckRow(lastKey.GetCells(), goldRow, types);
}

struct TTableInfo {
    TString Name;

    TTableId TableId;
    ui64 TabletId;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;

    TActorId ClientId;

    TVector<TShardedTableOptions::TColumn> Columns;
};

struct TTestHelper {
    explicit TTestHelper(bool withFollower = false) {
        WithFollower = withFollower;
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        Init(serverSettings);
    }

    explicit TTestHelper(const TServerSettings& serverSettings, ui64 shardCount = 1, bool withFollower = false) {
        WithFollower = withFollower;
        ShardCount = shardCount;
        Init(serverSettings);
    }

    void Init(const TServerSettings& serverSettings) {
        Server = new TServer(serverSettings);

        auto &runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);

        InitRoot(Server, Sender);

        {
            auto& table1 = Tables["table-1"];
            table1.Name = "table-1";
            auto [shards, tableId] = CreateTable(Server, Sender, "/Root", "table-1", WithFollower, ShardCount);
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

            table1.TableId = tableId;
            table1.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table1.TabletId);
            table1.UserTable = tables["table-1"];

            table1.ClientId = runtime.ConnectToPipe(table1.TabletId, Sender, 0, GetTestPipeConfig());

            table1.Columns = GetColumns();
        }

        {
            auto& table2 = Tables["movies"];
            table2.Name = "movies";
            auto [shards, tableId] = CreateMoviesTable(Server, Sender, "/Root", "movies");
            ExecSQL(Server, Sender, R"(
                UPSERT INTO `/Root/movies`
                (id, title, rating)
                VALUES
                (1, "I Robot", 10),
                (2, "I Am Legend", 9),
                (3, "Hard die", 8);
            )");

            table2.TableId = tableId;
            table2.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table2.TabletId);
            table2.UserTable = tables["movies"];

            table2.ClientId = runtime.ConnectToPipe(table2.TabletId, Sender, 0, GetTestPipeConfig());

            table2.Columns = GetMoviesColumns();
        }

        {
            auto& table3 = Tables["table-1-many"];
            table3.Name = "table-1-many";
            auto [shards, tableId] = CreateTable(Server, Sender, "/Root", "table-1-many", WithFollower, ShardCount);

            table3.TableId = tableId;
            table3.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, table3.TabletId);
            table3.UserTable = tables["table-1-many"];

            table3.ClientId = runtime.ConnectToPipe(table3.TabletId, Sender, 0, GetTestPipeConfig());

            table3.Columns = GetColumns();
        }
    }

    void CreateCustomTable(const TString & name, TVector<TShardedTableOptions::TColumn> columns, bool hnsw = false,
                           const TVector<TShardedTableOptions::TFamily>& families = {},
                           std::function<void(NKikimrSchemeOp::TTableDescription&)> customize = {}) {
        auto &runtime = *Server->GetRuntime();
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (hnsw && ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType) {
                auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    NKikimrTxDataShard::TFlatSchemeTransaction tx;
                    UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
                    if (tx.HasCreateTable() && tx.GetCreateTable().GetName() == name) {
                        auto* table = tx.MutableCreateTable();
                        auto* index = table->AddTableIndexes();
                        index->SetName("hnsw");
                        index->SetPathOwnerId(table->GetPathId().GetOwnerId());
                        index->SetLocalPathId(Max<ui64>() - 1);
                        index->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                        auto* description = index->MutableHnswIndexDescription();
                        description->SetEmbeddingColumnId(columns.size());
                        auto* settings = description->MutableSettings()->mutable_settings();
                        settings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
                        settings->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
                        settings->set_vector_dimension(2);
                        settings->set_min_rows(1);
                        settings->set_delta_rows(1000);
                        if (customize) { customize(*table); }
                        UNIT_ASSERT(tx.SerializeToString(record.MutableTxBody()));
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };

        auto& table1 = Tables[name];
        table1.Name = name;

        auto opts = TShardedTableOptions()
            .Shards(ShardCount)
            .Columns(columns);
        if (!families.empty()) {
            opts.Families(families);
        }
        if (WithFollower)
            opts.Followers(1);
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", name, opts);

        table1.TableId = tableId;
        table1.TabletId = shards.at(0);

        auto [tables, ownerId] = GetTables(Server, table1.TabletId);
        table1.UserTable = tables[name];

        table1.ClientId = runtime.ConnectToPipe(table1.TabletId, Sender, 0, GetTestPipeConfig());

        table1.Columns = columns;
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
            auto event = handle->CastAsLocal<TEvDataShard::TEvUploadRowsResponse>();
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
        NKikimrDataEvents::EDataFormat format = NKikimrDataEvents::FORMAT_ARROW,
        const TRowVersion& snapshot = {})
    {
        const auto& table = Tables[tableName];

        TRowVersion readVersion;
        if (!snapshot) {
            readVersion = CreateVolatileSnapshot(
                Server,
                {"/Root/"+tableName},
                TDuration::Hours(1));
        } else {
            readVersion = snapshot;
        }

        return ::NKikimr::GetBaseReadRequest(
            table.TableId,
            table.UserTable.GetDescription(),
            readId,
            format,
            readVersion
        );
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

        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        return request;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> WaitReadResult(TDuration timeout = TDuration::Max()) {
        return ::NKikimr::WaitReadResult(Server, timeout);
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
        ::NKikimr::SendReadAsync(
            Server,
            table.TabletId,
            request,
            sender,
            node,
            GetTestPipeConfig(),
            table.ClientId
        );
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(
        const TString& tableName,
        TEvDataShard::TEvRead* request,
        ui32 node = 0,
        TActorId sender = {},
        TDuration timeout = TDuration::Max())
    {
        if (!sender) {
            sender = Sender;
        }

        const auto& table = Tables[tableName];
        return ::NKikimr::SendRead(
            Server,
            table.TabletId,
            request,
            sender,
            node,
            GetTestPipeConfig(),
            table.ClientId,
            timeout
        );
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

        const NKikimrDataEvents::TLock* prevLock;
        if (prevResult.Record.TxLocksSize()) {
            prevLock = &prevResult.Record.GetTxLocks(0);
        } else {
            prevLock = &prevResult.Record.GetBrokenTxLocks(0);
        }

        const NKikimrDataEvents::TLock* newLock;
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

        auto request = GetBaseReadRequest("table-1-many", 1, NKikimrDataEvents::FORMAT_CELLVEC, TRowVersion::Max());
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

    void TestReadOneKey(const TString& tableName, const std::vector<ui32>& keys, ui32 value)
    {
        auto readRequest = GetBaseReadRequest(tableName, 1);
        AddKeyQuery(*readRequest, keys);

        auto readResult = SendRead(tableName, readRequest.release());

        std::vector<std::vector<ui32>> gold(1);
        std::copy(keys.begin(), keys.end(), std::back_inserter(gold[0]));
        gold[0].push_back(value);

        CheckResult(Tables[tableName].UserTable, *readResult, gold);
    }

    void TestReadOneMissingKey(const TString& tableName, const std::vector<ui32>& keys)
    {
        auto readRequest = GetBaseReadRequest(tableName, 1);
        AddKeyQuery(*readRequest, keys);

        auto readResult = SendRead(tableName, readRequest.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), 0);
    }

    void WriteRowTwin(const TString& tableName, const TVector<ui32>& values, bool isEvWrite) {
        if(isEvWrite)
            WriteRow(tableName, values);
        else
            ExecSQL(Server, Sender, TStringBuilder()
                << "UPSERT INTO `/Root/" << tableName << "`\n"
                << "(" << JoinSeq(",", MakeMappedRange(Tables[tableName].Columns, [](const auto& col) { return col.Name; })) << ")\n"
                << "VALUES\n(" << JoinSeq(",", values) << ");");
    }

    std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeWriteRequest(const TString& tableName, ui64 txId, const TVector<ui32>& values, NKikimrDataEvents::TEvWrite::ETxMode txMode = NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE) {
        const auto& table = Tables[tableName];

        auto opts = TShardedTableOptions().Columns(table.Columns);
        size_t columnCount = table.Columns.size();

        std::vector<ui32> columnIds(columnCount);
        std::iota(columnIds.begin(), columnIds.end(), 1);

        Y_ENSURE(values.size() == columnCount);

        TVector<TCell> cells;
        for (ui32 col = 0; col < columnCount; ++col)
            cells.emplace_back(TCell((const char*)&values[col], sizeof(ui32)));

        TSerializedCellMatrix matrix(cells, 1, columnCount);

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, txMode);
        ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, table.TableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

        return evWrite;
    }

    NKikimrDataEvents::TEvWriteResult SendWrite(ui64 tabletId, std::unique_ptr<NEvents::TDataEvents::TEvWrite> writeRequest, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus = NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED) {
        return Write(*Server->GetRuntime(), Sender, tabletId, std::move(writeRequest), expectedStatus);
    }

    NKikimrDataEvents::TEvWriteResult WriteRow(const TString& tableName, const TVector<ui32>& values) {
        auto writeRequest = MakeWriteRequest(tableName, ++TxId, values);

        return SendWrite(Tables[tableName].TabletId, std::move(writeRequest));
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

        auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvTxProcessing::EvPlanStep: {
                    if (capturePlanStep) {
                        auto planMessage = event->Get<TEvTxProcessing::TEvPlanStep>();
                        result.LastPlanStep = planMessage->Record.GetStep();
                    }
                    break;
                }
                case TEvTxProcessing::EvReadSet: {
                    auto* msg = event->Get<TEvTxProcessing::TEvReadSet>();
                    auto flags = msg->Record.GetFlags();
                    auto isExpect = flags & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET;
                    auto isNoData = flags & NKikimrTx::TEvReadSet::FLAG_NO_DATA;
                    if (dropRS && !(isExpect && isNoData)) {
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

        const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();
        const size_t expectedReadSets = 1 + (finalUpserts && usesVolatileTxs ? 2 : 0);

        waitFor([&]{ return result.ReadSets.size() == expectedReadSets; }, "intercepted RS");

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
    ui64 TxId = 100;

    THashMap<TString, TTableInfo> Tables;
};

void TestReadKey(NKikimrDataEvents::EDataFormat format, bool withFollower = false) {
    TTestHelper helper(withFollower);

    for (ui32 k: {1, 3, 5}) {
        auto request = helper.GetBaseReadRequest("table-1", 1, format);
        AddKeyQuery(*request, {k, k, k});

        auto readResult = helper.SendRead("table-1", request.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {{k, k, k, k * 100}});
    }
}

void TestReadRangeInclusiveEnds(NKikimrDataEvents::EDataFormat format) {
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

void TestReadRangeMovies(NKikimrDataEvents::EDataFormat format) {
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
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::Uint32),
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::String),
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::Uint32)
    });
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIterator) {
    Y_UNIT_TEST(ShouldReadKeyCellVec) {
        TestReadKey(NKikimrDataEvents::FORMAT_CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadKeyArrow) {
        TestReadKey(NKikimrDataEvents::FORMAT_ARROW);
    }

    Y_UNIT_TEST(ShouldReadRangeCellVec) {
        TestReadRangeMovies(NKikimrDataEvents::FORMAT_CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadRangeArrow) {
        TestReadRangeMovies(NKikimrDataEvents::FORMAT_ARROW);
    }

    Y_UNIT_TEST(ShouldHelpQueuedReadOnOverload) {
        TTestHelper helper;
        auto& runtime = *helper.Server->GetRuntime();
        const auto& table = helper.Tables.at("table-1");
        const auto shardActor = ResolveTablet(runtime, table.TabletId);
        const auto sender = runtime.AllocateEdgeActor();
        const auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        auto makeRead = [&](ui64 readId) {
            auto request = helper.GetBaseReadRequest("table-1", readId, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            AddKeyQuery(*request, {1, 1, 1});
            return request;
        };

        // Warm the read so helping can finish it without waiting for pages.
        auto warmResult = helper.SendRead("table-1", makeRead(1).release());
        CheckResult(table.UserTable, *warmResult, {{1, 1, 1, 100}});
        UNIT_ASSERT(warmResult->Record.GetFinished());

        // CheckInflightLimit rejects when the count is greater than the limit,
        // so a limit of one admits two iterators before rejecting the next read.
        TControlBoard::SetValue(1, runtime.GetAppData().Icb->DataShardControls.MaxTxInFly);
        TBlockEvents<IEventHandle> blockedActivations(runtime, [&](const IEventHandle::TPtr& ev) {
            return ev->GetTypeName().Contains("TEvActivateLowExecution");
        });
        for (ui64 readId : {2, 3}) {
            runtime.Send(new IEventHandle(shardActor, sender, makeRead(readId).release()), 0, true);
        }
        runtime.WaitFor("blocked low-priority activation", [&] { return !blockedActivations.empty(); }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount"), 2);

        runtime.Send(new IEventHandle(shardActor, sender, makeRead(4).release()), 0, true);
        std::map<ui64, TEvDataShard::TEvReadResult::TPtr> results;
        for (size_t i = 0; i < 2; ++i) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(sender, TDuration::Seconds(5));
            UNIT_ASSERT_C(ev, "Expected the helped read and overload results while activations are blocked");
            const ui64 readId = ev->Get()->Record.GetReadId();
            UNIT_ASSERT(results.emplace(readId, std::move(ev)).second);
        }

        const auto& helped = *results.at(2)->Get();
        CheckResult(table.UserTable, helped, {{1, 1, 1, 100}});
        UNIT_ASSERT(helped.Record.GetFinished());
        const auto& rejected = results.at(4)->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(rejected.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(rejected.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(rejected.GetStatus().GetIssues(0).message().Contains("MaxTxInFly"));
        // Helping runs exactly one transaction and still rejects the new read,
        // even though completing the queued read frees an admission slot.
        UNIT_ASSERT_VALUES_EQUAL(helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount"), 1);

        blockedActivations.Stop().Unblock();
        auto remaining = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(remaining);
        UNIT_ASSERT_VALUES_EQUAL(remaining->Get()->Record.GetReadId(), 3);
        CheckResult(table.UserTable, *remaining->Get(), {{1, 1, 1, 100}});
        UNIT_ASSERT(remaining->Get()->Record.GetFinished());
        UNIT_ASSERT_VALUES_EQUAL(helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount"), 0);
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

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_CELLVEC);
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

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW);
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

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_CELLVEC);
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

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW);
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        UNIT_ASSERT(!record1.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record1.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record1.GetSeqNo(), 1UL);
        CheckContinuationToken(*readResult1, 1, {});

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100}
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);
        CheckContinuationToken(*readResult2, 2, {});

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
        UNIT_ASSERT(!record3.HasContinuationToken());
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        CheckContinuationToken(*readResult1, 1, {});

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {1, 1, 1, 100}
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);
        //CheckContinuationToken(*readResult1, 0, {});

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
        UNIT_ASSERT(!record3.HasContinuationToken());
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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

        std::vector<std::vector<ui32>> goldKeys = {
            {11, 11, 11},
            {8, 1, 1},
            {5, 5, 5},
            {3, 3, 3},
            {1, 1, 1},
        };

        auto readResult = helper.SendRead("table-1", request.release());
        UNIT_ASSERT(readResult);
        CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
            gold[0]
        });
        CheckContinuationToken(*readResult, 1, goldKeys[0]);

        for (size_t i = 1; i < gold.size(); ++i) {
            helper.SendReadAck("table-1", readResult->Record, 1, 10000);
            readResult = helper.WaitReadResult();
            UNIT_ASSERT(readResult);
            CheckResult(helper.Tables["table-1"].UserTable, *readResult, {
                gold[i]
            });
            if (i > 1) {
                CheckContinuationToken(*readResult, 0, goldKeys[i]);
            } else {
                CheckContinuationToken(*readResult, 1, goldKeys[i]);
            }
        }

        helper.SendReadAck("table-1", readResult->Record, 1, 10000);
        readResult = helper.WaitReadResult();
        UNIT_ASSERT(readResult);
        UNIT_ASSERT(readResult->Record.GetFinished());
        UNIT_ASSERT(!readResult->Record.HasContinuationToken());
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TAutoPtr<IEventHandle>& ev) {
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
        TestReadRangeInclusiveEnds(NKikimrDataEvents::FORMAT_CELLVEC);
    }

    Y_UNIT_TEST(ShouldReadRangeInclusiveEndsArrow) {
        TestReadRangeInclusiveEnds(NKikimrDataEvents::FORMAT_ARROW);
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

        CheckContinuationToken(*readResult1, 0, {1, 1, 1});

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {3, 3, 3, 300},
        });

        const auto& record2 = readResult2->Record;
        UNIT_ASSERT(!record2.GetLimitReached());
        UNIT_ASSERT(!record2.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record2.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record2.GetSeqNo(), 2UL);

        CheckContinuationToken(*readResult2, 0, {3, 3, 3});

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {5, 5, 5, 500}
        });

        const auto& record3 = readResult3->Record;
        UNIT_ASSERT(!record3.GetLimitReached());
        UNIT_ASSERT(!record3.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record3.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record3.GetSeqNo(), 3UL);

        CheckContinuationToken(*readResult3, 0, {5, 5, 5});

        auto readResult4 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult4, {
            {1, 1, 1, 100}
        });

        const auto& record4 = readResult4->Record;
        UNIT_ASSERT(!record4.GetLimitReached());
        UNIT_ASSERT(!record4.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record4.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record4.GetSeqNo(), 4UL);

        CheckContinuationToken(*readResult4, 1, {1, 1, 1});

        auto readResult5 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult5, {
        });

        const auto& record5 = readResult5->Record;
        UNIT_ASSERT(!record5.GetLimitReached());
        UNIT_ASSERT(record5.HasFinished());
        UNIT_ASSERT_VALUES_EQUAL(record5.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(record5.GetSeqNo(), 5UL);

        UNIT_ASSERT(!record5.HasContinuationToken());
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

        auto originalObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue: {
                if (shouldDrop) {
                    continueEvent = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            default:
                return originalObserver(ev);
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
        TAutoPtr<TEvDataShard::TEvReadContinue> request = IEventHandle::Release<TEvDataShard::TEvReadContinue>(continueEvent);

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

        auto originalObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue: {
                if (shouldDrop) {
                    continueEvent = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            default:
                return originalObserver(ev);
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
        TAutoPtr<TEvDataShard::TEvReadContinue> request = IEventHandle::Release<TEvDataShard::TEvReadContinue>(continueEvent);

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
        TestReadKey(NKikimrDataEvents::FORMAT_CELLVEC, true);
    }

    Y_UNIT_TEST(ShouldNotReadFutureMvccFromFollower) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount, true);

        TRowVersion someVersion = TRowVersion(10000, Max<ui64>());
        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, someVersion);
        AddKeyQuery(*request, {3, 3, 3});
        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(ShouldReadHeadFromFollower) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount, true);

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, TRowVersion::Max());
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
        runtime.SetObserverFunc([&continueCounter, &connectedFromDifferentNode, &serverConnectedCount](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvReadContinue:
                ++continueCounter;
                break;
            case TEvTabletPipe::EvServerConnected: {
                auto* typedEvent = ev->CastAsLocal<TEvTabletPipe::TEvServerConnected>();
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
            sender = runtime.AllocateEdgeActor(node);
            serverConnectedCount = 0;
            table.ClientId = runtime.ConnectToPipe(table.TabletId, sender, node, GetPipeConfigWithRetries());
            UNIT_ASSERT(table.ClientId);
            waitFor([&]{ return serverConnectedCount != 0; }, "intercepted EvServerConnected");
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

        exhaustedCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsExhaustedCount", node);
        while (exhaustedCount != 0) {
            SimulateSleep(helper.Server, TDuration::Seconds(1));
            exhaustedCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsExhaustedCount", node);
        }

        iteratorsCount = helper.GetSimpleCounter("table-1", "DataShard/ReadIteratorsCount", node);
        UNIT_ASSERT_VALUES_EQUAL(iteratorsCount, 0UL);
    }

    Y_UNIT_TEST(ShouldReadFromHead) {
        // read from HEAD when there is no conflicting operation
        TTestHelper helper;

        auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, TRowVersion::Max());
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
            .SetUseRealThreads(false)
            // Blocked volatile transactions block reads, disable
            .SetEnableDataShardVolatileTransactions(false)
            .SetEnableDataShardWriteAlwaysVolatile(false);

        const ui64 shardCount = 1;
        TTestHelper helper(serverSettings, shardCount);

        auto hangedInfo = helper.HangWithTransactionWaitingRS(shardCount, false);

        {
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, TRowVersion::Max());
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
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, TRowVersion::Max());
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
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, oldVersion);
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
            auto request = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, oldVersion);
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
            auto request = helper.GetBaseReadRequest("table-1", 2, NKikimrDataEvents::FORMAT_ARROW, oldVersion);

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
            auto request = helper.GetBaseReadRequest("table-1", 3, NKikimrDataEvents::FORMAT_ARROW, oldVersion);
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
            auto request = helper.GetBaseReadRequest("table-1", 4, NKikimrDataEvents::FORMAT_ARROW, oldVersion);
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
            auto request = helper.GetBaseReadRequest("table-1", 5, NKikimrDataEvents::FORMAT_ARROW, oldVersion);
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

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(*helper.Server->GetRuntime());

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

        auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvMediatorTimecast::EvUpdate: {
                    auto* update = event->Get<TEvMediatorTimecast::TEvUpdate>();
                    lastStep = update->Record.GetTimeBarrier();
                    Cerr << "... observed TEvUpdate(" << lastStep << ")" << Endl;
                    if (captureTimecast) {
                        Cerr << "---- dropped EvUpdate ----" << Endl;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvWaitPlanStep: {
                    auto* waitEvent = event->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                    Cerr << "... observed TEvWaitPlanStep(" << waitEvent->PlanStep << ")" << Endl;
                    if (captureWaitNotify) {
                        waitPlanStep = waitEvent->PlanStep;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvNotifyPlanStep: {
                    auto* notifyEvent = event->Get<TEvMediatorTimecast::TEvNotifyPlanStep>();
                    Cerr << "... observed TEvNotifyPlanStep(" << notifyEvent->PlanStep << ")" << Endl;
                    if (captureWaitNotify) {
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
        snapshot = TRowVersion(lastStep + 3000, Max<ui64>());

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, snapshot);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());

        waitFor([&]{ return notifyPlanStep >= snapshot.Step; }, TStringBuilder() << "intercepted TEvNotifyPlanStep for snapshot " << snapshot);
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        // NOTE: our snapshot is not from coordinator so we may get a reduced
        // resolution step. Previously we just happened to always generate
        // snapshot rounded to 1 second.
        UNIT_ASSERT_GE(notifyPlanStep, snapshot.Step);

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

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(*helper.Server->GetRuntime());

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

        auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvMediatorTimecast::EvUpdate: {
                    auto* update = event->Get<TEvMediatorTimecast::TEvUpdate>();
                    lastStep = update->Record.GetTimeBarrier();
                    Cerr << "... observed TEvUpdate(" << lastStep << ")" << Endl;
                    if (captureTimecast) {
                        Cerr << "---- dropped EvUpdate ----" << Endl;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvWaitPlanStep: {
                    auto* waitEvent = event->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                    Cerr << "... observed TEvWaitPlanStep(" << waitEvent->PlanStep << ")" << Endl;
                    if (captureWaitNotify) {
                        waitPlanStep = waitEvent->PlanStep;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvNotifyPlanStep: {
                    auto* notifyEvent = event->Get<TEvMediatorTimecast::TEvNotifyPlanStep>();
                    Cerr << "... observed TEvNotifyPlanStep(" << notifyEvent->PlanStep << ")" << Endl;
                    if (captureWaitNotify) {
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
        snapshot = TRowVersion(lastStep + 3000, Max<ui64>());

        auto request1 = helper.GetBaseReadRequest("table-1", 1, NKikimrDataEvents::FORMAT_ARROW, snapshot);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRowsInResult(1);

        helper.SendReadAsync("table-1", request1.release());

        waitFor([&]{ return waitPlanStep >= snapshot.Step; }, "intercepted TEvWaitPlanStep");
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        UNIT_ASSERT_LT(notifyPlanStep, snapshot.Step);

        helper.SendCancel("table-1", 1);

        waitFor([&]{ return notifyPlanStep >= snapshot.Step; }, "intercepted TEvNotifyPlanStep");
        UNIT_ASSERT_VALUES_EQUAL(waitPlanStep, snapshot.Step);
        // NOTE: our snapshot is not from coordinator so we may get a reduced
        // resolution step. Previously we just happened to always generate
        // snapshot rounded to 1 second.
        UNIT_ASSERT_GE(notifyPlanStep, snapshot.Step);

        SimulateSleep(helper.Server, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(readResults, 0);
    }

    Y_UNIT_TEST(ShouldCommitLocksWhenReadWriteInOneTransaction) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const ui64 tabletId = helper.Tables["table-1"].TabletId;
        const ui64 nodeId = runtime->GetNodeId();

        auto snapshot = AcquireReadSnapshot(*runtime, "/Root");

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        // Read in a transaction.
        auto readRequest1 = helper.GetBaseReadRequest(tableName, 1);
        readRequest1->Record.SetLockTxId(lockTxId);
        snapshot.ToProto(readRequest1->Record.MutableSnapshot());
        AddKeyQuery(*readRequest1, {1, 1, 1});

        auto readResult1 = helper.SendRead(tableName, readRequest1.release());

        CheckResult(helper.Tables[tableName].UserTable, *readResult1, { {1, 1, 1, 100} });

        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);
        const auto& readLock = readResult1->Record.GetTxLocks(0);

        // Write in the same transaction.
        {
            auto writeRequest = helper.MakeWriteRequest(tableName, ++helper.TxId, {1, 1, 1, 101});
            writeRequest->Record.SetLockTxId(lockTxId);
            writeRequest->Record.SetLockNodeId(nodeId);
            writeRequest->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
            writeRequest->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 1);
            const auto& writeLock = writeResult.GetTxLocks(0);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetLockId(), readLock.GetLockId());
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetDataShard(), readLock.GetDataShard());
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetGeneration(), readLock.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetCounter(), readLock.GetCounter());
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetSchemeShard(), readLock.GetSchemeShard());
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetPathId(), readLock.GetPathId());
            UNIT_ASSERT_VALUES_UNEQUAL(writeLock.GetHasWrites(), readLock.GetHasWrites());
        }

        // Commit locks.
        {
            auto writeRequest = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++helper.TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            NKikimrDataEvents::TKqpLocks& kqpLocks = *writeRequest->Record.MutableLocks();
            kqpLocks.MutableLocks()->CopyFrom(readResult1->Record.GetTxLocks());
            kqpLocks.AddSendingShards(tabletId);
            kqpLocks.AddReceivingShards(tabletId);
            kqpLocks.SetOp(::NKikimrDataEvents::TKqpLocks::Commit);

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);
        }

        // Read written data.
        helper.TestReadOneKey(tableName, {1, 1, 1}, 101);
    }

    Y_UNIT_TEST_QUAD(TryCommitLocksPrepared, Volatile, BreakLocks) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        const ui64 lockTxId = 1011121314;
        const TString tableName1 = "table-1";
        const TString tableName2 = "table-1-many";
        const ui64 tabletId1 = helper.Tables[tableName1].TabletId;
        const ui64 tabletId2 = helper.Tables[tableName2].TabletId;
        const ui64 nodeId = runtime->GetNodeId();
        ui64 minStep1, maxStep1;
        ui64 minStep2, maxStep2;
        ui64 coordinator;

        // Upsert 3 rows to table 2
        helper.UpsertMany(1, 3);

        Cout << "========= Read origin data from table 2" << Endl;
        {
            helper.TestReadOneKey(tableName2, {1, 1, 1}, 1);
        }

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        Cerr << "===== Read in a transaction on table 1" << Endl;
        ::google::protobuf::RepeatedPtrField<::NKikimrDataEvents::TLock> readLocks;
        {
            auto readRequest1 = helper.GetBaseReadRequest(tableName1, 1);
            readRequest1->Record.SetLockTxId(lockTxId);
            readRequest1->Record.SetLockNodeId(nodeId);
            AddKeyQuery(*readRequest1, {1});

            auto readResult1 = helper.SendRead(tableName1, readRequest1.release());
            CheckResult(helper.Tables[tableName1].UserTable, *readResult1, { {1, 1, 1, 100} });

            UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);
            readLocks = readResult1->Record.GetTxLocks();
            UNIT_ASSERT_VALUES_EQUAL(readLocks.size(), 1);
        }

        if (BreakLocks) {
            Cout << "========= Break lock by writing data to table 1" << Endl;
            helper.WriteRow(tableName1, {1, 1, 1, 999});
        }

        Cerr << "===== Commit locks on table 1" << Endl;
        {
            auto writeRequest = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++helper.TxId,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            NKikimrDataEvents::TKqpLocks& kqpLocks = *writeRequest->Record.MutableLocks();
            kqpLocks.MutableLocks()->CopyFrom(readLocks);
            kqpLocks.AddSendingShards(tabletId1);
            kqpLocks.AddReceivingShards(tabletId2);
            kqpLocks.SetOp(::NKikimrDataEvents::TKqpLocks::Commit);

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId1, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);
            minStep1 = writeResult.GetMinStep();
            maxStep1 = writeResult.GetMaxStep();
            coordinator = writeResult.GetDomainCoordinators(0);
        }

        Cerr << "===== Write and commit locks on table 2" << Endl;
        {
            auto writeRequest = helper.MakeWriteRequest(tableName2, helper.TxId, {1, 1, 1, 1001},
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            NKikimrDataEvents::TKqpLocks& kqpLocks = *writeRequest->Record.MutableLocks();
            kqpLocks.AddSendingShards(tabletId1);
            kqpLocks.AddReceivingShards(tabletId2);
            kqpLocks.SetOp(::NKikimrDataEvents::TKqpLocks::Commit);

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId2, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);
            minStep2 = writeResult.GetMinStep();
            maxStep2 = writeResult.GetMaxStep();
        }

        Cerr << "========= Send propose to coordinator" << Endl;
        SendProposeToCoordinator(
            *runtime, helper.Sender, {tabletId1, tabletId2}, {
                .TxId = helper.TxId,
                .Coordinator = coordinator,
                .MinStep = Max(minStep1, minStep2),
                .MaxStep = Min(maxStep1, maxStep2),
            });

        Cerr << "========= Wait for completed transactions" << Endl;
        for (ui8 i = 0; i < 1; ++i)
        {
            auto expectedStatus = BreakLocks ?
                NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN :
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED;

            auto writeResult = WaitForWriteCompleted(*runtime, helper.Sender, expectedStatus);

            if (!BreakLocks) {
                UNIT_ASSERT_GE(writeResult.GetStep(), Max(minStep1, minStep2));
                UNIT_ASSERT_LE(writeResult.GetStep(), Min(maxStep1, maxStep2));
                UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), helper.TxId);
                UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), helper.TxId);

                UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);

                if (writeResult.GetOrigin() == tabletId1) {
                    const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + tableName1);
                } else if (writeResult.GetOrigin() == tabletId2) {
                    const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + tableName2);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), 1);
                } else {
                    UNIT_FAIL("Unknown origin tablet");
                }
            }

        }

        Cout << "========= Read written data" << Endl;
        {
            auto expectedValue = BreakLocks ? 1 : 1001;
            helper.TestReadOneKey(tableName2, {1, 1, 1}, expectedValue);
        }
    }

    Y_UNIT_TEST(ShouldCommitLocksWhenReadWriteInSeparateTransactions) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const ui64 tabletId = helper.Tables["table-1"].TabletId;
        const ui64 nodeId = runtime->GetNodeId();

        auto snapshot = AcquireReadSnapshot(*runtime, "/Root");

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        // Write in first transaction.
        auto writeRequest = helper.MakeWriteRequest(tableName, ++helper.TxId, {1, 1, 1, 101});
        writeRequest->Record.SetLockTxId(lockTxId);
        writeRequest->Record.SetLockNodeId(nodeId);
        writeRequest->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
        writeRequest->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);

        NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 1);
        const auto& writeLock = writeResult.GetTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetLockId(), lockTxId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetDataShard(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetCounter(), 0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetHasWrites(), true);

        // Read in separate transaction. No dirty-read.
        helper.TestReadOneKey(tableName, {1, 1, 1}, 100);

        // Commit locks in first transaction.
        auto writeRequest2 = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++helper.TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        NKikimrDataEvents::TKqpLocks& kqpLocks2 = *writeRequest2->Record.MutableLocks();
        kqpLocks2.MutableLocks()->CopyFrom(writeResult.GetTxLocks());
        kqpLocks2.AddSendingShards(tabletId);
        kqpLocks2.AddReceivingShards(tabletId);
        kqpLocks2.SetOp(::NKikimrDataEvents::TKqpLocks::Commit);

        NKikimrDataEvents::TEvWriteResult writeResult2 = helper.SendWrite(tabletId, std::move(writeRequest2));

        UNIT_ASSERT_VALUES_EQUAL(writeResult2.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        // Read written data.
        helper.TestReadOneKey(tableName, {1, 1, 1}, 101);
    }

    Y_UNIT_TEST(ShouldRollbackLocksWhenWrite) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const ui64 tabletId = helper.Tables["table-1"].TabletId;
        const ui64 nodeId = runtime->GetNodeId();

        auto snapshot = AcquireReadSnapshot(*runtime, "/Root");

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        // Write in transaction.
        auto writeRequest = helper.MakeWriteRequest(tableName, ++helper.TxId, {1, 1, 1, 101});
        writeRequest->Record.SetLockTxId(lockTxId);
        writeRequest->Record.SetLockNodeId(nodeId);
        writeRequest->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
        writeRequest->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);

        NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 1);
        const auto& writeLock = writeResult.GetTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetLockId(), lockTxId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetDataShard(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetCounter(), 0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetHasWrites(), true);

        // Rollback locks in transaction.
        auto writeRequest2 = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++helper.TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        NKikimrDataEvents::TKqpLocks& kqpLocks2 = *writeRequest2->Record.MutableLocks();
        kqpLocks2.MutableLocks()->CopyFrom(writeResult.GetTxLocks());
        kqpLocks2.SetOp(::NKikimrDataEvents::TKqpLocks::Rollback);

        NKikimrDataEvents::TEvWriteResult writeResult2 = helper.SendWrite(tabletId, std::move(writeRequest2));

        UNIT_ASSERT_VALUES_EQUAL(writeResult2.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        // Read origin data.
        helper.TestReadOneKey(tableName, {1, 1, 1}, 100);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenWriteInSeparateTransactions, EvWrite) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const ui64 tabletId = helper.Tables["table-1"].TabletId;
        const ui64 nodeId = runtime->GetNodeId();

        auto snapshot = AcquireReadSnapshot(*runtime, "/Root");

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        // Write in first transaction.
        auto writeRequest = helper.MakeWriteRequest(tableName, ++helper.TxId, {1, 1, 1, 101});
        writeRequest->Record.SetLockTxId(lockTxId);
        writeRequest->Record.SetLockNodeId(nodeId);
        writeRequest->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
        writeRequest->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);

        NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 1);
        const auto& writeLock = writeResult.GetTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetLockId(), lockTxId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetDataShard(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetCounter(), 0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock.GetHasWrites(), true);

        // Breaks lock obtained above using write in separate transaction.
        helper.WriteRowTwin(tableName, {1, 1, 1, 202}, EvWrite);

        // Commit locks in first transaction. They should be broken.
        auto writeRequest2 = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++helper.TxId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        NKikimrDataEvents::TKqpLocks& kqpLocks2 = *writeRequest2->Record.MutableLocks();
        kqpLocks2.MutableLocks()->CopyFrom(writeResult.GetTxLocks());
        kqpLocks2.AddSendingShards(tabletId);
        kqpLocks2.AddReceivingShards(tabletId);
        kqpLocks2.SetOp(::NKikimrDataEvents::TKqpLocks::Commit);

        NKikimrDataEvents::TEvWriteResult writeResult2 = helper.SendWrite(tabletId, std::move(writeRequest2), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

        UNIT_ASSERT_VALUES_EQUAL(writeResult2.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        UNIT_ASSERT(writeResult2.TxLocksSize() == 1);
        const auto& writeLock2 = writeResult2.GetTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(writeLock2.GetLockId(), writeLock.GetLockId());
        UNIT_ASSERT_VALUES_EQUAL(writeLock2.GetDataShard(), writeLock.GetDataShard());
        UNIT_ASSERT_VALUES_EQUAL(writeLock2.GetGeneration(), writeLock.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(writeLock2.GetCounter(), writeLock.GetCounter());
        UNIT_ASSERT_VALUES_EQUAL(writeLock2.GetHasWrites(), writeLock.GetHasWrites());

        // read written data
        helper.TestReadOneKey(tableName, {1, 1, 1}, 202);
    }

    Y_UNIT_TEST_TWIN(TryWriteManyRows, Commit) {
        TTestHelper helper;

        auto runtime = helper.Server->GetRuntime();

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const TTableId& tableId = helper.Tables[tableName].TableId;
        const ui64 tabletId = helper.Tables[tableName].TabletId;
        const auto& columns = helper.Tables[tableName].Columns;
        const ui64 nodeId = runtime->GetNodeId();

        const ui64 initialRowCount = 8;

        const ui64 writeCount = 55;
        const ui64 rowCount = 77;

        NKikimrDataEvents::TLock firstLock;

        Cerr << "========= Wait for table stats" << Endl;
        {
            ui64 statRowCount = WaitTableStats(*runtime, tabletId).GetTableStats().GetRowCount();
            UNIT_ASSERT_VALUES_EQUAL(statRowCount, initialRowCount);
        }

        Cerr << "========= Read key" << Endl;
        {
            helper.TestReadOneKey(tableName, {1, 1, 1}, 100);
        }

        auto snapshot = AcquireReadSnapshot(*runtime, "/Root");

        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        Cerr << "========= Write many rows" << Endl;
        for (ui64 i = 0; i < writeCount; ++i) {
            ui64 seed = 1000000 + i * rowCount * columns.size();
            auto writeRequest = MakeWriteRequest({}, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columns, rowCount, seed);
            writeRequest->Record.SetLockTxId(lockTxId);
            writeRequest->Record.SetLockNodeId(nodeId);
            writeRequest->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
            writeRequest->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 1);
            const auto& writeLock = writeResult.GetTxLocks(0);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetLockId(), lockTxId);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetDataShard(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetCounter(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeLock.GetHasWrites(), true);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + tableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);

            if (i==0) {
                firstLock.CopyFrom(writeLock);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(firstLock.ShortDebugString(), writeLock.ShortDebugString());
            }
        }

        Cerr << "========= " << (Commit ? "Commit" : "Rollback") << " locks" << Endl;
        {
            auto writeRequest = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            NKikimrDataEvents::TKqpLocks& kqpLocks = *writeRequest->Record.MutableLocks();
            kqpLocks.MutableLocks()->Add()->CopyFrom(firstLock);

            kqpLocks.AddSendingShards(tabletId);
            kqpLocks.AddReceivingShards(tabletId);

            if (Commit) {
                kqpLocks.SetOp(::NKikimrDataEvents::TKqpLocks::Commit );
            } else {
                kqpLocks.SetOp(::NKikimrDataEvents::TKqpLocks::Rollback);
            }

            NKikimrDataEvents::TEvWriteResult writeResult = helper.SendWrite(tabletId, std::move(writeRequest));

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);
        }

        Cerr << "========= Read new key" << Endl;
        {
            const std::vector<ui32> key = {1000000, 1000001, 1000002};

            if (Commit) {
                helper.TestReadOneKey(tableName, key, 1000003);
            } else {
                helper.TestReadOneMissingKey(tableName, key);
            }
        }

        Cerr << "========= Compact table" << Endl;
        {
            CompactTable(*runtime, tabletId, tableId, false);
        }

        Cerr << "========= Wait for table stats" << Endl;
        {
            ui64 expectedRowCount = initialRowCount;
            if (Commit)
                 expectedRowCount += writeCount * rowCount;

            ui64 statRowCount = WaitTableStats(*runtime, tabletId, [expectedRowCount](const NKikimrTableStats::TTableStats& stats) {
                return stats.GetRowCount() >= expectedRowCount;
            }).GetTableStats().GetRowCount();
            UNIT_ASSERT_VALUES_EQUAL(statRowCount, expectedRowCount);
        }
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKey, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {1, 1, 1});

        auto readResult1 = helper.SendRead(tableName, request1.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);

        // breaks lock obtained above
        helper.WriteRowTwin(tableName, {1, 1, 1, 101}, EvWrite);

        // we use request2 to obtain same lock as in request1 to check it
        auto request2 = helper.GetBaseReadRequest(tableName, 1);
        request2->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request2, {1, 1, 1});

        auto readResult2 = helper.SendRead(tableName, request2.release());

        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadRange, EvWrite) {
        // upsert into "left border -1 " and to the "right border + 1" - lock not broken
        // upsert inside range - broken
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";
        const TVector<ui32> checkKey = {11, 11, 11};

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(*request1, {3, 3, 3}, true, {8, 0, 1}, true);
        auto readResult1 = helper.SendRead(tableName, request1.release());

        // upsert to the left and check that lock is not broken
        helper.WriteRowTwin(tableName, {1, 1, 1, 101}, EvWrite);
        helper.CheckLockValid(tableName, 2, checkKey, lockTxId);

        // upsert to the right and check that lock is not broken
        helper.WriteRowTwin(tableName, {8, 1, 0, 802}, EvWrite);
        helper.CheckLockValid(tableName, 2, checkKey, lockTxId);

        // breaks lock
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {4, 4, 4, 400}, EvWrite);
        helper.CheckLockBroken(tableName, 3, checkKey, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips, EvWrite) {
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

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        // write new data above snapshot
        helper.WriteRowTwin(tableName, {4, 4, 4, 44441}, EvWrite);


        auto request1 = helper.GetBaseReadRequest(tableName, 1, NKikimrDataEvents::FORMAT_ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);

        AddRangeQuery<ui32>(*request1, {1, 1, 1}, true, {5, 5, 5}, true);

        auto readResult1 = helper.SendRead(tableName, request1.release());
        CheckResult(helper.Tables[tableName].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });

        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 1);

        helper.CheckLockBroken(tableName, 10, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips2) {
        // Almost the same as ShouldReturnBrokenLockWhenReadRangeInvisibleRowSkips:
        // 1. tx1: read some **non-existing** range1
        // 2. tx2: upsert into range2 > range1 range and commit.
        // 3. tx1: read range2 -> lock should be broken

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TTestHelper helper(serverSettings);

        auto readVersion = CreateVolatileSnapshot(
            helper.Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1, NKikimrDataEvents::FORMAT_ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(*request1, {100, 0, 0}, true, {200, 0, 0}, true);

        auto readResult1 = helper.SendRead(tableName, request1.release());
        CheckResult(helper.Tables[tableName].UserTable, *readResult1, {});
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

        auto request2 = helper.GetBaseReadRequest(tableName, 2, NKikimrDataEvents::FORMAT_ARROW, readVersion);
        request2->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(*request2, {300, 0, 0}, true, {300, 0, 0}, true);

        auto readResult2 = helper.SendRead(tableName, request2.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1);
        helper.CheckLockBroken(tableName, 10, {300, 0, 0}, lockTxId, *readResult2);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadRangeLeftBorder, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(*request1, {3, 3, 3}, true, {8, 0, 1}, true);

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // breaks lock
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {3, 3, 3, 0xdead}, EvWrite);
        helper.CheckLockBroken(tableName, 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadRangeRightBorder, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddRangeQuery<ui32>(*request1, {3, 3, 3}, true, {8, 0, 1}, true);

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // breaks lock
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {8, 0, 1, 0xdead}, EvWrite);
        helper.CheckLockBroken(tableName, 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKeyPrefix, EvWrite) {
        // upsert into "left border -1 " and to the "right border + 1" - lock not broken
        // upsert inside range - broken
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // upsert to the left and check that lock is not broken
        helper.WriteRowTwin(tableName, {5, 5, 5, 555}, EvWrite);
        helper.CheckLockValid(tableName, 2, {11, 11, 11}, lockTxId);

        // upsert to the right and check that lock is not broken
        helper.WriteRowTwin(tableName, {9, 0, 0, 900}, EvWrite);
        helper.CheckLockValid(tableName, 2, {11, 11, 11}, lockTxId);

        // breaks lock obtained above
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {8, 1, 1, 8000}, EvWrite);
        helper.CheckLockBroken(tableName, 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKeyPrefixLeftBorder, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {8, 0, 0, 8000}, EvWrite);
        helper.CheckLockBroken(tableName, 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKeyPrefixRightBorder, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        request1->Record.SetLockTxId(lockTxId);
        AddKeyQuery(*request1, {8});

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {8, 1, 1, 8000}, EvWrite);
        helper.CheckLockBroken(tableName, 3, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKeyWithContinue, EvWrite) {
        TTestHelper helper;

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        auto request1 = helper.GetBaseReadRequest(tableName, 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});
        AddKeyQuery(*request1, {5, 5, 5});
        request1->Record.SetMaxRows(1);
        request1->Record.SetLockTxId(lockTxId);

        auto readResult1 = helper.SendRead(tableName, request1.release());

        // breaks lock obtained above
        // also we modify range: insert new key
        helper.WriteRowTwin(tableName, {1, 1, 1, 1000}, EvWrite);
        helper.SendReadAck(tableName, readResult1->Record, 3, 10000);

        auto readResult2 = helper.WaitReadResult();
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1UL);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());
    }

    Y_UNIT_TEST_TWIN(ShouldReturnBrokenLockWhenReadKeyWithContinueInvisibleRowSkips, EvWrite) {
        // If we read in v1, write in v2, then write breaks lock.
        // Because of out of order execution, v2 can happen before v1
        // and we should properly handle it in DS to break lock.

        TTestHelper helper;

        auto readVersion = CreateVolatileSnapshot(
            helper.Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

        const ui64 lockTxId = 1011121314;
        const TString tableName = "table-1";

        // write new data above snapshot
        helper.WriteRowTwin(tableName, {4, 4, 4, 4444}, EvWrite);

        auto request1 = helper.GetBaseReadRequest(tableName, 1, NKikimrDataEvents::FORMAT_ARROW, readVersion);
        request1->Record.SetLockTxId(lockTxId);
        request1->Record.SetMaxRows(1); // set quota so that DS hangs waiting for ACK

        AddRangeQuery<ui32>(*request1, {1, 1, 1}, true, {5, 5, 5}, true);

        auto readResult1 = helper.SendRead(tableName, request1.release());
        CheckResult(helper.Tables[tableName].UserTable, *readResult1, {
            {1, 1, 1, 100},
        });

        // we had read only key=1, so didn't see invisible key=4
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.BrokenTxLocksSize(), 0);

        helper.SendReadAck(tableName, readResult1->Record, 100, 10000);
        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables[tableName].UserTable, *readResult2, {
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });

        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.TxLocksSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(readResult2->Record.BrokenTxLocksSize(), 1UL);

        const auto& lock = readResult1->Record.GetTxLocks(0);
        const auto& brokenLock = readResult2->Record.GetBrokenTxLocks(0);
        UNIT_ASSERT_VALUES_EQUAL(lock.GetLockId(), brokenLock.GetLockId());
        UNIT_ASSERT(lock.GetCounter() < brokenLock.GetCounter());

        helper.CheckLockBroken(tableName, 10, {11, 11, 11}, lockTxId, *readResult1);
    }

    Y_UNIT_TEST(HandlePersistentSnapshotGoneInContinue) {
        // TODO
    }

    Y_UNIT_TEST(HandleMvccGoneInContinue) {
        // TODO
    }


    Y_UNIT_TEST(TliReadIteratorLocksBroken) {
        // This test verifies TLI logging when read iterator operations break locks
        TStringStream ss;
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetLogBackend(new TStreamLogBackend(&ss));

        TTestHelper helper(serverSettings);

        auto runtime = helper.Server->GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);

        const TString tableName = "table-1";
        const ui64 lockTxId = 1011121314;
        const ui64 nodeId = runtime->GetNodeId();
        const ui64 querySpanId = 12345;

        // Create a read lock
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime->GetActorSystem(0));

        auto readRequest1 = helper.GetBaseReadRequest(tableName, 1);
        readRequest1->Record.SetLockTxId(lockTxId);
        readRequest1->Record.SetLockNodeId(nodeId);
        readRequest1->Record.SetQuerySpanId(querySpanId);
        AddKeyQuery(*readRequest1, {1});

        auto readResult1 = helper.SendRead(tableName, readRequest1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.TxLocksSize(), 1);

        // Write to break the lock
        helper.WriteRow(tableName, {1, 1, 1, 999});

        // The TLI logging should have occurred when the lock was broken
        // Check for the TLI log entry (the write broke the read lock)
        TVector<std::pair<TString, ui64>> regexToMatchCount{
            {NTestTli::ConstructRegexToCheckLogs("INFO", "DataShard", "broke.*?locks"), 1},
        };

        NKikimr::NTestTli::CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

    Y_UNIT_TEST(ShouldAbortInCheckReadWhenTableIsDropped) {
        // Regression test for https://github.com/ydb-platform/ydb/issues/36147

        TTestHelper helper;
        auto& runtime = *helper.Server->GetRuntime();

        const auto& table = helper.Tables.at("table-1");
        const ui64 tabletId = table.TabletId;

        // Block immediate writes at BlockFailPoint
        TBlockOperationsFailPoint::TGuard blockGuard;

        // Send an immediate write — it will add itself to the dependency tracker
        // then block at BlockFailPoint. Its purpose is to block the DropTable operation until
        // blockGuard is unlocked.
        auto writeReq = helper.MakeWriteRequest("table-1", ++helper.TxId, {99, 99, 99, 99});
        runtime.SendToPipe(tabletId, helper.Sender, writeReq.release());
        runtime.WaitFor("write blocked", [&]{ return blockGuard.size() >= 1; });

        // Block TEvActivateLowExecution sent to the tablet executor to delay execution
        // of the TEvRead that we will send later.
        TBlockEvents<IEventHandle> blockedActivations(runtime,
            [&](const IEventHandle::TPtr& ev) {
                return ev->GetTypeName().Contains("TEvActivateLowExecution");
            });

        // Send a read without a snapshot — it is enqueued as a low-priority local tx and will
        // not execute until TEvActivateLowExecution is delivered to the tablet executor.
        auto request = GetBaseReadRequest(
            table.TableId, table.UserTable.GetDescription(), 1, NKikimrDataEvents::FORMAT_CELLVEC);
        AddRangeQuery<ui64>(*request, {Min<ui64>(),}, true, {Max<ui64>(),}, true);
        // Key point: this causes the read op to get upgraded to a repeatable read, and subsequently
        // get blocked on the DropTable in the ExecuteRead unit.
        request->Record.SetMaxRows(1);
        helper.SendReadAsync("table-1", request.release());

        runtime.WaitFor("blocked low-priority executor activation",
            [&]{ return blockedActivations.size() >= 1; });

        // Drop the table while the read is queued in the low priority executor queue.
        // Schema-change transactions go through the regular executor queue (TEvActivateExecution),
        // so they are not affected by the block above.
        AsyncDropTable(helper.Server, helper.Sender, "/Root", "table-1");

        runtime.SimulateSleep(TDuration::Seconds(1));
        // Unblock execution of the read. It will now get added to the pipeline and
        // would start executing if there were no additional checks on the presence of DropTable.
        blockedActivations.Stop().Unblock();

        runtime.SimulateSleep(TDuration::Seconds(1));
        // Unblock the DropTable execution.
        blockGuard.Unblock();

        auto readResult = helper.WaitReadResult(TDuration::Seconds(5));
        UNIT_ASSERT_C(readResult, "The read should not deadlock with the drop table op");
    }
}

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

        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_ARROW);

        auto readResult = helper.SendRead("table-1", request.release());
        const auto& record = readResult->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::UNSUPPORTED);
    }
}

Y_UNIT_TEST_SUITE(DataShardReadIteratorState) {
    Y_UNIT_TEST(ShouldCalculateQuota) {
        NDataShard::TReadIteratorState state(TReadIteratorId({}, 0), 0, TPathId(0, 0), {}, TRowVersion::Max(), true, {}, {});
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
}

Y_UNIT_TEST_SUITE(DataShardReadIteratorPageFaults) {
    Y_UNIT_TEST(CancelPageFaultedReadThenDropTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(0);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);
        // runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto opts = TShardedTableOptions()
                .ExecutorCacheSize(1 /* byte */);
        auto [shards, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)"));
        SimulateSleep(runtime, TDuration::Seconds(1));

        const auto shard1 = shards.at(0);
        CompactTable(runtime, shard1, tableId1, false);
        RebootTablet(runtime, shard1, sender);
        SimulateSleep(runtime, TDuration::Seconds(1));

        size_t observedReadResults = 0;
        bool captureCacheRequests = true;
        std::vector<std::unique_ptr<IEventHandle>> capturedCacheRequests;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
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

    Y_UNIT_TEST(LocksNotLostOnPageFault) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(0);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_INFO);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        // Use a policy that forces very small page sizes, effectively making each row on its own page
        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultTablePolicy();
        policy->MinDataPageSize = 1;

        auto opts = TShardedTableOptions()
                .Columns({{"key", "Int32", true, false},
                          {"index", "Int32", true, false},
                          {"value", "Int32", false, false}})
                .Policy(policy.Get())
                .ExecutorCacheSize(1 /* byte */);
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, index, value) VALUES (1, 0, 10), (3, 0, 30), (5, 0, 50), (7, 0, 70), (9, 0, 90);");
        runtime.SimulateSleep(TDuration::Seconds(1));

        const auto shard1 = shards.at(0);
        CompactTable(runtime, shard1, tableId, false);
        RebootTablet(runtime, shard1, sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Start a write transaction that has uncommitted write to key (2, 0)
        // This is because read iterator measures "work" in processed/skipped rows, so we have to give it something
        TString writeSessionId, writeTxId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, writeSessionId, writeTxId, R"(
                UPSERT INTO `/Root/table-1` (key, index, value) VALUES (2, 0, 20), (4, 0, 40);

                SELECT key, index, value FROM `/Root/table-1`
                WHERE key = 2
                ORDER BY key, index;
                )"),
            "{ items { int32_value: 2 } items { int32_value: 0 } items { int32_value: 20 } }");

        // Start a read transaction with several range read in a specific order
        // The first two prefixes don't exist (nothing committed yet)
        // The other two prefixes are supposed to page fault
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT key, index, value FROM `/Root/table-1`
                WHERE key IN (2, 4, 7, 9)
                ORDER BY key, index;
                )"),
            "{ items { int32_value: 7 } items { int32_value: 0 } items { int32_value: 70 } }, "
            "{ items { int32_value: 9 } items { int32_value: 0 } items { int32_value: 90 } }");

        // Commit the first transaction, it must succeed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, writeSessionId, writeTxId, "SELECT 1;"),
            "{ items { int32_value: 1 } }");

        // Commit the second transaction with a new upsert, it must not succeed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId,
                "UPSERT INTO `/Root/table-1` (key, index, value) VALUES (2, 0, 22);"),
            "ERROR: ABORTED");
    }
}

Y_UNIT_TEST_SUITE(DataShardReadIteratorConsistency) {

    Y_UNIT_TEST(LocalSnapshotReadWithPlanQueueRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        auto shardActor = ResolveTablet(runtime, shards.at(0));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (3, 30), (5, 50), (7, 70), (9, 90);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20), (4, 40), (6, 60), (8, 80);");

        std::vector<TEvDataShard::TEvRead::TPtr> reads;
        auto captureReads = runtime.AddObserver<TEvDataShard::TEvRead>([&](TEvDataShard::TEvRead::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured TEvRead for " << shardActor << Endl;
                reads.push_back(std::move(ev));
            }
        });

        std::vector<TEvTxProcessing::TEvPlanStep::TPtr> plans;
        auto capturePlans = runtime.AddObserver<TEvTxProcessing::TEvPlanStep>([&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured TEvPlanStep for " << shardActor << Endl;
                plans.push_back(std::move(ev));
            }
        });

        auto readFuture = KqpSimpleSend(runtime, R"(
            SELECT * FROM `/Root/table-1` ORDER BY key;
            )");

        auto upsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table-1` SELECT * FROM `/Root/table-2`;
            )");

        WaitFor(runtime, [&]{ return reads.size() > 0 && plans.size() > 0; }, "read and plan");

        captureReads.Remove();
        capturePlans.Remove();

        TRowVersion lastTx;
        for (auto& ev : plans) {
            auto* msg = ev->Get();
            for (auto& tx : msg->Record.GetTransactions()) {
                // Remember the last transaction in the plan
                lastTx = TRowVersion(msg->Record.GetStep(), tx.GetTxId());
            }
            runtime.Send(ev.Release(), 0, true);
        }
        plans.clear();

        for (auto& ev : reads) {
            auto* msg = ev->Get();
            // We expect it to be an immediate read
            UNIT_ASSERT_C(!msg->Record.HasSnapshot(), msg->Record.DebugString());
            // Limit each chunk to just 2 rows
            // This will force it to sleep and read in repeatable snapshot mode
            msg->Record.SetMaxRowsInResult(2);
            // Message must be immediate after plan in the mailbox
            runtime.Send(ev.Release(), 0, true);
        }
        reads.clear();

        std::vector<TEvDataShard::TEvReadContinue::TPtr> readContinues;
        auto captureReadContinues = runtime.AddObserver<TEvDataShard::TEvReadContinue>([&](TEvDataShard::TEvReadContinue::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured TEvReadContinue for " << shardActor << Endl;
                readContinues.push_back(std::move(ev));
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsertFuture))),
            "<empty>");

        captureReadContinues.Remove();
        for (auto& ev : readContinues) {
            runtime.Send(ev.Release(), 0, true);
        }
        readContinues.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            // Technically result without 2, 4, 6 and 8 is possible
            // In practice we will never block writes because of unfinished reads
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 40 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }, "
            "{ items { uint32_value: 6 } items { uint32_value: 60 } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 70 } }, "
            "{ items { uint32_value: 8 } items { uint32_value: 80 } }, "
            "{ items { uint32_value: 9 } items { uint32_value: 90 } }");
    }

    Y_UNIT_TEST(LocalSnapshotReadHasRequiredDependencies) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // We need to block transactions with readsets
            .SetEnableDataShardVolatileTransactions(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        auto shardActor = ResolveTablet(runtime, shards.at(0));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (3, 30), (5, 50), (7, 70);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20), (4, 40), (6, 60);");

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> readsets;
        auto captureReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured readset for " << ev->GetRecipientRewrite() << Endl;
                readsets.push_back(std::move(ev));
            }
        });

        // Block while writing to some keys
        auto upsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table-1` SELECT * FROM `/Root/table-2`;
        )");

        WaitFor(runtime, [&]{ return readsets.size() > 0; }, "readset");

        captureReadSets.Remove();

        auto modifyReads = runtime.AddObserver<TEvDataShard::TEvRead>([&](TEvDataShard::TEvRead::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... modifying TEvRead for " << shardActor << Endl;
                auto* msg = ev->Get();
                // We expect it to be an immediate read
                UNIT_ASSERT_C(!msg->Record.HasSnapshot(), msg->Record.DebugString());
                // Limit each chunk to just 2 rows
                // This will force it to sleep and read in repeatable snapshot mode
                msg->Record.SetMaxRowsInResult(2);
            }
        });

        // Read all rows, including currently undecided keys
        auto readFuture = KqpSimpleSend(runtime, R"(
            SELECT * FROM `/Root/table-1`
            WHERE key <= 5
            ORDER BY key;
            )");

        // Give read a chance to finish incorrectly
        runtime.SimulateSleep(TDuration::Seconds(1));

        for (auto& ev : readsets) {
            runtime.Send(ev.Release(), 0, true);
        }
        readsets.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsertFuture))),
            "<empty>");

        // We must have observed all rows at the given repeatable snapshot
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 40 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }");
    }

    Y_UNIT_TEST(LocalSnapshotReadNoUnnecessaryDependencies) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // We need to block transactions with readsets
            .SetEnableDataShardVolatileTransactions(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        auto shardActor = ResolveTablet(runtime, shards.at(0));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (3, 30), (5, 50), (7, 70);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20), (4, 40), (6, 60);");

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> readsets;
        auto captureReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured readset for " << ev->GetRecipientRewrite() << Endl;
                readsets.push_back(std::move(ev));
            }
        });

        // Block while writing to key 2
        auto upsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table-1` SELECT * FROM `/Root/table-2` WHERE key = 2;
        )");

        WaitFor(runtime, [&]{ return readsets.size() > 0; }, "readset");

        captureReadSets.Remove();

        auto modifyReads = runtime.AddObserver<TEvDataShard::TEvRead>([&](TEvDataShard::TEvRead::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... modifying TEvRead for " << shardActor << Endl;
                auto* msg = ev->Get();
                // We expect it to be an immediate read
                UNIT_ASSERT_C(!msg->Record.HasSnapshot(), msg->Record.DebugString());
                // Limit each chunk to just 2 rows
                // This will force it to sleep and read in repeatable snapshot mode
                msg->Record.SetMaxRowsInResult(2);
            }
        });

        // Read all rows, not including currently undecided keys
        auto readFuture = KqpSimpleSend(runtime, R"(
            SELECT * FROM `/Root/table-1`
            WHERE key >= 3
            ORDER BY key;
            )");

        // Read must complete without waiting for the above upsert to finish
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 70 } }");

        for (auto& ev : readsets) {
            runtime.Send(ev.Release(), 0, true);
        }
        readsets.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsertFuture))),
            "<empty>");
    }

    Y_UNIT_TEST(LocalSnapshotReadWithConcurrentWrites) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // We need to block transactions with readsets
            .SetEnableDataShardVolatileTransactions(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        auto shardActor = ResolveTablet(runtime, shards.at(0));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (3, 30), (5, 50), (7, 70);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20), (4, 40), (6, 60);");

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> readsets;
        auto captureReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                Cerr << "... captured readset for " << ev->GetRecipientRewrite() << Endl;
                readsets.push_back(std::move(ev));
            }
        });

        // The first upsert needs to block while writing to key 2
        auto upsertFuture1 = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table-1` SELECT * FROM `/Root/table-2` WHERE key = 2;
        )");

        WaitFor(runtime, [&]{ return readsets.size() > 0; }, "readset");

        captureReadSets.Remove();

        TRowVersion txVersion = TRowVersion::Min();
        auto observePlanSteps = runtime.AddObserver<TEvTxProcessing::TEvPlanStep>([&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                auto* msg = ev->Get();
                for (const auto& tx : msg->Record.GetTransactions()) {
                    txVersion = TRowVersion(msg->Record.GetStep(), tx.GetTxId());
                    Cerr << "... observed plan for tx " << txVersion << Endl;
                }
            }
        });

        // Start a transaction that reads from key 3
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT key, value FROM `/Root/table-1` WHERE key = 3;
            )"),
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }");

        // The second upsert should be ready to execute, but blocked by write-write conflict on key 2
        // Note we also read from key 3, so that later only one transaction may survive
        auto upsertFuture2 = KqpSimpleSend(runtime, R"(
            SELECT key, value FROM `/Root/table-1` WHERE key = 3;
            $rows = (
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
                UNION ALL
                SELECT 2u AS key, 21u AS value
                UNION ALL
                SELECT 3u AS key, 31u AS value
            );
            UPSERT INTO `/Root/table-1` SELECT * FROM $rows;
        )");

        WaitFor(runtime, [&]{ return txVersion != TRowVersion::Min(); }, "plan step");

        observePlanSteps.Remove();
        auto forceSnapshotRead = runtime.AddObserver<TEvDataShard::TEvRead>([&](TEvDataShard::TEvRead::TPtr& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                auto* msg = ev->Get();
                if (!msg->Record.HasSnapshot()) {
                    Cerr << "... forcing read snapshot " << txVersion << Endl;
                    txVersion.ToProto(msg->Record.MutableSnapshot());
                }
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 5
                ORDER BY key;
            )"),
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 70 } }");

        auto commitFuture = KqpSimpleSendCommit(runtime, sessionId, txId, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 32);
        )");

        // Give it all a chance to complete
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Unblock readsets
        for (auto& ev : readsets) {
            runtime.Send(ev.Release(), 0, true);
        }
        readsets.clear();

        auto result1 = FormatResult(AwaitResponse(runtime, std::move(upsertFuture2)));
        auto result2 = FormatResult(AwaitResponse(runtime, std::move(commitFuture)));

        UNIT_ASSERT_C(
            result1 == "ERROR: ABORTED" || result2 == "ERROR: ABORTED",
            "result1: " << result1 << ", "
            "result2: " << result2);
    }

    Y_UNIT_TEST(Bug_7674_IteratorDuplicateRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (6, 60), (7, 70), (8, 80), (9, 90), (10, 100);");
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto forceSmallChunks = runtime.AddObserver<TEvDataShard::TEvRead>(
            [&](TEvDataShard::TEvRead::TPtr& ev) {
                auto* msg = ev->Get();
                // Force chunks of at most 3 rows
                msg->Record.SetMaxRowsInResult(3);
            });

        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(runtime);
        TBlockEvents<TEvDataShard::TEvReadResult> blockedResults(runtime);
        TBlockEvents<TEvDataShard::TEvReadContinue> blockedContinue(runtime);

        auto waitFor = [&](const TString& description, const auto& condition, size_t count = 1) {
            while (!condition()) {
                UNIT_ASSERT_C(count > 0, "... failed to wait for " << description);
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                --count;
            }
        };

        auto readFuture = KqpSimpleSend(runtime, "SELECT key, value FROM `/Root/table-1` ORDER BY key LIMIT 7");
        waitFor("first TEvReadContinue", [&]{ return blockedContinue.size() >= 1; });
        waitFor("first TEvReadResult", [&]{ return blockedResults.size() >= 1; });

        blockedContinue.Unblock(1);
        waitFor("second TEvReadContinue", [&]{ return blockedContinue.size() >= 1; });
        waitFor("second TEvReadResult", [&]{ return blockedResults.size() >= 2; });

        // We need both results to arrive without pauses
        blockedResults.Unblock();

        waitFor("both TEvReadAcks", [&]{ return blockedAcks.size() >= 2; });

        // Unblock the first TEvReadAck and then pending TEvReadContinue
        blockedAcks.Unblock(1);
        blockedContinue.Unblock(1);

        // Give it some time to trigger the bug
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Stop blocking everything
        blockedAcks.Unblock().Stop();
        blockedResults.Unblock().Stop();
        blockedContinue.Unblock().Stop();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 40 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }, "
            "{ items { uint32_value: 6 } items { uint32_value: 60 } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 70 } }");
    }

    Y_UNIT_TEST(LeaseConfirmationNotOutOfOrder) {
        TTestHelper helper;

        auto& runtime = *helper.Server->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_TRACE);

        auto& table1 = helper.Tables.at("table-1");

        // Make sure the table is primed for reads
        auto request1 = GetBaseReadRequest(table1.TableId, table1.UserTable.GetDescription(), 1);
        AddRangeQuery<ui32>(*request1, { 1 }, true, { 3 }, true);
        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(table1.UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());

        // Slee for some time, this will make sure the lease expires and stops getting renewed
        runtime.SimulateSleep(TDuration::Seconds(10));

        // Start blocking new commits and leases for the tablet
        TBlockEvents<TEvBlobStorage::TEvPut> blockCommits(runtime,
            [tabletId = table1.TabletId](auto& ev) {
                if (ev->Get()->Id.TabletID() == tabletId) {
                    Cerr << "... blocking blob " << ev->Get()->Id << Endl;
                    return true;
                }
                return false;
            });
        TBlockEvents<TEvBlobStorage::TEvGetBlock> blockLeases(runtime,
            [tabletId = table1.TabletId](auto& ev) {
                if (ev->Get()->TabletId == tabletId) {
                    Cerr << "... blocking getblock " << ev->Get()->TabletId << Endl;
                    return true;
                }
                return false;
            });

        // Make one more request, this time forcing more than one result
        auto request2 = GetBaseReadRequest(table1.TableId, table1.UserTable.GetDescription(), 2);
        AddRangeQuery<ui32>(*request2, { 1 }, true, { 3 }, true);
        request2->Record.SetMaxRowsInResult(1);
        helper.SendReadAsync("table-1", request2.release());

        // Sleep for some time, giving the tablet a chance to produce out-of-order results
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C((blockCommits.size() + blockLeases.size()) > 0, "no commits or leases have been blocked");

        // Stop blocking commits and leases
        blockCommits.Stop().Unblock();
        blockLeases.Stop().Unblock();

        // The first result we receive should be with the first row
        auto readResult2 = helper.WaitReadResult();
        CheckResult(table1.UserTable, *readResult2, {
            {1, 1, 1, 100},
        });
        UNIT_ASSERT(!readResult2->Record.GetFinished());
    }

    Y_UNIT_TEST(BrokenWriteLockBeforeIteration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (42, 0, 1001),
                (42, 1, 1002);
        )");

        TString sessionId;
        TString txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                $maxIndex = (SELECT MAX(index) FROM `/Root/table` WHERE key = 42);

                UPSERT INTO `/Root/table` (key, index, value) VALUES
                    (42, COALESCE($maxIndex + 1, 0) + 0, 2001),
                    (42, COALESCE($maxIndex + 1, 0) + 1, 2002);

                SELECT count(*) FROM `/Root/table` WHERE key = 42;
            )"),
            "{ items { uint64_value: 4 } }"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                $maxIndex = (SELECT MAX(index) FROM `/Root/table` WHERE key = 42);

                UPSERT INTO `/Root/table` (key, index, value) VALUES
                    (42, COALESCE($maxIndex + 1, 0) + 0, 3001);
            )"),
            "<empty>"
        );

        // Transaction should abort instead of observing wild results like:
        // 0 -> 1001
        // 1 -> 1002
        // 3 -> 2002
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, R"(
                SELECT index, value FROM `/Root/table`
                WHERE key = 42
                ORDER BY index;
            )"),
            "ERROR: ABORTED"
        );
    }

    Y_UNIT_TEST(BrokenWriteLockDuringIteration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (42, 0, 1001),
                (42, 1, 1002);
        )");

        TString sessionId;
        TString txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                $maxIndex = (SELECT MAX(index) FROM `/Root/table` WHERE key = 42);

                UPSERT INTO `/Root/table` (key, index, value) VALUES
                    (42, COALESCE($maxIndex + 1, 0) + 0, 2001),
                    (42, COALESCE($maxIndex + 1, 0) + 1, 2002);

                SELECT count(*) FROM `/Root/table` WHERE key = 42;
            )"),
            "{ items { uint64_value: 4 } }"
        );

        TBlockEvents<TEvDataShard::TEvRead> blockedRead(runtime,
            [&](auto& ev) {
                ev->Get()->Record.SetMaxRowsInResult(1);
                return true;
            });
        auto readFuture = SendRequest(runtime, MakeSimpleRequestRPC(R"(
                SELECT index, value FROM `/Root/table`
                WHERE key = 42
                ORDER BY index;
            )", sessionId, txId, /* commit */ false));
        runtime.WaitFor("blocked read", [&]{ return blockedRead.size() >= 1; });
        blockedRead.Stop().Unblock();

        TBlockEvents<TEvDataShard::TEvReadContinue> blockedReadContinue(runtime);
        runtime.WaitFor("blocked read continue", [&]{ return blockedReadContinue.size() >= 1; });
        blockedReadContinue.Stop();

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                $maxIndex = (SELECT MAX(index) FROM `/Root/table` WHERE key = 42);

                UPSERT INTO `/Root/table` (key, index, value) VALUES
                    (42, COALESCE($maxIndex + 1, 0) + 0, 3001);
            )"),
            "<empty>"
        );

        blockedReadContinue.Unblock();

        // Transaction should abort instead of observing wild results like:
        // 0 -> 1001
        // 1 -> 1002
        // 3 -> 2002
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "ERROR: ABORTED"
        );
    }

    Y_UNIT_TEST(WriteLockThenUncommittedReadUpgradeRetryAndRestart) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // The bug requires restoring lock from persistent storage
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        THashMap<ui64, THashSet<TActorId>> shardPipes;
        auto shardPipeConnectObserver = runtime.AddObserver<TEvTabletPipe::TEvClientConnected>([&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Status == NKikimrProto::OK) {
                shardPipes[msg->TabletId].insert(ev->Sender);
            }
        });

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (2, 1002);
        )");

        TBlockEvents<TEvDataShard::TEvRead> blockedRead(runtime);

        TString sessionId;
        TString txId;
        auto writeReadFuture = KqpSimpleBeginSend(runtime, sessionId, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (3, 2003);
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )");
        runtime.WaitFor("blocked read", [&]{ return blockedRead.size() >= 1; });

        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommit(runtime, [&](const auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.TabletID() == shards.at(0) && msg->Id.Channel() == 0) {
                return true;
            }
            return false;
        });
        blockedRead.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // It's too difficult to make TEvRead across the network
        // Fake disconnect by closing all known pipes instead
        for (auto pipe : shardPipes[shards.at(0)]) {
            runtime.Send(new IEventHandle(pipe, sender, new TEvents::TEvPoison), 0, true);
        }
        shardPipes[shards.at(0)].clear();

        runtime.WaitFor("read retry", [&]{ return blockedRead.size() >= 1; });
        blockedRead.Stop().Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Force shard to restart by faking a blob storage error
        blockedCommit.Stop();
        for (auto& ev : blockedCommit) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        blockedCommit.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, txId, std::move(writeReadFuture)),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 3 } items { int32_value: 2003 } }"
        );

        // Execute a concurrent query that conflicts with the read above
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 3001);
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }"
        );

        // It's not serializable for both transactions to commit
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "ERROR: ABORTED"
        );
    }

    Y_UNIT_TEST(WriteLockThenUncommittedReadUpgradeRestartWithStateMigrationRetryAndRestartWithoutStateMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // This test requires state migration during the first restart
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (2, 1002);
        )");

        TBlockEvents<TEvDataShard::TEvRead> blockedRead(runtime);

        TString sessionId;
        TString txId;
        auto writeReadFuture = KqpSimpleBeginSend(runtime, sessionId, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (3, 2003);
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )");
        runtime.WaitFor("blocked read", [&]{ return blockedRead.size() >= 1; });

        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommit(runtime, [&](const auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.TabletID() == shards.at(0) && msg->Id.Channel() == 0) {
                return true;
            }
            return false;
        });
        blockedRead.Unblock();
        runtime.WaitFor("blocked commit", [&]{ return blockedCommit.size() >= 1; });

        // Force shard to restart by faking a blob storage error
        blockedCommit.Stop();
        for (auto& ev : blockedCommit) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        blockedCommit.clear();

        runtime.WaitFor("read retry", [&]{ return blockedRead.size() >= 1; });
        blockedRead.Stop().Unblock();

        // Note: currently we mark restored ranges as unpersisted and will
        // coincidentally persist them during read (since it's adding locks).
        // This test should fail if we ever start to optimize duplicate ranges,
        // in which case additional work will be needed.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, txId, std::move(writeReadFuture)),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 3 } items { int32_value: 2003 } }"
        );

        TBlockEvents<TEvDataShard::TEvInMemoryStateRequest> blockedStateRequest(runtime);

        Cerr << "... killing shard " << shards.at(0) << Endl;
        runtime.SendToPipe(shards.at(0), sender, new TEvents::TEvPoison);
        runtime.WaitFor("blocked state request", [&]{ return blockedStateRequest.size() >= 1; });

        for (auto& ev : blockedStateRequest) {
            Cerr << "... killing state actor " << ev->GetRecipientRewrite() << Endl;
            runtime.Send(new IEventHandle(ev->GetRecipientRewrite(), sender, new TEvents::TEvPoison), 0, true);
        }
        blockedStateRequest.Stop().Unblock();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Execute a concurrent query that conflicts with the read above
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 3001);
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }"
        );

        // It's not serializable for both transactions to commit
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "ERROR: ABORTED"
        );
    }

    Y_UNIT_TEST(WriteLockUncommittedConflictFailure) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // The bug requires restoring lock from persistent storage
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Populate tables with initial values
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (2, 1002);
        )");

        // Block reads so read locks are not established yet
        TBlockEvents<TEvDataShard::TEvRead> blockedReads(runtime);

        // Begin the 1st transaction upserting to and reading from table
        TString session1, tx1;
        auto future1 = KqpSimpleBeginSend(runtime, session1, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (3, 3003);
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )");

        // Begin the 2nd transaction upserting to and reading from table
        TString session2, tx2;
        auto future2 = KqpSimpleBeginSend(runtime, session2, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (4, 4004);
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )");

        // Wait until both transactions are stuck at their read phases
        runtime.WaitFor("blocked TEvRead x2", [&]{ return blockedReads.size() >= 2; });

        // Force read iterators to read rows one-by-one
        auto changeMaxRowsObserver = runtime.AddObserver<TEvDataShard::TEvRead>(
            [&](auto& ev) {
                auto* msg = ev->Get();
                msg->Record.SetMaxRowsInResult(1);
            });

        // Block TEvReadContinue so it stops after the first result
        TBlockEvents<TEvDataShard::TEvReadContinue> blockedReadContinue(runtime);

        // Unblock reads and wait for corresponding TEvReadContinue
        blockedReads.Stop().Unblock();

        runtime.WaitFor("blocked TEvReadContinue x2", [&]{ return blockedReadContinue.size() >= 2; });
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Block further commits
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.Channel() == 0 && msg->Id.TabletID() == shards.at(0)) {
                Cerr << "... blocking commit " << msg->Id << Endl;
                return true;
            }
            return false;
        });

        // Unblock TEvReadContinue and let kqp potentially receive results
        blockedReadContinue.Stop().Unblock();
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Stop blocking commits and make them fail (will cause shards to restart)
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, tx1, std::move(future1)),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 3 } items { int32_value: 3003 } }");
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, tx2, std::move(future2)),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 4 } items { int32_value: 4004 } }");

        // Commit the 1st transaction, it should succeed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session1, tx1, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }");

        // Try to commit the 2nd transaction, it must fail
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session2, tx2, R"(SELECT 1)"),
            "ERROR: ABORTED");
    }

    Y_UNIT_TEST(ReadLockConflictUncommittedRemoveMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // This test requires in-memory state migration to work
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Populate tables with initial values
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (1, 0, 1001),
                (1, 1, 1002),
                (11, 0, 2001);
        )");

        ui64 tx1lockId = 0;
        ui32 tx1lockNode = 0;
        auto shard1actor = ResolveTablet(runtime, shards.at(0));
        auto lockStatusObserver = runtime.AddObserver<TEvLongTxService::TEvLockStatus>(
            [&](auto& ev) {
                if (ev->GetRecipientRewrite() == shard1actor) {
                    auto* msg = ev->Get();
                    tx1lockId = msg->Record.GetLockId();
                    tx1lockNode = msg->Record.GetLockNode();
                }
            });

        // Begin the 1st transaction appending to key 1
        TString session1, tx1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session1, tx1, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 3003);
                SELECT key, index, value FROM `/Root/table` WHERE key = 11 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }\n"
            "{ items { int32_value: 11 } items { int32_value: 0 } items { int32_value: 2001 } }");

        runtime.WaitFor("subscribed lock", [&]{ return tx1lockId != 0; });
        lockStatusObserver.Remove();

        // Begin the 2nd transaction reading key 1
        TString session2, tx2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session2, tx2, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Make sure everything settles down
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Note: originally a graceful restart supposedly caused shards to send
        // restart notifications, which caused an internal error in kqp, which
        // aborted the commit and removed the lock. We send an unavailable lock
        // status here with a blocked and failing commit, which covers a broader
        // range of failures which could happen in practice.
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.Channel() == 0 && msg->Id.TabletID() == shards.at(0)) {
                Cerr << "... blocking commit " << msg->Id << Endl;
                return true;
            }
            return false;
        });
        runtime.Send(
            new IEventHandle(shard1actor, {},
                new TEvLongTxService::TEvLockStatus(tx1lockId, tx1lockNode, NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE)),
            0, true);
        runtime.WaitFor("blocked commits", [&]{ return blockedCommits.size() >= 1; });

        // Stop blocking commits and make them fail (will cause shards to restart)
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Commit the 1st transaction (it must abort, lock removal is migrated)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session1, tx1, R"(SELECT 1)"),
            "ERROR: ABORTED");

        // Commit the 2nd transaction writing to the same index
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session2, tx2, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 4003);
                )"),
            "<empty>");
    }

    Y_UNIT_TEST(ReadLockConflictUncommittedBreakMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // This test requires in-memory state migration to work
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Populate tables with initial values
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (1, 0, 1001),
                (1, 1, 1002),
                (11, 0, 2001);
        )");

        ui64 tx1lockId = 0;
        ui32 tx1lockNode = 0;
        auto shard1actor = ResolveTablet(runtime, shards.at(0));
        auto lockStatusObserver = runtime.AddObserver<TEvLongTxService::TEvLockStatus>(
            [&](auto& ev) {
                if (ev->GetRecipientRewrite() == shard1actor) {
                    auto* msg = ev->Get();
                    tx1lockId = msg->Record.GetLockId();
                    tx1lockNode = msg->Record.GetLockNode();
                }
            });

        // Begin the 1st transaction appending to key 1
        TString session1, tx1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session1, tx1, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 3003);
                SELECT key, index, value FROM `/Root/table` WHERE key = 2 ORDER BY key, index;
                SELECT key, index, value FROM `/Root/table` WHERE key = 11 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }"
            "\n"
            // empty result set
            "\n"
            "{ items { int32_value: 11 } items { int32_value: 0 } items { int32_value: 2001 } }");

        runtime.WaitFor("subscribed lock", [&]{ return tx1lockId != 0; });
        lockStatusObserver.Remove();

        // Begin the 2nd transaction reading key 1
        TString session2, tx2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session2, tx2, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Make sure everything settles down
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Block commits and try to break the first transaction by a blind upsert
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.Channel() == 0 && msg->Id.TabletID() == shards.at(0)) {
                Cerr << "... blocking commit " << msg->Id << Endl;
                return true;
            }
            return false;
        });
        KqpSimpleSend(runtime, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (2, 0, 4001);
            )");
        runtime.WaitFor("blocked commits", [&]{ return blockedCommits.size() >= 1; });

        // Stop blocking commits and make them fail (will cause shards to restart)
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Commit the 1st transaction (it must abort, lock break is migrated)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session1, tx1, R"(SELECT 1)"),
            "ERROR: ABORTED");

        // Commit the 2nd transaction writing to the same index
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session2, tx2, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 4003);
                )"),
            "<empty>");
    }

    Y_UNIT_TEST(ReadLockUncommittedRemoveThenNewConflictMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // This test requires in-memory state migration to work
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Populate tables with initial values
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (1, 0, 1001),
                (1, 1, 1002),
                (11, 0, 2001);
        )");

        ui64 tx1lockId = 0;
        ui32 tx1lockNode = 0;
        auto shard1actor = ResolveTablet(runtime, shards.at(0));
        auto lockStatusObserver = runtime.AddObserver<TEvLongTxService::TEvLockStatus>(
            [&](auto& ev) {
                if (ev->GetRecipientRewrite() == shard1actor) {
                    auto* msg = ev->Get();
                    tx1lockId = msg->Record.GetLockId();
                    tx1lockNode = msg->Record.GetLockNode();
                }
            });

        // Begin the 1st transaction appending to key 1
        TString session1, tx1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session1, tx1, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 3003);
                SELECT key, index, value FROM `/Root/table` WHERE key = 2 ORDER BY key, index;
                SELECT key, index, value FROM `/Root/table` WHERE key = 11 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }"
            "\n"
            // empty result set
            "\n"
            "{ items { int32_value: 11 } items { int32_value: 0 } items { int32_value: 2001 } }");

        runtime.WaitFor("subscribed lock", [&]{ return tx1lockId != 0; });
        lockStatusObserver.Remove();

        // Begin the 2nd transaction reading key 1
        TString session2, tx2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session2, tx2, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Block reads, so the next tx uses the current snapshot, but executes much later
        TBlockEvents<TEvDataShard::TEvRead> blockedReads(runtime);

        // Begin the 3rd transaction reading key 1
        TString session3, tx3;
        auto readFuture = KqpSimpleBeginSend(runtime, session3, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
            )");
        runtime.WaitFor("blocked read", [&]{ return blockedReads.size() >= 1; });
        blockedReads.Stop();

        // Make sure everything settles down
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Block commits and try to simulate a lock timeout which starts a rollback
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.Channel() == 0 && msg->Id.TabletID() == shards.at(0)) {
                Cerr << "... blocking commit " << msg->Id << Endl;
                return true;
            }
            return false;
        });
        runtime.Send(
            new IEventHandle(shard1actor, {},
                new TEvLongTxService::TEvLockStatus(tx1lockId, tx1lockNode, NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE)),
            0, true);
        runtime.WaitFor("blocked commits", [&]{ return blockedCommits.size() >= 1; });

        // Unblock the 3rd transaction and ensure the read succeeds
        blockedReads.Unblock();
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, tx3, std::move(readFuture)),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Stop blocking commits and make them fail (will cause shards to restart)
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Commit the 1st transaction (it must abort, lock removal is migrated)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session1, tx1, R"(SELECT 1)"),
            "ERROR: ABORTED");

        // Commit the 3rd transaction writing to the same index
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session3, tx3, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 4003);
                )"),
            "<empty>");
    }

    Y_UNIT_TEST(ReadLockUncommittedBreakThenNewConflictMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);

        // This test requires in-memory state migration to work
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, index int, value int, PRIMARY KEY (key, index))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Populate tables with initial values
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (1, 0, 1001),
                (1, 1, 1002),
                (11, 0, 2001);
        )");

        ui64 tx1lockId = 0;
        ui32 tx1lockNode = 0;
        auto shard1actor = ResolveTablet(runtime, shards.at(0));
        auto lockStatusObserver = runtime.AddObserver<TEvLongTxService::TEvLockStatus>(
            [&](auto& ev) {
                if (ev->GetRecipientRewrite() == shard1actor) {
                    auto* msg = ev->Get();
                    tx1lockId = msg->Record.GetLockId();
                    tx1lockNode = msg->Record.GetLockNode();
                }
            });

        // Begin the 1st transaction appending to key 1
        TString session1, tx1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session1, tx1, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 3003);
                SELECT key, index, value FROM `/Root/table` WHERE key = 2 ORDER BY key, index;
                SELECT key, index, value FROM `/Root/table` WHERE key = 11 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }"
            "\n"
            // empty result set
            "\n"
            "{ items { int32_value: 11 } items { int32_value: 0 } items { int32_value: 2001 } }");

        runtime.WaitFor("subscribed lock", [&]{ return tx1lockId != 0; });
        lockStatusObserver.Remove();

        // Begin the 2nd transaction reading key 1
        TString session2, tx2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, session2, tx2, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
                )"),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Block reads, so the next tx uses the current snapshot, but executes much later
        TBlockEvents<TEvDataShard::TEvRead> blockedReads(runtime);

        // Begin the 3rd transaction reading key 1
        TString session3, tx3;
        auto readFuture = KqpSimpleBeginSend(runtime, session3, R"(
                SELECT key, index, value FROM `/Root/table` WHERE key = 1 ORDER BY key, index;
            )");
        runtime.WaitFor("blocked read", [&]{ return blockedReads.size() >= 1; });
        blockedReads.Stop();

        // Make sure everything settles down
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Block commits and try to break the first transaction by a blind upsert
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime, [&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Id.Channel() == 0 && msg->Id.TabletID() == shards.at(0)) {
                Cerr << "... blocking commit " << msg->Id << Endl;
                return true;
            }
            return false;
        });
        KqpSimpleSend(runtime, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (2, 0, 4001);
            )");
        runtime.WaitFor("blocked commits", [&]{ return blockedCommits.size() >= 1; });

        // Unblock the 3rd transaction and ensure the read succeeds
        blockedReads.Unblock();
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBeginWait(runtime, tx3, std::move(readFuture)),
            "{ items { int32_value: 1 } items { int32_value: 0 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 1 } items { int32_value: 1 } items { int32_value: 1002 } }");

        // Stop blocking commits and make them fail (will cause shards to restart)
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto response = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, response.release()), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Commit the 1st transaction (it must abort, lock break is migrated)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session1, tx1, R"(SELECT 1)"),
            "ERROR: ABORTED");

        // Commit the 3rd transaction writing to the same index
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, session3, tx3, R"(
                UPSERT INTO `/Root/table` (key, index, value) VALUES (1, 2, 4003);
                )"),
            "<empty>");
    }

}

Y_UNIT_TEST_SUITE(DataShardReadIteratorLatency) {

    Y_UNIT_TEST(ReadSplitLatency) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        TServer::TPtr server = new TServer(serverSettings);

        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        // Insert initial data
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (6, 60), (7, 70), (8, 80), (9, 90), (10, 100);");

        // Copy table (this will ensure original shards stay alive after split)
        {
            auto senderCopy = runtime.AllocateEdgeActor();
            ui64 txId = AsyncCreateCopyTable(server, senderCopy, "/Root", "table-2", "/Root/table-1");
            WaitTxNotification(server, senderCopy, txId);
        }

        TBlockEvents<TEvDataShard::TEvRead> blockedReads(runtime);

        Cerr << "... starting read from table-1" << Endl;
        TString readSessionId;
        auto readFuture = KqpSimpleBeginSend(runtime, readSessionId, R"(
            SELECT * FROM `/Root/table-1` ORDER BY key;
            )");

        runtime.WaitFor("blocked TEvRead", [&]{ return blockedReads.size() >= 1; });

        {
            Cerr << "... splitting table-1" << Endl;
            SetSplitMergePartCountLimit(server->GetRuntime(), -1);
            auto shards1before = GetTableShards(server, sender, "/Root/table-1");
            ui64 txId = AsyncSplitTable(server, sender, "/Root/table-1", shards1before.at(0), 5);
            Cerr << "... split txId# " << txId << " started" << Endl;
            WaitTxNotification(server, sender, txId);
            Cerr << "... split txId# " << txId << " finished" << Endl;
        }

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        auto readStartTs = runtime.GetCurrentTime();
        blockedReads.Unblock();
        blockedReads.Stop();
        auto readResponse = runtime.WaitFuture(std::move(readFuture));
        UNIT_ASSERT_VALUES_EQUAL(readResponse.operation().status(), Ydb::StatusIds::SUCCESS);
        auto readLatency = runtime.GetCurrentTime() - readStartTs;
        Cerr << "... read latency was " << readLatency << Endl;
        UNIT_ASSERT_C(readLatency < TDuration::MilliSeconds(100),
            "unexpected read latency " << readLatency);
    }

}

Y_UNIT_TEST_SUITE(DataShardReadIteratorBatchMode) {

    Y_UNIT_TEST(RangeFull) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { }, true, { }, true);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803},
            {11, 11, 11, 1111},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(RangeFromInclusive) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { 8 }, true, { }, true);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803},
            {11, 11, 11, 1111},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(RangeFromNonInclusive) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { 8 }, false, { }, true);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {11, 11, 11, 1111},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(RangeToInclusive) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { }, true, { 8 }, true);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(RangeToNonInclusive) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { }, true, { 8 }, false);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(MultipleRanges) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { 1 }, true, { 1 }, true);
        AddRangeQuery<ui32>(*request1, { 5 }, true, { 5 }, true);
        AddRangeQuery<ui32>(*request1, { 3 }, true, { 3 }, true);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
            {5, 5, 5, 500},
            {3, 3, 3, 300},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());
    }

    Y_UNIT_TEST(SelectingColumns) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { }, true, { }, true);

        std::vector<ui32> columns{ 4, 3 };
        request1->Record.ClearColumns();
        for (ui32 column : columns) {
            request1->Record.AddColumns(column);
        }

        request1->Record.SetMaxRows(5);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {100, 1},
            {300, 3},
            {500, 5},
            {800, 0},
            {801, 1},
        }, columns);
        UNIT_ASSERT(!readResult1->Record.GetFinished());
        UNIT_ASSERT(readResult1->Record.GetLimitReached());

        UNIT_ASSERT(readResult1->Record.HasContinuationToken());
        NKikimrTxDataShard::TReadContinuationToken token;
        UNIT_ASSERT(token.ParseFromString(readResult1->Record.GetContinuationToken()));
        UNIT_ASSERT_VALUES_EQUAL(token.GetFirstUnprocessedQuery(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(token.GetLastProcessedKey(), TSerializedCellVec::Serialize(ToCells<ui32>({ 8, 0, 1 })));
    }

    Y_UNIT_TEST(ShouldHandleReadAck) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        AddRangeQuery<ui32>(*request1, { 1 }, true, { 8 }, true);

        // limit quota
        request1->Record.SetMaxRows(1);

        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables["table-1"].UserTable, *readResult1, {
            {1, 1, 1, 100},
        });
        UNIT_ASSERT(readResult1->Record.GetLimitReached());

        helper.SendReadAck("table-1", readResult1->Record, 2, 10000);

        auto readResult2 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult2, {
            {3, 3, 3, 300},
            {5, 5, 5, 500},
        });
        UNIT_ASSERT(readResult2->Record.GetLimitReached());

        helper.SendReadAck("table-1", readResult2->Record, 100, 10000);

        auto readResult3 = helper.WaitReadResult();
        CheckResult(helper.Tables["table-1"].UserTable, *readResult3, {
            {8, 0, 0, 800},
            {8, 0, 1, 801},
            {8, 1, 0, 802},
            {8, 1, 1, 803},
        });
        UNIT_ASSERT(readResult3->Record.GetFinished());
        UNIT_ASSERT_VALUES_EQUAL(readResult3->Record.GetReadId(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(readResult3->Record.GetSeqNo(), 3UL);
    }

}

Y_UNIT_TEST_SUITE(DataShardReadIteratorFastCancel) {

    Y_UNIT_TEST(ShouldProcessFastCancel) {
        TTestHelper helper;

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddRangeQuery<ui32>(*request1, { 1 }, true, { 1 }, true);
        auto readResult1 = helper.SendRead("table-1", request1.release());
        CheckResult(helper.Tables.at("table-1").UserTable, *readResult1, {
            {1, 1, 1, 100},
        });
        UNIT_ASSERT(readResult1->Record.GetFinished());

        auto& runtime = *helper.Server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        auto shardActor = ResolveTablet(runtime, helper.Tables.at("table-1").TabletId);

        auto request2 = helper.GetBaseReadRequest("table-1", 2);
        AddRangeQuery<ui32>(*request2, { 1 }, true, { 1 }, true);
        auto request3 = helper.GetBaseReadRequest("table-1", 3);
        AddRangeQuery<ui32>(*request3, { 1 }, true, { 1 }, true);
        auto cancel2 = std::make_unique<TEvDataShard::TEvReadCancel>();
        cancel2->Record.SetReadId(2);
        auto cancel3 = std::make_unique<TEvDataShard::TEvReadCancel>();
        cancel3->Record.SetReadId(3);

        runtime.Send(new IEventHandle(shardActor, sender, request2.release()), 0, true);
        runtime.Send(new IEventHandle(shardActor, sender, request3.release()), 0, true);
        runtime.Send(new IEventHandle(shardActor, sender, cancel2.release()), 0, true);
        runtime.Send(new IEventHandle(shardActor, sender, cancel3.release()), 0, true);

        auto ev = runtime.GrabEdgeEvent<TEvDataShard::TEvReadResult>(sender, TDuration::Seconds(1));
        UNIT_ASSERT_C(!ev, "Unexpected response received (should have been cancelled)");
    }

}

Y_UNIT_TEST_SUITE(DataShardReadIteratorVectorTopK) {
    Y_UNIT_TEST(HnswMultipleIndexMetadataSurvivesRestartAndDrop) {
        NKikimrSchemeOp::TTableDescription schema;
        schema.SetPath("/Root/posting");
        for (ui32 id : {2u, 3u}) {
            auto* index = schema.AddTableIndexes();
            index->SetName(TStringBuilder() << "hnsw" << id);
            index->SetPathOwnerId(1);
            index->SetLocalPathId(id);
            index->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
            index->SetSchemaVersion(id + 10);
            auto* description = index->MutableHnswIndexDescription();
            description->SetEmbeddingColumnId(id + 40);
            description->MutableSettings()->mutable_settings()->set_vector_dimension(2);
        }
        TUserTable table(1, schema, 0);
        UNIT_ASSERT_VALUES_EQUAL(table.HnswIndexes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(table.HnswIndexes.at(TPathId(1, 2)).VectorColumnTag, 42);
        UNIT_ASSERT_VALUES_EQUAL(table.HnswIndexes.at(TPathId(1, 3)).SchemaVersion, 13);
        table.GetSchema(schema);
        TUserTable restored(1, schema, 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.HnswIndexes.size(), 2);
        restored.DropIndex(TPathId(1, 2));
        restored.GetSchema(schema);
        TUserTable dropped(1, schema, 0);
        UNIT_ASSERT_VALUES_EQUAL(dropped.HnswIndexes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dropped.HnswIndexes.at(TPathId(1, 3)).VectorColumnTag, 43);
    }

    Y_UNIT_TEST(HnswIndexMetadataAddDropAndRestart) {
        NKikimrSchemeOp::TTableDescription schema;
        schema.SetPath("/Root/posting");
        TUserTable table(1, schema, 0);

        NKikimrSchemeOp::TIndexDescription index;
        index.SetName("hnsw");
        index.SetPathOwnerId(1);
        index.SetLocalPathId(2);
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
        auto* description = index.MutableHnswIndexDescription();
        description->MutableSettings()->mutable_settings()->set_vector_dimension(2);
        // The logical index on the base table has no posting-table column ID.
        auto baseSchema = schema;
        baseSchema.AddTableIndexes()->CopyFrom(index);
        TUserTable baseTable(1, baseSchema, 0);
        UNIT_ASSERT(baseTable.HnswIndexes.empty());

        description->SetEmbeddingColumnId(42);
        table.AddIndex(index);
        UNIT_ASSERT_VALUES_EQUAL(table.HnswIndexes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.HnswIndexes.begin()->second.VectorColumnTag, 42);
        table.GetSchema(schema);
        TUserTable restored(1, schema, 0);
        UNIT_ASSERT_VALUES_EQUAL(restored.HnswIndexes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(restored.HnswIndexes.begin()->second.VectorColumnTag, 42);

        restored.DropIndex(TPathId(1, 2));
        UNIT_ASSERT(restored.HnswIndexes.empty());
        restored.GetSchema(schema);
        TUserTable dropped(1, schema, 0);
        UNIT_ASSERT(dropped.HnswIndexes.empty());
    }


    Y_UNIT_TEST_TWIN(HnswRequiresDeclaredVectorColumn, WrongColumn) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings);
        helper.CreateCustomTable("exact-vector", {{"key", "Uint32", true, false}, {"emb", "String", false, false},
                                                  {"other", "String", false, false}}, WrongColumn);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/exact-vector` (key, emb) VALUES
                (1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                (2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(
            ResolveTablet(runtime, helper.Tables.at("exact-vector").TabletId)));
        UNIT_ASSERT(shard);
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        for (ui64 readId = 1; readId <= 5; ++readId) {
            auto request = helper.GetBaseReadRequest("exact-vector", readId, NKikimrDataEvents::FORMAT_CELLVEC);
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(1); // HNSW metadata, if present, belongs to column 3.
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            auto* vector = top->MutableSettings();
            vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vector->set_vector_dimension(2);
            vector->set_min_rows(1);
            auto result = helper.SendRead("exact-vector", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[0].template AsValue<ui32>(), 1u);
            runtime.SimulateSleep(TDuration::Seconds(6));
        }
        UNIT_ASSERT_VALUES_EQUAL(shard->GetHnswBuildToken(tid), 0u);
        UNIT_ASSERT(!shard->IsHnswCacheMemoryLimitKnown());
    }

    void TestHnswSmallShardBuildResumesAfterGrowth(bool growthDuringScan, bool lowerMinRows = false, bool emptyShard = false) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings);
        helper.CreateCustomTable("small-hnsw", {{"key", "Uint32", true, false}, {"emb", "String", false, false}}, true);
        if (!emptyShard) {
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/small-hnsw` (key, emb) VALUES
                    (1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                    (2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
            )");
        }
        auto& runtime = *helper.Server->GetRuntime();
        auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(
            ResolveTablet(runtime, helper.Tables.at("small-hnsw").TabletId)));
        UNIT_ASSERT(shard);
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        TVector<TAutoPtr<IEventHandle>> pending;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            constexpr ui32 buildResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
            if (growthDuringScan && ev->GetTypeRewrite() == buildResult) {
                pending.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER {
            runtime.SetObserverFunc(observer);
        };
        ui64 readId = 0;
        ui64 minRows = emptyShard ? 0 : 3;
        auto read = [&] {
            auto request = helper.GetBaseReadRequest("small-hnsw", ++readId, NKikimrDataEvents::FORMAT_CELLVEC);
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(1);
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            auto* vector = top->MutableSettings();
            vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vector->set_vector_dimension(2);
            vector->set_min_rows(minRows);
            auto result = helper.SendRead("small-hnsw", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            return result->Record.GetStats().GetRows();
        };
        for (ui32 attempt = 0; attempt < 20 && !shard->GetHnswBuildToken(tid); ++attempt) {
            UNIT_ASSERT_VALUES_EQUAL(read(), emptyShard ? 0u : 2u);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        if (growthDuringScan) {
            runtime.WaitFor("small HNSW snapshot scan", [&] { return !pending.empty(); });
        }
        const auto token = shard->GetHnswBuildToken(tid);
        UNIT_ASSERT(token);
        for (ui32 attempt = 0; attempt < 5; ++attempt) {
            runtime.SimulateSleep(TDuration::Seconds(6));
            UNIT_ASSERT_VALUES_EQUAL(read(), emptyShard ? 0u : 2u);
            UNIT_ASSERT_VALUES_EQUAL(shard->GetHnswBuildToken(tid), token);
        }
        if (lowerMinRows) {
            minRows = 1;
        } else {
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/small-hnsw` (key, emb) VALUES
                    (3, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            )");
        }
        runtime.SetObserverFunc(observer);
        for (auto& event : pending) {
            runtime.Send(event.Release());
        }
        bool cached = false;
        for (ui32 attempt = 0; attempt < 20 && !cached; ++attempt) {
            cached = read() == 1 && bool(shard->GetHnswIndex(tid, 2,
                                                             shard->GetUserTables().begin()->second->HnswIndexes.begin()->second.Settings, false));
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_C(cached, "growth did not re-enable the HNSW build");
    }

    Y_UNIT_TEST(HnswSmallShardBuildResumesAfterGrowth) {
        TestHnswSmallShardBuildResumesAfterGrowth(false);
    }

    Y_UNIT_TEST(HnswSmallShardGrowthDuringScan) {
        TestHnswSmallShardBuildResumesAfterGrowth(true);
    }

    Y_UNIT_TEST(HnswSmallShardBuildResumesAfterSettingsChange) {
        TestHnswSmallShardBuildResumesAfterGrowth(false, true);
    }

    Y_UNIT_TEST(HnswEmptyShardDisablesBuildWithZeroMinRows) {
        TestHnswSmallShardBuildResumesAfterGrowth(false, false, true);
    }

    Y_UNIT_TEST_TWIN(HnswFeatureFlagCache, DisableDuringBuild) {
        constexpr bool Followers = false;
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings, 1, Followers);
        helper.CreateCustomTable("hnsw-flag", {{"parent", "Uint32", true, false}, {"key", "Uint32", true, false},
                                               {"emb", "String", false, false}}, true);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-flag` (parent, key, emb) VALUES
                (1, 1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                (1, 2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        const auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/hnsw-flag"}, TDuration::Hours(1));
        runtime.SimulateSleep(TDuration::Seconds(2));
        ui64 readId = 0;
        auto read = [&] {
            auto request = helper.GetBaseReadRequest("hnsw-flag", ++readId,
                                                     NKikimrDataEvents::FORMAT_CELLVEC, Followers ? snapshot : TRowVersion{});
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(2);
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            auto* vectorSettings = top->MutableSettings();
            vectorSettings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            vectorSettings->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vectorSettings->set_vector_dimension(2);
            vectorSettings->set_min_rows(1);
            auto result = helper.SendRead("hnsw-flag", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[1].template AsValue<ui32>(), 1u);
            return result->Record.GetStats().GetRows();
        };
        auto setEnabled = [&](bool enabled) {
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                runtime.GetAppData(node).FeatureFlags.SetEnableHnswIndex(enabled);
            }
        };
        // Default-off reads stay correct and never become accelerated.
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read(), 2u);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        if (DisableDuringBuild) {
            TVector<TAutoPtr<IEventHandle>> pending;
            constexpr ui32 buildResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
            auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == buildResult) {
                    pending.push_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
            Y_DEFER {
                runtime.SetObserverFunc(observer);
            };
            setEnabled(true);
            for (ui32 attempt = 0; attempt < 30 && pending.empty(); ++attempt) {
                read();
                runtime.SimulateSleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT_C(!pending.empty(), "HNSW build did not produce a result");
            setEnabled(false);
            UNIT_ASSERT_VALUES_EQUAL(read(), 2u);
            runtime.SetObserverFunc(observer);
            for (auto& ev : pending) {
                runtime.Send(ev.Release());
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
            UNIT_ASSERT_VALUES_EQUAL(read(), 2u);
        }
        for (ui32 cycle = 0; cycle < 2; ++cycle) {
            setEnabled(true);
            bool accelerated = false;
            for (ui32 attempt = 0; attempt < 30; ++attempt) {
                if (read() == 1) {
                    accelerated = true;
                    break;
                }
                runtime.SimulateSleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT_C(accelerated, "enabling HNSW did not enable the cache");
            setEnabled(false);
            UNIT_ASSERT_VALUES_EQUAL(read(), 2u);
        }
    }

    Y_UNIT_TEST_TWIN(HnswPrefixRangeUsesLegacyBorders, Distinct) {
        constexpr bool Followers = false;
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        serverSettings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(serverSettings, 1, Followers);
        helper.CreateCustomTable("table-vector-prefix", {{"parent", "Uint64", true, false},
                                                         {"key", "Uint64", true, false},
                                                         {"emb", "String", false, false}}, true, {}, [](auto& table) {
            table.MutableTableIndexes(0)->MutableHnswIndexDescription()->MutableSettings()->mutable_settings()
                ->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        });
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-vector-prefix` (parent, key, emb) VALUES
                (9223372036854775809ul, 1ul, "\x00\x00\xC8\x42\x00\x00\x00\x00\x01"),
                (9223372036854775810ul, 2ul, "\x00\x00\x00\x40\x00\x00\x00\x00\x01"),
                (9223372036854775810ul, 3ul, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                (9223372036854775811ul, 4ul, "\x00\x00\x48\x43\x00\x00\x00\x00\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(2));
        constexpr ui64 previous = (ui64(1) << 63) + 1;
        constexpr ui64 selected = previous + 1;
        ui64 readId = 0;
        auto read = [&](bool restricted) {
            // Avoid creating a new volatile snapshot on every probe: that
            // advances the follower edge and prevents a repeatable HEAD cache.
            auto request = helper.GetBaseReadRequest("table-vector-prefix", ++readId,
                                                     NKikimrDataEvents::FORMAT_CELLVEC, TRowVersion::Max());
            request->Record.ClearSnapshot();
            if (restricted) {
                // Exactly the legacy prefix range produced by VectorSearch:
                // (previous,+inf) .. (selected,+inf], excluding ALL previous keys.
                AddRangeQuery<ui64>(*request, {previous}, false, {selected}, true);
            } else {
                AddRangeQuery<ui64>(*request, {}, true, {}, true);
            }
            auto* topK = request->Record.MutableVectorTopK();
            topK->SetColumn(2);
            topK->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            topK->SetLimit(1);
            if (Distinct) {
                topK->AddDistinctColumns(1);
            }
            auto* settings = topK->MutableSettings();
            settings->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
            settings->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            settings->set_vector_dimension(2);
            settings->set_min_rows(1);
            return helper.SendRead("table-vector-prefix", request.release());
        };
        const auto cold = read(true);
        UNIT_ASSERT_VALUES_EQUAL(cold->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(cold->GetRowsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cold->GetCells(0)[0].template AsValue<ui64>(), selected);
        bool accelerated = false;
        for (ui32 attempt = 0; attempt < 20; ++attempt) {
            auto result = read(false);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            if (result->Record.GetStats().GetRows() == 1) {
                accelerated = true;
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_C(accelerated, "full-range graph did not become ready");
        auto result = read(true);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->Record.GetFinished());
        CheckResult(helper.Tables.at("table-vector-prefix").UserTable, *result, {
                                                                                    {TCell::Make(selected), TCell::Make(ui64(2)),
                                                                                     TCell("\x00\x00\x00\x40\x00\x00\x00\x00\x01", 9)},
                                                                                },
                    {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
                     NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
                     NScheme::TTypeInfo(NScheme::NTypeIds::String)});
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2);
    }

    void TestHnswDistinctCandidateBudget(ui32 duplicateRows, bool withChanges) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings);
        helper.CreateCustomTable("hnsw-distinct", {{"parent", "Uint32", true, false},
                                                 {"key", "Uint32", true, false},
                                                 {"emb", "String", false, false}}, true);
        auto writeRows = [&](bool changed) {
            TStringBuilder sql;
            sql << "UPSERT INTO `/Root/hnsw-distinct` (parent, key, emb) VALUES ";
            for (ui32 parent = 0; parent <= duplicateRows; ++parent) {
                if (parent) {
                    sql << ", ";
                }
                const bool far = parent == duplicateRows;
                sql << "(" << parent << ", " << (far ? 2 : 1) << ", "
                    << (far ? "\"\\x00\\x00\\x80\\xBF" : "\"\\x00\\x00\\x80\\x3F")
                    << (changed ? "\\x00\\x00\\x00\\x3F\\x01\")" : "\\x00\\x00\\x00\\x00\\x01\")");
            }
            // Duplicate keys are closest to (1, 0); key 2 is farther away.
            // Changing the second component from 0 to 0.5 preserves this order.
            sql << ";";
            ExecSQL(helper.Server, helper.Sender, sql);
        };
        writeRows(false);

        auto& runtime = *helper.Server->GetRuntime();
        ui64 readId = 0;
        auto read = [&](bool distinct) {
            auto request = helper.GetBaseReadRequest("hnsw-distinct", ++readId,
                NKikimrDataEvents::FORMAT_CELLVEC, TRowVersion::Max());
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(2);
            top->SetLimit(distinct ? 2 : 1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            if (distinct) {
                top->AddDistinctColumns(1);
            }
            auto* vector = top->MutableSettings();
            vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vector->set_vector_dimension(2);
            vector->set_min_rows(1);
            vector->set_delta_rows(1000);
            auto result = helper.SendRead("hnsw-distinct", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            return result;
        };
        bool accelerated = false;
        for (ui32 attempt = 0; attempt < 30; ++attempt) {
            if (read(false)->Record.GetStats().GetRows() == 1) {
                accelerated = true;
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_C(accelerated, "HNSW graph did not become ready");
        if (withChanges) {
            writeRows(true);
            auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(
                ResolveTablet(runtime, helper.Tables.at("hnsw-distinct").TabletId)));
            UNIT_ASSERT(shard);
            const auto& table = *shard->GetUserTables().begin()->second;
            auto graph = shard->GetHnswIndex(table.LocalTid, 3, table.HnswIndexes.begin()->second.Settings, false);
            UNIT_ASSERT(graph);
            UNIT_ASSERT_VALUES_EQUAL(graph->ChangeCount(), duplicateRows + 1);
        }

        auto result = read(true);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[1].AsValue<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(1)[1].AsValue<ui32>(), 2);
        // Enough duplicates must exhaust the ANN budget and take the exact scan,
        // even though wider graph searches could eventually find the second key.
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), duplicateRows > 2 ? duplicateRows + 1 : 2);
    }

    Y_UNIT_TEST_TWIN(HnswDistinctCandidateBudgetFallsBack, WithChanges) {
        TestHnswDistinctCandidateBudget(16, WithChanges);
    }

    Y_UNIT_TEST(HnswDistinctCandidateBudgetAllowsOneExpansion) {
        TestHnswDistinctCandidateBudget(2, false);
    }

    Y_UNIT_TEST_TWIN(HnswMaterializationRetriesExternalBlobs, ExternalEmbedding) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true)
            .AddStoragePool("ssd").AddStoragePool("hdd").AddStoragePool("ext");
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings);
        const TShardedTableOptions::TFamily family = {
            .Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd",
            .ExternalPoolKind = "ext", .DataThreshold = 1u, .ExternalThreshold = ExternalEmbedding ? 8u : 1_KB,
        };
        helper.CreateCustomTable("hnsw-blobs", {{"key", "Uint32", true, false},
                                              {"payload", "String", false, false},
                                              {"emb", "String", false, false}}, true, {family});
        const TString payload(2_KB, 'x');
        ExecSQL(helper.Server, helper.Sender, TStringBuilder()
            << "UPSERT INTO `/Root/hnsw-blobs` (key, payload, emb) VALUES "
            << "(1, \"inline\", \"\\x00\\x00\\x80\\x3F\\x00\\x00\\x00\\x00\\x01\"), "
            << "(2, \"" << payload << "\", \"\\x00\\x00\\x00\\x00\\x00\\x00\\x80\\x3F\\x01\"), "
            << "(3, \"unused\", \"\\x00\\x00\\x80\\xBF\\x00\\x00\\x00\\x00\\x01\");");
        auto& runtime = *helper.Server->GetRuntime();
        Ydb::Table::VectorIndexSettings vectorSettings;
        vectorSettings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vectorSettings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        vectorSettings.set_vector_dimension(2);
        vectorSettings.set_min_rows(1);
        vectorSettings.set_delta_rows(1000);
        ui64 readId = 0;
        auto read = [&] {
            auto request = helper.GetBaseReadRequest("hnsw-blobs", ++readId,
                NKikimrDataEvents::FORMAT_CELLVEC, TRowVersion::Max());
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(2);
            top->SetLimit(2);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            *top->MutableSettings() = vectorSettings;
            auto result = helper.SendRead("hnsw-blobs", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            return result;
        };
        bool accelerated = false;
        for (ui32 attempt = 0; attempt < 30; ++attempt) {
            if (read()->Record.GetStats().GetRows() == 2) {
                accelerated = true;
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_C(accelerated, "HNSW graph did not become ready");

        // Build from in-memory rows, then write cold external blobs while keeping
        // the graph. Neither compaction nor the graph scan preloads these blobs.
        const auto& table = helper.Tables.at("hnsw-blobs");
        CompactTable(runtime, table.TabletId, table.TableId, false);
        ui32 blobRequests = 0;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvGet::EventType) {
                blobRequests += ev->Get<TEvBlobStorage::TEvGet>()->QuerySize;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };

        const auto result = read();
        UNIT_ASSERT_GT(blobRequests, 0u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[0].template AsValue<ui32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(1)[0].template AsValue<ui32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[1].AsBuf(), "inline");
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(1)[1].AsBuf(), payload);
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[2].AsBuf(),
            TStringBuf("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
        UNIT_ASSERT_VALUES_EQUAL(result->GetCells(1)[2].AsBuf(),
            TStringBuf("\x00\x00\x00\x00\x00\x00\x80\x3F\x01", 9));
    }

    Y_UNIT_TEST(HnswSharesPageCacheBudgetAndEvictsOnLimit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        serverSettings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(serverSettings);
        helper.CreateCustomTable("table-vector-memory", {{"parent", "Uint32", true, false},
                                                         {"key", "Uint32", true, false},
                                                         {"emb", "String", false, false}}, true);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-vector-memory` (parent, key, emb) VALUES
                (1, 1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                (1, 2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        const auto shardActor = ResolveTablet(runtime, helper.Tables.at("table-vector-memory").TabletId);
        auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(shardActor));
        UNIT_ASSERT(shard);
        const auto localTid = shard->GetUserTables().begin()->second->LocalTid;
        TIntrusivePtr<NMemory::TRegistrantConsumer> consumer;
        ui32 registrations = 0;
        std::optional<ui64> limitOverride;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMemory::TEvConsumerRegister::EventType && ev->Sender == shardActor) {
                UNIT_ASSERT(ev->Get<NMemory::TEvConsumerRegister>()->Kind == NMemory::EMemoryConsumerKind::SharedCache);
                ++registrations;
            } else if (ev->GetTypeRewrite() == NMemory::TEvConsumerRegistered::EventType && ev->Recipient == shardActor) {
                consumer = dynamic_cast<NMemory::TRegistrantConsumer*>(ev->Get<NMemory::TEvConsumerRegistered>()->Consumer.Get());
            } else if (ev->GetTypeRewrite() == NMemory::TEvConsumerLimit::EventType && ev->Recipient == shardActor && limitOverride) {
                ev->Get<NMemory::TEvConsumerLimit>()->LimitBytes = *limitOverride;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER {
            runtime.SetObserverFunc(observer);
        };

        Ydb::Table::VectorIndexSettings settings;
        settings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.set_vector_dimension(2);
        settings.set_min_rows(1);
        // Isolate the reader's pinned graph and journal from rebuild buffers.
        settings.set_delta_rows(1000);
        ui64 readId = 0;
        auto read = [&] {
            auto request = helper.GetBaseReadRequest("table-vector-memory", ++readId, NKikimrDataEvents::FORMAT_CELLVEC);
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* topK = request->Record.MutableVectorTopK();
            topK->SetColumn(2);
            topK->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            topK->SetLimit(1);
            topK->AddDistinctColumns(1);
            *topK->MutableSettings() = settings;
            return helper.SendRead("table-vector-memory", request.release());
        };
        auto cached = [&] { return shard->GetHnswIndex(localTid, 3, settings, false); };
        auto warmCache = [&] {
            for (ui32 attempt = 0; attempt < 20 && !cached(); ++attempt) {
                read();
                runtime.SimulateSleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT(cached());
        };
        warmCache();
        UNIT_ASSERT_VALUES_EQUAL(registrations, 1);
        UNIT_ASSERT(consumer);
        UNIT_ASSERT_GT(consumer->GetReport().Used, 0);
        UNIT_ASSERT_GT(shard->GetHnswCacheMemoryLimit(), 0);
        UNIT_ASSERT_LT(shard->GetHnswCacheMemoryLimit(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(read()->Record.GetStats().GetRows(), 1);

        // Simulate an active reader pinning a graph and its write deltas.
        auto pinned = cached();
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-vector-memory` (parent, key, emb) VALUES
                (1, 3, "\x00\x00\x00\x00\x00\x00\x80\xBF\x01");
        )");
        const ui64 pinnedBytes = consumer->GetReport().Used;
        limitOverride = 0;
        runtime.Send(new IEventHandle(shardActor, helper.Sender, new NMemory::TEvConsumerLimit(0)));
        runtime.WaitFor("HNSW cache eviction", [&] { return !cached(); });
        UNIT_ASSERT_VALUES_EQUAL(consumer->GetReport().Used, pinnedBytes);
        UNIT_ASSERT_VALUES_EQUAL(read()->Record.GetStats().GetRows(), 3);
        pinned.reset();
        runtime.WaitFor("released HNSW reservations", [&] { return consumer->GetReport().Used == 0; });

        limitOverride.reset();
        warmCache();
        UNIT_ASSERT_VALUES_EQUAL(registrations, 1);
        UNIT_ASSERT_VALUES_EQUAL(read()->Record.GetStats().GetRows(), 1);
    }

    Y_UNIT_TEST(HnswMvccBuildCatchesConcurrentWrites) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        serverSettings.SetNeedStatsCollectors(true);
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        serverSettings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(serverSettings);

        TVector<TShardedTableOptions::TColumn> columns = {
            {"parent", "Uint32", true, false},
            {"key", "Uint32", true, false},
            {"emb", "String", false, false}};
        helper.CreateCustomTable("table-vector-race", columns, true);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-vector-race` (parent, key, emb) VALUES
                (1, 1, "\x66\x66\x66\x3F\xCD\xCC\xCC\x3D\x01"),
                (1, 2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
        )");

        auto makeRequest = [&](ui64 readId) {
            auto request = helper.GetBaseReadRequest(
                "table-vector-race", readId, NKikimrDataEvents::FORMAT_CELLVEC);
            request->Record.ClearSnapshot();
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* topK = request->Record.MutableVectorTopK();
            topK->SetColumn(2);
            topK->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            topK->SetLimit(1);
            topK->AddDistinctColumns(1);
            auto* settings = topK->MutableSettings();
            settings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            settings->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            settings->set_vector_dimension(2);
            settings->set_min_rows(1);
            settings->set_delta_rows(1000);
            return request;
        };

        auto& runtime = *helper.Server->GetRuntime();
        TVector<TAutoPtr<IEventHandle>> delayedBuildResults;
        // Keep in sync with TDataShard::TEvPrivate::EvHnswIndexBuildResult.
        constexpr ui32 hnswBuildResultEvent = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == hnswBuildResultEvent) {
                delayedBuildResults.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER {
            runtime.SetObserverFunc(previousObserver);
        };

        auto result = helper.SendRead("table-vector-race", makeRequest(1).release());
        UNIT_ASSERT(result->Record.GetFinished());
        // Registration and the first controller grant precede lazy construction.
        runtime.SimulateSleep(TDuration::Seconds(2));
        result = helper.SendRead("table-vector-race", makeRequest(10).release());
        runtime.WaitFor("lazy HNSW build result", [&] { return !delayedBuildResults.empty(); });

        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/table-vector-race` (parent, key, emb) VALUES
                (1, 100, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
        )");
        for (auto& event : delayedBuildResults) {
            runtime.Send(event.Release());
        }
        delayedBuildResults.clear();

        // The original graph installs with the concurrent insert in its journal.
        // The write and foreground reads did not wait for the delayed build.
        result = helper.SendRead("table-vector-race", makeRequest(2).release());
        CheckResult(helper.Tables.at("table-vector-race").UserTable, *result, {
                                                                                  {TCell::Make<ui32>(1), TCell::Make<ui32>(100),
                                                                                   TCell("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9)},
                                                                              },
                    {
                        NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
                        NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
                        NScheme::TTypeInfo(NScheme::NTypeIds::String),
                    });
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1);
        UNIT_ASSERT(delayedBuildResults.empty());

        result = helper.SendRead("table-vector-race", makeRequest(3).release());
        CheckResult(helper.Tables.at("table-vector-race").UserTable, *result, {
                                                                                  {TCell::Make<ui32>(1), TCell::Make<ui32>(100),
                                                                                   TCell("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9)},
                                                                              },
                    {
                        NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
                        NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
                        NScheme::TTypeInfo(NScheme::NTypeIds::String),
                    });
    }

    Y_UNIT_TEST_TWIN(HnswMvccReadsIntermediateInsertAfterDelete, Rebuild) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        settings.AppConfig->MutableDataShardConfig()->SetKeepSnapshotTimeout(1000);
        settings.AppConfig->MutableDataShardConfig()->SetCleanupSnapshotPeriod(100);
        TTestHelper helper(settings);
        helper.CreateCustomTable("hnsw-mvcc", {{"parent", "Uint32", true, false}, {"key", "Uint32", true, false},
                                               {"emb", "String", false, false}}, true);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-mvcc` (parent, key, emb) VALUES
                (1, 1, "\x66\x66\x66\x3F\xCD\xCC\xCC\x3D\x01"),
                (1, 2, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        const auto shardActor = ResolveTablet(runtime, helper.Tables.at("hnsw-mvcc").TabletId);
        auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(shardActor));
        UNIT_ASSERT(shard);
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        Ydb::Table::VectorIndexSettings indexSettings;
        indexSettings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        indexSettings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        indexSettings.set_vector_dimension(2);
        indexSettings.set_min_rows(1);
        indexSettings.set_delta_rows(Rebuild ? 1 : 1000);
        ui64 readId = 0;
        auto read = [&](TRowVersion version) {
            auto request = helper.GetBaseReadRequest("hnsw-mvcc", ++readId,
                                                     NKikimrDataEvents::FORMAT_CELLVEC, version);
            if (version.IsMax()) {
                request->Record.ClearSnapshot();
            }
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(2);
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            *top->MutableSettings() = indexSettings;
            auto result = helper.SendRead("hnsw-mvcc", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
            return result;
        };
        for (ui32 attempt = 0; attempt < 20 && !shard->GetHnswIndex(tid, 3, indexSettings, false); ++attempt) {
            read(TRowVersion::Max());
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT(shard->GetHnswIndex(tid, 3, indexSettings, false));
        const auto before = CreateVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, TDuration::Hours(1));
        auto original = shard->GetHnswIndex(tid, 3, indexSettings, false);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-mvcc` (parent, key, emb) VALUES
                (1, 3, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
        )");
        const auto inserted = CreateVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, TDuration::Hours(1));
        ExecSQL(helper.Server, helper.Sender, "DELETE FROM `/Root/hnsw-mvcc` WHERE parent=1 AND key=3;");
        const auto deleted = CreateVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, TDuration::Hours(1));
        for (const auto& [version, expected] : std::vector<std::pair<TRowVersion, ui32>>{
                 {before, 1}, {inserted, 3}, {deleted, 1}}) {
            auto result = read(version);
            UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[1].template AsValue<ui32>(), expected);
            // Assert acceleration, not just correctness of the fallback scan.
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1);
        }
        if (Rebuild) {
            // Inserting and deleting the same key changes one distinct row:
            // exactly delta_rows=1, so no automatic rebuild may have started.
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
            UNIT_ASSERT(shard->GetHnswIndex(tid, 3, indexSettings, false) == original);
            TVector<TAutoPtr<IEventHandle>> delayed;
            bool capture = true;
            constexpr ui32 buildResultEvent = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
            auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (capture && ev->GetTypeRewrite() == buildResultEvent) {
                    delayed.push_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
            Y_DEFER {
                runtime.SetObserverFunc(observer);
            };
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/hnsw-mvcc` (parent, key, emb) VALUES
                    (1, 2, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            )");
            runtime.WaitFor("automatic HNSW rebuild", [&] { return !delayed.empty(); });
            UNIT_ASSERT_VALUES_EQUAL(delayed.size(), 1);
            // Both a write and a read complete while the rebuild is suspended.
            ExecSQL(helper.Server, helper.Sender, R"(
                UPSERT INTO `/Root/hnsw-mvcc` (parent, key, emb) VALUES
                    (1, 4, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
            )");
            UNIT_ASSERT_VALUES_EQUAL(read(TRowVersion::Max())->GetCells(0)[1].template AsValue<ui32>(), 4);
            UNIT_ASSERT(shard->GetHnswIndex(tid, 3, indexSettings, false) == original);
            capture = false;
            for (auto& ev : delayed) {
                runtime.Send(ev.Release());
            }
            runtime.WaitFor("published HNSW rebuild", [&] {
                auto current = shard->GetHnswIndex(tid, 3, indexSettings, false);
                return current && current != original;
            });
            auto current = shard->GetHnswIndex(tid, 3, indexSettings, false);
            UNIT_ASSERT_VALUES_EQUAL(current->Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(current->ChangeCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(read(TRowVersion::Max())->GetCells(0)[1].template AsValue<ui32>(), 4);
            UNIT_ASSERT_VALUES_EQUAL(read(inserted)->GetCells(0)[1].template AsValue<ui32>(), 3);
            // A completely erased table can publish an empty generation.
            ExecSQL(helper.Server, helper.Sender, "DELETE FROM `/Root/hnsw-mvcc` WHERE parent=1;");
            runtime.WaitFor("empty HNSW rebuild", [&] {
                auto empty = shard->GetHnswIndex(tid, 3, indexSettings, false);
                return empty && empty->Size() == 0;
            });
            const auto emptyBase = shard->GetHnswIndex(tid, 3, indexSettings, false)->GetBaseVersion();
            runtime.SimulateSleep(TDuration::Seconds(10));
            UNIT_ASSERT(shard->GetSnapshotManager().GetLowWatermark() >= emptyBase);
            // Explicit snapshots below the watermark still keep their generation.
            UNIT_ASSERT(shard->GetHnswIndex(tid, 3, indexSettings, false, before) == original);
            UNIT_ASSERT(DiscardVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, before));
            UNIT_ASSERT(DiscardVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, inserted));
            UNIT_ASSERT(DiscardVolatileSnapshot(helper.Server, {"/Root/hnsw-mvcc"}, deleted));
            runtime.SimulateSleep(TDuration::Seconds(6));
            UNIT_ASSERT(!shard->GetHnswIndex(tid, 3, indexSettings, false, before));
            // Cache retirement cannot destroy a reader's owning reference.
            std::weak_ptr<THnswIndex> oldGraph = original;
            UNIT_ASSERT(!oldGraph.expired());
            original.reset();
            UNIT_ASSERT(oldGraph.expired());
        }
    }

    Y_UNIT_TEST(HnswMvccColdHistoricalReadDoesNotPoisonHead) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetEnableHnswIndex(true).SetDomainName("Root").SetUseRealThreads(false).SetNeedStatsCollectors(true);
        settings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TTestHelper helper(settings);
        helper.CreateCustomTable("hnsw-cold", {{"key", "Uint32", true, false}, {"emb", "String", false, false}}, true);
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-cold` (key, emb) VALUES
                (1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                (2, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
        )");
        const auto old = CreateVolatileSnapshot(helper.Server, {"/Root/hnsw-cold"}, TDuration::Hours(1));
        ExecSQL(helper.Server, helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-cold` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01"),
                (2, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
        )");
        auto& runtime = *helper.Server->GetRuntime();
        auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(
            ResolveTablet(runtime, helper.Tables.at("hnsw-cold").TabletId)));
        UNIT_ASSERT(shard);
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        Ydb::Table::VectorIndexSettings indexSettings;
        indexSettings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        indexSettings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        indexSettings.set_vector_dimension(2);
        indexSettings.set_min_rows(1);
        ui64 id = 0;
        auto read = [&](TRowVersion version, ui32 expected) {
            auto request = helper.GetBaseReadRequest("hnsw-cold", ++id, NKikimrDataEvents::FORMAT_CELLVEC, version);
            if (version.IsMax()) {
                request->Record.ClearSnapshot();
            }
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(1);
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            *top->MutableSettings() = indexSettings;
            auto result = helper.SendRead("hnsw-cold", request.release());
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[0].AsValue<ui32>(), expected);
            return result->Record.GetStats().GetRows();
        };
        for (ui32 attempt = 0; attempt < 20 && !shard->GetHnswIndex(tid, 2, indexSettings, false); ++attempt) {
            UNIT_ASSERT_VALUES_EQUAL(read(old, 1), 2);
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT(shard->GetHnswIndex(tid, 2, indexSettings, false));
        UNIT_ASSERT_VALUES_EQUAL(read(TRowVersion::Max(), 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(read(old, 1), 2);
        UNIT_ASSERT(!shard->GetHnswIndex(tid, 2, indexSettings, false, old));
    }

    struct THnswLifecycleFixture {
        TPortManager Ports;
        TTestHelper Helper;
        ui64 ReadId = 0;

        static TServerSettings Configure(TPortManager& ports) {
            TServerSettings settings(ports.GetPort(2134));
            settings.SetDomainName("Root").SetEnableHnswIndex(true).SetEnableForceImmediateEffectsExecution(true).SetUseRealThreads(false).SetNeedStatsCollectors(true);
            settings.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
            settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
            settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
            settings.AppConfig->MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
            return settings;
        }

        explicit THnswLifecycleFixture(bool follower = false, ui32 indexCount = 1, bool sameColumn = true)
            : Helper(Configure(Ports), 1, follower)
        {
            TVector<TShardedTableOptions::TColumn> columns = {{"key", "Uint32", true, false}, {"label", "Uint32", false, false},
                                                             {"emb", "String", false, false}};
            if (indexCount > 1 && !sameColumn) { columns.push_back({"emb2", "String", false, false}); }
            Helper.CreateCustomTable("hnsw-lifecycle", columns, true, {}, [&](auto& table) {
                auto first = table.GetTableIndexes(0);
                table.ClearTableIndexes();
                for (ui32 i = 0; i < indexCount; ++i) {
                    auto* index = table.AddTableIndexes();
                    *index = first;
                    index->SetName(TStringBuilder() << "hnsw" << i);
                    index->SetLocalPathId(Max<ui64>() - 1 - i);
                    index->MutableHnswIndexDescription()->SetEmbeddingColumnId(sameColumn ? 3 : 3 + i % 2);
                    index->MutableHnswIndexDescription()->MutableSettings()->mutable_settings()->set_m(16 + i);
                }
            });
            ExecSQL(Helper.Server, Helper.Sender, R"(
                UPSERT INTO `/Root/hnsw-lifecycle` (key, label, emb) VALUES
                    (1, 7, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01"),
                    (2, 8, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01");
            )");
            if (indexCount > 1 && !sameColumn) {
                ExecSQL(Helper.Server, Helper.Sender, R"(
                    UPSERT INTO `/Root/hnsw-lifecycle` (key, emb2) VALUES
                        (1, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01"),
                        (2, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
                )");
            }
        }

        TDataShard* Shard() {
            auto& runtime = *Helper.Server->GetRuntime();
            auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(
                ResolveTablet(runtime, Helper.Tables.at("hnsw-lifecycle").TabletId)));
            UNIT_ASSERT(shard);
            return shard;
        }

        Ydb::Table::VectorIndexSettings Settings() const {
            Ydb::Table::VectorIndexSettings settings;
            settings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            settings.set_vector_dimension(2);
            settings.set_min_rows(1);
            settings.set_delta_rows(1000);
            return settings;
        }

        std::shared_ptr<THnswIndex> Index() {
            auto* shard = Shard();
            const auto tid = shard->GetUserTables().begin()->second->LocalTid;
            return shard->GetHnswIndex(tid, 3, Settings(), false);
        }

        std::unique_ptr<TEvDataShard::TEvRead> Request(TRowVersion version = TRowVersion::Max()) {
            auto request = Helper.GetBaseReadRequest("hnsw-lifecycle", ++ReadId,
                                                     NKikimrDataEvents::FORMAT_CELLVEC, version);
            request->Record.MutableTableId()->ClearSchemaVersion();
            if (version.IsMax()) {
                request->Record.ClearSnapshot();
            }
            AddRangeQuery<ui32>(*request, {}, true, {}, true);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(2);
            top->SetLimit(1);
            top->SetTargetVector(TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
            *top->MutableSettings() = Settings();
            return request;
        }

        std::unique_ptr<TEvDataShard::TEvReadResult> Read(ui32 expected = 1) {
            auto result = Helper.SendRead("hnsw-lifecycle", Request().release());
            Check(*result, expected);
            return result;
        }

        static void Check(const TEvDataShard::TEvReadResult& result, ui32 expected) {
            UNIT_ASSERT_VALUES_EQUAL(result.Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result.GetRowsCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetCells(0)[0].AsValue<ui32>(), expected);
        }

        void Warm() {
            auto& runtime = *Helper.Server->GetRuntime();
            for (ui32 attempt = 0; attempt < 30 && !Index(); ++attempt) {
                Read();
                runtime.SimulateSleep(TDuration::MilliSeconds(200));
            }
            UNIT_ASSERT(Index());
            UNIT_ASSERT_VALUES_EQUAL(Read()->Record.GetStats().GetRows(), 1u);
        }
    };

    struct THnswMultiFixture : THnswLifecycleFixture {
        ui32 Count;
        bool SameColumn;
        TVector<Ydb::Table::VectorIndexSettings::Metric> Metrics;
        TVector<ui64> Thresholds;

        explicit THnswMultiFixture(ui32 count = 2, bool sameColumn = true)
            : THnswLifecycleFixture(false, count, sameColumn), Count(count), SameColumn(sameColumn)
            , Metrics(count, Ydb::Table::VectorIndexSettings::DISTANCE_COSINE), Thresholds(count, 1000)
        {}

        TPathId Id(ui32 index) {
            return {Helper.Tables.at("hnsw-lifecycle").TableId.PathId.OwnerId, Max<ui64>() - 1 - index};
        }

        ui32 Column(ui32 index) const { return SameColumn ? 3 : 3 + index % 2; }

        auto SettingsAt(ui32 index) const {
            auto settings = Settings();
            settings.set_m(16 + index);
            settings.set_metric(Metrics[index]);
            settings.set_delta_rows(Thresholds[index]);
            return settings;
        }

        auto Graph(ui32 index) {
            return Shard()->GetHnswIndex(Shard()->GetUserTables().begin()->second->LocalTid,
                Column(index), SettingsAt(index), false, TRowVersion::Max(), Id(index));
        }

        auto ReadAt(ui32 index, bool explicitId = true, TRowVersion version = TRowVersion::Max()) {
            auto request = Request(version);
            auto* top = request->Record.MutableVectorTopK();
            top->SetColumn(Column(index) - 1);
            top->MutableSettings()->set_metric(Metrics[index]);
            top->MutableSettings()->set_delta_rows(Thresholds[index]);
            // Missing M is inherited from an explicitly selected index. Without
            // identity, same-column candidates are intentionally ambiguous.
            if (explicitId) { Id(index).ToProto(top->MutableHnswIndexId()); }
            return Helper.SendRead("hnsw-lifecycle", request.release());
        }

        void WarmAll() {
            auto& runtime = *Helper.Server->GetRuntime();
            for (ui32 attempt = 0; attempt < 60; ++attempt) {
                bool ready = true;
                for (ui32 i = 0; i < Count; ++i) { ReadAt(i); ready &= bool(Graph(i)); }
                if (ready) { return; }
                runtime.SimulateSleep(TDuration::MilliSeconds(200));
            }
            UNIT_FAIL("Not all independent HNSW graphs became ready");
        }

        // Exercise the actual schema-transaction persistence/reconciliation path.
        void Alter(ui32 index, bool enabled, ui64 version = 1) {
            auto& runtime = *Helper.Server->GetRuntime();
            auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType) {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                        NKikimrTxDataShard::TFlatSchemeTransaction tx;
                        UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
                        if (tx.HasAlterTable() && tx.GetAlterTable().GetName() == "hnsw-lifecycle") {
                            auto* description = tx.MutableAlterTable()->AddTableIndexes();
                            description->SetName(TStringBuilder() << "hnsw" << index);
                            description->SetPathOwnerId(Id(index).OwnerId);
                            description->SetLocalPathId(Id(index).LocalPathId);
                            description->SetSchemaVersion(version);
                            description->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                            auto* hnsw = description->MutableHnswIndexDescription();
                            if (enabled) {
                                hnsw->SetEmbeddingColumnId(Column(index));
                                *hnsw->MutableSettings()->mutable_settings() = SettingsAt(index);
                            }
                            UNIT_ASSERT(tx.SerializeToString(record.MutableTxBody()));
                        }
                    }
                }
            });
            UNIT_ASSERT_VALUES_EQUAL(KqpSchemeExec(runtime, TStringBuilder()
                << "ALTER TABLE `/Root/hnsw-lifecycle` ADD COLUMN extra_" << ++ReadId << " String;"), "SUCCESS");
        }
    };

    Y_UNIT_TEST_TWIN(HnswMultipleIndexesSelectAndUpdateIndependently, SameColumn) {
        THnswMultiFixture fixture(2, SameColumn);
        fixture.WarmAll();
        auto first = fixture.Graph(0);
        auto second = fixture.Graph(1);
        UNIT_ASSERT(first && second && first != second);
        for (ui32 i = 0; i < 2; ++i) {
            auto result = fixture.ReadAt(i);
            fixture.Check(*result, SameColumn || i == 0 ? 1 : 2);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1u);
        }
        auto ambiguous = fixture.ReadAt(0, false);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous->Record.GetStats().GetRows(), SameColumn ? 2u : 1u);
        auto wrong = fixture.Request();
        TPathId(999, 999).ToProto(wrong->Record.MutableVectorTopK()->MutableHnswIndexId());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Helper.SendRead("hnsw-lifecycle", wrong.release())->Record.GetStats().GetRows(), 2u);

        ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
        )");
        fixture.Check(*fixture.ReadAt(0), 2);
        fixture.Check(*fixture.ReadAt(1), 2);
        UNIT_ASSERT(first->HasChanges());
        UNIT_ASSERT_VALUES_EQUAL(second->HasChanges(), SameColumn);
        UNIT_ASSERT(fixture.Graph(0) == first && fixture.Graph(1) == second);
        ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, "DELETE FROM `/Root/hnsw-lifecycle` WHERE key = 2;");
        fixture.Check(*fixture.ReadAt(0), 1);
        fixture.Check(*fixture.ReadAt(1), 1);
        UNIT_ASSERT(second->HasChanges());
    }

    Y_UNIT_TEST_TWIN(HnswMultipleIndexesTrackCommitAndRollback, Commit) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        TString session, tx;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, session, tx, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;
        )"), "{ items { uint32_value: 1 } }");
        fixture.Check(*fixture.ReadAt(0), 1);
        fixture.Check(*fixture.ReadAt(1), 1);
        if (Commit) {
            UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, session, tx, "SELECT 1;"), "{ items { int32_value: 1 } }");
        } else {
            Ydb::Table::DeleteSessionRequest request;
            request.set_session_id(session);
            auto future = NRpcService::DoLocalRpc<TEvDeleteSessionRequest>(std::move(request), "", "", runtime.GetActorSystem(0));
            UNIT_ASSERT_VALUES_EQUAL(AwaitResponse(runtime, std::move(future)).operation().status(), Ydb::StatusIds::SUCCESS);
        }
        for (ui32 i = 0; i < 2; ++i) {
            fixture.Check(*fixture.ReadAt(i), Commit ? 2 : 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Graph(i)->HasChanges(), Commit);
        }
    }

    Y_UNIT_TEST(HnswMultipleIndexesPreserveSiblingAcrossSchemaAndRestart) {
        THnswMultiFixture fixture(4);
        fixture.WarmAll();
        auto sibling = fixture.Graph(1);
        auto removed = fixture.Graph(0);
        fixture.Alter(0, false);
        UNIT_ASSERT(!fixture.Graph(0));
        UNIT_ASSERT(!removed->CanRead(TRowVersion::Max()));
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        fixture.Alter(0, true, 2);
        fixture.WarmAll();
        UNIT_ASSERT(fixture.Graph(0) != removed);
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        fixture.Alter(0, true, 3);
        fixture.WarmAll();
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        RebootTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId, fixture.Helper.Sender);
        fixture.Helper.Tables.at("hnsw-lifecycle").ClientId = {};
        fixture.WarmAll();
        UNIT_ASSERT(fixture.Graph(1) != sibling);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Shard()->GetUserTables().begin()->second->HnswIndexes.size(), 4u);
    }

    Y_UNIT_TEST(HnswMultipleIndexesUseDifferentMetricsAndRebuildThresholds) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto sibling = fixture.Graph(1);
        fixture.Metrics[0] = Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN;
        fixture.Thresholds[0] = 0;
        fixture.Alter(0, true, 1);
        fixture.WarmAll();
        auto first = fixture.Graph(0);
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x00\x40\x00\x00\x00\x00\x01"),
                (2, "\x00\x00\x80\x3F\x00\x00\x80\x3E\x01");
        )");
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        for (ui32 attempt = 0; attempt < 30 && fixture.Graph(0) == first; ++attempt) {
            fixture.ReadAt(0);
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT(fixture.Graph(0) && fixture.Graph(0) != first);
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        fixture.Check(*fixture.ReadAt(0), 2);
        fixture.Check(*fixture.ReadAt(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadAt(0, false)->Record.GetStats().GetRows(), 1u);
        auto mismatch = fixture.Request(); // Cosine request explicitly selecting the L2 index.
        fixture.Id(0).ToProto(mismatch->Record.MutableVectorTopK()->MutableHnswIndexId());
        auto result = fixture.Helper.SendRead("hnsw-lifecycle", mismatch.release());
        fixture.Check(*result, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2u);
    }

    Y_UNIT_TEST(HnswMultipleIndexesKeepHistoricalAndProjectedOnlyWrites) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto first = fixture.Graph(0);
        auto second = fixture.Graph(1);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        const auto snapshot = CreateVolatileSnapshot(fixture.Helper.Server,
            {"/Root/hnsw-lifecycle"}, TDuration::Hours(1));
        TString session, tx;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, session, tx, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, label) VALUES (1, 99);
            SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;
        )"), "{ items { uint32_value: 1 } }");
        fixture.ReadAt(0);
        fixture.ReadAt(1);
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, session, tx, "SELECT 1;"), "{ items { int32_value: 1 } }");
        for (ui32 i = 0; i < 2; ++i) {
            auto result = fixture.ReadAt(i);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result->GetCells(0)[1].AsValue<ui32>(), 99u);
            UNIT_ASSERT(!fixture.Graph(i)->HasChanges());
        }
        ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
        )");
        for (ui32 i = 0; i < 2; ++i) {
            fixture.Check(*fixture.ReadAt(i), 2);
            fixture.Check(*fixture.ReadAt(i, true, snapshot), 1);
        }
        UNIT_ASSERT(fixture.Graph(0) == first && fixture.Graph(1) == second);
    }

    Y_UNIT_TEST(HnswMultipleIndexesRejectStaleRecreatedWorker) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto sibling = fixture.Graph(1);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        TVector<TAutoPtr<IEventHandle>> delayed;
        constexpr ui32 buildResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == buildResult) {
                delayed.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        fixture.Alter(0, true, 1);
        runtime.WaitFor("old HNSW worker", [&] { return !delayed.empty(); });
        const auto tid = fixture.Shard()->GetUserTables().begin()->second->LocalTid;
        const auto token = fixture.Shard()->GetHnswBuildToken(tid, fixture.Id(0));
        fixture.Alter(0, false, 2);
        fixture.Alter(0, true, 3);
        runtime.WaitFor("recreated HNSW worker", [&] { return delayed.size() >= 2; });
        UNIT_ASSERT(fixture.Shard()->GetHnswBuildToken(tid, fixture.Id(0)) != token);
        runtime.SetObserverFunc(observer);
        runtime.Send(delayed.front().Release());
        fixture.ReadAt(0);
        UNIT_ASSERT(!fixture.Graph(0));
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        for (size_t i = 1; i < delayed.size(); ++i) { runtime.Send(delayed[i].Release()); }
        fixture.WarmAll();
        UNIT_ASSERT(fixture.Graph(1) == sibling);
    }

    Y_UNIT_TEST(HnswMultipleIndexesBoundConcurrentBuildsAndDrainQueue) {
        THnswMultiFixture fixture(4);
        fixture.WarmAll();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        const auto tid = fixture.Shard()->GetUserTables().begin()->second->LocalTid;
        fixture.Shard()->InvalidateHnswTable(tid);
        TVector<TAutoPtr<IEventHandle>> delayed;
        constexpr ui32 buildResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 35;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == buildResult) {
                delayed.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        for (ui32 i = 0; i < 4; ++i) { fixture.ReadAt(i); }
        runtime.WaitFor("two admitted HNSW builds", [&] { return delayed.size() == 2; });
        runtime.SimulateSleep(TDuration::Seconds(1));
        ui32 building = 0;
        for (ui32 i = 0; i < 4; ++i) {
            const auto stats = fixture.Shard()->GetHnswIndexStats(tid, fixture.Id(i));
            UNIT_ASSERT(stats);
            building += stats->Building;
        }
        UNIT_ASSERT_VALUES_EQUAL(building, 2u);
        UNIT_ASSERT_VALUES_EQUAL(delayed.size(), 2u);
        runtime.SetObserverFunc(observer);
        for (auto& result : delayed) { runtime.Send(result.Release()); }
        fixture.WarmAll();
    }

    Y_UNIT_TEST(HnswDisabledQueuedIndexDoesNotBlockSiblingBuilds) {
        THnswMultiFixture fixture(4);
        fixture.WarmAll();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        auto* shard = fixture.Shard();
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        shard->InvalidateHnswTable(tid);
        runtime.RunCall([&] {
            const auto base = shard->GetHnswBuildVersion();
            auto start = [&](ui32 index) {
                return shard->TryStartHnswIndexBuild(tid, fixture.Column(index), fixture.SettingsAt(index), base, fixture.Id(index));
            };
            UNIT_ASSERT(start(0));
            UNIT_ASSERT(start(1));
            UNIT_ASSERT(!start(2));
            UNIT_ASSERT(!start(3));
            shard->DisableHnswIndexBuild(tid, fixture.Id(2));
            // Complete one admitted job. The disabled queue head must not keep
            // the remaining sibling out of the newly available build slot.
            shard->SetHnswIndex(tid, {}, {}, 0, fixture.Column(0), fixture.SettingsAt(0), base,
                shard->GetHnswBuildToken(tid, fixture.Id(0)), fixture.Id(0));
            UNIT_ASSERT(start(3));
            return true;
        });
    }

    Y_UNIT_TEST(HnswMemoryLimitEvictsOnlyRequiredIndexes) {
        THnswMultiFixture fixture(3);
        fixture.WarmAll();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        auto* shard = fixture.Shard();
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        const auto actor = ResolveTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId);
        std::vector<std::shared_ptr<THnswIndex>> pinned;
        ui64 used = 0;
        for (ui32 i = 0; i < fixture.Count; ++i) {
            pinned.push_back(fixture.Graph(i));
            used += shard->GetHnswIndexStats(tid, fixture.Id(i))->MemoryBytes;
        }
        UNIT_ASSERT_GT(used, 0u);
        std::optional<ui64> limit = used - 1;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (limit && ev->Recipient == actor && ev->GetTypeRewrite() == NMemory::TEvConsumerLimit::EventType) {
                ev->Get<NMemory::TEvConsumerLimit>()->LimitBytes = *limit;
            }
        });
        auto ready = [&] {
            ui32 count = 0;
            for (ui32 i = 0; i < fixture.Count; ++i) { count += bool(fixture.Graph(i)); }
            return count;
        };
        auto setLimit = [&] {
            runtime.Send(new IEventHandle(actor, fixture.Helper.Sender, new NMemory::TEvConsumerLimit(*limit)));
        };
        setLimit();
        runtime.WaitFor("one HNSW graph evicted", [&] { return ready() != fixture.Count; });
        UNIT_ASSERT_VALUES_EQUAL(ready(), 2u);
        // Pin the retired graph to make deferred reclamation deterministic.
        // Repeated grants and reads must neither evict siblings nor start a
        // replacement build while the old reservation still exceeds the limit.
        setLimit();
        for (ui32 i = 0; i < fixture.Count; ++i) { fixture.ReadAt(i); }
        runtime.SimulateSleep(TDuration::Seconds(6));
        UNIT_ASSERT_VALUES_EQUAL(ready(), 2u);
        for (ui32 i = 0; i < fixture.Count; ++i) {
            const auto stats = shard->GetHnswIndexStats(tid, fixture.Id(i));
            UNIT_ASSERT(!stats->Building);
            UNIT_ASSERT_VALUES_EQUAL(stats->Builds, 1u);
            if (fixture.Graph(i)) { UNIT_ASSERT(fixture.Graph(i) == pinned[i]); }
        }
        pinned.clear();
        limit = 64_MB;
        setLimit();
        fixture.WarmAll();
    }

    Y_UNIT_TEST_TWIN(HnswEvictedIndexExhaustsMemoryRetryBudget, ZeroLimit) {
        THnswLifecycleFixture fixture;
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        const auto actor = ResolveTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId);
        TIntrusivePtr<NMemory::TRegistrantConsumer> consumer;
        std::optional<ui64> limit;
        ui32 rebuildRequests = 0;
        constexpr ui32 rebuild = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 36;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() != actor) { return; }
            if (ev->GetTypeRewrite() == NMemory::TEvConsumerRegistered::EventType) {
                consumer = dynamic_cast<NMemory::TRegistrantConsumer*>(ev->Get<NMemory::TEvConsumerRegistered>()->Consumer.Get());
            } else if (ev->GetTypeRewrite() == NMemory::TEvConsumerLimit::EventType && limit) {
                ev->Get<NMemory::TEvConsumerLimit>()->LimitBytes = *limit;
            } else if (ev->GetTypeRewrite() == rebuild) {
                ++rebuildRequests;
            }
        });
        fixture.Warm();
        UNIT_ASSERT(consumer);
        auto pinned = fixture.Index();
        const auto used = consumer->GetReport().Used;
        UNIT_ASSERT_GT(used, 0u);
        limit = ZeroLimit ? 0 : used - 1;
        runtime.Send(new IEventHandle(actor, fixture.Helper.Sender, new NMemory::TEvConsumerLimit(*limit)));
        runtime.WaitFor("HNSW graph evicted", [&] { return !fixture.Index(); });
        const auto before = rebuildRequests;
        fixture.Read();
        for (ui32 second = 0; second < 40; ++second) { runtime.SimulateSleep(TDuration::Seconds(1)); }
        UNIT_ASSERT_GT(rebuildRequests, before);
        UNIT_ASSERT_LE(rebuildRequests - before, 32u);
        const auto exhausted = rebuildRequests;
        for (ui32 second = 0; second < 10; ++second) { runtime.SimulateSleep(TDuration::Seconds(1)); }
        UNIT_ASSERT_VALUES_EQUAL(rebuildRequests, exhausted);
        pinned.reset();
        runtime.WaitFor("retired HNSW graph released", [&] { return consumer->GetReport().Used == 0; });
        UNIT_ASSERT_VALUES_EQUAL(consumer->GetReport().Demand, 0u);
        fixture.Read();
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(rebuildRequests, exhausted);

        limit = 64_MB;
        runtime.Send(new IEventHandle(actor, fixture.Helper.Sender, new NMemory::TEvConsumerLimit(*limit)));
        fixture.Warm(); // A real grant increase may start a fresh bounded attempt.
    }

    Y_UNIT_TEST(HnswHotKeyWritesDoNotAccumulateBetweenPeriodicPrunes) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        auto* shard = fixture.Shard();
        auto graph = fixture.Index();
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        const auto actor = ResolveTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId);
        constexpr ui32 periodic = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 7;
        TVector<TAutoPtr<IEventHandle>> periodicEvents;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == actor && ev->GetTypeRewrite() == periodic) {
                periodicEvents.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto write = [&](bool negative) {
            ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, negative ? R"(
                UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                    (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            )" : R"(
                UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                    (1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
            )");
        };
        write(true);
        const auto snapshot = CreateVolatileSnapshot(fixture.Helper.Server,
            {"/Root/hnsw-lifecycle"}, TDuration::Hours(1));
        const auto baseline = shard->GetHnswIndexStats(tid, {})->MemoryBytes;
        const TVector<TCell> key{TCell::Make<ui32>(1)};
        const auto versionBytes = THnswIndexChanges::EstimateBytes(TSerializedCellVec::Serialize(key), 9);
        for (ui32 update = 0; update < 128; ++update) {
            write(update % 2 == 0);
            UNIT_ASSERT(fixture.Index() == graph);
            UNIT_ASSERT_LE(shard->GetHnswIndexStats(tid, {})->MemoryBytes, baseline + 8 * versionBytes);
        }
        auto historical = fixture.Helper.SendRead("hnsw-lifecycle", fixture.Request(snapshot).release());
        fixture.Check(*historical, 2);
        UNIT_ASSERT_VALUES_EQUAL(historical->Record.GetStats().GetRows(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read(1)->Record.GetStats().GetRows(), 1u);
        observer.Remove();
        for (auto& ev : periodicEvents) { runtime.Send(ev.Release()); }
    }

    Y_UNIT_TEST(HnswMultipleIndexesSurviveSplitRecovery) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto sourceFirst = fixture.Graph(0);
        auto sourceSecond = fixture.Graph(1);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        auto& table = fixture.Helper.Tables.at("hnsw-lifecycle");
        SetSplitMergePartCountLimit(&runtime, -1);
        const auto tx = AsyncSplitTable(fixture.Helper.Server, fixture.Helper.Sender,
            "/Root/hnsw-lifecycle", table.TabletId, 2);
        WaitTxNotification(fixture.Helper.Server, fixture.Helper.Sender, tx);
        UNIT_ASSERT(!sourceFirst->CanRead(TRowVersion::Max()));
        UNIT_ASSERT(!sourceSecond->CanRead(TRowVersion::Max()));
        const auto shards = GetTableShards(fixture.Helper.Server, fixture.Helper.Sender, "/Root/hnsw-lifecycle");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        for (const auto tablet : shards) {
            table.TabletId = tablet;
            table.ClientId = {};
            auto [tables, owner] = GetTables(fixture.Helper.Server, tablet);
            Y_UNUSED(owner);
            table.UserTable = tables.at("hnsw-lifecycle");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Shard()->GetUserTables().begin()->second->HnswIndexes.size(), 2u);
            auto raw = fixture.Request();
            raw->Record.ClearVectorTopK();
            auto rows = fixture.Helper.SendRead("hnsw-lifecycle", raw.release());
            UNIT_ASSERT_VALUES_EQUAL(rows->GetRowsCount(), 1u);
            fixture.WarmAll();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Graph(0)->Size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Graph(1)->Size(), 1u);
        }
    }

    Y_UNIT_TEST(HnswMultipleIndexesHonorFeatureFlagAndIndependentEviction) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto sibling = fixture.Graph(1);
        const auto tid = fixture.Shard()->GetUserTables().begin()->second->LocalTid;
        fixture.Shard()->InvalidateHnswIndex(tid, fixture.Id(0));
        UNIT_ASSERT(!fixture.Graph(0));
        UNIT_ASSERT(fixture.Graph(1) == sibling);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ReadAt(1)->Record.GetStats().GetRows(), 1u);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableHnswIndex(false);
        }
        for (ui32 i = 0; i < 2; ++i) {
            auto result = fixture.ReadAt(i);
            fixture.Check(*result, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2u);
        }
    }

    // Opt-in measurements use the same tablet/read/write paths as the regression
    // fixtures. They compare index-count scaling, not a network SQL service SLA.
    Y_UNIT_TEST(HnswMultipleIndexScalingMeasurements) {
        if (GetEnv("YDB_HNSW_MULTI_INDEX_BENCH").empty()) { return; }
        for (ui32 count : {1u, 2u, 4u}) {
            THnswMultiFixture fixture(count);
            auto& runtime = *fixture.Helper.Server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_ERROR);
            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_ERROR);
            for (ui32 batch = 0; batch < 16; ++batch) {
                TStringBuilder sql;
                sql << "UPSERT INTO `/Root/hnsw-lifecycle` (key, label, emb) VALUES ";
                for (ui32 row = 0; row < 256; ++row) {
                    const ui32 key = 100 + batch * 256 + row;
                    const float coordinates[] = {1.0f + float(key) / 8192, 1.0f + float(key % 31) / 32};
                    TString vector(reinterpret_cast<const char*>(coordinates), sizeof(coordinates));
                    vector.push_back(1); // Float vector format suffix.
                    const auto hex = HexEncode(vector);
                    if (row) { sql << ','; }
                    sql << '(' << key << ",0,\"";
                    for (size_t i = 0; i < hex.size(); i += 2) { sql << "\\x" << hex.substr(i, 2); }
                    sql << "\")";
                }
                sql << ';';
                ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, sql);
            }
            // Rebuild from the completed import, excluding loading and warmup.
            for (ui32 i = 0; i < count; ++i) { fixture.Alter(i, true, 1); }
            fixture.WarmAll();
            auto write = [&] {
                    ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
                        UPSERT INTO `/Root/hnsw-lifecycle` (key, label, emb) VALUES
                            (100, 99, "\x00\x00\x80\x3F\x00\x00\x80\x3F\x01");
                    )");
            };
            write(); // Warm the SQL write plan and the committed journal path.
            for (ui32 i = 0; i < count; ++i) { fixture.ReadAt(i); }
            runtime.SimulateSleep(TDuration::Seconds(1));
            const auto tid = fixture.Shard()->GetUserTables().begin()->second->LocalTid;
            auto totals = [&] {
                std::pair<ui64, ui64> result;
                for (ui32 i = 0; i < count; ++i) {
                    const auto stats = fixture.Shard()->GetHnswIndexStats(tid, fixture.Id(i));
                    UNIT_ASSERT(stats && stats->Ready);
                    result.first += stats->MemoryBytes;
                    result.second += stats->Builds;
                }
                return result;
            };
            const auto before = totals();
            auto cpu = [] {
                rusage usage{};
                UNIT_ASSERT_VALUES_EQUAL(getrusage(RUSAGE_SELF, &usage), 0);
                return double(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec)
                    + double(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
            };
            TVector<ui64> latencies;
            const double cpuBefore = cpu();
            const auto start = TMonotonic::Now();
            constexpr ui32 reads = 4096;
            for (ui32 query = 0; query < reads; ++query) {
                if (query % 64 == 0) {
                    write();
                }
                const auto readStart = TMonotonic::Now();
                auto result = fixture.ReadAt(query % count);
                latencies.push_back((TMonotonic::Now() - readStart).MicroSeconds());
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1u);
            }
            const double elapsed = (TMonotonic::Now() - start).MicroSeconds() / 1e6;
            const double cpuSeconds = cpu() - cpuBefore;
            const auto after = totals();
            Sort(latencies);
            Cerr << "HNSW_MULTI_INDEX_MEASUREMENT indexes=" << count << " rows=4098 reads=" << reads
                 << " writes=64 qps=" << reads / elapsed << " p50_us=" << latencies[reads / 2]
                 << " p95_us=" << latencies[reads * 95 / 100] << " cpu_seconds=" << cpuSeconds
                 << " memory_before=" << before.first << " memory_after=" << after.first
                 << " builds=" << before.second << " additional_builds=" << after.second - before.second << Endl;
            UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
        }
    }

    Y_UNIT_TEST_TWIN(HnswMultipleIndexesInvalidateOnTruncate, Unsafe) {
        THnswMultiFixture fixture;
        fixture.WarmAll();
        auto first = fixture.Graph(0);
        auto second = fixture.Graph(1);
        if (Unsafe) {
            ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
                PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
                TRUNCATE TABLE `/Root/hnsw-lifecycle` WITH (unsafe = true);
            )");
        } else {
            const auto tx = AsyncTruncateTable(fixture.Helper.Server, fixture.Helper.Sender,
                "/Root", "hnsw-lifecycle");
            WaitTxNotification(fixture.Helper.Server, fixture.Helper.Sender, tx);
        }
        UNIT_ASSERT(!first->CanRead(TRowVersion::Max()));
        UNIT_ASSERT(!second->CanRead(TRowVersion::Max()));
        ExecSQL(fixture.Helper.Server, fixture.Helper.Sender, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, label, emb) VALUES
                (1, 7, "\x00\x00\x00\x00\x00\x00\x80\x3F\x01"),
                (2, 8, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
        )");
        fixture.WarmAll();
        fixture.Check(*fixture.ReadAt(0), 2);
        fixture.Check(*fixture.ReadAt(1), 2);
        UNIT_ASSERT(fixture.Graph(0) != first && fixture.Graph(1) != second);
    }

    Y_UNIT_TEST(HnswVolatileCommitRanksOwnWriteAboveFlushedLockWrite) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        TString session, tx;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, session, tx, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;
        )"), "{ items { uint32_value: 1 } }");
        // The first request flushed the negative vector under the lock id.
        // A distributed commit supplies a newer positive vector under its own id.
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, session, tx, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\x3F\x00\x00\x00\x00\x01");
            UPSERT INTO `/Root/movies` (id, title, rating) VALUES (99, "participant", 1);
            SELECT 1;
        )"), "{ items { int32_value: 1 } }");
        UNIT_ASSERT(fixture.Index() == graph);
        fixture.Read(1);
        TString stored;
        const TVector<TCell> key{TCell::Make<ui32>(1)};
        UNIT_ASSERT(graph->GetVector(TSerializedCellVec::Serialize(key), stored));
        UNIT_ASSERT_VALUES_EQUAL(stored, TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9));
    }

    Y_UNIT_TEST(HnswHistoricalReadIgnoresFutureVolatileTransaction) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardWriteAlwaysVolatile(true);
        const auto snapshot = CreateVolatileSnapshot(fixture.Helper.Server,
            {"/Root/hnsw-lifecycle"}, TDuration::Hours(1));
        const auto actor = ResolveTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId);
        std::vector<TEvTxProcessing::TEvReadSet::TPtr> blocked;
        auto capture = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            if (ev->GetRecipientRewrite() == actor) { blocked.push_back(std::move(ev)); }
        });
        auto write = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            UPSERT INTO `/Root/movies` (id, title, rating) VALUES (99, "participant", 1);
        )");
        runtime.WaitFor("future volatile transaction", [&] {
            return !blocked.empty() && fixture.Shard()->GetVolatileTxManager().GetTxInFlight();
        });
        UNIT_ASSERT(!fixture.Shard()->GetVolatileTxManager().HasVolatileTxsAtSnapshot(snapshot));
        auto old = fixture.Helper.SendRead("hnsw-lifecycle", fixture.Request(snapshot).release());
        fixture.Check(*old, 1);
        UNIT_ASSERT_VALUES_EQUAL(old->Record.GetStats().GetRows(), 1u);
        capture.Remove();
        for (auto& ev : blocked) { runtime.Send(ev.Release(), 0, true); }
        UNIT_ASSERT_VALUES_EQUAL(FormatResult(AwaitResponse(runtime, std::move(write))), "<empty>");
        UNIT_ASSERT(fixture.Index() == graph);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read(2)->Record.GetStats().GetRows(), 1u);
    }

    Y_UNIT_TEST_TWIN(HnswExactRangeDoesNotWaitForUnrelatedVolatileTransaction, ReadyGraph) {
        THnswLifecycleFixture fixture;
        if (ReadyGraph) { fixture.Warm(); }
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        auto* shard = fixture.Shard();
        if (!ReadyGraph) {
            shard->InvalidateHnswIndexes();
            shard->DisableHnswIndexBuild(shard->GetUserTables().begin()->second->LocalTid);
        }
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardWriteAlwaysVolatile(true);
        const auto actor = ResolveTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId);
        std::vector<TEvTxProcessing::TEvReadSet::TPtr> blocked;
        auto capture = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            if (ev->GetRecipientRewrite() == actor) { blocked.push_back(std::move(ev)); }
        });
        auto write = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (2, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            UPSERT INTO `/Root/movies` (id, title, rating) VALUES (99, "participant", 1);
        )");
        runtime.WaitFor("unrelated volatile transaction", [&] {
            return !blocked.empty() && shard->GetVolatileTxManager().GetTxInFlight();
        });
        auto request = fixture.Request();
        request->Ranges.clear();
        AddRangeQuery<ui32>(*request, {1}, true, {1}, true);
        auto result = fixture.Helper.SendRead("hnsw-lifecycle", request.release(), 0, {}, TDuration::Seconds(5));
        UNIT_ASSERT(result);
        fixture.Check(*result, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 1u);
        UNIT_ASSERT(shard->GetVolatileTxManager().GetTxInFlight());
        capture.Remove();
        for (auto& ev : blocked) { runtime.Send(ev.Release(), 0, true); }
        UNIT_ASSERT_VALUES_EQUAL(FormatResult(AwaitResponse(runtime, std::move(write))), "<empty>");
    }

    Y_UNIT_TEST(HnswLockOnlyCommitPreservesGraph) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        TString session, tx;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, session, tx,
            "SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;"), "{ items { uint32_value: 1 } }");
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, session, tx, R"(
            UPSERT INTO `/Root/movies` (id, title, rating) VALUES (99, "participant", 1);
            SELECT 1;
        )"), "{ items { int32_value: 1 } }");
        UNIT_ASSERT(fixture.Index() == graph);
        UNIT_ASSERT(!graph->HasChanges());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read()->Record.GetStats().GetRows(), 1u);
    }

    Y_UNIT_TEST(HnswPreJournalWritesResolveBeforeInitialBuild) {
        THnswLifecycleFixture fixture;
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        runtime.GetAppData(0).FeatureFlags.SetEnableHnswIndex(false);
        fixture.Shard()->InvalidateHnswIndexes();
        TString session, tx;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, session, tx, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, emb) VALUES
                (1, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;
        )"), "{ items { uint32_value: 1 } }");
        runtime.GetAppData(0).FeatureFlags.SetEnableHnswIndex(true);
        for (ui32 i = 0; i < 5; ++i) {
            fixture.Read(1);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(!fixture.Index()); // Unobserved pre-journal intents must resolve first.
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, session, tx, "SELECT 1;"), "{ items { int32_value: 1 } }");
        runtime.WaitFor("HNSW graph after pre-journal commit", [&] { return bool(fixture.Index()); });
        auto graph = fixture.Index();
        UNIT_ASSERT(graph);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read(2)->Record.GetStats().GetRows(), 1u);
        UNIT_ASSERT(!graph->HasChanges());
    }

    Y_UNIT_TEST(HnswDroppingTableInvalidatesReaderOwnedGraph) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        UNIT_ASSERT_VALUES_EQUAL(KqpSchemeExec(runtime, "DROP TABLE `/Root/hnsw-lifecycle`;"), "SUCCESS");
        UNIT_ASSERT(!graph->CanRead(TRowVersion::Max()));
    }

    Y_UNIT_TEST(HnswGraphDestructionRetainsReservationUntilBackgroundRelease) {
        THnswLifecycleFixture fixture;
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        const auto caller = std::this_thread::get_id();
        std::atomic<bool> released = false;
        std::atomic<bool> releasedOffThread = false;
        auto reservation = std::shared_ptr<void>(new int(0), [&](void* value) {
            delete static_cast<int*>(value);
            releasedOffThread.store(std::this_thread::get_id() != caller);
            released.store(true);
        });
        std::optional<THnswIndexBuildResult> built;
        runtime.Register(CreateHnswIndexBuildWorker(fixture.Settings(), {
            {"key", TString("\x00\x00\x80\x3F\x00\x00\x00\x00\x01", 9)}
        }, std::move(reservation), 0,
            [&](THnswIndexBuildResult&& result, const TActorContext&) { built.emplace(std::move(result)); }));
        runtime.WaitFor("background HNSW build", [&] { return built.has_value(); });
        UNIT_ASSERT_C(built->Index, built->Error);
        built->MemoryReservation.reset();
        UNIT_ASSERT(!released.load());
        built->Index.reset();
        runtime.WaitFor("background HNSW graph release", [&] { return released.load(); });
        UNIT_ASSERT(releasedOffThread.load());
    }

    Y_UNIT_TEST(HnswCancelledSnapshotCannotStartGraphWorker) {
        THnswLifecycleFixture fixture;
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        constexpr ui32 snapshotFinished = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 38;
        TVector<TAutoPtr<IEventHandle>> snapshots;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == snapshotFinished) {
                snapshots.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Shard()->InvalidateHnswIndexes();
        fixture.Read();
        runtime.WaitFor("completed HNSW snapshot scan", [&] { return !snapshots.empty(); });
        auto* shard = fixture.Shard();
        const auto tid = shard->GetUserTables().begin()->second->LocalTid;
        const auto token = shard->GetHnswBuildToken(tid);
        UNIT_ASSERT(shard->IsHnswBuildCurrent(tid, token));
        runtime.GetAppData(0).FeatureFlags.SetEnableHnswIndex(false);
        shard->InvalidateHnswIndexes();
        UNIT_ASSERT(!shard->IsHnswBuildCurrent(tid, token));
        observer.Remove();
        for (auto& ev : snapshots) { runtime.Send(ev.Release()); }
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!fixture.Index());
        const auto stats = shard->GetHnswIndexStats(tid, {});
        UNIT_ASSERT(stats);
        UNIT_ASSERT(!stats->Building);
        UNIT_ASSERT_VALUES_EQUAL(stats->MemoryBytes, 0u);
    }

    Y_UNIT_TEST_TWIN(HnswTabletShutdownWithSnapshotScan, TabletDead) {
        THnswLifecycleFixture fixture;
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        constexpr ui32 snapshotFinished = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 38;
        TVector<TAutoPtr<IEventHandle>> snapshots;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == snapshotFinished) {
                snapshots.push_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Read();
        runtime.WaitFor("registered HNSW snapshot scan", [&] { return !snapshots.empty(); });
        const auto tablet = fixture.Helper.Tables.at("hnsw-lifecycle").TabletId;
        const auto actor = ResolveTablet(runtime, tablet);
        if (TabletDead) {
            runtime.Send(new IEventHandle(actor, fixture.Helper.Sender,
                new TEvTablet::TEvTabletDead(tablet, TEvTablet::TEvTabletDead::ReasonPill, 0)));
        } else {
            runtime.Send(new IEventHandle(actor, fixture.Helper.Sender, new TEvents::TEvPoison()));
        }
        runtime.WaitFor("DataShard shutdown with a registered scan", [&] { return !runtime.FindActor(actor); });
    }

    Y_UNIT_TEST_TWIN(HnswPendingWritesPublishOnlyOnCommit, Commit) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        UNIT_ASSERT(!graph->HasChanges());
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(KqpSimpleBegin(runtime, sessionId, txId, R"(
            UPSERT INTO `/Root/hnsw-lifecycle` (key, label, emb) VALUES
                (1, 7, "\x00\x00\x80\xBF\x00\x00\x00\x00\x01");
            SELECT key FROM `/Root/hnsw-lifecycle` WHERE key = 1;
        )"), "{ items { uint32_value: 1 } }");
        UNIT_ASSERT(!txId.empty());
        fixture.Read(1);
        UNIT_ASSERT(!graph->HasChanges());
        if (Commit) {
            UNIT_ASSERT_VALUES_EQUAL(KqpSimpleCommit(runtime, sessionId, txId, "SELECT 1;"),
                                     "{ items { int32_value: 1 } }");
        } else {
            Ydb::Table::DeleteSessionRequest request;
            request.set_session_id(sessionId);
            auto future = NRpcService::DoLocalRpc<TEvDeleteSessionRequest>(
                std::move(request), "", "", runtime.GetActorSystem(0));
            const auto response = AwaitResponse(runtime, std::move(future));
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
        fixture.Read(Commit ? 2 : 1);
        UNIT_ASSERT(fixture.Index() == graph);
        UNIT_ASSERT_VALUES_EQUAL(graph->HasChanges(), Commit);
    }

    Y_UNIT_TEST_TWIN(HnswPreservesUnrelatedSchemaAndRebuildsOnRestart, Reboot) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        if (Reboot) {
            RebootTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId,
                         fixture.Helper.Sender);
            fixture.Helper.Tables.at("hnsw-lifecycle").ClientId = {};
        } else {
            UNIT_ASSERT_VALUES_EQUAL(KqpSchemeExec(runtime,
                "ALTER TABLE `/Root/hnsw-lifecycle` ADD COLUMN extra String;"), "SUCCESS");
        }
        UNIT_ASSERT_VALUES_EQUAL(graph->CanRead(TRowVersion::Max()), !Reboot);
        fixture.Warm();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Index() == graph, !Reboot);
    }

    Y_UNIT_TEST(HnswExplicitMetadataDisableSurvivesRestart) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto graph = fixture.Index();
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        ui32 annotationsCleared = 0;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType) {
                auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    NKikimrTxDataShard::TFlatSchemeTransaction tx;
                    UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
                    if (tx.HasAlterTable() && tx.GetAlterTable().GetName() == "hnsw-lifecycle") {
                        auto* index = tx.MutableAlterTable()->AddTableIndexes();
                        index->SetName("hnsw");
                        index->SetPathOwnerId(tx.GetAlterTable().GetPathId().GetOwnerId());
                        index->SetLocalPathId(Max<ui64>() - 1);
                        index->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                        index->MutableHnswIndexDescription();
                        UNIT_ASSERT(tx.SerializeToString(record.MutableTxBody()));
                        ++annotationsCleared;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        UNIT_ASSERT_VALUES_EQUAL(KqpSchemeExec(runtime,
            "ALTER TABLE `/Root/hnsw-lifecycle` ADD COLUMN extra String;"), "SUCCESS");
        runtime.SetObserverFunc(observer);
        UNIT_ASSERT(annotationsCleared);
        UNIT_ASSERT(!graph->CanRead(TRowVersion::Max()));
        UNIT_ASSERT(fixture.Shard()->GetUserTables().begin()->second->HnswIndexes.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read()->Record.GetStats().GetRows(), 2u);
        RebootTablet(runtime, fixture.Helper.Tables.at("hnsw-lifecycle").TabletId,
            fixture.Helper.Sender);
        fixture.Helper.Tables.at("hnsw-lifecycle").ClientId = {};
        UNIT_ASSERT_VALUES_EQUAL(fixture.Read()->Record.GetStats().GetRows(), 2u);
        UNIT_ASSERT(fixture.Shard()->GetUserTables().begin()->second->HnswIndexes.empty());
        UNIT_ASSERT(!fixture.Index());
    }

    Y_UNIT_TEST_TWIN(HnswUnsupportedReadsKeepExactScan, Locked) {
        THnswLifecycleFixture fixture;
        fixture.Warm();
        auto request = fixture.Request();
        if (Locked) {
            request->Record.SetLockTxId(12345678);
            request->Record.SetLockNodeId(fixture.Helper.Server->GetRuntime()->GetNodeId());
        } else {
            // label is a payload column, so graph keys cannot deduplicate it.
            request->Record.MutableVectorTopK()->AddDistinctColumns(1);
        }
        auto result = fixture.Helper.SendRead("hnsw-lifecycle", request.release());
        fixture.Check(*result, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2u);
    }

    Y_UNIT_TEST(HnswFollowersKeepExactScan) {
        THnswLifecycleFixture fixture(true);
        auto& runtime = *fixture.Helper.Server->GetRuntime();
        const auto snapshot = CreateVolatileSnapshot(fixture.Helper.Server,
                                                     {"/Root/hnsw-lifecycle"}, TDuration::Hours(1));
        runtime.SimulateSleep(TDuration::Seconds(2));
        TDataShard* follower = nullptr;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                auto* shard = dynamic_cast<TDataShard*>(runtime.FindActor(ev->GetRecipientRewrite()));
                if (shard && shard->IsFollower()) {
                    follower = shard;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER {
            runtime.SetObserverFunc(observer);
        };
        for (ui32 attempt = 0; attempt < 5; ++attempt) {
            auto result = fixture.Helper.SendRead("hnsw-lifecycle", fixture.Request(snapshot).release());
            fixture.Check(*result, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStats().GetRows(), 2u);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(follower);
        const auto tid = follower->GetUserTables().begin()->second->LocalTid;
        UNIT_ASSERT_VALUES_EQUAL(follower->GetHnswBuildToken(tid), 0u);
        UNIT_ASSERT(!follower->IsHnswCacheMemoryLimitKnown());
    }

    Y_UNIT_TEST(BadRequest) {
        TTestHelper helper;
        TVector<TShardedTableOptions::TColumn> columns = {
            {"parent", "Uint32", true, false},
            {"key", "Uint32", true, false},
            {"emb", "String", false, false}};
        helper.CreateCustomTable("table-vector", columns);

        auto createRequest = [&]() {
            auto request1 = helper.GetBaseReadRequest("table-vector", 1, NKikimrDataEvents::FORMAT_CELLVEC);
            AddRangeQuery<ui32>(*request1, { 1 }, true, { 20 }, true);
            auto topK = request1->Record.MutableVectorTopK();
            topK->SetColumn(2);
            topK->SetTargetVector("\xE4\x16\x02");
            topK->SetLimit(3);
            topK->AddDistinctColumns(1);
            auto idx = topK->MutableSettings();
            idx->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            idx->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
            idx->set_vector_dimension(2);
            return request1;
        };

        auto request1 = createRequest();
        request1->Record.MutableVectorTopK()->SetLimit(0);
        auto readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("limit is 0"));

        request1 = createRequest();
        request1->Record.MutableVectorTopK()->SetColumn(3);
        readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("Too large topK column index"));

        request1 = createRequest();
        request1->Record.MutableVectorTopK()->ClearTargetVector();
        readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("Target vector is not specified"));

        request1 = createRequest();
        request1->Record.MutableVectorTopK()->SetTargetVector("\xE4\x16\x10");
        readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("Target vector has invalid format"));

        request1 = createRequest();
        request1->Record.MutableVectorTopK()->MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED);
        readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("either distance or similarity"));

        request1 = createRequest();
        request1->Record.MutableVectorTopK()->AddDistinctColumns(3);
        readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(readResult1->Record.GetStatus().IssuesSize(), 1);
        UNIT_ASSERT(readResult1->Record.GetStatus().GetIssues(0).message().Contains("Too large unique column index"));
    }

    Y_UNIT_TEST_TWIN(Simple, Batch) {
        TTestHelper helper;
        TVector<TShardedTableOptions::TColumn> columns = {
            {"parent", "Uint32", true, false},
            {"key", "Uint32", true, false},
            {"emb", "String", false, false}};
        helper.CreateCustomTable("table-vector", columns);

        // Insert initial data
        ExecSQL(helper.Server, helper.Sender, R"(UPSERT INTO `/Root/table-vector` (parent, key, emb) VALUES
            (1,  1, "\x00\xFF\x02"),
            (1,  2, "\x10\xF0\x02"),
            (1,  3, "\x20\xE0\x02"),
            (1,  4, "\x30\xD0\x02"),
            (1,  5, "\x40\xC0\x02"),
            (1,  6, "\x50\xB0\x02"),
            (1,  7, "\x60\xA0\x02"),
            (1,  8, "\x70\x90\x02"),
            (1,  9, "\x80\x80\x02"),
            (1, 10, "\x90\x70\x02"),
            (1, 11, "\xA0\x60\x02"),
            (1, 12, "\xB0\x50\x02"),
            (1, 13, "\xC0\x40\x02"),
            (1, 14, "\xD0\x30\x02"),
            (1, 15, "\xE0\x20\x02"),
            (1, 16, "\xF0\x10\x02"),
            (2, 16, "\xF0\x10\x02"),
            (1, 17, "\xFF\x00\x02");)");

        auto request1 = helper.GetBaseReadRequest("table-vector", 1, NKikimrDataEvents::FORMAT_CELLVEC);
        if (Batch) {
            request1->Record.SetHints(TEvDataShard::TEvRead::HINT_BATCH);
        }
        AddRangeQuery<ui32>(*request1, { 1, 1 }, true, { 2, 20 }, true);
        auto topK = request1->Record.MutableVectorTopK();
        topK->SetColumn(2);
        topK->SetTargetVector("\xE4\x16\x02");
        topK->SetLimit(3);
        topK->AddDistinctColumns(1);
        auto idx = topK->MutableSettings();
        idx->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        idx->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        idx->set_vector_dimension(2);
        auto readResult1 = helper.SendRead("table-vector", request1.release());
        UNIT_ASSERT(readResult1->Record.GetFinished());
        UNIT_ASSERT_C(readResult1->Record.GetStats().GetRows() == 18, TStringBuilder() << "Rows: " << readResult1->Record.GetStats().GetRows());
        UNIT_ASSERT_C(readResult1->Record.GetStats().GetBytes() == 864, TStringBuilder() << "Bytes: " << readResult1->Record.GetStats().GetBytes());
        CheckResult(helper.Tables.at("table-vector").UserTable, *readResult1, {
            {TCell::Make(1), TCell::Make(16), TCell("\xF0\x10\x02", 3)},
            {TCell::Make(1), TCell::Make(15), TCell("\xE0\x20\x02", 3)},
            {TCell::Make(1), TCell::Make(17), TCell("\xFF\x00\x02", 3)},
        }, {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::String)
        });
    }

}

} // namespace NKikimr
