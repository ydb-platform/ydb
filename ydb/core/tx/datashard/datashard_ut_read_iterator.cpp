#include "datashard_ut_common.h"
#include "datashard_active_transaction.h"
#include "read_iterator.h"

#include <ydb/core/formats/arrow_helpers.h>
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
                 bool withFollower = false)
{
    TVector<TShardedTableOptions::TColumn> columns = {
        {"key1", "Uint32", true, false},
        {"key2", "Uint32", true, false},
        {"key3", "Uint32", true, false},
        {"value", "Uint32", false, false}
    };

    auto opts = TShardedTableOptions()
        .Shards(1)
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
    const TVector<std::pair<TString, NScheme::TTypeId>>& batchSchema,
    const TEvDataShard::TEvReadResult& result)
{
    UNIT_ASSERT(result.ArrowBatch);

    // TODO: use schema from ArrowBatch
    TRowWriter writer;
    NArrow::TArrowToYdbConverter converter(batchSchema, writer);

    TString error;
    UNIT_ASSERT(converter.Process(*result.ArrowBatch, error));

    return std::move(writer.Rows);
}

void CheckRow(
    const TConstArrayRef<TCell>& row,
    const TCellVec& gold,
    const std::vector<NScheme::TTypeIdOrder>& goldTypes)
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
    const std::vector<NScheme::TTypeIdOrder>& goldTypes)
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
    const std::vector<NScheme::TTypeIdOrder>& goldTypes,
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
    const std::vector<NScheme::TTypeIdOrder>& goldTypes,
    std::vector<NTable::TTag> columns = {})
{
    UNIT_ASSERT(!gold.empty());
    UNIT_ASSERT(result.ArrowBatch);

    TVector<std::pair<TString, NScheme::TTypeId>> batchSchema;
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
    const std::vector<NScheme::TTypeIdOrder>& goldTypes,
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
        UNIT_ASSERT(!result.ArrowBatch && result.GetRowsCount() == 0);
    }
}

void CheckResult(
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TEvDataShard::TEvReadResult& result,
    const std::vector<std::vector<ui32>>& gold,
    std::vector<NTable::TTag> columns = {})
{
    std::vector<NScheme::TTypeIdOrder> types;
    if (!gold.empty() && !gold[0].empty()) {
        types.reserve(gold[0].size());
        for (auto i: xrange(gold[0].size())) {
            Y_UNUSED(i);
            types.emplace_back(NScheme::NTypeIds::Uint32);
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

    explicit TTestHelper(const TServerSettings& serverSettings) {
        init(serverSettings);
    }

    void init(const TServerSettings& serverSettings) {
        Server = new TServer(serverSettings);

        auto &runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(Server, Sender);

        auto& table1 = Tables["table-1"];
        table1.Name = "table-1";
        {
            CreateTable(Server, Sender, "/Root", "table-1", WithFollower);
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

        auto& table2 = Tables["movies"];
        table2.Name = "movies";
        {
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
        NKikimrTxDataShard::EScanDataFormat format = NKikimrTxDataShard::ARROW)
    {
        const auto& table = Tables[tableName];

        std::unique_ptr<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(table.OwnerId);
        record.MutableTableId()->SetTableId(table.UserTable.GetPathId());

        const auto& description = table.UserTable.GetDescription();
        std::vector<ui32> keyColumns(
            description.GetKeyColumnIds().begin(),
            description.GetKeyColumnIds().end());

        for (const auto& column: description.GetColumns()) {
            record.AddColumns(column.GetId());
        }

        record.MutableTableId()->SetSchemaVersion(description.GetTableSchemaVersion());

        auto readVersion = CreateVolatileSnapshot(
            Server,
            {"/Root/movies", "/Root/table-1"},
            TDuration::Hours(1));

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

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(
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

        return WaitReadResult();
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

    NTabletPipe::TClientConfig GetTestPipeConfig() {
        auto config = GetPipeConfigWithRetries();
        if (WithFollower)
            config.ForceFollower = true;
        return config;
    }

public:
    bool WithFollower = false;
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
        UNIT_ASSERT(!record1.HasFinished());
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

    Y_UNIT_TEST(ShouldStopWhenDisconnected) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(20);

        const ui32 node = 13;

        TTestHelper helper(serverSettings);

        ui32 continueCounter = 0;
        helper.Server->GetRuntime()->SetObserverFunc([&continueCounter](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvReadContinue) {
                ++continueCounter;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto& table = helper.Tables["table-1"];
        auto prevClient = table.ClientId;

        auto &runtime = *helper.Server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor(node);

        // we need to connect from another node
        table.ClientId = runtime.ConnectToPipe(table.TabletId, sender, node, GetPipeConfigWithRetries());
        UNIT_ASSERT(table.ClientId);

        auto request1 = helper.GetBaseReadRequest("table-1", 1);
        AddKeyQuery(*request1, {3, 3, 3});
        AddKeyQuery(*request1, {1, 1, 1});

        // set quota so that DS hangs waiting for ACK
        request1->Record.SetMaxRows(1);

        auto readResult1 = helper.SendRead("table-1", request1.release(), node, sender);

        runtime.DisconnectNodes(node, node + 1, false);

        // restore our nodeId=0 client
        table.ClientId = prevClient;
        helper.SendReadAck("table-1", readResult1->Record, 3, 10000); // DS must ignore it

        auto readResult2 = helper.WaitReadResult(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!readResult2);
        UNIT_ASSERT_VALUES_EQUAL(continueCounter, 0);
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
        NDataShard::TReadIteratorState state({}, {});
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

} // namespace NKikimr
