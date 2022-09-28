#include "datashard_ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h> // Y_UNIT_TEST_(TWIN|QUAD), Q_

#include <ydb/library/yql/minikql/mkql_node_printer.h>

#include "testload/test_load_actor.h"

namespace NKikimr {

using namespace NKikimr::NDataShardLoad;
using namespace NSchemeShard;
using namespace Tests;

namespace {

// We use YCSB defaule schema: table named 'usertable' with 'key' column
// and 'field0' to 'field9' value columns. All fields are Utf8
const TString TableName = "usertable";
const TString FieldPrefix = "field";
const size_t ValueColumnsCount = 10;

void CreateTable(Tests::TServer::TPtr server,
                 TActorId sender,
                 const TString &root)
{
    TVector<TShardedTableOptions::TColumn> columns;
    columns.reserve(ValueColumnsCount + 1);

    columns.emplace_back("key", "Utf8", true, false);

    for (size_t i = 0; i < ValueColumnsCount; ++i) {
        TString fieldName = FieldPrefix + ToString(i);
        columns.emplace_back(fieldName, "Utf8", false, false);
    }

    auto opts = TShardedTableOptions()
        .Shards(1)
        .Columns(columns);

    CreateShardedTable(server, sender, root, TableName, opts);
}

TVector<TCell> ToCells(const std::vector<TString>& keys) {
    TVector<TCell> cells;
    for (auto& key: keys) {
        cells.emplace_back(TCell(key.data(), key.size()));
    }
    return cells;
}

void AddRangeQuery(
    TEvDataShard::TEvRead& request,
    const std::vector<TString>& from,
    bool fromInclusive,
    const std::vector<TString>& to,
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
    TTestHelper() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        init(serverSettings);
    }

    void init(const TServerSettings& serverSettings) {
        Server = new TServer(serverSettings);

        auto &runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::DS_LOAD_TEST, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);

        InitRoot(Server, Sender);

        Table.Name = TableName;
        {
            CreateTable(Server, Sender, "/Root");

            auto shards = GetTableShards(Server, Sender, "/Root/" + TableName);
            Table.TabletId = shards.at(0);

            auto [tables, ownerId] = GetTables(Server, Table.TabletId);
            Table.OwnerId = ownerId;
            Table.UserTable = tables[TableName];
        }

        Table.ClientId = runtime.ConnectToPipe(Table.TabletId, Sender, 0, GetPipeConfigWithRetries());
    }

    std::unique_ptr<TEvDataShard::TEvRead> GetBaseReadRequest() {
        std::unique_ptr<TEvDataShard::TEvRead> request(new TEvDataShard::TEvRead());
        auto& record = request->Record;

        record.SetReadId(ReadId++);
        record.MutableTableId()->SetOwnerId(Table.OwnerId);
        record.MutableTableId()->SetTableId(Table.UserTable.GetPathId());

        const auto& description = Table.UserTable.GetDescription();
        std::vector<ui32> keyColumns(
            description.GetKeyColumnIds().begin(),
            description.GetKeyColumnIds().end());

        for (const auto& column: description.GetColumns()) {
            record.AddColumns(column.GetId());
        }

        record.SetResultFormat(::NKikimrTxDataShard::EScanDataFormat::CELLVEC);

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

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(TEvDataShard::TEvRead* request)
    {
        auto &runtime = *Server->GetRuntime();
        runtime.SendToPipe(
            Table.TabletId,
            Sender,
            request,
            0,
            GetPipeConfigWithRetries(),
            Table.ClientId);

        return WaitReadResult();
    }

    void CheckKeysCount(size_t expectedRowCount) {
        TVector<TString> from = {TString("user")};
        TVector<TString> to = {TString("zzz")};

        auto request = GetBaseReadRequest();
        AddRangeQuery(
            *request,
            from,
            true,
            to,
            true
        );

        auto readResult = SendRead(request.release());
        UNIT_ASSERT(readResult);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), expectedRowCount);
    }

    std::unique_ptr<TEvDataShardLoad::TEvTestLoadFinished> RunTestLoad(
        std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> request)
    {
        request->Record.SetNotifyWhenFinished(true);
        auto &runtime = *Server->GetRuntime();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters());
        auto testLoadActor = runtime.Register(CreateTestLoadActor(counters));

        runtime.Send(new IEventHandle(testLoadActor, Sender, request.release()), 0, true);

        {
            // check load started
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDataShardLoad::TEvTestLoadResponse>(handle);
            UNIT_ASSERT(handle);
            auto response = handle->Release<TEvDataShardLoad::TEvTestLoadResponse>();
            auto& responseRecord = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(responseRecord.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }

        {
            // wait until load finished
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvDataShardLoad::TEvTestLoadFinished>(handle);
            UNIT_ASSERT(handle);
            auto response = handle->Release<TEvDataShardLoad::TEvTestLoadFinished>();
            UNIT_ASSERT(response->Report);
            UNIT_ASSERT(!response->ErrorReason);

            return std::unique_ptr<TEvDataShardLoad::TEvTestLoadFinished>(response.Release());
        }
    }

    void RunUpsertTestLoad(std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> loadRequest, size_t expectedRowCount) {
        RunTestLoad(std::move(loadRequest));
        CheckKeysCount(expectedRowCount);
    }

public:
    Tests::TServer::TPtr Server;
    TActorId Sender;
    TTableInfo Table;

    ui64 ReadId = 1;
};

} // anonymous

Y_UNIT_TEST_SUITE(UpsertLoad) {
    Y_UNIT_TEST(ShouldWriteDataBulkUpsert) {
        TTestHelper helper;

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> request(new TEvDataShardLoad::TEvTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetTabletId(helper.Table.TabletId);
        command.SetTableId(helper.Table.UserTable.GetPathId());
        command.SetInflight(3);

        helper.RunUpsertTestLoad(std::move(request), expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertLocalMkql) {
        TTestHelper helper;

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> request(new TEvDataShardLoad::TEvTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertLocalMkqlStart();

        command.SetRowCount(expectedRowCount);
        command.SetTabletId(helper.Table.TabletId);
        command.SetTableId(helper.Table.UserTable.GetPathId());
        command.SetInflight(3);

        helper.RunUpsertTestLoad(std::move(request), expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteKqpUpsert) {
        TTestHelper helper;

        const ui64 expectedRowCount = 20;

        std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> request(new TEvDataShardLoad::TEvTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertKqpStart();

        command.SetRowCount(expectedRowCount);
        command.SetTabletId(helper.Table.TabletId);
        command.SetTableId(helper.Table.UserTable.GetPathId());
        command.SetInflight(5);
        command.SetPath("/Root");

        helper.RunUpsertTestLoad(std::move(request), expectedRowCount);
    }

} // Y_UNIT_TEST_SUITE(UpsertLoad)

Y_UNIT_TEST_SUITE(ReadLoad) {
    Y_UNIT_TEST(ShouldReadIterate) {
        TTestHelper helper;

        const ui64 expectedRowCount = 1000;

        std::unique_ptr<TEvDataShardLoad::TEvTestLoadRequest> request(new TEvDataShardLoad::TEvTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableReadIteratorStart();

        command.AddChunks(0);
        command.AddChunks(1);
        command.AddChunks(10);

        command.AddInflights(1);

        command.SetRowCount(expectedRowCount);
        command.SetPath("/Root/usertable");

        auto result = helper.RunTestLoad(std::move(request));
        UNIT_ASSERT(result->Report);

        UNIT_ASSERT_VALUES_EQUAL(result->Report->SubtestCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(result->Report->OperationsOK, (4 * expectedRowCount));

        // sanity check that there was data in table
        helper.CheckKeysCount(expectedRowCount);
    }

} // Y_UNIT_TEST_SUITE(ReadLoad)

} // namespace NKikimr
