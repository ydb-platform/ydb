#include <ydb/core/kqp/ut/common/kqp_ut_common.h> // Y_UNIT_TEST_(TWIN|QUAD), Q_
#include <ydb/core/load_test/events.h>
#include <ydb/core/load_test/ycsb/common.h>
#include <ydb/core/load_test/ycsb/test_load_actor.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/yql/minikql/mkql_node_printer.h>

namespace NKikimr {

using namespace NKikimr::NDataShardLoad;
using namespace NSchemeShard;
using namespace Tests;

namespace {

// We use YCSB defaule schema: table with 'key' column
// and 'field0' to 'field9' value columns. All fields are Utf8
const TString DefaultTableName = "usertable";
const TString FieldPrefix = "field";
const size_t ValueColumnsCount = 10;

void InitRoot(Tests::TServer::TPtr server,
    TActorId sender) {
    server->SetupRootStoragePools(sender);
}

ui64 RunSchemeTx(
        TTestActorRuntimeBase& runtime,
        THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
        TActorId sender = {},
        bool viaActorSystem = false,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress)
{
    if (!sender) {
        sender = runtime.AllocateEdgeActor();
    }

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

    return ev->Get()->Record.GetTxId();
}

THolder<TEvTxUserProxy::TEvProposeTransaction> SchemeTxTemplate(
        NKikimrSchemeOp::EOperationType type,
        const TString& workingDir = {})
{
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(type);

    if (workingDir) {
        tx.SetWorkingDir(workingDir);
    }

    return request;
}

void WaitTxNotification(Tests::TServer::TPtr server, TActorId sender, ui64 txId) {
    auto &runtime = *server->GetRuntime();
    auto &settings = server->GetSettings();

    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void CreateTable(Tests::TServer::TPtr server,
                 TActorId sender,
                 const TString &root,
                 const TString& tableName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateTable, root);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
    NKikimrSchemeOp::TTableDescription* desc = tx.MutableCreateTable();

    UNIT_ASSERT(desc);
    desc->SetName(tableName);
    desc->SetUniformPartitionsCount(1);

    {
        auto col = desc->AddColumns();
        col->SetName("id");
        col->SetType("Utf8");
        col->SetNotNull(true);
        desc->AddKeyColumnNames("id");
    }

    for (size_t i = 0; i < ValueColumnsCount; ++i) {
        TString fieldName = FieldPrefix + ToString(i);
        auto col = desc->AddColumns();
        col->SetName(fieldName);
        col->SetType("String");
        col->SetNotNull(false);
    }

    WaitTxNotification(server, sender, RunSchemeTx(*server->GetRuntime(), std::move(request), sender));
}

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer::TPtr server,
                                                     TActorId sender,
                                                     const TString &path)
{
    auto &runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    TVector<ui64> shards;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    auto reply = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDescribeSchemeResult>(handle);

    return *reply->MutableRecord();
}

TVector<ui64> GetTableShards(Tests::TServer::TPtr server,
                             TActorId sender,
                             const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(server, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    return shards;
}

using TTableInfoMap = THashMap<TString, NKikimrTxDataShard::TEvGetInfoResponse::TUserTable>;

std::pair<TTableInfoMap, ui64> GetTables(
    Tests::TServer::TPtr server,
    ui64 tabletId)
{
    auto &runtime = *server->GetRuntime();

    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvGetInfoRequest>();
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TTableInfoMap result;

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
    for (auto& table: response->Record.GetUserTables()) {
        result[table.GetName()] = table;
    }

    auto ownerId = response->Record.GetTabletInfo().GetSchemeShard();

    return std::make_pair(result, ownerId);
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

    request.Ranges.emplace_back(std::move(fromBuf), std::move(toBuf), fromInclusive, toInclusive);
}

struct TTableInfo {
    TString Name;

    ui64 TabletId;
    ui64 OwnerId;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;

    TActorId ClientId;
};

struct TSettings {
    TString TableName = DefaultTableName;
    bool CreateTable = true;

    TSettings() = default;

    TSettings(const char* tableName, bool createTable = true)
        : TableName(tableName)
        , CreateTable(createTable)
    {
    }
};

struct TTestHelper {
    TTestHelper(const TSettings& settings = {})
        : Settings(settings)
    {
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

        runtime.SetLogPriority(NKikimrServices::DS_LOAD_TEST, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);

        InitRoot(Server, Sender);

        Table.Name = Settings.TableName;
        if (!Settings.CreateTable)
            return;

        CreateTable(Server, Sender, "/Root", Settings.TableName);
        ResolveTable();
    }

    void ResolveTable() {
        auto shards = GetTableShards(Server, Sender, "/Root/" + Settings.TableName);
        Table.TabletId = shards.at(0);

        auto [tables, ownerId] = GetTables(Server, Table.TabletId);
        Table.OwnerId = ownerId;
        Table.UserTable = tables[Settings.TableName];

        auto &runtime = *Server->GetRuntime();
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

        record.SetResultFormat(::NKikimrDataEvents::FORMAT_CELLVEC);

        return request;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> WaitReadResult(TDuration timeout = TDuration::Max()) {
        auto &runtime = *Server->GetRuntime();
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(handle, timeout);
        if (!handle) {
            return nullptr;
        }
        return std::unique_ptr<TEvDataShard::TEvReadResult>(handle->Release<TEvDataShard::TEvReadResult>().Release());
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

    void CheckKeys(size_t keyFrom, size_t expectedRowCount) {
        Y_UNUSED(keyFrom);

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

        const auto& record = readResult->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetRowsCount(), expectedRowCount);

        auto nrows = readResult->GetRowsCount();
        for (size_t i = 0; i < nrows; ++i) {
            auto cells = readResult->GetCells(i);
            const auto& keyCell = cells[0];
            TString key(keyCell.Data(), keyCell.Size());
            UNIT_ASSERT_VALUES_EQUAL(key, GetKey(i + keyFrom));
        }
    }

    std::unique_ptr<TEvLoad::TEvLoadTestFinished> RunTestLoad(
        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request)
    {
        request->Record.SetNotifyWhenFinished(true);
        auto &runtime = *Server->GetRuntime();
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters());
        runtime.Register(CreateTestLoadActor(request->Record, Sender, counters, 0));

        {
            // wait until load finished
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvLoad::TEvLoadTestFinished>(handle);
            UNIT_ASSERT(handle);
            auto response = IEventHandle::Release<TEvLoad::TEvLoadTestFinished>(handle);
            UNIT_ASSERT(response->ErrorReason.Empty());

            return std::unique_ptr<TEvLoad::TEvLoadTestFinished>(response.Release());
        }
    }

    void RunUpsertTestLoad(
        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> loadRequest,
        size_t keyFrom,
        size_t expectedRowCount,
        bool forceResolve = false)
    {
        RunTestLoad(std::move(loadRequest));
        if (!Settings.CreateTable || forceResolve)
            ResolveTable();
        CheckKeys(keyFrom, expectedRowCount);
    }

public:
    const TSettings Settings;
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

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(DefaultTableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertBatch) {
        // same as ShouldWriteDataBulkUpsert, but with batch size
        TTestHelper helper;

        const ui64 expectedRowCount = 100;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);
        command.SetBatchSize(7);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(DefaultTableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsert2) {
        // check nondefault tablename
        TSettings settings("JustTable");
        TTestHelper helper(settings);

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(settings.TableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertKeyFrom) {
        // check nondefault keyFrom
        TTestHelper helper;

        const ui64 keyFrom = 12345;
        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);
        command.SetKeyFrom(keyFrom);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(DefaultTableName);

        helper.RunUpsertTestLoad(std::move(request), keyFrom, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertLocalMkql) {
        TTestHelper helper;

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertLocalMkqlStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(DefaultTableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertLocalMkql2) {
        // check nondefault tablename
        TSettings settings("JustTable");
        TTestHelper helper(settings);

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertLocalMkqlStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(settings.TableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteDataBulkUpsertLocalMkqlKeyFrom) {
        // check nondefault keyFrom
        TTestHelper helper;

        const ui64 keyFrom = 12345;
        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertLocalMkqlStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);
        command.SetKeyFrom(keyFrom);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetTableName(DefaultTableName);

        helper.RunUpsertTestLoad(std::move(request), keyFrom, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteKqpUpsert) {
        TTestHelper helper;

        const ui64 expectedRowCount = 20;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertKqpStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(5);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetWorkingDir("/Root");
        target.SetTableName("usertable");

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteKqpUpsert2) {
        // check nondefault tablename
        TSettings settings("JustTable");
        TTestHelper helper(settings);

        const ui64 expectedRowCount = 20;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertKqpStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(5);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetWorkingDir("/Root");
        target.SetTableName(settings.TableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldWriteKqpUpsertKeyFrom) {
        TTestHelper helper;

        const ui64 keyFrom = 12345;
        const ui64 expectedRowCount = 20;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertKqpStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(5);
        command.SetKeyFrom(keyFrom);

        auto& target = *record.MutableTargetShard();
        target.SetTabletId(helper.Table.TabletId);
        target.SetTableId(helper.Table.UserTable.GetPathId());
        target.SetWorkingDir("/Root");
        target.SetTableName("usertable");

        helper.RunUpsertTestLoad(std::move(request), keyFrom, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldCreateTable) {
        TSettings settings("BrandNewTable", false);
        TTestHelper helper(settings);

        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetCreateTable(true);
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName(settings.TableName);

        setupTable.SetMinParts(11);
        setupTable.SetMaxParts(13);
        setupTable.SetMaxPartSizeMb(1234);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount);

        const auto& description = helper.Table.UserTable.GetDescription();
        const auto& partitioning = description.GetPartitionConfig().GetPartitioningPolicy();
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetMinPartitionsCount(), 11UL);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetMaxPartitionsCount(), 13UL);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetSizeToSplit(), 1234UL << 20);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetSplitByLoadSettings().GetEnabled(), false);
    }

    Y_UNIT_TEST(ShouldDropCreateTable) {
        TSettings settings("table");
        TTestHelper helper(settings);

        {
            // write some data, which should not be seen after drop
            std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
            auto& record = request->Record;
            auto& command = *record.MutableUpsertBulkStart();

            command.SetRowCount(100);
            command.SetInflight(3);

            auto& target = *record.MutableTargetShard();
            target.SetTabletId(helper.Table.TabletId);
            target.SetTableId(helper.Table.UserTable.GetPathId());

            helper.RunUpsertTestLoad(std::move(request), 0, 100);
        }

        // because of drop we should see only these rows
        const ui64 expectedRowCount = 10;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableUpsertBulkStart();

        command.SetRowCount(expectedRowCount);
        command.SetInflight(3);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetDropTable(true);
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName(settings.TableName);

        helper.RunUpsertTestLoad(std::move(request), 0, expectedRowCount, true);

        // check default table params
        const auto& description = helper.Table.UserTable.GetDescription();
        const auto& partitioning = description.GetPartitionConfig().GetPartitioningPolicy();
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetMinPartitionsCount(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetMaxPartitionsCount(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetSizeToSplit(), 2000UL << 20);
        UNIT_ASSERT_VALUES_EQUAL(partitioning.GetSplitByLoadSettings().GetEnabled(), false);
    }

} // Y_UNIT_TEST_SUITE(UpsertLoad)

Y_UNIT_TEST_SUITE(ReadLoad) {
    Y_UNIT_TEST(ShouldReadIterate) {
        TTestHelper helper;

        const ui64 expectedRowCount = 1000;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableReadIteratorStart();

        command.AddChunks(0);
        command.AddChunks(1);
        command.AddChunks(10);

        command.AddInflights(1);
        command.SetRowCount(expectedRowCount);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName("usertable");

        auto result = helper.RunTestLoad(std::move(request));

        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["subtests"].GetInteger(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["oks"].GetInteger(), (4 * expectedRowCount));

        // sanity check that there was data in table
        helper.CheckKeys(0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldReadIterateMoreThanRows) {
        // 10 rows, but ask to read 1000
        TTestHelper helper;

        const ui64 expectedRowCount = 10;
        const ui64 expectedReadCount = 1000;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableReadIteratorStart();

        command.AddChunks(0);
        command.AddChunks(1);
        command.AddChunks(10);

        command.AddInflights(1);
        command.SetRowCount(expectedRowCount);
        command.SetReadCount(expectedReadCount);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName("usertable");

        auto result = helper.RunTestLoad(std::move(request));

        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["subtests"].GetInteger(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["oks"].GetInteger(), (3 * expectedRowCount + expectedReadCount));

        // sanity check that there was data in table
        helper.CheckKeys(0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldReadKqp) {
        TTestHelper helper;

        const ui64 expectedRowCount = 100;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableReadKqpStart();

        command.AddInflights(10);
        command.SetRowCount(expectedRowCount);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName("usertable");

        auto result = helper.RunTestLoad(std::move(request));

        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["oks"].GetInteger(), (10 * expectedRowCount));

        // sanity check that there was data in table
        helper.CheckKeys(0, expectedRowCount);
    }

    Y_UNIT_TEST(ShouldReadKqpMoreThanRows) {
        // 10 rows, but ask to read 100
        TTestHelper helper;

        const ui64 expectedRowCount = 10;
        const ui64 expectedReadCount = 100;

        std::unique_ptr<TEvDataShardLoad::TEvYCSBTestLoadRequest> request(new TEvDataShardLoad::TEvYCSBTestLoadRequest());
        auto& record = request->Record;
        auto& command = *record.MutableReadKqpStart();

        command.AddInflights(10);
        command.SetRowCount(expectedRowCount);
        command.SetReadCount(expectedReadCount);

        auto& setupTable = *record.MutableTableSetup();
        setupTable.SetWorkingDir("/Root");
        setupTable.SetTableName("usertable");

        auto result = helper.RunTestLoad(std::move(request));

        UNIT_ASSERT_VALUES_EQUAL(result->JsonResult["oks"].GetInteger(), (10 * expectedReadCount));

        // sanity check that there was data in table
        helper.CheckKeys(0, expectedRowCount);
    }

} // Y_UNIT_TEST_SUITE(ReadLoad)

} // namespace NKikimr
