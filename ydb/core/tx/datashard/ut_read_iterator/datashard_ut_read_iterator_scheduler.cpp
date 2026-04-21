#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_read.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>

namespace NKikimr {

using namespace Tests;

namespace {

const TString TEST_DATABASE_ID = "/Root";
const TString TEST_POOL_ID = "test_pool";

// Minimal setup that sets up the scheduler BEFORE the shard is created.
struct TSchedulerTestHelper {
    Tests::TServer::TPtr Server;
    TActorId Sender;

    TTableId TableId;
    ui64 TabletId = 0;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;
    TActorId ClientId;

    explicit TSchedulerTestHelper(std::optional<ui64> readLimitMs = std::nullopt) {
        using namespace NKqp;
        using namespace NKqp::NScheduler;
        using namespace NKqp::NScheduler::NHdrf;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePoolsScheduler(true);

        Server = new TServer(serverSettings);
        auto& runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE_SCHEDULER, NLog::PRI_TRACE);

        // Create test database
        {
            auto ev = std::make_unique<TEvAddDatabase>(TEST_DATABASE_ID);
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        }

        // Create test pool
        {
            auto ev = std::make_unique<TEvAddPool>(TEST_DATABASE_ID, TEST_POOL_ID);
            if (readLimitMs.has_value()) {
                ev->Params.TotalCpuLimitPercentPerNode = (*readLimitMs) / 10.;
            }
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        }

        InitRoot(Server, Sender);

        TVector<TShardedTableOptions::TColumn> columns = {
            {"key1", "Uint32", true, false},
            {"key2", "Uint32", true, false},
            {"key3", "Uint32", true, false},
            {"value", "Uint32", false, false},
        };

        auto opts = TShardedTableOptions().Shards(1).Columns(columns);
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "table-1", opts);

        TableId = tableId;
        TabletId = shards.at(0);

        auto [tables, ownerId] = GetTables(Server, TabletId);
        UserTable = tables["table-1"];

        ClientId = runtime.ConnectToPipe(TabletId, Sender, 0, GetPipeConfigWithRetries());
    }

    void Upsert(ui32 key, ui32 value) const {
        ExecSQL(Server, Sender, TStringBuilder()
            << "UPSERT INTO `/Root/table-1` (key1, key2, key3, value) VALUES ("
            << key << ", " << key << ", " << key << ", " << value << ");");
    }

    void UpsertMany(ui32 first, ui32 count) const {
        for (ui32 i = 0; i < count; ++i) {
            Upsert(first + i, (first + i) * 100);
        }
    }

    std::unique_ptr<TEvDataShard::TEvRead> MakeReadRequest(ui64 readId,
        NKikimrDataEvents::EDataFormat fmt = NKikimrDataEvents::FORMAT_CELLVEC) const
    {
        auto snapshot = CreateVolatileSnapshot(Server, {"/Root/table-1"}, TDuration::Hours(1));
        auto request = GetBaseReadRequest(TableId, UserTable.GetDescription(), readId, fmt, snapshot);
        request->Record.SetPoolId(TEST_POOL_ID);
        return request;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(TEvDataShard::TEvRead* request) const {
        return ::NKikimr::SendRead(Server, TabletId, request, Sender, 0,
                                   GetPipeConfigWithRetries(), ClientId);
    }

    void SendReadAsync(TEvDataShard::TEvRead* request) const {
        ::NKikimr::SendReadAsync(Server, TabletId, request, Sender, 0,
                                  GetPipeConfigWithRetries(), ClientId);
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> WaitResult(TDuration timeout = TDuration::Seconds(30)) const {
        return WaitReadResult(Server, timeout);
    }

    void SendAck(const NKikimrTxDataShard::TEvReadResult& result, ui64 rows, ui64 bytes) {
        auto& runtime = *Server->GetRuntime();
        auto* ack = new TEvDataShard::TEvReadAck();
        ack->Record.SetReadId(result.GetReadId());
        ack->Record.SetSeqNo(result.GetSeqNo());
        ack->Record.SetMaxRows(rows);
        ack->Record.SetMaxBytes(bytes);
        runtime.SendToPipe(TabletId, Sender, ack, 0, GetPipeConfigWithRetries(), ClientId);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler) {

    // A plain key read with PoolId set must succeed when the scheduler service is present and quota is available.
    Y_UNIT_TEST(ShouldReadWithSchedulerQuota) {
        TSchedulerTestHelper helper; // default = unlimited quota
        helper.Upsert(1, 100);

        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});

        Sleep(TDuration::Seconds(1));

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
        // Verify row contents: key=(1,1,1), value=100
        auto cells = result->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(cells[3].AsValue<ui32>(), 100u);
    }

    // A range read that produces multiple continuation chunks must deliver all
    // rows correctly when PoolId is set (quota is consumed and returned per chunk).
    Y_UNIT_TEST(ShouldContinuationReadWithSchedulerQuota) {
        constexpr ui32 kRows = 5;
        TSchedulerTestHelper helper;
        helper.UpsertMany(1, kRows);

        auto request = helper.MakeReadRequest(1, NKikimrDataEvents::FORMAT_CELLVEC);
        AddRangeQuery<ui32>(*request, {1, 1, 1}, true, {kRows, Max<ui32>(), Max<ui32>()}, true);
        // Force one row per result so we exercise the continuation path.
        request->Record.SetMaxRowsInResult(1);

        helper.SendReadAsync(request.release());

        ui32 rowsReceived = 0;
        while (true) {
            auto result = helper.WaitResult();
            UNIT_ASSERT_C(result, "Timed out waiting for read result");
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            rowsReceived += result->GetRowsCount();
            if (result->Record.GetFinished()) {
                break;
            }
            helper.SendAck(result->Record, 1000, 50_MB);
        }

        UNIT_ASSERT_VALUES_EQUAL(rowsReceived, kRows);
    }

    // When the pool's ReadLimit is 0 ms (quota permanently exhausted), an
    // initial TEvRead with that PoolId must receive OVERLOADED — not a crash
    // and not SUCCESS.
    Y_UNIT_TEST(ShouldGetOverloadedOnInitialReadWhenQuotaExhausted) {
        TSchedulerTestHelper helper(/*readLimitMs=*/0u);
        helper.Upsert(1, 100);

        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED);
    }

    // Verify that setting PoolId on a request against a scheduler with tight
    // quota still delivers all rows — continuations must retry internally rather
    // than returning OVERLOADED to the client.
    // With ReadLimit=10 ms and in-test reads completing in ~0 ms, ReturnQuota
    // refills the bucket immediately, so reads complete quickly.
    Y_UNIT_TEST(ShouldCompleteContinuationReadWithTightQuota) {
        constexpr ui32 kRows = 3;
        // 10 ms per second = tight budget; in unit-tests reads take ~0 ms so
        // ReturnQuota() restores almost all quota, allowing every continuation
        // to proceed without a real delay.
        TSchedulerTestHelper helper(/*readLimitMs=*/10u);
        helper.UpsertMany(1, kRows);

        auto request = helper.MakeReadRequest(1, NKikimrDataEvents::FORMAT_CELLVEC);
        AddRangeQuery<ui32>(*request, {1, 1, 1}, true, {kRows, Max<ui32>(), Max<ui32>()}, true);
        request->Record.SetMaxRowsInResult(1);

        helper.SendReadAsync(request.release());

        ui32 rowsReceived = 0;
        while (true) {
            auto result = helper.WaitResult();
            UNIT_ASSERT_C(result, "Timed out waiting for read result");
            // Continuations must never surface OVERLOADED to the client.
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(),
                                      Ydb::StatusIds::SUCCESS);
            rowsReceived += result->GetRowsCount();
            if (result->Record.GetFinished()) {
                break;
            }
            helper.SendAck(result->Record, 1000, 50_MB);
        }

        UNIT_ASSERT_VALUES_EQUAL(rowsReceived, kRows);
    }

    // Reads without PoolId must be unaffected by scheduler presence.
    Y_UNIT_TEST(ShouldReadWithoutPoolIdUnaffectedByScheduler) {
        TSchedulerTestHelper helper;
        helper.Upsert(3, 300);

        // Deliberately omit SetPoolId — quota machinery must not be invoked.
        auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        auto request = GetBaseReadRequest(helper.TableId, helper.UserTable.GetDescription(),
                                          1, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        AddKeyQuery(*request, {3, 3, 3});

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
    }

} // Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler)

} // namespace NKikimr
