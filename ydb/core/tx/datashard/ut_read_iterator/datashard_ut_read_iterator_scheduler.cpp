#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_read.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>

namespace NKikimr {

using namespace Tests;

namespace {

const TString TEST_DATABASE_ID = "/Root";
const TString TEST_POOL_ID = "test_pool";

// Minimal setup that sets up the scheduler BEFORE the shard is created.
//
// Parameters:
//   readLimitMs           — ReadLimit for the test pool (nullopt = unlimited).
//   blockSchedulerFactory — Drop every TEvGetReadFactory event so the leader
//                           shard never receives a real factory from the scheduler.
//                           The constructor then advances simulated time by 2 s to
//                           let the shard's built-in 1-second fail-safe timer fire,
//                           completing mediator-state init with a null factory.
//   withFollower          — Create the table with one follower and route all
//                           SendRead() calls through a ForceFollower pipe.
//                           Followers never request a scheduler factory themselves,
//                           so quota is never applied on the follower regardless of
//                           the pool settings.
struct TSchedulerTestHelper {
    Tests::TServer::TPtr Server;
    TActorId Sender;

    TTableId TableId;
    ui64 TabletId = 0;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;

    // Default read target: follower pipe when withFollower=true, leader pipe otherwise.
    TActorId ClientId;
    // Always the leader pipe (available even when withFollower=true, for comparison tests).
    TActorId LeaderClientId;

    bool WithFollower = false;

    explicit TSchedulerTestHelper(std::optional<ui64> readLimitMs = std::nullopt,
                                  bool blockSchedulerFactory = false,
                                  bool withFollower = false)
        : WithFollower(withFollower)
    {
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

        if (blockSchedulerFactory) {
            // Drop TEvGetReadFactory so the scheduler never sends a real factory
            // to the leader shard.  The leader will rely on its 1-second fail-safe
            // timer instead.  Follower shards never send this event at all, so
            // dropping it has no effect on their behaviour.
            runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == NKqp::NScheduler::TEvGetReadFactory::EventType) {
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });
        }

        {
            auto ev = std::make_unique<TEvAddDatabase>(TEST_DATABASE_ID);
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        }
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
        if (withFollower) {
            opts.Followers(1);
        }
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "table-1", opts);

        TableId = tableId;
        TabletId = shards.at(0);

        auto [tables, ownerId] = GetTables(Server, TabletId);
        UserTable = tables["table-1"];

        LeaderClientId = runtime.ConnectToPipe(TabletId, Sender, 0, GetPipeConfigWithRetries());

        if (withFollower) {
            auto followerConfig = GetPipeConfigWithRetries();
            followerConfig.ForceFollower = true;
            ClientId = runtime.ConnectToPipe(TabletId, Sender, 0, followerConfig);
        } else {
            ClientId = LeaderClientId;
        }

        if (blockSchedulerFactory) {
            // Advance simulated time past the 1-second fail-safe timer so the
            // leader shard completes mediator-state restoration with a null factory.
            runtime.SimulateSleep(TDuration::Seconds(2));
        }
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

    std::unique_ptr<TEvDataShard::TEvRead> MakeReadRequest(ui64 readId, const TString& poolId = TEST_POOL_ID) const {
        auto snapshot = CreateVolatileSnapshot(Server, {"/Root/table-1"}, TDuration::Hours(1));
        auto request = GetBaseReadRequest(TableId, UserTable.GetDescription(), readId, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        request->Record.SetPoolId(poolId);
        return request;
    }

    // Send to the default target (follower if WithFollower, leader otherwise).
    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(TEvDataShard::TEvRead* request) const {
        return ::NKikimr::SendRead(Server, TabletId, request, Sender, 0,
                                   GetPipeConfigWithRetries(), ClientId);
    }

    // Always send to the leader (useful for leader-vs-follower comparison tests).
    std::unique_ptr<TEvDataShard::TEvReadResult> SendReadToLeader(TEvDataShard::TEvRead* request) const {
        return ::NKikimr::SendRead(Server, TabletId, request, Sender, 0,
                                   GetPipeConfigWithRetries(), LeaderClientId);
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

    // Register an extra pool with zero quota in the same database.
    void AddZeroQuotaPool(const TString& poolId) const {
        using namespace NKqp::NScheduler;
        auto& runtime = *Server->GetRuntime();
        NResourcePool::TPoolSettings params;
        params.TotalCpuLimitPercentPerNode = 0.;
        auto ev = std::make_unique<TEvAddPool>(TEST_DATABASE_ID, poolId, params);
        runtime.Send(NKqp::MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        runtime.SimulateSleep(TDuration::Zero());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler) {

    // A plain key read with PoolId set must succeed when quota is available.
    //
    // For the leader (WithFollower=false):
    //   additionally verifies that a zero-quota pool returns OVERLOADED, proving
    //   that quota IS actively enforced.
    //
    // For the follower (WithFollower=true):
    //   the zero-quota pool must also return SUCCESS, because followers skip the
    //   quota machinery entirely (SchedulableReadFactory is never set on followers).
    Y_UNIT_TEST_TWIN(ShouldReadWithSchedulerPoolId, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
                                    /*blockSchedulerFactory=*/false,
                                    /*withFollower=*/WithFollower);
        helper.Upsert(1, 100);

        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});
        Sleep(TDuration::Seconds(1));

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
        auto cells = result->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(cells[3].AsValue<ui32>(), 100u);

        // Register a zero-quota pool and observe the difference between leader and follower.
        const TString zeroPoolId = "zero_quota_pool";
        helper.AddZeroQuotaPool(zeroPoolId);
        auto zeroRequest = helper.MakeReadRequest(2, zeroPoolId);
        AddKeyQuery(*zeroRequest, {1, 1, 1});
        auto zeroResult = helper.SendRead(zeroRequest.release());
        UNIT_ASSERT(zeroResult);

        if constexpr (WithFollower) {
            // Followers bypass the quota check — must still succeed.
            UNIT_ASSERT_VALUES_EQUAL_C(zeroResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS,
                "Follower must bypass quota even for a zero-quota pool");
        } else {
            // Leader enforces the quota — must reject the read.
            UNIT_ASSERT_VALUES_EQUAL_C(zeroResult->Record.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED,
                "Leader must return OVERLOADED for a zero-quota pool, proving quota is enforced");
        }
    }

    // A range read that produces multiple continuation chunks must deliver all rows
    // correctly when PoolId is set.
    // On the leader each chunk consumes quota and immediately returns it.
    // On the follower quota is never consulted.
    Y_UNIT_TEST_TWIN(ShouldContinuationReadWithSchedulerPoolId, WithFollower) {
        constexpr ui32 kRows = 5;
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
                                    /*blockSchedulerFactory=*/false,
                                    /*withFollower=*/WithFollower);
        helper.UpsertMany(1, kRows);

        auto request = helper.MakeReadRequest(1);
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

    // When the pool's ReadLimit is 0 ms (quota permanently exhausted):
    //   leader  → OVERLOADED
    //   follower → SUCCESS (quota bypass)
    //
    // The WithFollower=false case is the canonical "leader rejects exhausted quota"
    // test.  The WithFollower=true case additionally verifies that the follower
    // serves the same data successfully, demonstrating the bypass.
    Y_UNIT_TEST_TWIN(ShouldReadWhenQuotaExhausted, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/0u,
                                    /*blockSchedulerFactory=*/false,
                                    /*withFollower=*/WithFollower);
        helper.Upsert(1, 100);

        // Leader must always return OVERLOADED regardless of follower mode.
        {
            auto request = helper.MakeReadRequest(1);
            AddKeyQuery(*request, {1, 1, 1});
            auto result = helper.SendReadToLeader(request.release());
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus().GetCode(), Ydb::StatusIds::OVERLOADED,
                "Leader must return OVERLOADED when quota is exhausted");
        }

        if constexpr (WithFollower) {
            // Follower must succeed — it never checks quota.
            auto request = helper.MakeReadRequest(2);
            AddKeyQuery(*request, {1, 1, 1});
            auto result = helper.SendRead(request.release());  // ForceFollower pipe
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS,
                "Follower must succeed even when the leader's quota is exhausted");
            UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
        }
    }

    // Tight quota (5 ms/s) on the leader: continuations must retry internally
    // rather than returning OVERLOADED to the client.
    // This scenario only applies to the leader; followers are always quota-free.
    Y_UNIT_TEST(ShouldCompleteContinuationReadWithTightQuota) {
        constexpr ui32 kRows = 3;
        TSchedulerTestHelper helper(/*readLimitMs=*/5u);
        helper.UpsertMany(1, kRows);

        auto request = helper.MakeReadRequest(1);
        AddRangeQuery<ui32>(*request, {1, 1, 1}, true, {kRows, Max<ui32>(), Max<ui32>()}, true);
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

    // Reads without PoolId must succeed regardless of scheduler presence or quota.
    // True for both leader and follower.
    Y_UNIT_TEST_TWIN(ShouldReadWithoutPoolIdUnaffectedByScheduler, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
                                    /*blockSchedulerFactory=*/false,
                                    /*withFollower=*/WithFollower);
        helper.Upsert(3, 300);

        auto snapshot = CreateVolatileSnapshot(helper.Server, {"/Root/table-1"}, TDuration::Hours(1));
        auto request = GetBaseReadRequest(helper.TableId, helper.UserTable.GetDescription(),
                                          1, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        // Deliberately omit SetPoolId — quota machinery must not be invoked.
        AddKeyQuery(*request, {3, 3, 3});

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
    }

    // When the datashard never receives a factory response from the scheduler
    // (e.g. the scheduler is temporarily unavailable), it falls back to reading
    // without quota after the built-in 1-second fail-safe timer fires.
    // Reads with a PoolId must still succeed on both leader and follower.
    //
    // Note: followers never request a factory in the first place, so they are
    // always in this "no-factory" state.  The blockSchedulerFactory flag here
    // affects only the leader; it verifies that the leader's fallback path also
    // works correctly.
    Y_UNIT_TEST_TWIN(ShouldReadSuccessfullyWhenSchedulerDoesNotRespond, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
                                    /*blockSchedulerFactory=*/true,
                                    /*withFollower=*/WithFollower);
        helper.Upsert(1, 100);

        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT_C(result, "Expected read to succeed when scheduler did not respond");
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);

        auto cells = result->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(cells[3].AsValue<ui32>(), 100u);
    }

} // Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler)

} // namespace NKikimr
