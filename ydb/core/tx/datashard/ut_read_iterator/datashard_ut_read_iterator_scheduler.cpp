#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_read.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <util/generic/scope.h>
#include <util/generic/vector.h>

#include <functional>

namespace NKikimr {

using namespace Tests;

namespace {

const TString TEST_DATABASE_ID = "/Root";
const TString TEST_POOL_ID = "test_pool";

// Minimal setup that sets up the scheduler BEFORE the shard is created.
//
// Parameters:
//   readLimitMs           — ReadLimit for the test pool (nullopt = unlimited).
//   withFollower          — Create the table with one follower and route all
//                           SendRead() calls through a ForceFollower pipe.
//                           Followers never request a scheduler factory themselves,
//                           so quota is never applied on the follower regardless of
//                           the pool settings.
//   numShards             — Number of shards to create (default 1).
struct TSchedulerTestHelper {
    Tests::TServer::TPtr Server;
    TActorId Sender;

    TTableId TableId;
    TVector<ui64> TabletIds;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;

    TVector<TActorId> ClientIds;
    TVector<TActorId> LeaderClientIds;

    bool WithFollower = false;

    explicit TSchedulerTestHelper(std::optional<ui64> readLimitMs = std::nullopt,
                                  bool withFollower = false,
                                  ui32 numShards = 1,
                                  std::optional<bool> memoryEnabled = std::nullopt)
        : WithFollower(withFollower)
    {
        Y_ABORT_UNLESS(!(withFollower && numShards > 1), "Follower mode with multiple shards not supported");
        Y_ABORT_UNLESS(numShards >= 1, "numShards must be at least 1");

        using namespace NKqp;
        using namespace NKqp::NScheduler;
        using namespace NKqp::NScheduler::NHdrf;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePoolsScheduler(true);
        if (memoryEnabled) {
            serverSettings.AppConfig->MutableFeatureFlags()->SetEnableComputeSchedulerMemory(*memoryEnabled);
        }

        Server = new TServer(serverSettings);
        auto& runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE_SCHEDULER, NLog::PRI_TRACE);

        InitRoot(Server, Sender);
        {
            auto ev = std::make_unique<TEvAddDatabase>(TEST_DATABASE_ID);
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        }
        CreatePool(TEST_POOL_ID, readLimitMs);

        TVector<TShardedTableOptions::TColumn> columns = {
            {"key1", "Uint32", true, false},
            {"key2", "Uint32", true, false},
            {"key3", "Uint32", true, false},
            {"value", "Uint32", false, false},
        };
        auto opts = TShardedTableOptions().Shards(numShards).Columns(columns);
        if (withFollower) {
            opts.Followers(1);
        }
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "table-1", opts);

        TableId = tableId;
        TabletIds = shards;

        auto [tables, ownerId] = GetTables(Server, TabletIds[0]);
        UserTable = tables["table-1"];

        for (ui64 tabletId : TabletIds) {
            LeaderClientIds.push_back(runtime.ConnectToPipe(tabletId, Sender, 0, GetPipeConfigWithRetries()));
            if (withFollower) {
                auto followerConfig = GetPipeConfigWithRetries();
                followerConfig.ForceFollower = true;
                ClientIds.push_back(runtime.ConnectToPipe(tabletId, Sender, 0, followerConfig));
            } else {
                ClientIds.push_back(LeaderClientIds.back());
            }
        }
    }

    ui64 TabletId() const { return TabletIds[0]; }
    TActorId ClientId() const { return ClientIds[0]; }
    TActorId LeaderClientId() const { return LeaderClientIds[0]; }

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
        request->Record.SetDatabaseId(TEST_DATABASE_ID);
        request->Record.SetPoolId(poolId);
        return request;
    }

    std::unique_ptr<TEvDataShard::TEvRead> MakeRangeReadRequest(ui64 readId, ui32 lo, ui32 hi, ui32 maxRowsInResult = 0, const TString& poolId = TEST_POOL_ID) const {
        auto request = MakeReadRequest(readId, poolId);
        AddRangeQuery<ui32>(*request, {lo, lo, lo}, true, {hi, hi, hi}, true);
        if (maxRowsInResult > 0) {
            request->Record.SetMaxRowsInResult(maxRowsInResult);
        }
        return request;
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(TEvDataShard::TEvRead* request, ui32 shardIdx = 0) const {
        return ::NKikimr::SendRead(Server, TabletIds[shardIdx], request, Sender, 0,
                                   GetPipeConfigWithRetries(), ClientIds[shardIdx]);
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> SendReadToLeader(TEvDataShard::TEvRead* request) const {
        return ::NKikimr::SendRead(Server, TabletId(), request, Sender, 0,
                                   GetPipeConfigWithRetries(), LeaderClientId());
    }

    void SendReadAsync(TEvDataShard::TEvRead* request, ui32 shardIdx = 0) const {
        ::NKikimr::SendReadAsync(Server, TabletIds[shardIdx], request, Sender, 0,
                                  GetPipeConfigWithRetries(), ClientIds[shardIdx]);
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
        runtime.SendToPipe(TabletId(), Sender, ack, 0, GetPipeConfigWithRetries(), ClientId());
    }

    void WaitForPoolQuota(const TString& poolId, ui64 readLimitMs) const {
        auto& runtime = *Server->GetRuntime();
        const auto scheduler = runtime.GetAppData(0).KqpComputeScheduler;
        runtime.WaitFor(TStringBuilder() << poolId << " read quota " << readLimitMs << " ms/s", [&] {
            const auto tree = scheduler->GetStaticTree();
            const auto* db = tree->GetDatabase(TEST_DATABASE_ID);
            const auto* pool = db ? db->GetPool(poolId) : nullptr;
            return pool && pool->GetReadLimit() == TDuration::MilliSeconds(readLimitMs);
        }, TDuration::Seconds(30));
    }

    void ExecPoolDDL(const TString& sql) const {
        auto& runtime = *Server->GetRuntime();
        auto request = MakeSQLRequest(sql, /*dml=*/false);
        request->Record.MutableRequest()->SetDatabase(TEST_DATABASE_ID);
        runtime.Send(NKqp::MakeKqpProxyID(runtime.GetNodeId()), Sender, request.Release());
        const auto response = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(Sender);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS, record.DebugString());
    }

    void CreatePool(const TString& poolId, std::optional<ui64> readLimitMs) const {
        // Register only after scheme creation: the scheduler subscribes to workload manager,
        // which would otherwise report the missing pool and remove it from the published tree.
        ExecPoolDDL(TStringBuilder() << "CREATE RESOURCE POOL `" << poolId
            << "` WITH (TOTAL_CPU_LIMIT_PERCENT_PER_NODE = \""
            << (readLimitMs ? *readLimitMs / 10. : -1.) << "\");");
        auto& runtime = *Server->GetRuntime();
        runtime.Send(NKqp::MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender,
            new NKqp::NScheduler::TEvAddPool(TEST_DATABASE_ID, poolId));
        WaitForPoolQuota(poolId, readLimitMs.value_or(1000));
    }

    NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr ReadQuery(const TString& poolId = TEST_POOL_ID) const {
        auto query = Server->GetRuntime()->GetAppData().KqpComputeScheduler->GetReadQuery(TEST_DATABASE_ID, poolId);
        UNIT_ASSERT(query);
        return query;
    }

    void WithThrottledRead(
            std::unique_ptr<TEvDataShard::TEvRead> request,
            std::function<void()> whileThrottled) const {
        auto& runtime = *Server->GetRuntime();
        auto query = ReadQuery();
        // Block both the no-snapshot and the published-snapshot admission paths.
        constexpr ui64 blockedUsage = 1'000'000;
        query->CpuUsage += blockedUsage;
        query->GetParent()->CpuUsage += blockedUsage;
        bool blocked = true;
        auto release = [&] {
            if (blocked) {
                query->CpuUsage -= blockedUsage;
                query->GetParent()->CpuUsage -= blockedUsage;
                blocked = false;
            }
        };
        Y_DEFER { release(); };

        bool replied = false;
        auto observer = runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto&) { replied = true; });
        // Scheduled quota retries have an empty sender, which TBlockEvents
        // cannot print through FindActorName. Capture them without that lookup.
        TVector<TEvDataShard::TEvReadContinue::TPtr> retries;
        auto retryObserver = runtime.AddObserver<TEvDataShard::TEvReadContinue>(
            [&](TEvDataShard::TEvReadContinue::TPtr& ev) { retries.push_back(std::move(ev)); });
        SendReadAsync(request.release());
        runtime.WaitFor("quota refusal before the first result", [&] { return replied || !retries.empty(); });
        UNIT_ASSERT_C(!replied, "Read bypassed quota instead of waiting before its first result");
        UNIT_ASSERT_C(!retries.empty(), "The test must actually exercise a quota refusal");
        UNIT_ASSERT_VALUES_EQUAL(query->CpuUsage.load(), blockedUsage);
        if (whileThrottled) {
            whileThrottled();
        }
        release();
        retryObserver.Remove();
        for (auto& ev : retries) {
            const auto nodeIdx = ev->GetRecipientRewrite().NodeId() - runtime.GetFirstNodeId();
            runtime.Send(ev.Release(), nodeIdx, /* viaActorSystem */ true);
        }
    }

    std::unique_ptr<TEvDataShard::TEvReadResult> ThrottleAndResume(
            std::unique_ptr<TEvDataShard::TEvRead> request,
            std::function<void()> whileThrottled = {}) const {
        WithThrottledRead(std::move(request), std::move(whileThrottled));
        return WaitResult();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler) {

    // A plain key read with PoolId set must succeed when quota is available.
    //
    // For the leader (WithFollower=false):
    //   additionally verifies that a zero-quota pool returns PRECONDITION_FAILED, proving
    //   that quota IS actively enforced.
    //
    // For the follower (WithFollower=true):
    //   the zero-quota pool must also return SUCCESS, because followers skip the
    //   quota machinery entirely (SchedulableReadFactory is never set on followers).
    Y_UNIT_TEST_TWIN(ShouldReadWithSchedulerPoolId, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
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
        helper.CreatePool(zeroPoolId, 0);
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
            UNIT_ASSERT_VALUES_EQUAL_C(zeroResult->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED,
                "Leader must return PRECONDITION_FAILED for a zero-quota pool, proving quota is enforced");
        }
    }

    // A range read that produces multiple continuation chunks must deliver all rows
    // correctly when PoolId is set.
    // On the leader each chunk consumes quota and immediately returns it.
    // On the follower quota is never consulted.
    Y_UNIT_TEST_TWIN(ShouldContinuationReadWithSchedulerPoolId, WithFollower) {
        constexpr ui32 kRows = 5;
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
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

    Y_UNIT_TEST(ShouldNumberFirstResultWhenPoolQuotaBecomesZeroBeforeAdmission) {
        TSchedulerTestHelper helper(500u, /*withFollower=*/false, /*numShards=*/1, /*memoryEnabled=*/true);
        helper.Upsert(1, 100);
        auto& runtime = *helper.Server->GetRuntime();
        const auto shard = ResolveTablet(runtime, helper.TabletId());
        constexpr ui64 readId = 42;
        auto request = helper.MakeRangeReadRequest(readId, 1, 1);

        // Deliver the read synchronously: the shard has created the iterator, but its
        // low-priority admission transaction won't run until we dispatch again.
        runtime.Send(shard, helper.Sender, request.release(), 0, /*viaActorSystem=*/false);
        auto scheduler = runtime.GetAppData().KqpComputeScheduler;
        auto config = scheduler->GetStaticTree()->Clone();
        auto& pool = config->GetOrCreateDatabase(TEST_DATABASE_ID).GetOrCreatePool(TEST_POOL_ID);
        pool.ReadLimit = TDuration::Zero();
        pool.CpuLimit = 0;
        scheduler->UpdateConfiguration(std::move(config));

        auto error = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(helper.Sender, TDuration::Seconds(5));
        UNIT_ASSERT(error);
        const auto& record = error->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(record.GetReadId(), readId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSeqNo(), 1);
    }

    Y_UNIT_TEST(ShouldFailLiveReadWhenPoolQuotaBecomesZero) {
        TSchedulerTestHelper helper(500u, /*withFollower=*/false, /*numShards=*/1, /*memoryEnabled=*/true);
        helper.UpsertMany(1, 3);
        auto request = helper.MakeRangeReadRequest(1, 1, 3);
        request->Record.SetMaxRows(1); // No continuation until we acknowledge the first row.
        auto first = helper.SendRead(request.release());
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL(first->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(first->GetRowsCount(), 1);
        UNIT_ASSERT(!first->Record.GetFinished());

        helper.ExecPoolDDL(TStringBuilder() << "ALTER RESOURCE POOL `" << TEST_POOL_ID
            << "` SET (TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 0);");
        helper.WaitForPoolQuota(TEST_POOL_ID, 0);
        helper.SendAck(first->Record, 10, 50_MB);
        auto error = helper.WaitResult(TDuration::Seconds(5));
        UNIT_ASSERT(error);
        UNIT_ASSERT_VALUES_EQUAL(error->Record.GetReadId(), first->Record.GetReadId());
        UNIT_ASSERT_VALUES_EQUAL(error->Record.GetSeqNo(), first->Record.GetSeqNo() + 1);
        UNIT_ASSERT_VALUES_EQUAL(error->Record.GetStatus().GetCode(), Ydb::StatusIds::PRECONDITION_FAILED);
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

    // When the datashard receives a read request with a pool ID that is not registered
    // in ComputeScheduler, the read must succeed without quota enforcement.
    //
    // For the leader: factory returns nullptr for the unknown pool (no read query is registered), so quota check is bypassed.
    // For the follower: quota is never applied regardless of pool registration.
    Y_UNIT_TEST_TWIN(ShouldReadWithUnknownPoolId, WithFollower) {
        TSchedulerTestHelper helper(/*readLimitMs=*/std::nullopt,
                                    /*withFollower=*/WithFollower);
        helper.Upsert(1, 100);

        const TString unknownPoolId = "unknown_pool";
        auto request = helper.MakeReadRequest(1, unknownPoolId);
        AddKeyQuery(*request, {1, 1, 1});

        auto result = helper.SendRead(request.release());
        UNIT_ASSERT_C(result, "Expected read to succeed with unknown pool ID");
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), 1);
        auto cells = result->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(cells[3].AsValue<ui32>(), 100u);
    }

} // Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler)

Y_UNIT_TEST_SUITE(DataShardReadQuotaRegression) {
    Y_UNIT_TEST(QuotaWaitDoesNotBlockOtherPools) {
        TSchedulerTestHelper helper;
        helper.Upsert(1, 100);
        helper.CreatePool("other_pool", 1000);
        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});
        auto unpooled = helper.MakeReadRequest(2, "");
        AddKeyQuery(*unpooled, {1, 1, 1});
        auto other = helper.MakeReadRequest(3, "other_pool");
        AddKeyQuery(*other, {1, 1, 1});

        auto result = helper.ThrottleAndResume(std::move(request), [&] {
            helper.Upsert(2, 200);
            auto unpooledResult = helper.SendRead(unpooled.release());
            UNIT_ASSERT_VALUES_EQUAL(unpooledResult->Record.GetReadId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(unpooledResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            auto otherResult = helper.SendRead(other.release());
            UNIT_ASSERT_VALUES_EQUAL(otherResult->Record.GetReadId(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(otherResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        });
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetReadId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(QuotaWaitDoesNotBlockDropTable) {
        TSchedulerTestHelper helper;
        auto request = helper.MakeReadRequest(1);
        AddKeyQuery(*request, {1, 1, 1});
        helper.WithThrottledRead(std::move(request), [&] {
            WaitTxNotification(helper.Server, AsyncDropTable(helper.Server, helper.Sender, "/Root", "table-1"));
        });
    }

    Y_UNIT_TEST_TWIN(FirstThrottleAcquiresKeyLock, MissingKey) {
        TSchedulerTestHelper helper;
        if (!MissingKey) {
            helper.Upsert(1, 100);
        }
        auto request = helper.MakeReadRequest(1);
        request->Record.SetLockTxId(12345);
        request->Record.SetLockNodeId(helper.Server->GetRuntime()->GetNodeId(0));
        AddKeyQuery(*request, {1, 1, 1});
        auto snapshot = request->Record.GetSnapshot();
        auto result = helper.ThrottleAndResume(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsCount(), MissingKey ? 0u : 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.TxLocksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.BrokenTxLocksSize(), 0);

        helper.Upsert(1, 200);
        auto next = helper.MakeReadRequest(2);
        *next->Record.MutableSnapshot() = snapshot;
        next->Record.SetLockTxId(12345);
        next->Record.SetLockNodeId(helper.Server->GetRuntime()->GetNodeId(0));
        AddKeyQuery(*next, {1, 1, 1});
        auto broken = helper.SendRead(next.release());
        UNIT_ASSERT_VALUES_EQUAL(broken->Record.BrokenTxLocksSize(), 1);
    }

    Y_UNIT_TEST(FirstThrottleAcquiresRangeLock) {
        TSchedulerTestHelper helper;
        helper.UpsertMany(1, 2);
        auto request = helper.MakeRangeReadRequest(1, 1, 10, 1);
        request->Record.SetLockTxId(12345);
        request->Record.SetLockNodeId(helper.Server->GetRuntime()->GetNodeId(0));
        request->Record.SetMaxRows(1);
        auto result = helper.ThrottleAndResume(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.TxLocksSize(), 1);
        helper.Upsert(5, 500);
        helper.SendAck(result->Record, 10, 50_MB);
        auto next = helper.WaitResult();
        UNIT_ASSERT_VALUES_EQUAL(next->Record.BrokenTxLocksSize(), 1);
    }

    Y_UNIT_TEST(FirstThrottleUpgradesHeadToRepeatableRead) {
        TSchedulerTestHelper helper;
        helper.UpsertMany(1, 2);
        auto request = helper.MakeRangeReadRequest(1, 1, 2, 1);
        request->Record.ClearSnapshot();
        request->Record.SetMaxRows(1);
        auto first = helper.ThrottleAndResume(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(first->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_C(first->Record.HasSnapshot(), "Multi-part HEAD read must publish its fixed snapshot after throttling");
        helper.Upsert(2, 999);
        helper.SendAck(first->Record, 10, 50_MB);
        auto second = helper.WaitResult();
        UNIT_ASSERT_VALUES_EQUAL(second->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(second->GetCells(0)[3].AsValue<ui32>(), 200u);
        UNIT_ASSERT_VALUES_EQUAL(second->Record.GetSnapshot().GetStep(), first->Record.GetSnapshot().GetStep());
        UNIT_ASSERT_VALUES_EQUAL(second->Record.GetSnapshot().GetTxId(), first->Record.GetSnapshot().GetTxId());
    }

    Y_UNIT_TEST_TWIN(InvalidFormatReturnsBadRequest, WithLock) {
        TSchedulerTestHelper helper;
        helper.Upsert(1, 100);
        auto request = helper.MakeReadRequest(1);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_UNSPECIFIED);
        if (WithLock) {
            request->Record.SetLockTxId(12345);
            request->Record.SetLockNodeId(helper.Server->GetRuntime()->GetNodeId(0));
        }
        AddKeyQuery(*request, {1, 1, 1});
        auto result = helper.SendRead(request.release());
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(helper.ReadQuery()->CpuUsage.load(), 0u);
    }

    Y_UNIT_TEST(AdmissionPrecedesColumnValidation) {
        TSchedulerTestHelper helper;
        auto request = helper.MakeReadRequest(1);
        request->Record.AddColumns(Max<ui32>());
        AddKeyQuery(*request, {1, 1, 1});
        auto result = helper.ThrottleAndResume(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(SamplingKeepsCheckpointAcrossInitialThrottle) {
        TSchedulerTestHelper helper;
        helper.UpsertMany(1, 2);
        auto request = helper.MakeRangeReadRequest(1, 1, 2, 1);
        request->Record.SetMaxRows(1);
        auto* sampling = request->Record.MutableSampling();
        sampling->SetRate(1.0);
        sampling->SetSeed(42);
        auto first = helper.ThrottleAndResume(std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(first->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        NKikimrTxDataShard::TReadContinuationToken token;
        UNIT_ASSERT(token.ParseFromString(first->Record.GetContinuationToken()));
        UNIT_ASSERT(token.HasSampling());
        helper.SendAck(first->Record, 10, 50_MB);
        auto second = helper.WaitResult();
        UNIT_ASSERT_VALUES_EQUAL(second->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(first->GetRowsCount() + second->GetRowsCount(), 2u);
    }
}

} // namespace NKikimr
