#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_read.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/tree/common.h>

#include <ydb/core/testlib/tablet_helpers.h>

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
                                  ui32 numShards = 1)
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

        Server = new TServer(serverSettings);
        auto& runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE_SCHEDULER, NLog::PRI_TRACE);

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

    void UpdatePoolQuota(double totalCpuLimitPercentPerNode, const TString& poolId = TEST_POOL_ID) const {
        using namespace NKqp::NScheduler;
        auto& runtime = *Server->GetRuntime();
        NResourcePool::TPoolSettings params;
        params.TotalCpuLimitPercentPerNode = totalCpuLimitPercentPerNode;
        auto ev = std::make_unique<TEvAddPool>(TEST_DATABASE_ID, poolId, params);
        runtime.Send(NKqp::MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), Sender, ev.release());
        runtime.SimulateSleep(TDuration::Zero());
    }

    void AddZeroQuotaPool(const TString& poolId) const {
        UpdatePoolQuota(0., poolId);
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

    // ----------------------------------------------------------------------------
    // Regression test for the ext-blob precharge / read-quota livelock.
    //
    // Bug shape: reading keys whose values are external blobs runs the ext-blob
    // precharge (PrechargeKeysAfter, gated by ReadIteratorKeysExtBlobsPrecharge).
    // It records missing blob references for a whole iteration-quota worth of keys
    // and the executor loads them into cache. The TSchedulableRead quota is checked
    // at the START of every Execute() attempt, INCLUDING page-fault restarts
    // (datashard__read_iterator.cpp ~2089). Under a tight pool ReadLimit the read
    // gets throttled between "blobs loaded" and "rows read", the loaded pages are
    // dropped, and on resume the same blobs are fetched again -> no forward progress.
    //
    // This test puts the read on the SchedulableRead path (PoolId + tiny ReadLimit)
    // with a cold/empty shared cache (forces every access to fetch from BlobStorage)
    // and paces continuations with explicit acks. It is written as a SINGLE-RUN
    // CALIBRATION probe: it never hangs (hard iteration cap), and it always prints a
    // diagnostic block with the numbers needed to lock the green/red thresholds.
    //
    // Expected on buggy code: does NOT finish within the cap and/or BlobsRequested
    // grows far beyond the number of keys (same blobs re-fetched repeatedly).
    // Expected after the fix: finishes, returns every row, BlobsRequested ~= #keys.
    //
    // !!! CALIBRATE the constants marked [CAL] from the first run's diagnostic block.
    Y_UNIT_TEST(ExtBlobPrechargeProgressUnderTightQuota) {
        using namespace NKqp;
        using namespace NKqp::NScheduler;

        // ---- [CAL] knobs --------------------------------------------------------
        constexpr ui32 kRows           = 20;          // number of ext-blob keys read
        constexpr ui64 kBlobSize       = 1u << 20;    // 1 MiB value -> external blob
        constexpr ui64 kReadLimitMs    = 5;           // tight pool read quota (ms/s)
        constexpr ui64 kSharedCacheB   = 0;           // 0 = no shared cache (cold)
        constexpr ui32 kMaxRowsInResult= 1;           // force a continuation per row
        constexpr ui32 kMaxIterations  = 500;         // hard cap so we never hang
        constexpr ui64 kAckRows        = 1000;
        constexpr ui64 kAckBytes       = 50_MB;
        // After calibration set this to e.g. 2*kRows; buggy code blows past it.
        constexpr ui64 kBlobBudget     = 4 * kRows;   // [CAL] max blobs we tolerate
        // -------------------------------------------------------------------------

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableResourcePoolsScheduler(true);
        serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(kSharedCacheB);
        serverSettings.AddStoragePool("ssd");
        serverSettings.AddStoragePool("ext");
        {
            TServerSettings::TControls controls;
            controls.MutableDataShardControls()->SetReadIteratorKeysExtBlobsPrecharge(1);
            serverSettings.SetControls(controls);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE_SCHEDULER, NLog::PRI_TRACE);

        // Register a database + pool with a tight read limit BEFORE the shard reads.
        {
            auto ev = std::make_unique<TEvAddDatabase>(TEST_DATABASE_ID);
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), sender, ev.release());
        }
        {
            auto ev = std::make_unique<TEvAddPool>(TEST_DATABASE_ID, TEST_POOL_ID);
            ev->Params.TotalCpuLimitPercentPerNode = kReadLimitMs / 10.;
            runtime.Send(MakeKqpSchedulerServiceId(runtime.GetFirstNodeId()), sender, ev.release());
        }

        InitRoot(server, sender);

        // External-blob table: a String value above ExternalThreshold is stored as
        // an external blob, so reads must page-fault into BlobStorage via EvGet.
        TVector<TShardedTableOptions::TColumn> columns = {
            {"key", "Uint32", true, false},
            {"value", "String", false, false},
        };
        TShardedTableOptions::TFamily fam{
            .Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd",
            .DataPoolKind = "ssd", .ExternalPoolKind = "ext",
            .DataThreshold = 100u, .ExternalThreshold = 512u * 1024,
        };
        auto opts = TShardedTableOptions()
            .Columns(columns)
            .Families({fam})
            .ExecutorCacheSize(1);
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards.at(0);

        // Fill N rows with large values, then compact so values land as external
        // blobs, then reboot to drop the tablet's in-memory state.
        for (ui32 i = 1; i <= kRows; ++i) {
            ExecSQL(server, sender, TStringBuilder()
                << "UPSERT INTO `/Root/table-1` (key, value) VALUES ("
                << i << ", \"" << TString(kBlobSize, 'L') << "\");");
        }
        WaitTableStats(runtime, shard, [](const NKikimrTableStats::TTableStats& s) {
            return s.GetRowCount() == kRows;
        });
        CompactTable(runtime, shard, tableId, false);
        WaitTableStats(runtime, shard, [](const NKikimrTableStats::TTableStats& s) {
            return s.GetPartCount() >= 1;
        });
        RebootTablet(runtime, shard, sender);

        // Count BlobStorage reads (after reboot, so compaction/recovery don't count).
        ui64 evGets = 0;
        ui64 blobsRequested = 0;
        ui64 readResults = 0;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvGet: {
                    ++evGets;
                    blobsRequested += ev->Get<TEvBlobStorage::TEvGet>()->QuerySize;
                    break;
                }
                case TEvDataShard::TEvReadResult::EventType:
                    ++readResults;
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto clientId = runtime.ConnectToPipe(shard, sender, 0, GetPipeConfigWithRetries());

        // Build a multi-key read on the SchedulableRead path (PoolId set).
        auto [tables, ownerId] = GetTables(server, shard);
        const auto& userTable = tables["table-1"];
        Y_UNUSED(ownerId);
        auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"}, TDuration::Hours(1));
        auto request = GetBaseReadRequest(tableId, userTable.GetDescription(), /*readId=*/1,
                                          NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        request->Record.SetDatabaseId(TEST_DATABASE_ID);
        request->Record.SetPoolId(TEST_POOL_ID);
        request->Record.SetMaxRowsInResult(kMaxRowsInResult);
        for (ui32 i = 1; i <= kRows; ++i) {
            AddKeyQuery(*request, {i});
        }

        ::NKikimr::SendReadAsync(server, shard, request.release(), sender, 0,
                                 GetPipeConfigWithRetries(), clientId);

        ui32 iterations = 0;
        ui32 rowsReceived = 0;
        bool finished = false;
        Ydb::StatusIds::StatusCode lastStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        for (; iterations < kMaxIterations; ++iterations) {
            auto result = WaitReadResult(server, TDuration::Seconds(10));
            if (!result) {
                break; // throttled with no result delivered within the window
            }
            lastStatus = result->Record.GetStatus().GetCode();
            if (lastStatus != Ydb::StatusIds::SUCCESS) {
                break;
            }
            rowsReceived += result->GetRowsCount();
            if (result->Record.GetFinished()) {
                finished = true;
                break;
            }
            auto* ack = new TEvDataShard::TEvReadAck();
            ack->Record.SetReadId(result->Record.GetReadId());
            ack->Record.SetSeqNo(result->Record.GetSeqNo());
            ack->Record.SetMaxRows(kAckRows);
            ack->Record.SetMaxBytes(kAckBytes);
            runtime.SendToPipe(shard, sender, ack, 0, GetPipeConfigWithRetries(), clientId);
        }

        runtime.SetObserverFunc(prevObserver);

        // ---- single-run calibration block (always printed) ----------------------
        Cerr << "=== ExtBlobPrechargeProgressUnderTightQuota CALIBRATION ===" << Endl
             << "  rows requested       : " << kRows << Endl
             << "  rows received        : " << rowsReceived << Endl
             << "  finished             : " << (finished ? "YES" : "NO") << Endl
             << "  last status          : " << Ydb::StatusIds::StatusCode_Name(lastStatus) << Endl
             << "  iterations (cap "      << kMaxIterations << "): " << iterations << Endl
             << "  read results          : " << readResults << Endl
             << "  EvGets                : " << evGets << Endl
             << "  blobs requested (sum) : " << blobsRequested
             << "   (#keys=" << kRows << ", budget [CAL]=" << kBlobBudget << ")" << Endl
             << "=== END CALIBRATION ===" << Endl;

        // Green after the fix. On buggy code at least one of these fails (and the
        // diagnostic block above gives the numbers to finalize kBlobBudget).
        UNIT_ASSERT_C(finished, "read did not finish within " << kMaxIterations
            << " iterations (last status " << Ydb::StatusIds::StatusCode_Name(lastStatus)
            << ") - see CALIBRATION block");
        UNIT_ASSERT_VALUES_EQUAL(rowsReceived, kRows);
        UNIT_ASSERT_LE_C(blobsRequested, kBlobBudget,
            "blobs were re-fetched far more than #keys - precharge/quota livelock");
    }

} // Y_UNIT_TEST_SUITE(DataShardReadIteratorScheduler)

} // namespace NKikimr
