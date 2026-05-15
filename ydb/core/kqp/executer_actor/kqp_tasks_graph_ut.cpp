/*
 * Test framework for TKqpTasksGraph::BuildAllTasks().
 *
 * Each test uses Y_UNIT_TEST_F with TKqpTasksGraphBuildFixture<N> (N = user-pool
 * thread count) and calls Execute() / BuildTasks() directly on the fixture.
 *
 * Initialization sequence inside TBuildTasksActor mirrors the real executer:
 *
 *  1. Compile the KQL query through the real KQP compile service
 *     → TPreparedQueryHolder (physical plan + TableConstInfoById).
 *
 *  2. Construct TKqpTasksGraph inside an actor context (required: the
 *     constructor calls LOG macros that dereference TlsActivationContext).
 *     FillStages() populates stageInfo.Meta for every stage.
 *
 *  3. Run TKqpTableResolver (real actor) against the running scheme cache
 *     → stageInfo.Meta.ShardKey: all shard IDs + key ranges for the table.
 *     No data needed — these are schema-level boundaries stored in SchemeShard.
 *
 *  4. Call TPartitionPruner::Prune() for each stage, mirroring
 *     kqp_executer_impl.h::HandleResolve():
 *       • kReadRangesSource  → Prune(source, stageInfo)
 *       • IsScan || IsOlap() → Prune(op, stageInfo) per TableOp
 *     Result: PrunedPartitions holds only shards overlapping the query range.
 *     (For UNIFORM_PARTITIONS=4, Uint64: boundaries ≈ UINT64_MAX/4 each.
 *      Key 42 → shard 0; SELECT * → all 4 shards.)
 *
 *  5. Build ShardToNode mapping.
 *     cfg.ShardToNode given → use verbatim.
 *     Otherwise: distribute shard IDs round-robin over cfg.NodeCount nodes
 *     (IDs 1..NodeCount).  MinPartitionsCount is auto-set to UNIFORM_PARTITIONS
 *     at table-creation time, so empty shards are never auto-merged.
 *
 *  6. TasksGraph.ResolveShards() → populates ShardIdToNodeId / ShardsOnNode.
 *
 *  7. BuildAllTasks() → task counts collected into TTaskDistribution.
 */

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/executer_actor/kqp_partition_helper.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/executer_actor/kqp_table_resolver.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYql::NDq;

struct TTaskDistribution {
    THashMap<TStageId, ui32> TasksPerStage; // (txIdx, stageIdx) → task count

    ui32 Count(ui32 txIdx = 0, ui32 stageIdx = 0) const {
        auto it = TasksPerStage.find(TStageId(txIdx, stageIdx));
        return it != TasksPerStage.end() ? it->second : 0;
    }

    ui32 Total() const {
        ui32 total = 0;
        for (const auto& [_, cnt] : TasksPerStage) {
            total += cnt;
        }
        return total;
    }
};

struct TBuildConfig {
    // Number of simulated cluster nodes.  Shards are distributed round-robin
    // across node IDs 1..NodeCount.  Ignored when ShardToNode is non-empty.
    ui32 NodeCount = 1;

    // Available memory per node (bytes), stored in TKqpNodeResources for
    // TKqpPlanner.  Does NOT affect BuildAllTasks() task counts.
    ui64 MemoryBytesPerNode = 8ULL << 30;

    // Explicit shard→node override.  When provided NodeCount is ignored.
    // Shard IDs are only known after table resolution, so prefer NodeCount
    // for simple scenarios.
    TGraphMeta::TShardToNodeMap ShardToNode;

    // Activates ScanExecuter mode (IsScan=true in graph meta).
    bool IsScan = false;
};

namespace {

constexpr ui32 EvBuildTasksDone = EventSpaceBegin(NActors::TEvents::ES_USERSPACE) + 512;

struct TEvBuildTasksDone : NActors::TEventLocal<TEvBuildTasksDone, EvBuildTasksDone> {
    TString ErrorMessage;
    TTaskDistribution Result;
};

class TBuildTasksActor : public NActors::TActorBootstrapped<TBuildTasksActor> {
public:
    TBuildTasksActor(
        NActors::TActorId owner,
        std::shared_ptr<const TPreparedQueryHolder> plan,
        TBuildConfig cfg)
        : Owner(owner)
        , Plan(std::move(plan))
        , Config(std::move(cfg))
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TxAlloc = std::make_shared<TTxAllocatorState>(
            NKikimr::AppData()->FunctionRegistry,
            CreateDefaultTimeProvider(),
            CreateDefaultRandomProvider());

        for (const auto& tx : Plan->GetTransactions()) {
            auto params = std::make_shared<TQueryData>(TxAlloc);
            Transactions.emplace_back(tx, std::move(params));
        }

        // Empty TAggregationConfig: GetUsableThreads() reads the real user-pool
        // thread count, which is pinned by TKqpTasksGraphBuildFixture<N>.
        Graph = std::make_unique<TKqpTasksGraph>(
            "/Root", Transactions, TxAlloc,
            NKikimrConfig::TTableServiceConfig::TAggregationConfig{},
            MakeIntrusive<TKqpRequestCounters>(),
            NActors::TActorId{}, nullptr);

        Graph->GetMeta().IsScan             = Config.IsScan;
        Graph->GetMeta().AllowOlapDataQuery = true;
        Graph->GetMeta().UserRequestContext = MakeIntrusive<TUserRequestContext>(
            "test-build-tasks", "/Root", "test-session");

        ctx.Register(CreateKqpTableResolver(ctx.SelfID, 1, nullptr, *Graph, false));
        Become(&TBuildTasksActor::WaitingForResolve);
    }

    void HandleResolveStatus(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev, const NActors::TActorContext& ctx) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyError(ctx, TStringBuilder() << "Table resolver: " << ev->Get()->Issues.ToString());
        }

        // Mirrors kqp_executer_impl.h::HandleResolve() partition pruning.
        TPartitionPruner pruner(TxAlloc->HolderFactory, TxAlloc->TypeEnv);
        TSet<ui64> allShardIds;

        for (auto& [_, stageInfo] : Graph->GetStagesInfo()) {
            if (stageInfo.Meta.IsSysView()) {
                continue;
            }

            const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
            if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                bool isFullScan = false;
                stageInfo.Meta.PrunedPartitions.emplace_back(pruner.Prune(stage.GetSources(0).GetReadRangesSource(), stageInfo, isFullScan));
            } else if (Graph->GetMeta().IsScan || stageInfo.Meta.IsOlap()) {
                for (const auto& op : stage.GetTableOps()) {
                    bool isFullScan = false;
                    stageInfo.Meta.PrunedPartitions.emplace_back(pruner.Prune(op, stageInfo, isFullScan));
                }
            }

            for (const auto& partMap : stageInfo.Meta.PrunedPartitions) {
                for (const auto& [shardId, _] : partMap) {
                    allShardIds.insert(shardId);
                }
            }
        }

        TGraphMeta::TShardToNodeMap shardToNode;
        if (!Config.ShardToNode.empty()) {
            shardToNode = Config.ShardToNode;
        } else {
            ui32 idx = 0;
            for (ui64 id : allShardIds) {
                shardToNode[id] = (idx++ % Config.NodeCount) + 1;
            }
        }

        Graph->ResolveShards(TGraphMeta::TShardToNodeMap{shardToNode});

        // One snapshot entry per distinct node.  Memory is for TKqpPlanner;
        // GetUsableThreads() reads the actor-system pool, not the snapshot.
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        {
            THashSet<ui64> seen;
            for (const auto& [_, nodeId] : shardToNode) {
                if (!seen.insert(nodeId).second) {
                    continue;
                }
                auto& res = snapshot.emplace_back();
                res.SetNodeId(nodeId);
                if (Config.MemoryBytesPerNode > 0) {
                    auto* mem = res.AddMemory();
                    mem->SetPool(0);
                    mem->SetAvailable(Config.MemoryBytesPerNode);
                }
            }
        }

        Graph->BuildAllTasks({}, snapshot, nullptr);
        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
            "Tasks graph after BuildAllTasks:\n" << Graph->DumpToString());

        auto reply = MakeHolder<TEvBuildTasksDone>();
        for (const auto& [stageId, stageInfo] : Graph->GetStagesInfo()) {
            reply->Result.TasksPerStage[stageId] = static_cast<ui32>(stageInfo.Tasks.size());
        }
        ctx.Send(Owner, reply.Release());
        Die(ctx);
    }

    STFUNC(WaitingForResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolveStatus);
        default: break;
        }
    }

private:
    void ReplyError(const NActors::TActorContext& ctx, TString msg) {
        auto reply = MakeHolder<TEvBuildTasksDone>();
        reply->ErrorMessage = std::move(msg);
        ctx.Send(Owner, reply.Release());
        Die(ctx);
    }

    NActors::TActorId Owner;
    std::shared_ptr<const TPreparedQueryHolder> Plan;
    TBuildConfig Config;

    TTxAllocatorState::TPtr TxAlloc;
    TVector<IKqpGateway::TPhysicalTxData> Transactions; // must outlive Graph
    std::unique_ptr<TKqpTasksGraph> Graph;
};

} // namespace

// N = 0: GetUsableThreads() reads the real actor-system user-pool size
//        (falls back to NSystemInfo::NumberOfCpus() if pool is unset).
// N > 0: actor-system user pool is pinned to N threads so that
//        TStagePredictor::GetUsableThreads() returns exactly N.
//        Path: GetUsableThreads()
//          → ActorSystem::GetPoolThreadsCount(AppData()->UserPoolId)
//          → Executors[UserPoolId]->GetDefaultThreadCount()
//          → TBasicExecutorPool::DefaultFullThreadCount
//          → ActorSystemConfig.Executor[UserExecutor].Threads
//
// Always uses UseRealThreads=true: with UseRealThreads=false the simulated
// single-threaded dispatcher conflicts with the real pool threads created by
// a custom ActorSystemConfig, causing SysViews roster init to time out.
//
// Usage:
//   Y_UNIT_TEST_F(MyTest, TKqpTasksGraphBuildFixture<8>) { ... }
//   Y_UNIT_TEST_F(MyTest, TKqpTasksGraphBuildFixture<>)  { ... }
template<ui32 N = 0>
class TKqpTasksGraphBuildFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext& /* unused */) override {
        TKikimrSettings settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetUseRealThreads(true);

        if constexpr (N > 0) {
            using TExecutor = NKikimrConfig::TActorSystemConfig::TExecutor;
            auto& asCfg = *settings.AppConfig.MutableActorSystemConfig();
            asCfg.ClearExecutor();

            const auto addPool = [&](const TString& name, ui32 threads, TExecutor::EType type) {
                auto& e = *asCfg.AddExecutor();
                e.SetType(type);
                e.SetName(name);
                e.SetThreads(threads);
                if (type == TExecutor::BASIC) {
                    e.SetSpinThreshold(1);
                }
            };

            // Pools: 0=System, 1=User, 2=Batch, 3=IO
            addPool("System", 1, TExecutor::BASIC);  asCfg.SetSysExecutor(0);
            addPool("User",   N, TExecutor::BASIC);  asCfg.SetUserExecutor(1);
            addPool("Batch",  1, TExecutor::BASIC);  asCfg.SetBatchExecutor(2);
            addPool("IO",     1, TExecutor::IO);     asCfg.SetIoExecutor(3);
        }

        Kikimr.emplace(settings);
        Runtime = Kikimr->GetTestServer().GetRuntime();
        Runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    }

    // Execute DDL / DML (CREATE TABLE, INSERT, …).
    void Execute(const TString& sql) {
        Kikimr->RunCall([&] {
            auto session = Kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
            auto res  = session.ExecuteSchemeQuery(sql).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            return true;
        });
    }

    // Compile sql, run the full TKqpTasksGraph init sequence, return task counts.
    TTaskDistribution BuildTasks(const TString& sql, TBuildConfig cfg = {}) {
        auto plan = CompileQuery(sql);
        UNIT_ASSERT_C(plan, "Failed to compile: " << sql);

        DumpExplain(*plan);

        auto edge = Runtime->AllocateEdgeActor();
        Runtime->Register(new TBuildTasksActor(edge, std::move(plan), std::move(cfg)));

        auto ev = Runtime->GrabEdgeEvent<TEvBuildTasksDone>(edge, TDuration::Seconds(30));
        UNIT_ASSERT_C(ev, "TBuildTasksActor timed out");
        UNIT_ASSERT_C(ev->Get()->ErrorMessage.empty(), ev->Get()->ErrorMessage);

        return ev->Get()->Result;
    }

private:
    static void DumpExplain(const TPreparedQueryHolder& plan) {
        const auto& physQuery = plan.GetPhysicalQuery();
        Cerr << "===== EXPLAIN plan (QueryPlan) =====" << Endl;
        Cerr << physQuery.GetQueryPlan() << Endl;
        Cerr << "====================================" << Endl;
    }

    std::shared_ptr<const TPreparedQueryHolder> CompileQuery(const TString& sql) {
        auto result = CompileQueryFull(sql);
        return result ? result->PreparedQuery : nullptr;
    }

    // Sends TEvCompileRequest directly to the compile service and returns the
    // full TKqpCompileResult (including QueryAst).
    // AddObserver cannot be used here: with UseRealThreads=true, actor-to-actor
    // sends bypass the test-runtime observer hook.
    TKqpCompileResult::TConstPtr CompileQueryFull(const TString& sql) {
        const ui32 nodeIdx = 0;
        auto edgeActor      = Runtime->AllocateEdgeActor(nodeIdx);
        auto compileService = MakeKqpCompileServiceID(Runtime->GetNodeId(nodeIdx));

        TKqpQuerySettings querySettings(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        querySettings.DocumentApiRestricted = false;

        TKqpQueryId queryId(
            TString(DefaultKikimrPublicClusterName), "/Root", /*databaseId*/ "",
            sql, querySettings, /*paramTypes*/ nullptr, TGUCSettings{});

        // Compile service unconditionally dereferences UserToken.
        TIntrusiveConstPtr<NACLib::TUserToken> anonToken =
            new NACLib::TUserToken(TVector<NACLib::TSID>{});

        Runtime->Send(new IEventHandle(compileService, edgeActor,
            new TEvKqp::TEvCompileRequest(
                anonToken, /*clientAddress*/ "", /*uid*/ Nothing(),
                TMaybe<TKqpQueryId>(std::move(queryId)),
                /*keepInCache*/          true,
                /*isQueryActionPrepare*/ false,
                /*perStatementResult*/   false,
                /*deadline*/             TInstant::Max(),
                /*dbCounters*/           nullptr,
                // GUCSettings must be non-null: compile actor calls ->ExportToJson().
                /*gUCSettings*/          std::make_shared<TGUCSettings>(),
                /*applicationName*/      Nothing(),
                /*intrestedInResult*/    std::make_shared<std::atomic<bool>>(true),
                MakeIntrusive<TUserRequestContext>("test-compile", "/Root", "test-session-compile"))));

        auto ev = Runtime->GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edgeActor, TDuration::Seconds(30));
        UNIT_ASSERT_C(ev, "Compile request timed out");
        UNIT_ASSERT_C(ev->Get()->CompileResult, "CompileResult is null");
        UNIT_ASSERT_VALUES_EQUAL_C(
            ev->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
            ev->Get()->CompileResult->Issues.ToString());
        return ev->Get()->CompileResult;
    }

private:
    std::optional<TKikimrRunner> Kikimr;
    TTestActorRuntime* Runtime = nullptr;
};

// ============================================================================
// Tests
// ============================================================================

Y_UNIT_TEST_SUITE(TKqpTasksGraphBuild) {

    Y_UNIT_TEST_F(TpchQuery01, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            $to_decimal = ($x) -> { return cast($x as decimal(12,2)); };

            SELECT
                l_returnflag,
                l_linestatus,
                Sum(l_quantity) AS sum_qty,
                Sum(l_extendedprice) AS sum_base_price,
                Sum(l_extendedprice * ($to_decimal(1) - l_discount)) AS sum_disc_price,
                Sum(l_extendedprice * ($to_decimal(1) - l_discount) * ($to_decimal(1) + l_tax)) AS sum_charge,
                Avg(l_quantity) AS avg_qty,
                Avg(l_extendedprice) AS avg_price,
                Avg(l_discount) AS avg_disc,
                Count(*) AS count_order
            FROM
                `/Root/lineitem` as lineitem
            WHERE
                l_shipdate <= Date('1998-12-01') - Interval('P90D')
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus
            ;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 608);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 456);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 1);
    }

    Y_UNIT_TEST_F(TpchQuery02, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM
                `/Root/part` as part,
                `/Root/supplier` as supplier,
                `/Root/partsupp` as partsupp,
                `/Root/nation` as nation,
                `/Root/region` as region
            WHERE
                p_partkey == ps_partkey
                AND s_suppkey == ps_suppkey
                AND p_size == 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey == n_nationkey
                AND n_regionkey == r_regionkey
                AND r_name == 'EUROPE'
                AND ps_supplycost == (
                    SELECT
                        min(ps_supplycost)
                    FROM
                        `/Root/partsupp` as partsupp,
                        `/Root/supplier` as supplier,
                        `/Root/nation` as nation,
                        `/Root/region` as region
                    WHERE
                        p_partkey == ps_partkey
                        AND s_suppkey == ps_suppkey
                        AND s_nationkey == n_nationkey
                        AND n_regionkey == r_regionkey
                        AND r_name == 'EUROPE'
                )
            ORDER BY
                s_acctbal DESC,
                n_name,
                s_name,
                p_partkey
            LIMIT 100
            ;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 18u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 76);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 6), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 7), 76);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 8), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 9), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 76);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 13), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 14), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 15), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 16), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 17), 1);
    }

    Y_UNIT_TEST_F(TpchQuery03, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            $to_decimal = ($x) -> { return cast($x as decimal(12,2)); };

            SELECT
                l_orderkey,
                Sum(l_extendedprice * ($to_decimal(1) - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                `/Root/customer` as customer,
                `/Root/orders` as orders,
                `/Root/lineitem` as lineitem
            WHERE
                c_mktsegment == 'BUILDING'
                AND c_custkey == o_custkey
                AND l_orderkey == o_orderkey
                AND o_orderdate < Date('1995-03-15')
                AND l_shipdate > Date('1995-03-15')
            GROUP BY
                l_orderkey,
                o_orderdate,
                o_shippriority
            ORDER BY
                revenue DESC,
                o_orderdate
            LIMIT 10
            ;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 380);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 380);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 6), 1);
    }

    Y_UNIT_TEST_F(TpchQuery04, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            SELECT
                o_orderpriority,
                Count(*) AS order_count
            FROM
                `/Root/orders` as orders
            WHERE
                o_orderdate >= Date('1993-07-01')
                AND o_orderdate < DateTime::MakeDate(DateTime::ShiftMonths(Date('1993-07-01'), 3))
                AND EXISTS (
                    SELECT
                        *
                    FROM
                        `/Root/lineitem` as lineitem
                    WHERE
                        l_orderkey == o_orderkey
                        AND l_commitdate < l_receiptdate
                )
            GROUP BY
                o_orderpriority
            ORDER BY
                o_orderpriority
            ;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 304);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 304);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 456);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 6), 456);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 7), 1);
    }

    Y_UNIT_TEST_F(TpchQuery05, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA AnsiImplicitCrossJoin;
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            $to_decimal = ($x) -> { return cast($x as decimal(12,2)); };

            SELECT
                nation.n_name,
                Sum(l_extendedprice * ($to_decimal(1) - l_discount)) AS revenue
            FROM
                `/Root/customer` as customer,
                `/Root/orders` as orders,
                `/Root/lineitem` as lineitem,
                `/Root/supplier` as supplier,
                `/Root/nation` as nation,
                `/Root/region` as region
            WHERE
                c_custkey == o_custkey
                AND l_orderkey == o_orderkey
                AND l_suppkey == s_suppkey
                AND c_nationkey == s_nationkey
                AND s_nationkey == n_nationkey
                AND n_regionkey == r_regionkey
                AND r_name == 'ASIA'
                AND o_orderdate >= Date('1994-01-01')
                AND o_orderdate < DateTime::MakeDate(DateTime::ShiftYears(Date('1994-01-01'), 1))
            GROUP BY
                nation.n_name
            ORDER BY
                revenue DESC
            ;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 2);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 2);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 228);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 1);
    }

    Y_UNIT_TEST_F(CustomQuery01, TKqpTasksGraphBuildFixture<8>) {
        Execute(R"(
            CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
            CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
            CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        )");

        TBuildConfig cfg;
        cfg.NodeCount = 76;

        auto dist = BuildTasks(R"(
            PRAGMA ydb.UseBlockHashJoin = 'true';
            PRAGMA ydb.HashJoinMode = 'graceandself';
            PRAGMA config.flags("OptimizerFlags", "EmitPruneKeys");

            PRAGMA ydb.OptimizerHints = '
                Rows(customer # 1.5e9 )
                Rows(lineitem #   6e10)
                Rows(nation   #    25 )
                Rows(orders   # 1.5e10)
                Rows(part     #   2e9 )
                Rows(partsupp #   8e9 )
                Rows(region   #     5 )
                Rows(supplier #   1e8 )

                Bytes(customer # 3.9e10)
                Bytes(lineitem #   1e12)
                Bytes(nation   #   2e3 )
                Bytes(orders   # 4.3e11)
                Bytes(part     # 142e9 )
                Bytes(partsupp # 524e9 )
                Bytes(region   #   1e3 )
                Bytes(supplier #   9e9 )

                Rows(orders lineitem # 6e8)
                Rows(customer orders lineitem # 6e8)
            ';

            $step1 = (
                SELECT
                    l.l_orderkey  AS l_orderkey,
                    l.l_suppkey   AS l_suppkey,
                    l.l_partkey   AS l_partkey,
                    SUM(CAST(l.l_extendedprice AS Double) * (1.0 - CAST(l.l_discount AS Double))) AS line_revenue,
                    SUM(CAST(l.l_quantity AS Double)) AS line_qty
                FROM `lineitem` AS l
                WHERE l.l_shipdate >= Date("1993-01-01")
                AND l.l_shipdate <  Date("1997-01-01")
                GROUP BY l.l_orderkey, l.l_suppkey, l.l_partkey
            );

            $step2 = (
                SELECT
                    s1.l_suppkey  AS l_suppkey,
                    s1.l_partkey  AS l_partkey,
                    o.o_custkey   AS o_custkey,
                    SUM(s1.line_revenue) AS supp_cust_revenue,
                    COUNT(DISTINCT s1.l_orderkey) AS supp_cust_orders
                FROM $step1 AS s1
                INNER JOIN `orders` AS o
                    ON s1.l_orderkey = o.o_orderkey
                GROUP BY s1.l_suppkey, s1.l_partkey, o.o_custkey
            );

            $step3 = (
                SELECT
                    s2.l_suppkey     AS l_suppkey,
                    c.c_nationkey    AS cust_nationkey,
                    SUM(s2.supp_cust_revenue)      AS supp_nation_revenue,
                    SUM(CAST(s2.supp_cust_orders AS Int64)) AS supp_nation_orders,
                    COUNT(DISTINCT c.c_custkey)    AS unique_customers
                FROM $step2 AS s2
                INNER JOIN `customer` AS c
                    ON s2.o_custkey = c.c_custkey
                GROUP BY s2.l_suppkey, c.c_nationkey
            );

            $step4 = (
                SELECT
                    s.s_nationkey     AS supp_nationkey,
                    s3.cust_nationkey AS cust_nationkey,
                    SUM(s3.supp_nation_revenue)        AS bilateral_revenue,
                    SUM(CAST(s3.unique_customers AS Int64)) AS bilateral_customers,
                    COUNT(DISTINCT s.s_suppkey)        AS bilateral_suppliers
                FROM $step3 AS s3
                INNER JOIN `supplier` AS s
                    ON s3.l_suppkey = s.s_suppkey
                GROUP BY s.s_nationkey, s3.cust_nationkey
            );

            $step5_supp = (
                SELECT
                    t.supp_nationkey           AS supp_nationkey,
                    SUM(t.bilateral_revenue)   AS total_export,
                    SUM(CAST(t.bilateral_suppliers AS Int64)) AS total_suppliers
                FROM $step4 AS t
                GROUP BY t.supp_nationkey
            );

            $step5_cust = (
                SELECT
                    t.cust_nationkey            AS cust_nationkey,
                    SUM(t.bilateral_revenue)    AS total_import,
                    SUM(CAST(t.bilateral_customers AS Int64)) AS total_customers
                FROM $step4 AS t
                GROUP BY t.cust_nationkey
            );

            SELECT
                sn.n_name                  AS supplier_nation,
                sr.r_name                  AS supplier_region,
                cn.n_name                  AS customer_nation,
                cr.r_name                  AS customer_region,
                s4.bilateral_revenue       AS bilateral_revenue,
                s4.bilateral_customers     AS bilateral_customers,
                s4.bilateral_suppliers     AS bilateral_suppliers,
                s4.bilateral_revenue   / s5s.total_export  AS share_of_export,
                s4.bilateral_revenue   / s5c.total_import  AS share_of_import,
                s5s.total_export - s5c.total_import        AS trade_balance
            FROM $step4 AS s4
            INNER JOIN $step5_supp AS s5s
                ON s4.supp_nationkey = s5s.supp_nationkey
            INNER JOIN $step5_cust AS s5c
                ON s4.cust_nationkey = s5c.cust_nationkey
            INNER JOIN `nation` AS sn
                ON s4.supp_nationkey = sn.n_nationkey
            INNER JOIN `region` AS sr
                ON sn.n_regionkey = sr.r_regionkey
            INNER JOIN `nation` AS cn
                ON s4.cust_nationkey = cn.n_nationkey
            INNER JOIN `region` AS cr
                ON cn.n_regionkey = cr.r_regionkey
            ORDER BY s4.bilateral_revenue DESC
            LIMIT 100;
        )", cfg);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 25u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 76);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 76);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 13), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 14), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 15), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 16), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 17), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 18), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 19), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 20), 304);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 21), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 22), 152);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 23), 456);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 24), 1);
    }

} // Y_UNIT_TEST_SUITE(TKqpTasksGraphBuild)

} // namespace NKikimr::NKqp
