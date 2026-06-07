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
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_partition_helper.h>
#include <ydb/core/kqp/executer_actor/kqp_table_resolver.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>

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
            NKikimrConfig::TTableServiceConfig::TResourceManager{},
            NKikimrConfig::TTableServiceConfig::TAggregationConfig{},
            MakeIntrusive<TKqpRequestCounters>(),
            NActors::TActorId{}, nullptr, false);

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

        // settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMaxChannelCountPerNode(100);
        // settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);

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
        Cerr << "===== EXPLAIN ast (QueryAst) =====" << Endl;
        Cerr << physQuery.GetQueryAst() << Endl;
        Cerr << "==================================" << Endl;

        NYdb::NConsoleClient::TQueryPlanPrinter queryPlanPrinter(
            NYdb::NConsoleClient::EDataFormat::PrettyTable,
            false, Cerr, 0
        );
        queryPlanPrinter.Print(physQuery.GetQueryPlan());
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

class TKqpTasksGraphTpchFixture : public TKqpTasksGraphBuildFixture<32> {
public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TKqpTasksGraphBuildFixture::SetUp(ctx);
        Execute(TString(CreateTables));
    }

    TTaskDistribution BuildTasks(const TString& query) {
        TBuildConfig cfg;
        cfg.NodeCount = 120;

        return TKqpTasksGraphBuildFixture::BuildTasks(OptimizerHints + query, cfg);
    }

private:
    static constexpr TStringBuf CreateTables = R"(
        CREATE TABLE customer ( `c_acctbal` Decimal (12, 2) NOT NULL, `c_address` Utf8 NOT NULL, `c_comment` Utf8 NOT NULL, `c_custkey` Int64 NOT NULL, `c_mktsegment` Utf8 NOT NULL, `c_name` Utf8 NOT NULL, `c_nationkey` Int32 NOT NULL, `c_phone` Utf8 NOT NULL, PRIMARY KEY (`c_custkey`) ) PARTITION BY HASH (`c_custkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        CREATE TABLE lineitem ( `l_comment` Utf8 NOT NULL, `l_commitdate` Date32 NOT NULL, `l_discount` Decimal (12, 2) NOT NULL, `l_extendedprice` Decimal (12, 2) NOT NULL, `l_linenumber` Int32 NOT NULL, `l_linestatus` Utf8 NOT NULL, `l_orderkey` Int64 NOT NULL, `l_partkey` Int64 NOT NULL, `l_quantity` Decimal (12, 2) NOT NULL, `l_receiptdate` Date32 NOT NULL, `l_returnflag` Utf8 NOT NULL, `l_shipdate` Date32 NOT NULL, `l_shipinstruct` Utf8 NOT NULL, `l_shipmode` Utf8 NOT NULL, `l_suppkey` Int64 NOT NULL, `l_tax` Decimal (12, 2) NOT NULL, PRIMARY KEY (`l_orderkey`, `l_linenumber`) ) PARTITION BY HASH (`l_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        CREATE TABLE nation ( `n_comment` Utf8 NOT NULL, `n_name` Utf8 NOT NULL, `n_nationkey` Int32 NOT NULL, `n_regionkey` Int32 NOT NULL, PRIMARY KEY (`n_nationkey`) ) PARTITION BY HASH (`n_nationkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
        CREATE TABLE orders ( `o_clerk` Utf8 NOT NULL, `o_comment` Utf8 NOT NULL, `o_custkey` Int64 NOT NULL, `o_orderdate` Date32 NOT NULL, `o_orderkey` Int64 NOT NULL, `o_orderpriority` Utf8 NOT NULL, `o_orderstatus` Utf8 NOT NULL, `o_shippriority` Int32 NOT NULL, `o_totalprice` Decimal (12, 2) NOT NULL, PRIMARY KEY (`o_orderkey`) ) PARTITION BY HASH (`o_orderkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        CREATE TABLE part ( `p_brand` Utf8 NOT NULL, `p_comment` Utf8 NOT NULL, `p_container` Utf8 NOT NULL, `p_mfgr` Utf8 NOT NULL, `p_name` Utf8 NOT NULL, `p_partkey` Int64 NOT NULL, `p_retailprice` Decimal (12, 2) NOT NULL, `p_size` Int32 NOT NULL, `p_type` Utf8 NOT NULL, PRIMARY KEY (`p_partkey`) ) PARTITION BY HASH (`p_partkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        CREATE TABLE partsupp ( `ps_availqty` Int32 NOT NULL, `ps_comment` Utf8 NOT NULL, `ps_partkey` Int64 NOT NULL, `ps_suppkey` Int64 NOT NULL, `ps_supplycost` Decimal (12, 2) NOT NULL, PRIMARY KEY (`ps_partkey`, `ps_suppkey`) ) PARTITION BY HASH (`ps_partkey`, `ps_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
        CREATE TABLE region ( `r_comment` Utf8 NOT NULL, `r_name` Utf8 NOT NULL, `r_regionkey` Int32 NOT NULL, PRIMARY KEY (`r_regionkey`) ) PARTITION BY HASH (`r_regionkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1 );
        CREATE TABLE supplier ( `s_acctbal` Decimal (12, 2) NOT NULL, `s_address` Utf8 NOT NULL, `s_comment` Utf8 NOT NULL, `s_name` Utf8 NOT NULL, `s_nationkey` Int32 NOT NULL, `s_phone` Utf8 NOT NULL, `s_suppkey` Int64 NOT NULL, PRIMARY KEY (`s_suppkey`) ) PARTITION BY HASH (`s_suppkey`) WITH ( STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 256 );
    )";

    static constexpr TStringBuf OptimizerHints = R"(
        PRAGMA ydb.OptimizerHints = '
            Rows(customer # 1.5e9 )
            Rows(lineitem #   6e10)
            Rows(nation   #    25 )
            Rows(orders   # 1.5e10)
            Rows(part     #   2e9 )
            Rows(partsupp #   8e9 )
            Rows(region   #     5 )
            Rows(supplier #   1e8 )

            Bytes(customer # 156e9 )
            Bytes(lineitem #   4e12)
            Bytes(nation   #   2e3 )
            Bytes(orders   # 969e9 )
            Bytes(part     # 142e9 )
            Bytes(partsupp # 524e9 )
            Bytes(region   #   1e3 )
            Bytes(supplier #   9e9 )
        ';
    )";
};

// ============================================================================
// Tests
// ============================================================================

Y_UNIT_TEST_SUITE(TKqpTasksGraphBuild) {

    Y_UNIT_TEST_F(TpchQuery01, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * ($z1_12 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * ($z1_12 - l_discount) * ($z1_12 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            from
                `/Root/lineitem`
            where
                l_shipdate <= Date('1998-12-01') - Interval("P90D")
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 3840);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 2880);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery02, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $r = (
            select
                r_regionkey
            from
                `/Root/region`
            where
                r_name='EUROPE'
            );
            $n = (
            select
                n_name,
                n_nationkey
            from
                `/Root/nation` as n
            left semi join
                $r as r
            on
                n.n_regionkey = r.r_regionkey
            );
            $s1 = (
            select
  	    s_suppkey
            from
                `/Root/supplier` as s
            left semi join
                $n as n
            on
                s.s_nationkey = n.n_nationkey
            );
            $min_ps_supplycost = (
            select
                min(ps_supplycost) as min_ps_supplycost,
                ps.ps_partkey as ps_partkey
            from
                `/Root/partsupp` as ps
            left semi join
                $s1 as s
            on
                ps.ps_suppkey = s.s_suppkey
            group by
                ps.ps_partkey
            );
            $p = (
            select
                p_partkey,
                p_mfgr
            from
                `/Root/part`
            where
                p_size = 15
                and p_type like '%BRASS'
            );
            $ps = (
            select
                ps.ps_partkey as ps_partkey,
                p.p_mfgr as p_mfgr,
                ps.ps_supplycost as ps_supplycost,
                ps.ps_suppkey as ps_suppkey
            from
                `/Root/partsupp` as ps
            join
                $p as p
            on
                p.p_partkey = ps.ps_partkey
            );
            $s2 = (
            select
                s_acctbal,
                s_name,
                s_address,
                s_phone,
                s_comment,
                s_suppkey,
                n_name
            from
                `/Root/supplier` as s
            join
                $n as n
            on
                s.s_nationkey = n.n_nationkey
            );
            $jp =(
            select
                ps_partkey,
                ps_supplycost,
                p_mfgr,
                s_acctbal,
                s_name,
                s_address,
                s_phone,
                s_comment,
                n_name
            from
                $ps as ps
            join
                $s2 as s
            on
                ps.ps_suppkey = s.s_suppkey
            );
            select
                s_acctbal,
                s_name,
                n_name,
                jp.ps_partkey as p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                $jp as jp
            join
                $min_ps_supplycost as m
            on
                jp.ps_partkey = m.ps_partkey
            where
                min_ps_supplycost = ps_supplycost
            order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey
            limit 100;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 13u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 8);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 7), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 8), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 9), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 1);
    }
    */

    Y_UNIT_TEST_F(TpchQuery03, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $round = ($x,$y) -> {return $x;};
            $c = (
            select
                c_custkey
            from
                `/Root/customer`
            where
                c_mktsegment = 'BUILDING'
            );
            $o = (
            select
                o_orderdate,
                o_shippriority,
                o_orderkey
            from
                `/Root/orders` as o
            left semi join
                $c as c
            on
                c.c_custkey = o.o_custkey
            where
                o_orderdate < Date('1995-03-15')
            );
            $join2 = (
            select
                o.o_orderdate as o_orderdate,
                o.o_shippriority as o_shippriority,
                l.l_orderkey as l_orderkey,
                l.l_discount as l_discount,
                l.l_extendedprice as l_extendedprice
            from
                `/Root/lineitem` as l
            join
                $o as o
            on
                l.l_orderkey = o.o_orderkey
            where
                l_shipdate > Date('1995-03-15')
            );
            select
                l_orderkey,
                $round(sum(l_extendedprice * ($z1_12 - l_discount)), -3) as revenue,
                o_orderdate,
                o_shippriority
            from
                $join2
            group by
                l_orderkey,
                o_orderdate,
                o_shippriority
            order by
                revenue desc,
                o_orderdate,
                l_orderkey
            limit 10;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 6), 1);
    }

    Y_UNIT_TEST_F(TpchQuery04, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $border = Date("1993-07-01");
            $o = (
            select
                o_orderpriority,
                o_orderkey
            from
                `/Root/orders`
            where
                o_orderdate >= $border
                and o_orderdate < DateTime::MakeDate(DateTime::ShiftMonths($border, 3))
            );
            $l = (
            select
                distinct l_orderkey
            from
                `/Root/lineitem`
            where
                l_commitdate < l_receiptdate
            );
            select
                o.o_orderpriority as o_orderpriority,
                count(*) as order_count
            from
                $o as o
            join
                $l as l
            on
                o.o_orderkey = l.l_orderkey
            group by
                o.o_orderpriority
            order by
                o_orderpriority;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 6u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 1), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 2), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 3), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 4), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 5), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery05, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $border = Date("1994-01-01");
            $j1 = (
            select
                n.n_name as n_name,
                n.n_nationkey as n_nationkey
            from
                `/Root/nation` as n
            join
                `/Root/region` as r
            on
                n.n_regionkey = r.r_regionkey
            where
                r_name = 'ASIA'
            );
            $j2 = (
            select
                j.n_name as n_name,
                j.n_nationkey as n_nationkey,
                s.s_suppkey as s_suppkey
            from
                `/Root/supplier` as s
            join
                $j1 as j
            on
                j.n_nationkey = s.s_nationkey
            );
            $j3 = (
            select
                j.n_name as n_name,
                j.n_nationkey as n_nationkey,
                l.l_extendedprice as l_extendedprice,
                l.l_discount as l_discount,
                l.l_orderkey as l_orderkey
            from
                `/Root/lineitem` as l
            join
                $j2 as j
            on
                l.l_suppkey = j.s_suppkey
            );
            $j4 = (
            select
                o.o_orderkey as o_orderkey,
                c.c_nationkey as c_nationkey
            from
                `/Root/orders` as o
            join
                `/Root/customer` as c
            on
                c.c_custkey = o.o_custkey
            where
                o.o_orderdate >= $border
                and o.o_orderdate < ($border + Interval("P365D"))
            );
            $j5 = (
            select
                j3.n_name as n_name,
                j3.l_extendedprice as l_extendedprice,
                j3.l_discount as l_discount
            from
                $j3 as j3
            join
                $j4 as j4
            on
                j3.n_nationkey = j4.c_nationkey
                and j3.l_orderkey = j4.o_orderkey
            );
            select
                n_name,
                sum(l_extendedprice * ($z1_12 - l_discount)) as revenue
            from
                $j5
            group by
                n_name
            order by
                revenue desc;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 8);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 32);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 1);
    }
    */

    Y_UNIT_TEST_F(TpchQuery06, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z0_0100001_12 = cast("0.0100001" as decimal(12,2));
            $z0_06_12 = cast("0.06" as decimal(12,2));
            $border = Date("1994-01-01");
            select
                sum(l_extendedprice * l_discount) as revenue
            from
                `/Root/lineitem`
            where
                l_shipdate >= $border
                and l_shipdate < ($border + Interval("P365D"))
                and l_discount between $z0_06_12 - $z0_0100001_12 and $z0_06_12 + $z0_0100001_12
                and l_quantity < 24;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 3840);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery07, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $n = select n_name, n_nationkey from `/Root/nation` as n
                where n_name = 'FRANCE' or n_name = 'GERMANY';
            $l = select
                l_orderkey, l_suppkey,
                DateTime::GetYear(cast(l_shipdate as timestamp)) as l_year,
                l_extendedprice * ($z1_12 - l_discount) as volume
            from
                `/Root/lineitem` as l
            where
                l.l_shipdate between Date('1995-01-01') and Date('1996-12-31');
            $j1 = select
                n_name as supp_nation,
                s_suppkey
            from
                `/Root/supplier` as supplier
            join
                $n as n1
            on
                supplier.s_nationkey = n1.n_nationkey;
            $j2 = select
                n_name as cust_nation,
                c_custkey
            from
                `/Root/customer` as customer
            join
                $n as n2
            on
                customer.c_nationkey = n2.n_nationkey;
            $j3 = select
                cust_nation, o_orderkey
            from
                `/Root/orders` as orders
            join
                $j2 as customer
            on
                orders.o_custkey = customer.c_custkey;
            $j4 = select
                cust_nation,
                l_orderkey, l_suppkey,
                l_year,
                volume
            from
                $l as lineitem
            join
                $j3 as orders
            on
                lineitem.l_orderkey = orders.o_orderkey;
            $j5 = select
                supp_nation, cust_nation,
                l_year, volume
            from
                $j4 as lineitem
            join
                $j1 as supplier
            on
                lineitem.l_suppkey = supplier.s_suppkey
            where (supp_nation = 'FRANCE' and cust_nation = 'GERMANY')
                OR (supp_nation = 'GERMANY' and cust_nation = 'FRANCE');
            select
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) as revenue
            from
                $j5 as shipping
            group by
                supp_nation,
                cust_nation,
                l_year
            order by
                supp_nation,
                cust_nation,
                l_year;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 960);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 8);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 1);
    }
    */

    Y_UNIT_TEST_F(TpchQuery08, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z0_12 = cast(0 as decimal(12,2));
            $z1_12 = cast(1 as decimal(12,2));
            $join1 = (
            select
                l.l_extendedprice * ($z1_12 - l.l_discount) as volume,
                l.l_suppkey as l_suppkey,
                l.l_orderkey as l_orderkey
            from
                `/Root/part` as p
            join
                `/Root/lineitem` as l
            on
                p.p_partkey = l.l_partkey
            where
                p.p_type = 'ECONOMY ANODIZED STEEL'
            );
            $join2 = (
            select
                j.volume as volume,
                j.l_orderkey as l_orderkey,
                s.s_nationkey as s_nationkey
            from
                $join1 as j
            join
                `/Root/supplier` as s
            on
                s.s_suppkey = j.l_suppkey
            );
            $join3 = (
            select
                j.volume as volume,
                j.l_orderkey as l_orderkey,
                n.n_name as nation
            from
                $join2 as j
            join
                `/Root/nation` as n
            on
                n.n_nationkey = j.s_nationkey
            );
            $join4 = (
            select
                j.volume as volume,
                j.nation as nation,
                DateTime::GetYear(cast(o.o_orderdate as Timestamp)) as o_year,
                o.o_custkey as o_custkey
            from
                $join3 as j
            join
                `/Root/orders` as o
            on
                o.o_orderkey = j.l_orderkey
            where o_orderdate between Date('1995-01-01') and Date('1996-12-31')
            );
            $join5 = (
            select
                j.volume as volume,
                j.nation as nation,
                j.o_year as o_year,
                c.c_nationkey as c_nationkey
            from
                $join4 as j
            join
                `/Root/customer` as c
            on
                c.c_custkey = j.o_custkey
            );
            $join6 = (
            select
                j.volume as volume,
                j.nation as nation,
                j.o_year as o_year,
                n.n_regionkey as n_regionkey
            from
                $join5 as j
            join
                `/Root/nation` as n
            on
                n.n_nationkey = j.c_nationkey
            );
            $join7 = (
            select
                j.volume as volume,
                j.nation as nation,
                j.o_year as o_year
            from
                $join6 as j
            join
                `/Root/region` as r
            on
                r.r_regionkey = j.n_regionkey
            where
                r.r_name = 'AMERICA'
            );
            select
                o_year,
                sum(case
                    when nation = 'BRAZIL' then volume
                    else $z0_12
                end) / sum(volume) as mkt_share
            from
                $join7 as all_nations
            group by
                o_year
            order by
                o_year;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 14u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 6);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 13), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery09, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $p = (select p_partkey, p_name
            from
                `/Root/part`
            where p_name like '%green%');
            $j1 = (select ps_partkey, ps_suppkey, ps_supplycost
            from
                `/Root/partsupp` as ps
            join $p as p
            on ps.ps_partkey = p.p_partkey);
            $j2 = (select l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
            from
                `/Root/lineitem` as l
            join $j1 as j
            on l.l_suppkey = j.ps_suppkey AND l.l_partkey = j.ps_partkey);
            $j3 = (select l_orderkey, s_nationkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
            from
                `/Root/supplier` as s
            join $j2 as j
            on j.l_suppkey = s.s_suppkey);
            $j4 = (select o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity, s_nationkey
            from
                `/Root/orders` as o
            join $j3 as j
            on o.o_orderkey = j.l_orderkey);
            $j5 = (select n_name, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
            from
                `/Root/nation` as n
            join $j4 as j
            on j.s_nationkey = n.n_nationkey
            );
            $profit = (select
                n_name as nation,
                DateTime::GetYear(cast(o_orderdate as timestamp)) as o_year,
                l_extendedprice * ($z1_12 - l_discount) - ps_supplycost * l_quantity as amount
            from $j5);
            select
                nation,
                o_year,
                sum(amount) as sum_profit
            from $profit
            group by
                nation,
                o_year
            order by
                nation,
                o_year desc;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 13u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 600);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 600);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 840);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 600);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 1);
    }

    Y_UNIT_TEST_F(TpchQuery10, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $border = Date("1993-10-01");
            $join1 = (
            select
                c.c_custkey as c_custkey,
                c.c_name as c_name,
                c.c_acctbal as c_acctbal,
                c.c_address as c_address,
                c.c_phone as c_phone,
                c.c_comment as c_comment,
                c.c_nationkey as c_nationkey,
                o.o_orderkey as o_orderkey
            from
                `/Root/customer` as c
            join
                `/Root/orders` as o
            on
                c.c_custkey = o.o_custkey
            where
                o.o_orderdate >= $border
                and o.o_orderdate < ($border + Interval("P92D"))
            );
            $join2 = (
            select
                j.c_custkey as c_custkey,
                j.c_name as c_name,
                j.c_acctbal as c_acctbal,
                j.c_address as c_address,
                j.c_phone as c_phone,
                j.c_comment as c_comment,
                j.c_nationkey as c_nationkey,
                l.l_extendedprice as l_extendedprice,
                l.l_discount as l_discount
            from
                $join1 as j
            join
                `/Root/lineitem` as l
            on
                l.l_orderkey = j.o_orderkey
            where
                l.l_returnflag = 'R'
            );
            $join3 = (
            select
                j.c_custkey as c_custkey,
                j.c_name as c_name,
                j.c_acctbal as c_acctbal,
                j.c_address as c_address,
                j.c_phone as c_phone,
                j.c_comment as c_comment,
                j.c_nationkey as c_nationkey,
                j.l_extendedprice as l_extendedprice,
                j.l_discount as l_discount,
                n.n_name as n_name
            from
                $join2 as j
            join
                `/Root/nation` as n
            on
                n.n_nationkey = j.c_nationkey
            );
            select
                c_custkey,
                c_name,
                sum(l_extendedprice * ($z1_12 - l_discount)) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            from
                $join3
            group by
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            order by
                revenue desc
            limit 20;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 10);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1);
    }
    */

    Y_UNIT_TEST_F(TpchQuery11, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $scale_factor = cast('10000' as decimal(35,2));
            $j1 = (
            select
                s.s_suppkey as s_suppkey
            from
                `/Root/supplier` as s
            join
                `/Root/nation` as n
            on
                n.n_nationkey = s.s_nationkey
            where
                n.n_name = 'GERMANY'
            );
            $j2 = (
            select
                ps.ps_partkey as ps_partkey,
                ps.ps_supplycost as ps_supplycost,
                ps.ps_availqty as ps_availqty
            from
                `/Root/partsupp` as ps
            join
                $j1 as j
            on
                ps.ps_suppkey = j.s_suppkey
            );
            $threshold = (
            select
                sum(ps_supplycost * ps_availqty) / 10000 / $scale_factor as threshold
            from
                $j2
            );
            $values = (
            select
                ps_partkey,
                sum(ps_supplycost * ps_availqty) as value
            from
                $j2
            group by
                ps_partkey
            );
            select
                v.ps_partkey as ps_partkey,
                v.value as value
            from
                $values as v
            cross join
                $threshold as t
            where
                v.value > t.threshold
            order by
                value desc;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 16);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  1), 16);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  4), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  5), 1);
    }

    Y_UNIT_TEST_F(TpchQuery12, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $join = (
                select
                    l.l_shipmode as l_shipmode,
                    o.o_orderpriority as o_orderpriority,
                    l.l_commitdate as l_commitdate,
                    l.l_shipdate as l_shipdate,
                    l.l_receiptdate as l_receiptdate
                from
                    `/Root/orders` as o
                    join `/Root/lineitem` as l
                    on o.o_orderkey = l.l_orderkey
            );
            $border = Date("1994-01-01");
            select
                l_shipmode,
                sum(case
                    when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
                sum(case
                    when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) as low_line_count
            from $join
            where
                (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')
                and l_commitdate < l_receiptdate
                and l_shipdate < l_commitdate
                and l_receiptdate >= $border
                and l_receiptdate < ($border + Interval("P365D"))
            group by
                l_shipmode
            order by
                l_shipmode;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1);
    }

    Y_UNIT_TEST_F(TpchQuery13, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $orders = (
                select
                    o_orderkey,
                    o_custkey
                from
                    `/Root/orders`
                where
                    o_comment NOT LIKE "%special%requests%"
            );
            select
                c_count as c_count,
                count(*) as custdist
            from
                (
                    select
                        c.c_custkey as c_custkey,
                        count(o.o_orderkey) as c_count
                    from
                        `/Root/customer` as c left outer join $orders as o on
                            c.c_custkey = o.o_custkey
                    group by
                        c.c_custkey
                ) as c_orders
            group by
                c_count
            order by
                custdist desc,
                c_count desc;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 6u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 1);
    }

    Y_UNIT_TEST_F(TpchQuery14, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z100_35 = cast(100 as decimal(35,2));
            $z0_12 = cast(0 as decimal(12,2));
            $z1_12 = cast(1 as decimal(12,2));
            $border = Date("1995-09-01");
            select
                $z100_35 * sum(case
                    when p.p_type like 'PROMO%'
                        then l.l_extendedprice * ($z1_12 - l.l_discount)
                    else $z0_12
                end) / sum(l.l_extendedprice * ($z1_12 - l.l_discount)) as promo_revenue
            from
                `/Root/lineitem` as l
            join
                `/Root/part` as p
            on
                l.l_partkey = p.p_partkey
            where
                l.l_shipdate >= $border
                and l.l_shipdate < ($border + Interval("P30D"));
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  0), 1);
    }

    Y_UNIT_TEST_F(TpchQuery15, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            $round = ($x,$y) -> {return $x;};
            $border = Date("1996-01-01");
            $revenue0 = (
                select
                    l_suppkey as supplier_no,
                    $round(sum(l_extendedprice * ($z1_12 - l_discount)), -8) as total_revenue
                from
                    `/Root/lineitem`
                where
                    l_shipdate  >= $border
                    and l_shipdate < ($border + Interval("P91D"))
                group by
                    l_suppkey
            );
            $max_revenue = (
            select
                max(total_revenue) as max_revenue
            from
                $revenue0
            );
            $join1 = (
            select
                s.s_suppkey as s_suppkey,
                s.s_name as s_name,
                s.s_address as s_address,
                s.s_phone as s_phone,
                r.total_revenue as total_revenue
            from
                `/Root/supplier` as s
            join
                $revenue0 as r
            on
                s.s_suppkey = r.supplier_no
            );
            select
                j.s_suppkey as s_suppkey,
                j.s_name as s_name,
                j.s_address as s_address,
                j.s_phone as s_phone,
                j.total_revenue as total_revenue
            from
                $join1 as j
            join
                $max_revenue as m
            on
                j.total_revenue = m.max_revenue
            order by
                s_suppkey;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1);
    }

    Y_UNIT_TEST_F(TpchQuery16, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $p = (
            select
                p.p_brand as p_brand,
                p.p_type as p_type,
                p.p_size as p_size,
                ps.ps_suppkey as ps_suppkey
            from
                `/Root/part` as p
            join
                `/Root/partsupp` as ps
            on
                p.p_partkey = ps.ps_partkey
            where
                p.p_brand <> 'Brand#45'
                and p.p_type not like 'MEDIUM POLISHED%'
                and (p.p_size = 49 or p.p_size = 14 or p.p_size = 23 or p.p_size = 45 or p.p_size = 19 or p.p_size = 3 or p.p_size = 36 or p.p_size = 9)
            );
            $s = (
            select
                s_suppkey
            from
                `/Root/supplier`
            where
                s_comment like "%Customer%Complaints%"
            );
            $j = (
            select
                p.p_brand as p_brand,
                p.p_type as p_type,
                p.p_size as p_size,
                p.ps_suppkey as ps_suppkey
            from
                $p as p
            left only join
                $s as s
            on
                p.ps_suppkey = s.s_suppkey
            );
            select
                j.p_brand as p_brand,
                j.p_type as p_type,
                j.p_size as p_size,
                count(distinct j.ps_suppkey) as supplier_cnt
            from
                $j as j
            group by
                j.p_brand,
                j.p_type,
                j.p_size
            order by
                supplier_cnt desc,
                p_brand,
                p_type,
                p_size
            ;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery17, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z7_35 = cast("7." as decimal(35,2));
            $z0_2_12 = cast("0.2" as decimal(12,2));
            $p = select p_partkey from `/Root/part`
            where
                p_brand = 'Brand#23'
                and p_container = 'MED BOX'
            ;
            $threshold = (
            select
                $z0_2_12 * avg(l_quantity) as threshold,
                l.l_partkey as l_partkey
            from
                `/Root/lineitem` as l
            left semi join
                $p as p
            on
                p.p_partkey = l.l_partkey
            group by
                l.l_partkey
            );
            $l = select l.l_partkey as l_partkey, l.l_quantity as l_quantity, l.l_extendedprice as l_extendedprice
            from
                `/Root/lineitem` as l
            join
                $p as p
            on
                p.p_partkey = l.l_partkey;
            select
                sum(l.l_extendedprice) / $z7_35 as avg_yearly
            from
                $l as l
            join
                $threshold as t
            on
                t.l_partkey = l.l_partkey
            where
                l.l_quantity < t.threshold;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(1,  0), 1);
    }
    */

    Y_UNIT_TEST_F(TpchQuery18, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $in = (
            select
                l_orderkey,
                sum(l_quantity) as sum_l_quantity
            from
                `/Root/lineitem`
            group by
                l_orderkey having
                    sum(l_quantity) > 300
            );
            $join1 = (
            select
                c.c_name as c_name,
                c.c_custkey as c_custkey,
                o.o_orderkey as o_orderkey,
                o.o_orderdate as o_orderdate,
                o.o_totalprice as o_totalprice
            from
                `/Root/customer` as c
            join
                `/Root/orders` as o
            on
                c.c_custkey = o.o_custkey
            );
            select
                j.c_name as c_name,
                j.c_custkey as c_custkey,
                j.o_orderkey as o_orderkey,
                j.o_orderdate as o_orderdate,
                j.o_totalprice as o_totalprice,
                sum(i.sum_l_quantity) as sum_l_quantity
            from
                $join1 as j
            join
                $in as i
            on
                i.l_orderkey = j.o_orderkey
            group by
                j.c_name,
                j.c_custkey,
                j.o_orderkey,
                j.o_orderdate,
                j.o_totalprice
            order by
                o_totalprice desc,
                o_orderdate,
                o_orderkey
            limit 100;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1);
    }

    Y_UNIT_TEST_F(TpchQuery19, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z1_12 = cast(1 as decimal(12,2));
            select
                sum(l.l_extendedprice* ($z1_12 - l.l_discount)) as revenue
            from
                `/Root/lineitem` as l
            join
                `/Root/part` as p
            on
                p.p_partkey = l.l_partkey
            where
                (
                    p.p_brand = 'Brand#12'
                    and (p.p_container = 'SM CASE' or p.p_container = 'SM BOX' or p.p_container = 'SM PACK' or p.p_container = 'SM PKG')
                    and l.l_quantity >= 1 and l.l_quantity <= 1 + 10
                    and p.p_size between 1 and 5
                    and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p.p_brand = 'Brand#23'
                    and (p.p_container = 'MED BAG' or p.p_container = 'MED BOX' or p.p_container = 'MED PKG' or p.p_container = 'MED PACK')
                    and l.l_quantity >= 10 and l.l_quantity <= 10 + 10
                    and p.p_size between 1 and 10
                    and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p.p_brand = 'Brand#34'
                    and (p.p_container = 'LG CASE' or p.p_container = 'LG BOX' or p.p_container = 'LG PACK' or p.p_container = 'LG PKG')
                    and l.l_quantity >= 20 and l.l_quantity <= 20 + 10
                    and p.p_size between 1 and 15
                    and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                );
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1);
    }

    Y_UNIT_TEST_F(TpchQuery20, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z0_5_35 = cast("0.5" as decimal(35,2));
            $border = Date("1994-01-01");
            $threshold = (
            select
                $z0_5_35 * sum(l_quantity) as threshold,
                l_partkey as l_partkey,
                l_suppkey as l_suppkey
            from
                `/Root/lineitem`
            where
                l_shipdate >= $border
                and l_shipdate < ($border + Interval("P365D"))
            group by
                l_partkey, l_suppkey
            );
            $parts = (
            select
                p_partkey
            from
                `/Root/part`
            where
                p_name like 'forest%'
            );
            $join1 = (
            select
                ps.ps_suppkey as ps_suppkey,
                ps.ps_availqty as ps_availqty,
                ps.ps_partkey as ps_partkey
            from
                `/Root/partsupp` as ps
            join any
                $parts as p
            on
                ps.ps_partkey = p.p_partkey
            );
            $join2 = (
            select
                distinct(j.ps_suppkey) as ps_suppkey
            from
                $join1 as j
            join any
                $threshold as t
            on
                j.ps_partkey = t.l_partkey and j.ps_suppkey = t.l_suppkey
            where
                j.ps_availqty > t.threshold
            );
            $join3 = (
            select
                j.ps_suppkey as ps_suppkey,
                s.s_name as s_name,
                s.s_address as s_address,
                s.s_nationkey as s_nationkey
            from
                $join2 as j
            join any
                `/Root/supplier` as s
            on
                j.ps_suppkey = s.s_suppkey
            );
            select
                j.s_name as s_name,
                j.s_address as s_address
            from
                $join3 as j
            join
                `/Root/nation` as n
            on
                j.s_nationkey = n.n_nationkey
            where
                n.n_name = 'CANADA'
            order by
                s_name;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 6);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 1);
    }

    /*
    Y_UNIT_TEST_F(TpchQuery21, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $n = select n_nationkey from `/Root/nation`
            where n_name = 'SAUDI ARABIA';
            $s = select s_name, s_suppkey from `/Root/supplier` as supplier
            join $n as nation
            on supplier.s_nationkey = nation.n_nationkey;
            $l = select l_suppkey, l_orderkey from `/Root/lineitem`
            where l_receiptdate > l_commitdate;
            $j1 = select s_name, l_suppkey, l_orderkey from $l as l1
            join $s as supplier
            on l1.l_suppkey = supplier.s_suppkey;
            $j2 = select l1.l_orderkey as l_orderkey, l1.l_suppkey as l_suppkey, l1.s_name as s_name, l2.l_receiptdate as l_receiptdate, l2.l_commitdate as l_commitdate from $j1 as l1
            join `/Root/lineitem` as l2
            on l1.l_orderkey = l2.l_orderkey
            where l2.l_suppkey <> l1.l_suppkey;
            $j2_1 = select s_name, l1.l_suppkey as l_suppkey, l1.l_orderkey as l_orderkey from $j1 as l1
            left semi join $j2 as l2
            on l1.l_orderkey = l2.l_orderkey;
            $j2_2 = select l_orderkey from $j2 where l_receiptdate > l_commitdate;
            $j3 = select s_name, l_suppkey, l_orderkey from $j2_1 as l1
            left only join $j2_2 as l3
            on l1.l_orderkey = l3.l_orderkey;
            $j4 = select s_name from $j3 as l1
            join `/Root/orders` as orders
            on orders.o_orderkey = l1.l_orderkey
            where o_orderstatus = 'F';
            select s_name,
                count(*) as numwait from $j4
            group by
                s_name
            order by
                numwait desc,
                s_name
            limit 100;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1200);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 10);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 1440);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 1440);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 1440);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 1440);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 1);
    }

    Y_UNIT_TEST_F(TpchQuery22, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $z0_12 = cast(0 as decimal(12,2));
            $customers = (
            select
                c_acctbal,
                c_custkey,
                Substring(CAST(c_phone AS STRING), 0u, 2u) as cntrycode
            from
                `/Root/customer`
            );
            $c = (
            select
                c_acctbal,
                c_custkey,
                cntrycode
            from
                $customers
            where
                cntrycode = '13' or cntrycode = '31' or cntrycode = '23' or cntrycode = '29' or cntrycode = '30' or cntrycode = '18' or cntrycode = '17'
            );
            $avg = (
            select
                avg(c_acctbal) as a
            from
                $c
            where
                c_acctbal > $z0_12
            );
            $join1 = (
            select
                c.c_acctbal as c_acctbal,
                c.c_custkey as c_custkey,
                c.cntrycode as cntrycode
            from
                $c as c
            cross join
                $avg as a
            where
                c.c_acctbal > a.a
            );
            $join2 = (
            select
                j.cntrycode as cntrycode,
                c_custkey,
                j.c_acctbal as c_acctbal
            from
                $join1 as j
            left only join
                `/Root/orders` as o
            on
                o.o_custkey = j.c_custkey
            );
            select
                cntrycode,
                count(*) as numcust,
                sum(c_acctbal) as totacctbal
            from
                $join2 as custsale
            group by
                cntrycode
            order by
                cntrycode;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 1920);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 1);
    }
    */

    /*
    Y_UNIT_TEST_F(CustomQuery01, TKqpTasksGraphTpchFixture) {
        const TString& queryText = R"(
            $step1 = (
                SELECT
                    l.l_orderkey  AS l_orderkey,
                    l.l_suppkey   AS l_suppkey,
                    l.l_partkey   AS l_partkey,
                    SUM(CAST(l.l_extendedprice AS Double) * (1.0 - CAST(l.l_discount AS Double))) AS line_revenue,
                    SUM(CAST(l.l_quantity AS Double)) AS line_qty
                FROM `/Root/lineitem` AS l
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
                INNER JOIN `/Root/orders` AS o
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
                INNER JOIN `/Root/customer` AS c
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
                INNER JOIN `/Root/supplier` AS s
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
            INNER JOIN `/Root/nation` AS sn
                ON s4.supp_nationkey = sn.n_nationkey
            INNER JOIN `/Root/region` AS sr
                ON sn.n_regionkey = sr.r_regionkey
            INNER JOIN `/Root/nation` AS cn
                ON s4.cust_nationkey = cn.n_nationkey
            INNER JOIN `/Root/region` AS cr
                ON cn.n_regionkey = cr.r_regionkey
            ORDER BY s4.bilateral_revenue DESC
            LIMIT 100;
        )";

        auto dist = BuildTasks(queryText);

        UNIT_ASSERT_VALUES_EQUAL(dist.TasksPerStage.size(), 25u);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  0), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  1), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  2), 600);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  3), 600);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  4), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  5), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  6), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  7), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  8), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0,  9), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 10), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 11), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 12), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 13), 256);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 14), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 15), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 16), 5);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 17), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 18), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 19), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 20), 480);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 21), 1);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 22), 240);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 23), 720);
        UNIT_ASSERT_VALUES_EQUAL(dist.Count(0, 24), 1);
    }
    */

} // Y_UNIT_TEST_SUITE(TKqpTasksGraphBuild)

} // namespace NKikimr::NKqp
