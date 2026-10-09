#include <ydb/core/base/hive.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <cstdlib>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

NJson::TJsonValue GetLegacySimplifiedPlan(const TString& plan) {
    NYql::NDqProto::TDqExecutionStats executionStats;
    NJson::TJsonValue planJson;
    const auto planWithStats = AddExecStatsToTxPlan(plan, executionStats, false);
    UNIT_ASSERT_C(NJson::ReadJsonTree(planWithStats, &planJson, true), planWithStats);
    return planJson.GetMapSafe().at("SimplifiedPlan");
}

NJson::TJsonValue FindRequiredPlanNodeByKv(
    const NJson::TJsonValue& plan,
    const TString& key,
    const TString& value)
{
    auto node = FindPlanNodeByKv(plan, key, value);
    UNIT_ASSERT_C(node.IsDefined(), plan);
    return node;
}

void AssertCpuValues(
    const NJson::TJsonValue& node,
    double expectedSelfCpu,
    double expectedCpu,
    const NJson::TJsonValue& plan)
{
    UNIT_ASSERT_C(node.GetMapSafe().contains("A-SelfCpu"), plan);
    UNIT_ASSERT_C(node.GetMapSafe().contains("A-Cpu"), plan);
    UNIT_ASSERT_VALUES_EQUAL_C(node.GetMapSafe().at("A-SelfCpu").GetDoubleSafe(), expectedSelfCpu, plan);
    UNIT_ASSERT_VALUES_EQUAL_C(node.GetMapSafe().at("A-Cpu").GetDoubleSafe(), expectedCpu, plan);
}

void AssertNoCpuValues(const NJson::TJsonValue& node, const NJson::TJsonValue& plan) {
    UNIT_ASSERT_C(FindPlanNodes(node, "A-SelfCpu").empty(), plan);
    UNIT_ASSERT_C(FindPlanNodes(node, "A-Cpu").empty(), plan);
}

struct TReturningRun {
    NYdb::NQuery::TExecuteQueryResult Result;
    NJson::TJsonValue Plan;
};

TReturningRun RunReturningQuery(NYdb::NQuery::TQueryClient& client, const TString& query, ui64 expectedRows) {
    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .StatsMode(NYdb::NQuery::EStatsMode::Full);

    auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // RETURNING must actually return the written rows.
    const auto& resultSets = result.GetResultSets();
    UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
    NYdb::TResultSetParser parser(resultSets[0]);
    UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), expectedRows);

    UNIT_ASSERT(result.GetStats());
    UNIT_ASSERT(result.GetStats()->GetPlan());

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
    UNIT_ASSERT(ValidatePlanNodeIds(plan));

    return {std::move(result), std::move(plan)};
}

void AssertReturningSinkNode(const NJson::TJsonValue& plan, bool useStreamIndex) {
    UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(plan, "Node Type", "ReturningSink"), useStreamIndex ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(plan, "Node Type", "Sink"), useStreamIndex ? 0 : 1);
}

void AssertSingleOperatorName(const NJson::TJsonValue& plan, const TString& opName) {
    UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(plan, "Name", opName), 1);
}

Y_UNIT_TEST_SUITE(KqpStats) {

Y_UNIT_TEST_TWIN(CompilationCacheHitMissCounters, AstCache) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(AstCache);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    const auto counters = kikimr.GetTestServer().GetRuntime()->GetAppData().Counters->FindSubgroup("counters", "ydb");
    UNIT_ASSERT(counters);
    const auto cacheHits = counters->FindNamedCounter("name", "table.query.compilation.cache_hits");
    const auto cacheMisses = counters->FindNamedCounter("name", "table.query.compilation.cache_misses");
    const auto compilations = counters->FindNamedCounter("name", "table.query.compilation.count");
    const auto compilationErrors = counters->FindNamedCounter("name", "table.query.compilation.error_count");
    const auto kqpCounters = kikimr.GetTestServer().GetRuntime()->GetAppData().Counters->FindSubgroup("counters", "kqp");
    UNIT_ASSERT(kqpCounters);
    const auto compileRequests = kqpCounters->FindCounter("Compilation/Requests/Compile");
    const auto recompileRequests = kqpCounters->FindCounter("Compilation/Requests/Recompile");
    UNIT_ASSERT(cacheHits);
    UNIT_ASSERT(cacheMisses);
    UNIT_ASSERT(compilations);
    UNIT_ASSERT(compilationErrors);
    UNIT_ASSERT(compileRequests);
    UNIT_ASSERT(recompileRequests);

    const ui64 initialHits = cacheHits->Val();
    const ui64 initialMisses = cacheMisses->Val();
    const ui64 initialCompilations = compilations->Val();
    const ui64 initialErrors = compilationErrors->Val();
    const ui64 initialCompileRequests = compileRequests->Val();
    const ui64 initialRecompileRequests = recompileRequests->Val();
    const auto execSettings = TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto execute = [&](const TString& query, bool fromCache) {
        auto result = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        const auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), fromCache);
        UNIT_ASSERT(result.GetQuery());
        TString uid;
        UNIT_ASSERT(NOperationId::DecodePreparedQueryIdCompat(TString(result.GetQuery()->GetId()), uid.MutRef()));
        return uid;
    };

    const TString query = Q1_("SELECT 42 AS compilation_cache_counter;");
    const auto queryUid = execute(query, false);
    UNIT_ASSERT_VALUES_EQUAL(cacheMisses->Val(), initialMisses + 1);
    UNIT_ASSERT_VALUES_EQUAL(cacheHits->Val(), initialHits);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + 1);
    UNIT_ASSERT_VALUES_EQUAL(compileRequests->Val(), initialCompileRequests + 1);

    execute(query, true);
    UNIT_ASSERT_VALUES_EQUAL(cacheMisses->Val(), initialMisses + 1);
    UNIT_ASSERT_VALUES_EQUAL(cacheHits->Val(), initialHits + 1);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + 1);
    UNIT_ASSERT_VALUES_EQUAL(compileRequests->Val(), initialCompileRequests + (AstCache ? 2 : 1));

    // A text miss followed by an AST hit must not increase the miss counter.
    execute(Q1_("select 42 as compilation_cache_counter;"), AstCache);
    UNIT_ASSERT_VALUES_EQUAL(cacheMisses->Val(), initialMisses + (AstCache ? 1 : 2));
    UNIT_ASSERT_VALUES_EQUAL(cacheHits->Val(), initialHits + (AstCache ? 2 : 1));
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + (AstCache ? 1 : 2));
    // Requests/Compile includes requests that are subsequently satisfied by the AST cache.
    UNIT_ASSERT_VALUES_EQUAL(compileRequests->Val(), initialCompileRequests + (AstCache ? 3 : 2));

    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    const auto service = MakeKqpCompileServiceID(runtime.GetNodeId());
    TIntrusiveConstPtr<NACLib::TUserToken> token = new NACLib::TUserToken("root@builtin", {});
    auto context = MakeIntrusive<TUserRequestContext>("cache-counters", "/Root", "cache-counters");
    auto dbPublicCounters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto dbInternalCounters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto dbCounters = MakeIntrusive<TKqpDbCounters>(dbPublicCounters, dbInternalCounters);
    TMaybe<TQueryAst> queryAst;

    // Forced recompilation has its own counters and must not classify the client query twice.
    for (ui64 attempt = 1; attempt <= 2; ++attempt) {
        runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvRecompileRequest(
            token, "", queryUid, Nothing(), /*isQueryActionPrepare=*/false, TInstant::Max(),
            dbCounters, std::make_shared<TGUCSettings>(), Nothing(),
            std::make_shared<std::atomic<bool>>(true), context, NLWTrace::TOrbit(), nullptr, queryAst)));
        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(response && response->Get()->CompileResult);
        const auto& result = response->Get()->CompileResult;
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        UNIT_ASSERT(!response->Get()->Stats.FromCache);
        queryAst = result->QueryAst;
        UNIT_ASSERT_VALUES_EQUAL(queryAst.Defined(), AstCache);

        UNIT_ASSERT_VALUES_EQUAL(cacheMisses->Val(), initialMisses + (AstCache ? 1 : 2));
        UNIT_ASSERT_VALUES_EQUAL(cacheHits->Val(), initialHits + (AstCache ? 2 : 1));
        UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + (AstCache ? 1 : 2) + attempt);
        UNIT_ASSERT_VALUES_EQUAL(compileRequests->Val(), initialCompileRequests + (AstCache ? 3 : 2) + attempt);
        UNIT_ASSERT_VALUES_EQUAL(recompileRequests->Val(), initialRecompileRequests + attempt);
        UNIT_ASSERT_VALUES_EQUAL(dbPublicCounters->FindNamedCounter("name", "table.query.compilation.cache_misses")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(dbPublicCounters->FindNamedCounter("name", "table.query.compilation.cache_hits")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(dbPublicCounters->FindNamedCounter("name", "table.query.compilation.count")->Val(), attempt);
        UNIT_ASSERT_VALUES_EQUAL(dbInternalCounters->FindCounter("Compilation/Requests/Compile")->Val(), attempt);
        UNIT_ASSERT_VALUES_EQUAL(dbInternalCounters->FindCounter("Compilation/Requests/Recompile")->Val(), attempt);
    }

    // A compilation error still follows a definitive cache miss.
    auto failed = session.ExecuteDataQuery(Q1_("SELECT Key FROM `/Root/CompilationCacheMissingTable`;"),
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(failed.GetStatus(), EStatus::SCHEME_ERROR, failed.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(cacheMisses->Val(), initialMisses + (AstCache ? 1 : 2) + 1);
    UNIT_ASSERT_VALUES_EQUAL(cacheHits->Val(), initialHits + (AstCache ? 2 : 1));
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + (AstCache ? 1 : 2) + 3);
    UNIT_ASSERT_VALUES_EQUAL(compileRequests->Val(), initialCompileRequests + (AstCache ? 3 : 2) + 3);
    UNIT_ASSERT_VALUES_EQUAL(compilationErrors->Val(), initialErrors + 1);
}

Y_UNIT_TEST_TWIN(CompilationCacheRequestScenarios, AstCache) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    auto* config = settings.AppConfig.MutableTableServiceConfig();
    config->SetEnableAstCache(AstCache);
    config->SetEnableCreateTableAs(true);
    config->SetEnableDataShardCreateTableAs(true);
    config->SetEnablePerStatementQueryExecution(true);
    config->SetCompileMaxActiveRequests(1);
    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    const auto service = MakeKqpCompileServiceID(runtime.GetNodeId());
    const auto publicCounters = runtime.GetAppData().Counters->FindSubgroup("counters", "ydb");
    const auto internalCounters = runtime.GetAppData().Counters->FindSubgroup("counters", "kqp");
    UNIT_ASSERT(publicCounters && internalCounters);
    const auto hits = publicCounters->FindNamedCounter("name", "table.query.compilation.cache_hits");
    const auto misses = publicCounters->FindNamedCounter("name", "table.query.compilation.cache_misses");
    const auto compilations = publicCounters->FindNamedCounter("name", "table.query.compilation.count");
    const auto requests = internalCounters->FindCounter("Compilation/Requests/Compile");
    const auto queueSize = internalCounters->FindCounter("Compilation/QueueSize");
    UNIT_ASSERT(hits && misses && compilations && requests && queueSize);
    const ui64 initialHits = hits->Val();
    const ui64 initialMisses = misses->Val();
    auto context = MakeIntrusive<TUserRequestContext>("cache-scenarios", "/Root", "cache-scenarios");
    auto gucSettings = std::make_shared<TGUCSettings>();
    auto tempTables = std::make_shared<TKqpTempTablesState>();
    tempTables->Database = "/Root";
    tempTables->TempDirName = "cache-counter-session";
    auto makeQuery = [&](const TString& text, NKikimrKqp::EQueryType type = NKikimrKqp::QUERY_TYPE_SQL_DML) {
        return TKqpQueryId("db", "/Root", "cache-scenarios", "root@builtin", text,
            TKqpQuerySettings(type), nullptr, *gucSettings);
    };
    auto send = [&](const TKqpQueryId& query, bool keepInCache, TMaybe<TQueryAst> ast = Nothing(), bool split = false,
                    bool perStatementResult = false) {
        TIntrusiveConstPtr<NACLib::TUserToken> token = new NACLib::TUserToken(query.UserSid, {});
        runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvCompileRequest(
            token, "", Nothing(), TMaybe<TKqpQueryId>(query), keepInCache, false, perStatementResult,
            TInstant::Max(), nullptr, gucSettings, Nothing(), std::make_shared<std::atomic<bool>>(true),
            context, NLWTrace::TOrbit(), tempTables, false, ast, split)));
    };
    auto receive = [&] {
        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(response && response->Get()->CompileResult);
        return response;
    };

    // Disabling insertion must produce a miss on every request.
    const auto uncached = makeQuery(Q1_("SELECT 17 AS uncached_counter;"));
    for (ui64 attempt = 1; attempt <= 2; ++attempt) {
        send(uncached, false);
        const auto response = receive();
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
            response->Get()->CompileResult->Issues.ToString());
        UNIT_ASSERT(!response->Get()->Stats.FromCache);
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + attempt);
        UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits);
    }

    // Invalid SQL cannot be satisfied by either cache, even if AST translation fails first.
    const ui64 beforeInvalidCompilations = compilations->Val();
    send(makeQuery("SELEC invalid syntax;"), true);
    UNIT_ASSERT(receive()->Get()->CompileResult->Status != Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 3);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeInvalidCompilations + (AstCache ? 0 : 1));

    // Both cold requests need a new plan, but share one physical compilation.
    const auto shared = makeQuery(Q1_("SELECT 77 AS shared_compilation_counter;"));
    const ui64 beforeSharedCompilations = compilations->Val();
    const auto beforeSharedRequests = requests->Val();
    TBlockEvents<TEvKqp::TEvCompileResponse> blocked(runtime, [&](const auto& ev) {
        return ev->GetRecipientRewrite() != edge;
    });
    send(shared, true);
    runtime.WaitFor("blocked compilation response", [&] { return !blocked.empty(); }, TDuration::Seconds(30));
    send(shared, true);
    runtime.WaitFor("second request queued", [&] { return queueSize->Val() == 1; }, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL(requests->Val(), beforeSharedRequests + 2);
    UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSharedCompilations + 1);
    blocked.Stop().Unblock();
    TString sharedUid;
    for (ui64 attempt = 0; attempt < 2; ++attempt) {
        const auto response = receive();
        const auto& result = response->Get()->CompileResult;
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        UNIT_ASSERT(!response->Get()->Stats.FromCache);
        if (sharedUid.empty()) {
            sharedUid = result->Uid;
        }
        UNIT_ASSERT_VALUES_EQUAL(result->Uid, sharedUid);
    }
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 5);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSharedCompilations + 1);

    send(shared, true);
    UNIT_ASSERT(receive()->Get()->Stats.FromCache);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 5);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 1);

    // AST equivalence must not reuse another user's plan.
    auto anotherUser = shared;
    anotherUser.UserSid = "another@builtin";
    send(anotherUser, true);
    const auto anotherResponse = receive();
    UNIT_ASSERT_VALUES_EQUAL_C(anotherResponse->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
        anotherResponse->Get()->CompileResult->Issues.ToString());
    UNIT_ASSERT(!anotherResponse->Get()->Stats.FromCache);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 6);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 1);

    // NeedToSplit and SPLIT are intermediate results, not completed client compilations.
    const auto ctas = makeQuery(R"(
        CREATE TABLE `/Root/CompilationCacheCtas` (PRIMARY KEY (Key))
        WITH (STORE = ROW) AS SELECT 1u AS Key;
    )", NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    send(ctas, false);
    const auto ctasResponse = receive();
    UNIT_ASSERT(ctasResponse->Get()->CompileResult->NeedToSplit);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 6);
    const ui64 beforeSplitCompilations = compilations->Val();
    send(ctas, false, ctasResponse->Get()->CompileResult->QueryAst, true);
    const auto splitResponse = runtime.GrabEdgeEvent<TEvKqp::TEvSplitResponse>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(splitResponse);
    UNIT_ASSERT_VALUES_EQUAL_C(splitResponse->Get()->Status, Ydb::StatusIds::SUCCESS, splitResponse->Get()->Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 6);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 1);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSplitCompilations);

    // A missing UID is a service error, not a completed non-cached compilation.
    const auto getRequests = internalCounters->FindCounter("Compilation/Requests/Get");
    UNIT_ASSERT(getRequests);
    const auto beforeGetRequests = getRequests->Val();
    const auto beforeUidCompileRequests = requests->Val();
    TIntrusiveConstPtr<NACLib::TUserToken> token = new NACLib::TUserToken("root@builtin", {});
    for (const bool invalidated : {false, true}) {
        if (invalidated) {
            runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvCompileInvalidateRequest(sharedUid, nullptr)));
        }
        runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvCompileRequest(
            token, "", sharedUid, Nothing(), true, false, false, TInstant::Max(), nullptr, gucSettings,
            Nothing(), std::make_shared<std::atomic<bool>>(true), context)));
        const auto response = receive();
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->CompileResult->Status,
            invalidated ? Ydb::StatusIds::NOT_FOUND : Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Stats.FromCache, !invalidated);
        UNIT_ASSERT_VALUES_EQUAL(getRequests->Val(), beforeGetRequests + (invalidated ? 2 : 1));
        UNIT_ASSERT_VALUES_EQUAL(requests->Val(), beforeUidCompileRequests);
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 6);
        UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 2);
        UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSplitCompilations);
    }
    send(shared, true);
    const auto afterInvalidation = receive();
    UNIT_ASSERT_VALUES_EQUAL_C(afterInvalidation->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
        afterInvalidation->Get()->CompileResult->Issues.ToString());
    UNIT_ASSERT(!afterInvalidation->Get()->Stats.FromCache);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 7);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 2);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSplitCompilations + 1);

    // Per-statement execution counts compiled plans, without counting the initial parse as a miss.
    const auto multi = makeQuery(Q1_("SELECT 101 AS multi_counter; SELECT 202 AS multi_counter;"),
        NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    send(multi, true, Nothing(), false, true);
    if (AstCache) {
        const auto parsed = runtime.GrabEdgeEvent<TEvKqp::TEvParseResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(parsed);
        UNIT_ASSERT_VALUES_EQUAL(parsed->Get()->AstStatements.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 7);
        UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 2);
        for (ui64 attempt = 0; attempt < 2; ++attempt) {
            const auto& ast = parsed->Get()->AstStatements[attempt];
            send(multi, true, ast, false, true);
            const auto response = receive();
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
                response->Get()->CompileResult->Issues.ToString());
            UNIT_ASSERT(!response->Get()->Stats.FromCache);
            UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 8 + attempt);
        }
    } else {
        const auto response = receive();
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS,
            response->Get()->CompileResult->Issues.ToString());
        UNIT_ASSERT(!response->Get()->Stats.FromCache);
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 8);
    }
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + 2);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeSplitCompilations + (AstCache ? 3 : 2));
}

Y_UNIT_TEST_TWIN(CompilationCacheCompletedClientCounters, AstCache) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    auto* config = settings.AppConfig.MutableTableServiceConfig();
    config->SetEnableAstCache(AstCache);
    config->SetCompileMaxActiveRequests(1);
    config->SetCompileRequestQueueSize(2);
    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    const auto service = MakeKqpCompileServiceID(runtime.GetNodeId());
    const auto publicCounters = runtime.GetAppData().Counters->FindSubgroup("counters", "ydb");
    const auto internalCounters = runtime.GetAppData().Counters->FindSubgroup("counters", "kqp");
    UNIT_ASSERT(publicCounters && internalCounters);
    const auto hits = publicCounters->FindNamedCounter("name", "table.query.compilation.cache_hits");
    const auto misses = publicCounters->FindNamedCounter("name", "table.query.compilation.cache_misses");
    const auto compilations = publicCounters->FindNamedCounter("name", "table.query.compilation.count");
    const auto requests = internalCounters->FindCounter("Compilation/Requests/Compile");
    const auto queueSize = internalCounters->FindCounter("Compilation/QueueSize");
    const auto rejected = internalCounters->FindCounter("Compilation/Requests/Rejected");
    const auto timeouts = internalCounters->FindCounter("Compilation/Requests/Timeout");
    UNIT_ASSERT(hits && misses && compilations && requests && queueSize && rejected && timeouts);
    const auto initialHits = hits->Val();
    const auto initialMisses = misses->Val();
    const auto initialCompilations = compilations->Val();
    const auto initialRequests = requests->Val();
    const auto initialRejected = rejected->Val();
    const auto initialTimeouts = timeouts->Val();
    auto dbPublicCounters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto dbInternalCounters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto dbCounters = MakeIntrusive<TKqpDbCounters>(dbPublicCounters, dbInternalCounters);
    const auto dbHits = dbPublicCounters->FindNamedCounter("name", "table.query.compilation.cache_hits");
    const auto dbMisses = dbPublicCounters->FindNamedCounter("name", "table.query.compilation.cache_misses");
    TIntrusiveConstPtr<NACLib::TUserToken> token = new NACLib::TUserToken("root@builtin", {});
    auto context = MakeIntrusive<TUserRequestContext>("completed-cache", "/Root", "completed-cache");
    auto gucSettings = std::make_shared<TGUCSettings>();
    auto send = [&](const TString& text, bool warmup = false, TInstant deadline = TInstant::Max(),
                    std::shared_ptr<std::atomic<bool>> interested = std::make_shared<std::atomic<bool>>(true)) {
        TMaybe<TKqpQueryId> query = TKqpQueryId("db", "/Root", "completed-cache", "root@builtin", text,
            TKqpQuerySettings(NKikimrKqp::QUERY_TYPE_SQL_DML), nullptr, *gucSettings);
        runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvCompileRequest(
            token, "", Nothing(), std::move(query), true, false, false, deadline, dbCounters, gucSettings,
            Nothing(), interested, context, NLWTrace::TOrbit(), nullptr, false, Nothing(), false,
            nullptr, nullptr, warmup)));
    };
    auto receive = [&] {
        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(response && response->Get()->CompileResult);
        return response;
    };

    // No miss is reported before completion, even with the compiler and queue occupied.
    TBlockEvents<TEvKqp::TEvCompileResponse> blocked(runtime, [&](const auto& ev) {
        return ev->GetRecipientRewrite() != edge;
    });
    send("SELECT 1 AS active;");
    runtime.WaitFor("active compilation", [&] { return !blocked.empty(); }, TDuration::Seconds(30));
    send("SELECT 2 AS timed_out;", false, TInstant::MicroSeconds(1));
    auto interested = std::make_shared<std::atomic<bool>>(true);
    send("SELECT 3 AS cancelled;", false, TInstant::Max(), interested);
    runtime.WaitFor("queued requests", [&] { return queueSize->Val() == 2; }, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL(requests->Val(), initialRequests + 3);
    send("SELECT 4 AS overloaded;");
    UNIT_ASSERT_VALUES_EQUAL(receive()->Get()->CompileResult->Status, Ydb::StatusIds::OVERLOADED);
    UNIT_ASSERT_VALUES_EQUAL(rejected->Val(), initialRejected + 1);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits);
    UNIT_ASSERT_VALUES_EQUAL(dbMisses->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + 1);
    interested->store(false);
    blocked.Stop().Unblock();
    bool completed = false;
    bool timedOut = false;
    for (ui32 i = 0; i < 2; ++i) {
        const auto response = receive();
        const auto status = response->Get()->CompileResult->Status;
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::TIMEOUT);
        completed |= status == Ydb::StatusIds::SUCCESS;
        timedOut |= status == Ydb::StatusIds::TIMEOUT;
    }
    UNIT_ASSERT(completed && timedOut);
    UNIT_ASSERT_VALUES_EQUAL(timeouts->Val(), initialTimeouts + 1);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 1);
    UNIT_ASSERT_VALUES_EQUAL(dbMisses->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + 1);

    // A later completed request also proves that the cancelled queued request was dropped.
    send("SELECT 5 AS after_cancellation;");
    UNIT_ASSERT_VALUES_EQUAL(receive()->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 2);
    UNIT_ASSERT_VALUES_EQUAL(dbMisses->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), initialCompilations + 2);

    // Warmup compilation does not add a client miss; a warmup cache hit is still a hit.
    const TString warmQuery = "SELECT 6 AS warm_counter;";
    send(warmQuery, true);
    const auto warmResponse = receive();
    UNIT_ASSERT_VALUES_EQUAL(warmResponse->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(!warmResponse->Get()->Stats.FromCache);
    send(warmQuery, true);
    UNIT_ASSERT(receive()->Get()->Stats.FromCache);
    send("select 6 as warm_counter;", true);
    const auto warmAstResponse = receive();
    UNIT_ASSERT_VALUES_EQUAL(warmAstResponse->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(warmAstResponse->Get()->Stats.FromCache, AstCache);
    const auto warmupHits = AstCache ? 2 : 1;
    UNIT_ASSERT_VALUES_EQUAL(misses->Val(), initialMisses + 2);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + warmupHits);
    UNIT_ASSERT_VALUES_EQUAL(dbMisses->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(dbHits->Val(), warmupHits);
    send(warmQuery);
    UNIT_ASSERT(receive()->Get()->Stats.FromCache);
    UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + warmupHits + 1);
    UNIT_ASSERT_VALUES_EQUAL(dbHits->Val(), warmupHits + 1);

    // Classify each waiter separately, regardless of which one starts the shared compilation.
    for (const bool warmupFirst : {false, true}) {
        const TString query = warmupFirst ? "SELECT 7 AS warmup_first;" : "SELECT 8 AS client_first;";
        const auto beforeMisses = misses->Val();
        const auto beforeDbMisses = dbMisses->Val();
        const auto beforeRequests = requests->Val();
        const auto beforeCompilations = compilations->Val();
        TBlockEvents<TEvKqp::TEvCompileResponse> shared(runtime, [&](const auto& ev) {
            return ev->GetRecipientRewrite() != edge;
        });
        send(query, warmupFirst);
        runtime.WaitFor("shared compilation", [&] { return !shared.empty(); }, TDuration::Seconds(30));
        send(query, !warmupFirst);
        runtime.WaitFor("shared waiter queued", [&] { return queueSize->Val() == 1; }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(requests->Val(), beforeRequests + 2);
        UNIT_ASSERT_VALUES_EQUAL(shared.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeCompilations + 1);
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), beforeMisses);
        shared.Stop().Unblock();
        for (ui32 i = 0; i < 2; ++i) {
            const auto response = receive();
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(!response->Get()->Stats.FromCache);
        }
        UNIT_ASSERT_VALUES_EQUAL(misses->Val(), beforeMisses + 1);
        UNIT_ASSERT_VALUES_EQUAL(dbMisses->Val(), beforeDbMisses + 1);
        UNIT_ASSERT_VALUES_EQUAL(compilations->Val(), beforeCompilations + 1);
        UNIT_ASSERT_VALUES_EQUAL(hits->Val(), initialHits + warmupHits + 1);
        UNIT_ASSERT_VALUES_EQUAL(dbHits->Val(), warmupHits + 1);
    }
}

auto GetYqlStreamIterator(
        TKikimrRunner& kikimr,
        ECollectQueryStatsMode mode,
        const TString& query) {
    NYdb::NScripting::TExecuteYqlRequestSettings settings;
    settings.CollectQueryStats(mode);

    NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

    auto it = client.StreamExecuteYqlScript(query, settings).GetValueSync();
    return it;
}

auto GetScanStreamIterator(
        TKikimrRunner& kikimr,
        ECollectQueryStatsMode mode,
        const TString& query) {
    auto db = kikimr.GetTableClient();

    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(mode);

    auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
    return it;
}

template <typename Iterator>
void MultiTxStatsFullExp(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter) {
    auto kikimr = DefaultKikimrRunner();
    auto it = getIter(kikimr, ECollectQueryStatsMode::Profile, R"(
        SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 150 AND 266 ORDER BY Data LIMIT 4;
    )");
    auto res = CollectStreamResult(it);
    CompareYson(R"([
        [[1];[202u];["Value2"]];
        [[2];[201u];["Value1"]];
        [[3];[203u];["Value3"]]
    ])", res.ResultSetYson);

    UNIT_ASSERT(res.PlanJson);
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);
    auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableRangeScan");
    if (!node.IsDefined()) {
        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-Filter-TableRangeScan");
    }
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 2);
}

Y_UNIT_TEST(MultiTxStatsFullExpYql) {
    MultiTxStatsFullExp<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(MultiTxStatsFullExpScan) {
    MultiTxStatsFullExp<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

template <typename Iterator>
void JoinNoStats(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter) {
    auto kikimr = DefaultKikimrRunner();
    auto it = getIter(kikimr, ECollectQueryStatsMode::None, R"(
        SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
    )");
    auto res = CollectStreamResult(it);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.ResultSetYson, "[[16u]]");

    UNIT_ASSERT(!res.QueryStats);
    UNIT_ASSERT(!res.PlanJson);
}

Y_UNIT_TEST(JoinNoStatsYql) {
    JoinNoStats<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(JoinNoStatsScan) {
    JoinNoStats<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

template <typename Iterator>
TCollectedStreamResult JoinStatsBasic(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter, bool StreamLookupJoin = false) {
    TKikimrSettings settings;
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
    TKikimrRunner kikimr(settings);

    auto it = getIter(kikimr, ECollectQueryStatsMode::Basic, R"(
        SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
    )");
    auto res = CollectStreamResult(it);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(res.ResultSetYson, "[[16u]]");

    UNIT_ASSERT(res.QueryStats);
    return res;
}

Y_UNIT_TEST_TWIN(JoinStatsBasicYql, StreamLookupJoin) {
    auto res = JoinStatsBasic<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator, StreamLookupJoin);

    if (StreamLookupJoin) {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access().size(), 2);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(1).table_access(0).name(), "/Root/KeyValue");
    }
}

Y_UNIT_TEST(JoinStatsBasicScan) {
    auto res = JoinStatsBasic<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);

    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 2);
    if (res.QueryStats->query_phases(0).table_access(0).name() == "/Root/KeyValue") {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).partitions_count(), 8);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 8);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).partitions_count(), 1);
    }

    UNIT_ASSERT(!res.PlanJson);
}

template <typename Iterator>
void MultiTxStatsFull(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getResult) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
    app.MutableTableServiceConfig()->SetEnableSimpleProgramsSinglePartitionOptimization(true);
    app.MutableTableServiceConfig()->SetExtractPredicateParameterListSizeLimit(10000);
    app.MutableTableServiceConfig()->SetEnableSimpleProgramsSinglePartitionOptimizationBroadPrograms(true);
    TKikimrRunner kikimr(app);
    auto it = getResult(kikimr, ECollectQueryStatsMode::Full, R"(
        SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 150 AND 266 ORDER BY Data LIMIT 4;
    )");
    auto res = CollectStreamResult(it);

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(
        res.ResultSetYson,
        R"([[[1];[202u];["Value2"]];[[2];[201u];["Value1"]];[[3];[203u];["Value3"]]])"
    );

    UNIT_ASSERT(res.QueryStats);
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 2);

    UNIT_ASSERT(res.PlanJson);
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);
    Cerr << plan << Endl;
    auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort");
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 1);
}

Y_UNIT_TEST(MultiTxStatsFullYql) {
    MultiTxStatsFull<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(MultiTxStatsFullScan) {
    MultiTxStatsFull<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

Y_UNIT_TEST(DeferredEffects) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    TString planJson;
    NJson::TJsonValue plan;

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/TwoShard`
        SELECT Key + 100u AS Key, Value1 FROM `/Root/TwoShard` WHERE Key in (1,2,3,4,5);
    )", TTxControl::BeginTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // TODO(sk): do proper phase dependency tracking
    //
    // NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    // auto node = FindPlanNodeByKv(plan, "Node Type", "TablePointLookup");
    // UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 1);

    auto tx = result.GetTransaction();
    UNIT_ASSERT(tx);

    auto params = db.GetParamsBuilder()
        .AddParam("$key")
            .Uint32(100)
            .Build()
        .AddParam("$value")
            .String("New")
            .Build()
        .Build();

    result = session.ExecuteDataQuery(R"(

        DECLARE $key AS Uint32;
        DECLARE $value AS String;

        UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
            ($key, $value);
    )", TTxControl::Tx(*tx).CommitTx(), std::move(params), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_VALUES_EQUAL(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 2);

    result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard`;
        UPDATE `/Root/TwoShard` SET Value1 = "XXX" WHERE Key in (3,600);
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_VALUES_EQUAL(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 2);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());

    UNIT_ASSERT(std::atoi(ru->second.c_str()) > 1);
}

Y_UNIT_TEST(DataQueryWithEffects) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/TwoShard`
        SELECT Key + 1u AS Key, Value1 FROM `/Root/TwoShard`;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);

    auto node = FindPlanNodeByKv(plan, "Node Type", "Stage");
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 1);
}

Y_UNIT_TEST(DataQueryMulti) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        SELECT 1;
        SELECT 2;
        SELECT 3;
        SELECT 4;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_EQUAL_C(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 0, result.GetQueryPlan());
}

Y_UNIT_TEST(TxIdInFullStatsPlan) {
    NKikimrConfig::TAppConfig app;
    app.MutableFeatureFlags()->SetEnableTxIdInStats(true);
    TKikimrRunner kikimr(app);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard`;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true), result.GetQueryPlan());

    // Executer TxId is reported per execution phase, so that the plan can be matched with
    // the TxId written by LWTrace probes.
    const auto& plans = plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe();
    UNIT_ASSERT_C(!plans.empty(), result.GetQueryPlan());

    bool txIdFound = false;
    for (const auto& phase : plans) {
        const auto* txId = phase.GetMapSafe().FindPtr("TxId");
        if (txId) {
            UNIT_ASSERT_C(txId->GetUIntegerSafe() > 0, result.GetQueryPlan());
            txIdFound = true;
        }
    }
    UNIT_ASSERT_C(txIdFound, result.GetQueryPlan());
}

Y_UNIT_TEST(NoTxIdWhenFeatureFlagDisabled) {
    // EnableTxIdInStats defaults to false: TxId must not appear in the plan.
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard`;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true), result.GetQueryPlan());

    for (const auto& phase : plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe()) {
        UNIT_ASSERT_C(!phase.GetMapSafe().contains("TxId"), result.GetQueryPlan());
    }
}

Y_UNIT_TEST(NoTxIdForLiteralOnlyQuery) {
    NKikimrConfig::TAppConfig app;
    app.MutableFeatureFlags()->SetEnableTxIdInStats(true);
    TKikimrRunner kikimr(app);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    // Literal-only execution never reaches shards and gets no executer TxId,
    // so nothing must be reported (and nothing must crash).
    auto result = session.ExecuteDataQuery(R"(
        SELECT 1;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true), result.GetQueryPlan());

    for (const auto& phase : plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe()) {
        UNIT_ASSERT_C(!phase.GetMapSafe().contains("TxId"), result.GetQueryPlan());
    }
}

Y_UNIT_TEST(RequestUnitForBadRequestExecute) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(Q_(R"(
            INCORRECT_STMT
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), TExecDataQuerySettings().ReportCostInfo(true))
        .ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT_VALUES_EQUAL(ru->second, "1");
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    UNIT_ASSERT(result.GetConsumedRu() > 0);
}

Y_UNIT_TEST(RequestUnitForBadRequestExplicitPrepare) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.PrepareDataQuery(Q_(R"(
        INCORRECT_STMT
    )"), TPrepareDataQuerySettings().ReportCostInfo(true)).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT_VALUES_EQUAL(ru->second, "1");
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    UNIT_ASSERT(result.GetConsumedRu() > 0);
}

Y_UNIT_TEST(RequestUnitForSuccessExplicitPrepare) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.PrepareDataQuery(Q_(R"(
        SELECT 0; SELECT 1; SELECT 2; SELECT 3; SELECT 4;
        SELECT 5; SELECT 6; SELECT 7; SELECT 8; SELECT 9;
    )"), TPrepareDataQuerySettings().ReportCostInfo(true)).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT(atoi(ru->second.c_str()) > 1);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT(result.GetConsumedRu() > 1);
}

Y_UNIT_TEST(RequestUnitForExecute) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto query = Q1_(R"(
        SELECT COUNT(*) FROM TwoShard;
    )");

    auto settings = TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ReportCostInfo(true);

    // Cached/uncached executions
    for (ui32 i = 0; i < 2; ++i) {
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        Cerr << "Consumed units: " << result.GetConsumedRu() << Endl;
        UNIT_ASSERT(result.GetConsumedRu() > 1);

        auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
        UNIT_ASSERT(ru != result.GetResponseMetadata().end());
        UNIT_ASSERT(atoi(ru->second.c_str()) > 1);
    }
}

Y_UNIT_TEST(LegacySimplifiedPlanCpuWithActualRows) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Filter", "StageGuid": "stage-1",
            "Stats": {
                "Operator": [{"Type": "Filter", "Id": "0", "Rows": {"Sum": 6}}],
                "CpuTimeUs": {"Max": 7000}
            },
            "Operators": [{"Name": "Filter", "Id": "0", "Inputs": []}]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
    const auto filter = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "Filter");
    UNIT_ASSERT_VALUES_EQUAL_C(filter.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, simplifiedPlan);
    AssertCpuValues(filter, 7, 7, simplifiedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanStructuralCpu) {
    {
        const TString plan = R"({
            "Plan": {
                "Node Type": "Query",
                "Plans": [{
                    "Node Type": "Collect", "StageGuid": "collect-stage",
                    "Stats": {"CpuTimeUs": {"Max": 7000}},
                    "Plans": [{
                        "Node Type": "TableFullScan", "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                    }]
                }]
            }
        })";

        const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
        const auto fullScan = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "TableFullScan");
        AssertCpuValues(fullScan, 7, 7, simplifiedPlan);
    }

    {
        const TString plan = R"({
            "Plan": {
                "Node Type": "Query",
                "Plans": [{
                    "Node Type": "Collect", "StageGuid": "collect-stage",
                    "Stats": {"CpuTimeUs": {"Max": 7000}},
                    "Plans": [
                        {
                            "Node Type": "LeftScan", "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                        },
                        {
                            "Node Type": "RightScan", "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                        }
                    ]
                }]
            }
        })";

        const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
        AssertNoCpuValues(simplifiedPlan, simplifiedPlan);
    }

    {
        const TString plan = R"({
            "Plan": {
                "Node Type": "Query",
                "Plans": [{
                    "Node Type": "Collect", "StageGuid": "collect-stage",
                    "Stats": {"CpuTimeUs": {"Max": 7000}},
                    "Plans": [{
                        "Node Type": "TableFullScan", "StageGuid": "scan-stage",
                        "Stats": {"CpuTimeUs": {"Max": 3000}},
                        "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                    }]
                }]
            }
        })";

        const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
        const auto fullScan = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "TableFullScan");
        AssertCpuValues(fullScan, 3, 3, simplifiedPlan);
    }
}

Y_UNIT_TEST(LegacySimplifiedPlanSyntheticLookupCpu) {
    {
        const TString plan = R"({
            "Plan": {
                "Node Type": "Collect", "StageGuid": "lookup-stage",
                "Stats": {"CpuTimeUs": {"Max": 7000}},
                "Plans": [{
                    "Node Type": "TableLookup", "Table": "/Root/t1",
                    "Columns": ["Value"], "LookupKeyColumns": ["Key"],
                    "Plans": []
                }]
            }
        })";

        const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
        const auto lookup = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "TableLookup");
        AssertCpuValues(lookup, 7, 7, simplifiedPlan);
    }

    {
        const TString plan = R"({
            "Plan": {
                "Node Type": "Collect", "StageGuid": "lookup-join-stage",
                "Stats": {"CpuTimeUs": {"Max": 7000}},
                "Plans": [{
                    "Node Type": "TableLookupJoin", "Table": "/Root/t1",
                    "Columns": ["Value"], "LookupKeyColumns": ["Key"]
                }]
            }
        })";

        const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
        const auto lookup = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "Lookup");
        AssertCpuValues(lookup, 7, 7, simplifiedPlan);
    }
}

Y_UNIT_TEST(LegacySimplifiedPlanCpuBoundariesAndCumulativeValue) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Query",
            "Plans": [
                {
                    "Node Type": "Filter", "PlanNodeId": 1, "StageGuid": "filter-stage",
                    "Stats": {"CpuTimeUs": {"Max": 7000}},
                    "Operators": [{"Name": "Filter", "Inputs": [{"ExternalPlanNodeId": 2}]}],
                    "Plans": [{
                        "Node Type": "TableFullScan", "PlanNodeId": 2, "StageGuid": "scan-stage",
                        "Stats": {"CpuTimeUs": {"Max": 3000}},
                        "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                    }]
                },
                {
                    "Node Type": "Precompute", "Subplan Name": "precompute_1",
                    "Plans": [{
                        "Node Type": "TableRangeScan",
                        "Operators": [{"Name": "TableRangeScan", "Inputs": []}]
                    }]
                },
                {
                    "Node Type": "Collect", "StageGuid": "cte-owner-stage",
                    "Stats": {"CpuTimeUs": {"Max": 11000}},
                    "Plans": [{"Node Type": "CTE", "CTE Name": "precompute_1"}]
                }
            ]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);

    const auto filterNode = FindRequiredPlanNodeByKv(simplifiedPlan, "Node Type", "Filter");

    const auto filter = FindRequiredPlanNodeByKv(filterNode, "Name", "Filter");
    AssertCpuValues(filter, 7, 10, simplifiedPlan);

    const auto scan = FindRequiredPlanNodeByKv(filterNode, "Name", "TableFullScan");
    AssertCpuValues(scan, 3, 3, simplifiedPlan);

    const auto precomputeScan = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "TableRangeScan");
    AssertNoCpuValues(precomputeScan, simplifiedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanInheritedCpuStopsAtExternalEdge) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Query",
            "Plans": [{
                "Node Type": "Collect", "StageGuid": "collect-stage",
                "Stats": {"CpuTimeUs": {"Max": 7000}},
                "Plans": [{
                    "Node Type": "Filter", "PlanNodeId": 1,
                    "Operators": [{"Name": "Filter", "Inputs": [{"ExternalPlanNodeId": 2}]}],
                    "Plans": [{
                        "Node Type": "TableFullScan", "PlanNodeId": 2,
                        "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                    }]
                }]
            }]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);

    const auto filterNode = FindRequiredPlanNodeByKv(simplifiedPlan, "Node Type", "Filter");

    const auto filter = FindRequiredPlanNodeByKv(filterNode, "Name", "Filter");
    AssertCpuValues(filter, 7, 7, simplifiedPlan);

    const auto scan = FindRequiredPlanNodeByKv(filterNode, "Name", "TableFullScan");
    AssertNoCpuValues(scan, simplifiedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanDuplicateIndexesUseLastValue) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Query",
            "Plans": [
                {
                    "Node Type": "FilterStage",
                    "Operators": [{"Name": "Filter", "Inputs": [{"ExternalPlanNodeId": 7}]}]
                },
                {
                    "Node Type": "FirstDuplicate", "PlanNodeId": 7,
                    "Operators": [{"Name": "FirstDuplicate", "Inputs": []}]
                },
                {
                    "Node Type": "LastDuplicate", "PlanNodeId": 7,
                    "Operators": [{"Name": "LastDuplicate", "Inputs": []}]
                },
                {
                    "Node Type": "Precompute", "Subplan Name": "precompute_duplicate",
                    "Plans": [{
                        "Node Type": "FirstValue", "Operators": [{"Name": "FirstValue", "Inputs": []}]
                    }]
                },
                {
                    "Node Type": "Precompute", "Subplan Name": "precompute_duplicate",
                    "Plans": [{
                        "Node Type": "LastValue", "Operators": [{"Name": "LastValue", "Inputs": []}]
                    }]
                },
                {
                    "Node Type": "CteOwner",
                    "Plans": [{"Node Type": "CTE", "CTE Name": "precompute_duplicate"}]
                }
            ]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
    const auto filter = FindRequiredPlanNodeByKv(simplifiedPlan, "Node Type", "Filter");
    UNIT_ASSERT_C(FindPlanNodeByKv(filter, "Name", "LastDuplicate").IsDefined(), simplifiedPlan);
    UNIT_ASSERT_C(!FindPlanNodeByKv(filter, "Name", "FirstDuplicate").IsDefined(), simplifiedPlan);

    const auto cteOwner = FindRequiredPlanNodeByKv(simplifiedPlan, "Node Type", "CteOwner");
    UNIT_ASSERT_C(FindPlanNodeByKv(cteOwner, "Name", "LastValue").IsDefined(), simplifiedPlan);
    UNIT_ASSERT_C(!FindPlanNodeByKv(cteOwner, "Name", "FirstValue").IsDefined(), simplifiedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanMultiOperatorCpuOwner) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Stage",
            "Stats": {"CpuTimeUs": {"Max": 7000}},
            "Operators": [
                {"Name": "Filter", "Inputs": [{"InternalOperatorId": 1}]},
                {"Name": "Aggregate", "Inputs": []}
            ]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
    const auto filter = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "Filter");
    AssertCpuValues(filter, 7, 7, simplifiedPlan);

    const auto aggregate = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "Aggregate");
    AssertNoCpuValues(aggregate, simplifiedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanCpuAbsentAndScriptPlan) {
    const TString plan = R"({
        "Plan": {
            "Node Type": "Query",
            "Plans": [{
                "Node Type": "Collect",
                "Plans": [{
                    "Node Type": "ScanStage",
                    "Stats": {"OutputRows": {"Sum": 6}, "OutputBytes": {"Sum": 48}, "Tasks": 2},
                    "Operators": [{"Name": "TableFullScan", "Inputs": []}]
                }]
            }]
        }
    })";

    const auto simplifiedPlan = GetLegacySimplifiedPlan(plan);
    AssertNoCpuValues(simplifiedPlan, simplifiedPlan);

    const auto scan = FindRequiredPlanNodeByKv(simplifiedPlan, "Name", "TableFullScan");
    UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-Rows").GetIntegerSafe(), 6, simplifiedPlan);
    UNIT_ASSERT_VALUES_EQUAL_C(scan.GetMapSafe().at("A-Size").GetDoubleSafe(), 48, simplifiedPlan);

    const TVector<const TString> queryPlans = {plan};
    const auto serializedScriptPlan = SerializeScriptPlan(queryPlans);
    NJson::TJsonValue scriptPlan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(serializedScriptPlan, &scriptPlan, true), serializedScriptPlan);
    AssertNoCpuValues(scriptPlan, scriptPlan);

    const auto& scriptQuery = scriptPlan.GetMapSafe().at("queries").GetArraySafe().front();
    const auto& scriptSimplifiedPlan = scriptQuery.GetMapSafe().at("SimplifiedPlan");
    const auto scriptScan = FindRequiredPlanNodeByKv(scriptSimplifiedPlan, "Name", "TableFullScan");
    UNIT_ASSERT_VALUES_EQUAL_C(scriptScan.GetMapSafe().at("A-Rows").GetIntegerSafe(), 6, scriptPlan);
    UNIT_ASSERT_VALUES_EQUAL_C(scriptScan.GetMapSafe().at("A-Size").GetDoubleSafe(), 48, scriptPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanTableFullScanActualStats) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableNewRBO(false);

    TKikimrRunner kikimr{TKikimrSettings(app)};
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    const auto execute = [&](const TString& query) {
        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx(),
            settings
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        UNIT_ASSERT_C(NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true), result.GetQueryPlan());
        return plan.GetMapSafe().at("SimplifiedPlan");
    };

    const auto fullScanPlan = execute(R"(
        SELECT Key, Value1 FROM `/Root/TwoShard`;
    )");
    const auto fullScan = FindPlanNodeByKv(fullScanPlan, "Name", "TableFullScan");
    UNIT_ASSERT_C(fullScan.IsDefined(), fullScanPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().contains("A-Rows"), fullScanPlan);
    UNIT_ASSERT_VALUES_EQUAL_C(fullScan.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, fullScanPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().contains("A-Size"), fullScanPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().at("A-Size").GetDoubleSafe() > 0, fullScanPlan);
    const auto cpuValues = FindPlanNodes(fullScanPlan, "A-Cpu");
    UNIT_ASSERT_C(!cpuValues.empty(), fullScanPlan);
    for (const auto& cpu : cpuValues) {
        UNIT_ASSERT_C(cpu.GetDoubleSafe() >= 0, fullScanPlan);
    }

    const auto limitedPlan = execute(R"(
        SELECT Key, Value1 FROM `/Root/TwoShard` LIMIT 3;
    )");
    const auto limitNode = FindPlanNodeByKv(limitedPlan, "Node Type", "Limit");
    UNIT_ASSERT_C(limitNode.IsDefined(), limitedPlan);

    const auto limit = FindPlanNodeByKv(limitNode, "Name", "Limit");
    UNIT_ASSERT_C(limit.IsDefined(), limitedPlan);
    UNIT_ASSERT_C(limit.GetMapSafe().contains("A-Rows"), limitedPlan);
    UNIT_ASSERT_VALUES_EQUAL_C(limit.GetMapSafe().at("A-Rows").GetDoubleSafe(), 3, limitedPlan);

    const auto limitedScan = FindPlanNodeByKv(limitNode, "Name", "TableFullScan");
    UNIT_ASSERT_C(limitedScan.IsDefined(), limitedPlan);
    UNIT_ASSERT_C(limitedScan.GetMapSafe().contains("A-Rows"), limitedPlan);
    UNIT_ASSERT_C(limitedScan.GetMapSafe().at("A-Rows").GetDoubleSafe() > 0, limitedPlan);
    UNIT_ASSERT_C(limitedScan.GetMapSafe().contains("A-Size"), limitedPlan);
    UNIT_ASSERT_C(limitedScan.GetMapSafe().at("A-Size").GetDoubleSafe() > 0, limitedPlan);
}

Y_UNIT_TEST(LegacySimplifiedPlanQueryServiceTableFullScanActualStats) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableNewRBO(false);

    TKikimrRunner kikimr{TKikimrSettings(app)};
    auto client = kikimr.GetQueryClient();
    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .StatsMode(NYdb::NQuery::EStatsMode::Full);

    auto result = client.ExecuteQuery(R"(
        SELECT Key, Value1 FROM `/Root/TwoShard`;
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT(result.GetStats());
    UNIT_ASSERT(result.GetStats()->GetPlan());

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true),
        *result.GetStats()->GetPlan());
    const auto simplifiedPlan = plan.GetMapSafe().at("SimplifiedPlan");

    const auto fullScan = FindPlanNodeByKv(simplifiedPlan, "Name", "TableFullScan");
    UNIT_ASSERT_C(fullScan.IsDefined(), simplifiedPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().contains("A-Rows"), simplifiedPlan);
    UNIT_ASSERT_VALUES_EQUAL_C(fullScan.GetMapSafe().at("A-Rows").GetDoubleSafe(), 6, simplifiedPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().contains("A-Size"), simplifiedPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().at("A-Size").GetDoubleSafe() > 0, simplifiedPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().contains("A-Cpu"), simplifiedPlan);
    UNIT_ASSERT_C(fullScan.GetMapSafe().at("A-Cpu").GetDoubleSafe() >= 0, simplifiedPlan);
}

// Per-stage per-node task distribution: Stats.Nodes = [{NodeId, Tasks, Finished}] in FULL mode.
// Literal phases carry no node info, so only stages that have Nodes are checked against their totals.
Y_UNIT_TEST(StageNodesFull) {
    TKikimrRunner kikimr(TKikimrSettings().SetNodeCount(2));
    auto client = kikimr.GetQueryClient();
    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .StatsMode(NYdb::NQuery::EStatsMode::Full);

    auto result = client.ExecuteQuery(R"(
        SELECT COUNT(*) FROM `/Root/EightShard`;
    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT(result.GetStats());
    UNIT_ASSERT(result.GetStats()->GetPlan());

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true), *result.GetStats()->GetPlan());

    auto* runtime = kikimr.GetTestServer().GetRuntime();
    std::set<ui64> clusterNodeIds;
    for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
        clusterNodeIds.insert(runtime->GetNodeId(i));
    }

    ui32 stagesWithNodes = 0;
    std::function<void(const NJson::TJsonValue&)> checkStages = [&](const NJson::TJsonValue& node) {
        if (node.IsMap()) {
            if (auto* stats = node.GetMapSafe().FindPtr("Stats"); stats && stats->IsMap() && stats->Has("Nodes")) {
                ++stagesWithNodes;
                ui64 tasks = 0;
                ui64 finished = 0;
                ui64 lastNodeId = 0;
                for (const auto& nodeStats : stats->GetMapSafe().at("Nodes").GetArraySafe()) {
                    auto nodeId = nodeStats.GetMapSafe().at("NodeId").GetUIntegerSafe();
                    auto nodeTasks = nodeStats.GetMapSafe().at("Tasks").GetUIntegerSafe();
                    auto nodeFinished = nodeStats.GetMapSafe().at("Finished").GetUIntegerSafe();
                    UNIT_ASSERT_C(clusterNodeIds.contains(nodeId), plan);
                    UNIT_ASSERT_C(nodeId > lastNodeId, plan);
                    UNIT_ASSERT_C(nodeTasks > 0, plan);
                    UNIT_ASSERT_C(nodeFinished <= nodeTasks, plan);
                    lastNodeId = nodeId;
                    tasks += nodeTasks;
                    finished += nodeFinished;
                }
                UNIT_ASSERT_VALUES_EQUAL_C(tasks, stats->GetMapSafe().at("Tasks").GetUIntegerSafe(), plan);
                UNIT_ASSERT_VALUES_EQUAL_C(finished, stats->GetMapSafe().at("FinishedTasks").GetUIntegerSafe(), plan);
            }
            for (const auto& [_, child] : node.GetMapSafe()) {
                checkStages(child);
            }
        } else if (node.IsArray()) {
            for (const auto& child : node.GetArraySafe()) {
                checkStages(child);
            }
        }
    };
    checkStages(plan);
    UNIT_ASSERT_C(stagesWithNodes > 0, plan);
}

Y_UNIT_TEST(StatsProfile) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto result = session.ExecuteDataQuery(R"(
        SELECT COUNT(*) FROM TwoShard;
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetQueryPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);

    auto node1 = FindPlanNodeByKv(plan, "Node Type", "ResultSet");
    UNIT_ASSERT_GE(node1.GetMap().at("Nodes").GetArraySafe().size(), 1);
}

// The per node memory history of a profiled query carries what the query holds on the node (Memory + ExternalMemory
// via the resource manager), reported by the query quota manager of the node service: at least the start prepay of
// the tasks and the channels. A scan query never runs its tasks locally in the executer, they go to the node service
Y_UNIT_TEST(NodeMemQueryAllocatedProfile) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableChannelMemoryTracking(true);
    TKikimrRunner kikimr{TKikimrSettings(app)};

    auto it = GetScanStreamIterator(kikimr, ECollectQueryStatsMode::Profile, R"(
        SELECT COUNT(*) FROM `/Root/EightShard`;
    )");
    auto res = CollectStreamResult(it);
    UNIT_ASSERT(res.PlanJson);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);

    ui32 histories = 0;
    std::function<void(const NJson::TJsonValue&)> check = [&](const NJson::TJsonValue& value) {
        if (value.IsMap()) {
            for (const auto& [key, child] : value.GetMapSafe()) {
                if (key == "GlobalMemoryUsageMB") {
                    const auto& times = child.GetMapSafe().at("TimeMs").GetArraySafe();
                    const auto& allocated = child.GetMapSafe().at("MemQueryAllocated").GetArraySafe();
                    UNIT_ASSERT_VALUES_EQUAL(allocated.size(), times.size());
                    ui64 maxAllocated = 0;
                    for (const auto& mb : allocated) {
                        maxAllocated = std::max<ui64>(maxAllocated, mb.GetUIntegerSafe());
                    }
                    UNIT_ASSERT_GE_C(maxAllocated, 1, *res.PlanJson);
                    ++histories;
                } else {
                    check(child);
                }
            }
        } else if (value.IsArray()) {
            for (const auto& child : value.GetArraySafe()) {
                check(child);
            }
        }
    };
    check(plan);
    UNIT_ASSERT_GT_C(histories, 0, *res.PlanJson);
}

Y_UNIT_TEST_TWIN(StreamLookupStats, StreamLookupJoin) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);

    TKikimrRunner kikimr{ TKikimrSettings(app) };
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        $keys = SELECT Key FROM `/Root/KeyValue`;
        SELECT * FROM `/Root/TwoShard` WHERE Key in $keys;
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetQueryPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    auto streamLookup = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
    UNIT_ASSERT(streamLookup.IsDefined());

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    if (StreamLookupJoin) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 1);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
    }

    AssertTableStats(result, "/Root/TwoShard", {
        .ExpectedReads = 2,
    });
}

Y_UNIT_TEST(SelfJoin) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);

    TKikimrRunner kikimr{ TKikimrSettings(app) };
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        SELECT a.Key FROM `/Root/TwoShard` AS a INNER JOIN `/Root/TwoShard` AS b ON a.Key = b.Key;
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetQueryPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 2);
    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 4); // TODO: fix it

    AssertTableStats(result, "/Root/TwoShard", {
        .ExpectedReads = 12,
    });
}

Y_UNIT_TEST(SysViewClientLost) {
    TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false));

    auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
    auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

    kikimr.RunCall( [&] {
        CreateLargeTable(kikimr, 500000, 10, 100, 5000, 1);
        return true;
    });

    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    ui32 updateCount = 0;
    auto grab = [&updateCount](TAutoPtr<IEventHandle>& ev) -> auto {
        if (ev->GetTypeRewrite() == NSysView::TEvSysView::TEvCollectQueryStats::EventType) {
            ++updateCount;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };

    runtime.SetObserverFunc(grab);

    TStringStream timeoutedRequestStream;
    timeoutedRequestStream << R"(
        SELECT COUNT(*) FROM `/Root/LargeTable` WHERE SUBSTRING(DataText, 50, 5) = "22222";
    )";
    TString timeoutedRequest = timeoutedRequestStream.Str();

    auto settings = TStreamExecScanQuerySettings();
    settings.ClientTimeout(TDuration::MilliSeconds(50));
    auto resultFuture = kikimr.RunInThreadPool([&]{
        return db.StreamExecuteScanQuery(timeoutedRequest).GetValueSync();});

    TDispatchOptions opts;
    opts.FinalEvents.emplace_back([&updateCount](IEventHandle&) {
        return updateCount > 0;
    });
    runtime.DispatchEvents(opts);

    auto result = runtime.WaitFuture(resultFuture);
    UNIT_ASSERT_VALUES_EQUAL_C(updateCount, 1, "updated views more than once: " << updateCount);
}

Y_UNIT_TEST(SysViewCancelled) {
    TKikimrRunner kikimr;
    CreateLargeTable(kikimr, 500000, 10, 100, 5000, 1);

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {
        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 rowsCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    rowsCount++;
                }
            }
        }
        UNIT_ASSERT(rowsCount == 1);
    }

    TStringStream cancelledRequest;
    cancelledRequest << "SELECT COUNT(*) FROM `/Root/LargeTable` WHERE SUBSTRING(DataText, 50, 5) = \"33333\"";
    auto prepareResult = session.PrepareDataQuery(cancelledRequest.Str()).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), NYdb::EStatus::SUCCESS, prepareResult.GetIssues().ToString());
    auto dataQuery = prepareResult.GetQuery();

    auto settings = TExecDataQuerySettings();
    settings.CancelAfter(TDuration::MilliSeconds(100));

    auto result = dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync();

    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(),  NYdb::EStatus::CANCELLED);

    {
        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 queryCount = 0;
        ui64 rowsCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    if (*value == cancelledRequest.Str()) {
                        queryCount++;
                    }
                    rowsCount++;
                }
            }
        }

        UNIT_ASSERT(queryCount == 1);
        UNIT_ASSERT(rowsCount == 3);
    }
}

Y_UNIT_TEST(OneShardLocalExec) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

    UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 1);
    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 2);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, "1");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 3);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 4);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, "1");
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 5);
    }
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), 0);
}

Y_UNIT_TEST(OneShardNonLocalExec) {
    TKikimrRunner kikimr(TKikimrSettings().SetNodeCount(2));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto firstNodeId = kikimr.GetTestServer().GetRuntime()->GetFirstNodeId();
    Cerr << "OneShardNonLocalExec: firstNodeId=" << firstNodeId
         << " nodeCount=" << kikimr.GetTestServer().GetRuntime()->GetNodeCount() << Endl;

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

    auto expectedTotalSingleNodeReqCount = counters.TotalSingleNodeReqCount->Val();
    auto expectedNonLocalSingleNodeReqCount = counters.NonLocalSingleNodeReqCount->Val();

    auto drainNode = [runtime = kikimr.GetTestServer().GetRuntime()](size_t nodeId, bool undrain = false) {
        Cerr << "drainNode: nodeId=" << nodeId << " undrain=" << undrain << Endl;
        auto sender = runtime->AllocateEdgeActor();
        IEventBase* ev = nullptr;
        if (undrain) {
            ev = new TEvHive::TEvSetDown(nodeId, false);
        } else {
            ev = new TEvHive::TEvDrainNode(nodeId);
        }
        runtime->SendToPipe(72057594037968897, sender, ev, 0, GetPipeConfigWithRetries());
        if (undrain) {
            TAutoPtr<IEventHandle> handle;
            runtime->GrabEdgeEventRethrow<TEvHive::TEvSetDownReply>(handle, TDuration::Seconds(30));
            Cerr << "drainNode: undrain completed" << Endl;
        } else {
            TAutoPtr<IEventHandle> handle;
            auto drainResponse = runtime->GrabEdgeEventRethrow<TEvHive::TEvDrainNodeResult>(handle, TDuration::Seconds(30));
            Cerr << "drainNode: completed, status=" << (drainResponse ? static_cast<int>(drainResponse->Record.GetStatus()) : -1)
                 << " movements=" << (drainResponse ? drainResponse->Record.GetMovements() : -1) << Endl;
        }
    };

    auto waitTablets = [&session](size_t nodeId) mutable {
        Cerr << "waitTablets: waiting for all tablets on nodeId=" << nodeId << Endl;
        TDescribeTableSettings describeTableSettings =
            TDescribeTableSettings()
                .WithTableStatistics(true)
                .WithPartitionStatistics(true)
                .WithShardNodesInfo(true);

        bool done = false;
        for (int i = 0; i < 5; i++) {
            std::unordered_set<ui32> nodeIds;
            auto res = session.DescribeTable("Root/EightShard", describeTableSettings)
                .ExtractValueSync();

            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionsCount(), 8);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionStats().size(), 8);
            for (const auto& s : res.GetTableDescription().GetPartitionStats()) {
                nodeIds.emplace(s.LeaderNodeId);
            }
            Cerr << "waitTablets: attempt " << i << ", tablet leader nodes: {";
            for (auto it = nodeIds.begin(); it != nodeIds.end(); ++it) {
                if (it != nodeIds.begin()) Cerr << ", ";
                Cerr << *it;
            }
            Cerr << "}, expecting nodeId=" << nodeId << Endl;
            if (nodeIds.size() == 1 && *nodeIds.begin() == nodeId) {
                done = true;
                break;
            }
            Sleep(TDuration::Seconds(5));
        }
        UNIT_ASSERT_C(done, "unable to wait tablets move on specific node");
    };

    // Move all tablets on the node2, we have a grpc connection to node 1
    // so all sessions will be created on the node 1
    drainNode(firstNodeId);
    waitTablets(firstNodeId + 1);

    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }

    expectedNonLocalSingleNodeReqCount += 6;
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), expectedNonLocalSingleNodeReqCount);

    // Now resume node 1 and move all tablets on the node1
    // so all tablets will be on the same node with session
    drainNode(firstNodeId, true);
    drainNode(firstNodeId + 1);
    waitTablets(firstNodeId);

    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    // All executions are local - same value of counter
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), expectedNonLocalSingleNodeReqCount);
}

Y_UNIT_TEST_TWIN(CreateTableAsStats, IsOlap) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableMoveColumnTable(true);
    auto serverSettings = TKikimrSettings()
        .SetFeatureFlags(featureFlags)
        .SetWithSampleTables(false)
        .SetEnableTempTables(true);
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(false);
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(false);
    TKikimrRunner kikimr(serverSettings);
    auto client = kikimr.GetQueryClient();

    {
        auto result = client.ExecuteQuery(Sprintf(R"(
            CREATE TABLE `/Root/Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            ) WITH (STORE=%s);
        )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = client.ExecuteQuery( R"(
            UPSERT INTO `/Root/Source` (Col1, Col2) VALUES (1, 1), (2, 2);
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .StatsMode(NYdb::NQuery::EStatsMode::Full);

    {
        auto result = client.ExecuteQuery(Sprintf(R"(
            CREATE TABLE `/Root/Destination` (
                PRIMARY KEY (Col1)
            )
            WITH (STORE=%s)
            AS SELECT * FROM `/Root/Source`;
        )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSets().empty());

        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetPlan());

        Cerr << "PLAN::" << *result.GetStats()->GetPlan() << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));

        auto sink = FindPlanNodeByKv(
            plan,
            "Name",
            "FillTable"
        );

        UNIT_ASSERT(sink.IsDefined());

        UNIT_ASSERT_VALUES_EQUAL(sink["SinkType"], "KqpTableSink");
        UNIT_ASSERT_VALUES_EQUAL(sink["Path"], "/Root/Destination");
        UNIT_ASSERT_VALUES_EQUAL(sink["Table"], "Destination");

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).reads().rows(), 2);

        if (IsOlap) {
            // size of serialized may be a little different (because of arrow)
            UNIT_ASSERT_GE(stats.query_phases(0).table_access(0).updates().bytes(), 400);
            UNIT_ASSERT_LE(stats.query_phases(0).table_access(0).updates().bytes(), 500);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).reads().bytes(), 40);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).partitions_count(), 0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 24);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).reads().bytes(), 24);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).partitions_count(), 1);
        }
    }

    {
        auto result = client.ExecuteQuery( R"(
            $cnt = SELECT COUNT(*) FROM `/Root/Destination`;
            SELECT Ensure($cnt, $cnt == 2, "fail");
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_TWIN(UpsertWithReturningStats, UseStreamIndex) {
    auto serverSettings = TKikimrSettings();
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(UseStreamIndex);
    TKikimrRunner kikimr(serverSettings);
    auto client = kikimr.GetQueryClient();

    {
        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/ReturningDst` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/ReturningSrc` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/ReturningSrc` (Key, Value) VALUES (1, "a"), (2, "b"), (3, "c");
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto run = RunReturningQuery(client, R"(
            UPSERT INTO `/Root/ReturningDst`
            SELECT * FROM `/Root/ReturningSrc`
            RETURNING *;
        )", 3);

        AssertReturningSinkNode(run.Plan, UseStreamIndex);
        AssertSingleOperatorName(run.Plan, "Upsert");
        UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(run.Plan, "Node Type", "TableFullScan"), 1);

        // Both the source read and the destination write stats must be collected.
        AssertTableStats(run.Result, "/Root/ReturningDst", { .ExpectedUpdates = 3 });
        AssertTableStats(run.Result, "/Root/ReturningSrc", { .ExpectedReads = 3 });
    }
}

Y_UNIT_TEST_TWIN(ReturningStatsModes, UseStreamIndex) {
    auto serverSettings = TKikimrSettings();
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(UseStreamIndex);
    TKikimrRunner kikimr(serverSettings);
    auto client = kikimr.GetQueryClient();

    {
        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/ReturningDst` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/ReturningSrc` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/ReturningSrc` (Key, Value) VALUES (1, "a"), (2, "b"), (3, "c");
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto run = RunReturningQuery(client, R"(
            INSERT INTO `/Root/ReturningDst`
            SELECT * FROM `/Root/ReturningSrc`
            RETURNING *;
        )", 3);
        AssertReturningSinkNode(run.Plan, UseStreamIndex);
        AssertSingleOperatorName(run.Plan, "Insert");
        AssertTableStats(run.Result, "/Root/ReturningDst", { .ExpectedUpdates = 3 });
        AssertTableStats(run.Result, "/Root/ReturningSrc", { .ExpectedReads = 3 });
    }

    {
        auto run = RunReturningQuery(client, R"(
            REPLACE INTO `/Root/ReturningDst`
            SELECT * FROM `/Root/ReturningSrc`
            RETURNING *;
        )", 3);
        AssertReturningSinkNode(run.Plan, UseStreamIndex);
        AssertSingleOperatorName(run.Plan, "Replace");
        AssertTableStats(run.Result, "/Root/ReturningDst", { .ExpectedUpdates = 3 });
        AssertTableStats(run.Result, "/Root/ReturningSrc", { .ExpectedReads = 3 });
    }

    {
        auto run = RunReturningQuery(client, R"(
            UPDATE `/Root/ReturningSrc` SET Value = "x" WHERE Key <= 3 RETURNING *;
        )", 3);
        AssertReturningSinkNode(run.Plan, UseStreamIndex);
        AssertSingleOperatorName(run.Plan, "Upsert");
        // The read-before-write of the UPDATE is collected too.
        AssertTableStats(run.Result, "/Root/ReturningSrc", { .ExpectedReads = 3, .ExpectedUpdates = 3 });
    }

    {
        auto run = RunReturningQuery(client, R"(
            DELETE FROM `/Root/ReturningSrc` WHERE Key <= 3 RETURNING *;
        )", 3);
        AssertReturningSinkNode(run.Plan, UseStreamIndex);
        AssertSingleOperatorName(run.Plan, "Delete");
        // Delete reads each row (read-before-write plus the returned row).
        AssertTableStats(run.Result, "/Root/ReturningSrc", { .ExpectedReads = 6, .ExpectedDeletes = 3 });
    }
}

Y_UNIT_TEST_TWIN(ReturningStatsWithIndex, UseStreamIndex) {
    auto serverSettings = TKikimrSettings();
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(UseStreamIndex);
    TKikimrRunner kikimr(serverSettings);
    auto client = kikimr.GetQueryClient();
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session);

    {
        auto run = RunReturningQuery(client, R"(
            UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
                (10, 10, "A"), (11, 11, "B"), (12, 12, "C")
                RETURNING *;
        )", 3);

        // The plan shape for indexed writes is serializer-dependent (stream-write
        // uses the RBO serializer, legacy uses "Sink" nodes), so only the resulting
        // write stats and the presence of an upsert write are asserted here.
        UNIT_ASSERT(CountPlanNodesByKv(run.Plan, "Name", "Upsert") >= 1);

        // Both the main table write and the secondary index write must be collected.
        AssertTableStats(run.Result, "/Root/SecondaryKeys", { .ExpectedReads = 0, .ExpectedUpdates = 3 });
        AssertTableStats(run.Result, "/Root/SecondaryKeys/Index/indexImplTable", { .ExpectedReads = 0, .ExpectedUpdates = 3 });
    }

    {
        auto run = RunReturningQuery(client, R"(
            UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
                (10, 10, "A"), (11, 11, "B"), (12, 20, "C")
                RETURNING *;
        )", 3);

        // The plan shape for indexed writes is serializer-dependent (stream-write
        // uses the RBO serializer, legacy uses "Sink" nodes), so only the resulting
        // write stats and the presence of an upsert write are asserted here.
        UNIT_ASSERT(CountPlanNodesByKv(run.Plan, "Name", "Upsert") >= 1);

        // Both the main table write and the secondary index write must be collected.
        AssertTableStats(run.Result, "/Root/SecondaryKeys", { .ExpectedReads = 3, .ExpectedUpdates = 3 });
        AssertTableStats(run.Result, "/Root/SecondaryKeys/Index/indexImplTable", { .ExpectedReads = 0, .ExpectedUpdates = 1, .ExpectedDeletes = 1 });
    }
}

Y_UNIT_TEST_TWIN(ReturningStatsNeedsLookup, UseStreamIndex) {
    auto serverSettings = TKikimrSettings();
    serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(UseStreamIndex);
    TKikimrRunner kikimr(serverSettings);
    auto client = kikimr.GetQueryClient();

    {
        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/ReturningExtra` (
                Key Uint64 NOT NULL,
                Value String,
                Extra String,
                PRIMARY KEY (Key)
            );
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/ReturningExtra` (Key, Value, Extra) VALUES (1, "a", "old_extra");
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    // Extra is not written by the UPDATE, so RETURNING it requires reading the row.
    auto run = RunReturningQuery(client, R"(
        UPDATE `/Root/ReturningExtra` SET Value = "b" WHERE Key = 1 RETURNING Key, Value, Extra;
    )", 1);

    AssertReturningSinkNode(run.Plan, UseStreamIndex);
    AssertSingleOperatorName(run.Plan, "Upsert");

    NYdb::TResultSetParser parser(run.Result.GetResultSets()[0]);
    UNIT_ASSERT(parser.TryNextRow());
    auto optionalValue = parser.ColumnParser(1).GetOptionalString();
    UNIT_ASSERT(optionalValue);
    UNIT_ASSERT_VALUES_EQUAL(*optionalValue, "b");
    auto optionalExtra = parser.ColumnParser(2).GetOptionalString();
    UNIT_ASSERT(optionalExtra);
    UNIT_ASSERT_VALUES_EQUAL(*optionalExtra, "old_extra");

    // RETURNING a column that is not written ("Extra") requires reading the row.
    // In both modes this adds a lookup read next to the single-row write.
    AssertTableStats(run.Result, "/Root/ReturningExtra", { .ExpectedUpdates = 1 });
    AssertTableStats(run.Result, "/Root/ReturningExtra", { .ExpectedReads = 2 });
}

} // suite

} // namespace NKqp
} // namespace NKikimr
