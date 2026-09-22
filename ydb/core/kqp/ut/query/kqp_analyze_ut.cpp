#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/statistics/aggregator/analyze_actor.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <atomic>
#include <thread>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NTable;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpAnalyze) {

using namespace NStat;

ui64 FailedNewRboCompilations(TTestActorRuntime& runtime) {
    ui64 result = 0;
    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        TKqpCounters counters(runtime.GetAppData(node).Counters);
        result += counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Failed")->Val();
    }
    return result;
}

Y_UNIT_TEST_TWIN(AnalyzeScansWithNewRboWithoutFallback, PerShard) {
    TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
        auto* tableService = settings.AppConfig->MutableTableServiceConfig();
        tableService->SetEnableNewRBO(true);
        tableService->SetEnableFallbackToYqlOptimizer(false);
    });
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");
    const auto table = PrepareMultiColumnAllTypesTable(env, "Database", "nested/Table", true);
    WaitForSchemeShardStatsUpdate(runtime, table.PathId.OwnerId, true);

    size_t scanRequests = 0;
    auto observer = runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
        if (ev->Get()->GetRequestType() == NRequestTypes::Analyze) {
            UNIT_ASSERT(ev->Get()->IsInternalCall());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetQuery().Contains("WITH TabletId"), PerShard);
            ++scanRequests;
        }
    });
    const auto failedBefore = FailedNewRboCompilations(runtime);
    // Exercise generated scans with fallback disabled. Persistence uses a
    // separate query that still needs the normal optimizer fallback.
    TAnalyzeActor::TConfig config;
    config.ColumnTableWholeTableScanMaxBytes = PerShard ? 0 : (1ULL << 30);
    config.TableBytesSize = 1; // The fixture fits in the whole-table threshold.
    const auto edge = runtime.AllocateEdgeActor(1);
    runtime.Register(new TAnalyzeActor(edge, "newRbo", "/Root/Database", table.PathId, {}, config), 1);
    bool haveSummary = false;
    bool haveSketch = false;
    bool haveHistogram = false;
    while (true) {
        auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeActorResult>(edge);
        UNIT_ASSERT(response);
        const auto& result = *response->Get();
        UNIT_ASSERT_C(result.Status == TEvStatistics::TEvAnalyzeActorResult::EStatus::Success,
            result.Issues.ToString());
        for (const auto& item : result.Statistics) {
            if (item.Type == EStatType::TABLE_SUMMARY) {
                NKikimrStat::TTableSummaryStatistics summary;
                UNIT_ASSERT(summary.ParseFromString(item.Data));
                UNIT_ASSERT_VALUES_EQUAL(summary.GetRowCount(), ColumnTableRowsNumber);
                haveSummary = true;
            }
            const auto* tags = item.ColumnTags.AsMulti();
            if (tags && *tags == std::vector<ui32>{2, 3}) {
                UNIT_ASSERT(!item.Data.empty());
                haveSketch |= item.Type == EStatType::COUNT_MIN_SKETCH;
                haveHistogram |= item.Type == EStatType::EQ_HEIGHT_HISTOGRAM;
            }
        }
        if (result.Final) {
            break;
        }
    }
    UNIT_ASSERT(haveSummary);
    UNIT_ASSERT(haveSketch);
    UNIT_ASSERT(haveHistogram);
    UNIT_ASSERT_GT(scanRequests, 0);
    UNIT_ASSERT_VALUES_EQUAL(FailedNewRboCompilations(runtime), failedBefore);
}

Y_UNIT_TEST_TWIN(AnalyzeOptimizerCache, AnalyzeFirst) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    TKikimrRunner kikimr{TKikimrSettings(appConfig).SetWithSampleTables(false)};
    const auto session = kikimr.GetTableClient().CreateSession().GetValueSync();
    UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    const auto service = MakeKqpCompileServiceID(runtime.GetNodeId());
    TIntrusiveConstPtr<NACLib::TUserToken> token = new NACLib::TUserToken("root@builtin", {});
    auto context = MakeIntrusive<TUserRequestContext>("analyze-cache", "/Root", "analyze-cache");
    TKqpCounters counters(runtime.GetAppData().Counters);
    const auto successes = counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Success");
    const auto failures = counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Failed");

    const auto execute = [&](bool internalCall, bool analyzeRequest) {
        auto request = std::make_unique<TEvKqp::TEvQueryRequest>();
        request->Record.SetUserToken(token->GetSerializedToken());
        if (analyzeRequest) {
            request->Record.SetRequestType(TString(NRequestTypes::Analyze));
        }
        auto& query = *request->Record.MutableRequest();
        query.SetDatabase("/Root");
        query.SetSessionId(TString(session.GetSession().GetId()));
        query.SetKeepSession(true);
        query.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        query.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        query.SetQuery("SELECT 1 AS value;");
        query.SetIsInternalCall(internalCall);
        query.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        query.MutableTxControl()->set_commit_tx(true);
        query.MutableQueryCachePolicy()->set_keep_in_cache(true);
        query.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
        runtime.Send(new IEventHandle(MakeKqpProxyID(runtime.GetNodeId()), edge, request.release()));
        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvQueryResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS, record.DebugString());
        const auto& result = record.GetResponse();
        UNIT_ASSERT(!result.GetPreparedQuery().empty());
        UNIT_ASSERT(result.HasQueryStats());
        return std::make_pair(result.GetPreparedQuery(), result.GetQueryStats().GetCompilation().GetFromCache());
    };

    const auto failedBefore = failures->Val();
    // Go through the session actor: a request-type label alone must not select
    // the ANALYZE optimizer, and unrelated internal calls must still use new RBO.
    for (bool internalCall : {true, false}) {
        TString uids[2];
        for (bool analyzeRequest : {AnalyzeFirst, !AnalyzeFirst}) {
            const bool expectCached = !internalCall && analyzeRequest != AnalyzeFirst;
            const ui64 expectedCompilations = !expectCached && !(internalCall && analyzeRequest) ? 1 : 0;
            const auto before = successes->Val();
            const auto [uid, fromCache] = execute(internalCall, analyzeRequest);
            UNIT_ASSERT_VALUES_EQUAL(fromCache, expectCached);
            UNIT_ASSERT_VALUES_EQUAL(successes->Val() - before, expectedCompilations);
            uids[analyzeRequest] = uid;
            const auto cached = execute(internalCall, analyzeRequest);
            UNIT_ASSERT(cached.second);
            UNIT_ASSERT_VALUES_EQUAL(cached.first, uid);
            UNIT_ASSERT_VALUES_EQUAL(successes->Val() - before, expectedCompilations);
        }
        UNIT_ASSERT_VALUES_EQUAL(uids[0] == uids[1], !internalCall);
        if (internalCall) {
            // Recompile the session's cached queries by UID, preserving their
            // optimizer selection without reconstructing IsAnalyze in the test.
            for (bool analyzeRequest : {false, true}) {
                const auto before = successes->Val();
                runtime.Send(new IEventHandle(service, edge, new TEvKqp::TEvRecompileRequest(
                    token, "", uids[analyzeRequest], Nothing(), /*isQueryActionPrepare=*/false,
                    TInstant::Max(), nullptr, std::make_shared<TGUCSettings>(), Nothing(),
                    std::make_shared<std::atomic<bool>>(true), context)));
                auto response = runtime.GrabEdgeEvent<TEvKqp::TEvCompileResponse>(edge, TDuration::Seconds(30));
                UNIT_ASSERT(response && response->Get()->CompileResult);
                const auto& result = response->Get()->CompileResult;
                UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
                UNIT_ASSERT(!response->Get()->Stats.FromCache);
                UNIT_ASSERT(result->Query);
                UNIT_ASSERT_VALUES_EQUAL(result->Query->Settings.IsAnalyze, analyzeRequest);
                UNIT_ASSERT_VALUES_EQUAL(successes->Val() - before, analyzeRequest ? 0 : 1);
            }
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(failures->Val(), failedBefore);
}

Y_UNIT_TEST_TWIN(AnalyzeTable, ColumnStore) {
    TTestEnv env(1, 1, true, [](Tests::TServerSettings& settings) {
        auto* tableService = settings.AppConfig->MutableTableServiceConfig();
        tableService->SetEnableNewRBO(true);
        tableService->SetEnableFallbackToYqlOptimizer(true);
    });

    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    TString createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )", "Root/Database/Table");
    if (ColumnStore) {
        createTable +=
            R"(
                PARTITION BY HASH(Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16
                )
            )";
    }

    auto result = session.ExecuteSchemeQuery(createTable).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 1500; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalString("Hello,world!").Build();

        rows.AddListItem();
            rows.BeginStruct();
                rows.AddMember("Key", key);
                rows.AddMember("Value", value);
            rows.EndStruct();
    }
    rows.EndList();

    result = client.BulkUpsert("Root/Database/Table", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto& runtime = *env.GetServer().GetRuntime();
    const auto failedBefore = FailedNewRboCompilations(runtime);
    result = session.ExecuteSchemeQuery(
        Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "Table")
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    // Statistics-save queries retain the normal new-RBO fallback path.
    UNIT_ASSERT_GT(FailedNewRboCompilations(runtime), failedBefore);

    ui64 saTabletId;
    auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

    CheckCountMinSketch(runtime, pathId, {
        {.Tag = 2, .Probes = {{{"Hello,world!", 1500}}}},
    });
}

Y_UNIT_TEST_TWIN(AnalyzeServerlessTable, ColumnStore) {
    TTestEnv env(1, 1, true);
    CreateDatabase(env, "Shared", 1, true);
    CreateServerlessDatabase(env, "Database", "/Root/Shared");
    const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnStore);

    // Use the serverless database context to exercise cross-domain SA resolution.
    TDriver driver(TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDatabase("/Root/Database")
        .SetDiscoveryMode(EDiscoveryMode::Off));
    TTableClient client(driver);
    auto sessionResult = client.CreateSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();

    auto result = session.ExecuteSchemeQuery("ANALYZE `/Root/Database/Table`").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    ValidateStatistics(*env.GetServer().GetRuntime(), tableInfo.PathId);
    driver.Stop(true);
}

Y_UNIT_TEST(AnalyzeError) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] {
        return client.CreateSession().GetValueSync().GetSession();
    });

    {
        // Create table
        TString createTable = R"(
            CREATE TABLE `Root/Database/Table` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )";

        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery(createTable).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Simulate an ANALYZE error coming from the StatisticsAggregator tablet.
    auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeResponse>(
        [&](TEvStatistics::TEvAnalyzeResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        NYql::TIssue issue("mock issue");
        NYql::IssueToMessage(issue, record.AddIssues());
    });

    {
        // Run ANALYZE and check that the issue is reported.
        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery("ANALYZE `Root/Database/Table`").GetValueSync();
        });
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(
            HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR,
                [](const auto& issue) {
                    return issue.GetMessage() == "mock issue";
                }),
            result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_TWIN(AnalyzeSampling, QueryService) {
    TTestEnv env(1, 1, false);
    CreateDatabase(env, "Database");
    const auto table = PrepareMultiColumnTable(env, "Database", "Table", true);
    TTableClient client(env.GetDriver());
    NQuery::TQueryClient queryClient(env.GetDriver());
    auto session = env.RunInThreadPool([&] { return client.CreateSession().GetValueSync().GetSession(); });
    const auto execute = [&](const TString& query) {
        return env.RunInThreadPool([&]() -> TStatus {
            if (QueryService) {
                return queryClient.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            }
            return session.ExecuteSchemeQuery(query).GetValueSync();
        });
    };
    TVector<double> requestedRates;
    size_t fullRequests = 0;
    auto observer = env.GetServer().GetRuntime()->AddObserver<TEvStatistics::TEvAnalyze>([&](auto& ev) {
        for (const auto& table : ev->Get()->Record.GetTables()) {
            if (table.HasSampleRate()) {
                requestedRates.push_back(table.GetSampleRate());
            } else {
                ++fullRequests;
            }
        }
    });
    for (const TString rate : {"0.5", "(0.5 + 0.5) / 2", "$rate", "Math::Sqrt(0.25)"}) {
        const auto result = execute("$rate = 1.0 / 2; ANALYZE `Root/Database/Table` (Value1) SAMPLE " + rate + ";");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        if (rate == "0.5") {
            const auto stored = ExecuteYqlScriptWithResult(env, TStringBuilder()
                << "SELECT column_tags, data, sampled_data FROM `/Root/Database/.metadata/statistics_v2`"
                << " WHERE owner_id = " << table.PathId.OwnerId << "ul AND local_path_id = " << table.PathId.LocalPathId
                << "ul AND stat_type = " << static_cast<ui32>(EStatType::SIMPLE_COLUMN) << "u;");
            UNIT_ASSERT_VALUES_EQUAL(stored.rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stored.rows(0).items(0).bytes_value(), "2");
            UNIT_ASSERT(stored.rows(0).items(1).has_null_flag_value());
            NKikimrStat::TSampledStatistic payload;
            UNIT_ASSERT(payload.ParseFromString(stored.rows(0).items(2).bytes_value()));
            const auto& metadata = payload.GetSampling();
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetRequestedRate(), 0.5);
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetEligibleUnits(), 4);
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetSelectedUnits(), 2);
            UNIT_ASSERT(metadata.GetSampleRows() > 0 && metadata.GetSampleRows() < ColumnTableRowsNumber);
            NKikimrStat::TSimpleColumnStatistics statistics;
            UNIT_ASSERT(statistics.ParseFromString(payload.GetData()));
            UNIT_ASSERT_VALUES_EQUAL(statistics.GetCount(), metadata.GetSampleRows());
            UNIT_ASSERT(!statistics.HasCountDistinct());
        }
    }
    for (const TString clause : {"", " SAMPLE 1", " SAMPLE 0.5 + 0.5"}) {
        const auto result = execute("ANALYZE `Root/Database/Table`" + clause + ";");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    UNIT_ASSERT_VALUES_EQUAL(requestedRates.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(fullRequests, 3);
    for (size_t i = 0; i < 4; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(requestedRates[i], 0.5);
    }
    for (const TString rate : {"0", "-0.0", "-0.1", "1.5", "1.0000000000000002", "1.0 - 1.0", "0.75 * 2",
            "Double('nan')", "Double('inf')", "-Double('inf')", "1.0 / 0.0"}) {
        const auto result = execute("ANALYZE `Root/Database/Table` SAMPLE " + rate + ";");
        UNIT_ASSERT_C(!result.IsSuccess(), rate);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "ANALYZE SAMPLE rate must be a finite number in (0, 1]");
    }
    for (const TString rate : {"1e999", "1e-999", "NULL", "'0.5'", "TRUE", "[0.5]"}) {
        const auto result = execute("ANALYZE `Root/Database/Table` SAMPLE " + rate + ";");
        UNIT_ASSERT_C(!result.IsSuccess(), rate);
    }
    UNIT_ASSERT_VALUES_EQUAL(requestedRates.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(fullRequests, 3);
}

Y_UNIT_TEST(AnalyzeSamplingServerless) {
    TTestEnv env(1, 1, false);
    CreateDatabase(env, "Shared", 1, true);
    CreateServerlessDatabase(env, "Database", "/Root/Shared");
    PrepareMultiColumnTable(env, "Database", "Table", true);
    TVector<double> requestedRates;
    auto observer = env.GetServer().GetRuntime()->AddObserver<TEvStatistics::TEvAnalyze>([&](auto& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.TablesSize(), 1);
        const auto& table = ev->Get()->Record.GetTables(0);
        UNIT_ASSERT_VALUES_EQUAL(table.ColumnTagsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumnTags(0), 2);
        requestedRates.push_back(table.GetSampleRate());
    });
    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] { return client.CreateSession().GetValueSync().GetSession(); });
    for (const TString rate : {"0.5", "1"}) {
        const auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery("ANALYZE `Root/Database/Table` (Value1) SAMPLE " + rate + ";").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    UNIT_ASSERT_VALUES_EQUAL(requestedRates.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requestedRates[0], 0.5);
    UNIT_ASSERT_VALUES_EQUAL(requestedRates[1], 1.0);
}

Y_UNIT_TEST(AnalyzeSamplingRequiresColumnTable) {
    TTestEnv env(1, 1, false);
    CreateDatabase(env, "Database");
    CreateEmptyTable(env, "Database", "Table", false);
    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] { return client.CreateSession().GetValueSync().GetSession(); });
    const auto execute = [&](const TString& query) {
        return env.RunInThreadPool([&] { return session.ExecuteSchemeQuery(query).GetValueSync(); });
    };
    const auto result = execute("ANALYZE `Root/Database/Table` SAMPLE 0.5;");
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "ANALYZE SAMPLE is supported only for column tables");
    const auto full = execute("ANALYZE `Root/Database/Table` SAMPLE 1;");
    UNIT_ASSERT_C(full.IsSuccess(), full.GetIssues().ToString());
}

Y_UNIT_TEST(RetryPreservesSampleRate) {
    TTestEnv env(1, 1, false);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");
    const auto table = PrepareColumnTable(env, "Database", "Table", 4);
    size_t attempts = 0;
    auto forwards = runtime.AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
        if (ev->Get()->Ev->Type() != TEvStatistics::TEvAnalyze::EventType || ++attempts != 1) {
            return;
        }
        const auto gateway = ev->Sender;
        const auto cache = ev->GetRecipientRewrite();
        ev.Reset();
        runtime.Send(new IEventHandle(gateway, cache,
            new TEvPipeCache::TEvDeliveryProblem(table.SaTabletId, true)),
            gateway.NodeId() - runtime.GetFirstNodeId(), true);
    });
    auto requests = runtime.AddObserver<TEvStatistics::TEvAnalyze>([](auto& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTables(0).GetSampleRate(), 0.5);
    });
    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] { return client.CreateSession().GetValueSync().GetSession(); });
    const auto result = env.RunInThreadPool([&] {
        return session.ExecuteSchemeQuery("ANALYZE `Root/Database/Table` SAMPLE 0.5;").GetValueSync();
    });
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(attempts, 2);
}

} // suite

Y_UNIT_TEST_SUITE(KqpAnalyzeOperations) {

using namespace NStat;

Y_UNIT_TEST(AnalyzeOperationsLifecycle) {
    TTestEnv env(1, 1, true);
    CreateDatabase(env, "Database");

    // Use the same driver for both the session and operation client to ensure database name consistency.
    // DiscoveryMode::Off avoids discovery against the dynamic tenant (which races test setup);
    // requests go directly to the static node's gRPC port, which routes by the database header.
    NYdb::TDriver opDriver(NYdb::TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDatabase("/Root/Database")
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
    NYdb::NOperation::TOperationClient operationClient(opDriver);

    TTableClient tableClient(opDriver);
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    // Empty list before any ANALYZE
    {
        auto result = operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
            result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
    }

    // Create and populate a table in the tenant database
    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Root/Database/AnalyzeTest` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `Root/Database/AnalyzeTest` (Key, Value)
                VALUES (1, "a"), (2, "b"), (3, "c");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Run ANALYZE TABLE (blocking from caller's view)
    {
        auto result = session.ExecuteSchemeQuery(
            "ANALYZE `Root/Database/AnalyzeTest`"
        ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // After ANALYZE completes, the operation is retained as DONE
    NYdb::TOperation::TOperationId opId;
    {
        auto listResult = operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS,
            listResult.GetIssues().ToString());
        UNIT_ASSERT_GE(listResult.GetList().size(), 1);

        const auto& op = listResult.GetList()[0];
        opId = op.Id();
        UNIT_ASSERT_C(op.Ready(), op.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(op.Status().GetStatus(), NYdb::EStatus::SUCCESS);

        const auto& meta = op.Metadata();
        UNIT_ASSERT_VALUES_EQUAL(meta.State, NYdb::NTable::EAnalyzeState::Done);
        UNIT_ASSERT_DOUBLES_EQUAL(meta.Progress, 100.0f, 0.1f);
        UNIT_ASSERT_GE(meta.Paths.size(), 1u);
        UNIT_ASSERT(meta.InProgressPaths.empty());
        UNIT_ASSERT_VALUES_EQUAL(meta.DonePaths.size(), meta.Paths.size());
    }

    // Get by ID matches
    {
        auto getResult = operationClient.Get<NYdb::NTable::TAnalyzeOperation>(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.Status().GetStatus(), NYdb::EStatus::SUCCESS,
            getResult.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.Metadata().State, NYdb::NTable::EAnalyzeState::Done);
        UNIT_ASSERT_DOUBLES_EQUAL(getResult.Metadata().Progress, 100.0f, 0.1f);
    }

    // Cancel of a terminal op is idempotent
    {
        auto cancelResult = operationClient.Cancel(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(cancelResult.GetStatus(), NYdb::EStatus::SUCCESS,
            cancelResult.GetIssues().ToString());
    }

    // Forget removes it from history
    {
        auto forgetResult = operationClient.Forget(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(forgetResult.GetStatus(), NYdb::EStatus::SUCCESS,
            forgetResult.GetIssues().ToString());
    }

    // Now Get returns NOT_FOUND
    {
        auto getAfterForget = operationClient.Get<NYdb::NTable::TAnalyzeOperation>(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getAfterForget.Status().GetStatus(), NYdb::EStatus::NOT_FOUND,
            getAfterForget.Status().GetIssues().ToString());
    }

    opDriver.Stop(true);
}

Y_UNIT_TEST(AnalyzeContinuesOnQueryAbort) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");

    NYdb::TDriver opDriver(NYdb::TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDatabase("/Root/Database")
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
    NYdb::NOperation::TOperationClient operationClient(opDriver);

    TTableClient tableClient(opDriver);
    auto session = env.RunInThreadPool([&]{
        return tableClient.CreateSession().GetValueSync().GetSession();
    });

    {
        auto result = env.RunInThreadPool([&]{
            return session.ExecuteSchemeQuery(R"(
                CREATE TABLE `Root/Database/AnalyzeTest` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    {
        auto result = env.RunInThreadPool([&]{
            return session.ExecuteDataQuery(R"(
                UPSERT INTO `Root/Database/AnalyzeTest` (Key, Value)
                    VALUES (1, "a"), (2, "b"), (3, "c");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // No TEvAnalyzeCancel must reach the SA under the new policy.
    std::atomic<int> cancelCount{0};
    auto cancelObs = runtime.AddObserver<NStat::TEvStatistics::TEvAnalyzeCancel>(
        [&](NStat::TEvStatistics::TEvAnalyzeCancel::TPtr&) {
            cancelCount.fetch_add(1);
        });

    // The instant TAnalyzeActor's TEvAnalyze is observed on its way to the SA,
    // inject TEvAbortExecution back to it (the sender) to simulate a session
    // timeout. TEvAnalyze itself is not blocked; the SA still processes it and
    // runs the traversal to completion in the background.
    std::atomic<bool> aborted{false};
    auto analyzeObs = runtime.AddObserver<NStat::TEvStatistics::TEvAnalyze>(
        [&](NStat::TEvStatistics::TEvAnalyze::TPtr& ev) {
            if (aborted.exchange(true)) {
                return;
            }
            const TActorId target = ev->Sender;
            auto* handle = new IEventHandle(
                target, TActorId(),
                new TEvKqp::TEvAbortExecution(
                    NYql::NDqProto::StatusIds::TIMEOUT,
                    "test injected query timeout", NYql::TIssues{}));
            const ui32 nodeIdx = target.NodeId() - runtime.GetFirstNodeId();
            runtime.Send(handle, nodeIdx, /*viaActorSystem=*/true);
        });

    auto analyzeResult = env.RunInThreadPool([&]{
        return session.ExecuteSchemeQuery(
            "ANALYZE `Root/Database/AnalyzeTest`").GetValueSync();
    });
    UNIT_ASSERT_C(!analyzeResult.IsSuccess(),
        "expected ANALYZE to fail due to injected abort, got: "
            << analyzeResult.GetIssues().ToString());

    // Advance simulated time so the SA receives SchemeShard stats, schedules the
    // traversal, and completes it. With our policy change, no TEvAnalyzeCancel was
    // sent on abort, so the operation must reach STATE_DONE — not STATE_CANCELLED.
    runtime.SimulateSleep(TDuration::Seconds(60));

    auto listResult = env.RunInThreadPool([&]{
        return operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
    });
    UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS,
        listResult.GetIssues().ToString());

    bool foundDone = false;
    for (const auto& op : listResult.GetList()) {
        UNIT_ASSERT_VALUES_UNEQUAL_C(
            op.Metadata().State, NYdb::NTable::EAnalyzeState::Cancelled,
            "operation must not be cancelled by query abort");
        if (op.Metadata().State == NYdb::NTable::EAnalyzeState::Done) {
            foundDone = true;
        }
    }
    UNIT_ASSERT_C(foundDone, "ANALYZE long-running op did not reach DONE after abort");
    UNIT_ASSERT_VALUES_EQUAL_C(cancelCount.load(), 0,
        "TEvAnalyzeCancel must not be sent on query abort");

    opDriver.Stop(true);
}

} // suite KqpAnalyzeOperations

} // namespace NKqp
} // namespace NKikimr
