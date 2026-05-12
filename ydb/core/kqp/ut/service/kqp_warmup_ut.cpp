#include <atomic>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/compile_service/kqp_warmup_compile_actor.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/public/ydb_issue/ydb_issue_message.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    struct TCompileCacheEntry {
        TString QueryId;
        TString Query;
        TString UserSID;
    };

    void GrantPermissions(TKikimrRunner& kikimr, const TString& path, const TVector<TString>& userSids, bool isThreadLocked) {
        auto getDriver = [&] {
            TDriverConfig driverConfig;
            driverConfig
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken("root@builtin");
            return NYdb::TDriver(driverConfig);
        };
        auto driver = isThreadLocked ? kikimr.RunCall(getDriver) : getDriver();

        auto schemeClient = NYdb::NScheme::TSchemeClient(driver);

        for (const auto& userSid : userSids) {
            auto doModify = [&] {
                NYdb::NScheme::TPermissions permissions(userSid + "@builtin",
                    {"ydb.generic.read", "ydb.generic.write"}
                );
                return schemeClient.ModifyPermissions(path,
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync();
            };
            auto result = isThreadLocked ? kikimr.RunCall(doModify) : doModify();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed to grant permissions for user " << userSid << ": " << result.GetIssues().ToString());
        }
    }

    TDataQueryResult ExecuteQueryWithCache(TKikimrRunner& kikimr, const TString& userSid,
                                           const TString& query, bool isThreadLocked) {
        auto impl = [&] {
            TDriverConfig driverConfig;
            driverConfig
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken(userSid + "@builtin");

            auto driver = NYdb::TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);

            auto sessionResult = db.CreateSession().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed to create session for user " << userSid << ": " << sessionResult.GetIssues().ToString());

            auto session = sessionResult.GetSession();
            TExecDataQuerySettings settings;
            settings.KeepInQueryCache(true);

            return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        };
        return isThreadLocked ? kikimr.RunCall(impl) : impl();
    }

    TDataQueryResult ExecuteQueryWithCache(TKikimrRunner& kikimr, const TString& userSid,
                                           const TString& query, const TParams& params, bool isThreadLocked) {
        auto impl = [&] {
            TDriverConfig driverConfig;
            driverConfig
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken(userSid + "@builtin");

            auto driver = NYdb::TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);

            auto sessionResult = db.CreateSession().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed to create session for user " << userSid << ": " << sessionResult.GetIssues().ToString());

            auto session = sessionResult.GetSession();
            TExecDataQuerySettings settings;
            settings.KeepInQueryCache(true);

            return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        };
        return isThreadLocked ? kikimr.RunCall(impl) : impl();
    }

    void FillCache(TKikimrRunner& kikimr, const TVector<TString>& userSids, bool isThreadLocked) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            TString queryNoParams = TStringBuilder()
                << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << key << ";";

            auto result = ExecuteQueryWithCache(kikimr, userSid, queryNoParams, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed for user " << userSid << ": " << result.GetIssues().ToString());

            // Query with single parameter
            TString queryWithParam = "DECLARE $key AS Uint32; SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";

            auto params = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .Build();

            auto resultWithParam = ExecuteQueryWithCache(kikimr, userSid, queryWithParam, params, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultWithParam.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed parameterized query for user " << userSid << ": " << resultWithParam.GetIssues().ToString());

            // Query with multiple parameters
            TString queryMultiParams = "DECLARE $key AS Uint32; DECLARE $value AS String; "
                "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key OR Value = $value;";

            auto paramsMulti = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .AddParam("$value")
                    .String("Value" + ToString(key))
                    .Build()
                .Build();

            auto resultMultiParams = ExecuteQueryWithCache(kikimr, userSid, queryMultiParams, paramsMulti, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultMultiParams.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed multi-param query for user " << userSid << ": " << resultMultiParams.GetIssues().ToString());

            // UPSERT query
            TString queryUpsert = "DECLARE $key AS Uint32; DECLARE $value AS String; "
                "UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES ($key, $value);";

            auto paramsUpsert = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key + 1000)
                    .Build()
                .AddParam("$value")
                    .String("UpsertValue" + ToString(key))
                    .Build()
                .Build();

            auto resultUpsert = ExecuteQueryWithCache(kikimr, userSid, queryUpsert, paramsUpsert, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultUpsert.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed UPSERT query for user " << userSid << ": " << resultUpsert.GetIssues().ToString());

            // UPDATE query
            TString queryUpdate = "DECLARE $key AS Uint32; DECLARE $newValue AS String; "
                "UPDATE `/Root/KeyValue` SET Value = $newValue WHERE Key = $key;";

            auto paramsUpdate = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .AddParam("$newValue")
                    .String("Updated" + ToString(key))
                    .Build()
                .Build();

            auto resultUpdate = ExecuteQueryWithCache(kikimr, userSid, queryUpdate, paramsUpdate, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultUpdate.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed UPDATE query for user " << userSid << ": " << resultUpdate.GetIssues().ToString());

            // Query with Optional parameter
            TString queryOptional = "DECLARE $key AS Uint32?; "
                "SELECT Key, Value FROM `/Root/KeyValue` WHERE $key IS NULL OR Key = $key;";

            auto paramsOptional = TParamsBuilder()
                .AddParam("$key")
                    .OptionalUint32(key)
                    .Build()
                .Build();

            auto resultOptional = ExecuteQueryWithCache(kikimr, userSid, queryOptional, paramsOptional, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultOptional.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed Optional query for user " << userSid << ": " << resultOptional.GetIssues().ToString());

            TString queryDiffTypes = "DECLARE $uint64Param AS Uint64; DECLARE $boolParam AS Bool; "
                "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key < $uint64Param AND $boolParam;";

            auto paramsDiffTypes = TParamsBuilder()
                .AddParam("$uint64Param")
                    .Uint64(key + 100)
                    .Build()
                .AddParam("$boolParam")
                    .Bool(true)
                    .Build()
                .Build();

            auto resultDiffTypes = ExecuteQueryWithCache(kikimr, userSid, queryDiffTypes, paramsDiffTypes, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultDiffTypes.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed different types query for user " << userSid << ": " << resultDiffTypes.GetIssues().ToString());

            key++;
        }
    }

    void FillCacheWithImplicitParams(TKikimrRunner& kikimr, const TVector<TString>& userSids, bool isThreadLocked) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            TString queryImplicit = "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";

            auto paramsImplicit = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .Build();

            auto resultImplicit = ExecuteQueryWithCache(kikimr, userSid, queryImplicit, paramsImplicit, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultImplicit.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed implicit parameterized query for user " << userSid << ": " << resultImplicit.GetIssues().ToString());

            // Another implicitly parameterized query with different parameter type
            TString queryImplicitInt32 = "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";

            auto paramsImplicitInt32 = TParamsBuilder()
                .AddParam("$key")
                    .Int32(static_cast<i32>(key))
                    .Build()
                .Build();

            auto resultImplicitInt32 = ExecuteQueryWithCache(kikimr, userSid, queryImplicitInt32, paramsImplicitInt32, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultImplicitInt32.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed implicit Int32 parameterized query for user " << userSid << ": " << resultImplicitInt32.GetIssues().ToString());

            // Implicitly parameterized query with multiple parameters
            TString queryImplicitMulti = "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key OR Value = $value;";

            auto paramsImplicitMulti = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .AddParam("$value")
                    .String("Value" + ToString(key))
                    .Build()
                .Build();

            auto resultImplicitMulti = ExecuteQueryWithCache(kikimr, userSid, queryImplicitMulti, paramsImplicitMulti, isThreadLocked);
            UNIT_ASSERT_VALUES_EQUAL_C(resultImplicitMulti.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed implicit multi-param query for user " << userSid << ": " << resultImplicitMulti.GetIssues().ToString());

            key++;
        }
    }

    TVector<TCompileCacheEntry> GetCompileCacheEntries(TKikimrRunner& kikimr, bool isThreadLocked) {
        auto getDb = [&] { return kikimr.GetTableClient(); };
        auto db = isThreadLocked ? kikimr.RunCall(getDb) : getDb();

        auto getSession = [&] { return db.CreateSession().GetValueSync().GetSession(); };
        auto session = isThreadLocked ? kikimr.RunCall(getSession) : getSession();

        auto doQuery = [&] {
            return session.ExecuteDataQuery(
                "SELECT QueryId, Query, UserSID FROM `/Root/.sys/compile_cache_queries` ORDER BY AccessCount DESC",
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();
        };
        auto result = isThreadLocked ? kikimr.RunCall(doQuery) : doQuery();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

        auto resultSet = result.GetResultSet(0);
        TResultSetParser parser(resultSet);
        TVector<TCompileCacheEntry> entries;
        while (parser.TryNextRow()) {
            TCompileCacheEntry entry;
            entry.QueryId = *parser.ColumnParser("QueryId").GetOptionalUtf8();
            entry.Query = *parser.ColumnParser("Query").GetOptionalUtf8();
            entry.UserSID = *parser.ColumnParser("UserSID").GetOptionalUtf8();
            entries.push_back(std::move(entry));
        }

        return entries;
    }

    THashSet<TString> GetLocalCacheUserSids(
        TTestActorRuntime& runtime,
        ui32 nodeIndex,
        const TString& tenantName)
    {
        auto edgeActor = runtime.AllocateEdgeActor(nodeIndex);
        auto compileServiceId = MakeKqpCompileServiceID(runtime.GetNodeId(nodeIndex));

        auto* request = new TEvKqp::TEvListQueryCacheQueriesRequest();
        request->Record.SetTenantName(tenantName);
        request->Record.SetFreeSpace(1024 * 1024);

        runtime.Send(new IEventHandle(compileServiceId, edgeActor, request), nodeIndex);

        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvListQueryCacheQueriesResponse>(
            edgeActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(response, "Failed to get local cache entries from node " << nodeIndex);

        THashSet<TString> userSids;
        for (const auto& query : response->Get()->Record.GetCacheCacheQueries()) {
            userSids.insert(query.GetUserSID());
        }
        return userSids;
    }

    void VerifyLocalCacheContainsUsers(
        TTestActorRuntime& runtime,
        ui32 nodeIndex,
        const TString& tenantName,
        const TVector<TString>& expectedUserSids)
    {
        auto foundUserSids = GetLocalCacheUserSids(runtime, nodeIndex, tenantName);

        for (const auto& userSid : expectedUserSids) {
            TString fullUserSid = userSid + "@builtin";
            UNIT_ASSERT_C(foundUserSids.contains(fullUserSid),
                "Cache should contain entry for user " << fullUserSid
                << ". Found users: " << JoinSeq(", ", foundUserSids));
        }
    }

    struct TWarmupTestParams {
        ui32 NodeCount = 3;
        TVector<TString> UserSids;
        bool UseRealThreads = true;
        bool FillCache = true;
        bool FillImplicitParams = true;
        bool WaitBootstrap = false;
    };

    struct TWarmupTestEnv {
        TKikimrRunner& Kikimr;
        TTestActorRuntime& Runtime;
        bool IsThreadLocked;
        ui32 NodeId = 0;
        ui32 NodeCount = 3;
        TVector<TString> UserSids;
        size_t ExpectedUniqueCount = 0;
    };

    TKikimrSettings MakeWarmupTestSettings(const TWarmupTestParams& params) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableCompileCacheView(true);
        auto settings = TKikimrSettings()
            .SetUseRealThreads(params.UseRealThreads)
            .SetNodeCount(params.NodeCount)
            .SetWithSampleTables(true)
            .SetFeatureFlags(featureFlags);
        return settings;
    }

    TWarmupTestEnv PrepareWarmupTest(TKikimrRunner& kikimr, const TWarmupTestParams& params) {
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const bool isThreadLocked = !params.UseRealThreads;

        GrantPermissions(kikimr, "/Root/KeyValue", params.UserSids, isThreadLocked);
        if (params.FillCache) {
            FillCache(kikimr, params.UserSids, isThreadLocked);
        }
        if (params.FillImplicitParams) {
            FillCacheWithImplicitParams(kikimr, params.UserSids, isThreadLocked);
        }

        size_t expectedUniqueCount = 0;
        if (params.FillCache || params.FillImplicitParams) {
            auto cacheEntries = GetCompileCacheEntries(kikimr, isThreadLocked);
            UNIT_ASSERT_C(!cacheEntries.empty() || params.NodeCount == 1,
                "Compile cache should have entries after executing queries");
            THashSet<std::pair<TString, TString>> uniqueQueryUserPairs;
            for (const auto& entry : cacheEntries) {
                uniqueQueryUserPairs.insert({entry.Query, entry.UserSID});
            }
            expectedUniqueCount = uniqueQueryUserPairs.size();
        }

        return TWarmupTestEnv{kikimr, runtime, isThreadLocked, 0, params.NodeCount, params.UserSids, expectedUniqueCount};
    }

    TEvKqpWarmupComplete::TPtr RunWarmup(TWarmupTestEnv& env, const TKqpWarmupConfig& config,
            TDuration timeout, bool waitBootstrap = false) {
        auto warmupEdge = env.Runtime.AllocateEdgeActor(env.NodeId);
        auto* warmupActor = CreateKqpWarmupActor(config, "/Root", "", {warmupEdge});
        auto warmupActorId = env.Runtime.Register(warmupActor, env.NodeId);

        if (waitBootstrap) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
            env.Runtime.DispatchEvents(opts, TDuration::Seconds(1));
        }

        env.Runtime.Send(new IEventHandle(warmupActorId, warmupEdge, new TEvStartWarmup(env.NodeCount)), env.NodeId);
        return env.Runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, timeout);
    }

    void VerifyQueriesServedFromCache(TKikimrRunner& kikimr, const TVector<TString>& userSids, bool isThreadLocked) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            TString query = TStringBuilder()
                << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << key++ << ";";

            auto doQuery = [&] {
                TDriverConfig driverConfig;
                driverConfig
                    .SetEndpoint(kikimr.GetEndpoint())
                    .SetDatabase("/Root")
                    .SetAuthToken(userSid + "@builtin");

                auto driver = NYdb::TDriver(driverConfig);
                auto db = NYdb::NTable::TTableClient(driver);

                auto session = db.CreateSession().GetValueSync().GetSession();

                TExecDataQuerySettings settings;
                settings.KeepInQueryCache(true);
                settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

                return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            };
            auto result = isThreadLocked ? kikimr.RunCall(doQuery) : doQuery();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                "Query failed for user " << userSid << ": " << result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_C(stats.compilation().from_cache(),
                "Query for user " << userSid << " should be served from cache, but was recompiled");
        }
    }

    } // namespace

    Y_UNIT_TEST_SUITE(KqpWarmup) {

        Y_UNIT_TEST(WarmupActorBasic) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1", "user2", "user3", "user4"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should complete successfully: " << warmupComplete->Get()->Message);

            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, env.ExpectedUniqueCount,
                "Should load deduplicated entries. Loaded: " << warmupComplete->Get()->EntriesLoaded
                << ", expected unique: " << env.ExpectedUniqueCount);
            VerifyLocalCacheContainsUsers(env.Runtime, env.NodeId, "/Root", env.UserSids);
            VerifyQueriesServedFromCache(kikimr, env.UserSids, env.IsThreadLocked);
        }

        Y_UNIT_TEST(WarmupSoftDeadlineStopsNewQueriesButCompletesPending) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1", "user2"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(15);
            warmupActorConfig.MaxConcurrentCompilations = 2;
            warmupActorConfig.MaxQueriesToLoad = 50;

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor should complete before hard deadline");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should complete successfully: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > 0,
                "At least some queries should be compiled. Loaded: " << warmupComplete->Get()->EntriesLoaded);
        }

        Y_UNIT_TEST(WarmupRespectsHardDeadline) {
            TWarmupTestParams params;
            for (ui32 i = 0; i < 20; ++i) {
                params.UserSids.push_back("user" + ToString(i));
            }

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::MilliSeconds(10);
            warmupActorConfig.HardDeadline = TDuration::MilliSeconds(10);
            warmupActorConfig.MaxConcurrentCompilations = 8;
            warmupActorConfig.MaxQueriesToLoad = 500;
            warmupActorConfig.MaxCompilationDurationMs = 60000;

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete before hard deadline expires");
            UNIT_ASSERT_C(!warmupComplete->Get()->Success,
                "Warmup should fail due to hard deadline: " << warmupComplete->Get()->Message);
        }

        Y_UNIT_TEST(WarmupEmptyCache) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};
            params.FillCache = false;
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(10);

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed with empty cache: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, 0,
                "No entries should be loaded from empty cache");
        }

        Y_UNIT_TEST(WarmupSingleNodeSkips) {
            TWarmupTestParams params;
            params.NodeCount = 1;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(10);

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed (skip): " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->Message.Contains("single node"),
                "Message should indicate single node skip: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, 0,
                "No entries loaded when skipping for single node");
        }

        Y_UNIT_TEST(WarmupRespectsMaxQueriesToLoad) {
            TWarmupTestParams params;
            for (ui32 i = 0; i < 15; ++i) {
                params.UserSids.push_back("user" + ToString(i));
            }

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            const ui32 maxToLoad = 10;
            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(15);
            warmupActorConfig.MaxQueriesToLoad = maxToLoad;

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded <= maxToLoad,
                "EntriesLoaded should not exceed MaxQueriesToLoad. Loaded: "
                << warmupComplete->Get()->EntriesLoaded << ", max: " << maxToLoad);
        }

        Y_UNIT_TEST(WarmupFiltersByCompilationDuration) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1", "user2"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig configNoFilter;
            configNoFilter.SoftDeadline = TDuration::Seconds(5);
            configNoFilter.HardDeadline = TDuration::Seconds(15);
            configNoFilter.MaxCompilationDurationMs = 0;

            auto warmupNoFilter = RunWarmup(env, configNoFilter,
                configNoFilter.HardDeadline + TDuration::Seconds(1));
            UNIT_ASSERT_C(warmupNoFilter && warmupNoFilter->Get()->Success,
                "Baseline warmup should succeed");
            const ui32 baselineLoaded = warmupNoFilter->Get()->EntriesLoaded;

            TKqpWarmupConfig configWithFilter;
            configWithFilter.SoftDeadline = TDuration::Seconds(5);
            configWithFilter.HardDeadline = TDuration::Seconds(15);
            configWithFilter.MaxCompilationDurationMs = 1;

            auto warmupWithFilter = RunWarmup(env, configWithFilter,
                configWithFilter.HardDeadline + TDuration::Seconds(1));
            UNIT_ASSERT_C(warmupWithFilter && warmupWithFilter->Get()->Success,
                "Filtered warmup should succeed");
            const ui32 filteredLoaded = warmupWithFilter->Get()->EntriesLoaded;

            UNIT_ASSERT_C(filteredLoaded < baselineLoaded,
                "MaxCompilationDurationMs=1 should filter out some queries. "
                "Baseline: " << baselineLoaded << ", filtered: " << filteredLoaded);
        }

        Y_UNIT_TEST(WarmupAfterSchemaChange) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            {
                auto db = kikimr.GetTableClient();
                auto session = db.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteSchemeQuery(
                    "ALTER TABLE `/Root/KeyValue` ADD COLUMN Extra String;"
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                    "ALTER TABLE failed: " << result.GetIssues().ToString());
            }

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(10);
            warmupActorConfig.HardDeadline = TDuration::Seconds(20);
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed after schema change: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > 0,
                "Queries should still compile after ADD COLUMN: " << warmupComplete->Get()->EntriesLoaded);
        }

        Y_UNIT_TEST(WarmupAfterDropTable) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            {
                auto db = kikimr.GetTableClient();
                auto session = db.CreateSession().GetValueSync().GetSession();

                auto createResult = session.ExecuteSchemeQuery(
                    "CREATE TABLE `/Root/TempTable` (Id Uint32, Name String, PRIMARY KEY (Id));"
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), NYdb::EStatus::SUCCESS,
                    "CREATE TABLE failed: " << createResult.GetIssues().ToString());

                GrantPermissions(kikimr, "/Root/TempTable", params.UserSids, false);

                for (const auto& userSid : params.UserSids) {
                    auto result = ExecuteQueryWithCache(kikimr, userSid,
                        "SELECT Id, Name FROM `/Root/TempTable` WHERE Id = 1;", false);
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                        "Query on TempTable failed for " << userSid << ": " << result.GetIssues().ToString());
                }

                auto dropResult = session.ExecuteSchemeQuery(
                    "DROP TABLE `/Root/TempTable`;"
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(dropResult.GetStatus(), NYdb::EStatus::SUCCESS,
                    "DROP TABLE failed: " << dropResult.GetIssues().ToString());
            }

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(10);
            warmupActorConfig.HardDeadline = TDuration::Seconds(20);
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed even with dropped table queries: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, env.ExpectedUniqueCount,
                "Only original KeyValue queries should compile (TempTable queries should fail). Loaded: "
                << warmupComplete->Get()->EntriesLoaded << ", expected: " << env.ExpectedUniqueCount);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesFailed > 0,
                "TempTable queries should fail compilation. Failed: " << warmupComplete->Get()->EntriesFailed);
        }

        Y_UNIT_TEST(WarmupInvalidMetadata) {
            TWarmupTestParams params;
            params.UseRealThreads = false;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            const auto metadataObserver = env.Runtime.AddObserver<TEvKqp::TEvListQueryCacheQueriesResponse>(
                [&](TEvKqp::TEvListQueryCacheQueriesResponse::TPtr& ev) {
                    auto& record = ev->Get()->Record;
                    for (size_t i = 0; i < record.CacheCacheQueriesSize(); ++i) {
                        record.MutableCacheCacheQueries(i)->SetMetaInfo("{invalid json broken!!!}}}");
                    }
                });

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(15);

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1), /*waitBootstrap*/ true);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed with invalid metadata (graceful degradation): "
                << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, env.ExpectedUniqueCount,
                "All queries should compile despite corrupted metadata "
                "(FillYdbParametersFromMetadata silently ignores invalid JSON). "
                "Loaded: " << warmupComplete->Get()->EntriesLoaded
                << ", expected: " << env.ExpectedUniqueCount);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesFailed, 0,
                "No compilations should fail with invalid metadata (graceful degradation)");
        }

        Y_UNIT_TEST(WarmupMaxNodesToRequestZero) {
            TWarmupTestParams params;
            params.UserSids = {"user0", "user1", "user2"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(10);
            warmupActorConfig.HardDeadline = TDuration::Seconds(20);
            warmupActorConfig.MaxNodesToRequest = 0;

            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed with MaxNodesToRequest=0: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > 0,
                "Should load entries when querying all nodes: " << warmupComplete->Get()->EntriesLoaded);
        }

        Y_UNIT_TEST(WarmupSlowFetchHardDeadline) {
            TWarmupTestParams params;
            params.UseRealThreads = false;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(3);
            warmupActorConfig.HardDeadline = TDuration::Seconds(6);

            auto warmupEdge = env.Runtime.AllocateEdgeActor(env.NodeId);
            auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, "/Root", "", {warmupEdge});
            auto warmupActorId = env.Runtime.Register(warmupActor, env.NodeId);

            const auto compileBlocker = env.Runtime.AddObserver<TEvKqp::TEvQueryResponse>(
                [warmupActorId](TEvKqp::TEvQueryResponse::TPtr& ev) {
                    if (ev->Recipient == warmupActorId) {
                        ev.Reset();
                    }
                });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
                env.Runtime.DispatchEvents(opts, TDuration::Seconds(1));
            }

            env.Runtime.Send(new IEventHandle(warmupActorId, warmupEdge,
                new TEvStartWarmup(env.NodeCount)), env.NodeId);
            auto warmupComplete = env.Runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(
                warmupEdge, warmupActorConfig.HardDeadline + TDuration::Seconds(5));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete via hard deadline");
            UNIT_ASSERT_C(!warmupComplete->Get()->Success,
                "Warmup should fail when compilations are stuck (hard deadline): "
                << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded == 0,
                "No compilations should succeed when responses are blocked, loaded: "
                << warmupComplete->Get()->EntriesLoaded);
        }

        Y_UNIT_TEST(WarmupAllCompilationsFail) {
            TWarmupTestParams params;
            params.UseRealThreads = false;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(15);

            auto warmupEdge = env.Runtime.AllocateEdgeActor(env.NodeId);
            auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, "/Root", "", {warmupEdge});
            auto warmupActorId = env.Runtime.Register(warmupActor, env.NodeId);

            const auto compileObserver = env.Runtime.AddObserver<TEvKqp::TEvQueryResponse>(
                [warmupActorId](TEvKqp::TEvQueryResponse::TPtr& ev) {
                    if (ev->Recipient == warmupActorId) {
                        ev->Get()->Record.SetYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
                    }
                });

            {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
                env.Runtime.DispatchEvents(opts, TDuration::Seconds(1));
            }

            env.Runtime.Send(new IEventHandle(warmupActorId, warmupEdge,
                new TEvStartWarmup(env.NodeCount)), env.NodeId);
            auto warmupComplete = env.Runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(
                warmupEdge, warmupActorConfig.HardDeadline + TDuration::Seconds(1));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed even when all compilations fail: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, 0,
                "No entries should be loaded when all compilations fail");
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesFailed > 0,
                "All compilations should be counted as failed. Failed: " << warmupComplete->Get()->EntriesFailed);
        }

        Y_UNIT_TEST(WarmupQueryTypePropagation) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            auto cacheEntries = GetCompileCacheEntries(kikimr, false);
            UNIT_ASSERT_C(!cacheEntries.empty(), "Cache should have entries");

            {
                auto db = kikimr.GetTableClient();
                auto session = db.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(
                    "SELECT QueryType FROM `/Root/.sys/compile_cache_queries` WHERE QueryType IS NOT NULL AND QueryType != '' LIMIT 1",
                    TTxControl::BeginTx().CommitTx()
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
                auto rs = result.GetResultSet(0);
                TResultSetParser parser(rs);
                UNIT_ASSERT_C(parser.TryNextRow(), "Should have at least one entry with QueryType");
                auto queryType = *parser.ColumnParser("QueryType").GetOptionalUtf8();
                UNIT_ASSERT_C(!queryType.empty(),
                    "QueryType should not be empty for cached queries");
                UNIT_ASSERT_C(queryType.contains("QUERY_TYPE_SQL_"),
                    "QueryType should be a valid EQueryType name, got: " << queryType);
            }

            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > 0,
                "Should compile queries with proper QueryType: " << warmupComplete->Get()->EntriesLoaded);
            VerifyQueriesServedFromCache(kikimr, params.UserSids, false);
        }

        Y_UNIT_TEST(WarmupEmptyDatabase) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};
            params.FillCache = false;
            params.FillImplicitParams = false;
            params.UseRealThreads = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            auto& runtime = *kikimr.GetTestServer().GetRuntime();

            auto warmupEdge = runtime.AllocateEdgeActor(0);
            auto* warmupActor = CreateKqpWarmupActor(
                TKqpWarmupConfig{.SoftDeadline = TDuration::Seconds(5), .HardDeadline = TDuration::Seconds(10)},
                "", "", {warmupEdge});
            runtime.Register(warmupActor, 0);

            auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(
                warmupEdge, TDuration::Seconds(10));

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete for empty database");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed (skip) for empty database: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, 0,
                "No entries should be loaded for empty database");
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesFailed, 0,
                "No entries should fail for empty database");
        }

        Y_UNIT_TEST(WarmupServerlessUnavailable) {
            TWarmupTestParams params;
            params.UseRealThreads = false;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            const auto sysviewObserver = env.Runtime.AddObserver<TEvKqp::TEvListQueryCacheQueriesRequest>(
                [&](TEvKqp::TEvListQueryCacheQueriesRequest::TPtr& ev) {
                    auto response = std::make_unique<TEvKqp::TEvListQueryCacheQueriesResponse>();
                    response->Record.SetStatus(Ydb::StatusIds::UNAVAILABLE);
                    NYql::TIssue issue("Compile cache is not available for this database");
                    NYql::TIssues issues;
                    issues.AddIssue(std::move(issue));
                    NYql::IssuesToMessage(issues, response->Record.MutableIssues());
                    env.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.release()));
                });

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(10);

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1), /*waitBootstrap*/ true);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(!warmupComplete->Get()->Success,
                "Warmup should fail for serverless-like unavailable: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->Message.Contains("Fetch failed"),
                "Message should indicate fetch failure: " << warmupComplete->Get()->Message);
        }

        Y_UNIT_TEST(WarmupUnavailableSysview) {
            TWarmupTestParams params;
            params.UseRealThreads = false;
            params.UserSids = {"user0"};
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            std::atomic<ui32> sysviewRequestCount{0};
            const auto sysviewObserver = env.Runtime.AddObserver<TEvKqp::TEvListQueryCacheQueriesRequest>(
                [&](TEvKqp::TEvListQueryCacheQueriesRequest::TPtr& ev) {
                    if (sysviewRequestCount++ == 0) {
                        auto response = std::make_unique<TEvKqp::TEvListQueryCacheQueriesResponse>();
                        response->Record.SetStatus(Ydb::StatusIds::UNAVAILABLE);
                        NYql::TIssue issue("Compile cache is not available");
                        NYql::TIssues issues;
                        issues.AddIssue(std::move(issue));
                        NYql::IssuesToMessage(issues, response->Record.MutableIssues());
                        env.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.release()));
                    }
                });

            TKqpWarmupConfig warmupActorConfig;
            warmupActorConfig.SoftDeadline = TDuration::Seconds(5);
            warmupActorConfig.HardDeadline = TDuration::Seconds(10);

            auto warmupComplete = RunWarmup(env, warmupActorConfig,
                warmupActorConfig.HardDeadline + TDuration::Seconds(1), /*waitBootstrap*/ true);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(!warmupComplete->Get()->Success,
                "Warmup should fail when sysview is unavailable: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->Message.Contains("Fetch failed"),
                "Message should indicate fetch failure: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(sysviewRequestCount.load() >= 1,
                "At least one sysview request should have been intercepted");
        }
        Y_UNIT_TEST(WarmupPgSyntaxQueriesSkipped) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};
            params.FillCache = false;
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            auto& runtime = *kikimr.GetTestServer().GetRuntime();

            GrantPermissions(kikimr, "/Root/KeyValue", params.UserSids, false);

            {
                TDriverConfig driverConfig;
                driverConfig
                    .SetEndpoint(kikimr.GetEndpoint())
                    .SetDatabase("/Root")
                    .SetAuthToken("user0@builtin");
                auto driver = NYdb::TDriver(driverConfig);
                auto queryClient = NYdb::NQuery::TQueryClient(driver);

                auto pgSettings = NYdb::NQuery::TExecuteQuerySettings()
                    .Syntax(NYdb::NQuery::ESyntax::Pg);

                auto result1 = queryClient.ExecuteQuery(
                    "SELECT 1 AS result",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                    pgSettings
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result1.GetStatus(), NYdb::EStatus::SUCCESS,
                    "PG query failed: " << result1.GetIssues().ToString());

                auto result2 = queryClient.ExecuteQuery(
                    "SELECT 2 + 3 AS sum_result",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                    pgSettings
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), NYdb::EStatus::SUCCESS,
                    "PG query 2 failed: " << result2.GetIssues().ToString());

                auto yqlSettings = NYdb::NQuery::TExecuteQuerySettings()
                    .Syntax(NYdb::NQuery::ESyntax::YqlV1);
                auto result3 = queryClient.ExecuteQuery(
                    "SELECT 42 AS answer",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                    yqlSettings
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), NYdb::EStatus::SUCCESS,
                    "YQL query failed: " << result3.GetIssues().ToString());
            }

            {
                auto edgeActor = runtime.AllocateEdgeActor(0);
                auto compileServiceId = MakeKqpCompileServiceID(runtime.GetNodeId(0));
                auto* request = new TEvKqp::TEvListQueryCacheQueriesRequest();
                request->Record.SetTenantName("/Root");
                request->Record.SetFreeSpace(1024 * 1024);
                runtime.Send(new IEventHandle(compileServiceId, edgeActor, request), 0);

                auto response = runtime.GrabEdgeEvent<TEvKqp::TEvListQueryCacheQueriesResponse>(
                    edgeActor, TDuration::Seconds(5));
                UNIT_ASSERT_C(response, "Failed to get cache entries");

                bool foundPgSyntax = false;
                bool foundYqlSyntax = false;
                for (const auto& entry : response->Get()->Record.GetCacheCacheQueries()) {
                    if (entry.GetSyntax() == "SYNTAX_PG") {
                        foundPgSyntax = true;
                    }
                    if (entry.GetSyntax() == "SYNTAX_YQL_V1") {
                        foundYqlSyntax = true;
                    }
                }
                UNIT_ASSERT_C(foundPgSyntax, "Sysview should store SYNTAX_PG for PG queries");
                UNIT_ASSERT_C(foundYqlSyntax, "Sysview should store SYNTAX_YQL_V1 for YQL queries");
            }

            // PG warmup is temporarily disabled (see kqp_warmup_compile_actor.cpp PG filter).
            // Warmup should succeed but only compile YQL queries, skipping PG ones.
            TWarmupTestEnv env{kikimr, runtime, false, 0, params.NodeCount, params.UserSids, 0};
            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed when PG queries are skipped: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded >= 1,
                "Should load at least 1 YQL entry (PG skipped). Loaded: " << warmupComplete->Get()->EntriesLoaded);
        }

        Y_UNIT_TEST(WarmupMixedQueryTypes) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};
            params.FillCache = false;
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            auto& runtime = *kikimr.GetTestServer().GetRuntime();

            GrantPermissions(kikimr, "/Root/KeyValue", {"user0"}, false);

            TDriverConfig driverConfig;
            driverConfig
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken("user0@builtin");
            auto driver = NYdb::TDriver(driverConfig);

            // 1. DML via Table API (QUERY_TYPE_SQL_DML)
            {
                auto tableClient = NYdb::NTable::TTableClient(driver);
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                TExecDataQuerySettings settings;
                settings.KeepInQueryCache(true);
                auto result = session.ExecuteDataQuery(
                    "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = 0",
                    TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                    "DML query failed: " << result.GetIssues().ToString());
            }

            // 2. SCAN via Table API (QUERY_TYPE_SQL_SCAN)
            {
                auto tableClient = NYdb::NTable::TTableClient(driver);
                auto it = tableClient.StreamExecuteScanQuery(
                    "SELECT Key FROM `/Root/KeyValue` LIMIT 1").GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), "Scan query failed: " << it.GetIssues().ToString());
                while (true) {
                    auto part = it.ReadNext().GetValueSync();
                    if (!part.IsSuccess()) break;
                }
            }

            // 3. GENERIC_QUERY via Query API (QUERY_TYPE_SQL_GENERIC_QUERY)
            {
                auto queryClient = NYdb::NQuery::TQueryClient(driver);
                auto result = queryClient.ExecuteQuery(
                    "SELECT 100 AS generic_result",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                    "Generic query failed: " << result.GetIssues().ToString());
            }

            // 4. GENERIC_SCRIPT via Query API (QUERY_TYPE_SQL_GENERIC_SCRIPT)
            {
                auto queryClient = NYdb::NQuery::TQueryClient(driver);
                auto scriptOp = queryClient.ExecuteScript("SELECT 200 AS script_result").ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(scriptOp.Status().GetStatus(), NYdb::EStatus::SUCCESS,
                    "Script submit failed: " << scriptOp.Status().GetIssues().ToString());

                NYdb::NOperation::TOperationClient opClient(driver);
                for (int i = 0; i < 100; ++i) {
                    auto op = opClient.Get<NYdb::NQuery::TScriptExecutionOperation>(
                        scriptOp.Id()).GetValueSync();
                    if (op.Ready()) break;
                    UNIT_ASSERT_C(op.Status().IsSuccess(),
                        "Script poll failed: " << op.Status().GetIssues().ToString());
                    Sleep(TDuration::MilliSeconds(100));
                }
            }

            // Verify sysview has different QueryType entries
            {
                auto edgeActor = runtime.AllocateEdgeActor(0);
                auto compileServiceId = MakeKqpCompileServiceID(runtime.GetNodeId(0));
                auto* request = new TEvKqp::TEvListQueryCacheQueriesRequest();
                request->Record.SetTenantName("/Root");
                request->Record.SetFreeSpace(1024 * 1024);
                runtime.Send(new IEventHandle(compileServiceId, edgeActor, request), 0);

                auto response = runtime.GrabEdgeEvent<TEvKqp::TEvListQueryCacheQueriesResponse>(
                    edgeActor, TDuration::Seconds(5));
                UNIT_ASSERT_C(response, "Failed to get cache entries");

                THashSet<TString> foundTypes;
                for (const auto& entry : response->Get()->Record.GetCacheCacheQueries()) {
                    if (!entry.GetQueryType().empty()) {
                        foundTypes.insert(entry.GetQueryType());
                    }
                }

                Cerr << "Found QueryTypes in cache: " << JoinSeq(", ", foundTypes) << Endl;
                UNIT_ASSERT_C(foundTypes.contains("QUERY_TYPE_SQL_DML"),
                    "Cache should have DML entries. Found: " << JoinSeq(", ", foundTypes));
                UNIT_ASSERT_C(foundTypes.contains("QUERY_TYPE_SQL_GENERIC_QUERY") ||
                              foundTypes.contains("QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY"),
                    "Cache should have Generic Query entries. Found: " << JoinSeq(", ", foundTypes));
            }

            TWarmupTestEnv env{kikimr, runtime, false, 0, params.NodeCount, params.UserSids, 0};
            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed for mixed query types: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded >= 3,
                "Should load at least 3 entries (DML + scan + generic). Loaded: "
                << warmupComplete->Get()->EntriesLoaded);
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesFailed, 0,
                "No entries should fail. Failed: " << warmupComplete->Get()->EntriesFailed);
        }

        Y_UNIT_TEST(WarmupTruncatedCountStats) {
            TWarmupTestParams params;
            params.UserSids = {"user0"};

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);

            NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed: " << warmupComplete->Get()->Message);

            Sleep(TDuration::Seconds(2));

            i64 truncated = counters.WarmupQueriesTruncated->Val();
            i64 emptyQueryType = counters.WarmupQueriesEmptyQueryType->Val();

            UNIT_ASSERT_VALUES_EQUAL_C(truncated, 0,
                "No queries should be truncated (all queries are small). "
                "Truncated: " << truncated);

            Cerr << "WarmupTruncatedCountStats: truncated=" << truncated
                 << ", emptyQueryType=" << emptyQueryType
                 << ", loaded=" << warmupComplete->Get()->EntriesLoaded
                 << ", failed=" << warmupComplete->Get()->EntriesFailed
                 << Endl;
        }

        Y_UNIT_TEST(WarmupParameterTypeParsing) {
            TWarmupTestParams params;
            params.UserSids = {};
            params.FillCache = false;
            params.FillImplicitParams = false;

            TKikimrRunner kikimr(MakeWarmupTestSettings(params));
            TWarmupTestEnv env = PrepareWarmupTest(kikimr, params);
            auto db = kikimr.GetTableClient();

            const TString explicitOptQuery =
                "DECLARE $key AS Uint32?; "
                "SELECT Key, Value FROM `/Root/KeyValue` WHERE $key IS NULL OR Key = $key;";

            const TString implicitQuery =
                "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $implKey;";

            {
                auto session = db.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(
                    explicitOptQuery,
                    TTxControl::BeginTx().CommitTx(),
                    TParamsBuilder().AddParam("$key").OptionalUint32(1).Build().Build(),
                    TExecDataQuerySettings().KeepInQueryCache(true)
                ).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
            {
                auto session = db.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(
                    implicitQuery,
                    TTxControl::BeginTx().CommitTx(),
                    TParamsBuilder().AddParam("$implKey").Uint32(1).Build().Build(),
                    TExecDataQuerySettings().KeepInQueryCache(true)
                ).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            TKqpWarmupConfig warmupActorConfig;
            auto warmupComplete = RunWarmup(env, warmupActorConfig, warmupActorConfig.HardDeadline);
            UNIT_ASSERT_C(warmupComplete, "Warmup actor must complete");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should succeed: " << warmupComplete->Get()->Message);
            UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > 0,
                "Warmup must compile at least one entry, loaded: " << warmupComplete->Get()->EntriesLoaded);


            auto verifyFromCache = [&](const TString& query, const TParams& queryParams) {
                auto session = db.CreateSession().GetValueSync().GetSession();
                TExecDataQuerySettings settings;
                settings.KeepInQueryCache(true);
                settings.CollectQueryStats(ECollectQueryStatsMode::Basic);
                auto result = session.ExecuteDataQuery(
                    query, TTxControl::BeginTx().CommitTx(), queryParams, settings
                ).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_C(stats.compilation().from_cache(),
                    "Query must be served from cache after warmup "
                    "(parameter types in Metadata JSON were not parsed correctly):\n" << query);
            };

            verifyFromCache(explicitOptQuery,
                TParamsBuilder().AddParam("$key").OptionalUint32(1).Build().Build());
            verifyFromCache(implicitQuery,
                TParamsBuilder().AddParam("$implKey").Uint32(1).Build().Build());
        }

    } // Y_UNIT_TEST_SUITE(KqpWarmup)

} // namespace NKikimr::NKqp

