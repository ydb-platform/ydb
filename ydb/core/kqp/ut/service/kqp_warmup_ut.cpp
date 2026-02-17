#include <memory>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/compile_service/kqp_warmup_compile_actor.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

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

    void GrantPermissions(TKikimrRunner& kikimr, const TString& path, const TVector<TString>& userSids) {
        auto driver = kikimr.RunCall([&] {
            TDriverConfig driverConfig;
            driverConfig
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken("root@builtin");
            return NYdb::TDriver(driverConfig);
        });
        
        auto schemeClient = NYdb::NScheme::TSchemeClient(driver);
        
        for (const auto& userSid : userSids) {
            auto result = kikimr.RunCall([&] {
                NYdb::NScheme::TPermissions permissions(userSid + "@builtin",
                    {"ydb.generic.read", "ydb.generic.write"}
                );
                return schemeClient.ModifyPermissions(path,
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed to grant permissions for user " << userSid << ": " << result.GetIssues().ToString());
        }
    }

    TDataQueryResult ExecuteQueryWithCache(TKikimrRunner& kikimr, const TString& userSid, 
                                           const TString& query) {
        return kikimr.RunCall([&] {
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
        });
    }

    TDataQueryResult ExecuteQueryWithCache(TKikimrRunner& kikimr, const TString& userSid,
                                           const TString& query, const TParams& params) {
        return kikimr.RunCall([&] {
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
        });
    }

    void FillCache(TKikimrRunner& kikimr, const TVector<TString>& userSids) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            // Query without parameters
            TString queryNoParams = TStringBuilder()
                << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << key << ";";

            auto result = ExecuteQueryWithCache(kikimr, userSid, queryNoParams);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, 
                "Failed for user " << userSid << ": " << result.GetIssues().ToString());

            // Query with single parameter
            TString queryWithParam = "DECLARE $key AS Uint32; SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";
            
            auto params = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .Build();
            
            auto resultWithParam = ExecuteQueryWithCache(kikimr, userSid, queryWithParam, params);
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
            
            auto resultMultiParams = ExecuteQueryWithCache(kikimr, userSid, queryMultiParams, paramsMulti);
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
            
            auto resultUpsert = ExecuteQueryWithCache(kikimr, userSid, queryUpsert, paramsUpsert);
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
            
            auto resultUpdate = ExecuteQueryWithCache(kikimr, userSid, queryUpdate, paramsUpdate);
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
            
            auto resultOptional = ExecuteQueryWithCache(kikimr, userSid, queryOptional, paramsOptional);
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
            
            auto resultDiffTypes = ExecuteQueryWithCache(kikimr, userSid, queryDiffTypes, paramsDiffTypes);
            UNIT_ASSERT_VALUES_EQUAL_C(resultDiffTypes.GetStatus(), NYdb::EStatus::SUCCESS, 
                "Failed different types query for user " << userSid << ": " << resultDiffTypes.GetIssues().ToString());

            key++;
        }
    }

    void FillCacheWithImplicitParams(TKikimrRunner& kikimr, const TVector<TString>& userSids) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            // Implicitly parameterized query - no DECLARE in text
            TString queryImplicit = "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";
            
            auto paramsImplicit = TParamsBuilder()
                .AddParam("$key")
                    .Uint32(key)
                    .Build()
                .Build();
            
            auto resultImplicit = ExecuteQueryWithCache(kikimr, userSid, queryImplicit, paramsImplicit);
            UNIT_ASSERT_VALUES_EQUAL_C(resultImplicit.GetStatus(), NYdb::EStatus::SUCCESS, 
                "Failed implicit parameterized query for user " << userSid << ": " << resultImplicit.GetIssues().ToString());

            // Another implicitly parameterized query with different parameter type
            TString queryImplicitInt32 = "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = $key;";
            
            auto paramsImplicitInt32 = TParamsBuilder()
                .AddParam("$key")
                    .Int32(static_cast<i32>(key))
                    .Build()
                .Build();
            
            auto resultImplicitInt32 = ExecuteQueryWithCache(kikimr, userSid, queryImplicitInt32, paramsImplicitInt32);
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
            
            auto resultImplicitMulti = ExecuteQueryWithCache(kikimr, userSid, queryImplicitMulti, paramsImplicitMulti);
            UNIT_ASSERT_VALUES_EQUAL_C(resultImplicitMulti.GetStatus(), NYdb::EStatus::SUCCESS, 
                "Failed implicit multi-param query for user " << userSid << ": " << resultImplicitMulti.GetIssues().ToString());

            key++;
        }
    }

    TVector<TCompileCacheEntry> GetCompileCacheEntries(TKikimrRunner& kikimr) {
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto result = kikimr.RunCall([&] {
            return session.ExecuteDataQuery(
                "SELECT QueryId, Query, UserSID FROM `/Root/.sys/compile_cache_queries` ORDER BY AccessCount DESC",
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();
        });

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

    void VerifyQueriesServedFromCache(TKikimrRunner& kikimr, const TVector<TString>& userSids) {
        ui32 key = 0;
        for (const auto& userSid : userSids) {
            TString query = TStringBuilder()
                << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << key++ << ";";

            auto result = kikimr.RunCall([&] {
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
            });
            
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
            ui32 nodeCount = 3;

            NKikimrConfig::TFeatureFlags featureFlags;
            featureFlags.SetEnableCompileCacheView(true);

            TKikimrSettings settings = TKikimrSettings()
                                        .SetUseRealThreads(false)
                                        .SetNodeCount(nodeCount)
                                        .SetWithSampleTables(true)
                                        .SetFeatureFlags(featureFlags);

            TKikimrRunner kikimr(settings);
            auto& runtime = *kikimr.GetTestServer().GetRuntime();

            TVector<TString> userSids = {"user0", "user1", "user2", "user3", "user4"};
            GrantPermissions(kikimr, "/Root/KeyValue", userSids);
            FillCache(kikimr, userSids);
            FillCacheWithImplicitParams(kikimr, userSids);

            auto cacheEntries = GetCompileCacheEntries(kikimr);
            UNIT_ASSERT_C(!cacheEntries.empty(),
                "Compile cache should have entries after executing queries");
            
            THashSet<std::pair<TString, TString>> uniqueQueryUserPairs;
            for (const auto& entry : cacheEntries) {
                uniqueQueryUserPairs.insert({entry.Query, entry.UserSID});
            }
            size_t expectedUniqueCount = uniqueQueryUserPairs.size();
            
            Cerr << "=== Cache entries before warmup: " << cacheEntries.size() 
                 << ", unique (Query, UserSID) pairs: " << expectedUniqueCount << Endl;
            for (size_t i = 0; i < std::min((size_t)5, cacheEntries.size()); ++i) {
                Cerr << "QueryId: " << cacheEntries[i].QueryId
                    << ", UserSID: " << cacheEntries[i].UserSID
                    << ", Query size: " << cacheEntries[i].Query.size() << Endl;
            }

            TKqpWarmupConfig warmupActorConfig;

            ui32 const nodeId = 0;
            auto warmupEdge = runtime.AllocateEdgeActor(nodeId);
            auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, "/Root", "", {warmupEdge});
            auto warmupActorId = runtime.Register(warmupActor, nodeId);

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
            runtime.DispatchEvents(opts, TDuration::Seconds(1));

            runtime.Send(new IEventHandle(warmupActorId, warmupEdge, new TEvStartWarmup(nodeCount)), nodeId);

            auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, warmupActorConfig.HardDeadline);

            UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
            UNIT_ASSERT_C(warmupComplete->Get()->Success,
                "Warmup should complete successfully: " << warmupComplete->Get()->Message);
            
            UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, expectedUniqueCount,
                "Should load deduplicated entries. Loaded: " << warmupComplete->Get()->EntriesLoaded
                << ", expected unique: " << expectedUniqueCount);
            VerifyLocalCacheContainsUsers(runtime, nodeId, "/Root", userSids);
            VerifyQueriesServedFromCache(kikimr, userSids);
        }
    }

} // namespace NKikimr::NKqp

