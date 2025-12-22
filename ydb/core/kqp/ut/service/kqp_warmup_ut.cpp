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

void FillCache(TKikimrRunner& kikimr, const TVector<TString>& userSids) {
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

            auto sessionResult = db.CreateSession().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), NYdb::EStatus::SUCCESS,
                "Failed to create session for user " << userSid << ": " << sessionResult.GetIssues().ToString());
            
            auto session = sessionResult.GetSession();
            TExecDataQuerySettings settings;
            settings.KeepInQueryCache(true);
            return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        });
        
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, 
            "Failed for user " << userSid << ": " << result.GetIssues().ToString());
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

} // namespace

Y_UNIT_TEST_SUITE(KqpWarmup) {

    Y_UNIT_TEST(WarmupActorBasic) {
        ui32 nodeCount = 2;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(nodeCount);
        settings.SetWithSampleTables(true);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableCompileCacheView(true);
        settings.SetFeatureFlags(featureFlags);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        TVector<TString> userSids = {"user0", "user1", "user2", "user3", "user4",
                                      "user5", "user6", "user7", "user8", "user9",
                                      "user10", "user11", "user12", "user13", "user14",
                                      "user15", "user16", "user17", "user18", "user19"};
        GrantPermissions(kikimr, "/Root/KeyValue", userSids);
        FillCache(kikimr, userSids);

        auto cacheEntries = GetCompileCacheEntries(kikimr);
        UNIT_ASSERT_C(!cacheEntries.empty(),
            "Compile cache should have entries after executing queries");
        
        Cerr << "=== Cache entries before warmup: " << cacheEntries.size() << Endl;
        for (size_t i = 0; i < std::min((size_t)5, cacheEntries.size()); ++i) {
            Cerr << "QueryId: " << cacheEntries[i].QueryId
                 << ", UserSID: " << cacheEntries[i].UserSID
                 << ", Query size: " << cacheEntries[i].Query.size() << Endl;
        }

        auto deadline = TDuration::Seconds(30);
        TKqpWarmupConfig warmupActorConfig;
        warmupActorConfig.Enabled = true;
        warmupActorConfig.Deadline = deadline;
        warmupActorConfig.CompileCacheWarmupEnabled = true;
        warmupActorConfig.MaxConcurrentCompilations = 3;

        ui32 const nodeId = 0;
        auto warmupEdge = runtime.AllocateEdgeActor(nodeId);
        auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, "/Root", "", warmupEdge);
        runtime.Register(warmupActor, nodeId);

        auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, deadline + TDuration::Seconds(1));

        UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
        UNIT_ASSERT_C(warmupComplete->Get()->Success, 
            "Warmup should complete successfully: " << warmupComplete->Get()->Message);
        UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, cacheEntries.size(),
            "All entries should be loaded. Loaded: " << warmupComplete->Get()->EntriesLoaded 
            << ", expected: " << cacheEntries.size());
        
        Cerr << "=== Warmup completed: " << warmupComplete->Get()->EntriesLoaded 
             << " entries loaded with MaxConcurrentCompilations=" 
             << warmupActorConfig.MaxConcurrentCompilations << Endl;
    }

    Y_UNIT_TEST(WarmupActorNoPermissions) {
        ui32 nodeCount = 2;

        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(nodeCount);
        settings.SetWithSampleTables(true);
        settings.SetEnableResourcePools(false);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableCompileCacheView(true);
        featureFlags.SetEnableResourcePools(false);
        settings.SetFeatureFlags(featureFlags);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        TVector<TString> userSids = {"user0", "user1", "user2"};
        
        GrantPermissions(kikimr, "/Root/KeyValue", userSids);
        FillCache(kikimr, userSids);

        auto cacheEntries = GetCompileCacheEntries(kikimr);
        UNIT_ASSERT_C(!cacheEntries.empty(), "Cache should have entries");
        Cerr << "=== Cache entries: " << cacheEntries.size() << Endl;

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
                return schemeClient.ModifyPermissions("/Root/KeyValue",
                    NYdb::NScheme::TModifyPermissionsSettings().AddRevokePermissions(permissions)
                ).ExtractValueSync();
            });
            Cerr << "Revoked permissions for " << userSid << ": " << result.GetStatus() << Endl;
        }

        TKqpWarmupConfig warmupActorConfig;
        warmupActorConfig.Enabled = true;
        warmupActorConfig.Deadline = TDuration::Seconds(30);
        warmupActorConfig.CompileCacheWarmupEnabled = true;
        warmupActorConfig.MaxConcurrentCompilations = 5;

        ui32 const nodeId = 0;
        auto warmupEdge = runtime.AllocateEdgeActor(nodeId);
        auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, "/Root", "", warmupEdge);
        runtime.Register(warmupActor, nodeId);

        auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, TDuration::Seconds(30));

        UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
        UNIT_ASSERT_C(warmupComplete->Get()->Success, 
            "Warmup should complete successfully even without user permissions: " << warmupComplete->Get()->Message);
        UNIT_ASSERT_VALUES_EQUAL_C(warmupComplete->Get()->EntriesLoaded, cacheEntries.size(),
            "All entries should be loaded despite missing permissions. Loaded: " 
            << warmupComplete->Get()->EntriesLoaded << ", expected: " << cacheEntries.size());
        Cerr << "=== Warmup completed without user permissions: " 
             << warmupComplete->Get()->EntriesLoaded << " entries loaded" << Endl;
    }
    
}

} // namespace NKikimr::NKqp

