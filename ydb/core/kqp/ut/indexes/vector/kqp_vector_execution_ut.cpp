#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpSpecializedVectorExecution) {
    void CheckMatchesGenericAndFallsBack(bool innerProduct) {
        auto settings = TKikimrSettings().SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)
            .SetEnableForceFollowers(true).SetUseRealThreads(false);
        // The level cache retains 10 MiB reservations under its own transaction.
        // Exclude it when asserting that per-query allocations return to zero.
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetKqpLevelCacheMaxSizeBytes(0);
        // Force allocator growth so the resource cleanup assertions cover both
        // the initial reservation and memory granted while a query is running.
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1_KB);
        TKikimrRunner kikimr(settings);
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto scheme = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(sql).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto write = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteDataQuery(sql,
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme("CREATE TABLE Vectors (id Uint64 NOT NULL, emb String, payload String, PRIMARY KEY(id));");
        write(R"(
            UPSERT INTO Vectors (id,emb,payload) VALUES
            (1,Untag(Knn::ToBinaryStringFloat([0.0f,0.0f]),"FloatVector"),"one"),
            (2,Untag(Knn::ToBinaryStringFloat([1.0f,0.0f]),"FloatVector"),"two"),
            (3,Untag(Knn::ToBinaryStringFloat([3.0f,0.0f]),"FloatVector"),"three"),
            (0,Untag(Knn::ToBinaryStringFloat([-2.0f,0.0f]),"FloatVector"),"zero"),
            (UNWRAP(CAST("18446744073709551615" AS Uint64)),Untag(Knn::ToBinaryStringFloat([4.0f,0.0f]),"FloatVector"),"max");
        )");
        if (innerProduct) {
            // Nonzero, fractional targets and signed documents keep rankings
            // distinct while exercising both signs of the similarity score.
            write(R"(UPSERT INTO Vectors (id,emb) VALUES
                (1,Untag(Knn::ToBinaryStringFloat([0.5f,0.25f]),"FloatVector"));)");
        }
        scheme(R"(ALTER TABLE Vectors SET (READ_REPLICAS_SETTINGS="PER_AZ:1"))");
        scheme(TStringBuilder() << "ALTER TABLE Vectors ADD INDEX idx GLOBAL USING hnsw ON(emb) WITH ("
            << (innerProduct ? "similarity=inner_product" : "distance=euclidean")
            << ",vector_type=\"float\",vector_dimension=2,levels=1,clusters=1,min_rows=1)");
        const TString posting = "/Root/Vectors/idx/indexImplPostingTable";
        WaitForCompaction(&kikimr.GetTestServer(), posting);
        scheme("ALTER TABLE `" + posting + "` SET (READ_REPLICAS_SETTINGS=\"PER_AZ:1\")");
        scheme(R"(ALTER TABLE `/Root/Vectors/idx/indexImplLevelTable` SET (READ_REPLICAS_SETTINGS="PER_AZ:1"))");
        auto group = GetServiceCounters(runtime->GetAppData().Counters, "kqp");
        auto started = group->GetCounter("Vector/SpecializedStarted", true);
        auto completed = group->GetCounter("Vector/SpecializedCompleted", true);
        auto fallback = group->GetCounter("Vector/SpecializedFallback", true);
        auto waitForResources = [&](i64 memoryLimit, bool allResources) {
            // gRPC cancellation and deferred destruction also run on real
            // threads, so a simulated-time timeout can expire before they run.
            const auto released = [&] {
                return group->GetCounter("RM/Memory", false)->Val() <= memoryLimit
                    && (!allResources || (group->GetCounter("IteratorReads/ReadActorCount", false)->Val() == 0
                        && group->GetCounter("RM/ComputeActors", false)->Val() == 0
                        && group->GetCounter("RM/ExternalMemory", false)->Val() == 0));
            };
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!released() && TInstant::Now() < deadline) {
                runtime->SimulateSleep(TDuration::MilliSeconds(1));
                Sleep(TDuration::MilliSeconds(1));
            }
            UNIT_ASSERT_C(released(), TStringBuilder() << "Resource cleanup timed out: reads="
                << group->GetCounter("IteratorReads/ReadActorCount", false)->Val()
                << " compute=" << group->GetCounter("RM/ComputeActors", false)->Val()
                << " external=" << group->GetCounter("RM/ExternalMemory", false)->Val()
                << " memory=" << group->GetCounter("RM/Memory", false)->Val()
                << " limit=" << memoryLimit);
        };
        auto client = kikimr.GetQueryClient();
        const TString metricFunction = innerProduct ? "Knn::InnerProductSimilarity" : "Knn::EuclideanDistance";
        auto sql = [innerProduct, metricFunction](bool specialized, const TString& projection, const TString& tail) {
            return TStringBuilder() << "PRAGMA ydb.HNSWReturnScores='true'; "
                << "PRAGMA ydb.HNSWSpecializedExecution='" << (specialized ? "true" : "false") << "'; "
                << "PRAGMA ydb.HNSWFuseTopSort='true'; PRAGMA ydb.HNSWUseClusterRouting='true'; PRAGMA ydb.HNSWPreciseShardRouting='true'; "
                << "DECLARE $key AS Uint64; DECLARE $limit AS Uint64; "
                << "$q=(SELECT emb FROM Vectors WHERE id=$key); SELECT " << projection
                << " FROM Vectors VIEW idx " << tail << " ORDER BY " << metricFunction << "(emb,$q)"
                << (innerProduct ? " DESC" : " ASC") << " LIMIT $limit;";
        };
        auto query = [&](bool specialized, ui64 key, ui64 limit, const TString& projection = "UNWRAP(CAST(id AS String)) AS id",
                         const TString& tail = "", bool stale = true,
                         NYdb::NQuery::EStatsMode statsMode = NYdb::NQuery::EStatsMode::None, ui32 chunkSize = 0) {
            Cerr << "Vector execution case: specialized=" << specialized << " key=" << key
                << " limit=" << limit << " projection=" << projection << " tail=" << tail << Endl;
            auto params = NYdb::TParamsBuilder().AddParam("$key").Uint64(key).Build()
                .AddParam("$limit").Uint64(limit).Build().Build();
            auto executionSettings = NYdb::NQuery::TExecuteQuerySettings().StatsMode(statsMode);
            if (chunkSize) {
                executionSettings.OutputChunkMaxSize(chunkSize);
            }
            auto result = kikimr.RunCall([&] { return client.ExecuteQuery(sql(specialized, projection, tail),
                NYdb::NQuery::TTxControl::BeginTx(stale ? NYdb::NQuery::TTxSettings::StaleRO()
                    : NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params,
                executionSettings).ExtractValueSync(); });
            return result;
        };
        UNIT_ASSERT(query(false, 1, 3).IsSuccess());
        waitForResources(0, true);
        const auto retainedMemory = group->GetCounter("RM/Memory", false)->Val();
        for (ui64 key : TVector<ui64>{1, 2, 99, 0, Max<ui64>()}) {
            for (ui64 limit : {0u, 1u, 3u, 1025u}) {
                auto allocations = group->GetCounter("Rm/OnStartAllocs", true);
                const auto genericBefore = allocations->Val();
                const auto expected = query(false, key, limit);
                const auto genericAllocations = allocations->Val() - genericBefore;
                const auto before = started->Val();
                const auto nativeBefore = allocations->Val();
                const auto growthBefore = group->GetCounter("RM/ExtraMemAllocs", true)->Val();
                const auto memoryBefore = group->GetCounter("RM/Memory", false)->Val();
                const auto actual = query(true, key, limit);
                UNIT_ASSERT_C(expected.IsSuccess(), expected.GetIssues().ToString());
                UNIT_ASSERT_C(actual.IsSuccess(), actual.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(actual.GetResultSet(0)),
                    NYdb::FormatResultSetYson(expected.GetResultSet(0)));
                if (limit <= 1024) {
                    UNIT_ASSERT_VALUES_EQUAL_C(started->Val(), before + 1, "Eligible query did not use specialized execution");
                    waitForResources(memoryBefore, false);
                    if (key == 1 && limit == 3) {
                        Cerr << "Task admissions: generic=" << genericAllocations
                            << " specialized=" << allocations->Val() - nativeBefore << Endl;
                        UNIT_ASSERT(allocations->Val() - nativeBefore < genericAllocations);
                        UNIT_ASSERT(group->GetCounter("RM/ExtraMemAllocs", true)->Val() > growthBefore);
                    }
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(started->Val(), before);
                }
            }
        }
        for (const TString projection : {"id"}) {
            const auto before = started->Val();
            auto actual = query(true, 1, 3, projection);
            auto expected = query(false, 1, 3, projection);
            UNIT_ASSERT_C(actual.IsSuccess(), actual.GetIssues().ToString());
            UNIT_ASSERT_C(expected.IsSuccess(), expected.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(actual.GetResultSet(0)),
                NYdb::FormatResultSetYson(expected.GetResultSet(0)));
            UNIT_ASSERT_VALUES_EQUAL(started->Val(), before + 1);
        }
        // Non-total projections, filters and locking isolation must execute the
        // original SQL program, even when the pragma is enabled.
        for (const TString& projection : TVector<TString>{"id,emb", "id,payload", "id+1 AS id",
                TStringBuilder() << "id, " << metricFunction << "(emb,$q) AS distance"}) {
            const auto before = started->Val();
            auto actual = query(true, 1, 3, projection);
            auto expected = query(false, 1, 3, projection);
            UNIT_ASSERT_C(actual.IsSuccess(), actual.GetIssues().ToString());
            UNIT_ASSERT_C(expected.IsSuccess(), expected.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(actual.GetResultSet(0)),
                NYdb::FormatResultSetYson(expected.GetResultSet(0)));
            UNIT_ASSERT_VALUES_EQUAL(started->Val(), before);
        }
        const auto before = started->Val();
        UNIT_ASSERT(query(true, 1, 3, "UNWRAP(CAST(id AS String)) AS id", "WHERE id>1").IsSuccess());
        UNIT_ASSERT(query(true, 1, 3, "UNWRAP(CAST(id AS String)) AS id", "", false).IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), before);
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), completed->Val());
        UNIT_ASSERT(fallback->Val() > 0);

        const auto basicBefore = started->Val();
        const auto basic = query(true, 1, 3, "UNWRAP(CAST(id AS String)) AS id", "", true, NYdb::NQuery::EStatsMode::Basic);
        UNIT_ASSERT_C(basic.IsSuccess(), basic.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), basicBefore + 1);
        UNIT_ASSERT(basic.GetStats());
        const auto full = query(true, 1, 3, "UNWRAP(CAST(id AS String)) AS id", "", true, NYdb::NQuery::EStatsMode::Full);
        UNIT_ASSERT_C(full.IsSuccess(), full.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), basicBefore + 1);
        UNIT_ASSERT(full.GetStats());
        UNIT_ASSERT(full.GetStats()->GetPlan());
        UNIT_ASSERT_STRING_CONTAINS(*full.GetStats()->GetPlan(), "VectorExecutionCandidate");
        const auto chunkedBefore = started->Val();
        const auto chunked = query(true, 1, 3, "UNWRAP(CAST(id AS String)) AS id", "", true,
            NYdb::NQuery::EStatsMode::None, 1024);
        UNIT_ASSERT_C(chunked.IsSuccess(), chunked.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), chunkedBefore);

        const TVector<ui64> concurrentKeys{1, 2, 0, Max<ui64>()};
        TVector<TString> expectedConcurrent;
        for (ui64 key : concurrentKeys) {
            auto result = query(false, key, 1);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            expectedConcurrent.push_back(NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
        const auto concurrentBefore = started->Val();
        auto concurrent = kikimr.RunCall([&] {
            TVector<NYdb::NQuery::TAsyncExecuteQueryResult> futures;
            for (ui64 key : concurrentKeys) {
                auto params = NYdb::TParamsBuilder().AddParam("$key").Uint64(key).Build()
                    .AddParam("$limit").Uint64(1).Build().Build();
                futures.push_back(client.ExecuteQuery(sql(true, "UNWRAP(CAST(id AS String)) AS id", ""),
                    NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::StaleRO()).CommitTx(), params));
            }
            TVector<NYdb::NQuery::TExecuteQueryResult> results;
            for (auto& future : futures) {
                results.push_back(future.ExtractValueSync());
            }
            return results;
        });
        for (ui32 i = 0; i < concurrent.size(); ++i) {
            UNIT_ASSERT_C(concurrent[i].IsSuccess(), concurrent[i].GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(concurrent[i].GetResultSet(0)), expectedConcurrent[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), concurrentBefore + concurrentKeys.size());

        // Hold only the posting read so cancellation happens after the native
        // coordinator starts. A following request must still work after late I/O.
        {
            NActors::TBlockEvents<TEvDataShard::TEvRead> blocked(*runtime, [](const auto& ev) {
                return ev->Get()->Record.HasVectorTopK();
            });
            auto params = NYdb::TParamsBuilder().AddParam("$key").Uint64(1).Build()
                .AddParam("$limit").Uint64(3).Build().Build();
            const auto result = kikimr.RunCall([&] { return client.ExecuteQuery(sql(true, "UNWRAP(CAST(id AS String)) AS id", ""),
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::StaleRO()).CommitTx(), params,
                NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(200))).ExtractValueSync(); });
            UNIT_ASSERT_C(!result.IsSuccess(), "Blocked vector query unexpectedly succeeded");
            UNIT_ASSERT(!blocked.empty());
            blocked.Stop().Unblock();
        }
        UNIT_ASSERT(query(true, 1, 3).IsSuccess());
        waitForResources(retainedMemory, true);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("IteratorReads/ReadActorCount", false)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("RM/ComputeActors", false)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("RM/ExternalMemory", false)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("RM/Memory", false)->Val(), retainedMemory);
        // Recompilation after schema invalidation must retain eligibility and
        // rebuild the descriptor from the new schema rather than a stale route.
        scheme("ALTER TABLE Vectors ADD COLUMN extra Uint64;");
        const auto schemaBefore = started->Val();
        UNIT_ASSERT(query(true, 1, 3).IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(started->Val(), schemaBefore + 1);
    }

    Y_UNIT_TEST(MatchesGenericAndFallsBack) {
        CheckMatchesGenericAndFallsBack(false);
    }

    Y_UNIT_TEST(InnerProductMatchesGenericAndFallsBack) {
        CheckMatchesGenericAndFallsBack(true);
    }
}

} // namespace NKikimr::NKqp
