#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <format>

namespace NKikimr::NKqp {
using namespace NYdb;
using namespace NYdb::NTable;

namespace {
TDataQueryResult ExecuteDataQuery(TSession& session, const TString& query) {
    return session.ExecuteDataQuery(query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
}
} // namespace

Y_UNIT_TEST_SUITE(KqpHnswIndexes) {
    Y_UNIT_TEST_TWIN(HnswFullRangeUsesCachedSettings, Parameterized) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto scheme = [&](const TString& query) {
            const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme(Q_(R"(
            CREATE TABLE `/Root/HnswFullRange` (
                pk Int64 NOT NULL,
                emb String,
                PRIMARY KEY (pk)
            );
        )"));
        const auto write = ExecuteDataQuery(session, Q_(R"(
            UPSERT INTO `/Root/HnswFullRange` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector")),
                (3, Untag(Knn::ToBinaryStringFloat([3.0f, 0.0f]), "FloatVector"));
        )"));
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        scheme(Q_(R"(
            ALTER TABLE `/Root/HnswFullRange`
                ADD INDEX index GLOBAL USING hnsw ON (emb)
                WITH (similarity=inner_product, vector_type="float", vector_dimension=2,
                      levels=1, clusters=2, min_rows=1);
        )"));

        // Like the benchmark, search the posting table directly. Its query
        // requests auto-detected dimensions and omits the graph's tuning.
        const TString target = Parameterized
            ? "DECLARE $q AS List<Float>; $target = Knn::ToBinaryStringFloat($q);"
            : "$target = Knn::ToBinaryStringFloat([1.0f, 0.0f]);";
        const TString query = target + Q_(R"(
            SELECT pk FROM `/Root/HnswFullRange/index/indexImplPostingTable`
            ORDER BY Knn::InnerProductSimilarity(emb, $target) DESC LIMIT 1;
        )");
        auto params = db.GetParamsBuilder();
        if (Parameterized) {
            params.AddParam("$q").BeginList()
                .AddListItem().Float(1.0f)
                .AddListItem().Float(0.0f)
                .EndList().Build();
        }
        const auto values = params.Build();
        const auto settings = TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic);
        ui32 cacheHits = 0;
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (cacheHits < 2 && TInstant::Now() < deadline) {
            const auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx(),
                values, settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[3]]");
            ui64 rows = 0;
            for (const auto& phase : NYdb::TProtoAccessor::GetProto(*result.GetStats()).query_phases()) {
                for (const auto& access : phase.table_access()) {
                    rows += access.reads().rows();
                }
            }
            if (rows == 1) {
                ++cacheHits;
            } else {
                Sleep(TDuration::MilliSeconds(100));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(cacheHits, 2);

        // Hold a read-only snapshot across a write. The versioned graph must
        // preserve the old result and accelerate it without taking read locks.
        auto snapshot = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SnapshotRW()),
            values, settings).ExtractValueSync();
        UNIT_ASSERT_C(snapshot.IsSuccess(), snapshot.GetIssues().ToString());
        const auto tx = snapshot.GetTransaction();
        UNIT_ASSERT(tx);
        auto writer = db.CreateSession().GetValueSync().GetSession();
        const auto update = ExecuteDataQuery(writer, Q_(R"(
            UPSERT INTO `/Root/HnswFullRange` (pk, emb) VALUES
                (3, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )"));
        UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
        snapshot = session.ExecuteDataQuery(query, TTxControl::Tx(*tx).CommitTx(),
            values, settings).ExtractValueSync();
        UNIT_ASSERT_C(snapshot.IsSuccess(), snapshot.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(snapshot.GetResultSet(0)), "[[3]]");
        AssertTableReads(snapshot, "/Root/HnswFullRange/index/indexImplPostingTable", 1);
    }

    Y_UNIT_TEST_TWIN(HnswIndexViewSkipsClusterTraversal, EnableVectorSearchActor) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableVectorSearchActor(EnableVectorSearchActor);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)
            .SetUseRealThreads(false)};
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto scheme = [&](const TString& query) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(query).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme("CREATE TABLE `/Root/HnswView` (pk Uint64 NOT NULL, emb String, PRIMARY KEY (pk));");
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, Q_(R"(
            $rows = ListMap(ListFromRange(0ul, 128ul), ($key) -> {
                RETURN AsStruct($key AS pk,
                    Untag(Knn::ToBinaryStringFloat([
                        IF($key < 64ul, -1000.0f, 1000.0f), Unwrap(CAST($key % 64ul AS Float))
                    ]), "FloatVector") AS emb);
            });
            UPSERT INTO `/Root/HnswView` SELECT * FROM AS_TABLE($rows);
        )")); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        scheme(Q_(R"(
            ALTER TABLE `/Root/HnswView` ADD INDEX vidx GLOBAL USING hnsw ON (emb)
            WITH (distance=euclidean, vector_type="float", vector_dimension=2,
                  levels=1, clusters=2, min_rows=1, M=24, ef_construction=100,
                  delta_rows=5);
        )"));
        const auto description = kikimr.RunCall([&] {
            return session.DescribeTable("/Root/HnswView").ExtractValueSync();
        });
        UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
        const auto indexes = description.GetTableDescription().GetIndexDescriptions();
        UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 1u);
        const auto& index = indexes.front();
        UNIT_ASSERT_VALUES_EQUAL(index.GetIndexType(), EIndexType::GlobalHnsw);
        const auto& vectorSettings = std::get<TKMeansTreeSettings>(index.GetIndexSettings()).Settings;
        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.MinRows, 1u);
        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.M, 24u);
        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.EfConstruction, 100u);
        UNIT_ASSERT_VALUES_EQUAL(vectorSettings.DeltaRows, 5u);
        const auto show = kikimr.RunCall([&] {
            return kikimr.GetQueryClient().ExecuteQuery("SHOW CREATE TABLE `/Root/HnswView`;",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(show.IsSuccess(), show.GetIssues().ToString());
        TResultSetParser createParser(show.GetResultSet(0));
        UNIT_ASSERT(createParser.TryNextRow());
        const TString createText = createParser.ColumnParser(0).GetOptionalUtf8().value_or("");
        UNIT_ASSERT_STRING_CONTAINS(createText, "USING hnsw");
        UNIT_ASSERT_STRING_CONTAINS(createText, "min_rows = 1");
        UNIT_ASSERT_STRING_CONTAINS(createText, "M = 24");
        UNIT_ASSERT_STRING_CONTAINS(createText, "ef_construction = 100");
        UNIT_ASSERT_STRING_CONTAINS(createText, "delta_rows = 5");
        UNIT_ASSERT(!createText.Contains("hnsw_search_candidates"));

        const TString posting = "/Root/HnswView/vidx/indexImplPostingTable";
        ui32 expectedEfSearch = 15;
        ui64 vectorReads = 0;
        auto observer = runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                const auto& read = ev->Get<TEvDataShard::TEvRead>()->Record;
                if (read.HasVectorTopK()) {
                    UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetHnswEfSearch(), expectedEfSearch);
                    ++vectorReads;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(observer); };
        // A query override must reach the shard without changing later defaults.
        for (const bool positive : {false, true, false}) {
            expectedEfSearch = positive ? 50 : 15;
            const ui64 readsBefore = vectorReads;
            const TString query = TStringBuilder()
                << (positive ? "PRAGMA ydb.HNSWEfSearch = \"50\"; " : "")
                << "PRAGMA ydb.KMeansTreeSearchTopSize = \"1\"; "
                << "$q = Knn::ToBinaryStringFloat([" << (positive ? "1000.0f" : "-1000.0f") << ", 32.0f]); "
                << "SELECT pk FROM `/Root/HnswView` VIEW vidx "
                << "ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 1;";
            bool accelerated = false;
            for (ui32 attempt = 0; attempt < 30; ++attempt) {
                const auto result = kikimr.RunCall([&] {
                    return session.ExecuteDataQuery(query,
                        TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(),
                        TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
                });
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), positive ? "[[96u]]" : "[[32u]]");
                ui64 rowsRead = 0;
                for (const auto& phase : NYdb::TProtoAccessor::GetProto(*result.GetStats()).query_phases()) {
                    for (const auto& access : phase.table_access()) {
                        UNIT_ASSERT_C(!access.name().EndsWith("/indexImplLevelTable"),
                            "hnsw VIEW must not traverse k-means clusters");
                        if (access.name() == posting) {
                            rowsRead += access.reads().rows();
                        }
                    }
                }
                if (rowsRead == 1) {
                    accelerated = true;
                    break;
                }
                runtime->SimulateSleep(TDuration::Seconds(1));
            }
            UNIT_ASSERT_C(accelerated, "VIEW vidx did not use its partition-local HNSW graph");
            UNIT_ASSERT_GT(vectorReads, readsBefore);
        }
        expectedEfSearch = 75;
        const ui64 readsBefore = vectorReads;
        const auto direct = kikimr.RunCall([&] {
            return session.ExecuteDataQuery(Q_(R"(
                PRAGMA ydb.HNSWEfSearch = "75";
                $q = Knn::ToBinaryStringFloat([1000.0f, 32.0f]);
                SELECT pk FROM `/Root/HnswView/vidx/indexImplPostingTable`
                ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 1;
            )"), TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(direct.IsSuccess(), direct.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(direct.GetResultSet(0)), "[[96u]]");
        UNIT_ASSERT_GT(vectorReads, readsBefore);
    }

    Y_UNIT_TEST_TWIN(HnswAutomaticRoutingSettings, Prefixed) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true)
            .SetNeedsStatsCollectors(true).SetUseRealThreads(false)};
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto scheme = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(sql).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme(R"(
            CREATE TABLE `/Root/HnswAutomatic` (
                pk Int64 NOT NULL, prefix Uint32, emb String, PRIMARY KEY(pk));
        )");
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswAutomatic` (pk, prefix, emb) VALUES
                (1, 1u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, 1u, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector")),
                (3, 2u, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        scheme(TStringBuilder() << "ALTER TABLE `/Root/HnswAutomatic` ADD INDEX idx GLOBAL USING hnsw ON ("
            << (Prefixed ? "prefix, emb" : "emb") << ") WITH (distance=cosine, vector_type=float, vector_dimension=2, min_rows=1);");
        auto checkSettings = [&](const TString& table) {
            const auto result = kikimr.RunCall([&] { return session.DescribeTable(table).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& proto = NYdb::TProtoAccessor::GetProto(result.GetTableDescription());
            UNIT_ASSERT_VALUES_EQUAL(proto.indexes_size(), 1);
            const auto& settings = proto.indexes(0).global_hnsw_index().vector_settings();
            UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 1);
            UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 1);
            UNIT_ASSERT(!settings.adaptive_clusters());
        };
        checkSettings("/Root/HnswAutomatic");
        const auto query = kikimr.RunCall([&] { return session.ExecuteDataQuery(TStringBuilder()
            << "$q = Knn::ToBinaryStringFloat([1.0f, 0.0f]); SELECT pk FROM `/Root/HnswAutomatic` VIEW idx "
            << (Prefixed ? "WHERE prefix=1u " : "")
            << "ORDER BY Knn::CosineDistance(emb, $q) LIMIT 1;",
            TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync(); });
        UNIT_ASSERT_C(query.IsSuccess(), query.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(query.GetResultSet(0)), "[[1]]");
        scheme("ALTER TABLE `/Root/HnswAutomatic` REBUILD INDEX idx WITH (clusters=1);");
        checkSettings("/Root/HnswAutomatic");
        scheme(R"(
            CREATE TABLE `/Root/HnswAutomaticEmpty` (
                pk Int64 NOT NULL, emb String, PRIMARY KEY(pk),
                INDEX idx GLOBAL USING hnsw ON (emb)
                WITH (distance=cosine, vector_type=float, vector_dimension=2));
        )");
        checkSettings("/Root/HnswAutomaticEmpty");
    }

    Y_UNIT_TEST_TWIN(HnswFeatureFlagCreation, Enabled) {
        NKikimrConfig::TFeatureFlags defaults;
        UNIT_ASSERT(!defaults.GetEnableHnswIndex());
        auto settings = TKikimrSettings();
        if (Enabled) {
            settings.SetEnableHnswIndex(true);
        }
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto check = [&](const auto& status) {
            UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), Enabled, status.GetIssues().ToString());
            if (!Enabled) {
                UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "EnableHnswIndex");
            }
        };
        check(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswInlineFlag` (
                id Uint64 NOT NULL, emb String, PRIMARY KEY(id),
                INDEX vidx GLOBAL USING hnsw ON (emb)
                    WITH (distance=cosine, vector_type=float, vector_dimension=2, levels=1, clusters=2)
            );
        )").ExtractValueSync());
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswAlterFlag` (id Uint64 NOT NULL, emb String, PRIMARY KEY(id));
            CREATE TABLE `/Root/KmeansFlag` (
                id Uint64 NOT NULL, emb String, PRIMARY KEY(id),
                INDEX vidx GLOBAL USING vector_kmeans_tree ON (emb)
                    WITH (distance=cosine, vector_type=float, vector_dimension=2, levels=1, clusters=2)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        check(session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/HnswAlterFlag` ADD INDEX vidx GLOBAL USING hnsw ON (emb)
                WITH (distance=cosine, vector_type=float, vector_dimension=2, levels=1, clusters=2);
        )").ExtractValueSync());
    }

    Y_UNIT_TEST(HnswFeatureFlagExistingIndex) {
        NKikimrConfig::TAppConfig config;
        config.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        config.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(config).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        const auto create = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswFlagExisting` (id Uint64 NOT NULL, emb String, PRIMARY KEY(id));
        )").ExtractValueSync(); });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswFlagExisting` (id, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([2.0f, 5.0f]), "FloatVector")),
                (3, Untag(Knn::ToBinaryStringFloat([3.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        const auto added = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/HnswFlagExisting` ADD INDEX idx GLOBAL USING hnsw ON (emb)
            WITH (similarity=inner_product, vector_type=float, vector_dimension=2,
                  levels=1, clusters=2, min_rows=1);
        )").ExtractValueSync(); });
        UNIT_ASSERT_C(added.IsSuccess(), added.GetIssues().ToString());
        TString expected = "[[3u]]";
        auto read = [&] {
            const auto result = kikimr.RunCall([&] { return session.ExecuteDataQuery(R"(
                $q = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
                SELECT id FROM `/Root/HnswFlagExisting` VIEW idx
                ORDER BY Knn::InnerProductSimilarity(emb, $q) DESC LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(),
                TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), expected);
            return result;
        };
        const TString posting = "/Root/HnswFlagExisting/idx/indexImplPostingTable";
        AssertTableReads(read(), posting, 1);
        NKikimrConfig::TAppConfig update;
        update.MutableFeatureFlags()->SetEnableHnswIndex(false);
        update.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).UpdateRuntimeFlags(update.GetFeatureFlags());
            const auto sender = runtime.AllocateEdgeActor(node);
            for (const auto service : {MakeKqpProxyID(runtime.GetNodeId(node)), MakeKqpCompileServiceID(runtime.GetNodeId(node))}) {
                auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
                *request->Record.MutableConfig() = update;
                runtime.Send(service, sender, request.Release());
                UNIT_ASSERT(runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender, TDuration::Seconds(10)));
            }
        }
        const auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *request->Record.MutableConfig() = update;
        runtime.SendToPipe(Tests::SchemeRoot, sender, request.Release());
        UNIT_ASSERT(runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender, TDuration::Seconds(10)));
        AssertTableReads(read(), posting, 3);
        const auto writeDisabled = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswFlagExisting` (id, emb) VALUES
                (4, Untag(Knn::ToBinaryStringFloat([4.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(writeDisabled.IsSuccess(), writeDisabled.GetIssues().ToString());
        expected = "[[4u]]";
        AssertTableReads(read(), posting, 4);
        const auto rebuild = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(
            "ALTER TABLE `/Root/HnswFlagExisting` REBUILD INDEX idx;").ExtractValueSync(); });
        UNIT_ASSERT_C(!rebuild.IsSuccess(), "HNSW rebuild was accepted with the feature disabled");
        UNIT_ASSERT_STRING_CONTAINS(rebuild.GetIssues().ToString(), "EnableHnswIndex");
        AssertTableReads(read(), posting, 4);
        const auto drop = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(
            "ALTER TABLE `/Root/HnswFlagExisting` DROP INDEX idx;").ExtractValueSync(); });
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
    }

    Y_UNIT_TEST(HnswEfSearchPragmaValidation) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        for (const TString& value : {"0", "1001", "-1", "invalid"}) {
            const auto result = session.ExplainDataQuery(
                "PRAGMA ydb.HNSWEfSearch = \"" + value + "\"; SELECT 1;").ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), value);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "HNSWEfSearch");
        }
        for (const TString& value : {"1", "15", "1000"}) {
            const auto result = session.ExplainDataQuery(
                "PRAGMA ydb.HNSWEfSearch = \"" + value + "\"; SELECT 1;").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(HnswCacheTracksLeaderWrites) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto execute = [&](const TString& query) {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };
        auto scheme = [&](const TString& query) {
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        scheme(Q_(R"(
            CREATE TABLE `/Root/HnswWrites` (
                pk Int64 NOT NULL,
                emb String,
                PRIMARY KEY (pk)
            );
        )"));
        execute(Q_(R"(
            UPSERT INTO `/Root/HnswWrites` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector")),
                (3, Untag(Knn::ToBinaryStringFloat([0.0f, -1.0f]), "FloatVector"));
        )"));
        scheme(Q_(R"(
            ALTER TABLE `/Root/HnswWrites`
                ADD INDEX index
                GLOBAL USING hnsw
                ON (emb)
                WITH (similarity=cosine, vector_type="float", vector_dimension=2,
                      levels=1, clusters=2, min_rows=1);
        )"));

        const TString search(Q_(R"(
            $target = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
            SELECT pk FROM `/Root/HnswWrites` VIEW index
            ORDER BY Knn::CosineDistance(emb, $target)
            LIMIT 1;
        )"));
        auto nearest = [&] {
            auto result = session.ExecuteDataQuery(search,
                TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };

        // Exercise the eagerly-built graph before updating it through normal
        // DataShard writes.
        nearest();
        nearest();

        execute(Q_(R"(
            UPSERT INTO `/Root/HnswWrites` (pk, emb) VALUES
                (4, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )"));
        UNIT_ASSERT_VALUES_EQUAL(nearest(), "[[4]]");

        execute(Q_(R"(
            UPSERT INTO `/Root/HnswWrites` (pk, emb) VALUES
                (4, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )"));
        UNIT_ASSERT_VALUES_EQUAL(nearest(), "[[2]]");

        execute(Q_(R"(DELETE FROM `/Root/HnswWrites` WHERE pk = 2;)"));
        UNIT_ASSERT_UNEQUAL(nearest(), "[[2]]");

        execute(Q_(R"(
            UPSERT INTO `/Root/HnswWrites` (pk, emb) VALUES
                (1, NULL),
                (3, "malformed");
        )"));
        UNIT_ASSERT_VALUES_EQUAL(nearest(), "[[4]]");
    }

    Y_UNIT_TEST_TWIN(HnswMvccPendingCommitAndRollback, Commit) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto reader = db.CreateSession().GetValueSync().GetSession();
        auto writer = db.CreateSession().GetValueSync().GetSession();
        auto scheme = [&](const TString& query) {
            auto result = reader.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme(Q_(R"(
            CREATE TABLE `/Root/HnswPending` (pk Int64 NOT NULL, emb String, PRIMARY KEY (pk));
        )"));
        auto initial = reader.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/HnswPending` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(initial.IsSuccess(), initial.GetIssues().ToString());
        scheme(Q_(R"(
            ALTER TABLE `/Root/HnswPending` ADD INDEX index GLOBAL USING hnsw ON (emb)
                WITH (similarity=cosine, vector_type="float", vector_dimension=2,
                    levels=1, clusters=2, min_rows=1,
                    delta_rows=1000);
        )"));
        const TString search(Q_(R"(
            $target = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
            SELECT pk FROM `/Root/HnswPending` VIEW index
                ORDER BY Knn::CosineDistance(emb, $target) LIMIT 1;
        )"));
        auto nearest = [&] {
            auto result = reader.ExecuteDataQuery(search,
                TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };
        UNIT_ASSERT_VALUES_EQUAL(nearest(), "[[1]]");
        auto pending = writer.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/HnswPending` (pk, emb) VALUES
                (3, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
        UNIT_ASSERT_C(pending.IsSuccess(), pending.GetIssues().ToString());
        auto tx = pending.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT_VALUES_EQUAL(nearest(), "[[1]]");
        // Own writes use the transaction-aware table path until ANN conflict
        // observation supports this context.
        auto own = writer.ExecuteDataQuery(search, TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(own.IsSuccess(), own.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(own.GetResultSet(0)), "[[3]]");
        if (Commit) {
            const auto result = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            const auto result = tx->Rollback().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(nearest(), Commit ? "[[3]]" : "[[1]]");
    }

    Y_UNIT_TEST(HnswSnapshotConsistencyAcrossWrites) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto snapshotSession = db.CreateSession().GetValueSync().GetSession();
        auto headSession = db.CreateSession().GetValueSync().GetSession();

        auto scheme = [&](const TString& query) {
            auto result = headSession.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto head = [&](const TString& query) {
            auto result = headSession.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };

        scheme(Q_(R"(
            CREATE TABLE `/Root/HnswMvcc` (
                pk Int64 NOT NULL,
                emb String,
                PRIMARY KEY (pk)
            );
        )"));
        head(Q_(R"(
            UPSERT INTO `/Root/HnswMvcc` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )"));
        scheme(Q_(R"(
            ALTER TABLE `/Root/HnswMvcc`
                ADD INDEX index
                GLOBAL USING hnsw
                ON (emb)
                WITH (similarity=cosine, vector_type="float", vector_dimension=2,
                      levels=1, clusters=2, min_rows=1);
        )"));

        const TString search(Q_(R"(
            $target = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
            SELECT pk FROM `/Root/HnswMvcc` VIEW index
            ORDER BY Knn::CosineDistance(emb, $target)
            LIMIT 1;
        )"));
        auto oldResult = snapshotSession.ExecuteDataQuery(search,
            TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
        UNIT_ASSERT_C(oldResult.IsSuccess(), oldResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(oldResult.GetResultSet(0)), "[[1]]");
        auto snapshot = oldResult.GetTransaction();
        UNIT_ASSERT(snapshot);

        head(Q_(R"(
            UPSERT INTO `/Root/HnswMvcc` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )"));
        auto headResult = headSession.ExecuteDataQuery(search,
            TTxControl::BeginTx(TTxSettings::OnlineRO(
                TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(headResult.IsSuccess(), headResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(headResult.GetResultSet(0)), "[[2]]");

        oldResult = snapshotSession.ExecuteDataQuery(search,
            TTxControl::Tx(*snapshot).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(oldResult.IsSuccess(), oldResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(oldResult.GetResultSet(0)), "[[1]]");
    }

    void TestHnswFallback(ui64 cacheBytes, ui64 minRows) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(cacheBytes);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(cacheBytes);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto scheme = [&](const TString& query) {
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto execute = [&](const TString& query) {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };

        scheme(Q_(R"(
            CREATE TABLE `/Root/HnswFallback` (
                pk Int64 NOT NULL,
                emb String,
                PRIMARY KEY (pk)
            );
        )"));
        execute(Q_(R"(
            UPSERT INTO `/Root/HnswFallback` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )"));
        scheme(Q1_(std::format(R"(
            ALTER TABLE `/Root/HnswFallback`
                ADD INDEX index
                GLOBAL USING hnsw
                ON (emb)
                WITH (similarity=cosine, vector_type="float", vector_dimension=2,
                      levels=1, clusters=2, min_rows={});
        )", minRows)));

        const TString search(Q_(R"(
            $target = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
            SELECT pk FROM `/Root/HnswFallback` VIEW index
            ORDER BY Knn::CosineDistance(emb, $target)
            LIMIT 1;
        )"));
        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            auto result = session.ExecuteDataQuery(search,
                TTxControl::BeginTx(TTxSettings::OnlineRO(
                    TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1]]");
        }
    }

    Y_UNIT_TEST(HnswFallbackBelowMinRows) {
        TestHnswFallback(64_MB, 3);
    }

    Y_UNIT_TEST(HnswFallbackWithCacheDisabled) {
        TestHnswFallback(0, 1);
    }

    Y_UNIT_TEST(HnswFallbackWithInsufficientMemory) {
        TestHnswFallback(1, 1);
    }

    Y_UNIT_TEST_TWIN(HnswInlineIndexMaintainsEmbeddings, Covered) {
        NKikimrConfig::TAppConfig config;
        config.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        config.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(config).SetEnableHnswIndex(true).SetNeedsStatsCollectors(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/HnswInlineDml` (
                pk Int64 NOT NULL, emb String, payload String, PRIMARY KEY(pk),
                INDEX idx GLOBAL USING hnsw ON (emb) )" << (Covered ? "COVER (payload) " : "") << R"(
                WITH (distance=euclidean, vector_type=float, vector_dimension=2,
                    levels=1, clusters=1, min_rows=1));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto write = [&](const TString& sql) {
            const auto result = ExecuteDataQuery(session, sql);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto check = [&](i64 key, const TString& payload) {
            const auto result = session.ExecuteDataQuery(R"(
                $q = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
                SELECT pk, payload FROM `/Root/HnswInlineDml` VIEW idx
                ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            TResultSetParser rows(result.GetResultSet(0));
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("pk").GetInt64(), key);
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("payload").GetOptionalString().value(), payload);
            UNIT_ASSERT(!rows.TryNextRow());
        };
        write(R"(
            INSERT INTO `/Root/HnswInlineDml` (pk, emb, payload) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"), "one"),
                (2, Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector"), "two")
            RETURNING pk;
        )");
        check(1, "one");
        write(R"(
            UPDATE `/Root/HnswInlineDml` SET emb=Untag(Knn::ToBinaryStringFloat([3.0f, 0.0f]), "FloatVector")
            WHERE pk=1;
            UPDATE `/Root/HnswInlineDml` SET payload="updated" WHERE pk=2;
        )");
        check(2, "updated");
        write(R"(
            REPLACE INTO `/Root/HnswInlineDml` (pk, emb, payload) VALUES
                (2, Untag(Knn::ToBinaryStringFloat([9.0f, 0.0f]), "FloatVector"), "replaced");
        )");
        check(1, "one");
        write(R"(
            UPSERT INTO `/Root/HnswInlineDml` (pk, emb, payload) VALUES
                (3, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"), "three");
        )");
        check(3, "three");
        write("DELETE FROM `/Root/HnswInlineDml` WHERE pk=3;");
        check(1, "one");
        write("UPDATE `/Root/HnswInlineDml` SET emb=NULL WHERE pk=1;");
        check(2, "replaced");
    }

    Y_UNIT_TEST(HnswUnsupportedVectorTypeUsesExactScan) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswUint8` (
                pk Int64 NOT NULL, emb String, PRIMARY KEY(pk),
                INDEX idx GLOBAL USING hnsw ON (emb)
                WITH (similarity=inner_product, vector_type=uint8, vector_dimension=2, levels=1, clusters=1));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto write = ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswUint8` (pk, emb) VALUES (1, "\x01\x00\x02"), (2, "\x00\x01\x02");
        )");
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        const auto result = session.ExecuteDataQuery(R"(
            SELECT pk FROM `/Root/HnswUint8` VIEW idx
            ORDER BY Knn::InnerProductSimilarity(emb, "\x01\x00\x02") DESC LIMIT 1;
        )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1]]");
    }

}

Y_UNIT_TEST_SUITE(KqpHnswReplicaReadiness) {
    void CheckInitialBuildReplicaReadiness(bool reboot, bool expire, bool disable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        appConfig.MutableSchemeShardConfig()->SetHnswIndexBuildWaitTimeoutSeconds(expire ? 30 : 120);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true)
            .SetNeedsStatsCollectors(true).SetEnableForceFollowers(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        const auto create = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswInitialReplicas` (pk Int64 NOT NULL, emb String, PRIMARY KEY(pk));
        )").ExtractValueSync(); });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswInitialReplicas` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        TVector<TAutoPtr<IEventHandle>> held;
        ui64 owner = 0, buildId = 0;
        ui64 round = 0;
        auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvIndexBuilder::TEvCreateRequest::EventType) {
                auto* index = event->Get<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>()->Record.MutableSettings()->mutable_index();
                index->mutable_global_hnsw_index()->mutable_posting_table_settings()
                    ->mutable_read_replicas_settings()->set_per_az_read_replicas_count(1);
            } else if (event->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvSnapshotRequest::EventType) {
                owner = event->Get<NDataShard::TEvHnswCache::TEvSnapshotRequest>()->Record.GetTableOwnerId();
            } else if (event->GetTypeRewrite() == NSchemeShard::TEvPrivate::TEvHnswReplicaStatus::EventType) {
                const auto& result = *event->Get<NSchemeShard::TEvPrivate::TEvHnswReplicaStatus>();
                if (result.IndexBuild) {
                    buildId = result.Record.GetOperationTxId();
                    round = result.Record.GetRound();
                }
            } else if (event->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvAcquired::EventType
                    && event->Get<NDataShard::TEvHnswCache::TEvAcquired>()->Graph) {
                auto* shard = dynamic_cast<NDataShard::TDataShard*>(runtime.FindActor(event->Recipient));
                if (shard && shard->IsFollower()) {
                    held.push_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        auto build = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/HnswInitialReplicas` ADD INDEX idx GLOBAL USING hnsw ON (emb)
            WITH (similarity=cosine, vector_type=float, vector_dimension=2,
                  levels=1, clusters=2, min_rows=1);
        )");
        runtime.WaitFor("initial build replica readiness barrier", [&] { return owner && buildId && round && !held.empty(); });
        runtime.SimulateSleep(TDuration::Seconds(expire ? 20 : 2));
        UNIT_ASSERT_C(!build.HasValue(), "Build completed before a configured follower installed its graph");
        if (disable) {
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                runtime.GetAppData(node).FeatureFlags.SetEnableHnswIndex(false);
            }
        }
        const auto sender = runtime.AllocateEdgeActor();
        auto state = [&] {
            runtime.SendToPipe(owner, sender, new NSchemeShard::TEvIndexBuilder::TEvGetRequest("/Root", buildId));
            const auto result = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvGetResponse>(sender);
            return result->Get()->Record.GetIndexBuild().GetState();
        };
        if (reboot) {
            RebootTablet(runtime, owner, sender);
        }
        if (expire) {
            runtime.SimulateSleep(TDuration::Seconds(15));
            UNIT_ASSERT_VALUES_EQUAL(state(), Ydb::Table::IndexBuildState::STATE_DONE);
        } else {
            runtime.SimulateSleep(TDuration::Seconds(2));
            // A restart may close the SQL caller's pipe. Verify the durable
            // operation independently of that transport-level result.
            UNIT_ASSERT_VALUES_EQUAL(state(), Ydb::Table::IndexBuildState::STATE_APPLYING);
            runtime.SetObserverFunc(observer);
            for (auto& event : held) {
                const auto node = event->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
                runtime.Send(event.Release(), node);
            }
            held.clear();
        }
        for (ui32 attempt = 0; attempt < 30 && state() != Ydb::Table::IndexBuildState::STATE_DONE; ++attempt) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(state(), Ydb::Table::IndexBuildState::STATE_DONE);
        const auto result = runtime.WaitFuture(build);
        if (reboot) {
            UNIT_ASSERT_C(result.IsSuccess() || result.GetIssues().ToString().find("Connection to tablet was lost") != std::string::npos,
                result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(HnswInitialBuildWaitsForReplicaBootstrap, Reboot) {
        CheckInitialBuildReplicaReadiness(Reboot, false, false);
    }

    Y_UNIT_TEST_TWIN(HnswInitialBuildReadinessDeadlineSurvivesRestart, DisableFeature) {
        CheckInitialBuildReplicaReadiness(true, true, DisableFeature);
    }


    Y_UNIT_TEST_QUAD(HnswReplicaAlterDownloadsAndSharesGraphs, SplitDuringWait, MultipleAZs) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true)
            .SetNodeCount(MultipleAZs ? 3 : 1).SetNeedsStatsCollectors(true).SetEnableForceFollowers(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto writer = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto scheme = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(sql).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme("CREATE TABLE `/Root/HnswDownload` (pk Int64 NOT NULL, emb String, PRIMARY KEY(pk));");
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswDownload` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")),
                (2, Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        scheme(R"(
            ALTER TABLE `/Root/HnswDownload` ADD INDEX idx GLOBAL USING hnsw ON (emb)
            WITH (similarity=inner_product, vector_type=float, vector_dimension=2,
                  levels=1, clusters=2, min_rows=1, delta_rows=1);
        )");
        TVector<TAutoPtr<IEventHandle>> held;
        THashMap<TString, TActorId> delayedReplicaByGeneration;
        bool releaseGraphs = false;
        THashMap<TString, std::shared_ptr<NDataShard::THnswIndex>> generations;
        ui32 acquisitions = 0;
        THashMap<ui64, TMap<ui32, THashSet<TActorId>>> readyFollowers;
        THashMap<TString, ui32> downloads;
        bool hideFollower = false;
        const ui32 followers = MultipleAZs ? 9 : 3;
        const TString posting = "/Root/HnswDownload/idx/indexImplPostingTable";
        const auto sender = runtime.AllocateEdgeActor();
        auto shards = GetTableShards(&kikimr.GetTestServer(), sender, posting);
        ui64 localBuilds = 0, leaderBuilds = 0;
        constexpr ui32 hnswBuildResultEvent = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 34;
        const auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvAcquired::EventType) {
                const auto& result = *ev->Get<NDataShard::TEvHnswCache::TEvAcquired>();
                if (result.Graph) {
                    auto [it, inserted] = generations.emplace(TStringBuilder() << result.Key << ":node:" << ev->Recipient.NodeId(), result.Graph);
                    UNIT_ASSERT(inserted || it->second->SharesGraphWith(*result.Graph));
                    ++acquisitions;
                    auto [delayed, first] = delayedReplicaByGeneration.emplace(result.Key, ev->Recipient);
                    Y_UNUSED(first);
                    if (!releaseGraphs && delayed->second == ev->Recipient) {
                        held.push_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            } else if (ev->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvStatusResponse::EventType) {
                const auto& status = ev->Get<NDataShard::TEvHnswCache::TEvStatusResponse>()->Record;
                if (status.GetFollowerId() && status.GetReady())
                    readyFollowers[status.GetTabletId()][status.GetLeaderGeneration()].insert(ev->Sender);
            } else if (ev->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvChunkRequest::EventType) {
                const auto& request = ev->Get<NDataShard::TEvHnswCache::TEvChunkRequest>()->Record;
                if (request.GetKey().find(":journal:") == TString::npos && request.GetOffset() == 0)
                    ++downloads[TStringBuilder() << request.GetKey() << ":node:" << ev->Sender.NodeId()];
            } else if (hideFollower && ev->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType) {
                auto& info = *ev->Get<TEvStateStorage::TEvInfo>();
                if (info.Followers.size() == followers)
                    info.Followers.pop_back();
            } else if (ev->GetTypeRewrite() == hnswBuildResultEvent) {
                const auto* shard = dynamic_cast<NDataShard::TDataShard*>(runtime.FindActor(ev->Recipient));
                if (shard && shard->IsFollower())
                    ++localBuilds;
                else if (shard)
                    ++leaderBuilds;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        auto alter = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/HnswDownload/idx/indexImplPostingTable`
            SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
        )");
        runtime.WaitFor("follower graph acquisition", [&] { return !held.empty() && acquisitions >= followers; });
        runtime.SimulateSleep(TDuration::Seconds(5));
        UNIT_ASSERT_C(!alter.HasValue(), TStringBuilder() << "ALTER completed while a follower had no graph: "
            << (alter.HasValue() ? alter.GetValueSync().GetIssues().ToString() : std::string()));
        UNIT_ASSERT_VALUES_EQUAL_C(leaderBuilds, 0, "Replica placement rebuilt the leader's compatible graph");
        if (MultipleAZs && !SplitDuringWait) {
            const auto previous = acquisitions;
            RebootTablet(runtime, shards.front(), sender);
            runtime.WaitFor("follower downloads after leader restart", [&] {
                return acquisitions >= previous + followers;
            });
            UNIT_ASSERT_C(!alter.HasValue(), "ALTER ignored an unfinished replacement incarnation");
        }
        hideFollower = SplitDuringWait;
        const auto duringTransfer = kikimr.RunCall([&] { return ExecuteDataQuery(writer, R"(
            UPSERT INTO `/Root/HnswDownload` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector")),
                (3, Untag(Knn::ToBinaryStringFloat([3.0f, 0.0f]), "FloatVector"));
            DELETE FROM `/Root/HnswDownload` WHERE pk = 2;
        )"); });
        UNIT_ASSERT_C(duringTransfer.IsSuccess(), duringTransfer.GetIssues().ToString());
        const auto uncommitted = kikimr.RunCall([&] { return writer.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/HnswDownload` (pk, emb) VALUES
                (99, Untag(Knn::ToBinaryStringFloat([1000.0f, 0.0f]), "FloatVector"));
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync(); });
        UNIT_ASSERT_C(uncommitted.IsSuccess(), uncommitted.GetIssues().ToString());
        auto transaction = *uncommitted.GetTransaction();
        const auto rollback = kikimr.RunCall([&] { return transaction.Rollback().ExtractValueSync(); });
        UNIT_ASSERT_C(rollback.IsSuccess(), rollback.GetIssues().ToString());
        if (SplitDuringWait) {
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
            // Wait for the source shard's finalized statistics before splitting.
            runtime.SimulateSleep(TDuration::Seconds(60));
            const auto status = kikimr.RunCall([&] {
                return kikimr.GetTestClient().SplitTable(posting, shards.front(), ui64(2));
            });
            UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
            shards = GetTableShards(&kikimr.GetTestServer(), sender, posting);
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2);
        }
        hideFollower = true;
        releaseGraphs = true;
        for (auto& event : held) {
            const ui32 nodeIndex = event->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            runtime.Send(event.Release(), nodeIndex);
        }
        held.clear();
        runtime.SimulateSleep(TDuration::Seconds(10));
        UNIT_ASSERT_C(!alter.HasValue(), "ALTER accepted a topology missing a configured follower");
        hideFollower = false;
        const auto result = runtime.WaitFuture(alter);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        for (const auto shard : shards) {
            const auto& incarnations = readyFollowers[shard];
            UNIT_ASSERT(!incarnations.empty());
            UNIT_ASSERT_VALUES_EQUAL(incarnations.rbegin()->second.size(), followers);
        }
        UNIT_ASSERT_VALUES_EQUAL(localBuilds, 0);
        if (MultipleAZs && !SplitDuringWait) {
            UNIT_ASSERT_GE(downloads.size(), 2);
            for (const auto& [_, count] : downloads)
                UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }
        for (auto it = generations.begin(); it != generations.end(); ++it) {
            for (auto other = std::next(it); other != generations.end(); ++other)
                UNIT_ASSERT(!it->second->SharesGraphWith(*other->second));
        }
        const auto read = kikimr.RunCall([&] { return session.ExecuteDataQuery(R"(
            $q = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
            SELECT pk FROM `/Root/HnswDownload/idx/indexImplPostingTable`
            ORDER BY Knn::InnerProductSimilarity(emb, $q) DESC LIMIT 1;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx(),
            TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync(); });
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read.GetResultSet(0)), "[[3]]");
        AssertTableReads(read, "/Root/HnswDownload/idx/indexImplPostingTable", 1);

        if (!MultipleAZs && !SplitDuringWait) {
            runtime.SimulateSleep(TDuration::Seconds(5));
            const auto buildsBeforeReplicaChanges = leaderBuilds;
            for (ui32 count : {1u, 2u, 1u}) {
                readyFollowers.clear();
                scheme(TStringBuilder() << "ALTER TABLE `" << posting
                    << "` SET (READ_REPLICAS_SETTINGS = \"PER_AZ:" << count << "\");");
                UNIT_ASSERT(!readyFollowers[shards.front()].empty());
                UNIT_ASSERT_VALUES_EQUAL(readyFollowers[shards.front()].rbegin()->second.size(), count);
                UNIT_ASSERT_VALUES_EQUAL_C(leaderBuilds, buildsBeforeReplicaChanges,
                    "Replica-count changes rebuilt the leader graph");
            }
        }
        runtime.SimulateSleep(TDuration::Seconds(5));
        const auto beforeRefresh = acquisitions;
        const ui32 activeFollowers = !MultipleAZs && !SplitDuringWait ? 1u : followers;
        const auto update = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswDownload` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([100.0f, 0.0f]), "FloatVector")),
                (3, Untag(Knn::ToBinaryStringFloat([4.0f, 0.0f]), "FloatVector")),
                (4, Untag(Knn::ToBinaryStringFloat([5.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
        runtime.WaitFor("followers acquiring the leader's replacement graph", [&] {
            return acquisitions >= beforeRefresh + activeFollowers;
        });
        runtime.SimulateSleep(TDuration::Seconds(5));
        for (ui32 i = 0; i < 16; ++i) {
            const auto current = kikimr.RunCall([&] { return session.ExecuteDataQuery(R"(
                $q = Knn::ToBinaryStringFloat([1.0f, 0.0f]);
                SELECT pk FROM `/Root/HnswDownload/idx/indexImplPostingTable`
                ORDER BY Knn::InnerProductSimilarity(emb, $q) DESC LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync(); });
            UNIT_ASSERT_C(current.IsSuccess(), current.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(current.GetResultSet(0)), "[[1]]");
        }
        UNIT_ASSERT_VALUES_EQUAL(localBuilds, 0);
    }


    Y_UNIT_TEST_TWIN(HnswReplicaAlterTimeoutSurvivesRestart, Reboot) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        appConfig.MutableSchemeShardConfig()->SetHnswIndexBuildWaitTimeoutSeconds(30);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true)
            .SetNeedsStatsCollectors(true).SetEnableForceFollowers(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto scheme = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(sql).ExtractValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        scheme("CREATE TABLE `/Root/HnswReplicaTimeout` (pk Int64 NOT NULL, emb String, PRIMARY KEY(pk));");
        const auto write = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswReplicaTimeout` (pk, emb) VALUES
                (1, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )"); });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        scheme(R"(
            ALTER TABLE `/Root/HnswReplicaTimeout` ADD INDEX idx GLOBAL USING hnsw ON (emb)
            WITH (distance=euclidean, vector_type=float, vector_dimension=2,
                  levels=1, clusters=2, min_rows=1);
        )");
        ui64 owner = 0, txId = 0;
        const auto observer = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvSnapshotRequest::EventType)
                owner = ev->Get<NDataShard::TEvHnswCache::TEvSnapshotRequest>()->Record.GetTableOwnerId();
            if (ev->GetTypeRewrite() == NSchemeShard::TEvPrivate::TEvHnswReplicaStatus::EventType)
                txId = ev->Get<NSchemeShard::TEvPrivate::TEvHnswReplicaStatus>()->Record.GetOperationTxId();
            if (ev->GetTypeRewrite() == NDataShard::TEvHnswCache::TEvSnapshotResponse::EventType)
                return TTestActorRuntime::EEventAction::DROP;
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(observer); };
        auto alter = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/HnswReplicaTimeout/idx/indexImplPostingTable`
            SET (READ_REPLICAS_SETTINGS = "PER_AZ:1");
        )");
        runtime.WaitFor("replica readiness operation", [&] { return owner && txId; });
        runtime.SimulateSleep(TDuration::Seconds(20));
        UNIT_ASSERT(!alter.HasValue());
        const auto sender = runtime.AllocateEdgeActor();
        if (Reboot) {
            RebootTablet(runtime, owner, sender);
        }
        runtime.SimulateSleep(TDuration::Seconds(15));
        auto checkFailure = [&] {
            runtime.SendToPipe(owner, sender, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(txId));
            const auto result = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), Ydb::StatusIds::UNAVAILABLE);
            UNIT_ASSERT_STRING_CONTAINS(result->Get()->Record.GetError(), "settings were applied");
        };
        checkFailure();
        if (Reboot) {
            RebootTablet(runtime, owner, sender);
            checkFailure();
        }
        const auto result = runtime.WaitFuture(alter);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::UNAVAILABLE);
        if (!Reboot) {
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "settings were applied");
        }
    }


}

} // namespace NKikimr::NKqp
