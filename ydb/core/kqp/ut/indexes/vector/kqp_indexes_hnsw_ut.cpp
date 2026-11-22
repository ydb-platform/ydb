#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/string/subst.h>

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
    Y_UNIT_TEST(HnswMetadataRoundTrip) {
        NKikimrSchemeOp::TIndexDescription index;
        index.SetName("vidx");
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
        index.SetState(NKikimrSchemeOp::EIndexStateReady);
        index.SetPathOwnerId(42);
        index.SetLocalPathId(99);
        index.SetSchemaVersion(7);
        index.AddKeyColumnNames("embedding");
        index.AddDataColumnNames("payload");
        auto& settings = *index.MutableHnswIndexDescription()->MutableSettings();
        settings.set_levels(1);
        settings.set_clusters(2);
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(2);
        settings.mutable_settings()->set_m(24);
        settings.mutable_settings()->set_ef_construction(100);
        settings.mutable_settings()->set_min_rows(1);
        settings.mutable_settings()->set_delta_rows(5);
        const NYql::TIndexDescription metadata(index);
        const auto& actual = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(metadata.SpecializedIndexDescription);
        UNIT_ASSERT_VALUES_EQUAL(actual.GetSettings().SerializeAsString(), settings.SerializeAsString());
        NKikimrKqp::TIndexDescriptionProto serialized;
        metadata.ToMessage(&serialized);
        const NYql::TIndexDescription restored(&serialized);
        UNIT_ASSERT(restored.Type == NYql::TIndexDescription::EType::GlobalSyncHnsw);
        UNIT_ASSERT_VALUES_EQUAL(restored.PathOwnerId, 42u);
        UNIT_ASSERT_VALUES_EQUAL(restored.LocalPathId, 99u);
        UNIT_ASSERT_VALUES_EQUAL(restored.SchemaVersion, 7u);
        UNIT_ASSERT(restored.KeyColumns == metadata.KeyColumns);
        UNIT_ASSERT(restored.DataColumns == metadata.DataColumns);
        UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(
            restored.SpecializedIndexDescription).GetSettings().SerializeAsString(), settings.SerializeAsString());
    }

    Y_UNIT_TEST_TWIN(HnswRejectsMOne, OnlineBuild) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        if (OnlineBuild) {
            const auto create = session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/HnswMOne` (pk Uint64 NOT NULL, emb String, PRIMARY KEY(pk));").ExtractValueSync();
            UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        }
        const TString definition = "INDEX idx GLOBAL USING hnsw ON (emb) "
            "WITH (distance=euclidean, vector_type=float, vector_dimension=2, clusters=1, levels=1, M=1)";
        const TString query = OnlineBuild
            ? "ALTER TABLE `/Root/HnswMOne` ADD " + definition + ";"
            : "CREATE TABLE `/Root/HnswMOne` (pk Uint64 NOT NULL, emb String, PRIMARY KEY(pk), " + definition + ");";
        const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "M=1 was accepted");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "between 2 and 100");
    }

    Y_UNIT_TEST_TWIN(HnswMultipleIndexesMaintainIndependentEmbeddings, EnableVectorSearchActor) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableVectorSearchActor(EnableVectorSearchActor);
        config.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        config.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(config).SetEnableHnswIndex(true)
            .SetNeedsStatsCollectors(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        const auto created = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswMultiple` (
                pk Uint64 NOT NULL, image String, text String, PRIMARY KEY(pk),
                INDEX image_idx GLOBAL USING hnsw ON (image)
                    WITH (distance=cosine, vector_type=float, vector_dimension=2,
                          levels=1, clusters=1, min_rows=1, M=16),
                INDEX text_idx GLOBAL USING hnsw ON (text)
                    WITH (similarity=inner_product, vector_type=float, vector_dimension=2,
                          levels=1, clusters=1, min_rows=1, M=24));
        )").ExtractValueSync(); });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto write = [&](const TString& sql) {
            const auto result = kikimr.RunCall([&] { return ExecuteDataQuery(session, sql); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        auto check = [&](const TString& index, const TString& score, ui64 expected) {
            const TString posting = "/Root/HnswMultiple/" + index + "/indexImplPostingTable";
            const TString expectedYson = TStringBuilder() << "[[" << expected << "u]]";
            for (ui32 attempt = 0; attempt < 30; ++attempt) {
                const auto result = kikimr.RunCall([&] { return session.ExecuteDataQuery(
                    "$q = Knn::ToBinaryStringFloat([1.0f, 0.0f]); SELECT pk FROM `/Root/HnswMultiple` VIEW "
                        + index + " ORDER BY " + score + " LIMIT 1;",
                    TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(),
                    TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync(); });
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), expectedYson);
                ui64 readRows = 0;
                for (const auto& phase : NYdb::TProtoAccessor::GetProto(*result.GetStats()).query_phases()) {
                    for (const auto& access : phase.table_access()) {
                        if (access.name() == posting) {
                            readRows += access.reads().rows();
                        }
                    }
                }
                if (readRows == 1) {
                    return;
                }
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            UNIT_FAIL("HNSW index did not become ready: " << index);
        };
        const TString imageScore = "Knn::CosineDistance(image, $q)";
        const TString textScore = "Knn::InnerProductSimilarity(text, $q) DESC";
        write(R"(
            INSERT INTO `/Root/HnswMultiple` (pk, image, text) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"),
                     Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector")),
                (2u, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector"),
                     Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector")),
                (3u, Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"),
                     Untag(Knn::ToBinaryStringFloat([-1.0f, 0.0f]), "FloatVector"));
        )");
        check("image_idx", imageScore, 1);
        check("text_idx", textScore, 2);
        write(R"(
            UPSERT INTO `/Root/HnswMultiple` (pk, image, text) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([0.0f, 1.0f]), "FloatVector"),
                     Untag(Knn::ToBinaryStringFloat([3.0f, 0.0f]), "FloatVector"));
            UPSERT INTO `/Root/HnswMultiple` (pk, image) VALUES
                (2u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"));
        )");
        check("image_idx", imageScore, 2);
        check("text_idx", textScore, 1);
        write("DELETE FROM `/Root/HnswMultiple` WHERE pk = 1u;");
        check("image_idx", imageScore, 2);
        check("text_idx", textScore, 2);
    }

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

        // SnapshotRW takes read locks, so it must retain the exact scan path
        // while preserving its old result across a concurrent write.
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
        AssertTableReads(snapshot, "/Root/HnswFullRange/index/indexImplPostingTable", 3);
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
        TVector<std::pair<ui32, ui32>> observedEfSearch;
        auto observer = runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                const auto& read = ev->Get<TEvDataShard::TEvRead>()->Record;
                if (read.HasVectorTopK()) {
                    observedEfSearch.emplace_back(read.GetVectorTopK().GetHnswEfSearch(), expectedEfSearch);
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
        for (const auto& [actual, expected] : observedEfSearch) {
            UNIT_ASSERT_VALUES_EQUAL(actual, expected);
        }
    }

    Y_UNIT_TEST_TWIN(HnswAutomaticRoutingSettings, Prefixed) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(64_MB);
        appConfig.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(64_MB);
        TKikimrRunner kikimr{TKikimrSettings(appConfig).SetEnableHnswIndex(true).SetEnableRebuildIndex(true)
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
        bool ready = false;
        for (ui32 attempt = 0; attempt < 30; ++attempt) {
            const auto result = read();
            ui64 rowsRead = 0;
            for (const auto& phase : NYdb::TProtoAccessor::GetProto(*result.GetStats()).query_phases()) {
                for (const auto& access : phase.table_access()) {
                    if (access.name() == posting) {
                        rowsRead += access.reads().rows();
                    }
                }
            }
            if (rowsRead == 1) {
                ready = true;
                break;
            }
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(ready, "HNSW graph did not become ready before disabling the feature");
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

    Y_UNIT_TEST_QUAD(HnswFilteredSearchAppliesLimitAfterWhere, EnableVectorSearchActor, CacheEnabled) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableVectorSearchActor(EnableVectorSearchActor);
        config.MutableMemoryControllerConfig()->SetSharedCacheMinBytes(CacheEnabled ? 64_MB : 0);
        config.MutableMemoryControllerConfig()->SetSharedCacheMaxBytes(CacheEnabled ? 64_MB : 0);
        TKikimrRunner kikimr{TKikimrSettings(config).SetEnableHnswIndex(true)
            .SetNeedsStatsCollectors(true).SetUseRealThreads(false)};
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        const auto created = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswFiltered` (
                id Uint64 NOT NULL, emb String, active Bool, PRIMARY KEY(id),
                INDEX plain GLOBAL USING hnsw ON (emb)
                    WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1, min_rows=1),
                INDEX covered GLOBAL USING hnsw ON (emb) COVER (active)
                    WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1, min_rows=1));
        )").ExtractValueSync(); });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto written = kikimr.RunCall([&] { return ExecuteDataQuery(session, R"(
            INSERT INTO `/Root/HnswFiltered` (id, emb, active) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([0.5f, 0.0f]), "FloatVector"), false),
                (2u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"), true),
                (3u, Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector"), true),
                (4u, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), NULL);
        )"); });
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        for (const TString index : {"plain", "covered"}) {
            auto read = [&](const TString& projection, const TString& predicate, ui32 limit, bool alias = false) {
                const TString query = TStringBuilder() << "$q = Knn::ToBinaryStringFloat([0.0f, 0.0f]); SELECT "
                    << projection << " FROM `/Root/HnswFiltered` VIEW " << index
                    << (predicate.empty() ? "" : TString(" WHERE ") + predicate)
                    << " ORDER BY " << (alias ? "distance" : "Knn::EuclideanDistance(emb, $q)")
                    << " LIMIT " << limit << ";";
                auto result = kikimr.RunCall([&] { return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(),
                    TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync(); });
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                return result;
            };
            if (CacheEnabled) {
                bool ready = false;
                for (ui32 attempt = 0; attempt < 30; ++attempt) {
                    const auto result = read("id", "", 1);
                    ui64 rows = 0;
                    for (const auto& phase : NYdb::TProtoAccessor::GetProto(*result.GetStats()).query_phases()) {
                        for (const auto& access : phase.table_access()) {
                            if (access.name().EndsWith("/indexImplPostingTable")) {
                                rows += access.reads().rows();
                            }
                        }
                    }
                    if (rows == 1) {
                        ready = true;
                        break;
                    }
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                UNIT_ASSERT_C(ready, "HNSW graph did not become ready for " << index);
            }
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read("id", "active", 1).GetResultSet(0)), "[[2u]]");
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read("id", "active", 3).GetResultSet(0)), "[[2u];[3u]]");
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read("id", "active AND id > 10u", 1).GetResultSet(0)), "[]");
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read("id", "active", 0).GetResultSet(0)), "[]");
            const auto projected = read("id, Knn::EuclideanDistance(emb, $q) AS distance", "active AND id >= 2u", 1, true);
            TResultSetParser parser(projected.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("id").GetUint64(), 2u);
            UNIT_ASSERT(!parser.TryNextRow());
        }
    }

    Y_UNIT_TEST_TWIN(HnswFilteredSearchDeduplicatesOverlappingPostings, Covered) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswOverlapFiltered` (id Uint64 NOT NULL, emb String, active Bool, PRIMARY KEY(id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto written = ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswOverlapFiltered` (id, emb, active) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), false),
                (2u, Untag(Knn::ToBinaryStringFloat([10.0f, 0.0f]), "FloatVector"), true),
                (3u, Untag(Knn::ToBinaryStringFloat([100.0f, 0.0f]), "FloatVector"), true);
        )");
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        result = session.ExecuteSchemeQuery(TStringBuilder()
            << "ALTER TABLE `/Root/HnswOverlapFiltered` ADD INDEX idx GLOBAL USING hnsw ON (emb) "
            << (Covered ? "COVER (active) " : "")
            << "WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=2, overlap_clusters=2);").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto count = ExecuteDataQuery(session,
            "SELECT COUNT(*) AS n FROM `/Root/HnswOverlapFiltered/idx/indexImplPostingTable`;");
        UNIT_ASSERT_C(count.IsSuccess(), count.GetIssues().ToString());
        TResultSetParser counts(count.GetResultSet(0));
        UNIT_ASSERT(counts.TryNextRow());
        UNIT_ASSERT_GT(counts.ColumnParser("n").GetUint64(), 3u);
        const auto read = ExecuteDataQuery(session, R"(
            $q = Knn::ToBinaryStringFloat([0.0f, 0.0f]);
            SELECT id FROM `/Root/HnswOverlapFiltered` VIEW idx WHERE active
            ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 3;
        )");
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(read.GetResultSet(0)), "[[2u];[3u]]");
    }

    Y_UNIT_TEST_QUAD(HnswFilteredSearchKeepsFrozenEmbedding, Covered, Overlap) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto created = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswFrozenFilter` (id Uint64 NOT NULL, emb String, active Bool, PRIMARY KEY(id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto write = ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswFrozenFilter` (id, emb, active) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), true),
                (2u, Untag(Knn::ToBinaryStringFloat([10.0f, 0.0f]), "FloatVector"), true);
        )");
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        created = session.ExecuteSchemeQuery(TStringBuilder()
            << "ALTER TABLE `/Root/HnswFrozenFilter` ADD INDEX idx GLOBAL USING hnsw ON (emb) "
            << (Covered ? "COVER (active) " : "")
            << "WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters="
            << (Overlap ? 2 : 1) << ", overlap_clusters=" << (Overlap ? 2 : 1) << ", autoupdate=false);").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        write = ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswFrozenFilter` (id, emb, active) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([20.0f, 0.0f]), "FloatVector"), true),
                (3u, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), true);
        )");
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        const auto read = ExecuteDataQuery(session, R"(
            $q = Knn::ToBinaryStringFloat([0.0f, 0.0f]);
            SELECT id FROM `/Root/HnswFrozenFilter` VIEW idx WHERE active
            ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 1;
            SELECT id FROM `/Root/HnswFrozenFilter` VIEW idx WHERE active
            ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 3;
            SELECT id, Knn::EuclideanDistance(emb, $q) AS distance
            FROM `/Root/HnswFrozenFilter` VIEW idx WHERE active ORDER BY distance LIMIT 1;
        )");
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(read.GetResultSet(0)), "[[1u]]");
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(read.GetResultSet(1)), "[[1u];[2u]]");
        TResultSetParser rows(read.GetResultSet(2));
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("id").GetUint64(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("distance").GetOptionalFloat().value(), 0.0f);
        UNIT_ASSERT(!rows.TryNextRow());
    }

    Y_UNIT_TEST(HnswFilteredSearchKeepsNullableKeysAndCoveredValues) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto schema = [&](const TString& sql) {
            const auto result = session.ExecuteSchemeQuery(sql).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        schema(R"(
            CREATE TABLE `/Root/HnswNullableFilter` (
                id Uint64, emb String, active Bool, note String, PRIMARY KEY(id));
        )");
        auto written = ExecuteDataQuery(session, R"(
            UPSERT INTO `/Root/HnswNullableFilter` (id, emb, active, note) VALUES
                (NULL, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), true, "old"),
                (1u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector"), true, "other");
        )");
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        schema(R"(
            ALTER TABLE `/Root/HnswNullableFilter` ADD INDEX idx GLOBAL USING hnsw ON (emb) COVER (active)
            WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1, autoupdate=false);
        )");
        written = ExecuteDataQuery(session, R"(
            UPDATE `/Root/HnswNullableFilter` SET active=false, note="updated" WHERE id IS NULL;
        )");
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        for (const TString projection : {"id", "id, note"}) {
            const auto read = ExecuteDataQuery(session, TStringBuilder()
                << "$q = Knn::ToBinaryStringFloat([0.0f, 0.0f]); SELECT " << projection
                << " FROM `/Root/HnswNullableFilter` VIEW idx WHERE active"
                << " ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT 1;");
            UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
            TResultSetParser rows(read.GetResultSet(0));
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT(!rows.ColumnParser("id").GetOptionalUint64());
            if (projection == "id, note") {
                UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("note").GetOptionalString().value(), "updated");
            }
            UNIT_ASSERT(!rows.TryNextRow());
        }
    }

    Y_UNIT_TEST(HnswFilteredSearchSupportsRowDependentTarget) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/HnswRowTarget` (
                id Uint64 NOT NULL, emb String NOT NULL, other_emb String NOT NULL, active Bool, PRIMARY KEY(id),
                INDEX idx GLOBAL USING hnsw ON (emb)
                WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto result = ExecuteDataQuery(session, R"(
            $zero = Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector");
            $one = Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector");
            UPSERT INTO `/Root/HnswRowTarget` (id, emb, other_emb, active) VALUES
                (1u, $zero, $one, true), (2u, $one, $one, true);
        )");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto read = ExecuteDataQuery(session, R"(
            SELECT id FROM `/Root/HnswRowTarget` VIEW idx WHERE active
            ORDER BY Knn::EuclideanDistance(emb, other_emb) LIMIT 1;
        )");
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(read.GetResultSet(0)), "[[2u]]");
    }

    Y_UNIT_TEST_QUAD(HnswFilteredSearchValidatesTarget, Covered, Parameterized) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/HnswFilteredTarget` (id Uint64 NOT NULL, emb String, active Bool, PRIMARY KEY(id),
                INDEX idx GLOBAL USING hnsw ON (emb) )" << (Covered ? "COVER (active) " : "") << R"(
                WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto written = ExecuteDataQuery(session, R"(
            INSERT INTO `/Root/HnswFilteredTarget` (id, emb, active) VALUES
                (1u, Untag(Knn::ToBinaryStringFloat([0.0f, 0.0f]), "FloatVector"), true);
        )");
        UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        auto read = [&](ui32 dimensions, const TString& predicate, ui32 limit = 1) {
            TString query = Parameterized ? "DECLARE $coordinates AS List<Float>; " : "";
            query += "$q = Knn::ToBinaryStringFloat(";
            if (Parameterized) {
                query += "$coordinates";
            } else {
                query += "[0.0f";
                for (ui32 i = 1; i < dimensions; ++i) {
                    query += ", 0.0f";
                }
                query += "]";
            }
            query += TStringBuilder() << "); SELECT id FROM `/Root/HnswFilteredTarget` VIEW idx WHERE "
                << predicate << " ORDER BY Knn::EuclideanDistance(emb, $q) LIMIT " << limit << ";";
            auto params = db.GetParamsBuilder();
            if (Parameterized) {
                auto& list = params.AddParam("$coordinates").BeginList();
                for (ui32 i = 0; i < dimensions; ++i) {
                    list.AddListItem().Float(0.0f);
                }
                list.EndList().Build();
            }
            return session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(),
                params.Build()).ExtractValueSync();
        };
        auto valid = read(2, "active");
        UNIT_ASSERT_C(valid.IsSuccess(), valid.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(valid.GetResultSet(0)), "[[1u]]");
        for (const ui32 dimensions : {1u, 3u}) {
            for (const TString predicate : {"active", "active AND id > 10u"}) {
                const auto invalid = read(dimensions, predicate);
                UNIT_ASSERT_VALUES_EQUAL_C(invalid.GetStatus(), EStatus::BAD_REQUEST,
                    "dimensions=" << dimensions << ", predicate=" << predicate << ": " << invalid.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(invalid.GetIssues().ToString(), "Target vector has invalid format");
            }
        }
        const auto zero = read(3, "active", 0);
        UNIT_ASSERT_C(zero.IsSuccess(), zero.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(zero.GetResultSet(0)), "[]");
        auto emptyTargetParams = db.GetParamsBuilder();
        emptyTargetParams.AddParam("$target").OptionalString(std::string{}).Build();
        auto nullTargetParams = db.GetParamsBuilder();
        nullTargetParams.AddParam("$target").EmptyOptional(EPrimitiveType::String).Build();
        for (const auto& params : {nullTargetParams.Build(), emptyTargetParams.Build()}) {
            const auto result = session.ExecuteDataQuery(R"(
                DECLARE $target AS String?;
                SELECT id FROM `/Root/HnswFilteredTarget` VIEW idx WHERE active
                ORDER BY Knn::EuclideanDistance(emb, $target) LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(), params,
                TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[]");
            UNIT_ASSERT(result.GetStats());
            const auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            for (const auto& phase : stats.query_phases()) {
                for (const auto& access : phase.table_access()) {
                    UNIT_ASSERT_VALUES_EQUAL_C(access.reads().rows(), 0, access.name());
                }
            }
        }
        for (const TString& target : {TString("\x01\x00\x02", 3), TString("invalid")}) {
            const auto params = db.GetParamsBuilder().AddParam("$target").String(target).Build().Build();
            const auto invalid = session.ExecuteDataQuery(R"(
                DECLARE $target AS String;
                SELECT id FROM `/Root/HnswFilteredTarget` VIEW idx WHERE active
                ORDER BY Knn::EuclideanDistance(emb, $target) LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(invalid.GetStatus(), EStatus::BAD_REQUEST, invalid.GetIssues().ToString());
        }
        const auto deleted = ExecuteDataQuery(session, "DELETE FROM `/Root/HnswFilteredTarget`;");
        UNIT_ASSERT_C(deleted.IsSuccess(), deleted.GetIssues().ToString());
        const auto emptyInvalid = read(3, "active");
        UNIT_ASSERT_VALUES_EQUAL_C(emptyInvalid.GetStatus(), EStatus::BAD_REQUEST, emptyInvalid.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(HnswShowCreatePreservesAdaptiveClusters, Adaptive) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/HnswAdaptiveOriginal` (id Uint64 NOT NULL, tenant Uint32, emb String, PRIMARY KEY(id),
                INDEX idx GLOBAL USING hnsw ON (tenant, emb)
                WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=4,
                    adaptive_clusters=)" << (Adaptive ? "true" : "false") << "));").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto shown = kikimr.GetQueryClient().ExecuteQuery("SHOW CREATE TABLE `/Root/HnswAdaptiveOriginal`;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(shown.IsSuccess(), shown.GetIssues().ToString());
        TResultSetParser parser(shown.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        TString ddl = parser.ColumnParser(0).GetOptionalUtf8().value();
        UNIT_ASSERT_STRING_CONTAINS(ddl, "adaptive_clusters");
        SubstGlobal(ddl, "HnswAdaptiveOriginal", "HnswAdaptiveReplay");
        const auto replayed = session.ExecuteSchemeQuery(ddl).ExtractValueSync();
        UNIT_ASSERT_C(replayed.IsSuccess(), replayed.GetIssues().ToString());
        for (const TString name : {"HnswAdaptiveOriginal", "HnswAdaptiveReplay"}) {
            const auto described = session.DescribeTable("/Root/" + name).ExtractValueSync();
            UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
            const auto& proto = NYdb::TProtoAccessor::GetProto(described.GetTableDescription());
            UNIT_ASSERT_VALUES_EQUAL(proto.indexes_size(), 1);
            const auto& settings = proto.indexes(0).global_hnsw_index().vector_settings();
            UNIT_ASSERT(settings.has_adaptive_clusters());
            UNIT_ASSERT_VALUES_EQUAL(settings.adaptive_clusters(), Adaptive);
        }
    }

    Y_UNIT_TEST_QUAD(HnswInsertAllowsMissingCoveredColumns, StreamWrite, Prefixed) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamWrite);
        TKikimrRunner kikimr{TKikimrSettings(config).SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/HnswMissingCover` (
                id Uint64 NOT NULL, tenant Uint32, emb String, payload String, note String, PRIMARY KEY(id),
                INDEX idx GLOBAL USING hnsw ON ()" << (Prefixed ? "tenant, emb" : "emb") << R"() COVER (payload, note)
                WITH (distance=euclidean, vector_type=float, vector_dimension=2, levels=1, clusters=1, min_rows=1));
        )").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto first = ExecuteDataQuery(session, R"(
            INSERT INTO `/Root/HnswMissingCover` (id, tenant, emb) VALUES
                (1u, 7u, Untag(Knn::ToBinaryStringFloat([1.0f, 0.0f]), "FloatVector")) RETURNING id;
        )");
        UNIT_ASSERT_C(first.IsSuccess(), first.GetIssues().ToString());
        const auto second = ExecuteDataQuery(session, R"(
            INSERT INTO `/Root/HnswMissingCover` (id, tenant, emb, note) VALUES
                (2u, 7u, Untag(Knn::ToBinaryStringFloat([2.0f, 0.0f]), "FloatVector"), "kept");
        )");
        UNIT_ASSERT_C(second.IsSuccess(), second.GetIssues().ToString());
        for (const TString table : {"/Root/HnswMissingCover", "/Root/HnswMissingCover/idx/indexImplPostingTable"}) {
            const auto read = ExecuteDataQuery(session, "SELECT id, payload, note FROM `" + table + "` ORDER BY id;");
            UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
            TResultSetParser rows(read.GetResultSet(0));
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("id").GetUint64(), 1u);
            UNIT_ASSERT(!rows.ColumnParser("payload").GetOptionalString());
            UNIT_ASSERT(!rows.ColumnParser("note").GetOptionalString());
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("id").GetUint64(), 2u);
            UNIT_ASSERT(!rows.ColumnParser("payload").GetOptionalString());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("note").GetOptionalString().value(), "kept");
            UNIT_ASSERT(!rows.TryNextRow());
        }
    }

    Y_UNIT_TEST_TWIN(HnswShowCreatePreservesAutoupdate, AutoUpdate) {
        TKikimrRunner kikimr{TKikimrSettings().SetEnableHnswIndex(true)};
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `/Root/HnswFrozenOriginal` (id Uint64 NOT NULL, emb String, PRIMARY KEY(id),
                INDEX idx GLOBAL USING hnsw ON (emb)
                WITH (distance=euclidean, vector_type=float, vector_dimension=2,
                    levels=1, clusters=1, min_rows=1, autoupdate=)" << (AutoUpdate ? "true" : "false") << "));").ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        const auto shown = kikimr.GetQueryClient().ExecuteQuery("SHOW CREATE TABLE `/Root/HnswFrozenOriginal`;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(shown.IsSuccess(), shown.GetIssues().ToString());
        TResultSetParser parser(shown.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        TString ddl = parser.ColumnParser(0).GetOptionalUtf8().value();
        UNIT_ASSERT_STRING_CONTAINS(ddl, "autoupdate");
        SubstGlobal(ddl, "HnswFrozenOriginal", "HnswFrozenReplay");
        const auto replayed = session.ExecuteSchemeQuery(ddl).ExtractValueSync();
        UNIT_ASSERT_C(replayed.IsSuccess(), replayed.GetIssues().ToString());
        for (const TString name : {"HnswFrozenOriginal", "HnswFrozenReplay"}) {
            const auto written = ExecuteDataQuery(session, "$v = Knn::ToBinaryStringFloat([1.0f, 0.0f]); INSERT INTO `/Root/"
                + name + "` (id, emb) VALUES (1u, Untag($v, \"FloatVector\"));");
            UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
            const auto read = ExecuteDataQuery(session,
                "SELECT COUNT(*) AS n FROM `/Root/" + name + "/idx/indexImplPostingTable`;");
            UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
            TResultSetParser rows(read.GetResultSet(0));
            UNIT_ASSERT(rows.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("n").GetUint64(), AutoUpdate ? 1u : 0u);
        }
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

} // namespace NKikimr::NKqp
