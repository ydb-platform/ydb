#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/testlib/helpers.h>

#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <util/generic/scope.h>
#include <util/string/subst.h>

#include <thread>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpKnn) {
    Y_UNIT_TEST_TWIN(RemoteEmbedderYieldsDuringHttp, streaming) {
        TOidcTestServer server;
        const TString url(server.Issuer());
        const TString origin(url.substr(0, url.rfind('/')));
        server.Enqueue(R"({"data":[{"index":0,"embedding":[1]}]})", HTTP_OK);

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        auto* queryConfig = settings.AppConfig.MutableQueryServiceConfig();
        queryConfig->AddHostnamePatterns("^localhost$");
        (*queryConfig->MutableRemoteEmbedderSecretOrigins())["/Root/embedder_token"] = origin;
        TKikimrRunner kikimr(settings);
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.GetQueryClient();
        auto create = kikimr.RunCall([&] {
            return client.ExecuteQuery("CREATE SECRET `/Root/embedder_token` WITH (value = 'embedder-token')",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        auto release = NThreading::NewPromise();
        auto handshake = NThreading::NewPromise();
        server.BlockTlsHandshakeUntil(release.GetFuture());
        std::jthread watchdog([&] {
            if (server.WaitForTlsHandshake()) {
                handshake.SetValue();
                release.GetFuture().Wait(TDuration::Seconds(5));
            }
            release.TrySetValue();
        });
        Y_DEFER { release.TrySetValue(); };

        TStringBuilder query;
        if (streaming) {
            query << R"(
                PROCESS AS_TABLE(AsList(AsStruct('text' AS Text, 1 AS PassThrough)))
                USING ApplyRemoteEmbedder(TableRows(), 'model', ')";
        } else {
            query << "SELECT ApplyRemoteEmbedder('text', 'model', '";
        }
        query << url << "', '/Root/embedder_token'";
        query << (streaming ? ", 1, 0, 0)" : ", 0).Status AS Status");
        auto pending = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
        });

        // The simulated runtime dispatches one actor at a time. It can reach this
        // condition while TLS is held only if the compute actor gives up the worker.
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return handshake.HasValue() || pending.IsReady(); };
        runtime->DispatchEvents(options);
        const bool parked = handshake.HasValue() && !release.HasValue() && !pending.IsReady();
        release.TrySetValue();
        const auto result = runtime->WaitFuture(pending);
        UNIT_ASSERT(parked);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto rows = TResultSetParser(result.GetResultSet(0));
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Status").GetUint32(), 200);
        UNIT_ASSERT(!rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    }

    Y_UNIT_TEST(RemoteEmbedderRfcSyntax) {
        TOidcTestServer server;
        const TString url(server.Issuer());
        const TString origin(url.substr(0, url.rfind('/')));
        for (size_t i = 0; i < 4; ++i) {
            server.Enqueue(R"({"data":[{"index":0,"embedding":[0.5,-0.25]}]})", HTTP_OK);
        }

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto* queryConfig = settings.AppConfig.MutableQueryServiceConfig();
        queryConfig->AddHostnamePatterns("^localhost$");
        (*queryConfig->MutableRemoteEmbedderSecretOrigins())["/Root/embedder_token"] = origin;
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto create = client.ExecuteQuery(R"(
            CREATE SECRET `/Root/embedder_token` WITH (value = 'embedder-token');
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        auto unbound = client.ExecuteQuery(R"(
            CREATE SECRET `/Root/unbound_embedder_token` WITH (value = 'unbound-token');
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(unbound.IsSuccess(), unbound.GetIssues().ToString());

        auto scalar = client.ExecuteQuery(TStringBuilder() << R"(
            SELECT ApplyRemoteEmbedder('Дом', 'test-model', ')" << url << R"(', '/Root/embedder_token', 0 AS MaxRetries).Status AS Status;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(scalar.IsSuccess(), scalar.GetIssues().ToString());
        auto scalarRows = TResultSetParser(scalar.GetResultSet(0));
        UNIT_ASSERT(scalarRows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(scalarRows.ColumnParser("Status").GetUint32(), 200);
        UNIT_ASSERT(!scalarRows.TryNextRow());

        const TString conditional = TStringBuilder() << R"(
            DECLARE $call AS Bool;
            SELECT ListMap(AsList('Молоко'), ($text) -> {
                RETURN IF($call, ApplyRemoteEmbedder($text, 'test-model', ')" << url << R"(', '/Root/embedder_token', 0).Status, 42u);
            }) AS Statuses;
        )";
        for (bool call : {false, true}) {
            auto params = TParamsBuilder().AddParam("$call").Bool(call).Build().Build();
            auto nested = client.ExecuteQuery(conditional, NQuery::TTxControl::NoTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(nested.IsSuccess(), nested.GetIssues().ToString());
            CompareYson(call ? "[[[200u]]]" : "[[[42u]]]", FormatResultSetYson(nested.GetResultSet(0)));
            UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), call ? 2 : 1);
        }

        auto batch = client.ExecuteQuery(TStringBuilder() << R"(
            $requests = AsList(AsStruct('Молоко' AS Text, AsStruct(1 AS Id) AS PassThrough));
            PROCESS AS_TABLE($requests)
            USING ApplyRemoteEmbedder(TableRows(), 'test-model', ')" << url << R"(', '/Root/embedder_token', 32, 10240, 5);
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(batch.IsSuccess(), batch.GetIssues().ToString());
        auto batchRows = TResultSetParser(batch.GetResultSet(0));
        UNIT_ASSERT(batchRows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(batchRows.ColumnParser("Status").GetUint32(), 200);
        UNIT_ASSERT(!batchRows.TryNextRow());

        for (const auto& [destination, expected] : std::initializer_list<std::pair<TString, TStringBuf>>{
            {TString("http://") + origin.substr(8), "HTTPS is required"},
            {"https://localhost:1", "destination does not match the approved HTTPS origin"},
        }) {
            auto blocked = client.ExecuteQuery(TStringBuilder() << R"(
                SELECT ApplyRemoteEmbedder('Дом', 'test-model', ')" << destination << R"(', '/Root/embedder_token', 0).Status;
            )", NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!blocked.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(blocked.GetIssues().ToString(), expected);
        }
        auto noBinding = client.ExecuteQuery(TStringBuilder() << R"(
            SELECT ApplyRemoteEmbedder('Дом', 'test-model', ')" << url << R"(', '/Root/unbound_embedder_token', 0).Status;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!noBinding.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(noBinding.GetIssues().ToString(), "no approved HTTPS origin");

        auto invalid = client.ExecuteQuery(TStringBuilder() << R"(
            SELECT ApplyRemoteEmbedder('Дом', 'test-model', ')" << url << R"(', '').Status;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(invalid.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(invalid.GetIssues().ToString(), "AuthSecretName must be a non-empty String literal");

        auto table = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/EmbedderInput` (id Int32, text String, PRIMARY KEY (id));
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(table.IsSuccess(), table.GetIssues().ToString());
        auto wrongKind = client.ExecuteQuery(TStringBuilder() << R"(
            SELECT ApplyRemoteEmbedder('Дом', 'test-model', ')" << url << R"(', '/Root/EmbedderInput', 0).Status;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(wrongKind.GetStatus(), EStatus::BAD_REQUEST, wrongKind.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(wrongKind.GetIssues().ToString(), "/Root/EmbedderInput");

        auto insert = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/EmbedderInput` (id, text) VALUES (1, 'Молоко');
        )", NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        auto scan = kikimr.GetTableClient().StreamExecuteScanQuery(TStringBuilder() << R"(
            SELECT ApplyRemoteEmbedder(Unwrap(text), 'test-model', ')" << url << R"(', '/Root/embedder_token', 0).Status
            FROM `/Root/EmbedderInput`;
        )").ExtractValueSync();
        UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
        CompareYson("[[200u]]", StreamResultToYson(scan));
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 4);
        for (const auto& request : requests) {
            UNIT_ASSERT_VALUES_EQUAL(request.Authorization, "Bearer embedder-token");
            UNIT_ASSERT_VALUES_EQUAL(request.Path, "/realm/embeddings");
        }
    }

    void CheckDistance(float actual, float expected) {
        UNIT_ASSERT_C(std::abs(actual - expected) < 0.0001f,
            "Expected distance " << expected << ", got " << actual);
    }

    template <bool Nullable>
    std::vector<std::pair<i64, float>> ExtractPksAndScores(TResultSetParser parser) {
        std::vector<std::pair<i64, float>> results;
        while (parser.TryNextRow()) {
            i64 pk;
            if constexpr (Nullable) {
                auto pkOpt = parser.ColumnParser("pk").GetOptionalInt64();
                UNIT_ASSERT(pkOpt.has_value());
                pk = *pkOpt;
            } else {
                pk = parser.ColumnParser("pk").GetInt64();
            }
            auto distanceOpt = parser.ColumnParser("distance").GetOptionalFloat();
            UNIT_ASSERT(distanceOpt.has_value());
            float distance = *distanceOpt;
            results.push_back({pk, distance});
        }
        return results;
    }

    TSession CreateTableForVectorSearch(TTableClient& db, bool nullable, const TString& dataCol = "data", bool singlePartition = false) {
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (nullable) {
                tableBuilder
                    .AddNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNullableColumn("emb", EPrimitiveType::String)
                    .AddNullableColumn(dataCol, EPrimitiveType::String);
            } else {
                tableBuilder
                    .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNonNullableColumn("emb", EPrimitiveType::String)
                    .AddNonNullableColumn(dataCol, EPrimitiveType::String);
            }
            tableBuilder.SetPrimaryKeyColumns({"pk"});

            if (!singlePartition) {
                tableBuilder.BeginPartitioningSettings()
                    .SetMinPartitionsCount(3)
                .EndPartitioningSettings();
                auto partitions = TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(4).EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(6).EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);
            }

            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query1 = TStringBuilder()
                << "UPSERT INTO `/Root/TestTable` (pk, emb, " << dataCol << ") VALUES "
                << "(0, \"\x03\x30\x02\", \"0\"),"
                    "(1, \"\x13\x31\x02\", \"1\"),"
                    "(2, \"\x23\x32\x02\", \"2\"),"
                    "(3, \"\x53\x33\x02\", \"3\"),"
                    "(4, \"\x43\x34\x02\", \"4\"),"
                    "(5, \"\x50\x60\x02\", \"5\"),"
                    "(6, \"\x61\x11\x02\", \"6\"),"
                    "(7, \"\x12\x62\x02\", \"7\"),"
                    "(8, \"\x75\x76\x02\", \"8\"),"
                    "(9, \"\x76\x76\x02\", \"9\");";

            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        return session;
    }

    template <bool Nullable>
    void VerifyVectorSearchResults(TKikimrRunner& kikimr, TSession& session, TTxSettings txSettings = TTxSettings::SerializableRW(), bool useRunCall = true) {
        // Verify actual results - check that top 3 PKs are correct
        // Target vector is 0x67, 0x71 (103, 113)
        // Cosine distances calculated:
        //   pk=8 (117, 118): 0.000882 - closest
        //   pk=5 (80, 96):   0.000985
        //   pk=9 (118, 118): 0.001070

        const TString query = Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
            ORDER BY distance
            LIMIT 3
        )");

        auto result = useRunCall
            ? kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx()).ExtractValueSync();
            })
            : session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Extract PKs and distances from result
        auto results = ExtractPksAndScores<Nullable>(result.GetResultSetParser(0));

        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(results[0].first, 8);
        UNIT_ASSERT_VALUES_EQUAL(results[1].first, 5);
        UNIT_ASSERT_VALUES_EQUAL(results[2].first, 9);
        CheckDistance(results[0].second, 0.000882f);
        CheckDistance(results[1].second, 0.000985f);
        CheckDistance(results[2].second, 0.001070f);
    }

    Y_UNIT_TEST_TWIN(VectorSearchKnnPushdown, Nullable) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, Nullable, "___data"); });

        ui64 expectedLimit = 3;
        ui64 testTableLocalId = 0;
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto* evRead = ev->Get();
            auto& read = evRead->Record;
            auto localId = read.GetTableId().GetTableId();

            // Learn testTableLocalId from the first read with VectorTopK
            if (read.HasVectorTopK()) {
                if (testTableLocalId == 0) {
                    testTableLocalId = localId;
                }
                if (localId == testTableLocalId) {
                    auto& topK = read.GetVectorTopK();
                    UNIT_ASSERT(topK.GetTargetVector() == "\x67\x71\x02");
                    UNIT_ASSERT_VALUES_EQUAL(topK.GetLimit(), expectedLimit);
                }
            } else if (testTableLocalId != 0 && localId == testTableLocalId) {
                // VectorTopK pushdown for point lookups is not yet implemented
                if (!evRead->Keys.empty()) {
                    return;
                }
                UNIT_ASSERT_C(false, "Read from TestTable must have VectorTopK");
            }
        });

        auto runQuery = [&](const TString& query) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto runQueryWithParams = [&](const TString& query, TParams params) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        // Literal target vector, normal order (column, target)
        runQuery(Q_(R"(
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, "\x67\x71\x02")
            LIMIT 3
        )"));

        // Literal target vector, reversed order (target, column)
        runQuery(Q_(R"(
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance("\x67\x71\x02", emb)
            LIMIT 3
        )"));

        // Variable equal to function call, normal order (column, target)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Variable equal to function call, reversed order (target, column)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance($TargetEmbedding, emb)
            LIMIT 3
        )"));

        // Implicit columns
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Inner product similarity
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::InnerProductSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        )"));

        // Cosine similarity (DESC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        )"));

        // Manhattan distance (ASC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::ManhattanDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Euclidean distance (ASC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::EuclideanDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Parameters
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), db.GetParamsBuilder()
            .AddParam("$TargetEmbedding")
                .String(TString("\x67\x71\x02", 3))
                .Build()
            .Build());

        // Tagged vector converted from a parameter
        runQueryWithParams(Q_(R"(
            DECLARE $embedding AS List<Uint8>;
            $TargetEmbedding = Knn::ToBinaryStringUint8($embedding);
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), db.GetParamsBuilder()
            .AddParam("$embedding")
                .BeginList()
                    .AddListItem().Uint8(0x67)
                    .AddListItem().Uint8(0x71)
                .EndList()
                .Build()
            .Build());

        // LIMIT 1
        expectedLimit = 1;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 1
        )"));

        // LIMIT 100
        expectedLimit = 100;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 100
        )"));
        expectedLimit = 3;

        // Test two-stage query from the same table: quantized search followed by full precision reranking
        // The first query uses pushdown of VectorTopK.
        // The second query uses point lookups (WHERE pk IN ...) where pushdown of VectorTopK is not implemented.
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");

            $Pks = SELECT pk
            FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3;

            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `/Root/TestTable`
            WHERE pk IN $Pks
            ORDER BY distance
            LIMIT 3;
        )"));

        // Verify actual results
        VerifyVectorSearchResults<Nullable>(kikimr, session);

        // Test with subquery: target vector from another table
        {
            // Create a second table to store the target vector
            auto schemeResult = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(R"(
                    CREATE TABLE `/Root/TargetVectors` (
                        id Int64 NOT NULL,
                        target_emb String,
                        PRIMARY KEY (id)
                    )
                )").ExtractValueSync();
            });
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

            // Insert the target vector
            runQuery(Q_(R"(
                UPSERT INTO `/Root/TargetVectors` (id, target_emb) VALUES
                (1, "\x67\x71\x02")
            )"));

            // Run query with subquery target, normal argument order (column first)
            runQuery(Q_(R"(
                $TargetEmbedding = (SELECT target_emb FROM `/Root/TargetVectors` WHERE id = 1);
                SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));

            // Run query with subquery target, reversed argument order (target first)
            runQuery(Q_(R"(
                $TargetEmbedding = (SELECT target_emb FROM `/Root/TargetVectors` WHERE id = 1);
                SELECT pk, Knn::CosineDistance($TargetEmbedding, emb) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));
        }

        // Observer is automatically removed when it goes out of scope
        observer.Remove();
    }

    Y_UNIT_TEST(VectorSearchStageDoesNotPushIntoUnrelatedSource) {
        using namespace NYql;
        using namespace NYql::NNodes;

        TExprContext ctx;
        const TPositionHandle pos;
        const auto sourceRows = Build<TCoArgument>(ctx, pos).Name("sourceRows").Done();
        const auto otherRows = Build<TCoArgument>(ctx, pos).Name("otherRows").Done();
        const auto rows = Build<TCoArgument>(ctx, pos).Name("rows").Done();
        const auto row = Build<TCoArgument>(ctx, pos).Name("row").Done();
        const auto sort = Build<TCoTopSort>(ctx, pos)
            .Input(rows)
            .Count<TCoUint64>().Literal().Build("1").Build()
            .SortDirections<TCoBool>().Literal().Build("true").Build()
            .KeySelectorLambda().Args({row}).Body<TCoMember>().Struct(row).Name().Build("emb").Build().Build()
            .Done();
        const auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add<TDqSource>()
                    .DataSource<TCoDataSource>().Category().Build("kqp").Build()
                    .Settings<TKqpReadRangesSourceSettings>()
                        .Table<TKqpTable>()
                            .Path().Build("/Root/TestTable")
                            .PathId().Build("1:1")
                            .SysView().Build("")
                            .Version().Build("1")
                            .Build()
                        .Columns().Build()
                        .Settings().Build()
                        .RangesExpr<TCoVoid>().Build()
                        .Build()
                    .Build()
                .Add(TExprBase(ctx.NewCallable(pos, "OtherSource", {})))
                .Build()
            .Program()
                .Args({sourceRows, otherRows})
                .Body<TCoMap>()
                    .Input<TCoAsList>().Add(otherRows).Build()
                    .Lambda().Args({rows}).Body(sort).Build()
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        // Resolving row-source metadata would be incorrect for this sort.
        const NOpt::TKqpOptimizeContext optimizeCtx("cluster", MakeIntrusive<TKikimrConfiguration>(),
            MakeIntrusive<TKikimrQueryContext>(nullptr, nullptr, nullptr), MakeIntrusive<TKikimrTablesData>(), {});
        UNIT_ASSERT_VALUES_EQUAL(NOpt::KqpApplyVectorTopKToStageWithSource(stage, ctx, optimizeCtx).Raw(), stage.Raw());
    }

    Y_UNIT_TEST_TWIN(VectorSearchPushdownPreservesReadPath, ScanQuery) {
        auto settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, true); });
        const auto table = Navigate(runtime, runtime.AllocateEdgeActor(), "/Root/TestTable",
            NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown)->ResultSet.at(0).TableId.PathId.LocalPathId;

        ui32 reads = 0;
        bool expectPushdown = true;
        auto observer = runtime.AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            const auto& read = ev->Get()->Record;
            if (read.GetTableId().GetTableId() == table) {
                ++reads;
                UNIT_ASSERT_VALUES_EQUAL(read.HasVectorTopK(), expectPushdown);
            }
        });
        auto check = [&](const TString& query, const char* expected) {
            reads = 0;
            const auto result = kikimr.RunCall([&]() -> TString {
                if constexpr (ScanQuery) {
                    auto stream = db.StreamExecuteScanQuery(query).GetValueSync();
                    UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
                    return StreamResultToYson(stream);
                } else {
                    auto result = session.ExecuteDataQuery(query,
                        TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return FormatResultSetYson(result.GetResultSet(0));
                }
            });
            CompareYson(expected, result);
            UNIT_ASSERT(reads > 0);
        };

        check(R"(
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, String::HexDecode("677102")) LIMIT 1;
        )", "[[[8]]]");

        expectPushdown = false;
        check(R"(
            SELECT pk FROM (
                SELECT pk, Knn::CosineDistance(emb, String::HexDecode("677102")) AS distance
                FROM `/Root/TestTable` WHERE data != "8"
            ) ORDER BY distance LIMIT 1;
        )", "[[[5]]]");
        check(R"(
            SELECT pk FROM `/Root/TestTable`
            FLATTEN LIST BY (IF(data = "8", ListCreate(Int64?), AsList(pk)) AS item)
            ORDER BY Knn::CosineDistance(emb, String::HexDecode("677102")) LIMIT 1;
        )", "[[[5]]]");
    }

    Y_UNIT_TEST_TWIN(VectorSearchSamplingSkipsPushdown, ScanQuery) {
        auto settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        TKikimrRunner kikimr(settings);
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, true); });

        ui32 reads = 0;
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.HasSampling());
            UNIT_ASSERT(!ev->Get()->Record.HasVectorTopK());
            ++reads;
        });

        for (const TString rate : {"1", "0.00000000000000000001"}) {
            const TString query = TStringBuilder()
                << "SELECT pk FROM `/Root/TestTable` WITH (sampling_rate=\"" << rate
                << "\", sampling_seed=\"42\", sampling_memtable_stride=\"1\")"
                << " ORDER BY Knn::CosineDistance(emb, String::HexDecode(\"677102\")) LIMIT 3";
            reads = 0;
            const auto result = kikimr.RunCall([&]() -> TString {
                if constexpr (ScanQuery) {
                    auto stream = db.StreamExecuteScanQuery(query).GetValueSync();
                    UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
                    return StreamResultToYson(stream);
                } else {
                    auto result = session.ExecuteDataQuery(query,
                        TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return FormatResultSetYson(result.GetResultSet(0));
                }
            });
            // Rate 1 preserves global ordering and LIMIT; a tiny rate proves
            // that sampling is still applied before compute-side ranking.
            CompareYson(rate == "1" ? "[[[8]];[[5]];[[9]]]" : "[]", result);
            UNIT_ASSERT(reads > 0);
        }
    }

    enum class EVectorType {
        Float,
        Float16,
        BFloat16,
        Float8E4M3,
        Float8E5M2,
        Bit,
        Uint8,
        Int8,
        Uint4,
        Int4
    };

    void DoVectorKnnPushdownTest(EVectorType vectorType) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] {
            return db.CreateSession().GetValueSync().GetSession();
        });

        // Create partitioned table
        {
            auto tableBuilder = db.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                .AddNonNullableColumn("emb", EPrimitiveType::String)
                .SetPrimaryKeyColumns({"pk"});
            tableBuilder.BeginPartitioningSettings()
                .SetMinPartitionsCount(3)
            .EndPartitioningSettings();
            auto partitions = TExplicitPartitions{}
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(3).EndTuple().Build())
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(5).EndTuple().Build());
            tableBuilder.SetPartitionAtKeys(partitions);
            auto result = kikimr.RunCall([&] {
                return session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString insertQuery;
        TString targetVector;
        TString tagName;
        i64 expectedMatchPk = 0;
        switch (vectorType) {
            case EVectorType::Float:
            case EVectorType::Float16:
            case EVectorType::BFloat16:
            case EVectorType::Float8E4M3:
            case EVectorType::Float8E5M2:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringFloat([1.0f, 2.0f, 3.0f]), "FloatVector")),
                    (2, Untag(Knn::ToBinaryStringFloat([4.0f, 5.0f, 6.0f]), "FloatVector")),
                    (3, Untag(Knn::ToBinaryStringFloat([7.0f, 8.0f, 9.0f]), "FloatVector")),
                    (4, Untag(Knn::ToBinaryStringFloat([10.0f, 20.0f, 30.0f]), "FloatVector")),
                    (5, Untag(Knn::ToBinaryStringFloat([13.0f, 14.0f, 15.0f]), "FloatVector")),
                    (6, Untag(Knn::ToBinaryStringFloat([100.0f, 110.0f, 120.0f]), "FloatVector"));
                )";
                targetVector = "Knn::ToBinaryStringFloat([100.0f, 110.0f, 120.0f])";
                tagName = "FloatVector";
                if (vectorType != EVectorType::Float) {
                    const TString type = vectorType == EVectorType::Float16 ? "Float16"
                        : vectorType == EVectorType::BFloat16 ? "BFloat16"
                        : vectorType == EVectorType::Float8E4M3 ? "Float8E4M3" : "Float8E5M2";
                    SubstGlobal(insertQuery, "Float", type);
                    SubstGlobal(targetVector, "Float", type);
                    tagName = type + "Vector";
                }
                if (vectorType == EVectorType::Float8E5M2) {
                    // E5M2 rounds row 5 to [12, 14, 16], proportional to
                    // the target [96, 112, 128]. Keep the cosine winner unique.
                    SubstGlobal(insertQuery, "[13.0f, 14.0f, 15.0f]", "[13.0f, -14.0f, 15.0f]");
                }
                expectedMatchPk = 6;
                break;
            case EVectorType::Uint4:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringUint4([1ut, 2ut, 3ut]), "Uint4Vector")),
                    (2, Untag(Knn::ToBinaryStringUint4([2ut, 3ut, 4ut]), "Uint4Vector")),
                    (3, Untag(Knn::ToBinaryStringUint4([3ut, 4ut, 5ut]), "Uint4Vector")),
                    (4, Untag(Knn::ToBinaryStringUint4([4ut, 5ut, 6ut]), "Uint4Vector")),
                    (5, Untag(Knn::ToBinaryStringUint4([5ut, 6ut, 7ut]), "Uint4Vector")),
                    (6, Untag(Knn::ToBinaryStringUint4([15ut, 14ut, 13ut]), "Uint4Vector"));
                )";
                targetVector = "Knn::ToBinaryStringUint4([15ut, 14ut, 13ut])";
                tagName = "Uint4Vector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Int4:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringInt4([1t, 2t, 3t]), "Int4Vector")),
                    (2, Untag(Knn::ToBinaryStringInt4([2t, 3t, 4t]), "Int4Vector")),
                    (3, Untag(Knn::ToBinaryStringInt4([3t, 4t, 5t]), "Int4Vector")),
                    (4, Untag(Knn::ToBinaryStringInt4([4t, 5t, 6t]), "Int4Vector")),
                    (5, Untag(Knn::ToBinaryStringInt4([5t, 6t, 7t]), "Int4Vector")),
                    (6, Untag(Knn::ToBinaryStringInt4([7t, -8t, 7t]), "Int4Vector"));
                )";
                targetVector = "Knn::ToBinaryStringInt4([7t, -8t, 7t])";
                tagName = "Int4Vector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Bit:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 1.f]), "BitVector")),
                    (2, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 1.f, 1.f]), "BitVector")),
                    (3, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (4, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 1.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (5, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 1.f, 1.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (6, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f]), "BitVector"));
                )";
                targetVector = "Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f])";
                tagName = "BitVector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Uint8:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringUint8([10ut, 20ut, 30ut]), "Uint8Vector")),
                    (2, Untag(Knn::ToBinaryStringUint8([40ut, 50ut, 60ut]), "Uint8Vector")),
                    (3, Untag(Knn::ToBinaryStringUint8([70ut, 80ut, 90ut]), "Uint8Vector")),
                    (4, Untag(Knn::ToBinaryStringUint8([100ut, 110ut, 120ut]), "Uint8Vector")),
                    (5, Untag(Knn::ToBinaryStringUint8([130ut, 140ut, 150ut]), "Uint8Vector")),
                    (6, Untag(Knn::ToBinaryStringUint8([200ut, 210ut, 220ut]), "Uint8Vector"));
                )";
                targetVector = "Knn::ToBinaryStringUint8([200ut, 210ut, 220ut])";
                tagName = "Uint8Vector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Int8:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringInt8([10t, 20t, 30t]), "Int8Vector")),
                    (2, Untag(Knn::ToBinaryStringInt8([40t, 50t, 60t]), "Int8Vector")),
                    (3, Untag(Knn::ToBinaryStringInt8([70t, 80t, 90t]), "Int8Vector")),
                    (4, Untag(Knn::ToBinaryStringInt8([-10t, -20t, -30t]), "Int8Vector")),
                    (5, Untag(Knn::ToBinaryStringInt8([-40t, -50t, -60t]), "Int8Vector")),
                    (6, Untag(Knn::ToBinaryStringInt8([100t, 110t, 120t]), "Int8Vector"));
                )";
                targetVector = "Knn::ToBinaryStringInt8([100t, 110t, 120t])";
                tagName = "Int8Vector";
                expectedMatchPk = 6;
                break;
            default:
                UNIT_ASSERT_C(false, "Unexpected vector type");
        }

        {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(insertQuery),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Setup observer to verify VectorTopK pushdown
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& read = ev->Get()->Record;
            UNIT_ASSERT(read.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 3u);
        });

        auto runDistanceQuery = [&](const TString& distanceFunc, const TString& orderDir = "") {
            TString query = TStringBuilder()
                << "$TargetEmbedding = Untag(" << targetVector << ", \"" << tagName << "\");\n"
                << "SELECT pk, Knn::" << distanceFunc << "(emb, $TargetEmbedding) AS distance "
                << "FROM `/Root/TestTable` ORDER BY distance " << orderDir << " LIMIT 3";
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(query),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };

        // CosineDistance (ASC)
        {
            auto result = runDistanceQuery("CosineDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // CosineSimilarity (DESC)
        {
            auto result = runDistanceQuery("CosineSimilarity", "DESC");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 1.0f);
        }

        // EuclideanDistance (ASC)
        {
            auto result = runDistanceQuery("EuclideanDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // ManhattanDistance (ASC)
        {
            auto result = runDistanceQuery("ManhattanDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // InnerProductSimilarity (DESC)
        {
            auto result = runDistanceQuery("InnerProductSimilarity", "DESC");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
        }

        observer.Remove();
    }

    Y_UNIT_TEST(FloatVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float);
    }

    Y_UNIT_TEST(Float16VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float16);
    }

    Y_UNIT_TEST(BFloat16VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::BFloat16);
    }

    Y_UNIT_TEST(Float8E4M3VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float8E4M3);
    }

    Y_UNIT_TEST(Float8E5M2VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float8E5M2);
    }

    Y_UNIT_TEST(Uint4VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Uint4);
    }

    Y_UNIT_TEST(Int4VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Int4);
    }

    Y_UNIT_TEST(BitVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Bit);
    }

    Y_UNIT_TEST(Uint8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Uint8);
    }

    Y_UNIT_TEST(Int8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Int8);
    }

    Y_UNIT_TEST_TWIN(VectorSearchKnnPushdownFollower, StaleRO) {
        const TString tableName = "/Root/TestTable";

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetEnableForceFollowers(true)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.GetTableClient();
        auto session = CreateTableForVectorSearch(db, false, "data", true); // singlePartition = true for followers

        // Setup observer to verify VectorTopK pushdown
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& read = ev->Get()->Record;
            UNIT_ASSERT(read.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 3u);
        });

        // Enable followers on the table
        {
            const TString alterTable(Q_(Sprintf(R"(
                ALTER TABLE `%s` SET (READ_REPLICAS_SETTINGS = "PER_AZ:1");
            )", tableName.c_str())));

            auto result = session.ExecuteSchemeQuery(alterTable).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify followers are configured
        {
            auto result = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            UNIT_ASSERT(table.GetReadReplicasSettings()->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(table.GetReadReplicasSettings()->GetReadReplicasCount(), 1);
        }

        // Perform read
        VerifyVectorSearchResults<false>(kikimr, session, StaleRO ? TTxSettings::StaleRO() : TTxSettings::SerializableRW(), false);

        if (StaleRO) {
            // from leader - should NOT read
            CheckTableReads(session, tableName, false, false);
            // from followers - should read
            CheckTableReads(session, tableName, true, true);
        } else {
            // from leader - should read
            CheckTableReads(session, tableName, false, true);
            // from followers - should NOT read
            CheckTableReads(session, tableName, true, false);
        }

        // Observer is automatically removed when it goes out of scope
        observer.Remove();
    }

    Y_UNIT_TEST(VectorSearchKnnPushdownAllocatorBound) {
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false);

        TKikimrRunner kikimr(serverSettings);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });

        auto session = kikimr.RunCall([&] {
            auto session = db.CreateSession().GetValueSync().GetSession();

            {
                auto tableBuilder = db.GetTableBuilder();
                tableBuilder
                    .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNonNullableColumn("emb", EPrimitiveType::String)
                    .SetPrimaryKeyColumns({"pk"});
                tableBuilder.BeginPartitioningSettings()
                    .SetMinPartitionsCount(3)
                .EndPartitioningSettings();
                auto partitions = TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(4).EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(6).EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);

                auto result = session.CreateTable("/Root/BigVecTable", tableBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                auto result = session.ExecuteDataQuery(Q_(R"(
                    UPSERT INTO `/Root/BigVecTable` (pk, emb) VALUES
                        (0, "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x02"),
                        (1, "\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20\x02"),
                        (2, "\x21\x22\x23\x24\x25\x26\x27\x28\x29\x2a\x2b\x2c\x2d\x2e\x2f\x30\x02"),
                        (3, "\x31\x32\x33\x34\x35\x36\x37\x38\x39\x3a\x3b\x3c\x3d\x3e\x3f\x40\x02"),
                        (4, "\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x02"),
                        (5, "\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5a\x5b\x5c\x5d\x5e\x5f\x60\x02"),
                        (6, "\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c\x6d\x6e\x6f\x70\x02"),
                        (7, "\x71\x72\x73\x74\x75\x76\x77\x78\x79\x7a\x7b\x7c\x7d\x7e\x7f\x70\x02"),
                        (8, "\x75\x76\x77\x78\x79\x7a\x7b\x7c\x7d\x7e\x7f\x70\x71\x72\x73\x74\x02"),
                        (9, "\x76\x76\x77\x78\x79\x7a\x7b\x7c\x7d\x7e\x7f\x70\x71\x72\x73\x74\x02");
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            return session;
        });

        const TString query = Q_(R"(
            SELECT pk FROM `/Root/BigVecTable`
            ORDER BY Knn::CosineDistance(emb, "\x71\x72\x73\x74\x75\x76\x77\x78\x79\x7a\x7b\x7c\x7d\x7e\x7f\x70\x02")
            LIMIT 3
        )");

        auto result = kikimr.RunCall([&] {
            return session.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        });

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto parser = result.GetResultSetParser(0);
        ui32 rowCount = 0;
        while (parser.TryNextRow()) {
            ++rowCount;
        }
        UNIT_ASSERT_VALUES_EQUAL(rowCount, 3u);
    }

}

}
}
