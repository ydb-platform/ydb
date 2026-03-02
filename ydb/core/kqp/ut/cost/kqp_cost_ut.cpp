#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <format>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static NKikimrConfig::TAppConfig GetAppConfig(bool scanSourceRead = false, bool streamLookupJoin = false, bool enableOltpSink = false) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(scanSourceRead);
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(streamLookupJoin);
    app.MutableTableServiceConfig()->SetEnableOlapSink(true);
    app.MutableTableServiceConfig()->SetEnableOltpSink(enableOltpSink);
    return app;
}

static NYdb::NTable::TExecDataQuerySettings GetDataQuerySettings() {
    NYdb::NTable::TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);
    return execSettings;
}

static NYdb::NQuery::TExecuteQuerySettings GetQuerySettings() {
    NYdb::NQuery::TExecuteQuerySettings execSettings;
    execSettings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
    return execSettings;
}

static void CreateSampleTables(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/Join1_1` (
            Key Int32,
            Fk21 Int32,
            Fk22 String,
            Value String,
            PRIMARY KEY (Key)
        );
        CREATE TABLE `/Root/Join1_2` (
            Key1 Int32,
            Key2 String,
            Fk3 String,
            Value String,
            PRIMARY KEY (Key1, Key2)
        );
        CREATE TABLE `/Root/Join1_3` (
            Key String,
            Value Int32,
            PRIMARY KEY (Key)
        );
    )").GetValueSync().IsSuccess());

     UNIT_ASSERT(session.ExecuteDataQuery(R"(

        REPLACE INTO `/Root/Join1_1` (Key, Fk21, Fk22, Value) VALUES
            (1, 101, "One", "Value1"),
            (2, 102, "Two", "Value1"),
            (3, 103, "One", "Value2"),
            (4, 104, "Two", "Value2"),
            (5, 105, "One", "Value3"),
            (6, 106, "Two", "Value3"),
            (7, 107, "One", "Value4"),
            (8, 108, "One", "Value5");

        REPLACE INTO `/Root/Join1_2` (Key1, Key2, Fk3, Value) VALUES
            (101, "One",   "Name1", "Value21"),
            (101, "Two",   "Name1", "Value22"),
            (101, "Three", "Name3", "Value23"),
            (102, "One",   "Name2", "Value24"),
            (103, "One",   "Name1", "Value25"),
            (104, "One",   "Name3", "Value26"),
            (105, "One",   "Name2", "Value27"),
            (105, "Two",   "Name4", "Value28"),
            (106, "One",   "Name3", "Value29"),
            (108, "One",    NULL,   "Value31"),
            (109, "Four",   NULL,   "Value41");

        REPLACE INTO `/Root/Join1_3` (Key, Value) VALUES
            ("Name1", 1001),
            ("Name2", 1002),
            ("Name4", 1004);

    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}


Y_UNIT_TEST_SUITE(KqpCost) {
    void EnableDebugLogging(NActors::TTestActorRuntime * runtime) {
        //runtime->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_TRACE);
        // runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_CONVEYOR, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
    }

    Y_UNIT_TEST_TWIN(IndexLookup, useSink) {
        TKikimrRunner kikimr(GetAppConfig(true, false, useSink));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/SecondaryKeys` (
                Key Int32,
                Fk Int32,
                Value String,
                ValueInt Int32,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )").GetValueSync();

        auto prepare = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value, ValueInt) VALUES
                (1,    1,    "Payload1", 100),
                (2,    2,    "Payload2", 200),
                (5,    5,    "Payload5", 500),
                (NULL, 6,    "Payload6", 600),
                (7,    NULL, "Payload7", 700),
                (NULL, NULL, "Payload8", 800);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(prepare.GetStatus(), EStatus::SUCCESS, prepare.GetIssues().ToString());

        auto query = Q_(R"(
            SELECT Value FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk = 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [["Payload1"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        std::unordered_map<TString, std::pair<int, int>> readsByTable;
        for(const auto& queryPhase : stats.query_phases()) {
            for(const auto& tableAccess: queryPhase.table_access()) {
                auto [it, success] = readsByTable.emplace(tableAccess.name(), std::make_pair(0, 0));
                it->second.first += tableAccess.reads().rows();
                it->second.second += tableAccess.reads().bytes();
            }
        }

        for(const auto& [name, rowsAndBytes]: readsByTable) {
            Cerr << name << " " << rowsAndBytes.first << " " << rowsAndBytes.second << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").first, 1);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").second, 8);

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").first, 1);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").second, 8);
    }

    Y_UNIT_TEST_TWIN(IndexLookupAtLeast8BytesInStorage, useSink) {
        TKikimrRunner kikimr(GetAppConfig(true, false, useSink));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/SecondaryKeys` (
                Key Int32,
                Fk Int32,
                Value String,
                ValueInt Int32,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );

        )").GetValueSync();

        session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value, ValueInt) VALUES
                (1,    1,    "Payload1", 100),
                (2,    2,    "Payload2", 200),
                (5,    5,    "Payload5", 500),
                (NULL, 6,    "Payload6", 600),
                (7,    NULL, "Payload7", 700),
                (NULL, NULL, "Payload8", 800);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        auto query = Q_(R"(
            SELECT ValueInt FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk = 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[100]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        std::unordered_map<TString, std::pair<int, int>> readsByTable;
        for(const auto& queryPhase : stats.query_phases()) {
            for(const auto& tableAccess: queryPhase.table_access()) {
                auto [it, success] = readsByTable.emplace(tableAccess.name(), std::make_pair(0, 0));
                it->second.first += tableAccess.reads().rows();
                it->second.second += tableAccess.reads().bytes();
            }
        }

        for(const auto& [name, rowsAndBytes]: readsByTable) {
            Cerr << name << " " << rowsAndBytes.first << " " << rowsAndBytes.second << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").first, 1);
        // 4 bytes is unexpected, because datashards has 8 bytes per row in storage.
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").second, 8);

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").first, 1);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").second, 8);
    }

    Y_UNIT_TEST_TWIN(IndexLookupAndTake, useSink) {
        TKikimrRunner kikimr(GetAppConfig(true, false, useSink));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/SecondaryKeys` (
                Key Int32,
                Fk Int32,
                Value String,
                ValueInt Int32,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );

        )").GetValueSync();

        session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value, ValueInt) VALUES
                (1,    1,    "Payload1", 100),
                (2,    2,    "Payload2", 200),
                (5,    5,    "Payload5", 500),
                (NULL, 6,    "Payload6", 600),
                (7,    NULL, "Payload7", 700),
                (NULL, NULL, "Payload8", 800);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        auto query = Q_(R"(
            SELECT Value FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk >= 1 and Fk <= 2 AND StartsWith(Value, "Payload") LIMIT 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [["Payload1"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        std::unordered_map<TString, std::pair<int, int>> readsByTable;
        for(const auto& queryPhase : stats.query_phases()) {
            for(const auto& tableAccess: queryPhase.table_access()) {
                auto [it, success] = readsByTable.emplace(tableAccess.name(), std::make_pair(0, 0));
                it->second.first += tableAccess.reads().rows();
                it->second.second += tableAccess.reads().bytes();
            }
        }

        for(const auto& [name, rowsAndBytes]: readsByTable) {
            Cerr << name << " " << rowsAndBytes.first << " " << rowsAndBytes.second << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").first, 1);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys").second, 8);

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").first, 2);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/SecondaryKeys/Index/indexImplTable").second, 16);
    }

    Y_UNIT_TEST_TWIN(VectorIndexLookup, useSink) {
        TKikimrRunner kikimr(GetAppConfig(true, false, useSink));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableAccessToIndexImplTables(true);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_INFO);
        NSchemeShard::gVectorIndexSeed = 1337;

        { // 1. CREATE TABLE
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                CREATE TABLE `/Root/Vectors` (
                    Key Int32,
                    Value Int32,
                    Prefix Int32,
                    Embedding String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        { // 2. UPSERT VALUES
            for (ui32 key = 0; key < 100; key++) {
                TString embedding = "00\x02";
                embedding[0] = 'a' + key % 26;
                embedding[1] = 'A' + (key * 17) % 26;

                auto query = Sprintf(R"(
                    UPSERT INTO `/Root/Vectors` (Key, Value, Prefix, Embedding) VALUES
                        (%u, %u, %u, "%s");
                )", key, key * 10, key % 10, embedding.c_str());

                auto result = session.ExecuteDataQuery(query,
                    TTxControl::BeginTx().CommitTx()).GetValueSync();

                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            }
        }

        { // 4a. ADD INDEX ON (Embedding) --- vector_idx
            auto result = session.ExecuteSchemeQuery(R"(
                ALTER TABLE `/Root/Vectors`
                    ADD INDEX vector_idx
                    GLOBAL USING vector_kmeans_tree
                    ON (Embedding)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=3, clusters=2);
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        { // 4b. ADD INDEX ON (Embedding) COVER (Embedding, Value) --- vector_idx_covered
            auto result = session.ExecuteSchemeQuery(R"(
                ALTER TABLE `/Root/Vectors`
                    ADD INDEX vector_idx_covered
                    GLOBAL USING vector_kmeans_tree
                    ON (Embedding) COVER (Embedding, Value)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=3, clusters=2);
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        { // 4c. ADD INDEX ON (Embedding) --- vector_idx_prefixed
            auto result = session.ExecuteSchemeQuery(R"(
                ALTER TABLE `/Root/Vectors`
                    ADD INDEX vector_idx_prefixed
                    GLOBAL USING vector_kmeans_tree
                    ON (Prefix, Embedding)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        auto debugPringTable = [&](const TString& name) {
            auto query = Q_(Sprintf(R"(
                SELECT * FROM `%s`
            )", name.c_str()));

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), GetDataQuerySettings()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            Cerr << name << ":" << Endl;
            Cerr << NYdb::FormatResultSetYson(result.GetResultSet(0)) << Endl;
        };

        auto checkSelect = [&](auto query, TMap<TString, ui64> expectedReadsByTable) {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), GetDataQuerySettings()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            TMap<TString, ui64> readsByTable;
            for (const auto& queryPhase : stats.query_phases()) {
                for(const auto& tableAccess: queryPhase.table_access()) {
                    readsByTable[tableAccess.name()] += tableAccess.reads().rows();
                }
            }

            for (const auto& kvp : readsByTable) {
                debugPringTable(kvp.first);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(expectedReadsByTable, readsByTable, query);
        };

        { // 5x. SELECT VIEW PRIMARY KEY (with brute force vector search pushdown)
            // SELECT Key
            checkSelect(Q_(R"(
                SELECT Key FROM `/Root/Vectors` VIEW PRIMARY KEY
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors", 100} // brute force vector search pushdown returns only 10 rows but scans 100 rows
            });

            // SELECT Key, Value --- same stats
            checkSelect(Q_(R"(
                SELECT Key, Value FROM `/Root/Vectors` VIEW PRIMARY KEY
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors", 100}
            });
        }

        { // 5a. SELECT VIEW vector_idx
            // SELECT Key
            checkSelect(Q_(R"(
                SELECT Key FROM `/Root/Vectors` VIEW vector_idx
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx/indexImplLevelTable", 6}, // about levels * clusters = 3 * 2 = 6
                {"/Root/Vectors/vector_idx/indexImplPostingTable", 12}, // about rows / clusters^levels = 100 / 2^3 = 12.5
                {"/Root/Vectors", 12} // same as posting
            });

            // SELECT Key, Value --- same stats
            checkSelect(Q_(R"(
                SELECT Key, Value FROM `/Root/Vectors` VIEW vector_idx
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx/indexImplLevelTable", 6},
                {"/Root/Vectors/vector_idx/indexImplPostingTable", 12},
                {"/Root/Vectors", 12}
            });

            // KMeansTreeSearchTopSize = 2 --- about twice more
            checkSelect(Q_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT Key FROM `/Root/Vectors` VIEW vector_idx
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx/indexImplLevelTable", 10},
                {"/Root/Vectors/vector_idx/indexImplPostingTable", 24},
                {"/Root/Vectors", 24}
            });
        }

        { // 5b. SELECT VIEW vector_idx_covered
            // SELECT Key
            checkSelect(Q_(R"(
                SELECT Key FROM `/Root/Vectors` VIEW vector_idx_covered
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx_covered/indexImplLevelTable", 6}, // about levels * clusters = 3 * 2 = 6
                {"/Root/Vectors/vector_idx_covered/indexImplPostingTable", 12}, // about rows / clusters^levels = 100 / 2^3 = 12.5
                // no main table
            });

            // SELECT Key, Value --- same stats
            checkSelect(Q_(R"(
                SELECT Key, Value FROM `/Root/Vectors` VIEW vector_idx_covered
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx_covered/indexImplLevelTable", 6},
                {"/Root/Vectors/vector_idx_covered/indexImplPostingTable", 12},
            });

            // KMeansTreeSearchTopSize = 2 --- about twice more
            checkSelect(Q_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT Key FROM `/Root/Vectors` VIEW vector_idx_covered
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx_covered/indexImplLevelTable", 10},
                {"/Root/Vectors/vector_idx_covered/indexImplPostingTable", 24},
            });
        }

        { // 5c. SELECT VIEW vector_idx_prefixed
            // SELECT Key
            checkSelect(Q_(R"(
                SELECT Key FROM `/Root/Vectors` VIEW vector_idx_prefixed
                    WHERE Prefix = 7
                    ORDER BY Knn::CosineDistance(Embedding, "pQ\x02")
                    LIMIT 10;
            )"), {
                {"/Root/Vectors/vector_idx_prefixed/indexImplPrefixTable", 1},
                {"/Root/Vectors/vector_idx_prefixed/indexImplLevelTable", 4},
                {"/Root/Vectors/vector_idx_prefixed/indexImplPostingTable", 4}, // about rows / 10 / clusters^levels = 100 / 10 / 2^2 = 2.5
                {"/Root/Vectors", 4} // same as posting
            });
        }
    }

    Y_UNIT_TEST(PointLookup) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1u AND Name = "Anna";
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];
                [1u];["Anna"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
    }

    Y_UNIT_TEST(Range) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group < 2u ORDER BY Group;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];[1u];["Anna"]];
                [[300u];["None"];[1u];["Paul"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = stats.query_phases_size() - 1;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().bytes(), 40);
    }

    Y_UNIT_TEST_TWIN(IndexLookupJoin, StreamLookupJoin) {
        TKikimrRunner kikimr(GetAppConfig(true, StreamLookupJoin, false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1_1` AS t1
            INNER JOIN `/Root/Join1_2` AS t2
            ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
            WHERE t1.Value = 'Value3' AND t2.Value IS NOT NULL
        )"), TTxControl::BeginTx().CommitTx(), GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        std::unordered_map<TString, std::pair<int, int>> readsByTable;
        for(const auto& queryPhase : stats.query_phases()) {
            for(const auto& tableAccess: queryPhase.table_access()) {
                auto [it, success] = readsByTable.emplace(tableAccess.name(), std::make_pair(0, 0));
                it->second.first += tableAccess.reads().rows();
                it->second.second += tableAccess.reads().bytes();
            }
        }

        for(const auto& [name, rowsAndBytes]: readsByTable) {
            Cerr << name << " " << rowsAndBytes.first << " " << rowsAndBytes.second << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/Join1_2").first, 1);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/Join1_2").second, 19);

        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/Join1_1").first, 8);
        UNIT_ASSERT_VALUES_EQUAL(readsByTable.at("/Root/Join1_1").second, 136);
    }

    Y_UNIT_TEST(AAARangeFullScan) {
        TKikimrRunner kikimr(GetAppConfig());

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Amount < 5000ul ORDER BY Group LIMIT 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        Cerr << result.GetQueryPlan() << Endl;

        CompareYson(R"(
            [
                [[3500u];["None"];[1u];["Anna"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
    }

    const static TString Query = R"(SELECT * FROM `/Root/Test` WHERE Amount < 5000ul ORDER BY Group LIMIT 1;)";
    const static TString Expected = R"([[[3500u];["None"];[1u];["Anna"]]])";

    Y_UNIT_TEST_TWIN(ScanQueryRangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

        auto db = kikimr.GetTableClient();
        EnableDebugLogging(kikimr.GetTestServer().GetRuntime());
        auto query = Q_(Query);

        NYdb::NTable::TStreamExecScanQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto it = db.StreamExecuteScanQuery(query, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
/*
        const auto& stats = *res.QueryStats;

        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
*/
    }

    Y_UNIT_TEST_TWIN(ScanScriptingRangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
        auto query = Q_(Query);

        NYdb::NScripting::TExecuteYqlRequestSettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto it = client.StreamExecuteYqlScript(query, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
    }

    Y_UNIT_TEST(QuerySeviceRangeFullScan) {
        TKikimrRunner kikimr(GetAppConfig());

        NYdb::NQuery::TQueryClient client(kikimr.GetDriver());
        auto query = Q_(Query);

        NYdb::NQuery::TExecuteQuerySettings execSettings;

        auto it = client.StreamExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
            execSettings
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
    }

    void CreateTestTable(auto session, bool isColumn, TString suff="", bool secondaryIndex=false) {
        UNIT_ASSERT(session.ExecuteQuery(std::format(R"(
            --!syntax_v1
            CREATE TABLE `/Root/TestTable{}` (
                Group Uint32 not null,
                Name String not null,
                Amount Uint64,
                Comment String,
                {}
                PRIMARY KEY (Group, Name)
            ) WITH (
                STORE = {},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );

        )",
            suff.c_str(),
            secondaryIndex ? "INDEX idx GLOBAL SYNC ON (Name)," : "",
            isColumn ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).GetValueSync().IsSuccess());

        auto result = session.ExecuteQuery(std::format(R"(
            REPLACE INTO `/Root/TestTable{}` (Group, Name, Amount, Comment) VALUES
                    (1u, "Anna", 3500ul, "None"),
                    (1u, "Paul", 300ul, "None"),
                    (2u, "Tony", 7200ul, "None"),
                    (100000u, "Anna", 3500ul, "None");
        )", suff.c_str()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapPointLookup) {
        TKikimrRunner kikimr(GetAppConfig(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, true);

        auto query = Q_(R"(
            SELECT * FROM `/Root/TestTable` WHERE Group = 1u AND Name = "Anna";
        )");

        auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];
                1u;"Anna"]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        Cerr << stats.query_phases().size() << Endl;
        size_t phase = stats.query_phases_size() - 1;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().bytes(), 36);
    }

    Y_UNIT_TEST(OlapRange) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, true);

        auto query = Q_(R"(
            SELECT * FROM `/Root/TestTable` WHERE Group < 2u ORDER BY Group, Name;
        )");

        auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];1u;"Anna"];
                [[300u];["None"];1u;"Paul"]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = stats.query_phases_size() - 1;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().bytes(), 72);
    }

    Y_UNIT_TEST(OlapRangeFullScan) {
        TKikimrRunner kikimr(GetAppConfig());

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, true);

        auto query = Q_(R"(
            SELECT * FROM `/Root/TestTable` WHERE Amount < 5000ul ORDER BY Group, Name LIMIT 1;
        )");

        auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];1u;"Anna"]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 3); // Limit???
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 108);
    }

    Y_UNIT_TEST(OlapWriteRow) {
        TKikimrRunner kikimr(GetAppConfig(false, false, true));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        const TString testTableName = "/Root/TestTable";
        const ui64 minRowBytes = 300;

        CreateTestTable(session, true);

        auto checkUpdatesStats = [&testTableName, &session](const TString& query, ui64 rows) {
            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            const auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            Cerr << stats.DebugString() << Endl;

            UNIT_ASSERT_VALUES_UNEQUAL(stats.query_phases_size(), 0);
            const auto& phase = stats.query_phases(0);

            UNIT_ASSERT_VALUES_EQUAL(phase.table_access_size(), 1);
            const auto& tableAccess = phase.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess.updates().rows(), rows);
            UNIT_ASSERT_GT(tableAccess.updates().bytes(), rows * minRowBytes);
        };

        checkUpdatesStats(Q_(R"(
            REPLACE INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
        )"), 1);

        checkUpdatesStats(R"(
            UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
        )", 1);

        // INSERT 1 NEW ROW
        checkUpdatesStats(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (3u, "Anna", 3500u, "None");
        )"), 1);

        // INSERT 2 NEW ROWS
        checkUpdatesStats(Q_(R"(
            INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
            VALUES (101u, "Anna", 3500u, "None"), (102u, "Anna", 3500u, "None");
        )"), 2);

        // UPDATE
        // TODO: reads ??
        checkUpdatesStats(Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 3u AS Group, "Anna" AS Name, 4000u AS Amount, "None" AS Comment;
        )"), 1);

        {
            // 2 SEPARATE INSERTS
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (401u, "Anna", 3500u, "None");

                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (402u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            Cerr << stats.DebugString() << Endl;

            UNIT_ASSERT_GT(stats.query_phases_size(), 1);
            const auto& phase0 = stats.query_phases(0);

            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access_size(), 1);
            const auto& tableAccess0 = phase0.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess0.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess0.updates().rows(), 1);
            UNIT_ASSERT_GT(tableAccess0.updates().bytes(), minRowBytes);

            const auto& phase1 = stats.query_phases(1);

            UNIT_ASSERT_VALUES_EQUAL(phase1.table_access_size(), 1);
            const auto& tableAccess1 = phase1.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess1.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess1.updates().rows(), 1);
            UNIT_ASSERT_GT(tableAccess1.updates().bytes(), minRowBytes);
        }

        {
            // INSERT EXISTS
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            // TODO: reads???
            for (const auto& phase : stats.query_phases()) {
                for (const auto& tableAccess : phase.table_access()) {
                    if (tableAccess.name() != testTableName) {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.updates().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.updates().bytes(), 0);
                }
            }
        }

        {
            // UPDATE empty
            auto query = Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 4u AS Group, "Anna" AS Name, 4000u AS Amount, "None" AS Comment;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            // No writes
            // TODO: reads ??
            for (const auto& phase : stats.query_phases()) {
                for (const auto& tableAccess : phase.table_access()) {
                    if (tableAccess.name() != testTableName) {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.updates().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.updates().bytes(), 0);
                }
            }
        }

        {
            // DELETE empty
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 4u AS Group, "Anna" AS Name;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            UNIT_ASSERT_VALUES_UNEQUAL(stats.query_phases_size(), 0);
            const auto& phase = stats.query_phases(0);

            UNIT_ASSERT_VALUES_EQUAL(phase.table_access_size(), 1);
            const auto& tableAccess = phase.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().bytes(), 0);
        }

        {
            // DELETE
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 3u AS Group, "Anna" AS Name;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            UNIT_ASSERT_VALUES_UNEQUAL(stats.query_phases_size(), 0);
            const auto& phase = stats.query_phases(0);

            UNIT_ASSERT_VALUES_EQUAL(phase.table_access_size(), 1);
            const auto& tableAccess = phase.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().bytes(), 0);
        }

        {
            // DELETE THEN INSERT
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 1u AS Group, "Paul" AS Name;

                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment)
                VALUES (501u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            Cerr << stats.DebugString() << Endl;

            UNIT_ASSERT_GT(stats.query_phases_size(), 1);
            const auto& phase0 = stats.query_phases(0);

            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access_size(), 1);
            const auto& tableAccess0 = phase0.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess0.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess0.deletes().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess0.deletes().bytes(), 0);

            const auto& phase1 = stats.query_phases(1);

            UNIT_ASSERT_VALUES_EQUAL(phase1.table_access_size(), 1);
            const auto& tableAccess1 = phase1.table_access(0);

            UNIT_ASSERT_VALUES_EQUAL(tableAccess1.name(), testTableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess1.updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tableAccess1.updates().bytes(), 368);
        }

        {
            // DELETE 2 ROWS
            // rows are added in the 2 SEPARATE INSERTS test above
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT Group, Name FROM `/Root/TestTable` WHERE Group = 401u OR Group = 402u;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            Cerr << stats.DebugString() << Endl;

            bool tableDeletesFound = false;
            for (const auto& phase : stats.query_phases()) {
                for (const auto& tableAccess : phase.table_access()) {
                    if (tableAccess.name() != testTableName) {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().rows(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(tableAccess.deletes().bytes(), 0);
                    UNIT_ASSERT_C(!tableDeletesFound, "Found two deletes entries");
                    tableDeletesFound = true;
                }
            }
            UNIT_ASSERT_C(tableDeletesFound, "No deletes entries found");
        }
    }


    struct TTotalStats {
        size_t Writes = 0;
        size_t Reads = 0;
        size_t Deletes = 0;

        size_t WriteBytes = 0;
        size_t ReadBytes = 0;
        size_t DeleteBytes = 0;
    };

    TTotalStats FromProto(const Ydb::TableStats::QueryStats& proto) {
        TTotalStats stats;
        for (int phase = 0; phase < proto.query_phases_size(); ++phase) {
            for (int access = 0; access < proto.query_phases(phase).table_access_size(); ++access) {
                stats.Writes += proto.query_phases(phase).table_access(access).updates().rows();
                stats.Reads += proto.query_phases(phase).table_access(access).reads().rows();
                stats.Deletes += proto.query_phases(phase).table_access(access).deletes().rows();

                stats.WriteBytes += proto.query_phases(phase).table_access(access).updates().bytes();
                stats.ReadBytes += proto.query_phases(phase).table_access(access).reads().bytes();
                stats.DeleteBytes += proto.query_phases(phase).table_access(access).deletes().bytes();
            }
        }
        return stats;
    }

    void Check(const TTotalStats& lhs, const TTotalStats& rhs) {
        UNIT_ASSERT_VALUES_EQUAL(lhs.Writes, rhs.Writes);
        UNIT_ASSERT_VALUES_EQUAL(lhs.Reads, rhs.Reads);
        UNIT_ASSERT_VALUES_EQUAL(lhs.Deletes, rhs.Deletes);

        UNIT_ASSERT_VALUES_EQUAL(lhs.WriteBytes, rhs.WriteBytes);
        UNIT_ASSERT_VALUES_EQUAL(lhs.ReadBytes, rhs.ReadBytes);
        UNIT_ASSERT_VALUES_EQUAL(lhs.DeleteBytes, rhs.DeleteBytes);
    }


    Y_UNIT_TEST_QUAD(WriteRow, isSink, isOlap) {
        if (isOlap) {
            // TODO: same stats for olap?
            return;
        }
        TKikimrRunner kikimr(GetAppConfig(false, false, isSink));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, isOlap);

        {
            auto query = Q_(R"(
                REPLACE INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);


            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = isOlap ? 0 : stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), 20);

            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                for (int access = 0; access < stats.query_phases(phase).table_access_size(); ++access) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().bytes(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 0,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }

        {
            auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = isOlap ? 0 : stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), 20);

            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                for (int access = 0; access < stats.query_phases(phase).table_access_size(); ++access) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().bytes(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 0,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT EXISTS
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = isSink ? 0 : 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().bytes(), 8);

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT NEW
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (3u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), 20);

            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                for (int access = 0; access < stats.query_phases(phase).table_access_size(); ++access) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().bytes(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 0,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }

        {
            // UPDATE empty
            auto query = Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 4u AS Group, "Anna" AS Name, 4000u AS Amount, "None" AS Comment;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            // No reads & no writes
            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                if (stats.query_phases(phase).table_access_size() > 0) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 0,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }

        {
            // UPDATE
            auto query = Q_(R"(
                UPDATE `/Root/TestTable` ON SELECT 3u AS Group, "Anna" AS Name, 4000u AS Amount, "None" AS Comment;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), 20);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(isSink ? phase : 1).table_access(0).reads().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(isSink ? phase : 1).table_access(0).reads().bytes(), 8);

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // DELETE empty
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 4u AS Group, "Anna" AS Name;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).deletes().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).deletes().bytes(), 0);

            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                for (int access = 0; access < stats.query_phases(phase).table_access_size(); ++access) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().bytes(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 0,
                    .Deletes = 1,

                    .WriteBytes = 0,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }

        {
            // DELETE
            auto query = Q_(R"(
                DELETE FROM `/Root/TestTable` ON SELECT 3u AS Group, "Anna" AS Name;
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            size_t phase = stats.query_phases_size() - 1;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).deletes().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).deletes().bytes(), 0);

            for (int phase = 0; phase < stats.query_phases_size(); ++phase) {
                for (int access = 0; access < stats.query_phases(phase).table_access_size(); ++access) {
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().rows(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(access).reads().bytes(), 0);
                }
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 0,
                    .Deletes = 1,

                    .WriteBytes = 0,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }
    }

    Y_UNIT_TEST_TWIN(WriteRowInsertFailsSecondary, isSink) {
        TKikimrRunner kikimr(GetAppConfig(false, false, isSink));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, false, "", true);
        CreateTestTable(session, false, "2", true);

        {
            // Three inserts
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (10u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (100u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 2,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 28,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT + UPSERT
            auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (10u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (100u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 2,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 28,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT to different tales
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT to different tales
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT&UPSERT to different tales
            auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (1u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (100000u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None"),
                    (100000u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT duplicates
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (999997u, "Anna", 3500u, "None"),
                    (999998u, "Anna", 3500u, "None"),
                    (999999u, "Anna", 3500u, "None"),
                    (999998u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 0,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 0,
                    .DeleteBytes = 0,
                });
        }
    }

    Y_UNIT_TEST_QUAD(WriteRowInsertFails, isSink, isOlap) {
        if (isOlap) {
            // TODO: same stats for olap?
            return;
        }
        TKikimrRunner kikimr(GetAppConfig(false, false, isSink));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, isOlap);
        CreateTestTable(session, isOlap, "2");

        {
            // Three inserts
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (10u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (100u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(3).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(3).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(5).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(5).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT + UPSERT
            auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (10u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (100u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(3).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(3).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 1,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 20,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT to different tales
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(1).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(1).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT to different tales
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(1).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(1).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT&UPSERT to different tales
            auto query = Q_(R"(
                UPSERT INTO `/Root/TestTable` (Group, Name, Amount, Comment) VALUES (1000u, "Anna", 3500u, "None");
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES (1u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 0,
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (1u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = isSink ? 1 : 0, // EvWrite writes before next read
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = isSink ? 20 : 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (100000u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 20);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = isSink ? 1 : 0, // EvWrite writes before next read
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = isSink ? 20 : 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT many
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (1000u, "Anna", 3500u, "None"),
                    (1001u, "Anna", 3500u, "None"),
                    (100000u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 2);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 40);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 8);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = isSink ? 2 : 0, // EvWrite writes before next read
                    .Reads = 1,
                    .Deletes = 0,

                    .WriteBytes = isSink ? 40 : 0,
                    .ReadBytes = 8,
                    .DeleteBytes = 0,
                });
        }

        {
            // INSERT duplicates
            auto query = Q_(R"(
                INSERT INTO `/Root/TestTable2` (Group, Name, Amount, Comment) VALUES
                    (999997u, "Anna", 3500u, "None"),
                    (999998u, "Anna", 3500u, "None"),
                    (999999u, "Anna", 3500u, "None"),
                    (999998u, "Anna", 3500u, "None");
            )");

            auto txControl = NYdb::NQuery::TTxControl::BeginTx().CommitTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            if (isSink) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 3);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 60);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 8);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 0);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().bytes(), 0);
            }

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = isSink ? 3 : 0, // EvWrite writes before next read
                    .Reads = isSink ? 1 : 0,
                    .Deletes = 0,

                    .WriteBytes = isSink ? 60 : 0,
                    .ReadBytes = isSink ? 8 : 0,
                    .DeleteBytes = 0,
                });
        }
    }

    Y_UNIT_TEST_TWIN(CTAS, isOlap) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, isOlap);

        {
            auto query = std::format(R"(
                CREATE TABLE `/Root/TestTable2` (PRIMARY KEY (Group, Name)) WITH (STORE={}) AS SELECT * FROM `/Root/TestTable`;
            )", isOlap ? "COLUMN" : "ROW");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();

            auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());


            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
            size_t phase = 0;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), isOlap ? 1472 : 80);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(1).reads().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(1).reads().bytes(), isOlap ? 144 : 80);

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 4,
                    .Reads = 4,
                    .Deletes = 0,

                    .WriteBytes = isOlap ? 1472 : 80,
                    .ReadBytes = isOlap ? 144 : 80,
                    .DeleteBytes = 0,
                });
        }
    }

    Y_UNIT_TEST_TWIN(CTASWithRetry, isOlap) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->MutableWriteActorSettings()->SetInFlightMemoryLimitPerActorBytes(40);
        // For executing REPLACE
        appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(true);
        appConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        TKikimrSettings settings(appConfig);
        settings.SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            CreateTestTable(session, isOlap);
        });

        size_t messages = 0;

        auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                ++messages;
                auto* msg = ev->Get<NEvents::TDataEvents::TEvWriteResult>();
                for (size_t index = 0; index < 3; ++index) {
                    // Send several duplicates
                    auto copy = std::make_unique<NEvents::TDataEvents::TEvWriteResult>();
                    copy->Record = msg->Record;
                    runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, copy.release(), ev->Flags, ev->Cookie));
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(grab);

        {
            auto query = std::format(R"(
                CREATE TABLE `/Root/TestTable2` (PRIMARY KEY (Group, Name)) WITH (STORE={}) AS SELECT * FROM `/Root/TestTable`;
            )", isOlap ? "COLUMN" : "ROW");

            auto txControl = NYdb::NQuery::TTxControl::NoTx();

            auto result = kikimr.RunCall([&] { return session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());


            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            Cerr << stats.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
            size_t phase = 0;
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).updates().bytes(), isOlap ? 1472 : 80);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(1).reads().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(1).reads().bytes(), isOlap ? 144 : 80);

            Check(
                FromProto(stats),
                TTotalStats{
                    .Writes = 4,
                    .Reads = 4,
                    .Deletes = 0,

                    .WriteBytes = isOlap ? 1472 : 80,
                    .ReadBytes = isOlap ? 144 : 80,
                    .DeleteBytes = 0,
                });
        }

        UNIT_ASSERT_EQUAL(messages, isOlap ? 4 : 1);
    }

    Y_UNIT_TEST(BatchOperation_Update) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);

        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, false);

        const auto query = R"(
            BATCH UPDATE `/Root/TestTable`
                SET Amount = 1000;
        )";

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        Cerr << "BATCH UPDATE: " << Endl << stats.DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 32);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 64);
    }

    Y_UNIT_TEST(BatchOperation_Delete) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);

        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, false);

        const auto query = R"(
            BATCH DELETE FROM `/Root/TestTable`;
        )";

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        Cerr << "BATCH DELETE: " << Endl << stats.DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 32);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).deletes().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).deletes().bytes(), 0);
    }

    Y_UNIT_TEST(BatchOperation_RetryIsFree) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);

        auto settings = TKikimrSettings(appConfig)
            .SetUseRealThreads(false)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            CreateTestTable(session, false);
        });

        // Number of retries to force before success
        constexpr size_t maxRetries = 3;
        size_t retryCount = 0;

        std::optional<TActorId> partitionedId;
        std::set<TActorId> executerIds;

        // Get id of the PartitionedExecuterActor
        using TEvTestGetPartitioned = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto partitionedObserver = runtime.AddObserver<TEvTestGetPartitioned>([&](TEvTestGetPartitioned::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER" && !partitionedId.has_value()) {
                partitionedId = ev->Recipient;
            }
        });

        // Get id of the Executers
        using TEvTestGetExecuter = TEvTxUserProxy::TEvProposeKqpTransaction;
        const auto executerObserver = runtime.AddObserver<TEvTestGetExecuter>([&](TEvTestGetExecuter::TPtr& ev) {
            if (partitionedId.has_value() && ev->Sender == *partitionedId) {
                executerIds.insert(ev->Get()->ExecuterId);
            }
        });

        // Change status of TEvTxResponse to set retriable error status for first maxRetries responses
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (partitionedId.has_value() && ev->Recipient == *partitionedId) {
                if (executerIds.find(ev->Sender) == executerIds.end()) {
                    return;
                }

                if (ev->Get()->Record.GetResponse().GetStatus() == Ydb::StatusIds::SUCCESS) {
                    if (retryCount < maxRetries) {
                        // Set retriable error status to trigger retry
                        ev->Get()->Record.MutableResponse()->SetStatus(Ydb::StatusIds::OVERLOADED);
                        ++retryCount;
                    }
                }
            }
        });

        const auto query = R"(
            BATCH UPDATE `/Root/TestTable`
                SET Amount = 1000;
        )";

        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Ensure retries happened
        UNIT_ASSERT_VALUES_EQUAL_C(retryCount, maxRetries, "Expected " << maxRetries << " retries, got " << retryCount);

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        Cerr << "BATCH UPDATE with retries: " << Endl << stats.DebugString() << Endl;

        // Verify stats are the same as in BatchOperation_Update (retries should not affect final stats)
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 32);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 64);
    }

    Y_UNIT_TEST(BatchOperation_PartialErrorStats) {
        auto appConfig = GetAppConfig(false, false, true);
        // Set MaxBatchSize to 1 so each row is processed in a separate batch
        appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetMaxBatchSize(1);
        appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);

        auto settings = TKikimrSettings(appConfig)
            .SetUseRealThreads(false)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            CreateTestTable(session, false);
        });

        // Number of successful responses before injecting error
        const size_t successBeforeErrorCount = 2;
        size_t responseCount = 0;

        std::optional<TActorId> partitionedId;
        std::set<TActorId> executerIds;

        // Get id of the PartitionedExecuterActor
        using TEvTestGetPartitioned = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto partitionedObserver = runtime.AddObserver<TEvTestGetPartitioned>([&](TEvTestGetPartitioned::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER" && !partitionedId.has_value()) {
                partitionedId = ev->Recipient;
            }
        });

        // Get id of the Executers
        using TEvTestGetExecuter = TEvTxUserProxy::TEvProposeKqpTransaction;
        const auto executerObserver = runtime.AddObserver<TEvTestGetExecuter>([&](TEvTestGetExecuter::TPtr& ev) {
            if (partitionedId.has_value() && ev->Sender == *partitionedId) {
                executerIds.insert(ev->Get()->ExecuterId);
            }
        });

        // On the third successful response, inject INTERNAL_ERROR
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (partitionedId.has_value() && ev->Recipient == *partitionedId) {
                if (executerIds.find(ev->Sender) == executerIds.end()) {
                    return;
                }

                if (ev->Get()->Record.GetResponse().GetStatus() == Ydb::StatusIds::SUCCESS) {
                    if (responseCount == successBeforeErrorCount) {
                        // Inject INTERNAL_ERROR on the third successful response
                        ev->Get()->Record.MutableResponse()->SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
                    }

                    ++responseCount;
                }
            }
        });

        const auto query = R"(
            BATCH UPDATE `/Root/TestTable`
                SET Amount = 1000;
        )";

        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        });

        // The query should fail due to the injected error
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(responseCount, successBeforeErrorCount + 1);

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        Cerr << "BATCH UPDATE with partial error: " << Endl << stats.DebugString() << Endl;

        // Verify stats only count the successfully processed rows (2 out of 4)
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 16);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 32);
    }

    Y_UNIT_TEST(BatchOperation_SecondaryIndex) {
        auto appConfig = GetAppConfig(false, false, true);
        appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);

        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateTestTable(session, false);

        {
            const auto query = R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX `TestIndex` GLOBAL SYNC ON (Amount);
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                BATCH UPDATE `/Root/TestTable`
                    SET Amount = 1000;
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            Cerr << "BATCH UPDATE secondary index: " << Endl << stats.DebugString() << Endl;

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access_size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 64);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().bytes(), 64);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).updates().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).updates().bytes(), 64);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).deletes().rows(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(1).deletes().bytes(), 0);
        }
    }
}

}
}
