#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static NKikimrConfig::TAppConfig GetAppConfig(bool sourceRead, bool streamLookup = true, bool streamLookupJoin = false) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(sourceRead);
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(sourceRead);
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(streamLookup);
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(streamLookupJoin);
    return app;
}

static NYdb::NTable::TExecDataQuerySettings GetDataQuerySettings() {
    NYdb::NTable::TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
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
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_CONVEYOR, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
    }

    Y_UNIT_TEST_TWIN(IndexLookup, StreamLookup) {
        TKikimrRunner kikimr(GetAppConfig(true, StreamLookup));
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

    Y_UNIT_TEST_TWIN(IndexLookupAtLeast8BytesInStorage, StreamLookup) {
        TKikimrRunner kikimr(GetAppConfig(true, StreamLookup));
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

    Y_UNIT_TEST_TWIN(IndexLookupAndTake, StreamLookup) {
        TKikimrRunner kikimr(GetAppConfig(true, StreamLookup));
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

    Y_UNIT_TEST_TWIN(PointLookup, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead, false));
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

    Y_UNIT_TEST_TWIN(Range, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));
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
        TKikimrRunner kikimr(GetAppConfig(true, true, StreamLookupJoin));
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

    Y_UNIT_TEST_TWIN(RangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Amount < 5000ul ORDER BY Group LIMIT 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

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

    Y_UNIT_TEST_TWIN(QuerySeviceRangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

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

}

}
}
