#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void PrintMemoryStats(const TString& testName, const TDataQueryResult& result) {
    if (!result.GetStats()) {
        return;
    }

    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(stats.query_plan(), &plan, true);

    std::function<void(const NJson::TJsonValue&)> walkPlan = [&](const NJson::TJsonValue& node) {
        if (node.Has("Stats")) {
            const auto& s = node["Stats"];
            auto nodeType = node.Has("Node Type") ? node["Node Type"].GetStringRobust() : "?";
            ui64 stageId = s.Has("PhysicalStageId") ? s["PhysicalStageId"].GetUIntegerRobust() : 0;
            ui64 peakMem = 0;
            if (s.Has("MaxMemoryUsage") && s["MaxMemoryUsage"].Has("Sum")) {
                peakMem = s["MaxMemoryUsage"]["Sum"].GetUIntegerRobust();
            }
            ui64 outputBytes = s.Has("OutputBytes") ? s["OutputBytes"]["Sum"].GetUIntegerRobust() : 0;
            ui64 inputBytes = s.Has("InputBytes") ? s["InputBytes"]["Sum"].GetUIntegerRobust() : 0;
            ui64 durationUs = 0;
            if (s.Has("DurationUs") && s["DurationUs"].Has("Sum")) {
                durationUs = s["DurationUs"]["Sum"].GetUIntegerRobust();
            }
            ui64 cpuUs = 0;
            if (s.Has("CpuTimeUs") && s["CpuTimeUs"].Has("Sum")) {
                cpuUs = s["CpuTimeUs"]["Sum"].GetUIntegerRobust();
            }

            Cerr << "  Stage " << stageId << " (" << nodeType << ")"
                 << ": PeakMemory=" << (peakMem / 1024 / 1024) << "MB"
                 << ", Duration=" << (durationUs / 1000) << "ms"
                 << ", Cpu=" << (cpuUs / 1000) << "ms"
                 << ", In=" << (inputBytes / 1024 / 1024) << "MB"
                 << ", Out=" << (outputBytes / 1024 / 1024) << "MB"
                 << Endl;
        }

        if (node.Has("Plans")) {
            for (const auto& child : node["Plans"].GetArraySafe()) {
                walkPlan(child);
            }
        }
    };

    Cerr << "=== " << testName << " memory stats ===" << Endl;
    Cerr << "  TotalDuration=" << (stats.total_duration_us() / 1000) << "ms"
         << ", TotalCpu=" << (stats.total_cpu_time_us() / 1000) << "ms" << Endl;
    walkPlan(plan["Plan"]);
    Cerr << "================================================" << Endl;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpStreamLookup) {

    Y_UNIT_TEST(StreamLookupManyPartitions) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        constexpr ui64 TotalRows = 100000;
        constexpr ui64 BatchSize = 1000;

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/MainTable` (
                    Key Uint64,
                    Fk Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX FkIndex GLOBAL ON (Fk)
                ) WITH (
                    UNIFORM_PARTITIONS = 2000,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000
                );
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString padding(10240, 'x');
        for (ui64 offset = 0; offset < TotalRows; offset += BatchSize) {
            auto params = db.GetParamsBuilder()
                .AddParam("$offset").Uint64(offset).Build()
                .AddParam("$padding").String(padding).Build()
                .Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $offset AS Uint64;
                DECLARE $padding AS String;

                $data = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i * 184467440737095ul AS Key, 1ul AS Fk, $padding AS Value); }
                );

                UPSERT INTO `/Root/MainTable`
                SELECT * FROM AS_TABLE($data);
            )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

            auto result = session.ExecuteDataQuery(Q1_(R"(
                $q = SELECT Value FROM `/Root/MainTable` VIEW FkIndex WHERE Fk = 1;

                SELECT COUNT(Value) AS cnt FROM (
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                );
            )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            PrintMemoryStats("StreamLookupManyPartitions", result);

            auto rs = result.GetResultSet(0);
            TResultSetParser parser(rs);
            UNIT_ASSERT(parser.TryNextRow());
            ui64 cnt = parser.ColumnParser("cnt").GetUint64();
            Cerr << "StreamLookupManyPartitions: count = " << cnt << Endl;
            UNIT_ASSERT_VALUES_EQUAL(cnt, 8 * TotalRows);
        }
    }

    // Runs StreamLookupJoinManyPartitions with the given right-table storage kind.
    // When rightIsColumn is true, the right (lookup) table is column-store and the
    // stream lookup join reads it via the new TEvDataShard::TEvRead handler in ColumnShard.
    void DoStreamLookupJoinManyPartitions(bool rightIsColumn) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        constexpr ui64 TotalRows = 100000;
        constexpr ui64 BatchSize = 1000;

        const TString rightStore = rightIsColumn ? "WITH (STORE = COLUMN)" :
            "WITH (UNIFORM_PARTITIONS = 2000, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000)";

        {
            auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE TABLE `/Root/RightTable` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                ) %s;

                CREATE TABLE `/Root/LeftTable` (
                    Id Uint64,
                    Fk Uint64,
                    PRIMARY KEY (Id)
                );
            )", rightStore.c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString padding(10240, 'x');
        for (ui64 offset = 0; offset < TotalRows; offset += BatchSize) {
            auto params = db.GetParamsBuilder()
                .AddParam("$offset").Uint64(offset).Build()
                .AddParam("$padding").String(padding).Build()
                .Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $offset AS Uint64;
                DECLARE $padding AS String;

                $right = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i * 184467440737095ul AS Key, $padding AS Value); }
                );

                UPSERT INTO `/Root/RightTable`
                SELECT * FROM AS_TABLE($right);

                $left = ListMap(
                    ListFromRange($offset, $offset + 1000ul),
                    ($i) -> { RETURN AsStruct($i AS Id, $i * 184467440737095ul AS Fk); }
                );

                UPSERT INTO `/Root/LeftTable`
                SELECT * FROM AS_TABLE($left);
            )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

            auto result = session.ExecuteDataQuery(Q1_(R"(
                $q = SELECT b.Value AS Value
                    FROM `/Root/LeftTable` a
                    JOIN `/Root/RightTable` b ON a.Fk = b.Key;

                SELECT COUNT(Value) AS cnt FROM (
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                    UNION ALL
                    SELECT * FROM $q
                );
            )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            PrintMemoryStats("StreamLookupJoinManyPartitions", result);

            auto rs = result.GetResultSet(0);
            TResultSetParser parser(rs);
            UNIT_ASSERT(parser.TryNextRow());
            ui64 cnt = parser.ColumnParser("cnt").GetUint64();
            Cerr << "StreamLookupJoinManyPartitions: count = " << cnt << Endl;
            UNIT_ASSERT_VALUES_EQUAL(cnt, 8 * TotalRows);
        }
    }

    Y_UNIT_TEST(StreamLookupJoinManyPartitions) {
        DoStreamLookupJoinManyPartitions(/* rightIsColumn */ false);
    }

    // Stream Lookup Join with a column-store right (lookup) table.
    // The stream lookup issues point reads by primary key against ColumnShard
    // via TEvDataShard::TEvRead.
    Y_UNIT_TEST(StreamLookupJoinManyPartitionsRightColumn) {
        DoStreamLookupJoinManyPartitions(/* rightIsColumn */ true);
    }

    // Simple Stream Idx Lookup Join cases with column-store (OLAP) tables.
    // The left (streaming) side supplies join keys, the right side is a column-store
    // table looked up by its primary key.

    void CreateSimpleJoinTables(NYdb::NQuery::TQueryClient& db, bool leftColumn, bool rightIsColumn) {
        auto session = db.GetSession().GetValueSync().GetSession();

        const TString leftStore = leftColumn ? "WITH (STORE = COLUMN)" : "";
        const TString rightStore = rightIsColumn ? "WITH (STORE = COLUMN)" : "";

        auto ddl = Sprintf(R"(
            CREATE TABLE `/Root/LeftTable` (
                Id Int32 NOT NULL,
                Fk Int32 NOT NULL,
                PRIMARY KEY (Id)
            ) %s;

            CREATE TABLE `/Root/RightTable` (
                Key Int32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            ) %s;
        )", leftStore.c_str(), rightStore.c_str());

        auto result = session.ExecuteQuery(ddl, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto dml = session.ExecuteQuery(R"(
            REPLACE INTO `/Root/LeftTable` (Id, Fk) VALUES
                (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);

            REPLACE INTO `/Root/RightTable` (Key, Value) VALUES
                (10, "Value10"), (20, "Value20"), (30, "Value30"), (40, "Value40");
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(dml.IsSuccess(), dml.GetIssues().ToString());
    }

    // Runs `LeftTable JOIN RightTable ON Fk = Key` with the requested storage
    // kind for each side and checks both the produced rows and whether a Stream
    // Idx Lookup Join was actually chosen by the optimizer.
    //
    // Stream Idx Lookup Join performs point lookups by primary key against the
    // right (lookup) table using the datashard read-iterator protocol
    // (TEvDataShard::TEvRead). ColumnShard now implements that protocol (see
    // ydb/core/tx/columnshard/columnshard__read.cpp), so a column-store table is
    // allowed on the lookup (right) side.
    //
    // When both sides are column-store the optimizer instead keeps the whole plan
    // in block/OLAP form and picks a block-based broadcast MapJoin; that is an
    // independent optimizer strategy choice for OLAP-on-OLAP joins and does not
    // depend on read-iterator support. In every case the produced rows must match.
    void DoSimpleStreamLookupJoin(bool leftColumn, bool rightIsColumn) {
        const bool expectStreamLookup = !(leftColumn && rightIsColumn);

        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();

        CreateSimpleJoinTables(db, leftColumn, rightIsColumn);

        const TString query = R"(
            SELECT a.Id AS Id, b.Value AS Value
            FROM `/Root/LeftTable` AS a
            INNER JOIN `/Root/RightTable` AS b ON a.Fk = b.Key
            ORDER BY Id;
        )";

        {
            auto explain = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
            auto ast = explain.GetStats()->GetAst();
            UNIT_ASSERT(ast.has_value());
            Cerr << "=== AST (leftColumn=" << leftColumn << ", rightIsColumn=" << rightIsColumn << ") ===" << Endl;
            Cerr << *ast << Endl;
            const bool hasStreamLookup = TString(*ast).Contains("StreamLookup");
            UNIT_ASSERT_VALUES_EQUAL_C(hasStreamLookup, expectStreamLookup,
                TStringBuilder() << "Unexpected join strategy for leftColumn=" << leftColumn
                    << ", rightIsColumn=" << rightIsColumn << "; AST: " << *ast);
        }

        auto result = db.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [1;["Value10"]];
            [2;["Value20"]];
            [3;["Value30"]];
            [4;["Value40"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    // Right (lookup) side is a column-store table: Stream Lookup Join issues point
    // reads by primary key against ColumnShard via TEvDataShard::TEvRead.
    Y_UNIT_TEST(StreamLookupJoinRightColumnTable) {
        DoSimpleStreamLookupJoin(/* leftColumn */ false, /* rightIsColumn */ true);
    }

    // Left (streaming) side is a column-store table, lookup side is row-store:
    // Stream Lookup Join is used as usual.
    Y_UNIT_TEST(StreamLookupJoinLeftColumnTable) {
        DoSimpleStreamLookupJoin(/* leftColumn */ true, /* rightIsColumn */ false);
    }

    // Both sides are column-store tables: the optimizer keeps the plan in block/OLAP
    // form and falls back to a block-based broadcast MapJoin. Result must still be
    // correct.
    Y_UNIT_TEST(StreamLookupJoinBothColumnTables) {
        DoSimpleStreamLookupJoin(/* leftColumn */ true, /* rightIsColumn */ true);
    }
}

} // namespace NKqp
} // namespace NKikimr
