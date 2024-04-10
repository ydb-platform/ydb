#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static void CreateSampleTables(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/TestNulls` (
            Key1 Uint32,
            Key2 Uint32,
            Value String,
            PRIMARY KEY (Key1, Key2)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TestNulls` (Key1, Key2, Value) VALUES
            (NULL, NULL, "One"),
            (NULL, 100u, "Two"),
            (NULL, 200u, "Three"),
            (1u,   NULL, "Four"),
            (1u,   100u, "Five"),
            (1u,   200u, "Six"),
            (2u,   NULL, "Seven"),
            (2u,   100u, "Eight"),
            (2u,   200u, "Nine"),
            (3u,   100u, "Ten"),
            (3u,   200u, "Eleven"),
            (3u,   300u, "Twelve"),
            (3u,   400u, "Thirteen"),
            (3u,   500u, "Fourteen"),
            (3u,   600u, NULL);
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/TestDate` (
            Key Date,
            Value String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TestDate` (Key, Value) VALUES
            (NULL, "One"),
            (Date("2019-05-08"), "Two"),
            (Date("2019-07-01"), "Three");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/TestJson` (
            Key1 Int32,
            Key2 Int32,
            Value Json,
            PRIMARY KEY (Key1, Key2)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TestJson` (Key1, Key2, Value) VALUES
            (0, 0, NULL),
            (0, 1, "[0]"),
            (1, 0, NULL),
            (1, 1, "[1]");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    {
        auto builder = TTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::Utf8)
            .AddNullableColumn("ValueInt", EPrimitiveType::Int32)
            .SetPrimaryKeyColumn("Key");

        UNIT_ASSERT(session.CreateTable("/Root/MultiShardTable",
            builder.Build(),
            TCreateTableSettings()
                .PartitioningPolicy(
                    TPartitioningPolicy()
                        .UniformPartitions(5)
                )
            ).GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/MultiShardTable` (Key, Value) VALUES
                (1, "One"),
                (2, "Two"),
                (3, "Three"),
                (4, "Four"),
                (4294967295, "LastOne");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/MultiShardTable` (Key, ValueInt) VALUES
                (10, 10);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());
    }

    {
        auto builder = TTableBuilder()
            .AddNullableColumn("Key1", EPrimitiveType::Uint32)
            .AddNullableColumn("Key2", EPrimitiveType::String)
            .AddNullableColumn("ValueInt", EPrimitiveType::Int32)
            .SetPrimaryKeyColumns({"Key1", "Key2"});

        UNIT_ASSERT(session.CreateTable("/Root/MultiShardTableCk",
            builder.Build(),
            TCreateTableSettings()
                .PartitioningPolicy(
                    TPartitioningPolicy()
                        .UniformPartitions(5)
                )
            ).GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/MultiShardTableCk` (Key1, Key2, ValueInt) VALUES
                (1, "One", NULL),
                (2, "Two", NULL),
                (3, "Three", NULL),
                (4, "Four", NULL),
                (4294967295, "LastOne", NULL),
                (10, "Ten", 10);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());
    }
}

namespace {

void CreateTableWithIntKey(TSession session, ui64 partitions, ui32 rangesPerPartition) {
    /*
     * Generate table with partitioning.
     * Every partition will start from 1000 * partitionNumber and have rangesPerPartition subranges
     * from 0 to 100 * rangeNumber
     */
    TExplicitPartitions explicitPartitions;
    static const ui32 itemsPerRange = 5;
    auto splitPoint = [](ui32 partitionNo) {
        return partitionNo * 100000;
    };

    YQL_ENSURE(rangesPerPartition < 100);

    for (ui32 i = 0; i < partitions; i++) {
        explicitPartitions.AppendSplitPoints(
            TValueBuilder().BeginTuple().AddElement().OptionalInt32(splitPoint(i)).EndTuple().Build()
            );
    }

    auto builder = TTableBuilder()
        .AddNullableColumn("Key1", EPrimitiveType::Int32)
        .AddNullableColumn("Key2", EPrimitiveType::Int32)
        .SetPrimaryKeyColumns({"Key1", "Key2"})
        .SetPartitionAtKeys(explicitPartitions);

    UNIT_ASSERT(session.CreateTable("/Root/TableWithIntKey", builder.Build()).GetValueSync().IsSuccess());

    TStringBuilder query;

    query << "REPLACE INTO `/Root/TableWithIntKey` (Key1, Key2) VALUES" << Endl;

    for (ui32 i = 0; i < partitions; i++) {
        ui32 partitionStart = splitPoint(i);

        for (ui32 j = 0; j < rangesPerPartition; j++) {
            ui32 rangeStart = j * 1000;

            for (ui32 k = 0; k < itemsPerRange; k++) {
                query << "(" << partitionStart + rangeStart + k << ", ";

                if (k % 2) {
                    query << "NULL)";
                } else {
                    query << k << ")";
                }

                query << "," << Endl;
            }
        }
    }

    query << "(NULL, NULL);" << Endl;

    bool success = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
    ).GetValueSync().IsSuccess();

    UNIT_ASSERT(success);
}

void ExecuteStreamQueryAndCheck(NYdb::NTable::TTableClient& db, const TString& query,
    const TString& expectedYson)
{
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
    UNIT_ASSERT(it.IsSuccess());

    auto res = CollectStreamResult(it);

    Cerr << "---------QUERY----------" << Endl;
    Cerr << query << Endl;
    Cerr << "---------RESULT---------" << Endl;
    Cerr << res.ResultSetYson << Endl;
    Cerr << "------------------------" << Endl;

    CompareYson(expectedYson, res.ResultSetYson);

    UNIT_ASSERT(res.QueryStats);
    ui64 readRows = res.QueryStats->query_phases().rbegin()->table_access(0).reads().rows();
    ui64 resultRows = res.RowsCount;

    UNIT_ASSERT_EQUAL_C(resultRows, readRows, "There are " << resultRows << " in result, but read " << readRows << " !");
}

void RunTestOverIntTable(const TString& query, const TString& expectedYson, ui64 partitions, ui32 rangesPerPartition) {
    TKikimrSettings kikimrSettings;
    TKikimrRunner kikimr(kikimrSettings);

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateTableWithIntKey(session, partitions, rangesPerPartition);

    ExecuteStreamQueryAndCheck(db, query, expectedYson);
}

void RunPredicateTest(const std::vector<TString>& predicates, bool withNulls) {
    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto builder = TTableBuilder()
        .AddNullableColumn("Key1", EPrimitiveType::Uint32)
        .AddNullableColumn("Key2", EPrimitiveType::Uint32)
        .AddNullableColumn("Key3", EPrimitiveType::String)
        .AddNullableColumn("Key4", EPrimitiveType::String)
        .AddNullableColumn("Value", EPrimitiveType::Uint32)
        .SetPrimaryKeyColumns({"Key1", "Key2", "Key3", "Key4"});

    UNIT_ASSERT(session.CreateTable(
        "/Root/TestPredicates",
        builder.Build(),
        TCreateTableSettings()
            .PartitioningPolicy(
                TPartitioningPolicy()
                    .UniformPartitions(3)
            )
    ).GetValueSync().IsSuccess());

    TString query;

    if (withNulls) {
        query = TString(R"(
            REPLACE INTO `/Root/TestPredicates` (Key1, Key2, Key3, Key4, Value) VALUES
                (NULL, NULL, NULL, NULL, 1),
                (NULL, NULL, NULL, "uid:10", 2),
                (NULL, NULL, "resource_1", "uid:10", 3),
                (NULL, 1, "resource_1", "uid:10", 4),
                (1000, 1, "resource_1", "uid:10", 5),
                (NULL, NULL, "resource_1", NULL, 6),
                (NULL, NULL, NULL, "uid:11", 7),
                (NULL, NULL, "resource_2", "uid:11", 8),
                (NULL, 2, "resource_2", "uid:11", 9),
                (2000, 2, "resource_3", "uid:11", 10),
                (3000, 3, "resource_3", "uid:11", 11),
                (4000, 4, "resource_3", "uid:11", 12),
                (5000, 5, "resource_4", "uid:12", 13),
                (6000, 5, "resource_4", "uid:12", 14),
                (7000, 5, "resource_4", "uid:12", 15),
                (8000, 8, "resource_4", "uid:12", 16),
                (8000, NULL, "resource_5", "uid:12", 17),
                (9000, NULL, "resource_5", NULL, 18),
                (9000, 9, NULL, "uid:12", 19),
                (9000, 9, NULL, NULL, 20);
        )");
    } else {
        query = TString(R"(
            REPLACE INTO `/Root/TestPredicates` (Key1, Key2, Key3, Key4, Value) VALUES
                (1, 0, "resource_0", "uid_0", 1),
                (2, 0, "resource_0", "uid:10", 2),
                (3, 0, "resource_1", "uid:10", 3),
                (4, 1, "resource_1", "uid:10", 4),
                (1000, 1, "resource_1", "uid:10", 5),
                (1001, 0, "resource_1", "uid:0", 6),
                (1002, 0, "resource_0", "uid:11", 7),
                (1003, 0, "resource_2", "uid:11", 8),
                (1004, 2, "resource_2", "uid:11", 9),
                (2000, 2, "resource_3", "uid:11", 10),
                (3000, 3, "resource_3", "uid:11", 11),
                (4000, 4, "resource_3", "uid:11", 12),
                (5000, 5, "resource_4", "uid:12", 13),
                (6000, 5, "resource_4", "uid:12", 14),
                (7000, 5, "resource_4", "uid:12", 15),
                (8000, 8, "resource_4", "uid:12", 16),
                (8000, 0, "resource_5", "uid:12", 17),
                (9000, 0, "resource_5", "uid:0", 18),
                (9000, 9, "resource_0", "uid:12", 19),
                (9000, 9, "resource_0", "uid:0", 20);
            )");
    }

    UNIT_ASSERT(
        session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync().IsSuccess()
    );

    auto streamDb = kikimr.GetTableClient();

    for (auto& item: predicates) {
        TString query = R"(
            SELECT `Value` FROM `/Root/TestPredicates` WHERE <PREDICATE> ORDER BY `Value`;
        )";

        SubstGlobal(query, "<PREDICATE>", item);

        Cerr << "Execute query" << Endl << query << Endl;

        auto it = streamDb.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT(it.IsSuccess());

        auto expectedYson = StreamResultToYson(it);
        it = streamDb.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT(it.IsSuccess());

        auto resultYson = StreamResultToYson(it);

        Cerr << "EXPECTED: " << expectedYson << Endl;
        Cerr << "RECEIVED: " << resultYson << Endl;

        CompareYson(expectedYson, resultYson);
    }

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        DROP TABLE `/Root/TestPredicates`;
    )").GetValueSync().IsSuccess());

}

} // anonymous namespace end

Y_UNIT_TEST_SUITE(KqpRanges) {
    Y_UNIT_TEST(IsNull) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Value FROM `/Root/TestNulls` WHERE
                    Key1 IS NULL AND Key2 IS NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNotNullSecondComponent) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestNulls` WHERE
                    Key1 IS NULL AND Key2 IS NOT NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[#;[100u];["Two"]];[#;[200u];["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNullInValue) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestNulls` WHERE
                    Key1 = 3u AND Value IS NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[3u];[600u];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNullInJsonValue) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestJson` WHERE
                    Key1 = 0 AND Value IS NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[0];[0];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNotNullInValue) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestNulls` WHERE
                    Key1 = 3u AND Value IS NOT NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[3u];[100u];["Ten"]];
            [[3u];[200u];["Eleven"]];
            [[3u];[300u];["Twelve"]];
            [[3u];[400u];["Thirteen"]];
            [[3u];[500u];["Fourteen"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNotNullInJsonValue) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestJson` WHERE
                    Key1 = 0 AND Value IS NOT NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[0];[1];["[0]"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNotNullInJsonValue2) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);
        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TestJson` WHERE
                    Value IS NOT NULL
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[0];[1];["[0]"]];
            [[1];[1];["[1]"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IsNullPartial) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/Test`
                WHERE Group == 1 AND Name IS NULL
            )"),
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(NullInKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Value FROM `/Root/TestNulls` WHERE
                Key1 <= 1
            ORDER BY Value
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[["Five"]];[["Four"]];[["Six"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(NullInKeySuffix) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Value FROM `/Root/TestNulls` WHERE Key1 > 1
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([
            [["Seven"]];[["Eight"]];[["Nine"]];[["Ten"]];[["Eleven"]];[["Twelve"]];[["Thirteen"]];[["Fourteen"]];[#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(NullInPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto params = db.GetParamsBuilder()
            .AddParam("$key1")
                .OptionalUint32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
                DECLARE $key1 AS Uint32?;
                DECLARE $key2 AS Uint32?;
                SELECT Value FROM `/Root/TestNulls` WHERE
                    Key1 = $key1 AND Key2 >= $key2
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(NullInPredicateRow) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto params = db.GetParamsBuilder()
            .AddParam("$key1")
                .OptionalUint32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
                DECLARE $key1 AS Uint32?;
                DECLARE $key2 AS Uint32?;
                SELECT Value FROM `/Root/TestNulls` WHERE
                    Key1 = $key1 AND Key2 == $key2
            )"),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(WhereInSubquery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Paul")
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $name AS String;
            $groups = (SELECT Group FROM `/Root/Test` WHERE Name = $name);
            SELECT * FROM `/Root/TwoShard` WHERE Key in $groups;
        )"), TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateMulti) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(UPDATE `/Root/KeyValue` SET Value = Value || "_updated" WHERE Key IN (1, 2, 3, 4))");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = Q_(R"(SELECT Key, Value FROM `/Root/KeyValue` ORDER BY Key)");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([[[1u];["One_updated"]];
                            [[2u];["Two_updated"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateWhereInNoFullScan) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_("UPDATE `/Root/MultiShardTable` SET Value = 'aaaaa' WHERE Key IN (1, 500)");

        {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());

            auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            size_t readPhase = 0;
            if (stats.query_phases().size() == 3) {
                readPhase = 1;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
            }

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase).affected_shards(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase).table_access(0).name(), "/Root/MultiShardTable");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase).table_access(0).reads().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase).table_access(0).partitions_count(), 1);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase + 1).affected_shards(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase + 1).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase + 1).table_access(0).name(), "/Root/MultiShardTable");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase + 1).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(readPhase + 1).table_access(0).partitions_count(), 1);
        }
    }

    Y_UNIT_TEST(UpdateWhereInWithNull) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);

        CreateSampleTables(session);

        const TString query = Q_(R"(
            UPDATE `/Root/MultiShardTable` SET ValueInt = ValueInt + 1
            WHERE Key IN (1,2,3,4,NULL))");
        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Key, ValueInt FROM `/Root/MultiShardTable` ORDER BY Key;

            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([[[1u];#];[[2u];#];[[3u];#];[[4u];#];[[10u];[10]];[[4294967295u];#]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateWhereInBigLiteralList) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/MultiShardTable` SET ValueInt = ValueInt + 1 WHERE Key IN
                (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
                31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
                61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,
                91,92,93,94,95,96,97,98,99,100)
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Key, ValueInt FROM `/Root/MultiShardTable` ORDER BY Key
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            CompareYson(R"([[[1u];#];[[2u];#];[[3u];#];[[4u];#];[[10u];[11]];[[4294967295u];#]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateWhereInBigLiteralListPrefix) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/MultiShardTableCk` SET ValueInt = ValueInt + 1 WHERE Key1 IN
                (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
                31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
                61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,
                91,92,93,94,95,96,97,98,99,100)
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Key1, ValueInt FROM `/Root/MultiShardTableCk` ORDER BY Key1;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            CompareYson(R"([[[1u];#];[[2u];#];[[3u];#];[[4u];#];[[10u];[11]];[[4294967295u];#]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateWhereInMultipleUpdate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);

        CreateSampleTables(session);

        const TString query = Q_(R"(
                UPDATE `/Root/MultiShardTable` SET ValueInt = ValueInt + 1 WHERE Key IN (1,2,10);
                UPDATE `/Root/TestNulls` SET Value = 'qq' WHERE Key1 IN (1,2);
            )");

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Key, ValueInt FROM `/Root/MultiShardTable` ORDER BY Key;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([[[1u];#];[[2u];#];[[3u];#];[[4u];#];[[10u];[11]];[[4294967295u];#]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT Key1, Value FROM `/Root/TestNulls` ORDER BY Key1;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([
                [#;["One"]];
                [#;["Two"]];
                [#;["Three"]];
                [[1u];["qq"]];
                [[1u];["qq"]];
                [[1u];["qq"]];
                [[2u];["qq"]];
                [[2u];["qq"]];
                [[2u];["qq"]];
                [[3u];["Ten"]];
                [[3u];["Eleven"]];
                [[3u];["Twelve"]];
                [[3u];["Thirteen"]];
                [[3u];["Fourteen"]];
                [[3u];#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(UpdateWhereInFullScan) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_("UPDATE `/Root/MultiShardTable` SET Value = 'aaaaa' WHERE Value IN ('One', 'www')");

        {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(result.IsSuccess());

            auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/MultiShardTable");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 6);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 5);

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/MultiShardTable");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
        }
    }

    Y_UNIT_TEST(DateKeyPredicate) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);
        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Value FROM `/Root/TestDate`
            WHERE Key = Date("2019-07-01")
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);

        if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 1);
        }
    }

    Y_UNIT_TEST(DuplicateKeyPredicateLiteral) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Key FROM `/Root/EightShard` WHERE
                Key > 200 AND Key >= 301 AND
                Key < 600 AND Key <= 501
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[301u]];[[302u]];[[303u]];[[401u]];[[402u]];[[403u]];[[501u]]])",
                FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DuplicateKeyPredicateParam) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key_from_1").Uint64(200).Build()
            .AddParam("$key_from_2").Uint64(301).Build()
            .AddParam("$key_to_1").Uint64(600).Build()
            .AddParam("$key_to_2").Uint64(501).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $key_from_1 AS Uint64;
            DECLARE $key_from_2 AS Uint64;
            DECLARE $key_to_1 AS Uint64;
            DECLARE $key_to_2 AS Uint64;

            SELECT Key FROM `/Root/EightShard` WHERE
                Key > $key_from_1 AND Key >= $key_from_2 AND
                Key < $key_to_1 AND Key <= $key_to_2
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[301u]];[[302u]];[[303u]];[[401u]];[[402u]];[[403u]];[[501u]]])",
            FormatResultSetYson(result.GetResultSet(0)));

    }

    Y_UNIT_TEST(DuplicateKeyPredicateMixed) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key_from_1").Uint64(200).Build()
            .AddParam("$key_to_1").Uint64(600).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $key_from_1 AS Uint64;
            DECLARE $key_to_1 AS Uint64;

            SELECT Key FROM `/Root/EightShard` WHERE
                Key > $key_from_1 AND Key >= 301 AND
                Key < $key_to_1 AND Key <= 501
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[301u]];[[302u]];[[303u]];[[401u]];[[402u]];[[403u]];[[501u]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DuplicateCompositeKeyPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Key2 FROM `/Root/TestNulls` WHERE Key1 = 3 AND
                Key2 >= 100 AND Key2 > 200 AND
                Key2 <= 600 AND Key2 < 500
            ORDER BY Key2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u]];[[400u]]])",
            FormatResultSetYson(result.GetResultSet(0)));

        auto params = db.GetParamsBuilder()
            .AddParam("$key_from_1").OptionalUint32(100).Build()
            .AddParam("$key_from_2").OptionalUint32(200).Build()
            .AddParam("$key_to_1").OptionalUint32(600).Build()
            .AddParam("$key_to_2").OptionalUint32(500).Build()
            .Build();

        result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $key_from_1 AS Uint32?;
            DECLARE $key_from_2 AS Uint32?;
            DECLARE $key_to_1 AS Uint32?;
            DECLARE $key_to_2 AS Uint32?;

            SELECT Key2 FROM `/Root/TestNulls` WHERE Key1 = 3 AND
                Key2 >= $key_from_1 AND Key2 > $key_from_2 AND
                Key2 <= $key_to_1 AND Key2 < $key_to_2
            ORDER BY Key2;
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u]];[[400u]]])",
            FormatResultSetYson(result.GetResultSet(0)));

        params = db.GetParamsBuilder()
            .AddParam("$key_from_2").OptionalUint32(200).Build()
            .AddParam("$key_to_2").OptionalUint32(500).Build()
            .Build();

        result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $key_from_2 AS Uint32?;
            DECLARE $key_to_2 AS Uint32?;

            SELECT Key2 FROM `/Root/TestNulls` WHERE Key1 = 3 AND
                Key2 >= 100 AND Key2 > $key_from_2 AND
                Key2 <= 600 AND Key2 < $key_to_2
            ORDER BY Key2;
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u]];[[400u]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ScanKeyPrefix) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            DECLARE $key1_from AS Uint32;
            DECLARE $name AS String;
            SELECT * FROM `/Root/Join2` WHERE Key1 > $key1_from AND Name = $name;
        )";

        auto result = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Cerr << result.GetPlan() << Endl;

        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(result.GetPlan(), &plan));

        UNIT_ASSERT_VALUES_EQUAL(plan["tables"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["name"], "/Root/Join2");
        UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["reads"].GetArray().size(), 1);
        auto& read = plan["tables"][0]["reads"][0];
        UNIT_ASSERT(!read.Has("lookup_by"));
    }

    Y_UNIT_TEST(DeleteNotFullScan) {
        TKikimrSettings serverSettings;
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            DELETE FROM `/Root/Join2`
            WHERE Key1 = 102 AND Key2 = "One" OR
                  Key1 = 101 AND Key2 = "Two" OR
                  Key1 = 101 AND Key2 = "Three"
        )");

        NYdb::NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        Cerr << result.GetQueryPlan() << Endl;

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).deletes().rows(), 3);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
    }

    Y_UNIT_TEST(LiteralOr) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);
        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/EightShard`
            WHERE Key = 101 OR Key = 302 OR Key = 403 OR Key = 705
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[101u];["Value1"]];
            [[2];[302u];["Value2"]];
            [[2];[403u];["Value3"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = 0;
        if (stats.query_phases().size() == 2) {
            phase = 1;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 3);

        if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).affected_shards(), 4);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).partitions_count(), 4);
        }
    }

    Y_UNIT_TEST(LiteralOrCompisite) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Logs`
            WHERE
                App = "apache" AND Ts = 0 OR
                App = "nginx" AND Ts = 2 OR
                App = "kikimr-db" AND Ts = 4 OR
                App = "ydb" AND Ts = 5
            ORDER BY App, Ts;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [["apache"];["front-42"];[" GET /index.html HTTP/1.1"];[0]];
            [["kikimr-db"];["kikimr-db-53"];["Discover"];[4]];
            [["nginx"];["nginx-23"];["PUT /form HTTP/1.1"];[2]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = 0;
        if (stats.query_phases().size() == 2) {
            phase = 1;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 3);
    }

    Y_UNIT_TEST(LiteralOrCompisiteCollision) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Logs`
            WHERE
                App = "apache" AND Ts = 0 OR
                App = "nginx" AND Ts = 2 OR
                App = "nginx" AND Ts = 3 OR
                App = "ydb" AND Ts = 5
            ORDER BY App, Ts;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [["apache"];["front-42"];[" GET /index.html HTTP/1.1"];[0]];
            [["nginx"];["nginx-23"];["PUT /form HTTP/1.1"];[2]];
            [["nginx"];["nginx-23"];["GET /cat.jpg HTTP/1.1"];[3]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = 0;
        if (stats.query_phases().size() == 2) {
            phase = 1;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 3);
    }

    Y_UNIT_TEST(NoFullScanAtScanQuery) {
        TVector<std::tuple<TString, TString, ui64, ui32>> testData = {
            /*
             * Predicate : Expected result : shards : ranges per shard
             */
            {
                "Key1 >= 2000 OR Key1 < 100",
                R"([
                    [[0];[0]];
                    [[1];#];
                    [[2];[2]];
                    [[3];#];
                    [[4];[4]];
                    [[2000];[0]];
                    [[2001];#];
                    [[2002];[2]];
                    [[2003];#];
                    [[2004];[4]];
                ])",
                1, 3,
            },
            {
                "(Key1 >= 200003 AND Key1 <= 301003) OR (Key1 > 3 AND Key1 < 1003)",
                R"([
                    [[4];[4]];
                    [[1000];[0]];
                    [[1001];#];
                    [[1002];[2]];
                    [[200003];#];
                    [[200004];[4]];
                    [[201000];[0]];
                    [[201001];#];
                    [[201002];[2]];
                    [[201003];#];
                    [[201004];[4]];
                    [[300000];[0]];
                    [[300001];#];
                    [[300002];[2]];
                    [[300003];#];
                    [[300004];[4]];
                    [[301000];[0]];
                    [[301001];#];
                    [[301002];[2]];
                    [[301003];#];
                ])",
                4, 2,
            },
            {
                R"(
                    (Key1 > 1 AND Key1 < 3) OR
                    (Key1 > 2002 AND Key1 < 2004) OR
                    (Key1 >= 4001 AND Key1 <= 4004)
                )",
                R"([
                    [[2];[2]];
                    [[2003];#];
                    [[4001];#];
                    [[4002];[2]];
                    [[4003];#];
                    [[4004];[4]]
                ])",
                1, 10,
            },
            {
                "Key1 IN (1, 2, 100, 101, 102, 200, 201, 201, 1000, 1001, 1002, 2000, 2001, 2002) AND (Key1 > 2000)",
                R"([
                    [[2001];#];[[2002];[2]]
                ])",
                1, 10,
            }
        };

        for (auto& data: testData) {
            auto query = TString(R"(
                --!syntax_v1
                SELECT * FROM `/Root/TableWithIntKey`
                WHERE <PREDICATE>
                ORDER BY Key1;
            )");

            SubstGlobal(query, "<PREDICATE>", std::get<0>(data));
            RunTestOverIntTable(query, std::get<1>(data), std::get<2>(data), std::get<3>(data));
        }
    }

    Y_UNIT_TEST(NoFullScanAtDNFPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDNF` (
                Key1 Uint32,
                Key2 Uint32,
                Value Uint32,
                PRIMARY KEY (Key1, Key2)
            );
        )").GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/TestDNF` (Key1, Key2, Value) VALUES
                (NULL, NULL, 1),
                (NULL, 100u, 2),
                (NULL, 200u, 3),
                (1u,   NULL, 4),
                (1u,   100u, 5),
                (1u,   200u, 6),
                (1u,   200u, 7),
                (1u,   200u, 8),
                (1u,   300u, 9),
                (1u,   400u, 10),
                (2u,   100u, 11),
                (2u,   200u, 12);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());

        TVector<std::pair<TString, TString>> testData = {
            /*
             * Predicate : Expected result
             */
            {
                "Key1 = 1 AND (Key2 = 100 OR Key2 = 300)",
                "[[[5u]];[[9u]]]"
            },
            {
                "Key1 = 1 AND Key2 IN (100, 300, 400)",
                "[[[5u]];[[9u]];[[10u]]]"
            }
        };

        for (auto& data: testData) {
            auto query = TString(R"(
                --!syntax_v1
                SELECT Value FROM `/Root/TestDNF`
                WHERE <PREDICATE>
                ORDER BY Value;
            )");
            SubstGlobal(query, "<PREDICATE>", data.first);
            ExecuteStreamQueryAndCheck(db, query, data.second);
        }
    }

    Y_UNIT_TEST(ValidatePredicates) {
        /* Table format:
         *   Key1 Uint32,
         *   Key2 Uint32,
         *   Key3 String,
         *   Key4 String,
         *   Value Int32,
         *   PRIMARY KEY (Key1, Key2, Key3, Key4)
         */
        std::vector<TString> testData = {
            "Key1 < 2000",
            "Key1 > 1000",
            "Key1 = 1000",
            "Key1 >= 1000",
            "Key1 < 2000",
            "Key1 <= 2000",
            "Key1 = 1000 AND Key2 > 0",
            "Key1 >= 1000 AND Key2 > 8",
            "Key1 >= 1000 AND Key2 = 8",
            "Key1 >= 8000 AND Key2 >= 8",
            "Key1 < 2000 AND Key2 < 2",
            "Key1 <= 2000 AND Key2 < 2",
            "Key1 <= 2000 AND Key2 <= 2",
            "Key1 > 4000 AND Key2 > 4 AND Key3 > \"resource_3\" AND Key4 > \"uid:11\"",
            "Key1 >= 4000 AND Key2 >= 4 AND Key3 >= \"resource_3\" AND Key4 >= \"uid:11\"",
            "Key1 < 2000 AND Key2 < 2 AND Key3 < \"resource_3\" AND Key4 < \"uid:11\"",
            "Key1 <= 2000 AND Key2 <= 2 AND Key3 <= \"resource_3\" AND Key4 <= \"uid:11\"",
            "Key2 > 8",
            "Key2 < 9",
            "Key2 <= 2 AND Key3 <= \"resource_3\" AND Key4 <= \"uid:11\"",
            "Key1 = 2000 AND Key2 = 2 AND Key3 = \"resource_3\" AND Key4 = \"uid:11\"",
            "Key1 != 2000 AND Key2 != 2 AND Key3 != \"resource_3\" AND Key4 != \"uid:11\"",
            "Key1 IS NULL",
            "Key2 IS NULL",
            "Key1 IS NOT NULL",
            "Key1 > 1000 AND Key2 IS NULL",
            "Key1 > 1000 OR Key2 IS NULL",
            "Key1 >= 1000 OR Key2 IS NOT NULL",
            "Key1 < 9000 OR Key3 IS NOT NULL",
            "Key1 < 9000 OR Key3 IS NULL",
            "Value = 20",
            "(Key1 <= 1000) OR (Key1 > 2000 AND Key1 < 5000) OR (Key1 >= 8000)",
            "Key1 < NULL"
        };

        RunPredicateTest(testData, /* withNulls */ true);
        RunPredicateTest(testData, /* withNulls */ false);
    }

    Y_UNIT_TEST(MergeRanges) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                Key1 Uint32,
                Key2 Uint32,
                Value Uint32,
                PRIMARY KEY (Key1, Key2)
            );
        )").GetValueSync().IsSuccess());

        auto replaceResult = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/TestTable` (Key1, Key2, Value) VALUES
                (1u, 10u, 1),
                (2u, 20u, 2),
                (3u, 30u, 3),
                (4u, 40u, 4),
                (5u, 50u, 5),
                (6u, 60u, 6);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();

        UNIT_ASSERT_C(replaceResult.IsSuccess(), replaceResult.GetIssues().ToString());

        auto query = TString(R"(
            --!syntax_v1
            SELECT Value FROM `/Root/TestTable` WHERE
                Key1 = 1 OR Key1 = 2 OR Key1 = 3
            ORDER BY Value;
        )");

        auto it = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CollectStreamResult(it);

        it = db.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        NJson::TJsonValue plan, readRange;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        // TODO: Need to get real ranges from explain, no anything in JSON
    }

    Y_UNIT_TEST(ValidatePredicatesDataQuery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Uint32,
                    Key2 Uint32,
                    Key3 String,
                    Value Uint32,
                    PRIMARY KEY (Key1, Key2, Key3)
                );
            )").GetValueSync().IsSuccess());

            auto result = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/TestTable` (Key1, Key2, Key3, Value) VALUES
                    (1u, 10u, 'SomeString1', 100),
                    (2u, 20u, 'SomeString2', 200),
                    (3u, 30u, 'SomeString3', 300),
                    (4u, 40u, 'SomeString4', 400),
                    (5u, 50u, 'SomeString5', 500),
                    (6u, 60u, 'SomeString6', 600),
                    (NULL, 70u, 'SomeString7', 700),
                    (8u, NULL, 'SomeString8', 800),
                    (9u, 90u, NULL, 900),
                    (10u, 100u, 'SomeString10', NULL);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        std::vector<TString> predicates = {
            "Key1 < $key_upper_bound",
            "Key1 > $key_lower_bound",
            "Key1 = $key",
            "Key1 = $key AND Key2 > $key_lower_bound",
            "Key1 >= $key_lower_bound AND Key2 > $key_lower_bound",
            "Key1 >= $key_lower_bound AND Key2 = $key",
            "Key1 >= $key_lower_bound AND Key2 >= $key_lower_bound",
            "Key1 < $key_upper_bound AND Key2 < $key_upper_bound",
            "Key1 <= $key_upper_bound AND Key2 < $key_upper_bound",
            "Key1 <= $key_upper_bound AND Key2 <= $key_upper_bound",
            "Key1 > $key_lower_bound AND Key2 > $key_lower_bound AND Key3 > $string_key_lower_bound",
            "Key1 >= $key_lower_bound AND Key2 >= $key_lower_bound AND Key3 >= $string_key_lower_bound",
            "Key2 > $key_lower_bound",
            "Key2 < $key_upper_bound",
            "Key2 <= $key_upper_bound AND Key3 <= $string_key_upper_bound",
            "Key1 IS NULL",
            "Key2 IS NULL",
            "Key1 IS NOT NULL",
            "Key1 > $key_upper_bound AND Key2 IS NULL",
            "Key1 > $key_upper_bound OR Key2 IS NULL",
            "Key1 >= $key_lower_bound OR Key2 IS NOT NULL",
            "Key1 < $key_upper_bound OR Key3 IS NOT NULL",
            "Key1 < $key_upper_bound OR Key3 IS NULL",
            "Value = $key",
            "(Key1 <= $key_upper_bound) OR (Key1 > $key_lower_bound AND Key1 < $key) OR (Key1 >= $key_lower_bound)",
            "Key1 < NULL"
        };

        auto params = TParamsBuilder()
            .AddParam("$key_upper_bound").OptionalUint32(1).Build()
            .AddParam("$key_lower_bound").OptionalUint32(2).Build()
            .AddParam("$key").OptionalUint32(3).Build()
            .AddParam("$string_key_upper_bound").OptionalString("SomeString1").Build()
            .AddParam("$string_key_lower_bound").OptionalString("SomeString2").Build()
            .Build();

        TString useSyntaxV1 = R"(
            --!syntax_v1
        )";

        for (const auto& predicate : predicates) {
            TString query = R"(

                DECLARE $key_upper_bound AS Uint32?;
                DECLARE $key_lower_bound AS Uint32?;
                DECLARE $key AS Uint32?;
                DECLARE $string_key_upper_bound AS String?;
                DECLARE $string_key_lower_bound AS String?;

                SELECT * FROM `/Root/TestTable` WHERE <PREDICATE> ORDER BY `Value`;
            )";

            SubstGlobal(query, "<PREDICATE>", predicate);
            auto expectedResult = session.ExecuteDataQuery(useSyntaxV1 + query, TTxControl::BeginTx().CommitTx(), params)
                .ExtractValueSync();
            UNIT_ASSERT_C(expectedResult.IsSuccess(), expectedResult.GetIssues().ToString());
            const auto expectedYson = FormatResultSetYson(expectedResult.GetResultSet(0));

            auto result = session.ExecuteDataQuery(useSyntaxV1 + query,
                TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(expectedYson, FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CastKeyBounds) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key_from").Uint64(1).Build()
            .AddParam("$key_to").Int32(3).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key_from AS Uint64;
            DECLARE $key_to AS Int32;
            SELECT * FROM `/Root/TwoShard` WHERE Key > $key_from AND Key < $key_to;
        )"), TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[2u];["Two"];[0]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        AssertTableStats(result, "/Root/TwoShard", {
            .ExpectedReads = 1,
        });
    }

    Y_UNIT_TEST(Like) {
        TKikimrSettings kikimrSettings;
        TKikimrRunner kikimr(kikimrSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto res = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                    ('SomeString1', '100'),
                    ('SomeString2', '200'),
                    ('SomeString3', '300'),
                    ('SomeString4', '400'),
                    ('SomeString5', '500'),
                    ('SomeString6', '600'),
                    ('SomeString7', '700'),
                    ('SomeString8', '800');
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto explainAndCheckRange = [&session](const auto& query) {
            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(result.GetPlan(), &plan));

            UNIT_ASSERT_VALUES_EQUAL(plan["tables"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["name"], "/Root/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["reads"].GetArray().size(), 1);
            auto& read = plan["tables"][0]["reads"][0];
            UNIT_ASSERT_VALUES_EQUAL(read["type"], "Scan");
            UNIT_ASSERT_VALUES_EQUAL(read["scan_by"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(read["scan_by"][0], "Key [SomeString, SomeStrinh)");
        };

        const TString useSyntaxV1 = R"(
            --!syntax_v1
        )";

        const TString query = R"(
            SELECT * FROM `/Root/TestTable` WHERE Key like "SomeString%";
        )";

        explainAndCheckRange(query);
        explainAndCheckRange(useSyntaxV1 + query);
    }
}

} // namespace NKqp
} // namespace NKikimr
