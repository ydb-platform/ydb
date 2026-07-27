#include "column_type_scenarios.h"
#include "column_type_test_base.h"

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpIntervalColumnShard) {
    namespace {

    struct TIntervalTraits {
        using TValue = i64;
        static constexpr const char* ColumnName = "ival";
        static constexpr const char* SqlTypeName = "Interval";
        static TKikimrSettings CreateSettings() {
            return CreateColumnshardSettings([](auto& f) { f.SetEnableColumnshardInterval(true); });
        }
        static auto GetTypeId() { return NScheme::NTypeIds::Interval; }
        static void AppendYdbValue(TValueBuilder& builder, const std::optional<i64>& val) {
            if (val.has_value()) {
                builder.BeginOptional().Interval(*val).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Interval);
            }
        }
        static void AppendCsvValue(TStringBuilder& builder, const std::optional<i64>& val) {
            if (val.has_value()) {
                builder << *val;
            }
        }
        static std::shared_ptr<arrow::Array> MakeArrowArray(const TVector<TTypedRow<i64>>& rows) {
            using namespace NKikimr::NKqp::NTestArrow;
            std::vector<std::optional<int64_t>> vals;
            vals.reserve(rows.size());
            for (auto&& r : rows) {
                vals.push_back(r.TypedVal);
            }
            return MakeInt64ArrayNullable(vals);
        }
        static std::shared_ptr<arrow::DataType> ArrowType() { return arrow::int64(); }
    };

    COLUMN_TYPE_TEST_USING(TIntervalTraits);

    constexpr i64 MaxValidInterval = (i64)(86400000000ULL * 49673U - 1);
    constexpr i64 MinValidInterval = -MaxValidInterval;

    void CreateIntervalPkDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    ival Interval NOT NULL,
                    val Int64,
                    PRIMARY KEY (ival)
                );
            )")
                       .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
    }

    void PrepareIntervalPkTable(TTestHelper& helper, ETableKind tableKind, const TString& tableName,
        TTestHelper::TColumnTable* colTableOut, TVector<TTestHelper::TColumnSchema>* schemaOut) {
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64),
        };

        *schemaOut = schema;
        if (tableKind == ETableKind::COLUMNSHARD) {
            TTestHelper::TColumnTable col;
            col.SetName(tableName).SetPrimaryKey({ "ival" }).SetSharding({ "ival" }).SetSchema(schema);
            helper.CreateTable(col);
            *colTableOut = col;
        } else {
            CreateIntervalPkDataShardTable(helper, tableName);
        }
    }

    void LoadIntervalPkData(TTestHelper& helper, ETableKind table, ELoadKind load, const TString& name,
        TTestHelper::TColumnTable* col) {
        if (load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto ivalArr = MakeInt64Array({ -7000000, -3000000, 0, 4000000, 9000000 });
            auto valArr = MakeInt64ArrayNullable({ (i64)1, (i64)2, std::nullopt, (i64)4, (i64)5 });
            auto batch = MakeBatch(
                { arrow::field("ival", arrow::int64(), false),
                  arrow::field("val", arrow::int64()) },
                { ivalArr, valArr });
            if (table == ETableKind::COLUMNSHARD) {
                Y_ABORT_UNLESS(col);
                helper.BulkUpsert(*col, batch);
            } else {
                TString strBatch = NArrow::SerializeBatchNoCompression(batch);
                TString strSchema = NArrow::SerializeSchema(*batch->schema());
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        } else if (load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(-7000000).AddMember("val").BeginOptional().Int64(1).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(-3000000).AddMember("val").BeginOptional().Int64(2).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(0).AddMember("val").EmptyOptional(EPrimitiveType::Int64).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(4000000).AddMember("val").BeginOptional().Int64(4).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(9000000).AddMember("val").BeginOptional().Int64(5).EndOptional().EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "-7000000,1\n-3000000,2\n0,\n4000000,4\n9000000,5\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }
    }

    TScenario<i64> FilterEqualScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)1000000 }, { 2, 20, (i64)2000000 }, { 3, 30, (i64)1000000 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)-500000 } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE ival = CAST(1000000 AS Interval) ORDER BY id", "[[1;[10];[1000000]];[3;[30];[1000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival = CAST(2000000 AS Interval) ORDER BY id", "[[2;[20];[2000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival = CAST(-500000 AS Interval) ORDER BY id", "[[5;[50];[-500000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival = CAST(999 AS Interval)", "[]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival != CAST(1000000 AS Interval) ORDER BY id", "[[2;[20];[2000000]];[5;[50];[-500000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival != CAST(-500000 AS Interval) ORDER BY id", "[[1;[10];[1000000]];[2;[20];[2000000]];[3;[30];[1000000]]]" },
            },
        };
    }

    TScenario<i64> FilterNullsScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)100 }, { 2, 20, std::nullopt }, { 3, 30, (i64)300 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)0 }, { 6, 60, std::nullopt } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE ival IS NULL ORDER BY id", "[[2;[20];#];[4;[40];#];[6;[60];#]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival IS NOT NULL ORDER BY id", "[[1;[10];[100]];[3;[30];[300]];[5;[50];[0]]]" },
                { "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NULL", "[[3u]]" },
                { "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NOT NULL", "[[3u]]" },
            },
        };
    }

    TScenario<i64> FilterCompareScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)-3000000 }, { 2, 20, (i64)-1000000 }, { 3, 30, (i64)0 },
              { 4, 40, (i64)1000000 }, { 5, 50, (i64)5000000 }, { 6, 60, std::nullopt } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE ival < CAST(0 AS Interval) ORDER BY id", "[[1;[10];[-3000000]];[2;[20];[-1000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival > CAST(0 AS Interval) ORDER BY id", "[[4;[40];[1000000]];[5;[50];[5000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival <= CAST(0 AS Interval) ORDER BY id", "[[1;[10];[-3000000]];[2;[20];[-1000000]];[3;[30];[0]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival >= CAST(0 AS Interval) ORDER BY id", "[[3;[30];[0]];[4;[40];[1000000]];[5;[50];[5000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival >= CAST(-1000000 AS Interval) AND ival <= CAST(1000000 AS Interval) ORDER BY id",
                    "[[2;[20];[-1000000]];[3;[30];[0]];[4;[40];[1000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival > CAST(-1000000 AS Interval) AND ival < CAST(5000000 AS Interval) ORDER BY id",
                    "[[3;[30];[0]];[4;[40];[1000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival < CAST(-1000000 AS Interval) ORDER BY id", "[[1;[10];[-3000000]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE ival < CAST(99999999 AS Interval) ORDER BY id",
                    "[[1;[10];[-3000000]];[2;[20];[-1000000]];[3;[30];[0]];[4;[40];[1000000]];[5;[50];[5000000]]]" },
            },
        };
    }

    TScenario<i64> OrderByScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)300 }, { 2, 20, (i64)-100 }, { 3, 30, (i64)200 },
              { 4, 40, (i64)-200 }, { 5, 50, (i64)0 } },
            {
                { "SELECT * FROM `/Root/Table1` ORDER BY ival", "[[4;[40];[-200]];[2;[20];[-100]];[5;[50];[0]];[3;[30];[200]];[1;[10];[300]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival DESC", "[[1;[10];[300]];[3;[30];[200]];[5;[50];[0]];[2;[20];[-100]];[4;[40];[-200]]]" },
            },
            {
                {
                    { { 6, 60, (i64)200 } },
                    {
                        { "SELECT * FROM `/Root/Table1` ORDER BY ival, id",
                            "[[4;[40];[-200]];[2;[20];[-100]];[5;[50];[0]];[3;[30];[200]];[6;[60];[200]];[1;[10];[300]]]" },
                        { "SELECT * FROM `/Root/Table1` ORDER BY ival DESC, id DESC",
                            "[[1;[10];[300]];[6;[60];[200]];[3;[30];[200]];[5;[50];[0]];[2;[20];[-100]];[4;[40];[-200]]]" },
                    },
                },
            },
        };
    }

    TScenario<i64> GroupByScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)100 }, { 2, 20, (i64)200 }, { 3, 30, (i64)100 },
              { 4, 40, (i64)200 }, { 5, 50, (i64)300 }, { 6, 60, (i64)100 },
              { 7, 70, (i64)-100 } },
            {
                { "SELECT ival, count(*) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
                    "[[[-100];1u];[[100];3u];[[200];2u];[[300];1u]]" },
                { "SELECT ival, count(*) AS cnt, min(id) AS min_id FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
                    "[[[-100];1u;7];[[100];3u;1];[[200];2u;2];[[300];1u;5]]" },
            },
        };
    }

    TScenario<i64> AggregationScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)-500 }, { 2, 20, (i64)100 }, { 3, 30, (i64)999 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)0 } },
            {
                { "SELECT min(ival) FROM `/Root/Table1`", "[[[-500]]]" },
                { "SELECT max(ival) FROM `/Root/Table1`", "[[[999]]]" },
                { "SELECT count(ival) FROM `/Root/Table1`", "[[4u]]" },
                { "SELECT count(*) FROM `/Root/Table1`", "[[5u]]" },
                { "SELECT min(ival), max(ival), count(ival), count(*) FROM `/Root/Table1`", "[[[-500];[999];4u;5u]]" },
            },
        };
    }

    TScenario<i64> AggregationAllNullScenario() {
        return {
            "/Root/Table2",
            { { 1, 0, std::nullopt }, { 2, 0, std::nullopt } },
            {
                { "SELECT min(ival), max(ival), count(ival), count(*) FROM `/Root/Table2`", "[[#;#;0u;2u]]" },
            },
        };
    }

    TJoinScenario<i64> JoinByIntervalScenario() {
        return {
            "/Root/JoinTable1",
            "/Root/JoinTable2",
            { { 1, 10, (i64)1000 }, { 2, 20, (i64)2000 }, { 3, 30, (i64)3000 } },
            { { 10, 100, (i64)1000 }, { 20, 200, (i64)2000 }, { 30, 300, (i64)4000 } },
            {
                { "SELECT t1.id, t2.id, t1.ival FROM `/Root/JoinTable1` AS t1 "
                  "JOIN `/Root/JoinTable2` AS t2 ON t1.ival = t2.ival "
                  "ORDER BY t1.id, t2.id",
                  "[[1;10;[1000]];[2;20;[2000]]]" },
            },
        };
    }

    TScenario<i64> OrderByWithLimitScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)500 }, { 2, 20, (i64)-200 }, { 3, 30, (i64)100 },
              { 4, 40, (i64)-300 }, { 5, 50, (i64)1000 }, { 6, 60, (i64)0 } },
            {
                { "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 1", "[[4;[40];[-300]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival DESC LIMIT 1", "[[5;[50];[1000]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 3", "[[4;[40];[-300]];[2;[20];[-200]];[6;[60];[0]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival DESC LIMIT 3", "[[5;[50];[1000]];[1;[10];[500]];[3;[30];[100]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 2 OFFSET 2", "[[6;[60];[0]];[3;[30];[100]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 100",
                    "[[4;[40];[-300]];[2;[20];[-200]];[6;[60];[0]];[3;[30];[100]];[1;[10];[500]];[5;[50];[1000]]]" },
                { "SELECT * FROM `/Root/Table1` ORDER BY ival, id LIMIT 2", "[[4;[40];[-300]];[2;[20];[-200]]]" },
            },
        };
    }

    TScenario<i64> GroupByWithNullsScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, (i64)100 }, { 2, 20, (i64)200 }, { 3, 30, std::nullopt },
              { 4, 40, (i64)100 }, { 5, 50, std::nullopt }, { 6, 60, (i64)200 },
              { 7, 70, std::nullopt }, { 8, 80, (i64)-100 } },
            {
                { "SELECT ival, count(*) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
                    "[[#;3u];[[-100];1u];[[100];2u];[[200];2u]]" },
                { "SELECT ival, count(ival) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
                    "[[#;0u];[[-100];1u];[[100];2u];[[200];2u]]" },
                { "SELECT ival, count(*) AS cnt FROM `/Root/Table1` WHERE ival IS NOT NULL GROUP BY ival ORDER BY ival",
                    "[[[-100];1u];[[100];2u];[[200];2u]]" },
                { "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NULL", "[[3u]]" },
                { "SELECT min(ival), max(ival) FROM `/Root/Table1`", "[[[-100];[200]]]" },
            },
        };
    }

    }   // namespace

    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();


        const TString tableName = "/Root/Table1";
        TTestHelper helper(TIntervalTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);
        Base::LoadData(helper, Table, Load, tableName,
            { { 1, 10, (i64)1000000 }, { 2, 20, (i64)-500000 }, { 3, 30, std::nullopt }, { 4, 40, (i64)0 },
              { 5, 50, MaxValidInterval }, { 6, 60, MinValidInterval } },
            &col, &schema);

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=1", "[[1;[10];[1000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=2", "[[2;[20];[-500000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=3", "[[3;[30];#]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=4", "[[4;[40];[0]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=5", TStringBuilder() << "[[5;[50];[" << MaxValidInterval << "]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=6", TStringBuilder() << "[[6;[60];[" << MinValidInterval << "]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY id",
            TStringBuilder() << "[[1;[10];[1000000]];[2;[20];[-500000]];[3;[30];#];[4;[40];[0]];[5;[50];[" << MaxValidInterval
                           << "]];[6;[60];[" << MinValidInterval << "]]]", Scan);

        CheckOrExec(helper, "SELECT ival FROM `/Root/Table1` WHERE id=1", "[[[1000000]]]", Scan);
        CheckOrExec(helper, "SELECT ival FROM `/Root/Table1` WHERE id=3", "[[#]]", Scan);

        Base::LoadData(helper, Table, Load, tableName, { { 7, 0, std::nullopt } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=7", "[[7;[0];#]]", Scan);
    }

    Y_UNIT_TEST_SCENARIO(TestFilterEqual, FilterEqualScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterNulls, FilterNullsScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterCompare, FilterCompareScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByInterval, OrderByScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByInterval, GroupByScenario);

    Y_UNIT_TEST(TestAggregation, EQueryMode, ETableKind, ELoadKind) {
        RunScenario<TIntervalTraits>(AggregationScenario(), Arg<0>(), Arg<1>(), Arg<2>());
        RunScenario<TIntervalTraits>(AggregationAllNullScenario(), Arg<0>(), Arg<1>(), Arg<2>());
    }

    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();


        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(TIntervalTraits::CreateSettings());
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
            };

            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);

            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int32),
                TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
            };

            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            Base::CreateDataShardTable(helper, t1);
            auto& session = helper.GetSession();
            auto res = session
                           .ExecuteSchemeQuery(TStringBuilder() << R"(
                    CREATE TABLE `)" << t2 << R"(` (
                        id Int32 NOT NULL,
                        table1_id Int32,
                        ival Interval,
                        PRIMARY KEY (id)
                    );
                )")
                           .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        Base::LoadData(helper, Table, Load, t1,
            { { 1, 10, (i64)1000000 }, { 2, 20, (i64)-500000 }, { 3, 30, std::nullopt } },
            &col1, &s1);

        if (Table == ETableKind::COLUMNSHARD) {
            using namespace NKikimr::NKqp::NTestArrow;
            if (Load == ELoadKind::ARROW) {
                auto idArr = MakeInt32Array({1, 2, 3, 4});
                auto t1idArr = MakeInt32Array({1, 1, 2, 3});
                auto ivalArr = MakeInt64ArrayNullable({(i64)2000000, (i64)3000000, (i64)-100000, std::nullopt});
                auto batch = MakeBatch(
                    { arrow::field("id", arrow::int32(), false),
                      arrow::field("table1_id", arrow::int32()),
                      arrow::field("ival", arrow::int64()) },
                    { idArr, t1idArr, ivalArr });
                helper.BulkUpsert(col2, batch);
            } else if (Load == ELoadKind::YDB_VALUE) {
                TValueBuilder builder;
                builder.BeginList();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("ival").BeginOptional().Interval(2000000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("ival").BeginOptional().Interval(3000000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("table1_id").BeginOptional().Int32(2).EndOptional().AddMember("ival").BeginOptional().Interval(-100000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(4).AddMember("table1_id").BeginOptional().Int32(3).EndOptional().AddMember("ival").EmptyOptional(EPrimitiveType::Interval).EndStruct();
                builder.EndList();
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, builder.Build()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                TStringBuilder csv;
                csv << "1,1,2000000\n2,1,3000000\n3,2,-100000\n4,3,\n";
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        } else {
            if (Load == ELoadKind::ARROW) {
                using namespace NKikimr::NKqp::NTestArrow;
                auto idArr = MakeInt32Array({1, 2, 3, 4});
                auto t1idArr = MakeInt32Array({1, 1, 2, 3});
                auto ivalArr = MakeInt64ArrayNullable({(i64)2000000, (i64)3000000, (i64)-100000, std::nullopt});
                auto batch = MakeBatch(
                    { arrow::field("id", arrow::int32(), false),
                      arrow::field("table1_id", arrow::int32()),
                      arrow::field("ival", arrow::int64()) },
                    { idArr, t1idArr, ivalArr });

                TString strBatch = NArrow::SerializeBatchNoCompression(batch);
                TString strSchema = NArrow::SerializeSchema(*batch->schema());
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else if (Load == ELoadKind::YDB_VALUE) {
                TValueBuilder builder;
                builder.BeginList();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("ival").BeginOptional().Interval(2000000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("ival").BeginOptional().Interval(3000000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("table1_id").BeginOptional().Int32(2).EndOptional().AddMember("ival").BeginOptional().Interval(-100000).EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(4).AddMember("table1_id").BeginOptional().Int32(3).EndOptional().AddMember("ival").EmptyOptional(EPrimitiveType::Interval).EndStruct();
                builder.EndList();
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, builder.Build()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                TStringBuilder csv;
                csv << "1,1,2000000\n2,1,3000000\n3,2,-100000\n4,3,\n";
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        CheckOrExec(helper,
            "SELECT t1.id, t1.ival, t2.ival FROM `/Root/Table1` AS t1 "
            "JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id "
            "ORDER BY t1.id, t2.id",
            "[[1;[1000000];[2000000]];[1;[1000000];[3000000]];[2;[-500000];[-100000]];[3;#;#]]", Scan);

        CheckOrExec(helper,
            "SELECT t1.id, t1.ival, t2.ival FROM `/Root/Table1` AS t1 "
            "JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id "
            "WHERE t1.ival IS NOT NULL "
            "ORDER BY t1.id, t2.id",
            "[[1;[1000000];[2000000]];[1;[1000000];[3000000]];[2;[-500000];[-100000]]]", Scan);
    }

    Y_UNIT_TEST_JOIN_SCENARIO(TestJoinByInterval, JoinByIntervalScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByWithLimit, OrderByWithLimitScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByWithNulls, GroupByWithNullsScenario);

    Y_UNIT_TEST(TestIntervalAsPrimaryKey, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TIntervalTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareIntervalPkTable(helper, Table, tableName, &col, &schema);
        LoadIntervalPkData(helper, Table, Load, tableName, &col);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival = CAST(0 AS Interval)", "[[0;#]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival = CAST(4000000 AS Interval)", "[[4000000;[4]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival = CAST(-7000000 AS Interval)", "[[-7000000;[1]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival < CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival > CAST(0 AS Interval)",
            "[[4000000;[4]];[9000000;[5]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival <= CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]];[0;#]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival >= CAST(0 AS Interval)",
            "[[0;#];[4000000;[4]];[9000000;[5]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival != CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]];[4000000;[4]];[9000000;[5]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival > CAST(-3000000 AS Interval) AND ival < CAST(9000000 AS Interval)",
            "[[0;#];[4000000;[4]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `" + tableName + "` WHERE ival = CAST(999 AS Interval)", "[]", Scan);
    }

    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto runnerSettings = TIntervalTraits::CreateSettings();
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(runnerSettings);

        const TString ds = "/Root/RowSrc";
        const TString cs = "/Root/ColSrc";

        Base::CreateDataShardTable(helper, ds);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
            TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, ival) VALUES (1, 100, CAST(1000000 AS Interval)), (2, 200, CAST(-500000 AS Interval))");

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto idArr = MakeInt32Array({1, 2});
            auto intArr = MakeInt64ArrayNullable({(i64)100, (i64)200});
            auto ivalArr = MakeInt64ArrayNullable({(i64)1000000, (i64)-500000});
            auto batch = MakeBatch(
                { arrow::field("id", arrow::int32(), false),
                  arrow::field("int", arrow::int64()),
                  arrow::field("ival", arrow::int64()) },
                { idArr, intArr, ivalArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("int").BeginOptional().Int64(100).EndOptional().AddMember("ival").BeginOptional().Interval(1000000).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("int").BeginOptional().Int64(200).EndOptional().AddMember("ival").BeginOptional().Interval(-500000).EndOptional().EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,100,1000000\n2,200,-500000\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[100];[1000000]];[2;[200];[-500000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[100];[1000000]];[2;[200];[-500000]]]", Scan);

        helper.ExecuteQuery("UPDATE `" + ds + "` SET int = int + 1 WHERE ival = CAST(1000000 AS Interval)");
        helper.ExecuteQuery("UPDATE `" + cs + "` SET int = int + 1 WHERE ival = CAST(1000000 AS Interval)");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]]]", Scan);

        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, ival) VALUES (3, 300, CAST(3000000 AS Interval))");
        helper.ExecuteQuery("UPSERT INTO `" + cs + "` (id, int, ival) VALUES (3, 300, CAST(3000000 AS Interval))");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]];[3;[300];[3000000]]]", Scan);

        helper.ExecuteQuery("REPLACE INTO `" + ds + "` (id, int, ival) VALUES (2, 250, CAST(-250000 AS Interval))");
        helper.ExecuteQuery("REPLACE INTO `" + cs + "` (id, int, ival) VALUES (2, 250, CAST(-250000 AS Interval))");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[250];[-250000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[250];[-250000]];[3;[300];[3000000]]]", Scan);

        helper.ExecuteQuery("DELETE FROM `" + ds + "` WHERE ival = CAST(-250000 AS Interval)");
        helper.ExecuteQuery("DELETE FROM `" + cs + "` WHERE ival = CAST(-250000 AS Interval)");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);

        const TString csFromDs = "/Root/CSFromDS";
        TVector<TTestHelper::TColumnSchema> schema2 = schema;
        TTestHelper::TColumnTable col2;
        col2.SetName(csFromDs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema2);
        helper.CreateTable(col2);
        helper.ExecuteQuery(
            "UPSERT INTO `" + csFromDs + "` (id, int, ival) "
            "SELECT id, int, ival FROM `" + ds + "`");
        CheckOrExec(helper, "SELECT * FROM `" + csFromDs + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);

        const TString dsFromCs = "/Root/DSFromCS";
        helper.ExecuteQuery("CREATE TABLE `" + dsFromCs + "` (id Int32 NOT NULL, int Int64, ival Interval, PRIMARY KEY (id))");
        helper.ExecuteQuery(
            "UPSERT INTO `" + dsFromCs + "` (id, int, ival) "
            "SELECT id, int, ival FROM `" + cs + "`");
        CheckOrExec(helper, "SELECT * FROM `" + dsFromCs + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);
    }

    Y_UNIT_TEST(TestCsv, EQueryMode, ETableKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();

        TTestHelper helper(TIntervalTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, "/Root/Table1", &col, &schema);

        {
            TStringBuilder builder;
            builder << "1,10,1000000" << Endl;
            builder << "2,20,-500000" << Endl;
            builder << "3,30,0" << Endl;
            auto result = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/Table1", EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY id",
            "[[1;[10];[1000000]];[2;[20];[-500000]];[3;[30];[0]]]", Scan);

        {
            TStringBuilder builder;
            builder << "4,40," << MaxValidInterval << Endl;
            builder << "5,50," << MinValidInterval << Endl;
            auto result = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/Table1", EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id >= 4 ORDER BY id",
            TStringBuilder() << "[[4;[40];[" << MaxValidInterval << "]];[5;[50];[" << MinValidInterval << "]]]", Scan);

        {
            TStringBuilder builder;
            builder << "1,10,9999" << Endl;
            auto result = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/Table1", EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id = 1",
            "[[1;[10];[9999]]]", Scan);
    }
}

} // namespace NKqp
} // namespace NKikimr
