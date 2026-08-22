#include "column_type_scenarios.h"
#include "column_type_test_base.h"

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpDecimalColumnShard) {
    namespace {

    template <ui32 Precision, ui32 Scale>
    struct TDecimalTraits {
        using TValue = TString;
        static constexpr const char* ColumnName = "dec";
        static constexpr ui32 DecimalPrecision = Precision;
        static constexpr ui32 DecimalScale = Scale;

        static TKikimrSettings CreateSettings() {
            return TKikimrSettings().SetWithSampleTables(false);
        }

        static auto GetTypeId() { return NScheme::TDecimalType(Precision, Scale); }

        static TString BuildSqlTypeName() {
            return TStringBuilder() << "Decimal(" << Precision << "," << Scale << ")";
        }

        static TString DecCast(const TString& val) {
            return TStringBuilder() << "CAST(\"" << val << "\" AS Decimal(" << Precision << "," << Scale << "))";
        }

        static void AppendYdbValue(TValueBuilder& builder, const std::optional<TString>& val) {
            if (val.has_value()) {
                builder.BeginOptional().Decimal(TDecimalValue(*val, Precision, Scale)).EndOptional();
            } else {
                builder.EmptyOptional(NYdb::TTypeBuilder().Decimal(TDecimalType(Precision, Scale)).Build());
            }
        }

        static void AppendCsvValue(TStringBuilder& builder, const std::optional<TString>& val) {
            if (val.has_value()) {
                builder << *val;
            }
        }

        static void AppendDecimalBytes(arrow::FixedSizeBinaryBuilder& builder, const TString& val) {
            TDecimalValue decimal(val, Precision, Scale);
            char bytes[NScheme::FSB_SIZE] = { 0 };
            for (i32 i = 0; i < 8; ++i) {
                bytes[i] = (decimal.Low_ >> (i << 3)) & 0xFF;
                bytes[i + 8] = (decimal.Hi_ >> (i << 3)) & 0xFF;
            }
            Y_ABORT_UNLESS(builder.Append(bytes).ok());
        }

        static std::shared_ptr<arrow::Array> MakeArrowArray(const TVector<TTypedRow<TString>>& rows) {
            auto type = arrow::fixed_size_binary(NScheme::FSB_SIZE);
            arrow::FixedSizeBinaryBuilder builder(type);
            for (auto&& r : rows) {
                if (r.TypedVal.has_value()) {
                    AppendDecimalBytes(builder, *r.TypedVal);
                } else {
                    Y_ABORT_UNLESS(builder.AppendNull().ok());
                }
            }
            std::shared_ptr<arrow::Array> arr;
            Y_ABORT_UNLESS(builder.Finish(&arr).ok());
            return arr;
        }

        static std::shared_ptr<arrow::DataType> ArrowType() {
            return arrow::fixed_size_binary(NScheme::FSB_SIZE);
        }

        static void LoadPkTable(TTestHelper& helper, ELoadKind load, const TString& tableName, TTestHelper::TColumnTable& table);
    };

    using Decimal229 = TDecimalTraits<22, 9>;
    COLUMN_TYPE_TEST_USING(Decimal229);

    template <>
    void TDecimalTraits<22, 9>::LoadPkTable(TTestHelper& helper, ELoadKind load, const TString& tableName, TTestHelper::TColumnTable& table) {
        if (load == ELoadKind::ARROW) {
            auto type = arrow::fixed_size_binary(NScheme::FSB_SIZE);
            arrow::FixedSizeBinaryBuilder decB(type);
            Decimal229::AppendDecimalBytes(decB, "3.14");
            Decimal229::AppendDecimalBytes(decB, "8.16");
            Decimal229::AppendDecimalBytes(decB, "12.46");
            using namespace NKikimr::NKqp::NTestArrow;
            auto valArr = MakeInt64Array({ (int64_t)1, (int64_t)2, (int64_t)3 });
            std::shared_ptr<arrow::Array> decArr;
            Y_ABORT_UNLESS(decB.Finish(&decArr).ok());
            auto batch = MakeBatch(
                { arrow::field("dec", arrow::fixed_size_binary(NScheme::FSB_SIZE), false),
                  arrow::field("val", arrow::int64()) },
                { decArr, valArr });
            helper.BulkUpsert(table, batch);
        } else if (load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("dec").Decimal(TDecimalValue("3.14", 22, 9)).AddMember("val").Int64(1).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dec").Decimal(TDecimalValue("8.16", 22, 9)).AddMember("val").Int64(2).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dec").Decimal(TDecimalValue("12.46", 22, 9)).AddMember("val").Int64(3).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(tableName, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "3.14,1\n8.16,2\n12.46,3\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(tableName, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }
    }

    TScenario<TString> FilterEqualScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
                { 3, 30, "8.492" },
                { 4, 40, "12.46" },
            },
            {
                { "SELECT id FROM `" + tableName + "` WHERE dec = " + Decimal229::DecCast("3.14"), "[[1]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec != " + Decimal229::DecCast("3.14") + " ORDER BY id", "[[2];[3];[4]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec = " + Decimal229::DecCast("12.46"), "[[4]]" },
            },
        };
    }

    TScenario<TString> FilterNullsScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, std::nullopt },
                { 3, 30, "8.16" },
                { 4, 40, std::nullopt },
                { 5, 50, "12.46" },
            },
            {
                { "SELECT id FROM `" + tableName + "` WHERE dec IS NULL ORDER BY id", "[[2];[4]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec IS NOT NULL ORDER BY id", "[[1];[3];[5]]" },
            },
        };
    }

    TScenario<TString> FilterCompareScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
                { 3, 30, "8.492" },
                { 4, 40, "12.46" },
            },
            {
                { "SELECT id FROM `" + tableName + "` WHERE dec < " + Decimal229::DecCast("12.46") + " ORDER BY id",
                    "[[1];[2];[3]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec > " + Decimal229::DecCast("8.16") + " ORDER BY id",
                    "[[3];[4]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec <= " + Decimal229::DecCast("12.46") + " ORDER BY id",
                    "[[1];[2];[3];[4]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec >= " + Decimal229::DecCast("8.492") + " ORDER BY id",
                    "[[3];[4]]" },
            },
        };
    }

    TScenario<TString> OrderByScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "12.46" },
                { 2, 20, "3.14" },
                { 3, 30, "8.492" },
                { 4, 40, "8.16" },
            },
            {
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY dec",
                    "[[[\"3.14\"];2];[[\"8.16\"];4];[[\"8.492\"];3];[[\"12.46\"];1]]" },
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY dec DESC",
                    "[[[\"12.46\"];1];[[\"8.492\"];3];[[\"8.16\"];4];[[\"3.14\"];2]]" },
            },
        };
    }

    TScenario<TString> GroupByScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
                { 3, 30, "3.14" },
                { 4, 40, "8.16" },
                { 5, 50, "12.46" },
            },
            {
                { "SELECT dec, count(*) AS cnt FROM `" + tableName + "` GROUP BY dec ORDER BY dec",
                    "[[[\"3.14\"];2u];[[\"8.16\"];2u];[[\"12.46\"];1u]]" },
            },
        };
    }

    TScenario<TString> AggregationScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
                { 3, 30, "8.492" },
                { 4, 40, std::nullopt },
                { 5, 50, "12.46" },
            },
            {
                { "SELECT min(dec) FROM `" + tableName + "`", "[[[\"3.14\"]]]" },
                { "SELECT max(dec) FROM `" + tableName + "`", "[[[\"12.46\"]]]" },
                { "SELECT count(dec) FROM `" + tableName + "`", "[[4u]]" },
                { "SELECT count(*) FROM `" + tableName + "`", "[[5u]]" },
                { "SELECT sum(dec) FROM `" + tableName + "`", "[[[\"32.252\"]]]" },
            },
        };
    }

    TJoinScenario<TString> JoinByDecimalScenario() {
        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        return {
            t1,
            t2,
            {
                { 2, 20, "8.16" },
                { 4, 40, "12.46" },
            },
            {
                { 1, 100, "12.46" },
                { 2, 200, "8.16" },
                { 3, 300, "12.46" },
                { 4, 400, "8.16" },
            },
            {
                { "SELECT t1.id, t2.id, t1.dec FROM `" + t1 + "` AS t1 "
                  "JOIN `" + t2 + "` AS t2 ON t1.dec = t2.dec ORDER BY t1.id, t2.id",
                  R"([[2;2;["8.16"]];[2;4;["8.16"]];[4;1;["12.46"]];[4;3;["12.46"]]])" },
            },
        };
    }

    TScenario<TString> OrderByWithLimitScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "12.46" },
                { 2, 20, "3.14" },
                { 3, 30, "8.16" },
                { 4, 40, "8.492" },
                { 5, 50, "100" },
            },
            {
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY dec LIMIT 2",
                    "[[[\"3.14\"];2];[[\"8.16\"];3]]" },
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY dec DESC LIMIT 2",
                    "[[[\"100\"];5];[[\"12.46\"];1]]" },
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY dec LIMIT 2 OFFSET 2",
                    "[[[\"8.492\"];4];[[\"12.46\"];1]]" },
            },
        };
    }

    TScenario<TString> GroupByWithNullsScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { 1, 10, "3.14" },
                { 2, 20, std::nullopt },
                { 3, 30, "3.14" },
                { 4, 40, std::nullopt },
                { 5, 50, "8.16" },
            },
            {
                { "SELECT dec, count(*) AS cnt FROM `" + tableName + "` GROUP BY dec ORDER BY dec",
                    "[[#;2u];[[\"3.14\"];2u];[[\"8.16\"];1u]]" },
                { "SELECT count(dec), count(*) FROM `" + tableName + "`", "[[3u;5u]]" },
            },
        };
    }

    TPkLookupScenario PkLookupScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { "SELECT val FROM `" + tableName + "` WHERE dec = " + Decimal229::DecCast("8.16"), "[[[2]]]" },
                { "SELECT val FROM `" + tableName + "` WHERE dec = " + Decimal229::DecCast("3.14"), "[[[1]]]" },
                { "SELECT val FROM `" + tableName + "` WHERE dec = " + Decimal229::DecCast("12.46"), "[[[3]]]" },
            },
        };
    }

    TCsvScenario CsvScenario() {
        const TString tableName = "/Root/Table1";
        return {
            tableName,
            "1,10,3.14\n2,20,8.16\n3,30,8.492\n",
            {
                { "SELECT id FROM `" + tableName + "` ORDER BY id", "[[1];[2];[3]]" },
                { "SELECT dec, id FROM `" + tableName + "` ORDER BY id",
                    "[[[\"3.14\"];1];[[\"8.16\"];2];[[\"8.492\"];3]]" },
            },
        };
    }

    template <ui32 Precision, ui32 Scale>
    void RunFilterEqualForPrecision(EQueryMode scan, ETableKind table, ELoadKind load) {
        using TTraits = TDecimalTraits<Precision, Scale>;
        const TString tableName = TStringBuilder() << "/Root/ColumnTableTest_" << Precision << "_" << Scale;
        const TString midValue = (Scale == 2) ? "8.49" : "8.492";
        TScenario<TString> scenario{
            tableName,
            {
                { 1, 4, "3.14" },
                { 2, 3, "8.16" },
                { 3, 2, midValue },
                { 4, 1, "12.46" },
            },
            {
                { "SELECT id FROM `" + tableName + "` WHERE dec = " + TTraits::DecCast("3.14"), "[[1]]" },
                { "SELECT id FROM `" + tableName + "` WHERE dec != " + TTraits::DecCast("3.14") + " ORDER BY id",
                    TStringBuilder() << "[[2];[3];[4]]" },
            },
        };
        RunScenario<TTraits>(scenario, scan, table, load);
    }

    }   // namespace

    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TTestHelper helper(Decimal229::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);
        Base::LoadData(helper, Table, Load, tableName, {
            { 1, 4, "3.14" },
            { 2, 3, "8.16" },
            { 3, 2, "8.492" },
            { 4, 1, "12.46" },
        }, &col, &schema);

        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` WHERE id=1", "[[1;[4]]]", Scan);
        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` WHERE id=3", "[[3;[2]]]", Scan);
        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` ORDER BY id",
            "[[1;[4]];[2;[3]];[3;[2]];[4;[1]]]", Scan);
        CheckOrExec(helper, "SELECT dec FROM `" + tableName + "` WHERE id=1", "[[[\"3.14\"]]]", Scan);
        CheckOrExec(helper, "SELECT dec FROM `" + tableName + "` WHERE id=3", "[[[\"8.492\"]]]", Scan);
    }

    Y_UNIT_TEST_SCENARIO(TestFilterEqual, FilterEqualScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterNulls, FilterNullsScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterCompare, FilterCompareScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByDecimal, OrderByScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByDecimal, GroupByScenario);
    Y_UNIT_TEST_SCENARIO(TestAggregation, AggregationScenario);

    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(Decimal229::CreateSettings());
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(22, 9)),
            };
            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);
            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(22, 9)),
            };
            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            Base::CreateDataShardTable(helper, t1);
            Base::CreateDataShardTableWithSecondColumn(helper, t2, "table1_id");
        }

        Base::LoadData(helper, Table, Load, t1, { { 1, 100, "3.14" }, { 2, 200, "8.16" } }, &col1, &s1);
        if (Table == ETableKind::COLUMNSHARD) {
            Base::LoadData(helper, Table, Load, t2,
                { { 1, 1, "8.16" }, { 2, 1, "12.46" }, { 3, 2, "8.16" }, { 4, 2, "12.46" } }, &col2, &s2);
        } else {
            if (Load == ELoadKind::ARROW) {
                Base::BulkUpsertRowTableArrowWithSecondColumn(
                    helper, t2, { { 1, 1, "8.16" }, { 2, 1, "12.46" }, { 3, 2, "8.16" }, { 4, 2, "12.46" } }, "table1_id");
            } else if (Load == ELoadKind::YDB_VALUE) {
                Base::BulkUpsertRowTableYdbValueWithSecondColumn(
                    helper, t2, { { 1, 1, "8.16" }, { 2, 1, "12.46" }, { 3, 2, "8.16" }, { 4, 2, "12.46" } }, "table1_id");
            } else {
                TStringBuilder csv;
                csv << "1,1,8.16\n2,1,12.46\n3,2,8.16\n4,2,12.46\n";
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        CheckOrExec(helper,
            R"(SELECT t1.id, t1.dec, t2.dec FROM `/Root/Table1` AS t1 JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id ORDER BY t1.id, t2.dec)",
            R"([[1;["3.14"];["8.16"]];[1;["3.14"];["12.46"]];[2;["8.16"];["8.16"]];[2;["8.16"];["12.46"]]])", Scan);
    }

    Y_UNIT_TEST_JOIN_SCENARIO(TestJoinByDecimal, JoinByDecimalScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByWithLimit, OrderByWithLimitScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByWithNulls, GroupByWithNullsScenario);
    Y_UNIT_TEST_PK_SCENARIO(TestDecimalAsPrimaryKey, PkLookupScenario);
    Y_UNIT_TEST_CSV_SCENARIO(TestCsv, CsvScenario);

    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto runnerSettings = Decimal229::CreateSettings();
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(runnerSettings);

        const TString ds = "/Root/RowSrc";
        const TString cs = "/Root/ColSrc";

        Base::CreateDataShardTable(helper, ds);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
            TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(22, 9)),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        helper.ExecuteQuery(
            "UPSERT INTO `" + ds + "` (id, int, dec) VALUES "
            "(1, 100, CAST(\"3.14\" AS Decimal(22,9))), "
            "(2, 200, CAST(\"8.16\" AS Decimal(22,9)))");

        if (Load == ELoadKind::ARROW) {
            auto batch = Base::MakeArrowBatch({
                { 1, 100, "3.14" },
                { 2, 200, "8.16" },
            });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            Base::BulkUpsertRowTableYdbValue(helper, cs, {
                { 1, 100, "3.14" },
                { 2, 200, "8.16" },
            });
        } else {
            Base::BulkUpsertRowTableCSV(helper, cs, {
                { 1, 100, "3.14" },
                { 2, 200, "8.16" },
            });
        }

        CheckOrExec(helper, "SELECT id, int, dec FROM `" + ds + "` ORDER BY id",
            "[[1;[100];[\"3.14\"]];[2;[200];[\"8.16\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dec FROM `" + cs + "` ORDER BY id",
            "[[1;[100];[\"3.14\"]];[2;[200];[\"8.16\"]]]", Scan);

        helper.ExecuteQuery(
            "UPSERT INTO `" + ds + "` (id, int, dec) VALUES "
            "(3, 300, CAST(\"12.46\" AS Decimal(22,9))), "
            "(1, 110, CAST(\"10.1\" AS Decimal(22,9)))");
        helper.ExecuteQuery(
            "UPSERT INTO `" + cs + "` (id, int, dec) VALUES "
            "(3, 300, CAST(\"12.46\" AS Decimal(22,9))), "
            "(1, 110, CAST(\"10.1\" AS Decimal(22,9)))");

        CheckOrExec(helper, "SELECT id, int, dec FROM `" + ds + "` ORDER BY id",
            "[[1;[110];[\"10.1\"]];[2;[200];[\"8.16\"]];[3;[300];[\"12.46\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dec FROM `" + cs + "` ORDER BY id",
            "[[1;[110];[\"10.1\"]];[2;[200];[\"8.16\"]];[3;[300];[\"12.46\"]]]", Scan);

        helper.ExecuteQuery("DELETE FROM `" + ds + "` WHERE id = 2");
        helper.ExecuteQuery("DELETE FROM `" + cs + "` WHERE id = 2");
        CheckOrExec(helper, "SELECT id FROM `" + ds + "` ORDER BY id", "[[1];[3]]", Scan);
        CheckOrExec(helper, "SELECT id FROM `" + cs + "` ORDER BY id", "[[1];[3]]", Scan);
    }

    Y_UNIT_TEST(TestPMInfDecimal, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(Decimal229::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);

        if (Load == ELoadKind::CSV) {
            Base::LoadData(helper, Table, ELoadKind::CSV, tableName, {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
            }, &col, &schema);
            Base::LoadData(helper, Table, ELoadKind::YDB_VALUE, tableName, {
                { 3, 30, "inf" },
                { 4, 40, "-inf" },
            }, &col, &schema);
        } else {
            Base::LoadData(helper, Table, Load, tableName, {
                { 1, 10, "3.14" },
                { 2, 20, "8.16" },
                { 3, 30, "inf" },
                { 4, 40, "-inf" },
            }, &col, &schema);
        }

        CheckOrExec(helper, "SELECT max(dec) FROM `" + tableName + "`", "[[[\"inf\"]]]", Scan);
        CheckOrExec(helper, "SELECT min(dec) FROM `" + tableName + "`", "[[[\"-inf\"]]]", Scan);
    }

    Y_UNIT_TEST(TestFilterEqualMultiplePrecisions, EQueryMode, ETableKind, ELoadKind) {
        RunFilterEqualForPrecision<22, 9>(Arg<0>(), Arg<1>(), Arg<2>());
        RunFilterEqualForPrecision<35, 10>(Arg<0>(), Arg<1>(), Arg<2>());
        RunFilterEqualForPrecision<12, 2>(Arg<0>(), Arg<1>(), Arg<2>());
    }
}

}   // namespace NKqp
}   // namespace NKikimr
