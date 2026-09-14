#include "column_type_scenarios.h"
#include "column_type_test_base.h"

#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/uuid/uuid.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpBoolColumnShard) {
    namespace {

    struct TBoolTraits {
        using TValue = bool;
        static constexpr const char* ColumnName = "b";
        static constexpr const char* SqlTypeName = "Bool";

        static TKikimrSettings CreateSettings() {
            return CreateColumnshardSettings([](auto& f) { f.SetEnableColumnshardBool(true); });
        }

        static auto GetTypeId() { return NScheme::NTypeIds::Bool; }

        static void AppendYdbValue(TValueBuilder& builder, const std::optional<bool>& val) {
            if (val.has_value()) {
                builder.BeginOptional().Bool(*val).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Bool);
            }
        }

        static void AppendCsvValue(TStringBuilder& builder, const std::optional<bool>& val) {
            if (val.has_value()) {
                builder << (*val ? "true" : "false");
            }
        }

        static std::shared_ptr<arrow::Array> MakeArrowArray(const TVector<TTypedRow<bool>>& rows) {
            using namespace NKikimr::NKqp::NTestArrow;
            std::vector<std::optional<bool>> bs;
            bs.reserve(rows.size());
            for (auto&& r : rows) {
                bs.push_back(r.TypedVal);
            }

            return MakeBoolArrayAsUInt8Nullable(bs);
        }

        static std::shared_ptr<arrow::DataType> ArrowType() { return arrow::uint8(); }
    };

    COLUMN_TYPE_TEST_USING(TBoolTraits);

    void BulkUpsertRowTableCSVWithFormat(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& trueValue, const TString& falseValue) {
        TStringBuilder builder;
        for (auto&& r : rows) {
            builder << r.Id << "," << r.IntVal << ",";
            if (r.TypedVal.has_value()) {
                builder << (*r.TypedVal ? trueValue : falseValue);
            }

            builder << '\n';
        }

        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    bool TryPrepareBase(TTestHelper& helper, ETableKind tableKind, const TString& tableName, TTestHelper::TColumnTable* colTableOut,
        TVector<TTestHelper::TColumnSchema>* schemaOut) {
        if (tableKind == ETableKind::COLUMNSHARD) {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            *schemaOut = schema;
            TTestHelper::TColumnTable col;
            col.SetName(tableName).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
            auto result = helper.GetSession().ExecuteSchemeQuery(col.BuildQuery()).GetValueSync();
            if (result.GetStatus() == NYdb::EStatus::SUCCESS) {
                *colTableOut = col;
                return true;
            } else {
                return false;
            }
        } else {
            Base::CreateDataShardTable(helper, tableName);
            return true;
        }
    }

    TScenario<bool> FilterEqualScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE b == true", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE b != true order by id", "[[[%false];2;[3]]]" },
            },
        };
    }

    TScenario<bool> FilterNullsScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true }, { 5, 5, std::nullopt }, { 6, 6, std::nullopt } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE b is NULL order by id", "[[#;5;[5]];[#;6;[6]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE b is not NULL order by id",
                  "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]" },
            },
        };
    }

    TScenario<bool> FilterCompareScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } },
            {
                { "SELECT * FROM `/Root/Table1` WHERE b < true order by id", "[[[%false];2;[3]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE b > false order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE b <= true order by id",
                  "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]" },
                { "SELECT * FROM `/Root/Table1` WHERE b >= true order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]" },
            },
        };
    }

    TScenario<bool> OrderByScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } },
            {
                { "SELECT * FROM `/Root/Table1` order by b, id",
                  "[[[%false];2;[3]];[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]" },
            },
        };
    }

    TScenario<bool> GroupByScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true }, { 5, 12, true }, { 6, 30, false } },
            {
                { "SELECT b, count(*) FROM `/Root/Table1` group by b order by b", "[[[%false];2u];[[%true];4u]]" },
            },
        };
    }

    TScenario<bool> AggregationScenario() {
        return {
            "/Root/Table1",
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } },
            {
                { "SELECT min(b) FROM `/Root/Table1`", "[[[%false]]]" },
                { "SELECT max(b) FROM `/Root/Table1`", "[[[%true]]]" },
            },
        };
    }

    TJoinScenario<bool> JoinByBoolScenario() {
        return {
            "/Root/Table1",
            "/Root/Table2",
            { { 2, 3, true }, { 4, 1, true } },
            { { 2, 2, false }, { 4, 4, false }, { 1, 1, true }, { 3, 3, true } },
            {
                { "SELECT t1.id, t2.id, t1.b FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.b = t2.b order by t1.id, t2.id, t1.b",
                  R"([[2;1;[%true]];[2;3;[%true]];[4;1;[%true]];[4;3;[%true]]])" },
            },
        };
    }

    }   // namespace

    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TBoolTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);
        Base::LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 4, 1, true }, { 3, 2, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=1", "[[[%true];1;[4]]]", Scan);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` order by id", "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_SCENARIO(TestFilterEqual, FilterEqualScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterNulls, FilterNullsScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterCompare, FilterCompareScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByBool, OrderByScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByBool, GroupByScenario);
    Y_UNIT_TEST_SCENARIO(TestAggregation, AggregationScenario);

    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(TBoolTraits::CreateSettings());
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);
            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            Base::CreateDataShardTable(helper, t1);
            Base::CreateDataShardTableWithSecondColumn(helper, t2, "table1_id");
        }

        Base::LoadData(helper, Table, Load, t1, { { 1, 4, true }, { 2, 3, true } }, &col1, &s1);
        if (Table == ETableKind::COLUMNSHARD) {
            Base::LoadData(helper, Table, Load, t2, { { 1, 1, true }, { 2, 1, false }, { 3, 2, true }, { 4, 2, false } }, &col2, &s2);
        } else {
            if (Load == ELoadKind::ARROW) {
                Base::BulkUpsertRowTableArrowWithSecondColumn(
                    helper, t2, { { 1, 1, true }, { 2, 1, false }, { 3, 2, true }, { 4, 2, false } }, "table1_id");
            } else if (Load == ELoadKind::YDB_VALUE) {
                Base::BulkUpsertRowTableYdbValueWithSecondColumn(
                    helper, t2, { { 1, 1, true }, { 2, 1, false }, { 3, 2, true }, { 4, 2, false } }, "table1_id");
            } else {
                TStringBuilder csv;
                csv << "1,1,true\n2,1,false\n3,2,true\n4,2,false\n";
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        CheckOrExec(helper,
            "SELECT t1.id, t1.b, t2.b FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.id = t2.table1_id order by t1.id, t1.b, t2.b",
            R"([[1;[%true];[%false]];[1;[%true];[%true]];[2;[%true];[%false]];[2;[%true];[%true]]])", Scan);
    }

    Y_UNIT_TEST_JOIN_SCENARIO(TestJoinByBool, JoinByBoolScenario);

    Y_UNIT_TEST(TestCSVBoolFormats, EQueryMode, ETableKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TBoolTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);

        struct TCSVFormat {
            TString TrueValue;
            TString FalseValue;
            TString Description;
        };

        TVector<TCSVFormat> formats = {
            { "true", "false", "true/false" },
            { "1", "0", "1/0" },
        };

        for (auto&& format : formats) {
            helper.ExecuteQuery("DELETE FROM `" + tableName + "`");
            TVector<TRow> rows = { { 1, 100, true }, { 2, 200, false }, { 3, 300, true }, { 4, 400, false } };

            BulkUpsertRowTableCSVWithFormat(helper, tableName, rows, format.TrueValue, format.FalseValue);

            CheckOrExec(helper, "SELECT id, int, b FROM `" + tableName + "` ORDER BY id",
                "[[1;[100];[%true]];[2;[200];[%false]];[3;[300];[%true]];[4;[400];[%false]]]", Scan);
        }
    }

    Y_UNIT_TEST(TestFeatureFlag, EQueryMode, ETableKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();

        const TString tableName = "/Root/Table1";
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableColumnshardBool(false);
        TTestHelper helperDisabled(TKikimrSettings().SetWithSampleTables(false).SetFeatureFlags(featureFlags));
        TTestHelper::TColumnTable col;
        
        TVector<TTestHelper::TColumnSchema> schema;

        TVector<TRow> rows = { { 1, 100, true }, { 2, 200, false } };

        if (Table == ETableKind::DATASHARD) {
            Base::PrepareBase(helperDisabled, Table, tableName, &col, &schema);
            Base::LoadData(helperDisabled, Table, ELoadKind::ARROW, tableName, rows, &col, &schema);
            CheckOrExec(
                helperDisabled, "SELECT id, int, b FROM `" + tableName + "` ORDER BY id", "[[1;[100];[%true]];[2;[200];[%false]]]", Scan);
        } else {
            bool tableCreated = TryPrepareBase(helperDisabled, Table, tableName, &col, &schema);
            if (tableCreated) {
                try {
                    Base::LoadData(helperDisabled, Table, ELoadKind::ARROW, tableName, rows, &col, &schema);
                    UNIT_ASSERT_C(false, "Expected error for ColumnShard with disabled feature flag");
                } catch (const std::exception& e) {
                    TString errorMsg = e.what();
                    UNIT_ASSERT_C(errorMsg.find("EnableColumnshardBool") != TString::npos || errorMsg.find("bool") != TString::npos,
                        "Expected error about bool support, got: " + errorMsg);
                }
            }
        }

        TTestHelper helperEnabled(TBoolTraits::CreateSettings());
        TTestHelper::TColumnTable col2;
        TVector<TTestHelper::TColumnSchema> schema2;
        Base::PrepareBase(helperEnabled, Table, tableName, &col2, &schema2);
        Base::LoadData(helperEnabled, Table, ELoadKind::ARROW, tableName, rows, &col2, &schema2);
        CheckOrExec(helperEnabled, "SELECT id, int, b FROM `" + tableName + "` ORDER BY id", "[[1;[100];[%true]];[2;[200];[%false]]]", Scan);
    }

    Y_UNIT_TEST(TestBoolAsPrimaryKey, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto createAndFill = [&](const TString& name,
                                 const TVector<TTestHelper::TColumnSchema>& schema,
                                 const TVector<TString>& pkColumns,
                                 const TVector<TRow>& rows,
                                 const TString& selectExpr,
                                 const TString& expectedScan) -> decltype(auto) {
            TTestHelper helper(TBoolTraits::CreateSettings());
            TTestHelper::TColumnTable col;
            col.SetName(name).SetPrimaryKey(pkColumns).SetSharding(pkColumns).SetSchema(schema);
            helper.CreateTable(col);

            if (schema.size() == 1) {
                if (Load == ELoadKind::ARROW) {
                    using namespace NKikimr::NKqp::NTestArrow;
                    std::vector<bool> bs; bs.reserve(rows.size());
                    for (auto&& r : rows) bs.push_back(r.TypedVal.value());
                    auto bArr = MakeBoolArrayAsUInt8(bs);
                    auto batch = MakeBatch({ arrow::field("b", arrow::uint8(), /*nullable*/ false) }, { bArr });
                    helper.BulkUpsert(col, batch);
                } else if (Load == ELoadKind::YDB_VALUE) {
                    TValueBuilder builder;
                    builder.BeginList();
                    for (auto&& r : rows) {
                        builder.AddListItem().BeginStruct().AddMember("b").Bool(r.TypedVal.value()).EndStruct();
                    }

                    builder.EndList();
                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                } else {
                    TStringBuilder csv;
                    for (auto&& r : rows) {
                        csv << (r.TypedVal.value() ? "true" : "false") << "\n";
                    }

                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, csv).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                }

                CheckOrExec(helper, "SELECT COUNT(*) FROM `" + name + "`", "[[2u]]", Scan);
            } else {
                if (Load == ELoadKind::ARROW) {
                    using namespace NKikimr::NKqp::NTestArrow;
                    std::vector<int32_t> ids; ids.reserve(rows.size());
                    std::vector<uint8_t> bools; bools.reserve(rows.size());
                    std::vector<int64_t> ints; ints.reserve(rows.size());
                    for (auto&& r : rows) { ids.push_back(r.Id); bools.push_back(r.TypedVal.value() ? 1u : 0u); ints.push_back(r.IntVal); }
                    auto idArr = MakeInt32Array(ids);
                    auto bArr  = MakeUInt8Array(bools);
                    auto iArr  = MakeInt64Array(ints);
                    auto batch = MakeBatch({ arrow::field("id", arrow::int32()), arrow::field("b", arrow::uint8(), /*nullable*/ false), arrow::field("int", arrow::int64()) }, { idArr, bArr, iArr });
                    helper.BulkUpsert(col, batch);
                } else if (Load == ELoadKind::YDB_VALUE) {
                    TValueBuilder builder;
                    builder.BeginList();
                    for (auto&& r : rows) {
                        builder.AddListItem().BeginStruct()
                            .AddMember("id").Int32(r.Id)
                            .AddMember("b").Bool(r.TypedVal.value())
                            .AddMember("int").Int64(r.IntVal)
                            .EndStruct();
                    }

                    builder.EndList();
                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                } else {
                    TStringBuilder csv;
                    for (auto&& r : rows) {
                        csv << r.Id << "," << (r.TypedVal.value() ? "true" : "false") << "," << r.IntVal << "\n";
                    }

                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, csv).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                }

                CheckOrExec(helper, "SELECT " + selectExpr + " FROM `" + name + "` ORDER BY id", expectedScan, Scan);
            }
        };

        TVector<TTestHelper::TColumnSchema> schemaOnly = {
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TVector<TTestHelper::TColumnSchema> schemaTriad = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
        };

        TVector<TRow> rows = { { 1, 100, true }, { 2, 200, false } };

        createAndFill("/Root/BoolPkOnly", schemaOnly, { "b" }, rows, "b", R"([[%true];[%false]])");

        createAndFill("/Root/BoolPkFirst", schemaTriad, { "b", "id" }, rows,
                      "b, id, int", R"([[%true;1;100];[%false;2;200]])");

        createAndFill("/Root/BoolPkMiddle", schemaTriad, { "id", "b", "int" }, rows,
                      "b, id, int", R"([[%true;1;100];[%false;2;200]])");

        createAndFill("/Root/BoolPkLast", schemaTriad, { "id", "int", "b" }, rows,
                      "b, id, int", R"([[%true;1;100];[%false;2;200]])");
    }

    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto settings = TBoolTraits::CreateSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(settings);

        const TString ds = "/Root/RowSrc";
        const TString cs = "/Root/ColSrc";

        Base::CreateDataShardTable(helper, ds);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, b) VALUES (1, 100, true), (2, 200, false)");
        if (Load == ELoadKind::ARROW) {
            arrow::Int32Builder id;
            arrow::Int64Builder i64;
            arrow::UInt8Builder b;
            Y_ABORT_UNLESS(id.Append(1).ok());
            Y_ABORT_UNLESS(i64.Append(100).ok());
            Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(2).ok());
            Y_ABORT_UNLESS(i64.Append(200).ok());
            Y_ABORT_UNLESS(b.Append(0).ok());
            std::shared_ptr<arrow::Array> idArr; Y_ABORT_UNLESS(id.Finish(&idArr).ok());
            std::shared_ptr<arrow::Array> iArr; Y_ABORT_UNLESS(i64.Finish(&iArr).ok());
            std::shared_ptr<arrow::Array> bArr; Y_ABORT_UNLESS(b.Finish(&bArr).ok());
            auto aSchema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false),
                arrow::field("int", arrow::int64(), /*nullable*/ false), arrow::field("b", arrow::uint8(), /*nullable*/ false) });
            auto batch = arrow::RecordBatch::Make(aSchema, 2, { idArr, iArr, bArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("int").Int64(100).AddMember("b").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("int").Int64(200).AddMember("b").Bool(false).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,100,true\n2,200,false\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT b, id, int FROM `" + ds + "` ORDER BY id", R"([[[%true];1;[100]];[[%false];2;[200]]])", Scan);
        CheckOrExec(helper, "SELECT b, id, int FROM `" + cs + "` ORDER BY id", R"([[%true;1;100];[%false;2;200]])", Scan);

        helper.ExecuteQuery("UPDATE `" + ds + "` SET int = int + 1 WHERE b = true");
        helper.ExecuteQuery("UPDATE `" + cs + "` SET int = int + 1 WHERE b = true");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + ds + "` ORDER BY id", R"([[[%true];1;[101]];[[%false];2;[200]]])", Scan);
        CheckOrExec(helper, "SELECT b, id, int FROM `" + cs + "` ORDER BY id", R"([[%true;1;101];[%false;2;200]])", Scan);

        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, b) VALUES (3, 300, true)");
        helper.ExecuteQuery("UPSERT INTO `" + cs + "` (id, int, b) VALUES (3, 300, true)");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + ds + "` ORDER BY id", R"([[[%true];1;[101]];[[%false];2;[200]];[[%true];3;[300]]])", Scan);
        CheckOrExec(helper, "SELECT b, id, int FROM `" + cs + "` ORDER BY id", R"([[%true;1;101];[%false;2;200];[%true;3;300]])", Scan);

        helper.ExecuteQuery("REPLACE INTO `" + ds + "` (id, int, b) VALUES (2, 250, false)");
        helper.ExecuteQuery("REPLACE INTO `" + cs + "` (id, int, b) VALUES (2, 250, false)");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + ds + "` ORDER BY id", R"([[[%true];1;[101]];[[%false];2;[250]];[[%true];3;[300]]])", Scan);
        CheckOrExec(helper, "SELECT b, id, int FROM `" + cs + "` ORDER BY id", R"([[%true;1;101];[%false;2;250];[%true;3;300]])", Scan);

        helper.ExecuteQuery("DELETE FROM `" + ds + "` WHERE b = false");
        helper.ExecuteQuery("DELETE FROM `" + cs + "` WHERE b = false");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + ds + "` ORDER BY id", R"([[[%true];1;[101]];[[%true];3;[300]]])", Scan);
        CheckOrExec(helper, "SELECT b, id, int FROM `" + cs + "` ORDER BY id", R"([[%true;1;101];[%true;3;300]])", Scan);

        const TString csFromDs = "/Root/CSFromDS";
        TVector<TTestHelper::TColumnSchema> schema2 = schema;
        TTestHelper::TColumnTable col2;
        col2.SetName(csFromDs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema2);
        helper.CreateTable(col2);
        helper.ExecuteQuery(
            "UPSERT INTO `" + csFromDs + "` (id, int, b) "
            "SELECT id, COALESCE(int, CAST(0 AS Int64)), COALESCE(b, CAST(false AS Bool)) FROM `" + ds + "`");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + csFromDs + "` ORDER BY id", R"([[%true;1;101];[%true;3;300]])", Scan);

        const TString dsFromCs = "/Root/DSFromCS";
        helper.ExecuteQuery("CREATE TABLE `" + dsFromCs + "` (id Int32 NOT NULL, int Int64 NOT NULL, b Bool NOT NULL, PRIMARY KEY (id))");
        helper.ExecuteQuery(
            "UPSERT INTO `" + dsFromCs + "` (id, int, b) "
            "SELECT id, COALESCE(int, CAST(0 AS Int64)), COALESCE(b, CAST(false AS Bool)) FROM `" + cs + "`");
        CheckOrExec(helper, "SELECT b, id, int FROM `" + dsFromCs + "` ORDER BY id", R"([[%true;1;101];[%true;3;300]])", Scan);
    }

    Y_UNIT_TEST(TestBoolOperatorsAndAggregations, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolOps";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto idArr = MakeInt32Array({1,2,3});
            auto bArr  = MakeBoolArrayAsUInt8Nullable({true, false, std::nullopt});
            auto batch = MakeBatch(
                { arrow::field("id", arrow::int32(), /*nullable*/ false),
                  arrow::field("b", arrow::uint8()) },
                { idArr, bArr }
            );

            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("b").BeginOptional().Bool(true).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("b").BeginOptional().Bool(false).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("b").EmptyOptional(EPrimitiveType::Bool).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,true\n2,false\n3,\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT NOT b FROM `" + cs + "` ORDER BY id", R"([[[%false]];[[%true]];[#]])", Scan);
        CheckOrExec(helper, "SELECT b AND true FROM `" + cs + "` ORDER BY id", R"([[[%true]];[[%false]];[#]])", Scan);
        CheckOrExec(helper, "SELECT b AND false FROM `" + cs + "` ORDER BY id", R"([[[%false]];[[%false]];[[%false]]])", Scan);
        CheckOrExec(helper, "SELECT b OR true FROM `" + cs + "` ORDER BY id", R"([[[%true]];[[%true]];[[%true]]])", Scan);
        CheckOrExec(helper, "SELECT b OR false FROM `" + cs + "` ORDER BY id", R"([[[%true]];[[%false]];[#]])", Scan);
        CheckOrExec(helper, "SELECT b = true FROM `" + cs + "` ORDER BY id", R"([[[%true]];[[%false]];[#]])", Scan);
        CheckOrExec(helper, "SELECT b != false FROM `" + cs + "` ORDER BY id", R"([[[%true]];[[%false]];[#]])", Scan);
        CheckOrExec(helper, "SELECT b IS NULL FROM `" + cs + "` ORDER BY id", R"([[%false];[%false];[%true]])", Scan);
        CheckOrExec(helper, "SELECT min(b), max(b), count(b), count(*) FROM `" + cs + "`",
            R"([[[%false];[%true];2u;3u]])", Scan);
        CheckOrExec(helper, "SELECT b, count(*) FROM `" + cs + "` GROUP BY b ORDER BY b",
            R"([[#;1u];[[%false];1u];[[%true];1u]])", Scan);
    }

    Y_UNIT_TEST(TestOrderByBoolWithLimit, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolOrderLimit";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            arrow::Int32Builder id;
            arrow::UInt8Builder b;
            Y_ABORT_UNLESS(id.Append(1).ok()); Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(2).ok()); Y_ABORT_UNLESS(b.Append(0).ok());
            Y_ABORT_UNLESS(id.Append(3).ok()); Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(4).ok()); Y_ABORT_UNLESS(b.Append(0).ok());
            std::shared_ptr<arrow::Array> idArr; Y_ABORT_UNLESS(id.Finish(&idArr).ok());
            std::shared_ptr<arrow::Array> bArr; Y_ABORT_UNLESS(b.Finish(&bArr).ok());
            auto aSchema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false),
                arrow::field("b", arrow::uint8(), /*nullable*/ false) });
            auto batch = arrow::RecordBatch::Make(aSchema, 4, { idArr, bArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("b").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("b").Bool(false).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("b").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(4).AddMember("b").Bool(false).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,true\n2,false\n3,true\n4,false\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString qAsc = "SELECT b, id FROM `" + cs + "` ORDER BY b, id LIMIT 1";
        const TString qDesc = "SELECT b, id FROM `" + cs + "` ORDER BY b DESC, id LIMIT 1";

        CheckOrExec(helper, qAsc, R"([[%false;2]])", Scan);
        CheckOrExec(helper, qDesc, R"([[%true;1]])", Scan);

        const TString csPk = "/Root/BoolOrderLimitPk";
        TVector<TTestHelper::TColumnSchema> schemaPk = {
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable colPk;
        colPk.SetName(csPk).SetPrimaryKey({ "b", "id" }).SetSharding({ "b", "id" }).SetSchema(schemaPk);
        helper.CreateTable(colPk);

        if (Load == ELoadKind::ARROW) {
            arrow::UInt8Builder b;
            arrow::Int32Builder id;
            Y_ABORT_UNLESS(b.Append(1).ok()); Y_ABORT_UNLESS(id.Append(1).ok());
            Y_ABORT_UNLESS(b.Append(0).ok()); Y_ABORT_UNLESS(id.Append(2).ok());
            Y_ABORT_UNLESS(b.Append(1).ok()); Y_ABORT_UNLESS(id.Append(3).ok());
            Y_ABORT_UNLESS(b.Append(0).ok()); Y_ABORT_UNLESS(id.Append(4).ok());
            std::shared_ptr<arrow::Array> bArr; Y_ABORT_UNLESS(b.Finish(&bArr).ok());
            std::shared_ptr<arrow::Array> idArr; Y_ABORT_UNLESS(id.Finish(&idArr).ok());
            auto aSchema = arrow::schema({ arrow::field("b", arrow::uint8(), /*nullable*/ false),
                arrow::field("id", arrow::int32(), /*nullable*/ false) });
            auto batch = arrow::RecordBatch::Make(aSchema, 4, { bArr, idArr });
            helper.BulkUpsert(colPk, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("b").Bool(true).AddMember("id").Int32(1).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("b").Bool(false).AddMember("id").Int32(2).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("b").Bool(true).AddMember("id").Int32(3).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("b").Bool(false).AddMember("id").Int32(4).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(csPk, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "true,1\nfalse,2\ntrue,3\nfalse,4\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(csPk, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString qAscPk = "SELECT b, id FROM `" + csPk + "` ORDER BY b, id LIMIT 1";
        const TString qDescPk = "SELECT b, id FROM `" + csPk + "` ORDER BY b DESC, id LIMIT 1";
        CheckOrExec(helper, qAscPk, R"([[%false;2]])", Scan);
        CheckOrExec(helper, qDescPk, R"([[%true;1]])", Scan);
    }

    Y_UNIT_TEST(TestBoolCompare, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();
        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolWhereCmp";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            arrow::Int32Builder id;
            arrow::Int64Builder i64;
            arrow::UInt8Builder b;
            Y_ABORT_UNLESS(id.Append(1).ok());
            Y_ABORT_UNLESS(i64.Append(100).ok());
            Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(2).ok());
            Y_ABORT_UNLESS(i64.Append(200).ok());
            Y_ABORT_UNLESS(b.Append(0).ok());
            Y_ABORT_UNLESS(id.Append(3).ok());
            Y_ABORT_UNLESS(i64.Append(300).ok());
            Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(4).ok());
            Y_ABORT_UNLESS(i64.Append(400).ok());
            Y_ABORT_UNLESS(b.Append(0).ok());
            
            std::shared_ptr<arrow::Array> idArr;
            Y_ABORT_UNLESS(id.Finish(&idArr).ok());
            
            std::shared_ptr<arrow::Array> iArr;
            Y_ABORT_UNLESS(i64.Finish(&iArr).ok());

            std::shared_ptr<arrow::Array> bArr;
            Y_ABORT_UNLESS(b.Finish(&bArr).ok());

            auto aSchema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false),
                arrow::field("int", arrow::int64(), /*nullable*/ false), arrow::field("b", arrow::uint8(), /*nullable*/ false) });
            auto batch = arrow::RecordBatch::Make(aSchema, 4, { idArr, iArr, bArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("int").Int64(100).AddMember("b").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("int").Int64(200).AddMember("b").Bool(false).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("int").Int64(300).AddMember("b").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(4).AddMember("int").Int64(400).AddMember("b").Bool(false).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,100,true\n2,200,false\n3,300,true\n4,400,false\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        auto check = [&](const TString& cond, const TString& expected) {
            const TString q = "SELECT b, id, int FROM `" + cs + "` WHERE " + cond + " ORDER BY id";
            CheckOrExec(helper, q, expected, Scan);
        };

        check("b > false", R"([[%true;1;100];[%true;3;300]])");
        check("b < false", R"([])");
        check("b >= false", R"([[%true;1;100];[%false;2;200];[%true;3;300];[%false;4;400]])");
        check("b <= false", R"([[%false;2;200];[%false;4;400]])");

        check("b > true", R"([])");
        check("b < true", R"([[%false;2;200];[%false;4;400]])");
        check("b >= true", R"([[%true;1;100];[%true;3;300]])");
        check("b <= true", R"([[%true;1;100];[%false;2;200];[%true;3;300];[%false;4;400]])");
    }

    Y_UNIT_TEST(TestBoolFilterWithColumns, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();
        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolFilterCols";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("v1").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("v2").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("flag").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            arrow::Int32Builder id;
            arrow::Int64Builder v1;
            arrow::Int64Builder v2;
            arrow::UInt8Builder f;
            auto add = [&](int i, i64 a, i64 b, bool fl) -> decltype(auto) {
                Y_ABORT_UNLESS(id.Append(i).ok());
                Y_ABORT_UNLESS(v1.Append(a).ok());
                Y_ABORT_UNLESS(v2.Append(b).ok());
                Y_ABORT_UNLESS(f.Append(fl ? 1 : 0).ok());
            };

            add(1, 100, 200, true);
            add(2, 300, 100, false);
            add(3, 50, 60, true);
            add(4, 70, 40, true);

            std::shared_ptr<arrow::Array> idArr;
            Y_ABORT_UNLESS(id.Finish(&idArr).ok());

            std::shared_ptr<arrow::Array> v1Arr;
            Y_ABORT_UNLESS(v1.Finish(&v1Arr).ok());

            std::shared_ptr<arrow::Array> v2Arr;
            Y_ABORT_UNLESS(v2.Finish(&v2Arr).ok());

            std::shared_ptr<arrow::Array> fArr;
            Y_ABORT_UNLESS(f.Finish(&fArr).ok());

            auto aSchema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false),
                arrow::field("v1", arrow::int64(), /*nullable*/ false), arrow::field("v2", arrow::int64(), /*nullable*/ false),
                arrow::field("flag", arrow::uint8(), /*nullable*/ false) });
            auto batch = arrow::RecordBatch::Make(aSchema, 4, { idArr, v1Arr, v2Arr, fArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            auto add = [&](int i, i64 a, i64 b, bool fl) -> decltype(auto) {
                builder.AddListItem().BeginStruct()
                    .AddMember("id").Int32(i)
                    .AddMember("v1").Int64(a)
                    .AddMember("v2").Int64(b)
                    .AddMember("flag").Bool(fl)
                    .EndStruct();
            };

            add(1, 100, 200, true);
            add(2, 300, 100, false);
            add(3, 50, 60, true);
            add(4, 70, 40, true);
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,100,200,true\n2,300,100,false\n3,50,60,true\n4,70,40,true\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString q = "SELECT flag, id FROM `" + cs + "` WHERE v1 < v2 AND flag ORDER BY id";
        CheckOrExec(helper, q, R"([[%true;1];[%true;3]])", Scan);
    }

    Y_UNIT_TEST(TestBoolWriteComparison, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolWriteCmp";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("a").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("cmp").SetType(NScheme::NTypeIds::Bool),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto idArr = MakeInt32Array({1, 2});
            auto aArr  = MakeInt64Array({10, 30});
            auto bArr  = MakeInt64Array({20, 5});
            auto cArr  = MakeUInt8ArrayNullable({std::nullopt, std::nullopt});
            auto batch = MakeBatch(
                { arrow::field("id", arrow::int32(), /*nullable*/ false),
                  arrow::field("a", arrow::int64(), /*nullable*/ false),
                  arrow::field("b", arrow::int64(), /*nullable*/ false),
                  arrow::field("cmp", arrow::uint8()) },
                { idArr, aArr, bArr, cArr }
            );

            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("a").Int64(10).AddMember("b").Int64(20).AddMember("cmp").EmptyOptional(EPrimitiveType::Bool).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("a").Int64(30).AddMember("b").Int64(5).AddMember("cmp").EmptyOptional(EPrimitiveType::Bool).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,10,20,\n2,30,5,\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        helper.ExecuteQuery("UPDATE `" + cs + "` SET cmp = (a < b)");

        const TString q = "SELECT cmp, id FROM `" + cs + "` ORDER BY id";
        CheckOrExec(helper, q, R"([[[%true];1];[[%false];2]])", Scan);
    }

    Y_UNIT_TEST(TestBoolNot, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolNot";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("flag").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto idArr = MakeInt32Array({1, 2});
            auto fArr  = MakeBoolArrayAsUInt8({true, false});
            auto batch = MakeBatch(
                { arrow::field("id", arrow::int32(), /*nullable*/ false),
                  arrow::field("flag", arrow::uint8(), /*nullable*/ false) },
                { idArr, fArr }
            );

            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("flag").Bool(true).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("flag").Bool(false).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv; csv << "1,true\n2,false\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString q = "SELECT NOT flag, id FROM `" + cs + "` ORDER BY id";
        CheckOrExec(helper, q, R"([[%false;1];[%true;2]])", Scan);
    }

    Y_UNIT_TEST(TestBoolGroupByCounts, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(TBoolTraits::CreateSettings());

        const TString cs = "/Root/BoolGroup";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            arrow::Int32Builder id;
            arrow::UInt8Builder b;
            Y_ABORT_UNLESS(id.Append(1).ok()); Y_ABORT_UNLESS(b.Append(1).ok());
            Y_ABORT_UNLESS(id.Append(2).ok()); Y_ABORT_UNLESS(b.Append(0).ok());
            Y_ABORT_UNLESS(id.Append(3).ok()); Y_ABORT_UNLESS(b.AppendNull().ok());
            std::shared_ptr<arrow::Array> idArr; Y_ABORT_UNLESS(id.Finish(&idArr).ok());
            std::shared_ptr<arrow::Array> bArr; Y_ABORT_UNLESS(b.Finish(&bArr).ok());
            auto aSchema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false), arrow::field("b", arrow::uint8()) });
            auto batch = arrow::RecordBatch::Make(aSchema, 3, { idArr, bArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("b").BeginOptional().Bool(true).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("b").BeginOptional().Bool(false).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("b").EmptyOptional(EPrimitiveType::Bool).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,true\n2,false\n3,\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT b, count(*) FROM `" + cs + "` GROUP BY b ORDER BY b",
            R"([[#;1u];[[%false];1u];[[%true];1u]])", Scan);
    }
}
}   // namespace NKqp
}   // namespace NKikimr
