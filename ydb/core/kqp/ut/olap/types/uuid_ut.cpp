#include "column_type_scenarios.h"
#include "column_type_test_base.h"

#include <yql/essentials/types/uuid/uuid.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpUuidColumnShard) {
    namespace {

    std::shared_ptr<arrow::Array> MakeUuidArrayNullable(const std::vector<std::optional<TString>>& uuidStrings) {
        auto type = arrow::fixed_size_binary(16);
        arrow::FixedSizeBinaryBuilder builder(type);
        for (const auto& u : uuidStrings) {
            if (u.has_value()) {
                TUuidValue uv{std::string(*u)};
                Y_ABORT_UNLESS(builder.Append(reinterpret_cast<const uint8_t*>(uv.Buf_.Bytes)).ok());
            } else {
                Y_ABORT_UNLESS(builder.AppendNull().ok());
            }
        }

        std::shared_ptr<arrow::Array> out;
        Y_ABORT_UNLESS(builder.Finish(&out).ok());
        return out;
    }

    struct TUuidTraits {
        using TValue = TString;
        static constexpr const char* ColumnName = "uid";
        static constexpr const char* SqlTypeName = "Uuid";

        static TKikimrSettings CreateSettings() {
            return CreateColumnshardSettings([](auto& f) { f.SetEnableColumnshardUuid(true); });
        }

        static auto GetTypeId() { return NScheme::NTypeIds::Uuid; }

        static void AppendYdbValue(TValueBuilder& builder, const std::optional<TString>& val) {
            if (val.has_value()) {
                builder.BeginOptional().Uuid(TUuidValue(std::string(*val))).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Uuid);
            }
        }

        static void AppendCsvValue(TStringBuilder& builder, const std::optional<TString>& val) {
            if (val.has_value()) {
                builder << *val;
            }
        }

        static std::shared_ptr<arrow::Array> MakeArrowArray(const TVector<TTypedRow<TString>>& rows) {
            std::vector<std::optional<TString>> uuids;
            uuids.reserve(rows.size());
            for (auto&& r : rows) {
                uuids.push_back(r.TypedVal);
            }
            return MakeUuidArrayNullable(uuids);
        }

        static std::shared_ptr<arrow::DataType> ArrowType() { return arrow::fixed_size_binary(16); }

        static void LoadPkTable(TTestHelper& helper, ELoadKind load, const TString& tableName, TTestHelper::TColumnTable& table);
    };

    COLUMN_TYPE_TEST_USING(TUuidTraits);

    static const TString UUID1 = "550e8400-e29b-41d4-a716-446655440000";
    static const TString UUID2 = "550e8400-e29b-41d4-a716-446655440001";
    static const TString UUID3 = "550e8400-e29b-41d4-a716-446655440002";
    static const TString UUID4 = "660e8400-e29b-41d4-a716-446655440000";
    static const TString UUID5 = "770e8400-e29b-41d4-a716-446655440000";

    static const TString UUID_A = "11111111-1111-1111-1111-111111111111";
    static const TString UUID_B = "22222222-2222-2222-2222-222222222222";
    static const TString UUID_C = "33333333-3333-3333-3333-333333333333";
    static const TString UUID_D = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    static const TString UUID_E = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    void TUuidTraits::LoadPkTable(TTestHelper& helper, ELoadKind load, const TString& tableName, TTestHelper::TColumnTable& table) {
        if (load == ELoadKind::ARROW) {
            std::vector<std::optional<TString>> uuids = { UUID_A, UUID_B, UUID_C };
            auto uuidArr = MakeUuidArrayNullable(uuids);
            using namespace NKikimr::NKqp::NTestArrow;
            auto valArr = MakeInt64Array({ (int64_t)1, (int64_t)2, (int64_t)3 });
            auto batch = MakeBatch(
                { arrow::field("uid", arrow::fixed_size_binary(16), /*nullable*/ false),
                  arrow::field("val", arrow::int64()) },
                { uuidArr, valArr }
            );
            helper.BulkUpsert(table, batch);
        } else if (load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_A))).AddMember("val").Int64(1).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_B))).AddMember("val").Int64(2).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_C))).AddMember("val").Int64(3).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(tableName, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "11111111-1111-1111-1111-111111111111,1\n";
            csv << "22222222-2222-2222-2222-222222222222,2\n";
            csv << "33333333-3333-3333-3333-333333333333,3\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(tableName, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }
    }

    TScenario<TString> FilterEqualScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID1 }, { 2, 0, UUID4 }, { 3, 0, UUID5 } },
            {
                { R"(SELECT id FROM `/Root/Table1` WHERE uid = CAST("550e8400-e29b-41d4-a716-446655440000" AS Uuid))", "[[1]]" },
                { R"(SELECT id FROM `/Root/Table1` WHERE uid != CAST("550e8400-e29b-41d4-a716-446655440000" AS Uuid) ORDER BY id)", "[[2];[3]]" },
                { R"(SELECT id FROM `/Root/Table1` WHERE uid = CAST("660e8400-e29b-41d4-a716-446655440000" AS Uuid))", "[[2]]" },
            },
        };
    }

    TScenario<TString> FilterNullsScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID1 }, { 2, 0, std::nullopt }, { 3, 0, UUID5 }, { 4, 0, std::nullopt } },
            {
                { "SELECT id FROM `/Root/Table1` WHERE uid IS NULL ORDER BY id", "[[2];[4]]" },
                { "SELECT id FROM `/Root/Table1` WHERE uid IS NOT NULL ORDER BY id", "[[1];[3]]" },
            },
        };
    }

    TScenario<TString> FilterCompareScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID_A }, { 2, 0, UUID_B }, { 3, 0, UUID_C }, { 4, 0, UUID_D } },
            {
                { R"(SELECT id FROM `/Root/Table1` WHERE uid < CAST("33333333-3333-3333-3333-333333333333" AS Uuid) ORDER BY id)", "[[1];[2]]" },
                { R"(SELECT id FROM `/Root/Table1` WHERE uid > CAST("22222222-2222-2222-2222-222222222222" AS Uuid) ORDER BY id)", "[[3];[4]]" },
                { R"(SELECT id FROM `/Root/Table1` WHERE uid <= CAST("22222222-2222-2222-2222-222222222222" AS Uuid) ORDER BY id)", "[[1];[2]]" },
                { R"(SELECT id FROM `/Root/Table1` WHERE uid >= CAST("33333333-3333-3333-3333-333333333333" AS Uuid) ORDER BY id)", "[[3];[4]]" },
            },
        };
    }

    TScenario<TString> OrderByScenario() {
        return {
            "/Root/Table1",
            { { 1, 10, UUID_C }, { 2, 20, UUID_A }, { 3, 30, UUID_B } },
            {
                { "SELECT uid, id FROM `/Root/Table1` ORDER BY uid",
                    R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3];[["33333333-3333-3333-3333-333333333333"];1]])" },
                { "SELECT uid, id FROM `/Root/Table1` ORDER BY uid DESC",
                    R"([[["33333333-3333-3333-3333-333333333333"];1];[["22222222-2222-2222-2222-222222222222"];3];[["11111111-1111-1111-1111-111111111111"];2]])" },
            },
        };
    }

    TScenario<TString> GroupByScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID_D }, { 2, 0, UUID_E }, { 3, 0, UUID_D }, { 4, 0, UUID_E }, { 5, 0, UUID1 } },
            {
                { "SELECT uid, count(*) AS cnt FROM `/Root/Table1` GROUP BY uid ORDER BY uid",
                    R"([[["550e8400-e29b-41d4-a716-446655440000"];1u];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2u];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];2u]])" },
            },
        };
    }

    TScenario<TString> AggregationScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID_A }, { 2, 0, UUID_C }, { 3, 0, UUID_B }, { 4, 0, std::nullopt } },
            {
                { "SELECT min(uid) FROM `/Root/Table1`", R"([[["11111111-1111-1111-1111-111111111111"]]])" },
                { "SELECT max(uid) FROM `/Root/Table1`", R"([[["33333333-3333-3333-3333-333333333333"]]])" },
                { "SELECT count(uid) FROM `/Root/Table1`", "[[3u]]" },
                { "SELECT count(*) FROM `/Root/Table1`", "[[4u]]" },
            },
        };
    }

    TJoinScenario<TString> JoinByUuidScenario() {
        return {
            "/Root/Table1",
            "/Root/Table2",
            { { 1, 10, UUID_D }, { 2, 20, UUID_E } },
            { { 1, 100, UUID_E }, { 2, 200, UUID_D }, { 3, 300, UUID_A } },
            {
                { R"(SELECT t1.id, t2.id, t1.uid FROM `/Root/Table1` AS t1 JOIN `/Root/Table2` AS t2 ON t1.uid = t2.uid ORDER BY t1.id, t2.id)",
                    R"([[1;2;["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]];[2;1;["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]]])" },
            },
        };
    }

    TScenario<TString> OrderByWithLimitScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID_C }, { 2, 0, UUID_A }, { 3, 0, UUID_B }, { 4, 0, UUID_D } },
            {
                { "SELECT uid, id FROM `/Root/Table1` ORDER BY uid LIMIT 2",
                    R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3]])" },
                { "SELECT uid, id FROM `/Root/Table1` ORDER BY uid DESC LIMIT 1",
                    R"([[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];4]])" },
                { "SELECT uid, id FROM `/Root/Table1` ORDER BY uid LIMIT 3",
                    R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3];[["33333333-3333-3333-3333-333333333333"];1]])" },
            },
        };
    }

    TScenario<TString> GroupByWithNullsScenario() {
        return {
            "/Root/Table1",
            { { 1, 0, UUID_D }, { 2, 0, std::nullopt }, { 3, 0, UUID_D }, { 4, 0, std::nullopt }, { 5, 0, UUID_E } },
            {
                { "SELECT uid, count(*) AS cnt FROM `/Root/Table1` GROUP BY uid ORDER BY uid",
                    R"([[#;2u];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2u];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];1u]])" },
            },
        };
    }

    TPkLookupScenario PkLookupScenario() {
        const TString tableName = "/Root/ColumnTableTest";
        return {
            tableName,
            {
                { R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("22222222-2222-2222-2222-222222222222" AS Uuid))", "[[[2]]]" },
                { R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("11111111-1111-1111-1111-111111111111" AS Uuid))", "[[[1]]]" },
                { R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("33333333-3333-3333-3333-333333333333" AS Uuid))", "[[[3]]]" },
            },
        };
    }

    TCsvScenario CsvScenario() {
        const TString tableName = "/Root/Table1";
        return {
            tableName,
            "1,0,550e8400-e29b-41d4-a716-446655440000\n"
            "2,0,660e8400-e29b-41d4-a716-446655440000\n"
            "3,0,770e8400-e29b-41d4-a716-446655440000\n",
            {
                { "SELECT id FROM `" + tableName + "` ORDER BY id", "[[1];[2];[3]]" },
                { "SELECT uid, id FROM `" + tableName + "` ORDER BY id",
                    R"([[["550e8400-e29b-41d4-a716-446655440000"];1];[["660e8400-e29b-41d4-a716-446655440000"];2];[["770e8400-e29b-41d4-a716-446655440000"];3]])" },
            },
        };
    }

}   // namespace

    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TUuidTraits::CreateSettings());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        Base::PrepareBase(helper, Table, tableName, &col, &schema);
        Base::LoadData(helper, Table, Load, tableName,
            { { 1, 10, UUID1 }, { 2, 20, UUID4 }, { 3, 30, std::nullopt } }, &col, &schema);

        CheckOrExec(helper, "SELECT id, int FROM `/Root/Table1` WHERE id=1", "[[1;[10]]]", Scan);
        CheckOrExec(helper, "SELECT id, int FROM `/Root/Table1` WHERE id=3", "[[3;[30]]]", Scan);
        CheckOrExec(helper, "SELECT id, int FROM `/Root/Table1` ORDER BY id",
            "[[1;[10]];[2;[20]];[3;[30]]]", Scan);
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` WHERE id=1",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1]])", Scan);
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` WHERE id=3",
            R"([[#;3]])", Scan);
    }

    Y_UNIT_TEST_SCENARIO(TestFilterEqual, FilterEqualScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterNulls, FilterNullsScenario);
    Y_UNIT_TEST_SCENARIO(TestFilterCompare, FilterCompareScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderBy, OrderByScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupBy, GroupByScenario);
    Y_UNIT_TEST_SCENARIO(TestAggregation, AggregationScenario);

    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(TUuidTraits::CreateSettings());
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid),
            };

            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);
            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid),
            };

            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            Base::CreateDataShardTable(helper, t1);
            Base::CreateDataShardTableWithSecondColumn(helper, t2, "table1_id");
        }

        Base::LoadData(helper, Table, Load, t1, { { 1, 100, UUID1 }, { 2, 200, UUID4 } }, &col1, &s1);
        if (Table == ETableKind::COLUMNSHARD) {
            Base::LoadData(helper, Table, Load, t2, { { 1, 1, UUID_D }, { 2, 1, UUID_E }, { 3, 2, UUID_A } }, &col2, &s2);
        } else {
            if (Load == ELoadKind::ARROW) {
                Base::BulkUpsertRowTableArrowWithSecondColumn(
                    helper, t2, { { 1, 1, UUID_D }, { 2, 1, UUID_E }, { 3, 2, UUID_A } }, "table1_id");
            } else if (Load == ELoadKind::YDB_VALUE) {
                Base::BulkUpsertRowTableYdbValueWithSecondColumn(
                    helper, t2, { { 1, 1, UUID_D }, { 2, 1, UUID_E }, { 3, 2, UUID_A } }, "table1_id");
            } else {
                TStringBuilder csv;
                csv << "1,1,aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n"
                    << "2,1,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\n"
                    << "3,2,11111111-1111-1111-1111-111111111111\n";
                auto result = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        CheckOrExec(helper,
            R"(SELECT t1.id, t1.uid, t2.uid FROM `/Root/Table1` AS t1 JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id ORDER BY t1.id, t2.id)",
            R"([[1;["550e8400-e29b-41d4-a716-446655440000"];["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]];[1;["550e8400-e29b-41d4-a716-446655440000"];["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]];[2;["660e8400-e29b-41d4-a716-446655440000"];["11111111-1111-1111-1111-111111111111"]]])", Scan);
    }

    Y_UNIT_TEST_JOIN_SCENARIO(TestJoinByUuid, JoinByUuidScenario);
    Y_UNIT_TEST_SCENARIO(TestOrderByWithLimit, OrderByWithLimitScenario);
    Y_UNIT_TEST_SCENARIO(TestGroupByWithNulls, GroupByWithNullsScenario);
    Y_UNIT_TEST_PK_SCENARIO(TestUuidAsPrimaryKey, PkLookupScenario);

    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto runnerSettings = TUuidTraits::CreateSettings();
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(runnerSettings);

        const TString cs = "/Root/ColSrc";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid),
            TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64)
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto idArr = MakeInt32Array({1, 2, 3});
            std::vector<std::optional<TString>> uuids = { UUID1, UUID4, UUID5 };
            auto uuidArr = MakeUuidArrayNullable(uuids);
            auto valArr = MakeInt64Array({ (int64_t)10, (int64_t)20, (int64_t)30 });
            auto batch = MakeBatch(
                { arrow::field("id", arrow::int32(), /*nullable*/ false),
                  arrow::field("uid", arrow::fixed_size_binary(16)),
                  arrow::field("val", arrow::int64()) },
                { idArr, uuidArr, valArr }
            );

            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(1)
                .AddMember("uid").BeginOptional().Uuid(TUuidValue(std::string(UUID1))).EndOptional()
                .AddMember("val").Int64(10)
                .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(2)
                .AddMember("uid").BeginOptional().Uuid(TUuidValue(std::string(UUID4))).EndOptional()
                .AddMember("val").Int64(20)
                .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(3)
                .AddMember("uid").BeginOptional().Uuid(TUuidValue(std::string(UUID5))).EndOptional()
                .AddMember("val").Int64(30)
                .EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "1,550e8400-e29b-41d4-a716-446655440000,10\n"
                << "2,660e8400-e29b-41d4-a716-446655440000,20\n"
                << "3,770e8400-e29b-41d4-a716-446655440000,30\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert(cs, EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT uid, id, val FROM `" + cs + "` ORDER BY id",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1;[10]];[["660e8400-e29b-41d4-a716-446655440000"];2;[20]];[["770e8400-e29b-41d4-a716-446655440000"];3;[30]]])", Scan);

        helper.ExecuteQuery(
            R"(UPDATE `)" + cs + R"(` SET uid = CAST("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" AS Uuid) WHERE id = 2)");

        CheckOrExec(helper,
            "SELECT uid, id FROM `" + cs + "` WHERE id = 2",
            R"([[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2]])", Scan);

        helper.ExecuteQuery(
            "DELETE FROM `" + cs + "` WHERE id = 3");

        CheckOrExec(helper,
            "SELECT id FROM `" + cs + "` ORDER BY id",
            "[[1];[2]]", Scan);

        helper.ExecuteQuery(
            R"(UPSERT INTO `)" + cs + R"(` (id, uid, val) VALUES
                (4, CAST("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" AS Uuid), 40))");

        CheckOrExec(helper,
            "SELECT uid, id, val FROM `" + cs + "` ORDER BY id",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1;[10]];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2;[20]];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];4;[40]]])", Scan);
    }

    Y_UNIT_TEST_CSV_SCENARIO(TestCsv, CsvScenario);
}

} // namespace NKqp
} // namespace NKikimr
