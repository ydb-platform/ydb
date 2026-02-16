#include "bool_test_enums.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>

#include <functional>
#include <tuple>

template <class... Es>
static TString BuildParamTestName(const char* base, const Es&... es) {
    TString s = base;
    ((s += "-", s += ToString(es)), ...);
    return s;
}

template <class F>
static void ForEachProductRanges(F&& f) {
    std::invoke(std::forward<F>(f));
}

template <class F, class FirstRange, class... RestRanges>
static void ForEachProductRanges(F&& f, const FirstRange& first, const RestRanges&... rest) {
    for (auto&& x : first) {
        auto bound = std::bind_front(std::forward<F>(f), x);
        ForEachProductRanges(std::move(bound), rest...);
    }
}

template <class... Enums, class F>
static void ForEachEnums(F&& f) {
    ForEachProductRanges(std::forward<F>(f), GetEnumAllValues<Enums>()...);
}

template <class Tuple, size_t... I>
static TString BuildParamTestNameFromTupleImpl(const char* base, const Tuple& t, std::index_sequence<I...>) {
    return BuildParamTestName(base, std::get<I>(t)...);
}

template <class Tuple>
static TString BuildParamTestNameFromTuple(const char* base, const Tuple& t) {
    return BuildParamTestNameFromTupleImpl(base, t, std::make_index_sequence<std::tuple_size<Tuple>::value>{});
}

#define Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(N, ...) \
    struct TTestCase##N: public TCurrentTestCase { \
        using Types = std::tuple<__VA_ARGS__>; \
        Types Args; \
        TString ParametrizedTestName; \
        explicit TTestCase##N(Types args) \
            : Args(std::move(args)) \
            , ParametrizedTestName(BuildParamTestNameFromTuple(#N, Args)) { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
        static THolder<NUnitTest::TBaseTestCase> Create(Types args) { \
            return ::MakeHolder<TTestCase##N>(std::move(args)); \
        } \
        void Execute_(NUnitTest::TTestContext&) override; \
        template <size_t I> \
        decltype(auto) Arg() const { \
            return std::get<I>(Args); \
        } \
    }; \
    struct TTestRegistration##N { \
        TTestRegistration##N() { \
            ForEachEnums<__VA_ARGS__>([&](auto... items) { \
                TCurrentTest::AddTest([=] { \
                    return TTestCase##N::Create(typename TTestCase##N::Types(items...)); \
                }); \
            }); \
        } \
    }; \
    static TTestRegistration##N testRegistration##N; \
    void TTestCase##N::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpBoolColumnShard) {
    namespace {
    struct TRow {
        i32 Id;
        i64 IntVal;
        std::optional<bool> B;
    };

    TKikimrSettings CreateKikimrSettingsWithBoolSupport() {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableColumnshardBool(true);
        return TKikimrSettings().SetWithSampleTables(false).SetFeatureFlags(featureFlags);
    }

    void CreateDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    int Int64,
                    b Bool,
                    PRIMARY KEY (id)
                );
            )")
                       .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
    }

    void CreateDataShardTableWithSecondColumn(TTestHelper& helper, const TString& name, const TString& secondName) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    )" << secondName << R"( Int64,
                    b Bool,
                    PRIMARY KEY (id)
                );
            )")
                       .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
    }

    void BulkUpsertRowTableYdbValueWithColumnName(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& columnName) {
        TValueBuilder builder;
        builder.BeginList();
        for (auto&& r : rows) {
            builder.AddListItem().BeginStruct().AddMember("id").Int32(r.Id).AddMember(columnName).Int64(r.IntVal).AddMember("b");
            if (r.B.has_value()) {
                builder.BeginOptional().Bool(*r.B).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Bool);
            }

            builder.EndStruct();
        }

        builder.EndList();
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void BulkUpsertRowTableYdbValue(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        BulkUpsertRowTableYdbValueWithColumnName(helper, name, rows, "int");
    }

    void BulkUpsertRowTableYdbValueWithSecondColumn(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& secondName) {
        TValueBuilder builder;
        builder.BeginList();
        for (auto&& r : rows) {
            builder.AddListItem().BeginStruct().AddMember("id").Int32(r.Id).AddMember(secondName).Int64(r.IntVal).AddMember("b");
            if (r.B.has_value()) {
                builder.BeginOptional().Bool(*r.B).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Bool);
            }

            builder.EndStruct();
        }

        builder.EndList();
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void BulkUpsertRowTableCSV(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        TStringBuilder builder;
        for (auto&& r : rows) {
            builder << r.Id << "," << r.IntVal << ",";
            if (r.B.has_value()) {
                builder << (*r.B ? "true" : "false");
            }

            builder << '\n';
        }

        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void BulkUpsertRowTableCSVWithFormat(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& trueValue, const TString& falseValue) {
        TStringBuilder builder;
        for (auto&& r : rows) {
            builder << r.Id << "," << r.IntVal << ",";
            if (r.B.has_value()) {
                builder << (*r.B ? trueValue : falseValue);
            }

            builder << '\n';
        }

        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithColumnName(const TVector<TRow>& rows, const TString& columnName) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> vals;
        std::vector<std::optional<bool>> bs;
        ids.reserve(rows.size());
        vals.reserve(rows.size());
        bs.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            vals.push_back(r.IntVal);
            bs.push_back(r.B);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(vals);
        auto boolArr = MakeBoolArrayAsUInt8Nullable(bs);
        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field(columnName, arrow::int64()),
            arrow::field("b", arrow::uint8())
        });

        return MakeBatch({ schema->field(0), schema->field(1), schema->field(2) }, { idArr, intArr, boolArr });
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        return MakeArrowBatchWithColumnName(rows, "int");
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithSecondColumn(const TVector<TRow>& rows, const TString& secondName) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> seconds;
        std::vector<std::optional<bool>> bs;
        ids.reserve(rows.size());
        seconds.reserve(rows.size());
        bs.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            seconds.push_back(r.IntVal);
            bs.push_back(r.B);
        }

        auto idArr = MakeInt32Array(ids);
        auto secondArr = MakeInt64Array(seconds);
        auto boolArr = MakeBoolArrayAsUInt8Nullable(bs);
        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field(secondName, arrow::int64()),
            arrow::field("b", arrow::uint8())
        });

        return MakeBatch({ schema->field(0), schema->field(1), schema->field(2) }, { idArr, secondArr, boolArr });
    }

    void BulkUpsertRowTableArrow(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        auto batch = MakeArrowBatch(rows);
        TString strBatch = NArrow::SerializeBatchNoCompression(batch);
        TString strSchema = NArrow::SerializeSchema(*batch->schema());
        auto result =
            helper.GetKikimr().GetTableClient().BulkUpsert(name, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void BulkUpsertRowTableArrowWithSecondColumn(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& secondName) {
        auto batch = MakeArrowBatchWithSecondColumn(rows, secondName);
        TString strBatch = NArrow::SerializeBatchNoCompression(batch);
        TString strSchema = NArrow::SerializeSchema(*batch->schema());
        auto result =
            helper.GetKikimr().GetTableClient().BulkUpsert(name, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void LoadData(TTestHelper& helper, ETableKind table, ELoadKind load, const TString& name, const TVector<TRow>& rows,
        TTestHelper::TColumnTable* col = nullptr, const TVector<TTestHelper::TColumnSchema>* schema = nullptr) {
        switch (table) {
            case ETableKind::COLUMNSHARD: {
                Y_ABORT_UNLESS(col && schema);
                if (load == ELoadKind::ARROW) {
                    TString columnName = "int";
                    if (schema->size() >= 2) {
                        columnName = (*schema)[1].GetName();
                    }

                    auto batch = MakeArrowBatchWithColumnName(rows, columnName);
                    helper.BulkUpsert(*col, batch);
                } else if (load == ELoadKind::YDB_VALUE) {
                    TString columnName = "int";
                    if (schema->size() >= 2) {
                        columnName = (*schema)[1].GetName();
                    }

                    BulkUpsertRowTableYdbValueWithColumnName(helper, name, rows, columnName);
                } else {
                    BulkUpsertRowTableCSV(helper, name, rows);
                }

                break;
            }
            case ETableKind::DATASHARD: {
                if (load == ELoadKind::ARROW) {
                    BulkUpsertRowTableArrow(helper, name, rows);
                } else if (load == ELoadKind::YDB_VALUE) {
                    BulkUpsertRowTableYdbValue(helper, name, rows);
                } else {
                    BulkUpsertRowTableCSV(helper, name, rows);
                }

                break;
            }
        }
    }

    void CheckOrExec(TTestHelper& helper, const TString& query, const TString& expected, EQueryMode scanMode) {
        if (scanMode == EQueryMode::SCAN_QUERY) {
            helper.ReadData(query, expected);
        } else {
            helper.ReadDataExecQuery(query, expected);
        }
    }

    void PrepareBase(TTestHelper& helper, ETableKind tableKind, const TString& tableName, TTestHelper::TColumnTable* colTableOut,
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
            helper.CreateTable(col);
            *colTableOut = col;
        } else {
            CreateDataShardTable(helper, tableName);
        }
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
            CreateDataShardTable(helper, tableName);
            return true;
        }
    }
    }   // namespace

    class TBoolTestCase {
    public:
        TBoolTestCase()
            : TestHelper(CreateKikimrSettingsWithBoolSupport()) {
        }

        TTestHelper::TUpdatesBuilder Inserter() {
            return TTestHelper::TUpdatesBuilder(TestTable.GetArrowSchema(Schema));
        }

        void Upsert(TTestHelper::TUpdatesBuilder& inserter) {
            TestHelper.BulkUpsert(TestTable, inserter);
        }

        void CheckQuery(const TString& query, const TString& expected, EQueryMode mode = EQueryMode::SCAN_QUERY) const {
            switch (mode) {
                case EQueryMode::SCAN_QUERY:
                    TestHelper.ReadData(query, expected);
                    break;
                case EQueryMode::EXECUTE_QUERY: {
                    TestHelper.ExecuteQuery(query);
                    break;
                }
            }
        }

        void ExecuteDataQuery(const TString& query) const {
            TestHelper.ExecuteQuery(query);
        }

        void PrepareTable1() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            TestTable.SetName("/Root/Table1").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(4).Add(true);
                inserter.AddRow().Add(2).Add(3).Add(false);
                Upsert(inserter);
            }
            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(4).Add(1).Add(true);
                inserter.AddRow().Add(3).Add(2).Add(true);

                Upsert(inserter);
            }
        }

        void PrepareTable2() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool),
            };

            TestTable.SetName("/Root/Table2").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(1).Add(true);
                inserter.AddRow().Add(2).Add(1).Add(false);
                inserter.AddRow().Add(3).Add(2).Add(true);
                inserter.AddRow().Add(4).Add(2).Add(false);
                Upsert(inserter);
            }
        }

        void PrepareTable3() {
            Schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Bool).SetNullable(false),
            };

            TestTable.SetName("/Root/Table3").SetPrimaryKey({ "b" }).SetSchema(Schema);
            TestHelper.CreateTable(TestTable);

            {
                TTestHelper::TUpdatesBuilder inserter = Inserter();
                inserter.AddRow().Add(1).Add(true);
                inserter.AddRow().Add(2).Add(false);
                Upsert(inserter);
            }
        }

    private:
        TTestHelper TestHelper;
        TVector<TTestHelper::TColumnSchema> Schema;
        TTestHelper::TColumnTable TestTable;
    };

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 4, 1, true }, { 3, 2, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=1", "[[[%true];1;[4]]]", Scan);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` order by id", "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestFilterEqual, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 4, 1, true }, { 3, 2, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b == true", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b != true order by id", "[[[%false];2;[3]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestFilterNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true }, { 5, 5, std::nullopt }, { 6, 6, std::nullopt } }, &col, &schema);
        const TString expectedNulls = "[[#;5;[5]];[#;6;[6]]]";
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b is NULL order by id", expectedNulls, Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b is not NULL order by id",
            "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestFilterCompare, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b < true order by id", "[[[%false];2;[3]]]", Scan);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` WHERE b > false order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b <= true order by id",
            "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` WHERE b >= true order by id", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestOrderByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } }, &col, &schema);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` order by b, id", "[[[%false];2;[3]];[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestGroupByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true }, { 5, 12, true }, { 6, 30, false } }, &col, &schema);
        CheckOrExec(helper, "SELECT b, count(*) FROM `/Root/Table1` group by b order by b", "[[[%false];2u];[[%true];4u]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestAggregation, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT min(b) FROM `/Root/Table1`", "[[[%false]]]", Scan);
        CheckOrExec(helper, "SELECT max(b) FROM `/Root/Table1`", "[[[%true]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
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
            CreateDataShardTable(helper, t1);
            CreateDataShardTableWithSecondColumn(helper, t2, "table1_id");
        }

        LoadData(helper, Table, Load, t1, { { 1, 4, true }, { 2, 3, true } }, &col1, &s1);
        if (Table == ETableKind::COLUMNSHARD) {
            LoadData(helper, Table, Load, t2, { { 1, 1, true }, { 2, 1, false }, { 3, 2, true }, { 4, 2, false } }, &col2, &s2);
        } else {
            if (Load == ELoadKind::ARROW) {
                BulkUpsertRowTableArrowWithSecondColumn(
                    helper, t2, { { 1, 1, true }, { 2, 1, false }, { 3, 2, true }, { 4, 2, false } }, "table1_id");
            } else if (Load == ELoadKind::YDB_VALUE) {
                BulkUpsertRowTableYdbValueWithSecondColumn(
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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestJoinByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
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
            CreateDataShardTable(helper, t1);
            CreateDataShardTable(helper, t2);
        }

        LoadData(helper, Table, Load, t1, { { 2, 3, true }, { 4, 1, true } }, &col1, &s1);
        LoadData(helper, Table, Load, t2, { { 2, 2, false }, { 4, 4, false }, { 1, 1, true }, { 3, 3, true } }, &col2, &s2);
        CheckOrExec(helper,
            "SELECT t1.id, t2.id, t1.b FROM `/Root/Table1` as t1 join `/Root/Table2` as t2 on t1.b = t2.b order by t1.id, t2.id, t1.b",
            R"([[2;1;[%true]];[2;3;[%true]];[4;1;[%true]];[4;3;[%true]]])", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestCSVBoolFormats, EQueryMode, ETableKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();

        const TString tableName = "/Root/Table1";
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestFeatureFlag, EQueryMode, ETableKind) {
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
            PrepareBase(helperDisabled, Table, tableName, &col, &schema);
            LoadData(helperDisabled, Table, ELoadKind::ARROW, tableName, rows, &col, &schema);
            CheckOrExec(
                helperDisabled, "SELECT id, int, b FROM `" + tableName + "` ORDER BY id", "[[1;[100];[%true]];[2;[200];[%false]]]", Scan);
        } else {
            bool tableCreated = TryPrepareBase(helperDisabled, Table, tableName, &col, &schema);
            if (tableCreated) {
                try {
                    LoadData(helperDisabled, Table, ELoadKind::ARROW, tableName, rows, &col, &schema);
                    UNIT_ASSERT_C(false, "Expected error for ColumnShard with disabled feature flag");
                } catch (const std::exception& e) {
                    TString errorMsg = e.what();
                    UNIT_ASSERT_C(errorMsg.find("EnableColumnshardBool") != TString::npos || errorMsg.find("bool") != TString::npos,
                        "Expected error about bool support, got: " + errorMsg);
                }
            }
        }

        TTestHelper helperEnabled(CreateKikimrSettingsWithBoolSupport());
        TTestHelper::TColumnTable col2;
        TVector<TTestHelper::TColumnSchema> schema2;
        PrepareBase(helperEnabled, Table, tableName, &col2, &schema2);
        LoadData(helperEnabled, Table, ELoadKind::ARROW, tableName, rows, &col2, &schema2);
        CheckOrExec(helperEnabled, "SELECT id, int, b FROM `" + tableName + "` ORDER BY id", "[[1;[100];[%true]];[2;[200];[%false]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolAsPrimaryKey, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto createAndFill = [&](const TString& name,
                                 const TVector<TTestHelper::TColumnSchema>& schema,
                                 const TVector<TString>& pkColumns,
                                 const TVector<TRow>& rows,
                                 const TString& selectExpr,
                                 const TString& expectedScan) -> decltype(auto) {
            TTestHelper helper(CreateKikimrSettingsWithBoolSupport());
            TTestHelper::TColumnTable col;
            col.SetName(name).SetPrimaryKey(pkColumns).SetSharding(pkColumns).SetSchema(schema);
            helper.CreateTable(col);

            if (schema.size() == 1) {
                if (Load == ELoadKind::ARROW) {
                    using namespace NKikimr::NKqp::NTestArrow;
                    std::vector<bool> bs; bs.reserve(rows.size());
                    for (auto&& r : rows) bs.push_back(r.B.value());
                    auto bArr = MakeBoolArrayAsUInt8(bs);
                    auto batch = MakeBatch({ arrow::field("b", arrow::uint8(), /*nullable*/ false) }, { bArr });
                    helper.BulkUpsert(col, batch);
                } else if (Load == ELoadKind::YDB_VALUE) {
                    TValueBuilder builder;
                    builder.BeginList();
                    for (auto&& r : rows) {
                        builder.AddListItem().BeginStruct().AddMember("b").Bool(r.B.value()).EndStruct();
                    }

                    builder.EndList();
                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                } else {
                    TStringBuilder csv;
                    for (auto&& r : rows) {
                        csv << (r.B.value() ? "true" : "false") << "\n";
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
                    for (auto&& r : rows) { ids.push_back(r.Id); bools.push_back(r.B.value() ? 1u : 0u); ints.push_back(r.IntVal); }
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
                            .AddMember("b").Bool(r.B.value())
                            .AddMember("int").Int64(r.IntVal)
                            .EndStruct();
                    }

                    builder.EndList();
                    auto res = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
                    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
                } else {
                    TStringBuilder csv;
                    for (auto&& r : rows) {
                        csv << r.Id << "," << (r.B.value() ? "true" : "false") << "," << r.IntVal << "\n";
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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        auto settings = CreateKikimrSettingsWithBoolSupport();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(settings);

        const TString ds = "/Root/RowSrc";
        const TString cs = "/Root/ColSrc";

        CreateDataShardTable(helper, ds);

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolOperatorsAndAggregations, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestOrderByBoolWithLimit, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolCompare, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolFilterWithColumns, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();
        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolWriteComparison, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolNot, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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

    Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(TestBoolGroupByCounts, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TTestHelper helper(CreateKikimrSettingsWithBoolSupport());

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
