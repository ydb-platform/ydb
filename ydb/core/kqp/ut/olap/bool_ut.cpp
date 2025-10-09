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

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/uuid/uuid.h>

#define Y_UNIT_TEST_ALL_ENUM_VALUES(N, ENUM_TYPE1, ENUM_TYPE2, ENUM_TYPE3) \
    struct TTestCase##N: public TCurrentTestCase { \
        ENUM_TYPE1 Arg0; \
        ENUM_TYPE2 Arg1; \
        ENUM_TYPE3 Arg2; \
        TString ParametrizedTestName; \
        TTestCase##N(ENUM_TYPE1 arg0, ENUM_TYPE2 arg1, ENUM_TYPE3 arg2) \
            : TCurrentTestCase() \
            , Arg0(arg0) \
            , Arg1(arg1) \
            , Arg2(arg2) \
            , ParametrizedTestName(#N "-" + ToString(Arg0) + "-" + ToString(Arg1) + "-" + ToString(Arg2)) { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
        static THolder<NUnitTest::TBaseTestCase> Create(ENUM_TYPE1 arg0, ENUM_TYPE2 arg1, ENUM_TYPE3 arg2) { \
            return ::MakeHolder<TTestCase##N>(arg0, arg1, arg2); \
        } \
        void Execute_(NUnitTest::TTestContext&) override; \
    }; \
    struct TTestRegistration##N { \
        TTestRegistration##N() { \
            const auto enumItems1 = GetEnumAllValues<ENUM_TYPE1>(); \
            const auto enumItems2 = GetEnumAllValues<ENUM_TYPE2>(); \
            const auto enumItems3 = GetEnumAllValues<ENUM_TYPE3>(); \
            for (auto&& item1 : enumItems1) { \
                for (auto&& item2 : enumItems2) { \
                    for (auto&& item3 : enumItems3) { \
                        TCurrentTest::AddTest([item1, item2, item3] { \
                            return TTestCase##N::Create(item1, item2, item3); \
                        }); \
                    } \
                } \
            } \
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

    void BulkUpsertRowTableYdbValue(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        TValueBuilder builder;
        builder.BeginList();
        for (auto&& r : rows) {
            builder.AddListItem().BeginStruct().AddMember("id").Int32(r.Id).AddMember("int").Int64(r.IntVal).AddMember("b");
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

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        arrow::Int32Builder idBuilder;
        arrow::Int64Builder intBuilder;
        arrow::BooleanBuilder boolBuilder;
        for (auto&& r : rows) {
            Y_ABORT_UNLESS(idBuilder.Append(r.Id).ok());
            Y_ABORT_UNLESS(intBuilder.Append(r.IntVal).ok());
            if (r.B.has_value()) {
                Y_ABORT_UNLESS(boolBuilder.Append(*r.B).ok());
            } else {
                Y_ABORT_UNLESS(boolBuilder.AppendNull().ok());
            }
        }

        std::shared_ptr<arrow::Array> idArr;
        Y_ABORT_UNLESS(idBuilder.Finish(&idArr).ok());
        std::shared_ptr<arrow::Array> intArr;
        Y_ABORT_UNLESS(intBuilder.Finish(&intArr).ok());
        std::shared_ptr<arrow::Array> boolArr;
        Y_ABORT_UNLESS(boolBuilder.Finish(&boolArr).ok());
        auto schema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false), arrow::field("int", arrow::int64()),
            arrow::field("b", arrow::boolean()) });
        return arrow::RecordBatch::Make(schema, rows.size(), { idArr, intArr, boolArr });
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithSecondColumn(const TVector<TRow>& rows, const TString& secondName) {
        arrow::Int32Builder idBuilder;
        arrow::Int64Builder secondBuilder;
        arrow::BooleanBuilder boolBuilder;
        for (auto&& r : rows) {
            Y_ABORT_UNLESS(idBuilder.Append(r.Id).ok());
            Y_ABORT_UNLESS(secondBuilder.Append(r.IntVal).ok());
            if (r.B.has_value()) {
                Y_ABORT_UNLESS(boolBuilder.Append(*r.B).ok());
            } else {
                Y_ABORT_UNLESS(boolBuilder.AppendNull().ok());
            }
        }

        std::shared_ptr<arrow::Array> idArr;
        Y_ABORT_UNLESS(idBuilder.Finish(&idArr).ok());
        std::shared_ptr<arrow::Array> secondArr;
        Y_ABORT_UNLESS(secondBuilder.Finish(&secondArr).ok());
        std::shared_ptr<arrow::Array> boolArr;
        Y_ABORT_UNLESS(boolBuilder.Finish(&boolArr).ok());
        auto schema = arrow::schema({ arrow::field("id", arrow::int32(), /*nullable*/ false), arrow::field(secondName, arrow::int64()),
            arrow::field("b", arrow::boolean()) });
        return arrow::RecordBatch::Make(schema, rows.size(), { idArr, secondArr, boolArr });
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
            case ETableKind::COLUMN_SHARD: {
                Y_ABORT_UNLESS(col && schema);
                if (load == ELoadKind::ARROW) {
                    auto batch = MakeArrowBatch(rows);
                    helper.BulkUpsert(*col, batch);
                } else if (load == ELoadKind::YDB_VALUE) {
                    BulkUpsertRowTableYdbValue(helper, name, rows);
                } else {
                    BulkUpsertRowTableCSV(helper, name, rows);
                }

                break;
            }
            case ETableKind::DATA_SHARD: {
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
            helper.ExecuteQuery(query);
        }
    }

    void PrepareBase(TTestHelper& helper, ETableKind tableKind, const TString& tableName, TTestHelper::TColumnTable* colTableOut,
        TVector<TTestHelper::TColumnSchema>* schemaOut) {
        if (tableKind == ETableKind::COLUMN_SHARD) {
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
    }   // namespace

    class TBoolTestCase {
    public:
        TBoolTestCase()
            : TestHelper(TKikimrSettings().SetWithSampleTables(false)) {
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

    private:
        TTestHelper TestHelper;
        TVector<TTestHelper::TColumnSchema> Schema;
        TTestHelper::TColumnTable TestTable;
    };

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 4, 1, true }, { 3, 2, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=1", "[[[%true];1;[4]]]", Scan);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` order by id", "[[[%true];1;[4]];[[%false];2;[3]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestFilterEqual, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 4, 1, true }, { 3, 2, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b == true", "[[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE b != true order by id", "[[[%false];2;[3]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestFilterNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
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

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestFilterCompare, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
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

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestOrderByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } }, &col, &schema);
        CheckOrExec(
            helper, "SELECT * FROM `/Root/Table1` order by b, id", "[[[%false];2;[3]];[[%true];1;[4]];[[%true];3;[2]];[[%true];4;[1]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestGroupByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true }, { 5, 12, true }, { 6, 30, false } }, &col, &schema);
        CheckOrExec(helper, "SELECT b, count(*) FROM `/Root/Table1` group by b order by b", "[[[%false];2u];[[%true];4u]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestAggregation, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString tableName = "/Root/Table1";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, { { 1, 4, true }, { 2, 3, false }, { 3, 2, true }, { 4, 1, true } }, &col, &schema);
        CheckOrExec(helper, "SELECT min(b) FROM `/Root/Table1`", "[[[%false]]]", Scan);
        CheckOrExec(helper, "SELECT max(b) FROM `/Root/Table1`", "[[[%true]]]", Scan);
    }

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMN_SHARD) {
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
        if (Table == ETableKind::COLUMN_SHARD) {
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

    Y_UNIT_TEST_ALL_ENUM_VALUES(TestJoinByBool, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg0;
        const auto Table = Arg1;
        const auto Load = Arg2;
        if (Table == ETableKind::COLUMN_SHARD) {
            return;   // skip until bool is supported in columnshard
        }

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;
        if (Table == ETableKind::COLUMN_SHARD) {
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
}

}   // namespace NKqp
}   // namespace NKikimr
