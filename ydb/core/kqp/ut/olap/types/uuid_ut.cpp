#include "bool_test_enums.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <yql/essentials/types/uuid/uuid.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpUuidColumnShard) {
    namespace {
    struct TRow {
        i32 Id;
        i64 IntVal;
        std::optional<TString> Uid;  // UUID string form like "550e8400-e29b-41d4-a716-446655440000"
    };

    void CreateDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    int Int64,
                    uid Uuid,
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
                    uid Uuid,
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
            builder.AddListItem().BeginStruct().AddMember("id").Int32(r.Id).AddMember(columnName).Int64(r.IntVal).AddMember("uid");
            if (r.Uid.has_value()) {
                builder.BeginOptional().Uuid(TUuidValue(std::string(*r.Uid))).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Uuid);
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
            builder.AddListItem().BeginStruct().AddMember("id").Int32(r.Id).AddMember(secondName).Int64(r.IntVal).AddMember("uid");
            if (r.Uid.has_value()) {
                builder.BeginOptional().Uuid(TUuidValue(std::string(*r.Uid))).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Uuid);
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
            if (r.Uid.has_value()) {
                builder << *r.Uid;
            }

            builder << '\n';
        }

        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

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

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithColumnName(const TVector<TRow>& rows, const TString& columnName) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> vals;
        std::vector<std::optional<TString>> uuids;
        ids.reserve(rows.size());
        vals.reserve(rows.size());
        uuids.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            vals.push_back(r.IntVal);
            uuids.push_back(r.Uid);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(vals);
        auto uuidArr = MakeUuidArrayNullable(uuids);
        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field(columnName, arrow::int64()),
            arrow::field("uid", arrow::fixed_size_binary(16))
        });

        return MakeBatch({ schema->field(0), schema->field(1), schema->field(2) }, { idArr, intArr, uuidArr });
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        return MakeArrowBatchWithColumnName(rows, "int");
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithSecondColumn(const TVector<TRow>& rows, const TString& secondName) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> seconds;
        std::vector<std::optional<TString>> uuids;
        ids.reserve(rows.size());
        seconds.reserve(rows.size());
        uuids.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            seconds.push_back(r.IntVal);
            uuids.push_back(r.Uid);
        }

        auto idArr = MakeInt32Array(ids);
        auto secondArr = MakeInt64Array(seconds);
        auto uuidArr = MakeUuidArrayNullable(uuids);
        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field(secondName, arrow::int64()),
            arrow::field("uid", arrow::fixed_size_binary(16))
        });

        return MakeBatch({ schema->field(0), schema->field(1), schema->field(2) }, { idArr, secondArr, uuidArr });
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
                TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid),
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

    // Helper: UUID string literal for use in SQL
    // UUID in YQL: Uuid("550e8400-e29b-41d4-a716-446655440000")
    // Also works: CAST("..." AS Uuid)

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
    }   // namespace

    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
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

    Y_UNIT_TEST(TestFilterEqual, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID1 }, { 2, 0, UUID4 }, { 3, 0, UUID5 } }, &col, &schema);

        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid = CAST("550e8400-e29b-41d4-a716-446655440000" AS Uuid))",
            "[[1]]", Scan);
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid != CAST("550e8400-e29b-41d4-a716-446655440000" AS Uuid) ORDER BY id)",
            "[[2];[3]]", Scan);
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid = CAST("660e8400-e29b-41d4-a716-446655440000" AS Uuid))",
            "[[2]]", Scan);
    }

    Y_UNIT_TEST(TestFilterNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID1 }, { 2, 0, std::nullopt }, { 3, 0, UUID5 }, { 4, 0, std::nullopt } }, &col, &schema);

        CheckOrExec(helper, "SELECT id FROM `/Root/Table1` WHERE uid IS NULL ORDER BY id",
            "[[2];[4]]", Scan);
        CheckOrExec(helper, "SELECT id FROM `/Root/Table1` WHERE uid IS NOT NULL ORDER BY id",
            "[[1];[3]]", Scan);
    }

    Y_UNIT_TEST(TestFilterCompare, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID_A }, { 2, 0, UUID_B }, { 3, 0, UUID_C }, { 4, 0, UUID_D } }, &col, &schema);

        // Less than
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid < CAST("33333333-3333-3333-3333-333333333333" AS Uuid) ORDER BY id)",
            "[[1];[2]]", Scan);

        // Greater than
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid > CAST("22222222-2222-2222-2222-222222222222" AS Uuid) ORDER BY id)",
            "[[3];[4]]", Scan);

        // Less than or equal
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid <= CAST("22222222-2222-2222-2222-222222222222" AS Uuid) ORDER BY id)",
            "[[1];[2]]", Scan);

        // Greater than or equal
        CheckOrExec(helper,
            R"(SELECT id FROM `/Root/Table1` WHERE uid >= CAST("33333333-3333-3333-3333-333333333333" AS Uuid) ORDER BY id)",
            "[[3];[4]]", Scan);
    }

    Y_UNIT_TEST(TestOrderBy, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 10, UUID_C }, { 2, 20, UUID_A }, { 3, 30, UUID_B } }, &col, &schema);

        // ASC order
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` ORDER BY uid",
            R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3];[["33333333-3333-3333-3333-333333333333"];1]])", Scan);

        // DESC order
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` ORDER BY uid DESC",
            R"([[["33333333-3333-3333-3333-333333333333"];1];[["22222222-2222-2222-2222-222222222222"];3];[["11111111-1111-1111-1111-111111111111"];2]])", Scan);
    }

    Y_UNIT_TEST(TestGroupBy, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID_D }, { 2, 0, UUID_E }, { 3, 0, UUID_D }, { 4, 0, UUID_E }, { 5, 0, UUID1 } }, &col, &schema);

        CheckOrExec(helper,
            "SELECT uid, count(*) AS cnt FROM `/Root/Table1` GROUP BY uid ORDER BY uid",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1u];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2u];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];2u]])", Scan);
    }

    Y_UNIT_TEST(TestAggregation, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID_A }, { 2, 0, UUID_C }, { 3, 0, UUID_B }, { 4, 0, std::nullopt } }, &col, &schema);

        CheckOrExec(helper,
            "SELECT min(uid) FROM `/Root/Table1`",
            R"([[["11111111-1111-1111-1111-111111111111"]]])", Scan);
        CheckOrExec(helper,
            "SELECT max(uid) FROM `/Root/Table1`",
            R"([[["33333333-3333-3333-3333-333333333333"]]])", Scan);
        CheckOrExec(helper,
            "SELECT count(uid) FROM `/Root/Table1`",
            "[[3u]]", Scan);
        CheckOrExec(helper,
            "SELECT count(*) FROM `/Root/Table1`",
            "[[4u]]", Scan);
    }

    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
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
            CreateDataShardTable(helper, t1);
            CreateDataShardTableWithSecondColumn(helper, t2, "table1_id");
        }

        LoadData(helper, Table, Load, t1, { { 1, 100, UUID1 }, { 2, 200, UUID4 } }, &col1, &s1);
        if (Table == ETableKind::COLUMNSHARD) {
            LoadData(helper, Table, Load, t2, { { 1, 1, UUID_D }, { 2, 1, UUID_E }, { 3, 2, UUID_A } }, &col2, &s2);
        } else {
            if (Load == ELoadKind::ARROW) {
                BulkUpsertRowTableArrowWithSecondColumn(
                    helper, t2, { { 1, 1, UUID_D }, { 2, 1, UUID_E }, { 3, 2, UUID_A } }, "table1_id");
            } else if (Load == ELoadKind::YDB_VALUE) {
                BulkUpsertRowTableYdbValueWithSecondColumn(
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

    Y_UNIT_TEST(TestJoinByUuid, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
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
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid),
            };

            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            CreateDataShardTable(helper, t1);
            CreateDataShardTable(helper, t2);
        }

        LoadData(helper, Table, Load, t1, { { 1, 10, UUID_D }, { 2, 20, UUID_E } }, &col1, &s1);
        LoadData(helper, Table, Load, t2, { { 1, 100, UUID_E }, { 2, 200, UUID_D }, { 3, 300, UUID_A } }, &col2, &s2);

        CheckOrExec(helper,
            R"(SELECT t1.id, t2.id, t1.uid FROM `/Root/Table1` AS t1 JOIN `/Root/Table2` AS t2 ON t1.uid = t2.uid ORDER BY t1.id, t2.id)",
            R"([[1;2;["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]];[2;1;["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"]]])", Scan);
    }

    Y_UNIT_TEST(TestOrderByWithLimit, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID_C }, { 2, 0, UUID_A }, { 3, 0, UUID_B }, { 4, 0, UUID_D } }, &col, &schema);

        // ASC LIMIT 2
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` ORDER BY uid LIMIT 2",
            R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3]])", Scan);

        // DESC LIMIT 1
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` ORDER BY uid DESC LIMIT 1",
            R"([[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];4]])", Scan);

        // ASC LIMIT 3
        CheckOrExec(helper,
            "SELECT uid, id FROM `/Root/Table1` ORDER BY uid LIMIT 3",
            R"([[["11111111-1111-1111-1111-111111111111"];2];[["22222222-2222-2222-2222-222222222222"];3];[["33333333-3333-3333-3333-333333333333"];1]])", Scan);
    }

    Y_UNIT_TEST(TestGroupByWithNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName,
            { { 1, 0, UUID_D }, { 2, 0, std::nullopt }, { 3, 0, UUID_D }, { 4, 0, std::nullopt }, { 5, 0, UUID_E } }, &col, &schema);

        CheckOrExec(helper,
            "SELECT uid, count(*) AS cnt FROM `/Root/Table1` GROUP BY uid ORDER BY uid",
            R"([[#;2u];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2u];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];1u]])", Scan);
    }

    Y_UNIT_TEST(TestUuidAsPrimaryKey, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("uid").SetType(NScheme::NTypeIds::Uuid).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"uid"}).SetSharding({"uid"}).SetSchema(schema);
        helper.CreateTable(testTable);

        if (Load == ELoadKind::ARROW) {
            std::vector<std::optional<TString>> uuids = { UUID_A, UUID_B, UUID_C };
            auto uuidArr = MakeUuidArrayNullable(uuids);
            using namespace NKikimr::NKqp::NTestArrow;
            auto valArr = MakeInt64Array({ (int64_t)1, (int64_t)2, (int64_t)3 });
            auto batch = MakeBatch(
                { arrow::field("uid", arrow::fixed_size_binary(16), /*nullable*/ false),
                  arrow::field("val", arrow::int64()) },
                { uuidArr, valArr }
            );
            helper.BulkUpsert(testTable, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_A))).AddMember("val").Int64(1).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_B))).AddMember("val").Int64(2).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("uid").Uuid(TUuidValue(std::string(UUID_C))).AddMember("val").Int64(3).EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "11111111-1111-1111-1111-111111111111,1" << Endl;
            csv << "22222222-2222-2222-2222-222222222222,2" << Endl;
            csv << "33333333-3333-3333-3333-333333333333,3" << Endl;
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        CheckOrExec(helper,
            R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("22222222-2222-2222-2222-222222222222" AS Uuid))",
            "[[[2]]]", Scan);
        CheckOrExec(helper,
            R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("11111111-1111-1111-1111-111111111111" AS Uuid))",
            "[[[1]]]", Scan);
        CheckOrExec(helper,
            R"(SELECT val FROM `/Root/ColumnTableTest` WHERE uid = CAST("33333333-3333-3333-3333-333333333333" AS Uuid))",
            "[[[3]]]", Scan);
    }

    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
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

        // UPDATE
        helper.ExecuteQuery(
            R"(UPDATE `)" + cs + R"(` SET uid = CAST("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" AS Uuid) WHERE id = 2)");

        CheckOrExec(helper,
            "SELECT uid, id FROM `" + cs + "` WHERE id = 2",
            R"([[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2]])", Scan);

        // DELETE
        helper.ExecuteQuery(
            "DELETE FROM `" + cs + "` WHERE id = 3");

        CheckOrExec(helper,
            "SELECT id FROM `" + cs + "` ORDER BY id",
            "[[1];[2]]", Scan);

        // UPSERT additional row
        helper.ExecuteQuery(
            R"(UPSERT INTO `)" + cs + R"(` (id, uid, val) VALUES
                (4, CAST("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" AS Uuid), 40))");

        CheckOrExec(helper,
            "SELECT uid, id, val FROM `" + cs + "` ORDER BY id",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1;[10]];[["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"];2;[20]];[["bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"];4;[40]]])", Scan);
    }

    Y_UNIT_TEST(TestCsv, EQueryMode, ETableKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();

        const TString tableName = "/Root/Table1";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);

        {
            TStringBuilder builder;
            builder << "1,0,550e8400-e29b-41d4-a716-446655440000" << Endl;
            builder << "2,0,660e8400-e29b-41d4-a716-446655440000" << Endl;
            builder << "3,0,770e8400-e29b-41d4-a716-446655440000" << Endl;
            const auto result = helper.GetKikimr().GetTableClient().BulkUpsert(
                tableName, EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT id FROM `" + tableName + "` ORDER BY id", "[[1];[2];[3]]", Scan);
        CheckOrExec(helper,
            "SELECT uid, id FROM `" + tableName + "` ORDER BY id",
            R"([[["550e8400-e29b-41d4-a716-446655440000"];1];[["660e8400-e29b-41d4-a716-446655440000"];2];[["770e8400-e29b-41d4-a716-446655440000"];3]])", Scan);
    }
}

} // namespace NKqp
} // namespace NKikimr
