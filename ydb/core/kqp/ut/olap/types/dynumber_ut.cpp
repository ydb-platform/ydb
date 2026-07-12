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

#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpDyNumberColumnShard) {
    namespace {

    struct TRow {
        i32 Id;
        i64 IntVal;
        std::optional<TString> Dyn;
    };

    static TString MakeDyNumber(const TStringBuf& str) {
        auto result = NDyNumber::ParseDyNumberString(str);
        Y_ABORT_UNLESS(result.Defined(), "Failed to parse DyNumber: %s", TString(str).c_str());
        return TString(str);
    }

    static void AppendDyNumber(arrow::BinaryBuilder& builder, const TStringBuf& str) {
        Y_ABORT_UNLESS(builder.Append(str.data(), str.size()).ok());
    }

    void CreateDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    int Int64,
                    dyn DyNumber,
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
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(r.Id)
                .AddMember("int").Int64(r.IntVal)
                .AddMember("dyn");
            if (r.Dyn.has_value()) {
                builder.BeginOptional().DyNumber(*r.Dyn).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::DyNumber);
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
            if (r.Dyn.has_value()) {
                builder << *r.Dyn;
            }
            builder << '\n';
        }
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> vals;
        ids.reserve(rows.size());
        vals.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            vals.push_back(r.IntVal);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(vals);

        arrow::BinaryBuilder dynBuilder;
        for (auto&& r : rows) {
            if (r.Dyn.has_value()) {
                Y_ABORT_UNLESS(dynBuilder.Append(r.Dyn->data(), r.Dyn->size()).ok());
            } else {
                Y_ABORT_UNLESS(dynBuilder.AppendNull().ok());
            }
        }
        std::shared_ptr<arrow::Array> dynArr;
        Y_ABORT_UNLESS(dynBuilder.Finish(&dynArr).ok());

        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field("int", arrow::int64()),
            arrow::field("dyn", arrow::binary())
        });

        return arrow::RecordBatch::Make(schema, rows.size(), { idArr, intArr, dynArr });
    }

    void BulkUpsertRowTableArrow(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        auto batch = MakeArrowBatch(rows);
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
                    auto batch = MakeArrowBatch(rows);
                    helper.BulkUpsert(*col, batch);
                } else if (load == ELoadKind::YDB_VALUE) {
                    BulkUpsertRowTableYdbValue(helper, name, rows);
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
                TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
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

    // ---- 1. Basic read/write with positive, negative, zero, null values ----
    Y_UNIT_TEST(TestSimpleQueries, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("3.14") },
            { 2, 20, MakeDyNumber("-100") },
            { 3, 30, std::nullopt },
            { 4, 40, MakeDyNumber("0") },
            { 5, 50, MakeDyNumber("1") },
            { 6, 60, MakeDyNumber("-5") }
        }, &col, &schema);

        // Select non-DyNumber columns to verify basic storage
        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` WHERE id=1", "[[1;[10]]]", Scan);
        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` WHERE id=3", "[[3;[30]]]", Scan);

        // Select all rows ordered by id, without the DyNumber column
        CheckOrExec(helper, "SELECT id, int FROM `" + tableName + "` ORDER BY id",
            "[[1;[10]];[2;[20]];[3;[30]];[4;[40]];[5;[50]];[6;[60]]]", Scan);

        // Select DyNumber column to verify normalized format output
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=1", "[[[\".314e1\"]]]", Scan);
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=2", "[[[\"-.1e3\"]]]", Scan);
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=3", "[[#]]", Scan);
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=4", "[[[\"0\"]]]", Scan);
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=5", "[[[\".1e1\"]]]", Scan);
        CheckOrExec(helper, "SELECT dyn FROM `" + tableName + "` WHERE id=6", "[[[\"-.5e1\"]]]", Scan);
    }

    // ---- 2. WHERE dyn = CAST("..." AS DyNumber), != tests ----
    Y_UNIT_TEST(TestFilterEqual, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("1") },
            { 2, 20, MakeDyNumber("2") },
            { 3, 30, MakeDyNumber("3.14") },
            { 4, 40, MakeDyNumber("100") }
        }, &col, &schema);

        // Equality filter
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn = CAST(\"3.14\" AS DyNumber)",
            "[[3]]", Scan);

        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn = CAST(\"1\" AS DyNumber)",
            "[[1]]", Scan);

        // Not-equal filter
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn != CAST(\"3.14\" AS DyNumber) ORDER BY id",
            "[[1];[2];[4]]", Scan);

        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn != CAST(\"100\" AS DyNumber) ORDER BY id",
            "[[1];[2];[3]]", Scan);
    }

    // ---- 3. IS NULL / IS NOT NULL ----
    Y_UNIT_TEST(TestFilterNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("1") },
            { 2, 20, std::nullopt },
            { 3, 30, MakeDyNumber("3") },
            { 4, 40, std::nullopt },
            { 5, 50, MakeDyNumber("0") }
        }, &col, &schema);

        CheckOrExec(helper, "SELECT id FROM `" + tableName + "` WHERE dyn IS NULL ORDER BY id",
            "[[2];[4]]", Scan);
        CheckOrExec(helper, "SELECT id FROM `" + tableName + "` WHERE dyn IS NOT NULL ORDER BY id",
            "[[1];[3];[5]]", Scan);
    }

    // ---- 4. Comparison queries (<, >, <=, >=) ----
    Y_UNIT_TEST(TestFilterCompare, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("-10") },
            { 2, 20, MakeDyNumber("0") },
            { 3, 30, MakeDyNumber("5") },
            { 4, 40, MakeDyNumber("10") },
            { 5, 50, MakeDyNumber("100") }
        }, &col, &schema);

        // Less than
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn < CAST(\"5\" AS DyNumber) ORDER BY id",
            "[[1];[2]]", Scan);

        // Greater than
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn > CAST(\"5\" AS DyNumber) ORDER BY id",
            "[[4];[5]]", Scan);

        // Less than or equal
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn <= CAST(\"5\" AS DyNumber) ORDER BY id",
            "[[1];[2];[3]]", Scan);

        // Greater than or equal
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn >= CAST(\"10\" AS DyNumber) ORDER BY id",
            "[[4];[5]]", Scan);

        // Negative boundary
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn > CAST(\"-10\" AS DyNumber) ORDER BY id",
            "[[2];[3];[4];[5]]", Scan);

        // Combined range
        CheckOrExec(helper,
            "SELECT id FROM `" + tableName + "` WHERE dyn >= CAST(\"0\" AS DyNumber) AND dyn <= CAST(\"10\" AS DyNumber) ORDER BY id",
            "[[2];[3];[4]]", Scan);
    }

    // ---- 5. ORDER BY dyn ----
    Y_UNIT_TEST(TestOrderBy, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("100") },
            { 2, 20, MakeDyNumber("-50") },
            { 3, 30, MakeDyNumber("0") },
            { 4, 40, MakeDyNumber("3.14") },
            { 5, 50, MakeDyNumber("-1") }
        }, &col, &schema);

        // ORDER BY dyn ASC (default)
        CheckOrExec(helper,
            "SELECT id, dyn FROM `" + tableName + "` ORDER BY dyn",
            "[[2;[\"-.5e2\"]];[5;[\"-.1e1\"]];[3;[\"0\"]];[4;[\".314e1\"]];[1;[\".1e3\"]]]", Scan);

        // ORDER BY dyn DESC
        CheckOrExec(helper,
            "SELECT id, dyn FROM `" + tableName + "` ORDER BY dyn DESC",
            "[[1;[\".1e3\"]];[4;[\".314e1\"]];[3;[\"0\"]];[5;[\"-.1e1\"]];[2;[\"-.5e2\"]]]", Scan);
    }

    // ---- 6. GROUP BY dyn with count(*) ----
    Y_UNIT_TEST(TestGroupBy, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("1") },
            { 2, 20, MakeDyNumber("2") },
            { 3, 30, MakeDyNumber("1") },
            { 4, 40, MakeDyNumber("2") },
            { 5, 50, MakeDyNumber("3") }
        }, &col, &schema);

        CheckOrExec(helper, "SELECT dyn, count(*) AS cnt FROM `" + tableName + "` GROUP BY dyn ORDER BY dyn",
            "[[[\".1e1\"];2u];[[\".2e1\"];2u];[[\".3e1\"];1u]]", Scan);
    }

    // ---- 7. min(dyn), max(dyn), count(dyn), count(*) ----
    Y_UNIT_TEST(TestAggregation, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("-5") },
            { 2, 20, MakeDyNumber("10") },
            { 3, 30, MakeDyNumber("99") },
            { 4, 40, std::nullopt },
            { 5, 50, MakeDyNumber("0") }
        }, &col, &schema);

        // min should return the smallest non-null value
        CheckOrExec(helper, "SELECT min(dyn) FROM `" + tableName + "`", "[[[\"-.5e1\"]]]", Scan);

        // max should return the largest non-null value
        CheckOrExec(helper, "SELECT max(dyn) FROM `" + tableName + "`", "[[[\".99e2\"]]]", Scan);

        // count(dyn) should exclude nulls
        CheckOrExec(helper, "SELECT count(dyn) FROM `" + tableName + "`", "[[4u]]", Scan);

        // count(*) should include all rows
        CheckOrExec(helper, "SELECT count(*) FROM `" + tableName + "`", "[[5u]]", Scan);
    }

    // ---- 8. JOIN two tables by id, select DyNumber columns ----
    Y_UNIT_TEST(TestJoinById, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";

        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;

        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
            };
            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);

            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("table1_id").SetType(NScheme::NTypeIds::Int32),
                TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
            };
            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            CreateDataShardTable(helper, t1);
            {
                auto& session = helper.GetSession();
                auto res = session
                               .ExecuteSchemeQuery(TStringBuilder() << R"(
                        CREATE TABLE `)" << t2 << R"(` (
                            id Int32 NOT NULL,
                            table1_id Int32,
                            dyn DyNumber,
                            PRIMARY KEY (id)
                        );
                    )")
                               .ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
            }
        }

        LoadData(helper, Table, Load, t1, {
            { 1, 10, MakeDyNumber("3.14") },
            { 2, 20, MakeDyNumber("100") }
        }, &col1, &s1);

        // Load table2 data
        if (Table == ETableKind::COLUMNSHARD) {
            // For columnshard, table2 has table1_id as Int32 in the second position
            // We need a custom arrow batch since schema differs
            if (Load == ELoadKind::ARROW) {
                arrow::Int32Builder idB, t1idB;
                arrow::BinaryBuilder dynB;
                Y_ABORT_UNLESS(idB.Append(1).ok()); Y_ABORT_UNLESS(t1idB.Append(1).ok()); AppendDyNumber(dynB, "50");
                Y_ABORT_UNLESS(idB.Append(2).ok()); Y_ABORT_UNLESS(t1idB.Append(1).ok()); AppendDyNumber(dynB, "60");
                Y_ABORT_UNLESS(idB.Append(3).ok()); Y_ABORT_UNLESS(t1idB.Append(2).ok()); AppendDyNumber(dynB, "70");
                std::shared_ptr<arrow::Array> idArr, t1idArr, dynArr;
                Y_ABORT_UNLESS(idB.Finish(&idArr).ok());
                Y_ABORT_UNLESS(t1idB.Finish(&t1idArr).ok());
                Y_ABORT_UNLESS(dynB.Finish(&dynArr).ok());
                auto aSchema = arrow::schema({
                    arrow::field("id", arrow::int32(), false),
                    arrow::field("table1_id", arrow::int32()),
                    arrow::field("dyn", arrow::binary())
                });
                auto batch = arrow::RecordBatch::Make(aSchema, 3, { idArr, t1idArr, dynArr });
                helper.BulkUpsert(col2, batch);
            } else if (Load == ELoadKind::YDB_VALUE) {
                TValueBuilder builder;
                builder.BeginList();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("dyn").BeginOptional().DyNumber("50").EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("dyn").BeginOptional().DyNumber("60").EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("table1_id").BeginOptional().Int32(2).EndOptional().AddMember("dyn").BeginOptional().DyNumber("70").EndOptional().EndStruct();
                builder.EndList();
                auto res = helper.GetKikimr().GetTableClient().BulkUpsert(t2, builder.Build()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            } else {
                TStringBuilder csv;
                csv << "1,1,.5e2\n2,1,.6e2\n3,2,.7e2\n";
                auto res = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        } else {
            if (Load == ELoadKind::ARROW) {
                arrow::Int32Builder idB, t1idB;
                arrow::BinaryBuilder dynB;
                Y_ABORT_UNLESS(idB.Append(1).ok()); Y_ABORT_UNLESS(t1idB.Append(1).ok()); AppendDyNumber(dynB, "50");
                Y_ABORT_UNLESS(idB.Append(2).ok()); Y_ABORT_UNLESS(t1idB.Append(1).ok()); AppendDyNumber(dynB, "60");
                Y_ABORT_UNLESS(idB.Append(3).ok()); Y_ABORT_UNLESS(t1idB.Append(2).ok()); AppendDyNumber(dynB, "70");
                std::shared_ptr<arrow::Array> idArr, t1idArr, dynArr;
                Y_ABORT_UNLESS(idB.Finish(&idArr).ok());
                Y_ABORT_UNLESS(t1idB.Finish(&t1idArr).ok());
                Y_ABORT_UNLESS(dynB.Finish(&dynArr).ok());
                auto aSchema = arrow::schema({
                    arrow::field("id", arrow::int32(), false),
                    arrow::field("table1_id", arrow::int32()),
                    arrow::field("dyn", arrow::binary())
                });
                auto batch = arrow::RecordBatch::Make(aSchema, 3, { idArr, t1idArr, dynArr });
                TString strBatch = NArrow::SerializeBatchNoCompression(batch);
                TString strSchema = NArrow::SerializeSchema(*batch->schema());
                auto res = helper.GetKikimr().GetTableClient().BulkUpsert(t2, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            } else if (Load == ELoadKind::YDB_VALUE) {
                TValueBuilder builder;
                builder.BeginList();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(1).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("dyn").BeginOptional().DyNumber("50").EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(2).AddMember("table1_id").BeginOptional().Int32(1).EndOptional().AddMember("dyn").BeginOptional().DyNumber("60").EndOptional().EndStruct();
                builder.AddListItem().BeginStruct().AddMember("id").Int32(3).AddMember("table1_id").BeginOptional().Int32(2).EndOptional().AddMember("dyn").BeginOptional().DyNumber("70").EndOptional().EndStruct();
                builder.EndList();
                auto res = helper.GetKikimr().GetTableClient().BulkUpsert(t2, builder.Build()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            } else {
                TStringBuilder csv;
                csv << "1,1,.5e2\n2,1,.6e2\n3,2,.7e2\n";
                auto res = helper.GetKikimr().GetTableClient().BulkUpsert(t2, EDataFormat::CSV, csv).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }

        CheckOrExec(helper,
            "SELECT t1.id, t1.dyn, t2.dyn FROM `" + t1 + "` AS t1 "
            "JOIN `" + t2 + "` AS t2 ON t1.id = t2.table1_id "
            "ORDER BY t1.id, t2.dyn",
            "[[1;[\".314e1\"];[\".5e2\"]];[1;[\".314e1\"];[\".6e2\"]];[2;[\".1e3\"];[\".7e2\"]]]", Scan);
    }

    // ---- 9. JOIN two tables ON t1.dyn = t2.dyn ----
    Y_UNIT_TEST(TestJoinByDyNumber, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);

        const TString t1 = "/Root/Table1";
        const TString t2 = "/Root/Table2";

        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;

        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
            };
            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);

            s2 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
            };
            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            CreateDataShardTable(helper, t1);
            CreateDataShardTable(helper, t2);
        }

        LoadData(helper, Table, Load, t1, {
            { 1, 10, MakeDyNumber("10") },
            { 2, 20, MakeDyNumber("20") },
            { 3, 30, MakeDyNumber("30") }
        }, &col1, &s1);

        LoadData(helper, Table, Load, t2, {
            { 10, 100, MakeDyNumber("10") },
            { 20, 200, MakeDyNumber("30") },
            { 30, 300, MakeDyNumber("50") }
        }, &col2, &s2);

        CheckOrExec(helper,
            "SELECT t1.id, t2.id, t1.dyn FROM `" + t1 + "` AS t1 "
            "JOIN `" + t2 + "` AS t2 ON t1.dyn = t2.dyn "
            "ORDER BY t1.id, t2.id",
            "[[1;10;[\".1e2\"]];[3;20;[\".3e2\"]]]", Scan);
    }

    // ---- 10. ORDER BY with LIMIT ----
    Y_UNIT_TEST(TestOrderByWithLimit, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("-100") },
            { 2, 20, MakeDyNumber("-1") },
            { 3, 30, MakeDyNumber("0") },
            { 4, 40, MakeDyNumber("5") },
            { 5, 50, MakeDyNumber("42") },
            { 6, 60, MakeDyNumber("999") }
        }, &col, &schema);

        // Top 3 smallest
        CheckOrExec(helper,
            "SELECT id, dyn FROM `" + tableName + "` ORDER BY dyn LIMIT 3",
            "[[1;[\"-.1e3\"]];[2;[\"-.1e1\"]];[3;[\"0\"]]]", Scan);

        // Top 2 largest
        CheckOrExec(helper,
            "SELECT id, dyn FROM `" + tableName + "` ORDER BY dyn DESC LIMIT 2",
            "[[6;[\".999e3\"]];[5;[\".42e2\"]]]", Scan);

        // LIMIT with OFFSET
        CheckOrExec(helper,
            "SELECT id, dyn FROM `" + tableName + "` ORDER BY dyn LIMIT 2 OFFSET 2",
            "[[3;[\"0\"]];[4;[\".5e1\"]]]", Scan);
    }

    // ---- 11. GROUP BY including NULL DyNumber values ----
    Y_UNIT_TEST(TestGroupByWithNulls, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString tableName = "/Root/ColumnTableTest";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, tableName, &col, &schema);
        LoadData(helper, Table, Load, tableName, {
            { 1, 10, MakeDyNumber("10") },
            { 2, 20, std::nullopt },
            { 3, 30, MakeDyNumber("10") },
            { 4, 40, std::nullopt },
            { 5, 50, MakeDyNumber("20") },
            { 6, 60, std::nullopt }
        }, &col, &schema);

        // GROUP BY dyn with NULLs -- NULLs should form their own group
        CheckOrExec(helper,
            "SELECT dyn, count(*) AS cnt FROM `" + tableName + "` GROUP BY dyn ORDER BY dyn",
            "[[#;3u];[[\".1e2\"];2u];[[\".2e2\"];1u]]", Scan);

        // Verify count(dyn) excludes nulls, count(*) includes them
        CheckOrExec(helper,
            "SELECT count(dyn), count(*) FROM `" + tableName + "`",
            "[[3u;6u]]", Scan);
    }

    // ---- 12. DyNumber NOT NULL as primary key (COLUMNSHARD only) ----
    Y_UNIT_TEST(TestDyNumberAsPrimaryKey, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"dyn"}).SetSharding({"dyn"}).SetSchema(schema);
        helper.CreateTable(testTable);

        if (Load == ELoadKind::ARROW) {
            arrow::BinaryBuilder dynB;
            arrow::Int64Builder valB;
            AppendDyNumber(dynB, "-100"); Y_ABORT_UNLESS(valB.Append(1).ok());
            AppendDyNumber(dynB, "0"); Y_ABORT_UNLESS(valB.Append(2).ok());
            AppendDyNumber(dynB, "3.14"); Y_ABORT_UNLESS(valB.Append(3).ok());
            AppendDyNumber(dynB, "100"); Y_ABORT_UNLESS(valB.Append(4).ok());
            AppendDyNumber(dynB, "999"); Y_ABORT_UNLESS(valB.Append(5).ok());
            std::shared_ptr<arrow::Array> dynArr, valArr;
            Y_ABORT_UNLESS(dynB.Finish(&dynArr).ok());
            Y_ABORT_UNLESS(valB.Finish(&valArr).ok());
            auto aSchema = arrow::schema({
                arrow::field("dyn", arrow::binary(), false),
                arrow::field("val", arrow::int64())
            });
            auto batch = arrow::RecordBatch::Make(aSchema, 5, { dynArr, valArr });
            helper.BulkUpsert(testTable, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("dyn").DyNumber("-100").AddMember("val").BeginOptional().Int64(1).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dyn").DyNumber("0").AddMember("val").BeginOptional().Int64(2).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dyn").DyNumber("3.14").AddMember("val").BeginOptional().Int64(3).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dyn").DyNumber("100").AddMember("val").BeginOptional().Int64(4).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("dyn").DyNumber("999").AddMember("val").BeginOptional().Int64(5).EndOptional().EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "-.1e3,1\n.0,2\n.314e1,3\n.1e3,4\n.999e3,5\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        // Filter by DyNumber PK using normalized form in CAST
        CheckOrExec(helper, "SELECT val FROM `/Root/ColumnTableTest` WHERE dyn = CAST(\".1e3\" AS DyNumber)", "[[[4]]]", Scan);
        CheckOrExec(helper, "SELECT val FROM `/Root/ColumnTableTest` WHERE dyn = CAST(\"0\" AS DyNumber)", "[[[2]]]", Scan);
        CheckOrExec(helper, "SELECT val FROM `/Root/ColumnTableTest` WHERE dyn = CAST(\"3.14\" AS DyNumber)", "[[[3]]]", Scan);
    }

    // ---- 13. DML parity between row/column tables ----
    Y_UNIT_TEST(TestDmlParityAndCTAS, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(true);
        TTestHelper helper(runnerSettings);

        const TString ds = "/Root/RowSrc";
        const TString cs = "/Root/ColSrc";

        CreateDataShardTable(helper, ds);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
            TTestHelper::TColumnSchema().SetName("dyn").SetType(NScheme::NTypeIds::DyNumber),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        // Insert initial data into datashard table via DML
        helper.ExecuteQuery(
            "UPSERT INTO `" + ds + "` (id, int, dyn) VALUES "
            "(1, 100, CAST(\"10\" AS DyNumber)), "
            "(2, 200, CAST(\"20\" AS DyNumber))");

        // Insert initial data into columnshard table
        if (Load == ELoadKind::ARROW) {
            auto batch = MakeArrowBatch({
                { 1, 100, MakeDyNumber("10") },
                { 2, 200, MakeDyNumber("20") }
            });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            BulkUpsertRowTableYdbValue(helper, cs, {
                { 1, 100, MakeDyNumber("10") },
                { 2, 200, MakeDyNumber("20") }
            });
        } else {
            BulkUpsertRowTableCSV(helper, cs, {
                { 1, 100, MakeDyNumber("10") },
                { 2, 200, MakeDyNumber("20") }
            });
        }

        // Verify initial state
        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + ds + "` ORDER BY id",
            "[[1;[100];[\".1e2\"]];[2;[200];[\".2e2\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + cs + "` ORDER BY id",
            "[[1;[100];[\".1e2\"]];[2;[200];[\".2e2\"]]]", Scan);

        // UPSERT a new row and overwrite an existing one
        helper.ExecuteQuery(
            "UPSERT INTO `" + ds + "` (id, int, dyn) VALUES "
            "(3, 300, CAST(\"30\" AS DyNumber)), "
            "(1, 110, CAST(\"11\" AS DyNumber))");
        helper.ExecuteQuery(
            "UPSERT INTO `" + cs + "` (id, int, dyn) VALUES "
            "(3, 300, CAST(\"30\" AS DyNumber)), "
            "(1, 110, CAST(\"11\" AS DyNumber))");

        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + ds + "` ORDER BY id",
            "[[1;[110];[\".11e2\"]];[2;[200];[\".2e2\"]];[3;[300];[\".3e2\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + cs + "` ORDER BY id",
            "[[1;[110];[\".11e2\"]];[2;[200];[\".2e2\"]];[3;[300];[\".3e2\"]]]", Scan);

        // DELETE a row
        helper.ExecuteQuery("DELETE FROM `" + ds + "` WHERE id = 2");
        helper.ExecuteQuery("DELETE FROM `" + cs + "` WHERE id = 2");

        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + ds + "` ORDER BY id",
            "[[1;[110];[\".11e2\"]];[3;[300];[\".3e2\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + cs + "` ORDER BY id",
            "[[1;[110];[\".11e2\"]];[3;[300];[\".3e2\"]]]", Scan);

        // REPLACE
        helper.ExecuteQuery(
            "REPLACE INTO `" + ds + "` (id, int, dyn) VALUES (1, 150, CAST(\"15\" AS DyNumber))");
        helper.ExecuteQuery(
            "REPLACE INTO `" + cs + "` (id, int, dyn) VALUES (1, 150, CAST(\"15\" AS DyNumber))");

        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + ds + "` ORDER BY id",
            "[[1;[150];[\".15e2\"]];[3;[300];[\".3e2\"]]]", Scan);
        CheckOrExec(helper, "SELECT id, int, dyn FROM `" + cs + "` ORDER BY id",
            "[[1;[150];[\".15e2\"]];[3;[300];[\".3e2\"]]]", Scan);
    }
}

} // namespace NKqp
} // namespace NKikimr
