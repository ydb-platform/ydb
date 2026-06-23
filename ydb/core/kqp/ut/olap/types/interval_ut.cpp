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

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpIntervalColumnShard) {
    namespace {
    struct TRow {
        i32 Id;
        i64 IntVal;
        std::optional<i64> Ival;  // Interval stored as i64 microseconds
    };

    constexpr i64 MaxValidInterval = (i64)(86400000000ULL * 49673U - 1);
    constexpr i64 MinValidInterval = -MaxValidInterval;

    void CreateDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    int Int64,
                    ival Interval,
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
                .AddMember("ival");
            if (r.Ival.has_value()) {
                builder.BeginOptional().Interval(*r.Ival).EndOptional();
            } else {
                builder.EmptyOptional(EPrimitiveType::Interval);
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
            if (r.Ival.has_value()) {
                builder << *r.Ival;
            }
            builder << '\n';
        }
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> ints;
        std::vector<std::optional<int64_t>> ivals;
        ids.reserve(rows.size());
        ints.reserve(rows.size());
        ivals.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            ints.push_back(r.IntVal);
            ivals.push_back(r.Ival);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(ints);
        auto ivalArr = MakeInt64ArrayNullable(ivals);
        auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), /*nullable*/ false),
            arrow::field("int", arrow::int64()),
            arrow::field("ival", arrow::int64())
        });

        return MakeBatch({ schema->field(0), schema->field(1), schema->field(2) }, { idArr, intArr, ivalArr });
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
                TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
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

        // Select only the interval column
        CheckOrExec(helper, "SELECT ival FROM `/Root/Table1` WHERE id=1", "[[[1000000]]]", Scan);
        CheckOrExec(helper, "SELECT ival FROM `/Root/Table1` WHERE id=3", "[[#]]", Scan);

        // Additional null row
        LoadData(helper, Table, Load, tableName, { { 7, 0, std::nullopt } }, &col, &schema);
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id=7", "[[7;[0];#]]", Scan);
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
            { { 1, 10, (i64)1000000 }, { 2, 20, (i64)2000000 }, { 3, 30, (i64)1000000 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)-500000 } },
            &col, &schema);

        // Equal filter - should match two rows
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival = CAST(1000000 AS Interval) ORDER BY id",
            "[[1;[10];[1000000]];[3;[30];[1000000]]]", Scan);

        // Equal filter - single match
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival = CAST(2000000 AS Interval) ORDER BY id",
            "[[2;[20];[2000000]]]", Scan);

        // Equal filter - negative value
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival = CAST(-500000 AS Interval) ORDER BY id",
            "[[5;[50];[-500000]]]", Scan);

        // Equal filter - no match
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival = CAST(999 AS Interval)", "[]", Scan);

        // Not equal filter
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival != CAST(1000000 AS Interval) ORDER BY id",
            "[[2;[20];[2000000]];[5;[50];[-500000]]]", Scan);

        // Not equal to negative value
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival != CAST(-500000 AS Interval) ORDER BY id",
            "[[1;[10];[1000000]];[2;[20];[2000000]];[3;[30];[1000000]]]", Scan);
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
            { { 1, 10, (i64)100 }, { 2, 20, std::nullopt }, { 3, 30, (i64)300 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)0 }, { 6, 60, std::nullopt } },
            &col, &schema);

        // IS NULL filter
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival IS NULL ORDER BY id",
            "[[2;[20];#];[4;[40];#];[6;[60];#]]", Scan);

        // IS NOT NULL filter
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival IS NOT NULL ORDER BY id",
            "[[1;[10];[100]];[3;[30];[300]];[5;[50];[0]]]", Scan);

        // Count nulls vs non-nulls
        CheckOrExec(helper, "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NULL", "[[3u]]", Scan);
        CheckOrExec(helper, "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NOT NULL", "[[3u]]", Scan);
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
            { { 1, 10, (i64)-3000000 }, { 2, 20, (i64)-1000000 }, { 3, 30, (i64)0 },
              { 4, 40, (i64)1000000 }, { 5, 50, (i64)5000000 }, { 6, 60, std::nullopt } },
            &col, &schema);

        // Less than
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival < CAST(0 AS Interval) ORDER BY id",
            "[[1;[10];[-3000000]];[2;[20];[-1000000]]]", Scan);

        // Greater than
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival > CAST(0 AS Interval) ORDER BY id",
            "[[4;[40];[1000000]];[5;[50];[5000000]]]", Scan);

        // Less than or equal
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival <= CAST(0 AS Interval) ORDER BY id",
            "[[1;[10];[-3000000]];[2;[20];[-1000000]];[3;[30];[0]]]", Scan);

        // Greater than or equal
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival >= CAST(0 AS Interval) ORDER BY id",
            "[[3;[30];[0]];[4;[40];[1000000]];[5;[50];[5000000]]]", Scan);

        // Range: between two values
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival >= CAST(-1000000 AS Interval) AND ival <= CAST(1000000 AS Interval) ORDER BY id",
            "[[2;[20];[-1000000]];[3;[30];[0]];[4;[40];[1000000]]]", Scan);

        // Strict range
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival > CAST(-1000000 AS Interval) AND ival < CAST(5000000 AS Interval) ORDER BY id",
            "[[3;[30];[0]];[4;[40];[1000000]]]", Scan);

        // Compare with negative value
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival < CAST(-1000000 AS Interval) ORDER BY id",
            "[[1;[10];[-3000000]]]", Scan);

        // Compare with large positive value - should return all non-null rows
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE ival < CAST(99999999 AS Interval) ORDER BY id",
            "[[1;[10];[-3000000]];[2;[20];[-1000000]];[3;[30];[0]];[4;[40];[1000000]];[5;[50];[5000000]]]", Scan);
    }

    Y_UNIT_TEST(TestOrderByInterval, EQueryMode, ETableKind, ELoadKind) {
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
            { { 1, 10, (i64)300 }, { 2, 20, (i64)-100 }, { 3, 30, (i64)200 },
              { 4, 40, (i64)-200 }, { 5, 50, (i64)0 } },
            &col, &schema);

        // Ascending order
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival",
            "[[4;[40];[-200]];[2;[20];[-100]];[5;[50];[0]];[3;[30];[200]];[1;[10];[300]]]", Scan);

        // Descending order
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival DESC",
            "[[1;[10];[300]];[3;[30];[200]];[5;[50];[0]];[2;[20];[-100]];[4;[40];[-200]]]", Scan);

        // Add duplicate ival and order by ival then by id
        LoadData(helper, Table, Load, tableName, { { 6, 60, (i64)200 } }, &col, &schema);

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival, id",
            "[[4;[40];[-200]];[2;[20];[-100]];[5;[50];[0]];[3;[30];[200]];[6;[60];[200]];[1;[10];[300]]]", Scan);

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival DESC, id DESC",
            "[[1;[10];[300]];[6;[60];[200]];[3;[30];[200]];[5;[50];[0]];[2;[20];[-100]];[4;[40];[-200]]]", Scan);
    }

    Y_UNIT_TEST(TestGroupByInterval, EQueryMode, ETableKind, ELoadKind) {
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
            { { 1, 10, (i64)100 }, { 2, 20, (i64)200 }, { 3, 30, (i64)100 },
              { 4, 40, (i64)200 }, { 5, 50, (i64)300 }, { 6, 60, (i64)100 },
              { 7, 70, (i64)-100 } },
            &col, &schema);

        CheckOrExec(helper, "SELECT ival, count(*) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
            "[[[-100];1u];[[100];3u];[[200];2u];[[300];1u]]", Scan);

        // Group by with min of id
        CheckOrExec(helper, "SELECT ival, count(*) AS cnt, min(id) AS min_id FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
            "[[[-100];1u;7];[[100];3u;1];[[200];2u;2];[[300];1u;5]]", Scan);
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
            { { 1, 10, (i64)-500 }, { 2, 20, (i64)100 }, { 3, 30, (i64)999 },
              { 4, 40, std::nullopt }, { 5, 50, (i64)0 } },
            &col, &schema);

        // min
        CheckOrExec(helper, "SELECT min(ival) FROM `/Root/Table1`", "[[[-500]]]", Scan);

        // max
        CheckOrExec(helper, "SELECT max(ival) FROM `/Root/Table1`", "[[[999]]]", Scan);

        // count(ival) - excludes nulls
        CheckOrExec(helper, "SELECT count(ival) FROM `/Root/Table1`", "[[4u]]", Scan);

        // count(*) - includes nulls
        CheckOrExec(helper, "SELECT count(*) FROM `/Root/Table1`", "[[5u]]", Scan);

        // Combined aggregations
        CheckOrExec(helper, "SELECT min(ival), max(ival), count(ival), count(*) FROM `/Root/Table1`",
            "[[[-500];[999];4u;5u]]", Scan);

        // Aggregation on table with all nulls
        {
            const TString tableName2 = "/Root/Table2";
            TTestHelper::TColumnTable col2;
            TVector<TTestHelper::TColumnSchema> schema2;
            PrepareBase(helper, Table, tableName2, &col2, &schema2);
            LoadData(helper, Table, Load, tableName2,
                { { 1, 0, std::nullopt }, { 2, 0, std::nullopt } },
                &col2, &schema2);

            CheckOrExec(helper, "SELECT min(ival), max(ival), count(ival), count(*) FROM `/Root/Table2`",
                "[[#;#;0u;2u]]", Scan);
        }
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
            CreateDataShardTable(helper, t1);
            // Table2 has table1_id instead of int
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

        // Load table1 data
        LoadData(helper, Table, Load, t1,
            { { 1, 10, (i64)1000000 }, { 2, 20, (i64)-500000 }, { 3, 30, std::nullopt } },
            &col1, &s1);

        // Load table2 data - need special handling for table1_id column in DATASHARD
        if (Table == ETableKind::COLUMNSHARD) {
            // For columnshard, table2 has schema: id, table1_id, ival
            // We build arrow batch manually since the schema differs from TRow
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
            // DATASHARD table2: id Int32 NOT NULL, table1_id Int32, ival Interval
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

        // Join by id, select interval columns from both tables
        CheckOrExec(helper,
            "SELECT t1.id, t1.ival, t2.ival FROM `/Root/Table1` AS t1 "
            "JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id "
            "ORDER BY t1.id, t2.id",
            "[[1;[1000000];[2000000]];[1;[1000000];[3000000]];[2;[-500000];[-100000]];[3;#;#]]", Scan);

        // Join with filter on interval
        CheckOrExec(helper,
            "SELECT t1.id, t1.ival, t2.ival FROM `/Root/Table1` AS t1 "
            "JOIN `/Root/Table2` AS t2 ON t1.id = t2.table1_id "
            "WHERE t1.ival IS NOT NULL "
            "ORDER BY t1.id, t2.id",
            "[[1;[1000000];[2000000]];[1;[1000000];[3000000]];[2;[-500000];[-100000]]]", Scan);
    }

    Y_UNIT_TEST(TestJoinByInterval, EQueryMode, ETableKind, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Table = Arg<1>();
        const auto Load = Arg<2>();

        const TString t1 = "/Root/JoinTable1";
        const TString t2 = "/Root/JoinTable2";
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col1, col2;
        TVector<TTestHelper::TColumnSchema> s1, s2;

        // Both tables have same schema: id, int, ival
        if (Table == ETableKind::COLUMNSHARD) {
            s1 = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
            };
            col1.SetName(t1).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s1);
            helper.CreateTable(col1);

            s2 = s1;
            col2.SetName(t2).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(s2);
            helper.CreateTable(col2);
        } else {
            CreateDataShardTable(helper, t1);
            CreateDataShardTable(helper, t2);
        }

        LoadData(helper, Table, Load, t1,
            { { 1, 10, (i64)1000 }, { 2, 20, (i64)2000 }, { 3, 30, (i64)3000 } },
            &col1, &s1);

        LoadData(helper, Table, Load, t2,
            { { 10, 100, (i64)1000 }, { 20, 200, (i64)2000 }, { 30, 300, (i64)4000 } },
            &col2, &s2);

        // Join on interval column - only matching ival values
        CheckOrExec(helper,
            "SELECT t1.id, t2.id, t1.ival FROM `/Root/JoinTable1` AS t1 "
            "JOIN `/Root/JoinTable2` AS t2 ON t1.ival = t2.ival "
            "ORDER BY t1.id, t2.id",
            "[[1;10;[1000]];[2;20;[2000]]]", Scan);
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
            { { 1, 10, (i64)500 }, { 2, 20, (i64)-200 }, { 3, 30, (i64)100 },
              { 4, 40, (i64)-300 }, { 5, 50, (i64)1000 }, { 6, 60, (i64)0 } },
            &col, &schema);

        // LIMIT 1 ascending
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 1",
            "[[4;[40];[-300]]]", Scan);

        // LIMIT 1 descending
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival DESC LIMIT 1",
            "[[5;[50];[1000]]]", Scan);

        // LIMIT 3 ascending
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 3",
            "[[4;[40];[-300]];[2;[20];[-200]];[6;[60];[0]]]", Scan);

        // LIMIT 3 descending
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival DESC LIMIT 3",
            "[[5;[50];[1000]];[1;[10];[500]];[3;[30];[100]]]", Scan);

        // LIMIT with OFFSET
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 2 OFFSET 2",
            "[[6;[60];[0]];[3;[30];[100]]]", Scan);

        // LIMIT larger than table size
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival LIMIT 100",
            "[[4;[40];[-300]];[2;[20];[-200]];[6;[60];[0]];[3;[30];[100]];[1;[10];[500]];[5;[50];[1000]]]", Scan);

        // ORDER BY ival, id with LIMIT
        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` ORDER BY ival, id LIMIT 2",
            "[[4;[40];[-300]];[2;[20];[-200]]]", Scan);
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
            { { 1, 10, (i64)100 }, { 2, 20, (i64)200 }, { 3, 30, std::nullopt },
              { 4, 40, (i64)100 }, { 5, 50, std::nullopt }, { 6, 60, (i64)200 },
              { 7, 70, std::nullopt }, { 8, 80, (i64)-100 } },
            &col, &schema);

        // GROUP BY with NULLs - NULLs should be grouped together
        CheckOrExec(helper, "SELECT ival, count(*) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
            "[[#;3u];[[-100];1u];[[100];2u];[[200];2u]]", Scan);

        // count(ival) in GROUP BY excludes NULLs from count
        CheckOrExec(helper, "SELECT ival, count(ival) AS cnt FROM `/Root/Table1` GROUP BY ival ORDER BY ival",
            "[[#;0u];[[-100];1u];[[100];2u];[[200];2u]]", Scan);

        // Aggregate with WHERE filtering out NULLs
        CheckOrExec(helper,
            "SELECT ival, count(*) AS cnt FROM `/Root/Table1` WHERE ival IS NOT NULL GROUP BY ival ORDER BY ival",
            "[[[-100];1u];[[100];2u];[[200];2u]]", Scan);

        // Aggregate only on NULL rows
        CheckOrExec(helper, "SELECT count(*) FROM `/Root/Table1` WHERE ival IS NULL",
            "[[3u]]", Scan);

        // min/max ignoring NULLs
        CheckOrExec(helper, "SELECT min(ival), max(ival) FROM `/Root/Table1`",
            "[[[-100];[200]]]", Scan);
    }

    Y_UNIT_TEST(TestIntervalAsPrimaryKey, EQueryMode, ELoadKind) {
        const auto Scan = Arg<0>();
        const auto Load = Arg<1>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64)
        };

        TTestHelper::TColumnTable col;
        col.SetName("/Root/ColumnTableTest").SetPrimaryKey({"ival"}).SetSharding({"ival"}).SetSchema(schema);
        helper.CreateTable(col);

        if (Load == ELoadKind::ARROW) {
            using namespace NKikimr::NKqp::NTestArrow;
            auto ivalArr = MakeInt64Array({-7000000, -3000000, 0, 4000000, 9000000});
            auto valArr = MakeInt64ArrayNullable({(i64)1, (i64)2, std::nullopt, (i64)4, (i64)5});
            auto batch = MakeBatch(
                { arrow::field("ival", arrow::int64(), false),
                  arrow::field("val", arrow::int64()) },
                { ivalArr, valArr });
            helper.BulkUpsert(col, batch);
        } else if (Load == ELoadKind::YDB_VALUE) {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(-7000000).AddMember("val").BeginOptional().Int64(1).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(-3000000).AddMember("val").BeginOptional().Int64(2).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(0).AddMember("val").EmptyOptional(EPrimitiveType::Int64).EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(4000000).AddMember("val").BeginOptional().Int64(4).EndOptional().EndStruct();
            builder.AddListItem().BeginStruct().AddMember("ival").Interval(9000000).AddMember("val").BeginOptional().Int64(5).EndOptional().EndStruct();
            builder.EndList();
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", builder.Build()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        } else {
            TStringBuilder csv;
            csv << "-7000000,1\n-3000000,2\n0,\n4000000,4\n9000000,5\n";
            auto res = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/ColumnTableTest", EDataFormat::CSV, csv).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        // Equality filter on PK
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival = CAST(0 AS Interval)", "[[0;#]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival = CAST(4000000 AS Interval)", "[[4000000;[4]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival = CAST(-7000000 AS Interval)", "[[-7000000;[1]]]", Scan);

        // Less than
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival < CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]]]", Scan);

        // Greater than
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival > CAST(0 AS Interval)",
            "[[4000000;[4]];[9000000;[5]]]", Scan);

        // Less than or equal
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival <= CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]];[0;#]]", Scan);

        // Greater than or equal
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival >= CAST(0 AS Interval)",
            "[[0;#];[4000000;[4]];[9000000;[5]]]", Scan);

        // Not equal
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival != CAST(0 AS Interval)",
            "[[-7000000;[1]];[-3000000;[2]];[4000000;[4]];[9000000;[5]]]", Scan);

        // Range filter on PK
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival > CAST(-3000000 AS Interval) AND ival < CAST(9000000 AS Interval)",
            "[[0;#];[4000000;[4]]]", Scan);

        // PK filter with no results
        CheckOrExec(helper, "SELECT * FROM `/Root/ColumnTableTest` WHERE ival = CAST(999 AS Interval)", "[]", Scan);
    }

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
            TTestHelper::TColumnSchema().SetName("ival").SetType(NScheme::NTypeIds::Interval),
        };

        TTestHelper::TColumnTable col;
        col.SetName(cs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col);

        // Load initial data via DML into datashard
        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, ival) VALUES (1, 100, CAST(1000000 AS Interval)), (2, 200, CAST(-500000 AS Interval))");

        // Load initial data into columnshard via bulk upsert
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

        // UPDATE
        helper.ExecuteQuery("UPDATE `" + ds + "` SET int = int + 1 WHERE ival = CAST(1000000 AS Interval)");
        helper.ExecuteQuery("UPDATE `" + cs + "` SET int = int + 1 WHERE ival = CAST(1000000 AS Interval)");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]]]", Scan);

        // UPSERT new row
        helper.ExecuteQuery("UPSERT INTO `" + ds + "` (id, int, ival) VALUES (3, 300, CAST(3000000 AS Interval))");
        helper.ExecuteQuery("UPSERT INTO `" + cs + "` (id, int, ival) VALUES (3, 300, CAST(3000000 AS Interval))");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[200];[-500000]];[3;[300];[3000000]]]", Scan);

        // REPLACE
        helper.ExecuteQuery("REPLACE INTO `" + ds + "` (id, int, ival) VALUES (2, 250, CAST(-250000 AS Interval))");
        helper.ExecuteQuery("REPLACE INTO `" + cs + "` (id, int, ival) VALUES (2, 250, CAST(-250000 AS Interval))");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[2;[250];[-250000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[2;[250];[-250000]];[3;[300];[3000000]]]", Scan);

        // DELETE
        helper.ExecuteQuery("DELETE FROM `" + ds + "` WHERE ival = CAST(-250000 AS Interval)");
        helper.ExecuteQuery("DELETE FROM `" + cs + "` WHERE ival = CAST(-250000 AS Interval)");
        CheckOrExec(helper, "SELECT * FROM `" + ds + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);
        CheckOrExec(helper, "SELECT * FROM `" + cs + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);

        // Cross-table copy: CS from DS
        const TString csFromDs = "/Root/CSFromDS";
        TVector<TTestHelper::TColumnSchema> schema2 = schema;
        TTestHelper::TColumnTable col2;
        col2.SetName(csFromDs).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema2);
        helper.CreateTable(col2);
        helper.ExecuteQuery(
            "UPSERT INTO `" + csFromDs + "` (id, int, ival) "
            "SELECT id, int, ival FROM `" + ds + "`");
        CheckOrExec(helper, "SELECT * FROM `" + csFromDs + "` ORDER BY id", "[[1;[101];[1000000]];[3;[300];[3000000]]]", Scan);

        // Cross-table copy: DS from CS
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

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper helper(runnerSettings);
        TTestHelper::TColumnTable col;
        TVector<TTestHelper::TColumnSchema> schema;
        PrepareBase(helper, Table, "/Root/Table1", &col, &schema);

        // Basic CSV upsert with positive and negative values
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

        // CSV upsert with large values
        {
            TStringBuilder builder;
            builder << "4,40," << MaxValidInterval << Endl;
            builder << "5,50," << MinValidInterval << Endl;
            auto result = helper.GetKikimr().GetTableClient().BulkUpsert("/Root/Table1", EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CheckOrExec(helper, "SELECT * FROM `/Root/Table1` WHERE id >= 4 ORDER BY id",
            TStringBuilder() << "[[4;[40];[" << MaxValidInterval << "]];[5;[50];[" << MinValidInterval << "]]]", Scan);

        // CSV upsert overwriting existing rows
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
