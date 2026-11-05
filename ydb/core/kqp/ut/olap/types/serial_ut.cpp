#include "column_type_test_base.h"

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

namespace NKikimr::NKqp {

using NYdb::NQuery::TTxControl;

namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

struct TSerialTypeInfo {
    const char* SqlAlias;
    EPrimitiveType PrimitiveType;
    arrow::Type::type ArrowType;
    i64 MaxValue;
};

TSerialTypeInfo GetSerialTypeInfo(ESerialType type) {
    switch (type) {
        case ESerialType::SmallSerial:
            return {"Serial2", EPrimitiveType::Int16, arrow::Type::INT16, Max<i16>()};
        case ESerialType::Serial:
            return {"Serial4", EPrimitiveType::Int32, arrow::Type::INT32, Max<i32>()};
        case ESerialType::BigSerial:
            return {"Serial8", EPrimitiveType::Int64, arrow::Type::INT64, Max<i64>()};
    }

    Y_ABORT("Unknown Serial type");
}

TKikimrSettings SerialSettings() {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.SetEnableArrowFormatAtDatashard(true);
    settings.FeatureFlags.SetEnableColumnshardSerial(true);
    return settings;
}

TExecuteQueryResult Execute(TQueryClient& client, const TString& sql) {
    auto result = client.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}

void CreateSerialTable(TQueryClient& client, const TString& name, const TString& columns, const TString& store) {
    auto result = client.ExecuteQuery("CREATE TABLE " + name + " (" + columns + ") WITH (STORE = " + store + ")",
        TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TStatus BulkUpsertExplicitKey(NYdb::NTable::TTableClient& client, ELoadKind load) {
    using NYdb::NTable::EDataFormat;
    switch (load) {
        case ARROW: {
            const auto batch = NTestArrow::MakeBatch(
                {arrow::field("id", arrow::int32(), false), arrow::field("v", arrow::int32(), false)},
                {NTestArrow::MakeInt32Array({-200}), NTestArrow::MakeInt32Array({50})});
            return client.BulkUpsert("/Root/T", EDataFormat::ApacheArrow,
                NArrow::SerializeBatchNoCompression(batch), NArrow::SerializeSchema(*batch->schema())).GetValueSync();
        }
        case YDB_VALUE: {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct()
                .AddMember("id").Int32(-200).AddMember("v").Int32(50).EndStruct().EndList();
            return client.BulkUpsert("/Root/T", rows.Build()).GetValueSync();
        }
        case CSV:
            return client.BulkUpsert("/Root/T", EDataFormat::CSV, "-200,50\n").GetValueSync();
    }

    Y_ABORT("Unknown load kind");
}

TStatus BulkUpsertWithoutKey(NYdb::NTable::TTableClient& client, ELoadKind load) {
    using NYdb::NTable::EDataFormat;
    switch (load) {
        case ARROW: {
            const auto batch = NTestArrow::MakeBatch({arrow::field("v", arrow::int32(), false)},
                {NTestArrow::MakeInt32Array({10, 20})});
            return client.BulkUpsert("/Root/T", EDataFormat::ApacheArrow,
                NArrow::SerializeBatchNoCompression(batch), NArrow::SerializeSchema(*batch->schema())).GetValueSync();
        }
        case YDB_VALUE: {
            TValueBuilder rows;
            rows.BeginList();
            for (i32 value : {10, 20}) {
                rows.AddListItem().BeginStruct().AddMember("v").Int32(value).EndStruct();
            }

            return client.BulkUpsert("/Root/T", rows.EndList().Build()).GetValueSync();
        }
        case CSV: {
            Ydb::Formats::CsvSettings csv;
            csv.set_header(true);
            return client.BulkUpsert("/Root/T", EDataFormat::CSV, "v\n10\n20\n", {},
                NYdb::NTable::TBulkUpsertSettings().FormatSettings(csv.SerializeAsString())).GetValueSync();
        }
    }

    Y_ABORT("Unknown load kind");
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSerialColumnShard) {
    Y_UNIT_TEST(DisabledByDefault, ESerialType) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        const auto result = client.ExecuteQuery("CREATE TABLE T (id " + ToString(Arg<0>()) + ", PRIMARY KEY(id)) WITH (STORE = COLUMN)",
            TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default sequences are not supported in column tables");
    }

    Y_UNIT_TEST(RequiresPrimaryKey, ESerialType) {
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        const auto result = client.ExecuteQuery(
            "CREATE TABLE T (id Int32 NOT NULL, v " + ToString(Arg<0>()) + ", PRIMARY KEY(id)) WITH (STORE = COLUMN)",
            TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "must be a NOT NULL primary key column");
    }

    Y_UNIT_TEST(GeneratedAndExplicitKeys, EQueryMode, ETableKind, ELoadKind) {
        const auto mode = Arg<0>();
        const auto store = Arg<1>() == COLUMNSHARD ? "COLUMN" : "ROW";
        TTestHelper helper(SerialSettings());
        auto& kikimr = helper.GetKikimr();
        auto client = kikimr.GetQueryClient();
        const TString columns = "id Serial, v Int32 NOT NULL, PRIMARY KEY(id)";
        CreateSerialTable(client, "T", columns, store);
        auto& flags = kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags;
        flags.SetEnableColumnshardSerial(false);
        Execute(client, "INSERT INTO T (v) VALUES (10)");
        Execute(client, "UPSERT INTO T (id, v) VALUES (-100, 20)");
        auto tableClient = kikimr.GetTableClient();
        const auto bulk = BulkUpsertExplicitKey(tableClient, Arg<2>());
        UNIT_ASSERT_C(bulk.IsSuccess(), bulk.GetIssues().ToString());
        Execute(client, "REPLACE INTO T (v) VALUES (30)");
        Execute(client, "UPSERT INTO T (v) VALUES (40)");
        CheckOrExec(helper, "SELECT id, v FROM T ORDER BY id", "[[-200;50];[-100;20];[1;10];[2;30];[3;40]]", mode);

        auto drop = client.ExecuteQuery("DROP TABLE T", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
        flags.SetEnableColumnshardSerial(true);
        CreateSerialTable(client, "T", columns, store);
        Execute(client, "UPSERT INTO T (v) VALUES (50)");
        CheckOrExec(helper, "SELECT id, v FROM T", "[[1;50]]", mode);
    }

    Y_UNIT_TEST(BulkUpsertRequiresSerialKey, ETableKind, ELoadKind) {
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        CreateSerialTable(client, "T", "id Serial, v Int32 NOT NULL, PRIMARY KEY(id)", Arg<0>() == COLUMNSHARD ? "COLUMN" : "ROW");

        auto tableClient = kikimr.GetTableClient();
        const auto load = Arg<1>();
        const auto result = BulkUpsertWithoutKey(tableClient, load);
        const auto issues = result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), load == CSV ? EStatus::BAD_REQUEST : EStatus::SCHEME_ERROR, issues);
        UNIT_ASSERT_STRING_CONTAINS(issues, load == CSV ? "Missing not null column: id" : "Missing key columns: id");

        Execute(client, "INSERT INTO T (v) VALUES (30)");
        CompareYson("[[1;30]]", FormatResultSetYson(Execute(client, "SELECT id, v FROM T ORDER BY id").GetResultSet(0)));
    }

    Y_UNIT_TEST(RenamePreservesSequence, ESerialType) {
        auto settings = SerialSettings();
        settings.FeatureFlags.SetEnableMoveColumnTable(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        CreateSerialTable(client, "T", "id " + ToString(Arg<0>()) + ", v Int32 NOT NULL, PRIMARY KEY(id)", "COLUMN");
        Execute(client, "UPSERT INTO T (v) VALUES (10)");
        auto rename = client.ExecuteQuery("ALTER TABLE T RENAME TO Moved", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(rename.IsSuccess(), rename.GetIssues().ToString());
        Execute(client, "UPSERT INTO Moved (v) VALUES (20)");
        CompareYson("[[1;10];[2;20]]", FormatResultSetYson(Execute(client, "SELECT id, v FROM Moved ORDER BY id").GetResultSet(0)));

        const auto show = Execute(client, "SHOW CREATE TABLE Moved");
        TResultSetParser parser(show.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto ddl = parser.ColumnParser("CreateQuery").GetOptionalUtf8();
        UNIT_ASSERT(ddl);
        UNIT_ASSERT_STRING_CONTAINS(*ddl, GetSerialTypeInfo(Arg<0>()).SqlAlias);
        UNIT_ASSERT_STRING_CONTAINS(*ddl, "_serial_column_id");
        UNIT_ASSERT_STRING_CONTAINS(*ddl, "RESTART WITH 3");
    }

    Y_UNIT_TEST(ArrowMatchesDataShard, ESerialType) {
        const auto type = Arg<0>();
        const auto info = GetSerialTypeInfo(type);
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        const TString columns = "a " + ToString(type) + ", b " + info.SqlAlias + ", v Int32 NOT NULL, PRIMARY KEY(a, b)";
        CreateSerialTable(client, "RowKeys", columns, "ROW");
        CreateSerialTable(client, "ColumnKeys", columns, "COLUMN");
        Execute(client, TStringBuilder() << "UPSERT INTO RowKeys (a,b,v) VALUES (" << (-info.MaxValue - 1) << "," << info.MaxValue << ",7)");

        const auto arrowSettings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);
        const auto read = [&](const TString& table) {
            auto result = client.ExecuteQuery("SELECT a,b,v FROM " + table + " ORDER BY a",
                TTxControl::BeginTx().CommitTx(), arrowSettings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetResultSet(0);
        };

        const auto row = read("RowKeys");
        const auto& wireSchema = TArrowAccessor::GetArrowSchema(row);
        const auto schema = NArrow::DeserializeSchema(TString(wireSchema));
        UNIT_ASSERT(schema);
        UNIT_ASSERT_VALUES_EQUAL(schema->num_fields(), 3);
        UNIT_ASSERT_C(schema->field(0)->type()->id() == info.ArrowType, schema->field(0)->ToString());
        UNIT_ASSERT_C(schema->field(1)->type()->id() == info.ArrowType, schema->field(1)->ToString());
        UNIT_ASSERT_C(schema->field(2)->type()->id() == arrow::Type::INT32, schema->field(2)->ToString());

        const auto& batches = TArrowAccessor::GetArrowBatches(row);
        UNIT_ASSERT_VALUES_EQUAL(batches.size(), 1);
        auto bulk = kikimr.GetTableClient().BulkUpsert("/Root/ColumnKeys", NYdb::NTable::EDataFormat::ApacheArrow, batches.front(), wireSchema).GetValueSync();
        UNIT_ASSERT_C(bulk.IsSuccess(), bulk.GetIssues().ToString());

        const auto col = read("ColumnKeys");
        const auto colSchema = NArrow::DeserializeSchema(TString(TArrowAccessor::GetArrowSchema(col)));
        UNIT_ASSERT(colSchema && colSchema->Equals(*schema));
        const auto& colBatches = TArrowAccessor::GetArrowBatches(col);
        UNIT_ASSERT_VALUES_EQUAL(colBatches.size(), 1);
        const auto expected = NArrow::DeserializeBatch(TString(batches.front()), schema);
        const auto actual = NArrow::DeserializeBatch(TString(colBatches.front()), colSchema);
        UNIT_ASSERT(actual && expected && actual->Equals(*expected));

        Execute(client, "UPSERT INTO ColumnKeys (v) VALUES (8)");
        const auto generated = Execute(client, "SELECT a,b FROM ColumnKeys WHERE v = 8");
        CompareYson("[[1;1]]", FormatResultSetYson(generated.GetResultSet(0)));
    }

    Y_UNIT_TEST(SequenceOverflow, ESerialType) {
        const auto info = GetSerialTypeInfo(Arg<0>());
        TKikimrRunner kikimr(SerialSettings());
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        NYdb::NTable::TSequenceDescription sequence;
        sequence.SetVal = NYdb::NTable::TSequenceDescription::TSetVal{info.MaxValue, false};
        auto description = NYdb::NTable::TTableBuilder()
            .SetStoreType(NYdb::NTable::EStoreType::Column)
            .AddSerialColumn("id", info.PrimitiveType, sequence)
            .AddNonNullableColumn("v", EPrimitiveType::Int32)
            .SetPrimaryKeyColumn("id")
            .Build();
        const auto create = session.CreateTable("/Root/T", std::move(description)).GetValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto describe = session.DescribeTable("/Root/T").GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
        const auto columns = describe.GetTableDescription().GetTableColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.front().Name, "id");
        UNIT_ASSERT(columns.front().SequenceDescription.has_value());

        auto client = kikimr.GetQueryClient();
        Execute(client, "UPSERT INTO T (v) VALUES (1)");
        CompareYson(TStringBuilder() << "[[" << info.MaxValue << "]]", FormatResultSetYson(Execute(client, "SELECT id FROM T").GetResultSet(0)));
        const auto overflow = client.ExecuteQuery("UPSERT INTO T (v) VALUES (2)", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!overflow.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(overflow.GetIssues().ToString(), "sequence");
        CompareYson(TStringBuilder() << "[[" << info.MaxValue << ";1]]", FormatResultSetYson(Execute(client, "SELECT id, v FROM T").GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
