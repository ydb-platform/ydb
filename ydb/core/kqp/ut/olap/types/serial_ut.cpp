#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

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

} // namespace

Y_UNIT_TEST_SUITE(KqpSerialColumnShard) {
    Y_UNIT_TEST(DisabledByDefault) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        const auto result = client.ExecuteQuery("CREATE TABLE T (id Serial, PRIMARY KEY(id)) WITH (STORE = COLUMN)",
            TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default sequences are not supported in column tables");
    }

    Y_UNIT_TEST(RequiresPrimaryKey) {
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        const auto result = client.ExecuteQuery(
            "CREATE TABLE T (id Int32 NOT NULL, v Serial, PRIMARY KEY(id)) WITH (STORE = COLUMN)",
            TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "must be a NOT NULL primary key column");
    }

    Y_UNIT_TEST(GeneratedAndExplicitKeys) {
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        const TString columns = "id Serial, v Int32 NOT NULL, PRIMARY KEY(id)";
        CreateSerialTable(client, "T", columns, "COLUMN");
        auto& flags = kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags;
        flags.SetEnableColumnshardSerial(false);
        Execute(client, "INSERT INTO T (v) VALUES (10)");
        Execute(client, "UPSERT INTO T (id, v) VALUES (-100, 20)");
        Execute(client, "REPLACE INTO T (v) VALUES (30)");
        Execute(client, "UPSERT INTO T (v) VALUES (40)");
        const auto rows = Execute(client, "SELECT id, v FROM T ORDER BY id");
        CompareYson("[[-100;20];[1;10];[2;30];[3;40]]", FormatResultSetYson(rows.GetResultSet(0)));

        auto drop = client.ExecuteQuery("DROP TABLE T", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
        flags.SetEnableColumnshardSerial(true);
        CreateSerialTable(client, "T", columns, "COLUMN");
        Execute(client, "UPSERT INTO T (v) VALUES (50)");
        CompareYson("[[1;50]]", FormatResultSetYson(Execute(client, "SELECT id, v FROM T").GetResultSet(0)));
    }

    Y_UNIT_TEST(RenamePreservesSequence) {
        auto settings = SerialSettings();
        settings.FeatureFlags.SetEnableMoveColumnTable(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        CreateSerialTable(client, "T", "id Serial, v Int32 NOT NULL, PRIMARY KEY(id)", "COLUMN");
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
        UNIT_ASSERT_STRING_CONTAINS(*ddl, "Serial4");
        UNIT_ASSERT_STRING_CONTAINS(*ddl, "_serial_column_id");
        UNIT_ASSERT_STRING_CONTAINS(*ddl, "RESTART WITH 3");
    }

    Y_UNIT_TEST(ArrowMatchesDataShard) {
        TKikimrRunner kikimr(SerialSettings());
        auto client = kikimr.GetQueryClient();
        const TString columns = "a SmallSerial, b Serial2, c Serial, d Serial4, e BigSerial, f Serial8, "
            "v Int32 NOT NULL, PRIMARY KEY(a, b, c, d, e, f)";
        CreateSerialTable(client, "RowKeys", columns, "ROW");
        CreateSerialTable(client, "ColumnKeys", columns, "COLUMN");
        Execute(client, "UPSERT INTO RowKeys (a,b,c,d,e,f,v) VALUES (-32768,32767,-2147483648,2147483647,-4294967296,4294967296,7)");

        const auto arrowSettings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);
        const auto read = [&](const TString& table) {
            auto result = client.ExecuteQuery("SELECT a,b,c,d,e,f,v FROM " + table + " ORDER BY a",
                TTxControl::BeginTx().CommitTx(), arrowSettings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetResultSet(0);
        };

        const auto row = read("RowKeys");
        const auto& wireSchema = TArrowAccessor::GetArrowSchema(row);
        const auto schema = NArrow::DeserializeSchema(TString(wireSchema));
        UNIT_ASSERT(schema);
        const TVector<arrow::Type::type> types = {arrow::Type::INT16, arrow::Type::INT16, arrow::Type::INT32,
            arrow::Type::INT32, arrow::Type::INT64, arrow::Type::INT64, arrow::Type::INT32};
        UNIT_ASSERT_VALUES_EQUAL(schema->num_fields(), types.size());
        for (int i = 0; i < schema->num_fields(); ++i) {
            UNIT_ASSERT_C(schema->field(i)->type()->id() == types[i], schema->field(i)->ToString());
        }

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
        const auto generated = Execute(client, "SELECT a,b,c,d,e,f FROM ColumnKeys WHERE v = 8");
        CompareYson("[[1;1;1;1;1;1]]", FormatResultSetYson(generated.GetResultSet(0)));
    }

    Y_UNIT_TEST(SmallSerialOverflow) {
        TKikimrRunner kikimr(SerialSettings());
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        NYdb::NTable::TSequenceDescription sequence;
        sequence.SetVal = NYdb::NTable::TSequenceDescription::TSetVal{32767, false};
        auto description = NYdb::NTable::TTableBuilder()
            .SetStoreType(NYdb::NTable::EStoreType::Column)
            .AddSerialColumn("id", EPrimitiveType::Int16, sequence)
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
        CompareYson("[[32767]]", FormatResultSetYson(Execute(client, "SELECT id FROM T").GetResultSet(0)));
        const auto overflow = client.ExecuteQuery("UPSERT INTO T (v) VALUES (2)", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!overflow.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(overflow.GetIssues().ToString(), "sequence");
        CompareYson("[[32767;1]]", FormatResultSetYson(Execute(client, "SELECT id, v FROM T").GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
