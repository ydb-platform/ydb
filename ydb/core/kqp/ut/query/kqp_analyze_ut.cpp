#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <thread>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NTable;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpAnalyze) {

void CreateTable(NStat::TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();
    
    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64 NOT NULL,
            Value String,
            PRIMARY KEY (Key)
        )
    )", fullTableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 1000; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalString("Hello, world!").Build();
        
        rows.AddListItem();
            rows.BeginStruct();
                rows.AddMember("Key", key);
                rows.AddMember("Value", value);
            rows.EndStruct();
    }
    rows.EndList();

    result = client.BulkUpsert(fullTableName, rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

using namespace NStat;

Y_UNIT_TEST(AnalyzeDatashardTable) {
    TTestEnv env(1, 1, 1, true);
    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(
        Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )", "Root/Database/Table")
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 1500; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalString("Hello,world!").Build();
        
        rows.AddListItem();
            rows.BeginStruct();
                rows.AddMember("Key", key);
                rows.AddMember("Value", value);
            rows.EndStruct();
    }
    rows.EndList();
    
    result = client.BulkUpsert("Root/Database/Table", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = session.ExecuteSchemeQuery(
        Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "Table")
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto& runtime = *env.GetServer().GetRuntime();
    ui64 saTabletId;
    auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
    
    auto countMin = ExtractCountMin(runtime, pathId, 2);
    TString value = "Hello,world!";
    auto stat = countMin->Probe(value.Data(), value.Size());
    UNIT_ASSERT_C(stat >= 1500, ToString(stat));
}


} // suite


} // namespace NKqp
} // namespace NKikimr
