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

using namespace NStat;

Y_UNIT_TEST_TWIN(AnalyzeTable, ColumnStore) {
    TTestEnv env(1, 1, 1, true);
    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    TString createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )", "Root/Database/Table");
    if (ColumnStore) {
        createTable += 
            R"(
                PARTITION BY HASH(Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16
                )
            )";
    }

    auto result = session.ExecuteSchemeQuery(createTable).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    if (ColumnStore) {
        result = session.ExecuteSchemeQuery(
            Sprintf(R"(
                    ALTER OBJECT `%s` (TYPE TABLE) 
                    SET (
                        ACTION=UPSERT_INDEX, 
                        NAME=cms_value, 
                        TYPE=COUNT_MIN_SKETCH,
                        FEATURES=`{"column_names" : ['Value']}`
                    );
                )", "Root/Database/Table"
            )
        ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

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

    if (ColumnStore) {
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    } else {
        UNIT_ASSERT(!result.IsSuccess());
        auto issues = result.GetIssues().ToString();
        UNIT_ASSERT_C(issues.find("analyze is not supported for oltp tables.") != TString::npos, issues);
        return;
    }

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
