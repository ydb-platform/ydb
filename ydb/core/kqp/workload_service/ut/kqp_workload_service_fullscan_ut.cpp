#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>

#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(FullScan) {
    Y_UNIT_TEST(TestReject) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();
        auto db = ydb->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create tables
        auto r = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/test_table` (
                Key Uint64,
                Value Utf8,
                PRIMARY KEY (Key)
            ) WITH (
                PARTITION_AT_KEYS = (100, 200, 300)
            );
        )").GetValueSync();

        UNIT_ASSERT(r.IsSuccess());

        r = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/other_table` (
                Key Uint64,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();

        // Classifier with FULL_SCAN reject for /Root/test_table
        r = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER fs_classifier WITH (
                RESOURCE_POOL="_reject",
                FULL_SCAN_ON="/Root/test_table"
            );
        )").GetValueSync();

        UNIT_ASSERT(r.IsSuccess());

        r = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/test_table` (Key, Value) 
            VALUES 
                (10,  'value'), (50,  'value'),  -- Partition 1
                (110, 'value'), (120, 'value'),  -- Partition 2
                (210, 'value'), (220, 'value'),  -- Partition 3
                (310, 'value'), (320, 'value');  -- Partition 4
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT(r.IsSuccess());

        Sleep(TDuration::Seconds(4));

        // Case 1: SELECT * (no WHERE) -> full scan -> should reject
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/test_table`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Query is rejected by classifier");
        }

        // Case 2: Range read (partial) -> NOT a full scan -> should succeeded
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/test_table` WHERE Key > 100;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Case 3: Point lookup -> NOT a full scan -> should succeeded
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/test_table` WHERE Key = 42;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Case 4: Full scan on a different table -> should succeeded
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/other_table`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }
}

} // namespace NKikimr::NKqp
