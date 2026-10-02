#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRollback) {
    Y_UNIT_TEST(DoubleUpdate) {

        // Given
        TKikimrSettings sts;
        sts.SetWithSampleTables(false);
        sts.SetColumnShardReaderClassName("SIMPLE");
        sts.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);        
        TKikimrRunner kikimr(sts);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/KVR` (
                id Uint64 NOT NULL,
                vn Int32,
                vs String,
                PRIMARY KEY (id)
            )
            WITH (
                STORE = COLUMN
            );
        )").GetValueSync());


        auto rowsBuilder = NYdb::TValueBuilder();
        rowsBuilder.BeginList();
        for (ui32 i = 0; i < 100; ++i) {
            rowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("id")
                    .Uint64(i)
                .AddMember("vn")
                    .Int32(i)
                .AddMember("vs")
                    .String(TString(3, '0' + i % 10))
                .EndStruct();
        }
        rowsBuilder.EndList();

        auto buRes = db.BulkUpsert("/Root/KVR", rowsBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(buRes.GetStatus(), NYdb::EStatus::SUCCESS, buRes.GetIssues().ToString());

        // When 0
        auto result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/KVR` SET vs = 'whatever' WHERE id = 2u;
        )"), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        auto tx1 = result.GetTransaction();
        auto rollbackResult = tx1->Rollback().GetValueSync();
        // Then 0
        UNIT_ASSERT_VALUES_EQUAL_C(rollbackResult.GetStatus(), EStatus::SUCCESS, rollbackResult.GetIssues().ToString());

        // When 1
        result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/KVR` SET vs = 'xyz';
        )"), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        auto tx2 = result.GetTransaction();
        rollbackResult = tx2->Rollback().GetValueSync();
        // Then 1
        UNIT_ASSERT_VALUES_EQUAL_C(rollbackResult.GetStatus(), EStatus::SUCCESS, rollbackResult.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
