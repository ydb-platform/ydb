#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpSelect) {
    Y_UNIT_TEST(NoPKAndLimit) {

        // Given
        TKikimrSettings sts;
        sts.SetWithSampleTables(false);
        sts.SetColumnShardReaderClassName("SIMPLE");
        sts.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);        
        TKikimrRunner kikimr(sts);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/KV` (
                id Uint64 NOT NULL,
                vn Int32,
                PRIMARY KEY (id)
            )
            WITH (
                STORE = COLUMN
            );
        )").GetValueSync());

        auto result = session.ExecuteDataQuery(Q_(R"(
            INSERT INTO `/Root/KV` (id, vn) VALUES (1, 11);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        // When
        result = session.ExecuteDataQuery(Q_(R"(
            SELECT vn FROM `/Root/KV` LIMIT 1;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        // Then
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());
    }
}

} // namespace NKqp
} // namespace NKikimr
