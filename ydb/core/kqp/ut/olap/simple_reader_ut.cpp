#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(SimpleReader) {
    Y_UNIT_TEST(NoPKAndLimit) {

        // Given
        TKikimrSettings sts;
        sts.SetWithSampleTables(false);
        sts.SetColumnShardReaderClassName("SIMPLE");
        sts.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);        
        TKikimrRunner kikimr(sts);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            CREATE TABLE `/Root/KV` (
                id Uint64 NOT NULL,
                vn Int32,
                PRIMARY KEY (id)
            )
            WITH (
                STORE = COLUMN
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());        

        result = session.ExecuteQuery(Q_(R"(
            INSERT INTO `/Root/KV` (id, vn) VALUES (1, 11);
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // When
        result = session.ExecuteQuery(Q_(R"(
            SELECT vn FROM `/Root/KV` LIMIT 1;
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();

        // Then
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        CompareYson(R"([[[11]]])", FormatResultSetYson(resultSet));
    }
}

} // namespace NKqp
} // namespace NKikimr
