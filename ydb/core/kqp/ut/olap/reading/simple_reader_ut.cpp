#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <util/string/printf.h>

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

    Y_UNIT_TEST(TrivialReaderLimitTimestampKernelFilter) {
        TKikimrSettings sts;
        sts.SetWithSampleTables(false);
        sts.SetColumnShardReaderClassName("TRIVIAL");
        sts.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        TKikimrRunner kikimr(sts);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            CREATE TABLE `/Root/TsKernelLimit` (
                ts Timestamp NOT NULL,
                req_id Utf8 NOT NULL,
                user_sid Utf8,
                PRIMARY KEY (ts, req_id)
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // one portion per INSERT, so SYNC_LIMIT has to compare a fetched portion with the start key of the next one
        for (ui32 i = 1; i <= 6; ++i) {
            result = session.ExecuteQuery(Sprintf(R"(
                INSERT INTO `/Root/TsKernelLimit` (ts, req_id, user_sid)
                VALUES (Timestamp("2020-01-0%uT00:00:00Z"), "r%u", "user");
            )", i, i), TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // `ts >= ... OR user_sid == ...` is not range-extractable, so the whole predicate is pushed
        // into the SSA program and evaluated by a YQL kernel over the `ts` column.
        result = session.ExecuteQuery(R"(
            SELECT ts, req_id FROM `/Root/TsKernelLimit`
            WHERE ts >= Timestamp("2020-01-01T00:00:00Z") OR user_sid == "nobody"
            ORDER BY ts DESC
            LIMIT 3;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(
            R"([[1578268800000000u;"r6"];[1578182400000000u;"r5"];[1578096000000000u;"r4"]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
