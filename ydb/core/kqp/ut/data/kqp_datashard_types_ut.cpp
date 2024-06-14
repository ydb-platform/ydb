#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpDatashardTypes) {
    Y_UNIT_TEST(Time64Columns) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE `/Root/TestTime64` (
                    DatetimePK Datetime64,
                    IntervalPK Interval64,
                    TimestampPK Timestamp64,
                    Datetime Datetime64,
                    Interval Interval64,
                    Timestamp Timestamp64,
                    PRIMARY KEY (DatetimePK, IntervalPK, TimestampPK))
            )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                UPSERT INTO `/Root/TestTime64` (DatetimePK, IntervalPK, TimestampPK, Datetime, Interval, Timestamp) VALUES
                    (Datetime64('1970-01-01T01:01:01Z'), Interval64('P222D'), Timestamp64('1970-03-03T03:03:03Z')
                    ,Datetime64('1970-04-04T04:04:04Z'), Interval64('P555D'), Timestamp64('1970-06-06T06:06:06Z'));
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_("SELECT * FROM `/Root/TestTime64`");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[8049844];[3661];[47952000000000];[19180800000000];[13500366000000];[5281383000000]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
