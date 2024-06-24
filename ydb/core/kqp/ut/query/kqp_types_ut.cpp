#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTypes) {
    Y_UNIT_TEST(QuerySpecialTypes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT null;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT [];
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT {};
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }


    Y_UNIT_TEST(UnsafeTimestampCastV0) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TsTest` (
                Key Timestamp,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const TString query = R"(
            --!syntax_v0
            DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPSERT INTO `/Root/TsTest` (Key, Value) VALUES
                ($key, $value);
        )";

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("foo").Build()
            .Build();

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx(),
            params
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(UnsafeTimestampCastV1) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TsTest` (
                Key Timestamp,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const TString query = Q1_(R"(
            DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPSERT INTO `/Root/TsTest` (Key, Value) VALUES
                ($key, $value);
        )");

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("foo").Build()
            .Build();

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx(),
            params
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }    

    Y_UNIT_TEST(DyNumberCompare) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Compare DyNumber
        auto result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);
            $dn2 = CAST("10.2" AS DyNumber);

            SELECT
                $dn1 = $dn2,
                $dn1 != $dn2,
                $dn1 > $dn2,
                $dn1 <= $dn2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[%false];[%true];[%true];[%false]]])", FormatResultSetYson(result.GetResultSet(0)));

        // Compare to float
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);

            SELECT
                $dn1 = 13.1,
                $dn1 != 13.1,
                $dn1 > 10.2,
                $dn1 <= 10.2,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        // Compare to int
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("15" AS DyNumber);

            SELECT
                $dn1 = 15,
                $dn1 != 15,
                $dn1 > 10,
                $dn1 <= 10,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        // Compare to decimal
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);
            $dc1 = CAST("13.1" AS Decimal(22,9));

            SELECT
                $dn1 = $dc1,
                $dn1 != $dc1,
                $dn1 > $dc1,
                $dn1 <= $dc1,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(SelectNull) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Null
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        auto rs = TResultSetParser(result.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());

        auto& cp = rs.ColumnParser(0);

        UNIT_ASSERT_VALUES_EQUAL(TTypeParser::ETypeKind::Null, cp.GetKind());
    }

    Y_UNIT_TEST(MultipleCurrentUtcTimestamp) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q1_(R"(
            SELECT * FROM `/Root/Logs` WHERE Ts > Cast(CurrentUtcTimestamp() as Int64)
            UNION ALL
            SELECT * FROM `/Root/Logs` WHERE Ts < Cast(CurrentUtcTimestamp() as Int64);
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }


    Y_UNIT_TEST_TWIN(Time64Columns, EnableTableDatetime64) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTableDatetime64(EnableTableDatetime64);

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetFeatureFlags(featureFlags);

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
            
            if constexpr (EnableTableDatetime64) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("Type 'Datetime64' specified for column 'DatetimePK', but support for new date/time 64 types is disabled (EnableTableDatetime64 feature flag is off)"));
                return;
            }
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
