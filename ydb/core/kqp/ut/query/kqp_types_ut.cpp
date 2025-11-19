#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>

#include <string_view>
#include <ranges>

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

    enum class DataType {
        DateTime64,
        Decimal
    };

    void CheckTypeIsRestricted(DataType type, bool isColumn, bool isAllowed) {

        NKikimrConfig::TFeatureFlags featureFlags;
        std::vector<std::tuple<std::string, std::string, std::string, std::string>> nameTypeValueAnswer;
        std::function<std::string(std::string, std::string)> getErrorMessage;

        switch (type) {
            case DataType::DateTime64:
                featureFlags.SetEnableTableDatetime64(isAllowed);
                nameTypeValueAnswer = {
                    {"Datetime", "Datetime64", "Datetime64('1970-01-01T01:01:01Z')", "3661"},
                    {"Interval", "Interval64", "Interval64('P222D')", "19180800000000"},
                    {"Timestamp", "Timestamp64", "Timestamp64('1970-03-03T03:03:03Z')", "5281383000000"}
                };
                getErrorMessage = [](const std::string& name, const std::string& type) {
                    return std::format("Type '{}' specified for column '{}', but support for new date/time 64 types is disabled (EnableTableDatetime64 feature flag is off)", type, name);
                };

                break;
            case DataType::Decimal:
                featureFlags.SetEnableParameterizedDecimal(isAllowed);
                nameTypeValueAnswer = {
                    {"Decimal_15_0", "Decimal(15,0)", "Decimal('12345', 15, 0)", "\"12345\""},
                    {"Decimal_22_9", "Decimal(22,9)", "Decimal('123456.789', 22, 9)", "\"123456.789\""},
                    {"Decimal_35_10", "Decimal(35,10)", "Decimal('123456789.123456', 35, 10)", "\"123456789.123456\""}
                };

                getErrorMessage = [](const std::string& name, const std::string& type) {
                    return std::format("Type '{}' specified for column '{}', but support for parametrized decimal is disabled (EnableParameterizedDecimal feature flag is off)", type, name);
                };
        }

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetFeatureFlags(featureFlags);

        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        std::string tableName = "/Root/Test" + get<0>(nameTypeValueAnswer[0]);

        {
            std::vector<std::string> columns;
            std::vector<std::string> pk;

            for (const auto& [name, type, _value, _answer] : nameTypeValueAnswer) {
                pk.push_back(name + "PK");
                columns.push_back(std::format("{}PK {} NOT NULL", name, type));
            }

            for (const auto& [name, type, _value, _answer] : nameTypeValueAnswer) {
                columns.push_back(std::format("{} {}", name, type));
            }


            const auto query = Q_(std::format(R"(
                CREATE TABLE `{}` (
                    {},
                    PRIMARY KEY ({})
                )
                {}
                )",
                tableName,
                JoinSeq(",\n", columns).c_str(),
                JoinSeq(',', pk).c_str(),
                isColumn ? " WITH (STORE = COLUMN)" : ""
            ));


            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();

            if (isAllowed) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
                auto& [name, type, _value, _answer] = nameTypeValueAnswer[0];
                UNIT_ASSERT_C(result.GetIssues().ToString().contains(getErrorMessage(name + "PK", type)), result.GetIssues().ToString());
            }
        }


        std::vector<std::string> columns;

        for (const auto& [name, _type, _value, _answer] : nameTypeValueAnswer) {
            columns.push_back(std::format("{}PK", name));
        }

        for (const auto& [name, _type, _value, _answer] : nameTypeValueAnswer) {
            columns.push_back(std::format("{}", name));
        }

        if (isAllowed) {
            std::vector<std::string> values;

            for (const auto& [_name, type, value, _answer] : nameTypeValueAnswer) {
                values.push_back(std::format("{}", value));
            }

            values.insert(values.end(), values.begin(), values.end());

            const auto query = Q_(std::format(R"(
                UPSERT INTO `{}` ({}) VALUES
                    ({});
                )",
                tableName,
                JoinSeq(',', columns).c_str(),
                JoinSeq(',', values).c_str()
            ));

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        if (isAllowed) {

            std::vector<std::string> answers;

            for (const auto& [_name, _type, _value, answer] : nameTypeValueAnswer) {
                answers.push_back(std::format("{}", answer));
            }

            for (const auto& [_name, _type, _value, answer] : nameTypeValueAnswer) {
                answers.push_back(std::format("[{}]", answer));
            }

            const auto query = Q_(std::format(R"(
                SELECT {} FROM `{}`)",
                JoinSeq(',', columns).c_str(),
                tableName
            ));

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(std::format(R"([[{}]])", JoinSeq(';', answers).c_str() ), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(std::format(R"(CREATE TABLE `/Root/KeyValue` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)) {}
            )", isColumn ? "WITH (STORE = COLUMN)" : ""));

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            for (const auto& [name, type, _value, answer] : nameTypeValueAnswer) {
                const auto query = Q_(std::format(R"(
                    ALTER TABLE `/Root/KeyValue` ADD COLUMN {} {})", name, type));
                auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();

                if (isAllowed) {
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                }
                else {
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetIssues().ToString().contains(getErrorMessage(name, type)), result.GetIssues().ToString());
                    return;
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(Time64Columns, EnableTableDatetime64, IsColumn) {
        CheckTypeIsRestricted(DataType::DateTime64, IsColumn, EnableTableDatetime64);
    }

    Y_UNIT_TEST_QUAD(ParametrizedDecimalColumns, EnableParameterizedDecimal, IsColumn) {
        CheckTypeIsRestricted(DataType::Decimal, IsColumn, EnableParameterizedDecimal);
    }


}

} // namespace NKqp
} // namespace NKikimr
