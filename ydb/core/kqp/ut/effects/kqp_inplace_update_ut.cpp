#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpInplaceUpdate) {

void PrepareTable(TSession& session) {
    auto ret = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/InplaceUpdate` (
            Key Uint64,
            ValueStr String,
            ValueInt Uint64,
            ValueDbl Double,
            PRIMARY KEY(Key)
        ) WITH (
            PARTITION_AT_KEYS = (10)
        )
    )").ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(ret.GetStatus(), EStatus::SUCCESS, ret.GetIssues().ToString());

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/InplaceUpdate` (Key, ValueStr, ValueInt, ValueDbl) VALUES
            (1, "One", 100, 101.0),
            (20, "Two", 200, 202.0)
    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void PreparePgTable(TSession& session) {
    auto ret = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/InplaceUpdate` (
            Key Uint64,
            ValueStr String,
            ValueInt PgInt2,
            ValueDbl Double,
            PRIMARY KEY(Key)
        ) WITH (
            PARTITION_AT_KEYS = (10)
        )
    )").ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(ret.GetStatus(), EStatus::SUCCESS, ret.GetIssues().ToString());

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/InplaceUpdate` (Key, ValueStr, ValueInt, ValueDbl) VALUES
            (1, "One", PgInt2(100), 101.0),
            (20, "Two", PgInt2(200), 202.0)
    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void Test(
    const TString& query,
    TParams&& params,
    const TString& expectedResult,
    std::function<void(const Ydb::TableStats::QueryStats&)>&& check,
    const std::function<void(TSession& session)>& prepareTable = &PrepareTable
) {
    auto setting = NKikimrKqp::TKqpSetting();
    setting.SetName("_KqpAllowUnsafeCommit");
    setting.SetValue("true");

    // source read and stream lookup use iterator interface, that doesn't use datashard transactions
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(false);

    auto settings = TKikimrSettings()
        .SetAppConfig(appConfig)
        .SetKqpSettings({setting});

    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    prepareTable(session);

    TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    auto q = TStringBuilder()
        << R"(
            --!syntax_v1
            PRAGMA kikimr.OptEnableInplaceUpdate = ')" <<  "true" << "';" << Endl
         << query;

    auto result = session.ExecuteDataQuery(q, TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    check(NYdb::TProtoAccessor::GetProto(*result.GetStats()));

    result = session.ExecuteDataQuery(R"(
        SELECT Key, ValueStr, ValueInt, ValueDbl FROM `/Root/InplaceUpdate` ORDER BY Key;
    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    CompareYson(expectedResult, FormatResultSetYson(result.GetResultSet(0)));
}

#define ASSERT_LITERAL_PHASE(stats, phaseNo) \
    UNIT_ASSERT_C(stats.query_phases(phaseNo).table_access().empty(), stats.DebugString());

#define ASSERT_PHASE(stats, phaseNo, table, readsCnt, updatesCnt) \
    UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(phaseNo).table_access().size(), 1, stats.DebugString());                     \
    UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(phaseNo).table_access(0).name(), table, stats.DebugString());                \
    UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(phaseNo).table_access(0).reads().rows(), readsCnt, stats.DebugString());     \
    UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(phaseNo).table_access(0).updates().rows(), updatesCnt, stats.DebugString()); \
    UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(phaseNo).table_access(0).partitions_count(), std::max(readsCnt, updatesCnt), stats.DebugString());


Y_UNIT_TEST(SingleRowSimple) {
    Test(
        R"( DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPDATE `/Root/InplaceUpdate` SET
                ValueStr = $value
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("updated").Build()
            .Build(),
        R"([
            [[1u];["updated"];[100u];[101.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

Y_UNIT_TEST(SingleRowStr) {
    Test(
        R"( DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPDATE `/Root/InplaceUpdate` SET
                ValueStr = Substring(ValueStr || $value, 1)
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("updated").Build()
            .Build(),
        R"([
            [[1u];["neupdated"];[100u];[101.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

Y_UNIT_TEST(SingleRowArithm) {
    Test(
        R"( DECLARE $key AS Uint64;
            DECLARE $x AS Uint64;
            DECLARE $y AS Double;

            UPDATE `/Root/InplaceUpdate` SET
                ValueInt = (ValueInt + $x) * ($x + 1ul),
                ValueDbl = (ValueDbl - $y) / ($y + 1.)
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$x").Uint64(10).Build()
            .AddParam("$y").Double(5.0).Build()
            .Build(),
        R"([
            [[1u];["One"];[1210u];[16.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

Y_UNIT_TEST(SingleRowIf) {
    Test(
        R"( DECLARE $key AS Uint64;

            $trim = ($v, $min, $max) -> {
                RETURN CASE
                    WHEN $v > $max THEN $max
                    WHEN $v < $min THEN $min
                    ELSE $v
                END;
            };

            UPDATE `/Root/InplaceUpdate` SET
                ValueInt = $trim(ValueInt, 0ul, 10ul) + 1ul,
                ValueDbl = $trim(ValueDbl, 0., 10.) / 10.0
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .Build(),
        R"([
            [[1u];["One"];[11u];[1.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

// allow multiple keys in KqpLookupTable to enable this test
Y_UNIT_TEST(Negative_SingleRowWithKeyCast) {
    Test(
        R"( DECLARE $key AS Uint32; -- not Uint64
            DECLARE $value AS String;

            UPDATE `/Root/InplaceUpdate` SET
                ValueStr = $value
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint32(1).Build()
            .AddParam("$value").String("updated").Build()
            .Build(),
        R"([
            [[1u];["updated"];[100u];[101.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            // if constexpr (EnableInplaceUpdate) {
            //     UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            //     ASSERT_LITERAL_PHASE(stats, 0);
            //     ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 1, 1);
            // } else {
                UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 3, stats.DebugString());
                ASSERT_LITERAL_PHASE(stats, 0);
                ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 1, 0);
                ASSERT_PHASE(stats, 2, "/Root/InplaceUpdate", 0, 1);
            // }
        });
}

Y_UNIT_TEST(Negative_SingleRowWithValueCast) {
/*
    (
    (declare $key (DataType 'Uint64))
    (declare $value (DataType 'Int32))
    (let $1 (KqpTable '"/Root/InplaceUpdate" '"72057594046644480:11" '"" '1))
    (let $2 (DataType 'Uint64))
    (let $3 (KqpLookupTable $1 (Iterator (AsList (AsStruct '('"Key" $key)))) '('"Key")))
    (return (FlatMap $3 (lambda '($4) (Just (AsStruct '('"Key" (Member $4 '"Key")) '('"ValueInt" (Just (Convert $value $2))))))))
    )

    `Convert` is not safe callable, so there is no InplaceUpdate optimization here
*/
    Test(
        R"( DECLARE $key AS Uint64;
            DECLARE $value AS Int32; -- not Uint64

            UPDATE `/Root/InplaceUpdate` SET
                ValueInt = $value
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").Int32(1).Build()
            .Build(),
        R"([
            [[1u];["One"];[1u];[101.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

Y_UNIT_TEST(Negative_SingleRowListFromRange) {
    Test(
        R"( DECLARE $key AS Uint64;

            $foo = ($x) -> {
                $list = ListFromRange(1, 10);
                RETURN $x || ListConcat(Cast($list as List<String>), "..");
            };

            UPDATE `/Root/InplaceUpdate` SET
                ValueStr = $foo(ValueStr)
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .Build(),
        R"([
            [[1u];["One1..2..3..4..5..6..7..8..9"];[100u];[101.]];
            [[20u];["Two"];[200u];[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        });
}

// allow multiple keys in KqpLookupTable to enable this test
Y_UNIT_TEST(Negative_BatchUpdate) {
    Test(
        R"( DECLARE $key1 AS Uint64;
            DECLARE $value1 AS String;
            DECLARE $key2 AS Uint64;
            DECLARE $value2 AS String;

            $foo = ($x, $k1, $v1, $v2) -> {
                RETURN CASE $x
                    WHEN $k1 THEN $v1
                    ELSE $v2
                END;
            };

            UPDATE `/Root/InplaceUpdate` SET
                ValueStr = $foo(Key, $key1, $value1, $value2)
            WHERE Key IN [$key1, $key2]
        )",
        TParamsBuilder()
            .AddParam("$key1").Uint64(1).Build()
            .AddParam("$value1").String("updated-1").Build()
            .AddParam("$key2").Uint64(20).Build()
            .AddParam("$value2").String("updated-2").Build()
            .Build(),
        R"([
            [[1u];["updated-1"];[100u];[101.]];
            [[20u];["updated-2"];[200u];[202.]]
        ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            // if constexpr (EnableInplaceUpdate) {
            //     UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 3, stats.DebugString());
            //     ASSERT_LITERAL_PHASE(stats, 0);
            //     ASSERT_LITERAL_PHASE(stats, 1);
            //     ASSERT_PHASE(stats, 2, "/Root/InplaceUpdate", 2, 2);
            // } else {
                UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 3, stats.DebugString());
                ASSERT_LITERAL_PHASE(stats, 0);
                ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 2, 0);
                ASSERT_PHASE(stats, 2, "/Root/InplaceUpdate", 0, 2);
            // }
        });
}

Y_UNIT_TEST(BigRow) {
    auto keysLimitSetting = NKikimrKqp::TKqpSetting();
    keysLimitSetting.SetName("_CommitPerShardKeysSizeLimitBytes");
    keysLimitSetting.SetValue("100");

    auto unsafeCommitSetting = NKikimrKqp::TKqpSetting();
    unsafeCommitSetting.SetName("_KqpAllowUnsafeCommit");
    unsafeCommitSetting.SetValue("true");

    // source read use iterator interface, that doesn't use datashard transactions
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(false);

    auto settings = TKikimrSettings()
        .SetAppConfig(appConfig)
        .SetKqpSettings({keysLimitSetting, unsafeCommitSetting});

    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/Temp` (
            Key Uint32,
            Value1 String,
            Value2 String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync().IsSuccess());

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/Temp` (Key, Value1) VALUES
            (1u, "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
            (3u, "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");
    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    auto query = Sprintf(R"(
        PRAGMA kikimr.OptEnableInplaceUpdate = '%s';

        DECLARE $Key AS Uint32;

        UPDATE `/Root/Temp` SET
            Value2 = Value1
        WHERE Key = $Key
    )", "true");

    auto params = db.GetParamsBuilder()
        .AddParam("$Key").Uint32(1).Build()
        .Build();

    result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    result = session.ExecuteDataQuery(R"(
        SELECT Value2 FROM `/Root/Temp` ORDER BY Value2;
    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    CompareYson(R"([
        [#];[["123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"]]
    ])", FormatResultSetYson(result.GetResultSet(0)));
}

Y_UNIT_TEST(SingleRowPgNotNull) {
    Test(
        R"( DECLARE $key AS Uint64;
            DECLARE $value AS PgInt2;

            UPDATE `/Root/InplaceUpdate` SET
                ValueInt = $value
            WHERE Key = $key
        )",
        TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").Pg(TPgValue(TPgValue::VK_TEXT, "123", TPgType("pgint2"))).Build()
            .Build(),
        R"([
            [[1u];["One"];"123";[101.]];
            [[20u];["Two"];"200";[202.]]
           ])",
        [](const Ydb::TableStats::QueryStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 2, stats.DebugString());
            ASSERT_PHASE(stats, 0, "/Root/InplaceUpdate", 1, 0);
            ASSERT_PHASE(stats, 1, "/Root/InplaceUpdate", 0, 1);
        },
        &PreparePgTable);
}

} // suite

} // namespace NKqp
} // namespace NKikimr
