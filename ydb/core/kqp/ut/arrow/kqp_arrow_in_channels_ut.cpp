#include <ydb/core/kqp/ut/common/kqp_ut_common.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

TKikimrRunner RunnerWithArrowInChannels() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableArrowFormatInChannels(true);

    return TKikimrRunner{featureFlags};
}

void InsertAllColumnsAndCheckSelectAll(TKikimrRunner* runner) {
    auto db = runner->GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto createResult = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/Tmp` (
            Key Uint64,
            BoolValue Bool,
            Int32Value Int32,
            Uint32Value Uint32,
            Int64Value Int64,
            Uint64Value Uint64,
            FloatValue Float,
            DoubleValue Double,
            StringValue String,
            Utf8Value Utf8,
            DateValue Date,
            DatetimeValue Datetime,
            TimestampValue Timestamp,
            IntervalValue Interval,
            DecimalValue Decimal(22,9),
            JsonValue Json,
            YsonValue Yson,
            JsonDocumentValue JsonDocument,
            DyNumberValue DyNumber,
            PRIMARY KEY (Key)
        );
    )").GetValueSync();
    UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

    auto insertResult = session.ExecuteDataQuery(R"(
        --!syntax_v1
        INSERT INTO `/Root/Tmp` (Key, BoolValue, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue) VALUES
        (42, true, -1, 1, -2, 2, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

    auto it = db.StreamExecuteScanQuery("SELECT Key, BoolValue, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue FROM `/Root/Tmp`").GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    auto streamPart = it.ReadNext().GetValueSync();
    UNIT_ASSERT_C(streamPart.IsSuccess(), streamPart.GetIssues().ToString());
    auto resultSet = streamPart.ExtractResultSet();
    auto columns = resultSet.GetColumnsMeta();
    UNIT_ASSERT_C(columns.size() == 19, "Wrong columns count");
    NYdb::TResultSetParser parser(resultSet);
    UNIT_ASSERT_C(parser.TryNextRow(), "Row is missing");
    UNIT_ASSERT(parser.ColumnParser(0).GetOptionalUint64().value() == 42);
    UNIT_ASSERT(parser.ColumnParser(1).GetOptionalBool().value() == true);
    UNIT_ASSERT(parser.ColumnParser(2).GetOptionalInt32().value() == -1);
    UNIT_ASSERT(parser.ColumnParser(3).GetOptionalUint32().value() == 1);
    UNIT_ASSERT(parser.ColumnParser(4).GetOptionalInt64().value() == -2);
    UNIT_ASSERT(parser.ColumnParser(5).GetOptionalUint64().value() == 2);
    UNIT_ASSERT(parser.ColumnParser(6).GetOptionalFloat().value() == 3.0);
    UNIT_ASSERT(parser.ColumnParser(7).GetOptionalDouble().value() == 4.0);
    UNIT_ASSERT(parser.ColumnParser(8).GetOptionalString().value() == "five");
    UNIT_ASSERT(parser.ColumnParser(9).GetOptionalUtf8().value() == "six");
    UNIT_ASSERT(parser.ColumnParser(10).GetOptionalDate().value() == TInstant::ParseIso8601("2007-07-07"));
    UNIT_ASSERT(parser.ColumnParser(11).GetOptionalDatetime().value() == TInstant::ParseIso8601("2008-08-08T08:08:08Z"));
    UNIT_ASSERT(parser.ColumnParser(12).GetOptionalTimestamp().value() == TInstant::ParseIso8601("2009-09-09T09:09:09.09Z"));
    Cerr << TInstant::Days(10).MicroSeconds() << Endl;
    UNIT_ASSERT(parser.ColumnParser(13).GetOptionalInterval().value() == TInstant::Days(10).MicroSeconds());
    UNIT_ASSERT(parser.ColumnParser(14).GetOptionalDecimal().value().ToString() == "11.11");
    UNIT_ASSERT(parser.ColumnParser(15).GetOptionalJson().value() == "[12]");
    UNIT_ASSERT(parser.ColumnParser(16).GetOptionalYson().value() == "[13]");
    UNIT_ASSERT(parser.ColumnParser(17).GetOptionalJsonDocument().value() == "[14]");
    UNIT_ASSERT(parser.ColumnParser(18).GetOptionalDyNumber().value() == ".1515e2");
}

}

Y_UNIT_TEST_SUITE(KqpScanArrowInChanels) {
    Y_UNIT_TEST(AggregateCountStar) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery("SELECT COUNT(*) FROM `/Root/EightShard`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[24u]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AllTypesColumns) {
        auto kikimr = RunnerWithArrowInChannels();
        InsertAllColumnsAndCheckSelectAll(&kikimr);
    }

    Y_UNIT_TEST(SingleKey) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto params = db.GetParamsBuilder()
            .AddParam("$key")
                .Uint64(202)
                .Build()
            .Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $key AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key = $key;
        )", params).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [[1];[202u];["Value2"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateByColumn) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Text, SUM(Key) AS Total FROM `/Root/EightShard`
            GROUP BY Text
            ORDER BY Total DESC;
        )").GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [["Value3"];[3624u]];
            [["Value2"];[3616u]];
            [["Value1"];[3608u]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateNoColumn) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT SUM(Data), AVG(Data), COUNT(*), MAX(Data), MIN(Data), SUM(Data * 3 + Key * 2) as foo
            FROM `/Root/EightShard`
            WHERE Key > 300
        )").GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([[[36];[2.];18u;[3];[1];[19980u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateNoColumnNoRemaps) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
                SELECT SUM(Data), AVG(Data), COUNT(*)
                FROM `/Root/EightShard`
                WHERE Key > 300
            )").GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([[[36];[2.];18u]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateWithFunction) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT (SUM(Data) * 100) / (MIN(Data) + 10)
            FROM `/Root/EightShard`
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([[[436]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateEmptySum) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery("SELECT SUM(Data) FROM `/Root/EightShard` WHERE Key < 10").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[#]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(JoinWithParams) {
        auto kikimr = RunnerWithArrowInChannels();
        auto db = kikimr.GetTableClient();
        auto params = TParamsBuilder().AddParam("$in")
            .BeginList()
                .AddListItem().BeginStruct().AddMember("key").Uint64(1).EndStruct()
            .EndList()
            .Build().Build();
        // table join params
        auto query1 = R"(
            declare $in as List<Struct<key: UInt64>>;
            select l.Key, l.Value
            from `/Root/KeyValue` as l join AS_TABLE($in) as r on l.Key = r.key
        )";
        // params join table
        auto query2 = R"(
            declare $in as List<Struct<key: UInt64>>;
            select r.Key, r.Value
            from AS_TABLE($in) as l join `/Root/KeyValue` as r on l.key = r.Key
        )";
        for (auto& query : {query1, query2}) {
            auto it = db.StreamExecuteScanQuery(query, params).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"]]])", StreamResultToYson(it));
        }
    }
} // Test suite

} // NKqp
} // NKikimr

