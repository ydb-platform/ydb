#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/generic/size_literals.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_planner.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

NKikimrConfig::TAppConfig AppCfg() {
    NKikimrConfig::TAppConfig appCfg;
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_MB);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(1_MB);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMaxTotalChannelBuffersSize(100_GB);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1_MB);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(1_MB);
    return appCfg;
}

void CreateSampleTables(TKikimrRunner& kikimr) {
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "FourShard"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value1", Type: "String" }
        Columns { Name: "Value2", Type: "String" }
        KeyColumnNames: ["Key"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 300 } } } }
    )");

    TTableClient tableClient{kikimr.GetDriver()};
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/FourShard` (Key, Value1, Value2) VALUES
            (1u,   "Value-001",  "1"),
            (2u,   "Value-002",  "2"),
            (101u, "Value-101",  "101"),
            (102u, "Value-102",  "102"),
            (201u, "Value-201",  "201"),
            (202u, "Value-202",  "202"),
            (301u, "Value-301",  "301"),
            (302u, "Value-302",  "302")
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    session.Close();
}

void CreateNullSampleTables(TKikimrRunner& kikimr) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/TestNulls` (
            Key1 Uint32,
            Key2 Uint32,
            Value String,
            PRIMARY KEY (Key1, Key2)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TestNulls` (Key1, Key2, Value) VALUES
            (NULL, NULL, "One"),
            (NULL, 100u, "Two"),
            (NULL, 200u, "Three"),
            (1u,   NULL, "Four"),
            (1u,   100u, "Five"),
            (1u,   200u, "Six"),
            (2u,   NULL, "Seven"),
            (2u,   100u, "Eight"),
            (2u,   200u, "Nine"),
            (3u,   100u, "Ten"),
            (3u,   200u, "Eleven"),
            (3u,   300u, "Twelve"),
            (3u,   400u, "Thirteen"),
            (3u,   500u, "Fourteen");
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync().IsSuccess());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpScan) {

    Y_UNIT_TEST(StreamExecuteScanQueryCancelation) {
        NKikimrConfig::TAppConfig appConfig;
        // This test expects SourceRead is enabled for ScanQuery
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};

        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER {
            // just in case if test fails.
            NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        };

        {
            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/EightShard` WHERE Text = "Value1";
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            // We must wait execution to be started
            int count = 60;
            while (counters.GetActiveSessionActors()->Val() != 1 && count) {
                count--;
                Sleep(TDuration::Seconds(1));
            }

            UNIT_ASSERT_C(count,
                "Unable to wait second session actor (executing compiled program) start, cur count: "
                << counters.GetActiveSessionActors()->Val());
        }

        NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        int count = 60;
        while (counters.GetActiveSessionActors()->Val() != 0 && count) {
            count--;
            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(count, "Unable to wait for proper active session count, it looks like cancelation doesn`t work");
    }

    Y_UNIT_TEST(StreamExecuteScanQueryClientTimeoutBruteForce) {
        TKikimrRunner kikimr;
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        int maxTimeoutMs = 100;

        for (int i = 1; i < maxTimeoutMs; i++) {
            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/EightShard` WHERE Text = "Value1" ORDER BY Key;
            )", TStreamExecScanQuerySettings().ClientTimeout(TDuration::MilliSeconds(i))).GetValueSync();

            if (it.IsSuccess()) {
                try {
                    auto yson = StreamResultToYson(it, true);
                    CompareYson(R"([[[1];[101u];["Value1"]];[[2];[201u];["Value1"]];[[3];[301u];["Value1"]];[[1];[401u];["Value1"]];[[2];[501u];["Value1"]];[[3];[601u];["Value1"]];[[1];[701u];["Value1"]];[[2];[801u];["Value1"]]])", yson);
                } catch (const TStreamReadError& ex) {
                    UNIT_ASSERT_VALUES_EQUAL(ex.Status, NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
                } catch (const std::exception& ex) {
                    UNIT_ASSERT_C(false, "unknown exception during the test");
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
            }
        }

        WaitForZeroSessions(counters);
    }

    Y_UNIT_TEST(IsNull) {
        auto kikimr = DefaultKikimrRunner();
        CreateNullSampleTables(kikimr);

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT Value FROM `/Root/TestNulls`
                WHERE Key1 IS NULL AND Key2 IS NULL
            )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[["One"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(IsNullPartial) {
        auto kikimr = DefaultKikimrRunner();
        CreateNullSampleTables(kikimr);

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/Test`
                WHERE Group == 1 AND Name IS NULL
            )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(NullInKey) {
        auto kikimr = DefaultKikimrRunner();
        CreateNullSampleTables(kikimr);

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT Value FROM `/Root/TestNulls`
                WHERE Key1 <= 1
                ORDER BY Value
            )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[["Five"]];[["Four"]];[["Six"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(NullInKeySuffix) {
        auto kikimr = DefaultKikimrRunner();
        CreateNullSampleTables(kikimr);

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                DECLARE $key1 AS Uint32?;
                SELECT Value FROM `/Root/TestNulls`
                WHERE Key1 > 1
            )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
                [["Seven"]];[["Eight"]];[["Nine"]];[["Ten"]];[["Eleven"]];[["Twelve"]];[["Thirteen"]];[["Fourteen"]]
            ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(DecimalColumn) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());

        TTableClient client{kikimr.GetDriver()};
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto partitions = TExplicitPartitions()
            .AppendSplitPoints(TValueBuilder()
                .BeginTuple().AddElement().BeginOptional().Decimal(TDecimalValue("1.5", 22, 9)).EndOptional().EndTuple()
                .Build());

        auto ret = session.CreateTable("/Root/DecimalTest",
                TTableBuilder()
                    .AddNullableColumn("Key", TDecimalType(22, 9))
                    .AddNullableColumn("Value", TDecimalType(22, 9))
                    .SetPrimaryKeyColumn("Key")
                    // .SetPartitionAtKeys(partitions)  // Error at split boundary 0: Unsupported typeId 4865 at index 0
                    .Build()).GetValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());

        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Decimal(TDecimalValue("1.0"))
                    .AddMember("Value").Decimal(TDecimalValue("10.123456789"))
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("Key").Decimal(TDecimalValue("2.0"))
                    .AddMember("Value").Decimal(TDecimalValue("20.987654321"))
                    .EndStruct()
                .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            DECLARE $in AS List<Struct<Key: Decimal(22, 9), Value: Decimal(22, 9)>>;
            REPLACE INTO `/Root/DecimalTest`
                SELECT Key, Value FROM AS_TABLE($in);
        )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto db = kikimr.GetTableClient();
        auto it = db.StreamExecuteScanQuery("select Key, max(Value) from `/Root/DecimalTest` group by Key order by Key").GetValueSync();
        CompareYson(R"([
            [["1"];["10.123456789"]];
            [["2"];["20.987654321"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(TaggedScalar) {
        auto kikimr = DefaultKikimrRunner();

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT AsTagged(789, "xxx") AS t;
            )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[789]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Offset) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key, Text, Data FROM `/Root/EightShard` ORDER BY Key LIMIT 3 OFFSET 6
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([
            [[301u];["Value1"];[3]];
            [[302u];["Value2"];[2]];
            [[303u];["Value3"];[1]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Limit) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue` LIMIT 10
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(TopSort) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue` ORDER BY Key LIMIT 1
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Grep) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Data == 1 ORDER BY Key;
        )").GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [[1];[101u];["Value1"]];
            [[1];[202u];["Value2"]];
            [[1];[303u];["Value3"]];
            [[1];[401u];["Value1"]];
            [[1];[502u];["Value2"]];
            [[1];[603u];["Value3"]];
            [[1];[701u];["Value1"]];
            [[1];[802u];["Value2"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(GrepByString) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            $value = 'some very very very very long string';
            SELECT * FROM `/Root/Logs` WHERE Message == $value ORDER BY App, Ts;
        )").GetValueSync();;

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [["ydb"];["ydb-1000"];["some very very very very long string"];[0]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Order) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = db.GetParamsBuilder()
            .AddParam("$items").BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("key").Uint64(646464646464)
                        .AddMember("index_0").Utf8("SomeUtf8Data")
                        .AddMember("value").Uint32(323232)
                    .EndStruct()
                .EndList()
            .Build().Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $items AS List<Struct<'key':Uint64,'index_0':Utf8,'value':Uint32>>;
            SELECT * FROM AS_TABLE($items);
        )", params).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            ["SomeUtf8Data";646464646464u;323232u]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(GrepRange) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = db.GetParamsBuilder()
            .AddParam("$low")
                .Uint64(202)
                .Build()
            .AddParam("$high")
                .Uint64(502)
                .Build()
            .Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $low AS Uint64;
            DECLARE $high AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key >= $low AND Key <= $high AND Data == 1 ORDER BY Key;
        )", params).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [[1];[202u];["Value2"]];
            [[1];[303u];["Value3"]];
            [[1];[401u];["Value1"]];
            [[1];[502u];["Value2"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(GrepLimit) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = db.GetParamsBuilder()
            .AddParam("$app")
                .Utf8("kikimr-db")
                .Build()
            .AddParam("$tsFrom")
                .Int64(1)
                .Build()
            .AddParam("$tsTo")
                .Int64(4)
                .Build()
            .Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $app AS Utf8;
            DECLARE $tsFrom AS Int64;
            DECLARE $tsTo AS Int64;

            SELECT *
            FROM `/Root/Logs`
            WHERE
                App == $app
                AND Ts > $tsFrom
                AND Ts <= $tsTo
            LIMIT 2;
        )", params).GetValueSync();;

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [["kikimr-db"];["kikimr-db-21"];["Read Data"];[2]];
            [["kikimr-db"];["kikimr-db-21"];["Stream Read Data"];[3]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(GrepNonKeyColumns) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        TParamsBuilder params;
        params.AddParam("$app").Utf8("nginx").Build();
        params.AddParam("$tsFrom").Uint64(0).Build();
        params.AddParam("$tsTo").Uint64(5).Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $app AS Utf8;
            DECLARE $tsFrom AS Uint64;
            DECLARE $tsTo AS Uint64;

            SELECT
                Message,
                Ts
            FROM `/Root/Logs`
            WHERE
                App == $app
                AND Ts > $tsFrom
                AND Ts <= $tsTo
            ORDER BY Ts;
        )", params.Build()).GetValueSync();;

        UNIT_ASSERT(it.IsSuccess());

        CompareYson(R"([
            [["GET /index.html HTTP/1.1"];[1]];
            [["PUT /form HTTP/1.1"];[2]];
            [["GET /cat.jpg HTTP/1.1"];[3]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(SingleKey) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
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
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
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
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
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
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
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
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT (SUM(Data) * 100) / (MIN(Data) + 10)
            FROM `/Root/EightShard`
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([[[436]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateCountStar) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery("SELECT COUNT(*) FROM `/Root/EightShard`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[24u]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateEmptyCountStar) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery("SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key < 10").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[0u]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(AggregateEmptySum) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery("SELECT SUM(Data) FROM `/Root/EightShard` WHERE Key < 10").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[#]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(JoinSimple) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/EightShard` AS l JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            ORDER BY Key, Text, Data, Value1, Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
             [[1];[101u];["Value1"];["Value-101"];["101"]];
             [[3];[102u];["Value2"];["Value-102"];["102"]];
             [[2];[201u];["Value1"];["Value-201"];["201"]];
             [[1];[202u];["Value2"];["Value-202"];["202"]];
             [[3];[301u];["Value1"];["Value-301"];["301"]];
             [[2];[302u];["Value2"];["Value-302"];["302"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Join) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto test = [&](bool simpleColumns) {
            auto it = db.StreamExecuteScanQuery(Sprintf(R"(
                PRAGMA %sSimpleColumns;
                $r = (select * from `/Root/FourShard` where Key > 201);

                SELECT l.Key as key, l.Text as text, r.Value1 as value
                FROM `/Root/EightShard` AS l JOIN $r AS r ON l.Key = r.Key
                ORDER BY key, text, value
            )", simpleColumns ? "" : "Disable")).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            CompareYson(R"([
                [[202u];["Value2"];["Value-202"]];
                [[301u];["Value1"];["Value-301"]];
                [[302u];["Value2"];["Value-302"]]
            ])", StreamResultToYson(it));
        };

        test(true);
        test(false);
    }

    Y_UNIT_TEST(Join2) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/EightShard` AS l
            JOIN `/Root/FourShard` AS r
            ON l.Key = r.Key
            WHERE l.Text != "Value1" AND r.Value2 > "1"
            ORDER BY Key, Text, Data, Value1, Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[3];[102u];["Value2"];["Value-102"];["102"]];
            [[1];[202u];["Value2"];["Value-202"];["202"]];
            [[2];[302u];["Value2"];["Value-302"];["302"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Join3) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto test = [&](bool simpleColumns) {
            auto it = db.StreamExecuteScanQuery(Sprintf(R"(
                PRAGMA %sSimpleColumns;
                $join = (
                    SELECT l.Key as Key, l.Text as Text, l.Data as Data, r.Value1 as Value1, r.Value2 as Value2
                    FROM `/Root/EightShard` AS l JOIN `/Root/FourShard` AS r ON l.Key = r.Key
                );

                SELECT Key, COUNT(*) AS Cnt
                FROM $join
                WHERE Cast(Data As Int64) < (Key - 100) and Value1 != 'Value-101'
                GROUP BY Key
                ORDER BY Key, Cnt
            )", simpleColumns ? "" : "Disable")).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            CompareYson(R"([
                [[201u];1u];
                [[202u];1u];
                [[301u];1u];
                [[302u];1u]
            ])", StreamResultToYson(it));
        };

        test(true);
        test(false);
    }

    Y_UNIT_TEST(Join4) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTables(kikimr);

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE Tmp (
                Id Int32,
                Value Uint64,
                PRIMARY KEY(Id)
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteDataQuery(R"(
            --!syntax_v1
            UPSERT INTO Tmp (Id, Value) VALUES
                (100, 300),
                (200, 300),
                (300, 100),
                (400, 300);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT t1.Name, t1.Amount, t2.Id
            FROM Test AS t1
            LEFT JOIN Tmp AS t2
            ON t1.Amount = t2.Value
            ORDER BY t1.Name, t1.Amount, t2.Id;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [["Anna"];[3500u];#];
            [["Paul"];[300u];[100]];
            [["Paul"];[300u];[200]];
            [["Paul"];[300u];[400]];
            [["Tony"];[7200u];#]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(LeftSemiJoinSimple) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/FourShard` AS l LEFT SEMI JOIN `/Root/EightShard` AS r ON l.Key = r.Key
            ORDER BY Key
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[101u];["Value-101"];["101"]];
            [[102u];["Value-102"];["102"]];
            [[201u];["Value-201"];["201"]];
            [[202u];["Value-202"];["202"]];
            [[301u];["Value-301"];["301"]];
            [[302u];["Value-302"];["302"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(RightJoinSimple) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT l.Key, l.Text, l.Data, r.Key, r.Value1, r.Value2
            FROM `/Root/EightShard` AS l RIGHT JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            WHERE r.Key < 200
            ORDER BY l.Key, l.Text, l.Data, r.Key, r.Value1, r.Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [#;#;#;[1u];["Value-001"];["1"]];
            [#;#;#;[2u];["Value-002"];["2"]];
            [[101u];["Value1"];[1];[101u];["Value-101"];["101"]];
            [[102u];["Value2"];[3];[102u];["Value-102"];["102"]]]
        )", StreamResultToYson(it));
    }

    Y_UNIT_TEST(RightOnlyJoinSimple) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key, Value1, Value2
            FROM `/Root/EightShard` AS l RIGHT ONLY JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            WHERE Key < 200
            ORDER BY Key, Value1, Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Value-001"];["1"]];
            [[2u];["Value-002"];["2"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(RightSemiJoinSimple) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key, Value1, Value2
            FROM `/Root/EightShard` AS l RIGHT SEMI JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            WHERE Key < 200
            ORDER BY Key, Value1, Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[101u];["Value-101"];["101"]];
            [[102u];["Value-102"];["102"]]]
        )", StreamResultToYson(it));
    }

    Y_UNIT_TEST(JoinWithParams) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
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

    Y_UNIT_TEST(NoTruncate) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tmp` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const ui64 RowsCount = 1005;

        auto replaceQuery = R"(
            DECLARE $rows AS
                List<Struct<
                    Key: Uint64?,
                    Value: String?
                >>;

            REPLACE INTO `/Root/Tmp`
            SELECT * FROM AS_TABLE($rows);
        )";

        {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            for (ui64 i = 0; i < RowsCount; ++i) {
                rowsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(i)
                    .AddMember("Value")
                        .OptionalString(ToString(i))
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/Tmp`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 totalRows = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto result = streamPart.ExtractResultSet();
                UNIT_ASSERT(!result.Truncated());

                totalRows += result.RowsCount();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(totalRows, RowsCount);
    }

    Y_UNIT_TEST(Join3TablesNoRemap) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            PRAGMA DisableSimpleColumns;
            SELECT *
            FROM `/Root/EightShard` AS t8
              JOIN `/Root/FourShard` AS t4
                ON t8.Key = t4.Key
              JOIN `/Root/TwoShard` AS t2
                ON t8.Data = t2.Key
            ORDER BY t8.Key, t8.Text, t8.Data, t4.Value1, t4.Value2, t2.Value1, t2.Value2
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"(
            [
                [[1u];["One"];[-1];[101u];["Value-101"];["101"];[1];[101u];["Value1"]];
                [[3u];["Three"];[1];[102u];["Value-102"];["102"];[3];[102u];["Value2"]];
                [[2u];["Two"];[0];[201u];["Value-201"];["201"];[2];[201u];["Value1"]];
                [[1u];["One"];[-1];[202u];["Value-202"];["202"];[1];[202u];["Value2"]];
                [[3u];["Three"];[1];[301u];["Value-301"];["301"];[3];[301u];["Value1"]];
                [[2u];["Two"];[0];[302u];["Value-302"];["302"];[2];[302u];["Value2"]]
            ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Join3Tables) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto test = [&](bool simpleColumns) {
            auto it = db.StreamExecuteScanQuery(Sprintf(R"(
                PRAGMA %sSimpleColumns;
                SELECT t8.Key as key, t8.Text as text, t4.Value1, t2.Value2
                FROM `/Root/EightShard` AS t8
                  JOIN `/Root/FourShard` AS t4
                    ON t8.Key = t4.Key
                  JOIN `/Root/TwoShard` AS t2
                    ON t8.Data = t2.Key
                WHERE t8.Key > 200 AND t2.Value2 >= 0
                ORDER BY key, text, t4.Value1, t2.Value2
            )", simpleColumns ? "" : "Disable")).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            CompareYson(R"(
                [
                    [[201u];["Value1"];["Value-201"];[0]];
                    [[301u];["Value1"];["Value-301"];[1]];
                    [[302u];["Value2"];["Value-302"];[0]]
                ])", StreamResultToYson(it));
        };

        test(true  /* SimpleColumns */);
        test(false /* DisableSimpleColumns */);
    }

    Y_UNIT_TEST(JoinLeftOnly) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto test = [&](bool simpleColumns) {
            auto it = db.StreamExecuteScanQuery(Sprintf(R"(
                PRAGMA %sSimpleColumns;
                SELECT *
                FROM `/Root/EightShard` AS l
                    LEFT ONLY JOIN `/Root/FourShard` AS r
                        ON l.Key = r.Key
                WHERE Data = 1
                ORDER BY Key, Text, Data
            )", simpleColumns ? "" : "Disable")).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            CompareYson(R"(
                [
                    [[1];[303u];["Value3"]];
                    [[1];[401u];["Value1"]];
                    [[1];[502u];["Value2"]];
                    [[1];[603u];["Value3"]];
                    [[1];[701u];["Value1"]];
                    [[1];[802u];["Value2"]]
                ])", StreamResultToYson(it));
        };

        test(true  /* SimpleColums */);
        test(false /* DisableSimpleColumns */);
    }

    Y_UNIT_TEST(CrossJoin) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT l.Key, r.Key, l.Text, r.Value1
            FROM `/Root/EightShard` AS l CROSS JOIN `/Root/FourShard` AS r
            WHERE l.Key > r.Key AND l.Data = 1 AND r.Value2 > "200"
            ORDER BY l.Key, l.Text, r.Value1
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[202u];[201u];["Value2"];["Value-201"]];
            [[303u];[201u];["Value3"];["Value-201"]];
            [[303u];[202u];["Value3"];["Value-202"]];
            [[303u];[301u];["Value3"];["Value-301"]];
            [[303u];[302u];["Value3"];["Value-302"]];
            [[401u];[201u];["Value1"];["Value-201"]];
            [[401u];[202u];["Value1"];["Value-202"]];
            [[401u];[301u];["Value1"];["Value-301"]];
            [[401u];[302u];["Value1"];["Value-302"]];
            [[502u];[201u];["Value2"];["Value-201"]];
            [[502u];[202u];["Value2"];["Value-202"]];
            [[502u];[301u];["Value2"];["Value-301"]];
            [[502u];[302u];["Value2"];["Value-302"]];
            [[603u];[201u];["Value3"];["Value-201"]];
            [[603u];[202u];["Value3"];["Value-202"]];
            [[603u];[301u];["Value3"];["Value-301"]];
            [[603u];[302u];["Value3"];["Value-302"]];
            [[701u];[201u];["Value1"];["Value-201"]];
            [[701u];[202u];["Value1"];["Value-202"]];
            [[701u];[301u];["Value1"];["Value-301"]];
            [[701u];[302u];["Value1"];["Value-302"]];
            [[802u];[201u];["Value2"];["Value-201"]];
            [[802u];[202u];["Value2"];["Value-202"]];
            [[802u];[301u];["Value2"];["Value-301"]];
            [[802u];[302u];["Value2"];["Value-302"]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(SelfJoin3xSameLabels) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            $foo = (
                SELECT t1.Key AS Key
                FROM `/Root/KeyValue` AS t1
                JOIN `/Root/KeyValue` AS t2
                ON t1.Key = t2.Key
                GROUP BY t1.Key
            );

            SELECT t1.Key AS Key
            FROM $foo AS Foo
            JOIN `/Root/KeyValue` AS t1
            ON t1.Key = Foo.Key
            ORDER BY Key
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson("[[[1u]];[[2u]]]", StreamResultToYson(it));
    }

    Y_UNIT_TEST(SelfJoin3x) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            $foo = (
                SELECT t1.Key AS Key
                FROM `/Root/KeyValue` AS t1
                JOIN `/Root/KeyValue` AS t2
                ON t1.Key = t2.Key
                GROUP BY t1.Key
            );

            SELECT t3.Key AS Key
            FROM $foo AS Foo
            JOIN `/Root/KeyValue` AS t3
            ON t3.Key = Foo.Key
            ORDER BY Key
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson("[[[1u]];[[2u]]]", StreamResultToYson(it));
    }

#if 0
    Y_UNIT_TEST(JoinParams) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = TParamsBuilder().AddParam("$in").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Uint64(1).AddMember("v").String("v1").EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Uint64(2).AddMember("v").String("v2").EndStruct()
                .EndList().Build().Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $in AS 'List<Struct<k: Uint64, v: String>>';
            SELECT *
            FROM `/Root/KeyValue` AS l
            JOIN AS_TABLE($in) AS r
            ON l.Key = r.k
        )", params).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        Cerr << StreamResultToYson(it) << Endl;
    }

    Y_UNIT_TEST(JoinParams2) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = TParamsBuilder()
                .AddParam("$in1").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Uint64(1).AddMember("v").String("v1").EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Uint64(2).AddMember("v").String("v2").EndStruct()
                .EndList().Build()
                .AddParam("$in2").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Uint64(1).AddMember("v").String("v1").EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Uint64(2).AddMember("v").String("v2").EndStruct()
                .EndList().Build()
                .Build();

        auto it = db.StreamExecuteScanQuery(R"(
                DECLARE $in1 AS 'List<Struct<k: Uint64, v: String>>';
                DECLARE $in2 AS 'List<Struct<k: Uint64, v: String>>';
                SELECT *
                FROM AS_TABLE($in1) AS l
                JOIN AS_TABLE($in2) AS r
                ON l.k = r.k;
                UPSERT INTO [/Root/KeyValue] (Key, Value) Values (1, "test");
            )", params).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        Cerr << StreamResultToYson(it) << Endl;
    }

    Y_UNIT_TEST(JoinParams3) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto params = TParamsBuilder()
                .AddParam("$in1").BeginList()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(1).AddMember("v").String("v1").EndStruct()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(2).AddMember("v").String("v2").EndStruct()
                .EndList().Build()
                .AddParam("$in2").BeginList()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(1).AddMember("v").String("v1").EndStruct()
                    .AddListItem().BeginStruct().AddMember("k").Uint64(2).AddMember("v").String("v2").EndStruct()
                .EndList().Build()
                .Build();

        auto it = db.StreamExecuteScanQuery(R"(
                    DECLARE $in1 AS 'List<Struct<k: Uint64, v: String>>';
                    DECLARE $in2 AS 'List<Struct<k: Uint64, v: String>>';

                    $l = (select * from AS_TABLE($in1) where k > 0);
                    $r = (select * from AS_TABLE($in2) where k > 10);

                    SELECT *
                    FROM $l AS l
                    JOIN $r AS r
                    ON l.k = r.k
                )", params).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        Cerr << StreamResultToYson(it) << Endl;
    }
#endif

    Y_UNIT_TEST(PrunePartitionsByLiteral) {
        auto cfg = AppCfg();
        auto kikimr = DefaultKikimrRunner({}, cfg);
        auto db = kikimr.GetTableClient();

        TStreamExecScanQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        // simple key
        {
            auto it = db.StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/EightShard` WHERE Key = 301;
            )", settings).GetValueSync();

            UNIT_ASSERT(it.IsSuccess());

            auto res = CollectStreamResult(it);
            CompareYson(R"([[[3];[301u];["Value1"]]])", res.ResultSetYson);

            UNIT_ASSERT(res.QueryStats);
            UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
            // TODO: KIKIMR-16691 (add stats for sream lookup)
            //UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).affected_shards(), 1);
        }

        // complex key
        {
            auto params = TParamsBuilder()
                .AddParam("$ts").Int64(2).Build()
                .Build();

            auto it = db.StreamExecuteScanQuery(R"(
                DECLARE $ts AS Int64;
                SELECT * FROM `/Root/Logs` WHERE App = "nginx" AND Ts > $ts
            )", params, settings).GetValueSync();

            UNIT_ASSERT(it.IsSuccess());

            auto res = CollectStreamResult(it);
            CompareYson(R"([[["nginx"];["nginx-23"];["GET /cat.jpg HTTP/1.1"];[3]]])", res.ResultSetYson);

            UNIT_ASSERT(res.QueryStats);
            UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
            // TODO: KIKIMR-16691 (add stats for sream lookup)
            //UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).affected_shards(), 1);
        }
    }

    Y_UNIT_TEST(PrunePartitionsByExpr) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        TStreamExecScanQuerySettings settings;
        settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(300).Build()
            .Build();

        auto it = db.StreamExecuteScanQuery(R"(
            DECLARE $key AS Uint64;
            SELECT * FROM `/Root/EightShard` WHERE Key = $key + 1;
        )", params, settings).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        auto res = CollectStreamResult(it);
        CompareYson(R"([
            [[3];[301u];["Value1"]]
        ])", res.ResultSetYson);

        UNIT_ASSERT(res.QueryStats);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).affected_shards(), 1);
    }

    Y_UNIT_TEST(TooManyComputeActors) {
        TVector<NKikimrKqp::TKqpSetting> settings;
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxComputeActors");
        setting.SetValue("5");
        settings.push_back(setting);

        auto kikimr = DefaultKikimrRunner(settings);
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto it = db.StreamExecuteScanQuery(R"(
            $join = (
                SELECT l.Key as Key, l.Text as Text, l.Data as Data, r.Value1 as Value1, r.Value2 as Value2
                FROM `/Root/EightShard` AS l JOIN `/Root/FourShard` AS r ON l.Key = r.Key
            );

            SELECT Key, COUNT(*) AS Cnt
            FROM $join
            WHERE Cast(Data As Int64) < (Key - 100) and Value1 != 'Value-101'
            GROUP BY Key
            ORDER BY Key, Cnt
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto part = it.ReadNext().GetValueSync();

        UNIT_ASSERT_EQUAL_C(part.GetStatus(), EStatus::PRECONDITION_FAILED, part.GetStatus());
        part.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(HasIssue(part.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
            [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("Requested too many execution units");
            }));

        part = it.ReadNext().GetValueSync();
        UNIT_ASSERT(part.EOS());
        UNIT_ASSERT_EQUAL(part.GetStatus(), EStatus::CLIENT_OUT_OF_RANGE);
    }

    Y_UNIT_TEST(EarlyFinish) {
        auto appCfg = AppCfg();
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(5);
        appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(5);

        NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = 1;
        Y_DEFER {
            NYql::NDq::GetDqExecutionSettingsForTests().Reset();
        };

        auto kikimr = DefaultKikimrRunner({}, appCfg);

        NYdb::NTable::TTableClient client(kikimr.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        for (int i = 0; i < 100; ++i) {
            AssertSuccessResult(session.ExecuteDataQuery(
                Sprintf(R"(
                    REPLACE INTO `/Root/EightShard` (Key, Text, Data) VALUES
                        (%d, "Value1", 0),
                        (%d, "Value2", 1),
                        (%d, "Value3", 2),
                        (%d, "Value4", 3),
                        (%d, "Value5", 4),
                        (%d, "Value6", 5),
                        (%d, "Value7", 6),
                        (%d, "Value8", 7)
                    )", i, 100 + i, 200 + i, 300 + i, 400 + i, 500 + i, 600 + i, 700 + i),
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());
        }

        auto db = kikimr.GetTableClient();
        auto it = db.StreamExecuteScanQuery("SELECT * FROM `/Root/EightShard` LIMIT 2").GetValueSync();

        Cerr << StreamResultToYson(it) << Endl;
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    Y_UNIT_TEST(MultipleResults) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/EightShard`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto part = it.ReadNext().GetValueSync();
        part.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL_C(part.GetStatus(), EStatus::PRECONDITION_FAILED, part.GetIssues().ToString());
    }

    Y_UNIT_TEST(Effects) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            UPSERT INTO `/Root/KeyValue`
            SELECT Key, Text AS Value FROM `/Root/EightShard`;

            SELECT * FROM `/Root/EightShard`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto part = it.ReadNext().GetValueSync();
        part.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL_C(part.GetStatus(), EStatus::PRECONDITION_FAILED, part.GetIssues().ToString());
    }

    Y_UNIT_TEST(PureExpr) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT '42', 42, 5*5;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto res = StreamResultToYson(it);
        CompareYson(R"([["42";42;25]])", res);
    }

    Y_UNIT_TEST(UnionWithPureExpr) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT '42', 42, 5*5
            UNION ALL
            SELECT 'forty-two';
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto res = StreamResultToYson(it);
        CompareYson(R"([["42";[42];[25]];["forty-two";#;#]])", res);
    }

    Y_UNIT_TEST(MiltiExprWithPure) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1;
            SELECT 2;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto part = it.ReadNext().GetValueSync();
        part.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL_C(part.GetStatus(), EStatus::PRECONDITION_FAILED, part.GetIssues().ToString());
    }

    Y_UNIT_TEST(UnionBasic) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key FROM (
              (SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1)
              UNION ALL
              (SELECT Key FROM `/Root/EightShard` ORDER BY Key LIMIT 1)
            ) ORDER BY Key;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u]];[[101u]]])", res);
    }

    Y_UNIT_TEST(UnionMixed) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT 42
            UNION ALL
            (SELECT Key FROM `/Root/EightShard` ORDER BY Key LIMIT 1);
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[42];#];[#;[101u]]])", res);
    }

    Y_UNIT_TEST(UnionThree) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key FROM (
            (SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1)
            UNION ALL
            (SELECT Key FROM `/Root/EightShard` ORDER BY Key LIMIT 1)
            UNION ALL
            (SELECT Key FROM `/Root/TwoShard` ORDER BY Key DESC LIMIT 1)
            ) ORDER BY Key;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u]];[[101u]];[[4000000003u]]])", res);
    }

    Y_UNIT_TEST(UnionSameTable) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            (SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1)
            UNION ALL
            (SELECT Key FROM `/Root/KeyValue` ORDER BY Key LIMIT 1);
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u]];[[1u]]])", res);
    }

    Y_UNIT_TEST(UnionAggregate) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT COUNT(*) FROM `/Root/KeyValue`
            UNION ALL
            SELECT COUNT(*) FROM `/Root/EightShard`
            UNION ALL
            SELECT SUM(Amount) FROM `/Root/Test`;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[2u]];[[24u]];[[11000u]]])", res);
    }

    Y_UNIT_TEST(CountDistinct) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT count(*) AS c1, count(DISTINCT Value) AS c2
            FROM `/Root/KeyValue` GROUP BY Key;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[1u;1u];[1u;1u]])", res);
    }

    Y_UNIT_TEST(FullFrameWindow) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT group_total, count(*) FROM (
                SELECT Key, Text, SUM(Data) OVER w1 AS group_total
                FROM `/Root/EightShard` WINDOW w1 AS (partition by Text)
            ) GROUP BY group_total ORDER BY group_total;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[15];8u];[[16];8u];[[17];8u]])", res);
    }

    Y_UNIT_TEST(SimpleWindow) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Text, running_total FROM (
                SELECT Key, Text, SUM(Data) OVER w1 AS running_total
                FROM `/Root/EightShard`
                WHERE Text = 'Value2'
                WINDOW w1 AS (partition by Text order by Key)
            ) ORDER BY running_total;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [["Value2"];[3]];
            [["Value2"];[4]];
            [["Value2"];[6]];
            [["Value2"];[9]];
            [["Value2"];[10]];
            [["Value2"];[12]];
            [["Value2"];[15]];
            [["Value2"];[16]]
        ])", res);
    }

    Y_UNIT_TEST(TwoAggregatesOneFullFrameWindow) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT tot, avg, count(*) FROM (
                SELECT Key, Text, SUM(Data) OVER w1 AS tot, avg(Data) OVER w1 AS avg
                FROM `/Root/EightShard`
                WHERE Text = 'Value3'
                WINDOW w1 AS (partition by Text)
            ) GROUP BY tot, avg
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[17];[2.125];8u]])", res);
    }

    Y_UNIT_TEST(TwoAggregatesTwoWindows) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key, Text, sum(Data) OVER w1 AS tot, sum(Data) OVER w2 AS avg
            FROM `/Root/EightShard`
            WHERE Text = 'Value2'
            WINDOW w1 AS (partition by Text),
                   w2 AS (partition by Text order by Key)
            ORDER BY Key;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([
            [[102u];["Value2"];[16];[3]];
            [[202u];["Value2"];[16];[4]];
            [[302u];["Value2"];[16];[6]];
            [[402u];["Value2"];[16];[9]];
            [[502u];["Value2"];[16];[10]];
            [[602u];["Value2"];[16];[12]];
            [[702u];["Value2"];[16];[15]];
            [[802u];["Value2"];[16];[16]]
        ])", res);
    }

    Y_UNIT_TEST(CustomWindow) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key, sum_short_win FROM (
                SELECT Key, Text, SUM(Data) OVER w1 AS sum_short_win
                FROM `/Root/EightShard`
                WHERE Text = 'Value2'
                WINDOW w1 AS (partition by Text order by Key ROWS BETWEEN CURRENT ROW and 2 FOLLOWING)
            ) ORDER BY Key;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([
            [[102u];[6]];
            [[202u];[6]];
            [[302u];[6]];
            [[402u];[6]];
            [[502u];[6]];
            [[602u];[6]];
            [[702u];[4]];
            [[802u];[1]]
        ])", res);
    }

    Y_UNIT_TEST(EmptySet) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT Key FROM `/Root/EightShard` WHERE false;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson("[]",res);
    }

     Y_UNIT_TEST(RestrictSqlV0) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            --!syntax_v0
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto part = it.ReadNext().GetValueSync();

        part.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL_C(part.GetStatus(), EStatus::GENERIC_ERROR, part.GetStatus());
    }

    Y_UNIT_TEST(LongStringCombiner) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT min(Message), max(Message) FROM `/Root/Logs`;
        )").GetValueSync();
        auto res = StreamResultToYson(it);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[" GET /index.html HTTP/1.1"];["some very very very very long string"]]])",res);
    }

    Y_UNIT_TEST(SqlInParameter) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto result = db.CreateSession().GetValueSync().GetSession().ExecuteDataQuery(R"(
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES
                (3u,   "Three"),
                (4u,   "Four"),
                (10u,  "Ten"),
                (NULL, "Null Value");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto query = R"(
            DECLARE $in AS List<Uint64?>;
            SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN $in
            ORDER BY Key
        )";

        auto params = TParamsBuilder();
        auto& pl = params.AddParam("$in").BeginList();
        for (auto v : {1, 2, 3, 42, 50, 100}) {
            pl.AddListItem().OptionalUint64(v);
        }
        pl.AddListItem().OptionalUint64(Nothing());
        pl.EndList().Build();

        auto it = db.StreamExecuteScanQuery(query, params.Build()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]];
                        [[2u];["Two"]];
                        [[3u];["Three"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(SqlInLiteral) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto result = db.CreateSession().GetValueSync().GetSession().ExecuteDataQuery(R"(
                REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES
                    (3u,   "Three"),
                    (4u,   "Four"),
                    (10u,  "Ten"),
                    (NULL, "Null Value");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto query = R"(
                SELECT Key, Value FROM `/Root/KeyValue` WHERE Key IN (1, 2, 3, 42)
                ORDER BY Key
            )";

        auto it = db.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]];
                        [[2u];["Two"]];
                        [[3u];["Three"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(UdfFailure) {
        auto testFn = [](bool disableLlvmForUdfStages) {
            TVector<NKikimrKqp::TKqpSetting> settings;
            auto setting = NKikimrKqp::TKqpSetting();
            setting.SetName("_KqpDisableLlvmForUdfStages");
            setting.SetValue(disableLlvmForUdfStages ? "true" : "false");
            settings.push_back(setting);

            auto kikimr = DefaultKikimrRunner(settings);
            auto db = kikimr.GetTableClient();

            auto it = db.StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/KeyValue`
                WHERE TestUdfs::TestFilterTerminate(Cast(Key as Int64) ?? 0, 10)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_C(!streamPart.IsSuccess(), streamPart.GetIssues().ToString());
            UNIT_ASSERT_C(!streamPart.EOS(), streamPart.GetIssues().ToString());
            UNIT_ASSERT(
                HasIssue(streamPart.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const NYql::TIssue& issue) {
                    return issue.GetMessage().Contains("Terminate was called")   // general termination prefix
                           && issue.GetMessage().Contains("Bad filter value.");  // test specific UDF exception
                }));
        };

        testFn(false);
        testFn(true);
    }

    Y_UNIT_TEST(SecondaryIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(true);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        // should work with ScanQuery
        auto itOk = db.StreamExecuteScanQuery(R"(
            PRAGMA AnsiInForEmptyOrNullableItemsCollections;

            SELECT Value
            FROM `/Root/SecondaryComplexKeys`
            WHERE (Fk1, Fk2) IN AsList((1, "Fk1"), (2, "Fk2"), (42, "Fk5"), (Null, "FkNull"))
            ORDER BY Value;
        )").GetValueSync();
        UNIT_ASSERT_C(itOk.IsSuccess(), itOk.GetIssues().ToString());
        CompareYson(R"([[["Payload1"]];[["Payload2"]]])", StreamResultToYson(itOk));

        {  // Index contains required columns
            auto itIndex = db.StreamExecuteScanQuery(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                SELECT Fk2
                FROM `/Root/SecondaryComplexKeys` VIEW Index
                WHERE Fk1 IN [1, 2, 3, 4, 5]
                ORDER BY Fk2;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([[["Fk1"]];[["Fk2"]];[["Fk5"]]])", StreamResultToYson(itIndex));
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                SELECT Value
                FROM `/Root/SecondaryComplexKeys` VIEW Index
                WHERE Fk1 >= 1 AND Fk1 < 5
                ORDER BY Value;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([[["Payload1"]];[["Payload2"]]])", StreamResultToYson(itIndex));
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                SELECT Value
                FROM `/Root/SecondaryComplexKeys` VIEW Index
                WHERE (Fk1, Fk2) IN AsList((1, "Fk1"), (2, "Fk2"), (42, "Fk5"), (Null, "FkNull"))
                ORDER BY Value;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([[["Payload1"]];[["Payload2"]]])", StreamResultToYson(itIndex));
        }
    }

    Y_UNIT_TEST(SecondaryIndexCustomColumnOrder) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        {  // prepare table
            auto res = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                CREATE TABLE `/Root/SecondaryKeysCustomOrder` (
                    Key2 Int32,
                    Key1 String,
                    Fk2 Int32,
                    Fk1 String,
                    Value String,
                    PRIMARY KEY (Key2, Key1),
                    INDEX Index GLOBAL ON (Fk2, Fk1)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/SecondaryKeysCustomOrder` (Key2, Key1, Fk2, Fk1, Value) VALUES
                (1u, "One", 1u, "Fk1", "Value1"),
                (2u, "Two", 2u, "Fk2", "Value2"),
                (3u, "Three", 3u, "Fk3", Null),
                (NULL, "Four", 4u, Null, "Value4"),
                (5u, Null, 5u, "Fk5",  "Value5");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                SELECT Value, Fk1
                FROM `/Root/SecondaryKeysCustomOrder` VIEW Index
                WHERE Fk2 = 2u;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([
                [["Value2"];["Fk2"]]
            ])", StreamResultToYson(itIndex));
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                SELECT Value, Fk1, Key2
                FROM `/Root/SecondaryKeysCustomOrder` VIEW Index
                WHERE Fk2 >= 4u AND Fk1 IS NULL;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([
                [["Value4"];#;#]
            ])", StreamResultToYson(itIndex));
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                SELECT Value, Fk1, Key1
                FROM `/Root/SecondaryKeysCustomOrder` VIEW Index
                WHERE (Fk2, Fk1) IN AsList((1u, "Fk1"), (2u, "Fk2"), (5u, "Fk5"))
                ORDER BY Value;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([
                [["Value1"];["Fk1"];["One"]];
                [["Value2"];["Fk2"];["Two"]];
                [["Value5"];["Fk5"];#]
            ])", StreamResultToYson(itIndex));
        }

        {
            auto itIndex = db.StreamExecuteScanQuery(R"(
                SELECT r.Value, l.Value
                FROM `/Root/SecondaryKeys` VIEW Index AS l
                INNER JOIN `/Root/SecondaryKeysCustomOrder` VIEW Index AS r
                ON l.Fk = r.Fk2 ORDER BY r.Value;
            )").GetValueSync();

            UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());
            CompareYson(R"([
                [["Value1"];["Payload1"]];
                [["Value2"];["Payload2"]];
                [["Value5"];["Payload5"]]
            ])", StreamResultToYson(itIndex));
        }
    }

    Y_UNIT_TEST(BoolFlag) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());

        TTableClient client{kikimr.GetDriver()};
        auto session = client.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.CreateTable("/Root/BoolTest",
            TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::Uint64)
                .AddNullableColumn("Flag", EPrimitiveType::Bool)
                .SetPrimaryKeyColumn("Key")
                .Build()
            ).GetValueSync().IsSuccess());

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            INSERT INTO `/Root/BoolTest` (Key, Value, Flag) VALUES
                (1, 100, true), (2, 200, false), (3, 300, true), (4, 400, false), (5, 500, null)
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto db = kikimr.GetTableClient();
        auto it = db.StreamExecuteScanQuery(R"(
            select Key, Value, Flag
            from `/Root/BoolTest`
            where not Flag and Value < 1000
        )").GetValueSync();

        CompareYson(R"([
            [[2u];[200u];[%false]];
            [[4u];[400u];[%false]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Counters) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key IN (1,2,3) LIMIT 10;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", StreamResultToYson(it));
        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.GetQueryTypeCounter(NKikimrKqp::EQueryType::QUERY_TYPE_SQL_SCAN)->Val());
    }

    Y_UNIT_TEST(DropRedundantSortByPk) {
        auto kikimr = DefaultKikimrRunner({}, AppCfg());
        auto db = kikimr.GetTableClient();

        auto settings = TStreamExecScanQuerySettings()
            .Explain(true);

        auto test = [&](const TString& table, const TString& tableRlPath, const TVector<TString>& keys, bool top, bool expectSort) {
            auto query = TStringBuilder()
                << "SELECT * FROM `" << table << "` "
                << "ORDER BY " << JoinSeq(", ", keys)
                << (top ? " LIMIT 42" : "");
            auto result = db.StreamExecuteScanQuery(query, settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto res = CollectStreamResult(result);
            UNIT_ASSERT(res.PlanJson);
            // Cerr << res.PlanJson << Endl;

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, /* throwOnError */ true);

            auto node = FindPlanNodeByKv(plan, "Tables", NJson::TJsonArray({tableRlPath}).GetStringRobust());
            UNIT_ASSERT_C(node.IsDefined(), query);
            UNIT_ASSERT_EQUAL_C(node.GetMapSafe().at("Node Type").GetStringSafe().Contains("Sort"), expectSort, query);
        };

        // simple key
        for (bool top : {false, true}) {
            test("/Root/KeyValue", "KeyValue", {"Key"}, top, false);  // key
            test("/Root/KeyValue", "KeyValue", {"Value"}, top, true); // not key
        }

        // complex key
        for (bool top : {false, true}) {
            test("/Root/Logs", "Logs", {"App", "Ts", "Host"}, top, false); // full key
            test("/Root/Logs", "Logs", {"App", "Ts"}, top, false);         // key prefix
            test("/Root/Logs", "Logs", {"App"}, top, false);               // key prefix
            test("/Root/Logs", "Logs", {"Ts", "Host"}, top, true);         // not key prefix
            test("/Root/Logs", "Logs", {"Message"}, top, true);            // not key
        }
    }

    Y_UNIT_TEST(LMapFunction) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        // EnableDebugLogging(kikimr);

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/SampleMapTable` (
                Key Int32,
                Value String,
                Price Int32,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/SampleMapTable` (Key, Value, Price) VALUES
                (1, "Bitcoin", 50000),
                (2, "Dogecoin", 1000),
                (3, "Ethereum", 5000),
                (4, "XTC", 1),
                (5, "Cardano", 2),
                (6, "Tether", 3);
         )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

        auto it = tableClient.StreamExecuteScanQuery(R"(
            $func = ($stream) -> {
                RETURN YQL::Filter($stream, ($r) -> { RETURN Coalesce($r.Price <= 1000, False); });
            };

            $inputTable = SELECT Key, Value, Price FROM SampleMapTable;

            PROCESS $inputTable USING $func(TableRows());
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        TString result = StreamResultToYson(it);
        std::cerr << result << std::endl;
        CompareYson(result, R"([[[2];[1000];["Dogecoin"]];[[4];[1];["XTC"]];[[5];[2];["Cardano"]];[[6];[3];["Tether"]]])");
    }

    Y_UNIT_TEST(YqlTableSample) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpYqlSyntaxVersion");
        setting.SetValue("1");

        auto kikimr = DefaultKikimrRunner({setting});
        auto db = kikimr.GetTableClient();

        const TString query(R"(SELECT * FROM `/Root/Test` TABLESAMPLE SYSTEM(1.0);)");
        auto it = db.StreamExecuteScanQuery(query).ExtractValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto result = it.ReadNext().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("ATOM evaluation is not supported in YDB queries.");
        }));
    }

    Y_UNIT_TEST(CrossJoinOneColumn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto result = db.StreamExecuteScanQuery(R"(
            SELECT COUNT(left.Key)
            FROM `/Root/EightShard` as left
            CROSS JOIN `/Root/FourShard` as right
            WHERE left.Key = 101u;
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[8u]])", StreamResultToYson(result));

        result = db.StreamExecuteScanQuery(R"(
            SELECT COUNT(right.Key)
            FROM `/Root/EightShard` as left
            CROSS JOIN `/Root/FourShard` as right
            WHERE right.Key = 1
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[24u]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(CrossJoinCount) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        auto result = db.StreamExecuteScanQuery(R"(
            SELECT COUNT(*)
            FROM `/Root/EightShard` as left
            CROSS JOIN `/Root/FourShard` as right
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[192u]])", StreamResultToYson(result));
    }

    Y_UNIT_TEST(SelectExistsUnexpected) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTables(kikimr);

        auto result = db.StreamExecuteScanQuery(R"(
            SELECT EXISTS(
                SELECT * FROM `/Root/EightShard` WHERE Key > 100
            ) as dataPresent;
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[%true]]", StreamResultToYson(result));

        result = db.StreamExecuteScanQuery(R"(
            SELECT EXISTS(
                SELECT * FROM `/Root/EightShard` WHERE Key > 10000
            ) as dataPresent;
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[%false]]", StreamResultToYson(result));
    }

    Y_UNIT_TEST(DqSourceFullScan) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            auto result = db.StreamExecuteScanQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard`;
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DqSource) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            auto result = db.StreamExecuteScanQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key = 101 or (Key >= 202 and Key < 200+4) ORDER BY Key;
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]];[[202u];[1]];[[203u];[3]]])", StreamResultToYson(result));
        }
    }

    Y_UNIT_TEST(DqSourceLiteralRange) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            auto result = db.StreamExecuteScanQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key = 101 ORDER BY Key;
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]]])", StreamResultToYson(result));
        }

        {
            auto params = TParamsBuilder().AddParam("$param").Uint64(101).Build().Build();

            auto result = db.StreamExecuteScanQuery(R"(
                DECLARE $param as Uint64;
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key = $param ORDER BY Key;
            )",
            params).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]]])", StreamResultToYson(result));
        }
    }

    Y_UNIT_TEST(StreamLookup) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            auto result = db.CreateSession().GetValueSync().GetSession().ExecuteDataQuery(R"(
            REPLACE INTO `/Root/EightShard` (Key, Text, Data) VALUES
                (1u, "Value1",  1),
                (2u, "Value2",  1),
                (3u, "Value3",  1),
                (4u, "Value4",  1),
                (5u, "Value5",  1);
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = db.StreamExecuteScanQuery(R"(
                $keys = SELECT Key FROM `/Root/EightShard`;
                SELECT * FROM `/Root/KeyValue` WHERE Key IN $keys ORDER BY Key;
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", StreamResultToYson(result));
        }
    }

    Y_UNIT_TEST(StreamLookupByPkPrefix) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "TestTable"
                Columns { Name: "Key1", Type: "Uint64" }
                Columns { Name: "Key2", Type: "Uint64" }
                Columns { Name: "Value", Type: "String" }
                KeyColumnNames: ["Key1", "Key2"]
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint64: 2 } }
                        Tuple { Optional { Uint64: 20 } }
                    }
                }
            )");

            auto result = db.CreateSession().GetValueSync().GetSession().ExecuteDataQuery(R"(
            REPLACE INTO `/Root/TestTable` (Key1, Key2, Value) VALUES
                (1u, 10, "Value1"),
                (2u, 19, "Value2"),
                (2u, 21, "Value2"),
                (3u, 30, "Value3"),
                (4u, 40, "Value4"),
                (5u, 50, "Value5");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = db.StreamExecuteScanQuery(R"(
                $keys = SELECT Key FROM `/Root/KeyValue`;
                SELECT * FROM `/Root/TestTable` WHERE Key1 IN $keys ORDER BY Key1, Key2;
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];[10u];["Value1"]];[[2u];[19u];["Value2"]];[[2u];[21u];["Value2"]]])", StreamResultToYson(result));
        }
    }

    Y_UNIT_TEST(StreamLookupByFullPk) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(true);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        CreateSampleTables(kikimr);

        {
            kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "TestTable"
                Columns { Name: "Key1", Type: "Uint64" }
                Columns { Name: "Key2", Type: "Uint64" }
                Columns { Name: "Value", Type: "String" }
                KeyColumnNames: ["Key1", "Key2"]
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint64: 2 } }
                        Tuple { Optional { Uint64: 20 } }
                    }
                }
            )");

            auto result = db.CreateSession().GetValueSync().GetSession().ExecuteDataQuery(R"(
                REPLACE INTO `/Root/TestTable` (Key1, Key2, Value) VALUES
                    (1u, 10, "Value1"),
                    (2u, 19, "Value2"),
                    (2u, 21, "Value2"),
                    (3u, 30, "Value3"),
                    (4u, 40, "Value4"),
                    (5u, 50, "Value5");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);

            auto it = db.StreamExecuteScanQuery(R"(
                SELECT * FROM `/Root/TestTable` WHERE Key1 = 1 AND Key2 = 10;
            )", settings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            CompareYson(R"([[[1u];[10u];["Value1"]]])", result.ResultSetYson);
            UNIT_ASSERT(result.QueryStats);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.QueryStats->query_plan(), &plan, true);
            auto streamLookup = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
            UNIT_ASSERT(streamLookup.IsDefined());
        }
    }

    Y_UNIT_TEST(StreamLookupTryGetDataBeforeSchemeInitialization) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);

        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        InitRoot(server, sender);

        std::vector<TAutoPtr<IEventHandle>> captured;
        bool firstAttemptToGetData = false;

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvResolveKeySetResult::EventType) {
                Cerr << "Captured TEvTxProxySchemeCache::TEvResolveKeySetResult from " << runtime->FindActorName(ev->Sender) << " to " << runtime->FindActorName(ev->GetRecipientRewrite()) << Endl;
                if (runtime->FindActorName(ev->GetRecipientRewrite()) == "KQP_STREAM_LOOKUP_ACTOR") {
                    if (!firstAttemptToGetData) {
                        // capture response from scheme cache until CA calls GetAsyncInputData()
                        captured.push_back(ev.Release());
                        return true;
                    }

                    for (auto ev : captured) {
                        runtime->Send(ev.Release());
                    }
                }
            } else if (ev->GetTypeRewrite() == NYql::NDq::IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::EventType) {
                firstAttemptToGetData = true;
            } else if (ev->GetTypeRewrite() == NKqp::TEvKqpExecuter::TEvStreamData::EventType) {
                auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;
                Y_ASSERT(record.GetResultSet().rows().size() == 0);

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetEnough(false);
                resp->Record.SetSeqNo(record.GetSeqNo());
                runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                return true;
            }

            return false;
        };

        auto createSession = [&]() {
            runtime->Send(new IEventHandle(kqpProxy, sender, new TEvKqp::TEvCreateSessionRequest()));
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(sender);
            auto record = reply->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            return record.GetResponse().GetSessionId();
        };

        auto createTable = [&](const TString& sessionId, const TString& queryText) {
            auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
            ev->Record.MutableRequest()->SetQuery(queryText);

            runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        };

        auto sendQuery = [&](const TString& queryText) {
            auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
            ev->Record.MutableRequest()->SetQuery(queryText);
            ev->Record.MutableRequest()->SetKeepSession(false);
            ActorIdToProto(sender, ev->Record.MutableRequestActorId());

            runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        };

        createTable(createSession(), R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table` (Key int32, Fk int32, Value int32, PRIMARY KEY(Key), INDEX Index GLOBAL ON (Fk));
        )");

        server->GetRuntime()->SetEventFilter(captureEvents);

        sendQuery(R"(
            SELECT Value FROM `/Root/Table` VIEW Index WHERE Fk IN AsList(1, 2, 3);
        )");
    }

    Y_UNIT_TEST(LimitOverSecondaryIndexRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(true);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        TStreamExecScanQuerySettings querySettings;
        querySettings.Explain(true);

        auto itIndex = db.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/SecondaryComplexKeys` VIEW Index
            WHERE Fk1 == 1
            LIMIT 2;
        )", querySettings).GetValueSync();

        UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());

        auto res = CollectStreamResult(itIndex);
        UNIT_ASSERT(res.PlanJson);

        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto indexRead = FindPlanNodeByKv(plan, "Node Type", "Limit-TableRangeScan");
        if (!indexRead.IsDefined()) {
            indexRead = FindPlanNodeByKv(plan, "Node Type", "Limit-Filter-TableRangeScan");
        }
        UNIT_ASSERT(indexRead.IsDefined());
        auto indexTable = FindPlanNodeByKv(indexRead, "Table", "SecondaryComplexKeys/Index/indexImplTable");
        UNIT_ASSERT(indexTable.IsDefined());
        auto limit = FindPlanNodeByKv(indexRead, "Limit", "2");
        UNIT_ASSERT(limit.IsDefined());
    }

    Y_UNIT_TEST(TopSortOverSecondaryIndexRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(true);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session);

        TStreamExecScanQuerySettings querySettings;
        querySettings.Explain(true);

        auto itIndex = db.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/SecondaryComplexKeys` VIEW Index
            WHERE Fk1 == 1
            ORDER BY Fk1 LIMIT 2;
        )", querySettings).GetValueSync();

        UNIT_ASSERT_C(itIndex.IsSuccess(), itIndex.GetIssues().ToString());

        auto res = CollectStreamResult(itIndex);
        UNIT_ASSERT(res.PlanJson);

        Cerr << *res.PlanJson;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        auto indexRead = FindPlanNodeByKv(plan, "Node Type", "Limit-TableRangeScan");
        if (!indexRead.IsDefined()) {
            indexRead = FindPlanNodeByKv(plan, "Node Type", "Limit-Filter-TableRangeScan");
        }
        UNIT_ASSERT(indexRead.IsDefined());
        auto indexTable = FindPlanNodeByKv(indexRead, "Table", "SecondaryComplexKeys/Index/indexImplTable");
        UNIT_ASSERT(indexTable.IsDefined());
        auto limit = FindPlanNodeByKv(indexRead, "Limit", "2");
        UNIT_ASSERT(limit.IsDefined());
    }

    Y_UNIT_TEST(Like) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                    ('SomeString1', '100'),
                    ('SomeString2', '200'),
                    ('SomeString3', '300'),
                    ('SomeString4', '400'),
                    ('SomeString5', '500'),
                    ('SomeString6', '600'),
                    ('SomeString7', '700'),
                    ('SomeString8', '800');
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TStreamExecScanQuerySettings querySettings;
        querySettings.Explain(true);
        auto it = db.StreamExecuteScanQuery(R"(
            SELECT * FROM `/Root/TestTable` WHERE Key like "SomeString%";
        )", querySettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto res = CollectStreamResult(it);
        UNIT_ASSERT(res.PlanJson);
        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*res.PlanJson, &plan, true);

        UNIT_ASSERT_VALUES_EQUAL(plan["tables"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["name"], "/Root/TestTable");
        UNIT_ASSERT_VALUES_EQUAL(plan["tables"][0]["reads"].GetArray().size(), 1);
        auto& read = plan["tables"][0]["reads"][0];
        UNIT_ASSERT_VALUES_EQUAL(read["type"], "Scan");
        UNIT_ASSERT_VALUES_EQUAL(read["scan_by"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read["scan_by"][0], "Key [SomeString, SomeStrinh)");
    }
}

Y_UNIT_TEST_SUITE(KqpRequestContext) {

    Y_UNIT_TEST(TraceIdInErrorMessage) {
        auto settings = TKikimrSettings()
            .SetAppConfig(AppCfg())
            .SetEnableScriptExecutionOperations(true)
            .SetNodeCount(4)
            .SetUseRealThreads(false);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();

        NKikimr::NKqp::TKqpPlanner::UseMockEmptyPlanner = true;
        Y_DEFER {
            NKikimr::NKqp::TKqpPlanner::UseMockEmptyPlanner = false;  // just in case if test fails
        };

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(NKikimr::NKqp::TKqpResourceInfoExchangerEvents::EvSendResources, 4);
            kikimr.GetTestServer().GetRuntime()->DispatchEvents(opts);
        }

        auto it = kikimr.RunCall([&db] {
            return db.StreamExecuteScanQuery(R"(
                SELECT Text, SUM(Key) AS Total FROM `/Root/EightShard`
                GROUP BY Text
                ORDER BY Total DESC;
            )").GetValueSync();
        });

        UNIT_ASSERT(it.IsSuccess());
        kikimr.RunCall([&it] {
            try {
                auto yson = StreamResultToYson(it, true, NYdb::EStatus::PRECONDITION_FAILED, "TraceId");
            } catch (const std::exception& ex) {
                UNIT_ASSERT_C(false, "Exception NYdb::EStatus::PRECONDITION_FAILED not found or IssueMessage doesn't contain 'TraceId'");
            }
            return true;
        });

        NKikimr::NKqp::TKqpPlanner::UseMockEmptyPlanner = false;
    }
}

} // namespace NKqp
} // namespace NKikimr
