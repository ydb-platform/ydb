#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpNewEngine) {
    Y_UNIT_TEST(Select1) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [ [1]; ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ContainerRegistryCombiner) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE `ImageByRegistry` (
                id_registryId String,
                id_name	String,
                id_imageId	String,
                created	Int64,
                updated	Int64,
                status	String,
                PRIMARY KEY (id_registryId, id_name, id_imageId)
            );

        )").GetValueSync());

        auto testQueryParams = [&] (TString query, TParams params) {
            AssertSuccessResult(session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).GetValueSync());

            UNIT_ASSERT(db.StreamExecuteScanQuery(query, params).GetValueSync().IsSuccess());
        };

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `ImageByRegistry` (id_registryId, id_name, id_imageId, status) VALUES
                ("10", "One", "10", "One"),
                ("20", "Two", "20", "Two");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        auto params4 = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$pred_registry").OptionalString("10").Build()
            .AddParam("$pred_image_name").String("One").Build()
            .AddParam("$pred_result_name_parts").Int32(2).Build()
            .Build();

        testQueryParams(R"(
            --!syntax_v1
            DECLARE $pred_registry AS String?;
            DECLARE $pred_image_name AS String;
            DECLARE $pred_result_name_parts AS Int32;
            SELECT String::JoinFromList(name, "/") as name, count(1) AS cnt
            FROM (SELECT ListTake(String::SplitToList(name, "/"), $pred_result_name_parts)
                AS name FROM (SELECT id_name AS name FROM `ImageByRegistry` WHERE
                (id_registryId = $pred_registry) AND (id_name LIKE $pred_image_name)) ) GROUP BY name ORDER BY name ASC;
        )", params4);
    }

    Y_UNIT_TEST(SimpleUpsertSelect) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE `KeyValue` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteDataQuery(R"(
            --!syntax_v1
            REPLACE INTO `KeyValue` (Key, Value) VALUES
                (1u, "One"),
                (2u, "Two"),
                (3u, "Three");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());

        auto selectResult = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM `KeyValue`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();

        AssertSuccessResult(selectResult);

        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]];
            [[3u];["Three"]]
        ])", FormatResultSetYson(selectResult.GetResultSet(0)));
    }

    Y_UNIT_TEST(PkSelect1) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            --!syntax_v1

            DECLARE $key AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key = $key;
        )";

        auto explainResult = session.ExplainDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());
        UNIT_ASSERT_C(explainResult.GetAst().Contains("KqpReadRangesSource"), explainResult.GetAst());

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$key").Uint64(302).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[2];[302u];["Value2"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases().size()); // no LiteralExecuter phase
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access().size());
        UNIT_ASSERT_VALUES_EQUAL("/Root/EightShard", stats.query_phases()[0].table_access()[0].name());
        if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].affected_shards());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access()[0].partitions_count());
        }

        params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$key").Uint64(330).Build()
            .Build();

        result = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases().size()); // no LiteralExecuter phase
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access().size());
        UNIT_ASSERT_VALUES_EQUAL("/Root/EightShard", stats.query_phases()[0].table_access()[0].name());
        if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].affected_shards());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access()[0].partitions_count());
        }
    }

    Y_UNIT_TEST(PkSelect2) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            --!syntax_v1

            DECLARE $group AS Uint32?;
            DECLARE $name AS String?;

            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )";

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$group").OptionalUint32(1).Build()
            .AddParam("$name").OptionalString("Paul").Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[300u];["None"];[1u];["Paul"]]])", FormatResultSetYson(result.GetResultSet(0)));

        AssertTableStats(result, "/Root/Test", {
            .ExpectedReads = 1,
        });

        params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$group").OptionalUint32(1).Build()
            .Build();

        result = session.ExecuteDataQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PkRangeSelect1) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$low")
                .Uint64(202)
                .Build()
            .AddParam("$high")
                .Uint64(402)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(

            DECLARE $low AS Uint64;
            DECLARE $high AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key > $low AND Key < $high ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"([
            [[3];[203u];["Value3"]];
            [[3];[301u];["Value1"]];
            [[2];[302u];["Value2"]];
            [[1];[303u];["Value3"]];
            [[1];[401u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PkRangeSelect2) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$low")
                .Uint64(202)
                .Build()
            .AddParam("$high")
                .Uint64(402)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $low AS Uint64;
            DECLARE $high AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key >= $low AND Key <= $high ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"([
            [[1];[202u];["Value2"]];
            [[3];[203u];["Value3"]];
            [[3];[301u];["Value1"]];
            [[2];[302u];["Value2"]];
            [[1];[303u];["Value3"]];
            [[1];[401u];["Value1"]];
            [[3];[402u];["Value2"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PkRangeSelect3) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$low")
                .Uint64(602)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $low AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key > $low ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"([
            [[1];[603u];["Value3"]];
            [[1];[701u];["Value1"]];
            [[3];[702u];["Value2"]];
            [[2];[703u];["Value3"]];
            [[2];[801u];["Value1"]];
            [[1];[802u];["Value2"]];
            [[3];[803u];["Value3"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PkRangeSelect4) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$high")
                .Uint64(202)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $high AS Uint64;

            SELECT * FROM `/Root/EightShard` WHERE Key < $high ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"([
            [[1];[101u];["Value1"]];
            [[3];[102u];["Value2"]];
            [[2];[103u];["Value3"]];
            [[2];[201u];["Value1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultiSelect) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value2 = 0 ORDER BY Value1 DESC, Key;
            SELECT * FROM `/Root/Test` WHERE Group = 1 ORDER BY Amount, Group, Name;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[2u];["Two"];[0]];
            [[4000000002u];["BigTwo"];[0]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        CompareYson(R"([
            [[300u];["None"];[1u];["Paul"]];
            [[3500u];["None"];[1u];["Anna"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 2 ORDER BY Amount, Group, Name;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[7200u];["None"];[2u];["Tony"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultiOutput) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
                $left = (select Key, Value1, Value2 from `/Root/TwoShard` where Value2 = 1);
                $right = (select Key, Value1, Value2 from `/Root/TwoShard` where Value2 = -1);
                select Key, Value1, Value2 from $left order by Key;
                select Key, Value1, Value2 from $right order by Key;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"([
            [[3u];["Three"];[1]];
            [[4000000003u];["BigThree"];[1]];
        ])", FormatResultSetYson(result.GetResultSet(0)));

        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST(InShardsWrite) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard`
            SELECT Key, Value1, Value2 + 1 AS Value2 FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[1u];["One"];[0]];
                [[2u];["Two"];[1]];
                [[3u];["Three"];[2]];
                [[4000000001u];["BigOne"];[0]];
                [[4000000002u];["BigTwo"];[1]];
                [[4000000003u];["BigThree"];[2]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ShuffleWrite) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard`
            SELECT Key - 3u AS Key, Value1, Value2 + 100 AS Value2 FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value2 > 10 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[0u];["Three"];[101]];
                [[3999999998u];["BigOne"];[99]];
                [[3999999999u];["BigTwo"];[100]];
                [[4000000000u];["BigThree"];[101]];
                [[4294967294u];["One"];[99]];
                [[4294967295u];["Two"];[100]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(KeyColumnOrder) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        // Please do not change the name of columns here
        {
            auto builder = TTableBuilder()
               .AddNullableColumn("key", EPrimitiveType::Uint64)
               .AddNullableColumn("index_0", EPrimitiveType::Utf8)
               .AddNullableColumn("value", EPrimitiveType::Uint32)
               .SetPrimaryKeyColumns({"key","index_0"});

            auto result = session.CreateTable("/Root/Test1", builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const TString query1(R"(
                --!syntax_v1
                DECLARE $items AS List<Struct<'key':Uint64,'index_0':Utf8,'value':Uint32>>;
                UPSERT INTO `/Root/Test1`
                   SELECT * FROM AS_TABLE($items);
            )");

            TParamsBuilder builder;
            builder.AddParam("$items").BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("key").Uint64(646464646464)
                        .AddMember("index_0").Utf8("SomeUtf8Data")
                        .AddMember("value").Uint32(323232)
                    .EndStruct()
                .EndList()
            .Build();

            static const TTxControl txControl = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
            auto result = session.ExecuteDataQuery(query1, txControl, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto& yson = ReadTablePartToYson(session, "/Root/Test1");
            const TString expected = R"([[[646464646464u];["SomeUtf8Data"];[323232u]]])";
            UNIT_ASSERT_VALUES_EQUAL(yson, expected);
        }
    }

    Y_UNIT_TEST(KeyColumnOrder2) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            const TString query1(R"(
                --!syntax_v1
                DECLARE $items AS List<Struct<'key':Uint64,'index_0':Utf8,'value':Uint32>>;
                SELECT * FROM AS_TABLE($items);
            )");

            TParamsBuilder builder;
            builder.AddParam("$items").BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("key").Uint64(646464646464)
                        .AddMember("index_0").Utf8("SomeUtf8Data")
                        .AddMember("value").Uint32(323232)
                    .EndStruct()
                .EndList()
            .Build();

            static const TTxControl txControl = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
            auto result = session.ExecuteDataQuery(query1, txControl, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto yson = FormatResultSetYson(result.GetResultSet(0));
            const auto expected = R"([["SomeUtf8Data";646464646464u;323232u]])";
            UNIT_ASSERT_VALUES_EQUAL(yson, expected);
        }
    }

    Y_UNIT_TEST(BlindWrite) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                (10u, "One", -10),
                (20u, "Two", -20);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value2 <= -10 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[10u];["One"];[-10]];
                [[20u];["Two"];[-20]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BlindWriteParameters) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key1")
                .Uint32(10)
                .Build()
            .AddParam("$value1")
                .String("New")
                .Build()
            .AddParam("$key2")
                .Uint32(4000000010u)
                .Build()
            .AddParam("$value2")
                .String("New")
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $key1 AS Uint32;
            DECLARE $value1 AS String;
            DECLARE $key2 AS Uint32;
            DECLARE $value2 AS String;

            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
                ($key1, $value1),
                ($key2, $value2);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            std::move(params)).ExtractValueSync();

        AssertSuccessResult(result);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value1 = "New" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[10u];["New"];#];
                [[4000000010u];["New"];#]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BlindWriteListParameter) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint32(10)
                    .AddMember("Value1")
                        .OptionalString("New")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint32(4000000010u)
                    .AddMember("Value1")
                        .OptionalString("New")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $items AS List<Struct<Key:Uint32?, Value1:String?>>;

            UPSERT INTO `/Root/TwoShard`
            SELECT * FROM AS_TABLE($items);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), std::move(params)).ExtractValueSync();
        AssertSuccessResult(result);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value1 = "New" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[10u];["New"];#];
                [[4000000010u];["New"];#]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BatchUpload) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto queryText = R"(
            DECLARE $items AS
                List<Struct<
                    Key1: Uint32?,
                    Key2: String?,
                    Value1: Int64?,
                    Value2: Double?,
                    Blob1: String?,
                    Blob2: String?>>;

            $itemsSource = (
                SELECT
                    Item.Key1 AS Key1,
                    Item.Key2 AS Key2,
                    Item.Value1 AS Value1,
                    Item.Value2 AS Value2,
                    Item.Blob1 AS Blob1,
                    Item.Blob2 AS Blob2
                FROM (SELECT $items AS Lst) FLATTEN BY Lst AS Item
            );

            UPSERT INTO `/Root/BatchUpload`
            SELECT * FROM $itemsSource;
        )";

        auto result = session.PrepareDataQuery(queryText).ExtractValueSync();
        auto query = result.GetQuery();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const ui32 BatchSize = 200;
        const ui32 BatchCount = 5;
        const ui32 Blob1Size = 100;
        const ui32 Blob2Size = 400;

        TDuration totalLatency;
        for (ui32 i = 0; i < BatchCount; ++i) {
            auto paramsBuilder = query.GetParamsBuilder();
            auto& itemsParam = paramsBuilder.AddParam("$items");

            itemsParam.BeginList();
            for (ui32 i = 0; i < BatchSize; ++i) {
                itemsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Blob1")
                        .OptionalString(TString(Blob1Size, '0' + i % 10))
                    .AddMember("Blob2")
                        .OptionalString(TString(Blob2Size, '0' + (i + 1) % 10))
                    .AddMember("Key1")
                        .OptionalUint32(RandomNumber<ui32>())
                    .AddMember("Key2")
                        .OptionalString(CreateGuidAsString())
                    .AddMember("Value1")
                        .OptionalInt64(i)
                    .AddMember("Value2")
                        .OptionalDouble(RandomNumber<ui64>())
                    .EndStruct();
            }
            itemsParam.EndList();
            itemsParam.Build();

            auto beginTime = TInstant::Now();

            auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            AssertSuccessResult(result);

            auto batchLatency = TInstant::Now() - beginTime;
            totalLatency += batchLatency;
        }
        Cout << "Total upload latency: " << totalLatency << Endl;
    }

#if !defined(_ubsan_enabled_)
    Y_UNIT_TEST(Aggregate) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT Data, SUM(Key) AS Total FROM `/Root/EightShard` GROUP BY Data ORDER BY Data;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"(
            [[[1];[3615u]];[[2];[3616u]];[[3];[3617u]]]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(AggregateTuple) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT Data, Text, COUNT(Key) AS Total FROM `/Root/EightShard` GROUP BY Data, Text ORDER BY Data, Text;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        AssertSuccessResult(result);

        CompareYson(R"([
            [[1];["Value1"];3u];[[1];["Value2"];3u];[[1];["Value3"];2u];
            [[2];["Value1"];3u];[[2];["Value2"];2u];[[2];["Value3"];3u];
            [[3];["Value1"];2u];[[3];["Value2"];3u];[[3];["Value3"];3u]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
#endif

    Y_UNIT_TEST(PureExpr) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            SELECT '42', 42, 5*5;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(), execSettings).ExtractValueSync();

        CompareYson(R"([["42";42;25]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT(stats.query_phases(0).table_access().size() == 0);
    }

    Y_UNIT_TEST(MultiStatement) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            SELECT 1;
            SELECT 2;
            SELECT 3;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(), execSettings).ExtractValueSync();

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[2]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[3]])", FormatResultSetYson(result.GetResultSet(2)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT(stats.query_phases(0).table_access().size() == 0);
    }

    Y_UNIT_TEST(MultiStatementMixPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT 1;
            SELECT Key FROM `/Root/TwoShard` ORDER BY Key DESC LIMIT 1;
            SELECT 2;
            SELECT Key FROM `/Root/EightShard` ORDER BY Key ASC LIMIT 1;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[4000000003u]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[2]])", FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[101u]]])", FormatResultSetYson(result.GetResultSet(3)));
    }

    Y_UNIT_TEST(LocksSingleShard) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        auto session2 = db.CreateSession().GetValueSync().GetSession();
        result = session2.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES(1, "NewValue");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 2;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(LocksMultiShard) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        auto session2 = db.CreateSession().GetValueSync().GetSession();
        result = session2.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES(101, "NewValue");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard`;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(LocksMultiShardOk) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard`;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();
        AssertSuccessResult(result);
    }

    Y_UNIT_TEST(LocksEffects) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        auto session2 = db.CreateSession().GetValueSync().GetSession();
        result = session2.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES(1, "NewValue");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        result = session1.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key,Value1) VALUES(2, "NewValue");
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED));

        result = session2.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key <= 2;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        CompareYson(R"([[[1u];["NewValue"];[-1]];[[2u];["Two"];[0]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LocksNoMutations) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(R"(
            SELECT 42;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[42]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LocksNoMutationsSharded) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(R"(
            SELECT 42;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[42]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BrokenLocksAtROTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        {
            auto session2 = db.CreateSession().GetValueSync().GetSession();
            result = session2.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value)
                    VALUES (3u, "Three")
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        result = session.ExecuteDataQuery(R"(
            SELECT 42;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(BrokenLocksAtROTxSharded) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        {
            auto session2 = db.CreateSession().GetValueSync().GetSession();
            result = session2.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2)
                    VALUES (4u, "Four", 4)
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        result = session.ExecuteDataQuery(R"(
            SELECT 42;
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(BrokenLocksOnUpdate) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            SELECT * FROM `/Root/TwoShard` WHERE Key = 4000000001u;       -- read 2nd shard

            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES     -- write 1st shard
                (11u, "Eleven", 11);
        )", TTxControl::BeginTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        {
            auto session2 = db.CreateSession().GetValueSync().GetSession();
            result = session2.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES  -- write 2nd shard
                    (4000000001u, "XXX", -101)
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto txResult = tx->Commit().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(txResult.GetStatus(), EStatus::ABORTED, txResult.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(txResult.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED));

        result = session.ExecuteDataQuery(R"(
            SELECT Key, Value1, Value2 FROM `/Root/TwoShard` WHERE Key = 11u
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeferredEffects) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            UPSERT INTO `/Root/TwoShard`
            SELECT Key + 1u AS Key, Value1 FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);

        auto params = db.GetParamsBuilder()
            .AddParam("$key")
                .Uint32(100)
                .Build()
            .AddParam("$value")
                .String("New")
                .Build()
            .Build();

        result = session.ExecuteDataQuery(R"(

            DECLARE $key AS Uint32;
            DECLARE $value AS String;

            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
                ($key, $value);
        )", TTxControl::Tx(*tx), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT COUNT(*) FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[6u]])", FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[2u];["One"];[0]];
            [[3u];["Two"];[1]];
            [[4u];["Three"];#];
            [[100u];["New"];#];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["BigOne"];[0]];
            [[4000000003u];["BigTwo"];[1]];
            [[4000000004u];["BigThree"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PureTxMixedWithDeferred) {
        auto settings = TKikimrSettings();
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (3u, "Three")
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(R"(
            SELECT 1=1;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [ [%true]; ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ReadAfterWrite) {
        auto settings = TKikimrSettings();
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO KeyValue (Key, Value) VALUES (3u, "Three")
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        result = session.ExecuteDataQuery(R"(
            SELECT Amount FROM Test WHERE Group = 1 ORDER BY Amount DESC;
        )", TTxControl::Tx(*tx).CommitTx(), execSettings).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"([[[3500u]];[[300u]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        // Commit cannot be merged with physical tx for read-write transactions
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
    }

    Y_UNIT_TEST(PrunePartitionsByLiteral) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 101 OR Key = 301
            ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx(), execSettings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1];[101u];["Value1"]];[[3];[301u];["Value1"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = 0;
        if (stats.query_phases().size() == 2) {
            phase = 1;
        } else if (stats.query_phases().size() == 1) {
            phase = 0;
        } else {
            UNIT_ASSERT(false);
        }

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access().size(), 1);
        if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).affected_shards(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).partitions_count(), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 2);
        UNIT_ASSERT(stats.query_phases(phase).table_access(0).reads().bytes() > 0);
        UNIT_ASSERT(stats.query_phases(phase).duration_us() > 0);
    }

    Y_UNIT_TEST(PrunePartitionsByExpr) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(300).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $key AS Uint64;
            SELECT * FROM `/Root/EightShard` WHERE Key = $key + 1;
        )", TTxControl::BeginTx().CommitTx(), params, execSettings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[3];[301u];["Value1"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        ui32 index = 0;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 0);
        UNIT_ASSERT(stats.query_phases(0).table_access().size() == 0);

        index = 1;

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).reads().rows(), 1);
        UNIT_ASSERT(stats.query_phases(index).table_access(0).reads().bytes() > 0);
        UNIT_ASSERT(stats.query_phases(index).duration_us() > 0);
    }

    Y_UNIT_TEST(PruneWritePartitions) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = TParamsBuilder()
            .AddParam("$Key").Uint32(10).Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            DECLARE $Key AS UInt32;

            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                ($Key, "One", -10)
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        AssertSuccessResult(result);

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 0);
        UNIT_ASSERT(stats.query_phases(0).table_access().size() == 0);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 1);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Value2 <= -10 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"([
            [[10u];["One"];[-10]];
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Truncated) {
        TVector<NKikimrKqp::TKqpSetting> settings;
        NKikimrKqp::TKqpSetting setting;
        setting.SetName("_ResultRowsLimit");
        setting.SetValue("5");
        settings.push_back(setting);

        auto kikimr = DefaultKikimrRunner(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
        UNIT_ASSERT(result.GetResultSet(0).Truncated());
    }

    Y_UNIT_TEST(Replace) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/TwoShard` (Value1, Key) VALUES
                ("Newvalue 1", 1u),
                ("Newvalue 5", 5u);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

            SELECT * FROM `/Root/TwoShard` WHERE Key <= 5 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Newvalue 1"];#];
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]];
            [[5u];["Newvalue 5"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/Logs` (App, Ts, Host, Message) VALUES
                ("new_app_1", 100, "new_app_host_1.search.yandex.net", "Initialize"),
                ("new_app_1", 200, "new_app_host_2.search.yandex.net", "Initialized");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

            SELECT * FROM `/Root/Logs` WHERE App = "new_app_1" ORDER BY App, Ts, Host;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [["new_app_1"];["new_app_host_1.search.yandex.net"];["Initialize"];[100]];
            [["new_app_1"];["new_app_host_2.search.yandex.net"];["Initialized"];[200]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/Logs` (App, Host, Message) VALUES
                ("new_app_2", "host_2_1", "Empty"),
                ("new_app_2", "host_2_2", "Empty");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Join) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            SELECT t1.Key AS Key, t2.Value2 AS Value
            FROM `/Root/KeyValue` AS t1
            INNER JOIN `/Root/Join2` AS t2
            ON t1.Value = t2.Key2
            WHERE t2.Name == "Name1"
            ORDER BY Key, Value;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Value21"]];
            [[1u];["Value25"]];
            [[2u];["Value22"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinWithParams) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$data")
                .BeginList()
                    .AddListItem().BeginStruct().AddMember("Key").Uint64(1).EndStruct()
                    .AddListItem().BeginStruct().AddMember("Key").Uint64(5).EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $data AS List<Struct<Key: Uint64>>;

            SELECT t.Key AS Key, t.Value AS Value
            FROM AS_TABLE($data) AS d
            INNER JOIN `/Root/KeyValue` AS t ON t.Key = d.Key
            ORDER BY Key, Value;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"]];
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinIdxLookup) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(

            $input = (
                SELECT Key, CAST(Fk21 AS Uint32) AS Fk21
                FROM `/Root/Join1` WHERE Value == "Value1"
            );

            SELECT t1.Key AS Key, t2.Value2 AS Value
            FROM $input AS t1
            INNER JOIN `/Root/Join2` AS t2
            ON t1.Fk21 = t2.Key1
            ORDER BY Key, Value;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];["Value21"]];
            [[1];["Value22"]];
            [[1];["Value23"]];
            [[2];["Value24"]];
            [[9];["Value21"]];
            [[9];["Value22"]];
            [[9];["Value23"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);

            for (const auto& tableStat : stats.query_phases(0).table_access()) {
                if (tableStat.name() == "/Root/Join2") {
                    UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), 7);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(tableStat.name(), "/Root/Join1");
                }
            }
        } else {
            if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
            }

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Join1");

            ui32 index = 1;
            if (!settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 0);
                index = 2;
            }

            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).name(), "/Root/Join2");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(index).table_access(0).reads().rows(), 4);
        }
    }

    Y_UNIT_TEST(JoinIdxLookupWithPredicate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Left` (
                Key Uint64,
                Value1 Uint64,
                Value2 String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Right` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `Left` (Key, Value1, Value2) VALUES
                (1, 6, "Value1"),
                (2, 2, "Value1"),
                (3, 3, "Value2"),
                (4, 4, "Value2"),
                (5, 5, "Value3"),
                (6, 6, "Value1");

            REPLACE INTO `Right` (Key, Value) VALUES
                (1, "One"),
                (2, "Two"),
                (3, "Three"),
                (4, "Four"),
                (5, "Five"),
                (6, "Six");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());

        auto result = session.ExecuteDataQuery(R"(
            $input = (
                SELECT Key, Value1
                FROM `Left` WHERE Value2 == "Value1"
            );

            SELECT t1.Key AS Key, t2.Value AS Value
            FROM $input AS t1
            INNER JOIN `Right` AS t2
            ON t1.Value1 = t2.Key AND t1.Key = t2.Key
            ORDER BY Key, Value;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[2u];["Two"]];
            [[6u];["Six"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LeftSemiJoin) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // add nulls
        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (4u, "Four"), (NULL, "Null");
            REPLACE INTO `/Root/Join2` (Key1, Key2, Name, Value2) VALUES (1, NULL, "Name Null", "Value Null");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

                SELECT Key1, Key2, Name, Value2
                FROM `/Root/Join2` AS t1
                LEFT SEMI JOIN `/Root/KeyValue` AS t2
                ON t1.Key2 == t2.Value
                ORDER BY Key1, Key2, Name;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[101u];["One"];["Name1"];["Value21"]];
            [[101u];["Two"];["Name1"];["Value22"]];
            [[102u];["One"];["Name2"];["Value24"]];
            [[103u];["One"];["Name1"];["Value25"]];
            [[104u];["One"];["Name3"];["Value26"]];
            [[105u];["One"];["Name2"];["Value27"]];
            [[105u];["Two"];["Name4"];["Value28"]];
            [[106u];["One"];["Name3"];["Value29"]];
            [[108u];["One"];#;["Value31"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            $list1 = AsList(
                AsStruct("One" AS Key1, 1 AS Value1),
                AsStruct("Two" AS Key1, 2 AS Value1),
            );

            $list2 = AsList(
                AsStruct("One" AS Key2, 10 AS Value2),
                AsStruct("Three" AS Key2, 30 AS Value2),
            );

            $list3 = AsList(
                AsStruct("v1" AS Value3),
                AsStruct("v2" AS Value3),
            );

            $list4 = AsList(
                AsStruct((Cast ("One" AS String?)) AS Key4, 100 AS Value4),
                AsStruct((Cast ("Two" AS String?)) AS Key4, 200 AS Value4),
                AsStruct((Cast (Null AS String?))  AS Key4, 300 AS Value4),
            );

            $table1 = SELECT * FROM AS_TABLE($list1);
            $table2 = SELECT * FROM AS_TABLE($list2);
            $table3 = SELECT * FROM AS_TABLE($list3);
            $table4 = SELECT * FROM AS_TABLE($list4);

            SELECT * FROM $table1 as t1
            JOIN $table2 as t2 ON t1.Key1 = t2.Key2
            JOIN $table4 as t4 ON t1.Key1 = t4.Key4
            CROSS JOIN $table3 as t3
            ORDER BY Value3;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            ["One";"One";["One"];1;10;"v1";100];
            ["One";"One";["One"];1;10;"v2";100]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinPureUncomparableKeys) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            $l = AsList(
                AsStruct(AsTuple(1,2,3) as Key, "1,2,3" as Payload),
                AsStruct(AsTuple(1,2,4) as Key, "1,2,4" as Payload)
            );
            $r = AsList(
                AsStruct(AsTuple(1,2) as Key, "1,2" as Payload),
                AsStruct(AsTuple(2,3) as Key, "2,3" as Payload)
            );

            select l.Key, l.Payload, r.Key, r.Payload
            from AS_TABLE($l) as l join AS_TABLE($r) as r on l.Key = r.Key;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SelfJoin) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT a.Key AS Key FROM TwoShard AS a
            INNER JOIN (SELECT * FROM TwoShard WHERE Value1 = "Three") AS b
            ON a.Value2 = b.Value2
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[3u]];
            [[4000000003u]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Update) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(

            UPDATE `/Root/TwoShard`
            SET Value1 = "Updated"
            WHERE Value2 = 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 0);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 2);
        UNIT_ASSERT(stats.query_phases(1).table_access(0).updates().bytes() > 0);
        UNIT_ASSERT(stats.query_phases(1).duration_us() > 0);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[2u];["Two"];[0]];
            [[3u];["Updated"];[1]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["BigTwo"];[0]];
            [[4000000003u];["Updated"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Delete) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(

            DELETE FROM `/Root/TwoShard`
            WHERE Value2 = -1;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        // Phase reading rows to delete
        UNIT_ASSERT(stats.query_phases(0).duration_us() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 6);

        // Phase deleting rows
        UNIT_ASSERT(stats.query_phases(1).duration_us() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).deletes().rows(), 2);

        result = session.ExecuteDataQuery(R"(

            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]];
            [[4000000002u];["BigTwo"];[0]];
            [[4000000003u];["BigThree"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteOn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(

            DELETE FROM `/Root/TwoShard` ON
            SELECT * FROM `/Root/TwoShard` WHERE Value2 = 1;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        // Phase reading rows to delete
        UNIT_ASSERT(stats.query_phases(0).duration_us() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 6);

        // Phase deleting rows
        UNIT_ASSERT(stats.query_phases(1).duration_us() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).deletes().rows(), 2);

        result = session.ExecuteDataQuery(R"(

            SELECT * FROM `/Root/TwoShard` ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[2u];["Two"];[0]];
            [[4000000001u];["BigOne"];[-1]];
            [[4000000002u];["BigTwo"];[0]];
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultiEffects) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(

                UPDATE `/Root/TwoShard` SET Value1 = "Updated" WHERE Value2 = 1;
                UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                    (4u, "Four", 4);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

                SELECT * FROM `/Root/TwoShard` ORDER BY Key;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
                [[1u];["One"];[-1]];
                [[2u];["Two"];[0]];
                [[3u];["Updated"];[1]];
                [[4u];["Four"];[4]];
                [[4000000001u];["BigOne"];[-1]];
                [[4000000002u];["BigTwo"];[0]];
                [[4000000003u];["Updated"];[1]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateFromParams) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = TParamsBuilder()
            .AddParam("$data")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("Key").Uint32(10)
                        .AddMember("Value1").String("Ten")
                        .AddMember("Value2").Int32(-10)
                        .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("Key").Uint32(11)
                        .AddMember("Value1").String("Eleven")
                        .AddMember("Value2").Int32(-11)
                        .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1

                DECLARE $data AS List<Struct<Key: Uint32, Value1: String, Value2: Int32>>;

                UPSERT INTO `/Root/TwoShard`
                SELECT Key, Value1, Value2 FROM AS_TABLE($data)
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

                SELECT * FROM `/Root/TwoShard` WHERE Key > 5 AND Key < 12 ORDER BY Key;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
                [[10u];["Ten"];[-10]];
                [[11u];["Eleven"];[-11]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PruneEffectPartitions) {
        TKikimrSettings serverSettings;
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
                --!syntax_v1

                declare $key as UInt64;
                declare $text as String;

                update `/Root/EightShard` set Text = $text where Key = $key
            )";

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(501).Build()
            .AddParam("$text").String("foo").Build()
            .Build();

        auto settings = TExecDataQuerySettings()
            .CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto it = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*it.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        if (!serverSettings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 0);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 1);

        it = session.ExecuteDataQuery(R"(

                select Key, Text, Data from `/Root/EightShard` where Text = "foo" order by Key
            )",TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[501u];["foo"];[2]];
        ])", FormatResultSetYson(it.GetResultSet(0)));
    }

    Y_UNIT_TEST(DecimalColumn) {
        auto kikimr = DefaultKikimrRunner();

        TTableClient client{kikimr.GetDriver()};
        auto session = client.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.CreateTable("/Root/DecimalTest",
            TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", TDecimalType(22, 9))
                .SetPrimaryKeyColumn("Key")
                .Build()).GetValueSync().IsSuccess());

        auto params = TParamsBuilder().AddParam("$in").BeginList()
            .AddListItem().BeginStruct()
                .AddMember("Key").Uint64(1)
                .AddMember("Value").Decimal(TDecimalValue("10.123456789"))
                .EndStruct()
            .AddListItem().BeginStruct()
                .AddMember("Key").Uint64(2)
                .AddMember("Value").Decimal(TDecimalValue("20.987654321"))
                .EndStruct()
            .EndList().Build().Build();

        auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                DECLARE $in AS List<Struct<Key: Uint64, Value: Decimal(22, 9)>>;

                REPLACE INTO `/Root/DecimalTest`
                    SELECT Key, Value FROM AS_TABLE($in);
            )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
                SELECT Key, Value FROM `/Root/DecimalTest` ORDER BY Key
            )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([
                [[1u];["10.123456789"]];
                [[2u];["20.987654321"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LocksInRoTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();

        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            select * from `/Root/TwoShard` where Key = 1;
        )", TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        for (ui64 i : {2u, 3u, 4u, 4000000001u, 4000000002u, 4000000003u}) {
            result = session.ExecuteDataQuery(Sprintf(R"(
                select * from `/Root/TwoShard` where Key = %ld;
            )", i), TTxControl::Tx(*tx)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto ret = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_C(ret.IsSuccess(), ret.GetIssues().ToString());
    }

    Y_UNIT_TEST(ItemsLimit) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        enum EType {
            None, Literal, Parameter, Expression, /* QueryResult */
        };

        for (auto takeType : {EType::Literal, EType::Parameter, EType::Expression}) {
            for (auto skipType : {EType::None, EType::Literal, EType::Parameter, EType::Expression}) {
                for (auto withSort : {false, true}) { // TODO: passthrough FlatMap
                    TStringBuilder query;
                    query << "--!syntax_v1" << Endl
                          << "declare $limit as Uint64;" << Endl
                          << "declare $offset as Uint64;" << Endl
                          << "select Key, Value from `/Root/KeyValue`";

                    if (withSort) {
                        query << " order by Key"; // redundant sort by PK
                    }

                    const int limitValue = 5;
                    const int offsetValue = 1;

                    switch (takeType) {
                        case None: UNIT_FAIL("Unexpected"); break;
                        case Literal: query << " limit " << limitValue; break;
                        case Parameter: query << " limit $limit"; break;
                        case Expression: query << " limit ($limit + 1)"; break;
                    }

                    switch (skipType) {
                        case None: break;
                        case Literal: query << " offset " << offsetValue; break;
                        case Parameter: query << " offset $offset"; break;
                        case Expression: query << " offset ($offset + 1)"; break;
                    }

                    auto result = session.ExplainDataQuery(query).GetValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                        TStringBuilder() << query << Endl << "Failed with: " << result.GetIssues().ToString());
                    UNIT_ASSERT_C(result.GetAst().Contains("('('\"ItemsLimit\""),
                        TStringBuilder() << query << Endl << "Failed with AST: " << result.GetAst());

                    NJson::TJsonValue plan;
                    NJson::ReadJsonTree(result.GetPlan(), &plan, true);
                    auto node = FindPlanNodeByKv(plan, "Node Type", "TableFullScan");
                    UNIT_ASSERT(node.IsDefined());

                    TStringBuilder readLimitValue;
                    if (takeType == EType::Literal && skipType == EType::None) {
                        readLimitValue << limitValue;
                    } else if (takeType == EType::Literal && skipType == EType::Literal) {
                        readLimitValue << limitValue + offsetValue;
                    } else {
                        readLimitValue << "%kqp%tx_result_binding_0_0";
                    }

                    auto readLimit = FindPlanNodeByKv(node, "ReadLimit", readLimitValue);
                    UNIT_ASSERT_C(readLimit.IsDefined(), result.GetPlan());
                }
            }
        }

        // TODO: Take(query result) . ReadTable
//        test(R"(
//            $limit = (
//                select Key + 1 from `/Root/KeyValue` where Value = 'Four'
//            );
//            select Key, Value from `/Root/KeyValue` limit $limit ?? 7;
//        )");
    }

    Y_UNIT_TEST(OnlineRO_Consistent) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_INFO);

        auto result = session.ExecuteDataQuery(R"(
            SELECT Value1, Value2, Key FROM `/Root/TwoShard` WHERE Value2 != 0 ORDER BY Key DESC;
        )", TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [["BigThree"];[1];[4000000003u]];
                [["BigOne"];[-1];[4000000001u]];
                [["Three"];[1];[3u]];
                [["One"];[-1];[1u]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(OnlineRO_Inconsistent) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_INFO);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Value1, Value2, Key FROM `/Root/TwoShard` WHERE Value2 != 0 ORDER BY Key DESC;
            )", TTxControl::BeginTx(TTxSettings::OnlineRO(TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx())
            .ExtractValueSync();
            AssertSuccessResult(result);

            CompareYson(R"(
                [
                    [["BigThree"];[1];[4000000003u]];
                    [["BigOne"];[-1];[4000000001u]];
                    [["Three"];[1];[3u]];
                    [["One"];[-1];[1u]]
                ]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {  // stream lookup query
            auto result = session.ExecuteDataQuery(R"(
                $list = SELECT Key FROM `/Root/TwoShard`;
                SELECT Value, Key FROM `/Root/KeyValue` WHERE Key IN $list ORDER BY Key;
            )", TTxControl::BeginTx(TTxSettings::OnlineRO(TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx())
            .ExtractValueSync();
            AssertSuccessResult(result);

            CompareYson(R"(
                [
                    [["One"];[1u]];
                    [["Two"];[2u]]
                ]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(StaleRO) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `FollowersKv` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                PARTITION_AT_KEYS = (10, 20, 30),
                READ_REPLICAS_SETTINGS = "ANY_AZ:1"
            );
        )").GetValueSync());

        AssertSuccessResult(session.ExecuteDataQuery(R"(
            --!syntax_v1

            REPLACE INTO `FollowersKv` (Key, Value) VALUES
                (1u, "One"),
                (11u, "Two"),
                (21u, "Three"),
                (31u, "Four");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_INFO);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        //kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::PIPE_SERVER, NActors::NLog::PRI_DEBUG);

        // Followers immediate
        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM FollowersKv WHERE Key = 21;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[21u];["Three"]];
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));

        // Followers distributed
        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM FollowersKv WHERE Value != "One" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[11u];["Two"]];
                [[21u];["Three"]];
                [[31u];["Four"]];
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));

        // No followers immediate
        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM TwoShard WHERE Key = 2;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[2u];["Two"];[0]];
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));

        // No followers distributed
        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM TwoShard WHERE Value2 < 0 ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [[1u];["One"];[-1]];
                [[4000000001u];["BigOne"];[-1]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(StaleRO_Immediate) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_INFO);

        auto result = session.ExecuteDataQuery(R"(
            SELECT Value1, Value2, Key FROM `/Root/TwoShard` WHERE Value2 != 0 ORDER BY Key DESC;
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        CompareYson(R"(
            [
                [["BigThree"];[1];[4000000003u]];
                [["BigOne"];[-1];[4000000001u]];
                [["Three"];[1];[3u]];
                [["One"];[-1];[1u]]
            ]
        )", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ReadRangeWithParams) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            DECLARE $max_key as Uint64;
            DECLARE $min_key as Uint64;
            SELECT * FROM `/Root/KeyValue` WHERE Key <= $max_key AND Key >= $min_key;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto range = FindPlanNodeByKv(plan, "ReadRange", "[\"Key [$min_key, $max_key]\"]");
        UNIT_ASSERT(range.IsDefined());
    }

    Y_UNIT_TEST(Nondeterministic) { // TODO: KIKIMR-4759
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            UPSERT INTO `/Root/TwoShard`
            SELECT
                100u AS Key,
                CAST(CurrentUtcTimestamp() AS String) AS Value1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM `/Root/TwoShard` WHERE Key = 100;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
    }

    Y_UNIT_TEST(ScalarFunctions) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/TableOne` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/TableTwo` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/TableThree` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/TableEmpty` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            REPLACE INTO `/Root/TableOne` (Key, Value) VALUES
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6);
            REPLACE INTO `/Root/TableTwo` (Key, Value) VALUES
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6);
            REPLACE INTO `/Root/TableThree` (Key, Value) VALUES
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TVector<std::pair<TString,TString>> testData = {
            {
                R"(
                    $lmt1 = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);
                    $lmt2 = (SELECT Value FROM `/Root/TableTwo` WHERE Key = 3);

                    SELECT Value FROM `/Root/TableThree` ORDER BY Value
                            LIMIT (CAST($lmt1 AS Uint64) ?? 0) * (CAST($lmt2 AS Uint64) ?? 0);
                )",
                R"([
                    [[1u]];[[2u]];[[3u]];[[4u]];[[5u]];[[6u]]
                ])"
            },
            {
                R"(
                    $lmt = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);

                    SELECT Value FROM `/Root/TableTwo` ORDER BY Value LIMIT CAST($lmt AS Uint64) ?? 0;
                )",
                R"([
                    [[1u]];[[2u]]
                ])"
            },
            {
                R"(
                    $lmt = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);

                    SELECT Value FROM `/Root/TableTwo` ORDER BY Value DESC LIMIT COALESCE($lmt, 1u);
                )",
                R"([
                    [[6u]];[[5u]]
                ])"
            },
            {
                R"(
                    $lmt = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);
                    $offt = (SELECT Value FROM `/Root/TableOne` WHERE Key = 3);

                    SELECT Value FROM `/Root/TableTwo` ORDER BY Value DESC
                        LIMIT COALESCE($lmt, 1u) OFFSET COALESCE($offt, 1u);
                )",
                R"([
                    [[3u]];[[2u]]
                ])"
            },
            {
                R"(
                    $key = (SELECT Value FROM `/Root/TableOne` WHERE Key = 5);

                    SELECT Value FROM `/Root/TableTwo` WHERE Key >= $key ORDER BY Value ASC;
                )",
                R"([
                    [[5u]];[[6u]]
                ])"
            },
            {
                R"(
                    $key = (SELECT Value FROM `/Root/TableOne` WHERE Key = 5);

                    SELECT Value FROM `/Root/TableTwo` WHERE Key >= $key ORDER BY Value DESC;
                )",
                R"([
                    [[6u]];[[5u]]
                ])"
            },
            {
                R"(
                    $key1 = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);
                    $key2 = (SELECT Value FROM `/Root/TableTwo` WHERE Key = 3);

                    SELECT Value FROM `/Root/TableTwo` WHERE Key = $key1 * $key2 ORDER BY Value;
                )",
                R"([
                    [[6u]]
                ])"
            },
            {
                R"(
                    $keys = (SELECT Value FROM `/Root/TableOne` WHERE Key > 2);

                    SELECT Value FROM `/Root/TableTwo` WHERE Key IN $keys ORDER BY Value;
                )",
                R"([
                    [[3u]];[[4u]];[[5u]];[[6u]]
                ])"
            },
            {
                R"(
                    $keys = (SELECT Value FROM `/Root/TableOne` WHERE Key > 2);

                    SELECT Value FROM `/Root/TableTwo` WHERE Value IN COMPACT $keys ORDER BY Value;
                )",
                R"([
                    [[3u]];[[4u]];[[5u]];[[6u]]
                ])"
            },
#if 0
            // Count IF is not supported in DqBuildAggregationResultStage, there is no AsStruct and
            // optimizer failed. Need to fix later.
            {
                R"(
                    $divisor = (SELECT Value FROM `/Root/TableOne` WHERE Key = 2);

                    SELECT COUNT_IF(Value % $divisor == 1) AS odd_count FROM `/Root/TableTwo`;
                )",
                R"([
                    [3u]
                ])"
            },
#endif
        };

        for (auto& item: testData) {
            auto result = session.ExecuteDataQuery(item.first,
                TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
            auto resultYson = FormatResultSetYson(result.GetResultSet(0));
            CompareYson(item.second, resultYson);
        }

        for (auto& item: testData) {
            auto it = db.StreamExecuteScanQuery(item.first).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            CompareYson(item.second, CollectStreamResult(it).ResultSetYson);
        }
    }

    Y_UNIT_TEST(DeleteWithBuiltin) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            DELETE FROM TwoShard WHERE Value1 != TestUdfs::RandString(10);
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_EQUAL(stats.query_phases().size(), 2);
    }

    Y_UNIT_TEST(MultiEffectsOnSameTable) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (100, 100500), (100500, 100);
            DELETE FROM `/Root/EightShard` ON (Key) VALUES (100);
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

//        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
//        Cerr << "!!!\n" << stats.DebugString() << Endl;

        result = session.ExecuteDataQuery(R"(
            SELECT Key, Data
            FROM `/Root/EightShard`
            WHERE Key=100 or Key=100500
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson("[[[100500u];[100]]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultiUsagePrecompute) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1


            $input1 = SELECT * FROM EightShard WHERE Text = "Value1";
            $input2 = SELECT * FROM EightShard WHERE Text = "Value2";

            $value = SELECT COUNT(Data) FROM $input1;

            SELECT Data FROM $input2 WHERE Data <= $value
            ORDER BY Data
            LIMIT 5;

        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson("[[[1]];[[1]];[[1]];[[2]];[[2]]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SqlInFromCompact) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE `/Root/table1` (
                key String,
                cached String,
                PRIMARY KEY (key)
            );

            CREATE TABLE `/Root/table2` (
                key String,
                in_cache String,
                value String,
                PRIMARY KEY (key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/table1` (key, cached) VALUES
                ("Key1", "CachedValue1"),
                ("Key2", "CachedValue2");
         )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(

            REPLACE INTO `/Root/table2` (
                key, in_cache, value
            ) VALUES
                ("Key1", "CachedValue1", "Value 1"),
                ("Key2", "CachedValue2", "Value 2");
         )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $t1 = SELECT `cached`, `key` FROM `/Root/table1`
                    WHERE `key` = "Key1";

            $cache = (SELECT `cached` FROM $t1);

            $t2 = SELECT `value`, `in_cache` FROM `table2`
                    WHERE `key` = "Key1" AND `in_cache` IN COMPACT $cache;

            SELECT `in_cache`, `value` FROM $t1 AS a
                INNER JOIN $t2 AS b
                    ON a.`cached` == b.`in_cache`
                    WHERE a.`cached` = b.`in_cache`;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(PrecomputeKey) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1


            $key = SELECT Fk22 AS Key2 FROM Join1 WHERE Key = 1;

            SELECT Value2 FROM Join2 WHERE Key1 = 101 AND Key2 = $key;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[["Value21"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UnionAllPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1


            $data =
                SELECT * FROM KeyValue
                UNION ALL
                SELECT 100ul AS Key, "NewValue" AS Value;

            SELECT * FROM $data ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]];
            [[100u];["NewValue"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteON) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DELETE FROM `/Root/Join2` where (Key1 = 1 and Key2 = "") OR Key1 = 3;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(JoinWithPrecompute) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $data =
                SELECT Key1, Key2, Name, Value2
                FROM `/Root/Join2`
                WHERE Name IS NOT NULL;

            $max = SELECT max(Key1) AS k FROM $data WHERE Name != "foo";

            $to_delete =
                SELECT l.Key AS Key, l.Fk21 AS Key1, l.Fk22 AS Key2
                FROM `/Root/Join1` AS l JOIN $data AS r ON l.Fk21 = r.Key1;

            SELECT $max ?? 0, Key1, Key2 FROM $to_delete ORDER BY Key1, Key2 LIMIT 1;

            DELETE FROM `/Root/Join1` ON SELECT Key FROM $to_delete;
            DELETE FROM `/Root/Join2` ON SELECT Key1, Key2 FROM $data;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[106u;[101u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinProjectMulti) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT t1.Key AS Key, t2.Value2 AS Value, t2.Value2 AS Text
            FROM KeyValue AS t1
            INNER JOIN Join2 AS t2
            ON t1.Value = t2.Key2
            WHERE t2.Name == "Name1"
            ORDER BY Key, Value;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Value21"];["Value21"]];
            [[1u];["Value25"];["Value25"]];
            [[2u];["Value22"];["Value22"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinMultiConsumer) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .String("One")
                    .AddMember("Text")
                        .String("Text1")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $items AS List<Struct<
                Key: String,
                Text: String
            >>;

            $list = SELECT * FROM AS_TABLE($items);

            $data = SELECT * FROM KeyValue WHERE Value != "Two";

            SELECT t1.Key AS Key, t2.Text AS Text
            FROM $data AS t1
            INNER JOIN $list AS t2
            ON t1.Value = t2.Key
            ORDER BY Key, Text;

            UPSERT INTO KeyValue
            SELECT Key + 1 AS Key, Value FROM $data;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];"Text1"]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpsertEmptyInput) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto param = db.GetParamsBuilder()
            .AddParam("$rows")
                .EmptyList(
                    TTypeBuilder()
                        .BeginStruct()
                            .AddMember("Key").BeginOptional().Primitive(EPrimitiveType::Uint32).EndOptional()
                            .AddMember("Value1").BeginOptional().Primitive(EPrimitiveType::String).EndOptional()
                            .AddMember("Value2").BeginOptional().Primitive(EPrimitiveType::Int32).EndOptional()
                        .EndStruct()
                    .Build()
                )
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $rows AS List<Struct<
                Key : Uint32?,
                Value1 : String?,
                Value2 : Int32?
            >>;

            UPSERT INTO `/Root/TwoShard`
            SELECT * FROM AS_TABLE($rows);
        )", TTxControl::BeginTx().CommitTx(), param).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DeleteByKey) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto params = TParamsBuilder()
            .AddParam("$group").Uint32(1).Build()
            .AddParam("$name").String("Paul").Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $group AS Uint32;
            DECLARE $name AS String;

            DELETE FROM Test WHERE Group = $group AND Name = $name;
        )", TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/Test", {
            .ExpectedReads = 0,
            .ExpectedDeletes = 1,
        });

        params = TParamsBuilder()
            .AddParam("$groups")
                .BeginList()
                .AddListItem().Uint32(2)
                .AddListItem().Uint32(3)
                .EndList()
                .Build()
            .Build();

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $groups AS List<Uint32>;

            DELETE FROM Test WHERE Group IN $groups;
        )", TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/Test", {
            .ExpectedReads = 1,
            .ExpectedDeletes = 1,
        });

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM Test;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        CompareYson(R"([
            [[3500u];["None"];[1u];["Anna"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        params = TParamsBuilder()
            .AddParam("$keys")
                .BeginList()
                .AddListItem().Uint32(1)
                .AddListItem().Uint32(2)
                .EndList()
                .Build()
            .Build();

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $keys AS List<Uint32>;

            DELETE FROM TwoShard WHERE Key IN $keys;
        )", TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/TwoShard", {
            .ExpectedReads = 0,
            .ExpectedDeletes = 2,
        });

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM TwoShard WHERE Key < 10;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        CompareYson(R"([
            [[3u];["Three"];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteWithInputMultiConsumption) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $keys = SELECT Fk21 AS Key1, Fk22 AS Key2 FROM Join1 WHERE Value = "Value1";

            DELETE FROM Join2 ON
            SELECT * FROM $keys;

            SELECT COUNT(*) FROM $keys;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/Join1", {
            .ExpectedReads = 9,
        });

        AssertTableStats(result, "/Root/Join2", {
            .ExpectedDeletes = 3,
        });

        CompareYson(R"([
            [3u]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM Join2 WHERE Key1 = 101 ORDER BY Key1, Key2;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[101u];["Three"];["Name3"];["Value23"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(DeleteWithInputMultiConsumptionLimit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $keys =
                SELECT Group, Name, Amount FROM Test
                WHERE Amount > 1000
                ORDER BY Group, Name
                LIMIT 1;

            DELETE FROM Test ON
            SELECT Group, Name FROM $keys;

            DELETE FROM Test ON
            SELECT Group, "Paul" AS Name FROM $keys;

            SELECT * FROM $keys;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/Test", {
            .ExpectedReads = 1,
            .ExpectedDeletes = 2,
        });

        CompareYson(R"([
            [[3500u];[1u];["Anna"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    // KIKIMR-14022
    Y_UNIT_TEST(JoinSameKey) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA DisableSimpleColumns;
            SELECT *
            FROM `Join1` AS l JOIN `Join2` AS r ON l.Fk21 = r.Key1 AND r.Key1 = l.Fk21
            WHERE l.Key = 1
            ORDER BY r.Value2
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[101u];["One"];[1];["Value1"];[101u];["One"];["Name1"];["Value21"]];
            [[101u];["One"];[1];["Value1"];[101u];["Two"];["Name1"];["Value22"]];
            [[101u];["One"];[1];["Value1"];[101u];["Three"];["Name3"];["Value23"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JoinDictWithPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT xx.id AS id, Key1, Key2, Name, Value2
            FROM
                (SELECT 101 AS id) AS xx
            LEFT JOIN
                (SELECT * FROM `/Root/Join2` AS a1 FULL JOIN (SELECT 101 AS id) AS yy ON (a1.Key1 = yy.id)) AS yy
            ON (xx.id = Coalesce(yy.id));
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [101;[101u];["One"];["Name1"];["Value21"]];
            [101;[101u];["Three"];["Name3"];["Value23"]];
            [101;[101u];["Two"];["Name1"];["Value22"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(AsyncIndexUpdate) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE TestTable (
                Key Int64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key),
                INDEX AsyncIndex GLOBAL ASYNC ON (Data, Value)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            UPDATE TestTable SET Data = 10 WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DuplicatedResults) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/Test`;
            SELECT * FROM `/Root/KeyValue`;
            SELECT * FROM `/Root/Test`;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 5);

        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[3500u];["None"];[1u];["Anna"]];[[300u];["None"];[1u];["Paul"]];[[7200u];["None"];[2u];["Tony"]]])", FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(R"([[[3500u];["None"];[1u];["Anna"]];[[300u];["None"];[1u];["Paul"]];[[7200u];["None"];[2u];["Tony"]]])", FormatResultSetYson(result.GetResultSet(4)));
    }

    Y_UNIT_TEST(LookupColumns) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            $current_value = SELECT Fk21 FROM Join1 WHERE Key = 2;

            UPDATE Join1 SET Fk22 = "New" WHERE Key = 1 AND Fk21 = 100;

            SELECT $current_value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        //Cerr << result.GetPlan() << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto reads = plan["tables"][0]["reads"].GetArraySafe();
        for (auto& read : reads) {
            UNIT_ASSERT(read["columns"].GetArraySafe().size() <= 2);
        }
    }

    Y_UNIT_TEST(OrderedScalarContext) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(

            $max_key = (SELECT Key FROM `/Root/KeyValue` ORDER BY Key DESC LIMIT 1);
            SELECT $max_key ?? 0
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[2u]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LiteralKeys) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto settings = TExecDataQuerySettings()
            .CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM `/Root/Logs` WHERE App = 'nginx'u AND Ts >= 2 LIMIT 1
        )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["nginx"];["nginx-23"];["PUT /form HTTP/1.1"];[2]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases().size()); // no LiteralExecuter phase
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access().size());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].affected_shards());
        UNIT_ASSERT_VALUES_EQUAL("/Root/Logs", stats.query_phases()[0].table_access()[0].name());

        // mix non-optional param and literal
        auto params = TParamsBuilder()
            .AddParam("$app").Utf8("nginx").Build()
            .Build();

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $app AS Utf8;

            SELECT * FROM `/Root/Logs` WHERE App = $app AND Ts >= 2 LIMIT 1
        )", TTxControl::BeginTx().CommitTx(), std::move(params), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[["nginx"];["nginx-23"];["PUT /form HTTP/1.1"];[2]]])", FormatResultSetYson(result.GetResultSet(0)));

        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases().size()); // no LiteralExecuter phase
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].table_access().size());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.query_phases()[0].affected_shards());
        UNIT_ASSERT_VALUES_EQUAL("/Root/Logs", stats.query_phases()[0].table_access()[0].name());
    }

    Y_UNIT_TEST(ReadDifferentColumns) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            SELECT Fk21 FROM Join1 WHERE Value = "Value1";
            SELECT Fk22 FROM Join1 WHERE Value = "Value2";
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto reads = plan["tables"][0]["reads"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reads[0]["columns"].GetArraySafe().size(), 3);
    }

    Y_UNIT_TEST(ReadDifferentColumnsPk) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            SELECT Fk21 FROM Join1 WHERE Key = 1;
            SELECT Fk22 FROM Join1 WHERE Value = "Value2";
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto reads = plan["tables"][0]["reads"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 2);

        TSet<TString> readTypes;
        readTypes.insert(reads[0]["type"].GetString());
        readTypes.insert(reads[1]["type"].GetString());
        UNIT_ASSERT(readTypes.contains("Lookup"));
    }

    Y_UNIT_TEST(DependentSelect) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $data = (
                SELECT Data FROM EightShard WHERE Key = 401
            );

            SELECT * FROM Join1 WHERE Key = $data;
        )", TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[101u];["One"];[1];["Value1"]]])", FormatResultSetYson(result.GetResultSet(0)));

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 1,
        });

        AssertTableStats(result, "/Root/Join1", {
            .ExpectedReads = 1,
        });
    }

    Y_UNIT_TEST(ScalarMultiUsage) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$key").Uint64(701).Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $key AS Uint64;

            $row = (SELECT TableRow() FROM EightShard WHERE Key = $key);

            SELECT $row.Text AS Text;
            DELETE FROM EightShard WHERE Key = $key AND $row.Data > 0;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[["Value1"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PagingNoPredicateExtract) {
        NKikimrConfig::TAppConfig appConfig;
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig);

        TKikimrRunner kikimr{serverSettings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            DECLARE $app AS Utf8;
            DECLARE $last_ts AS Int64;
            DECLARE $last_host AS Utf8;

            $part1 = (
                SELECT * FROM Logs
                WHERE App = $app AND Ts = $last_ts AND Host > $last_host
                ORDER BY App, Ts, Host
                LIMIT 10
            );

            $part2 = (
                SELECT * FROM Logs
                WHERE App = $app AND Ts > $last_ts
                ORDER BY App, Ts, Host
                LIMIT 10
            );

            $union = (
                SELECT * FROM $part1
                UNION ALL
                SELECT * FROM $part2
            );

            SELECT Ts, Host, Message
            FROM $union
            ORDER BY Ts, Host
            LIMIT 10;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Cerr << result.GetPlan() << Endl;

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(result.GetPlan(), &plan, true);
        auto reads = plan["tables"][0]["reads"].GetArraySafe();
        for (auto& read : reads) {
            UNIT_ASSERT(read.Has("limit"));
            UNIT_ASSERT_VALUES_EQUAL(FromString<i32>(read["limit"].GetString()), 10);
        }
    }

    Y_UNIT_TEST(IdxLookupExtractMembers) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            --!syntax_v1

            DECLARE $input AS List<Struct<
                Key: Uint64,
                Text: String,
            >>;

            $to_upsert = (
                SELECT
                    i.Key AS Key,
                FROM AS_TABLE($input) AS i
                JOIN EightShard AS s
                USING (Key)
                WHERE s.Key IS NULL OR s.Text != i.Text
            );

            $to_delete = (
                SELECT s.Key AS Key
                FROM EightShard AS s
                JOIN AS_TABLE($input) AS i
                USING (Key)
            );

            DELETE FROM EightShard ON SELECT Key FROM $to_delete;
            UPSERT INTO EightShard SELECT * FROM $to_upsert;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(PushFlatmapInnerConnectionsToStageInput) {
        NKikimrConfig::TAppConfig app;
        auto settings = TKikimrSettings()
            .SetAppConfig(app);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            $subquery = SELECT Key FROM `/Root/KeyValue`;
            $subquery2 = SELECT Amount FROM `/Root/Test`;

            SELECT * FROM `/Root/EightShard`
            WHERE Key IN $subquery OR Key == 101 OR Key IN $subquery2;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1];[101u];["Value1"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PushPureFlatmapInnerConnectionsToStage) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$rows").BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Name").String("Name1")
                        .AddMember("Value2").String("Value22")
                        .AddMember("Data").String("Data1")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $rows AS List<Struct<
                Name: String,
                Value2: String,
                Data: String>>;

            $values =
                SELECT (Name, Value2) FROM Join2
                WHERE Key1 = 101;

            SELECT * FROM AS_TABLE($rows)
            WHERE (Name, Value2) IN COMPACT $values;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([["Data1";"Name1";"Value22"]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SqlInAsScalar) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$value1").Int32(3).Build()
            .AddParam("$value2").Uint64(2).Build()
            .AddParam("$value3").Int32(5).Build()
            .AddParam("$value4").OptionalInt32(3).Build()
            .AddParam("$value5").OptionalInt32({}).Build()
            .AddParam("$value6").OptionalInt64(1).Build()
            .AddParam("$value7").OptionalInt64(7).Build()
            .Build();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            DECLARE $value1 AS Int32;
            DECLARE $value2 AS Uint64;
            DECLARE $value3 AS Int32;
            DECLARE $value4 AS Int32?;
            DECLARE $value5 AS Int32?;
            DECLARE $value6 AS Int64?;
            DECLARE $value7 AS Int64?;

            $data = SELECT Data FROM EightShard WHERE Text = "Value1";

            SELECT
                $value1 IN $data,
                $value2 IN $data,
                $value3 IN $data,
                $value4 IN $data,
                $value5 IN $data,
                $value6 IN $data,
                $value7 IN $data;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[
            %true;
            %true;
            %false;
            [%true];
            #;
            [%true];
            [%false]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultiUsageInnerConnection) {
        NKikimrConfig::TAppConfig app;
        auto settings = TKikimrSettings()
            .SetAppConfig(app);

        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $count1 = SELECT COUNT (*) FROM `/Root/KeyValue`;
            $count2 = SELECT COUNT(*)
                FROM `/Root/KeyValue` AS l
                LEFT JOIN `/Root/EightShard` AS r
                ON l.Key = r.Key;
            SELECT * FROM `/Root/TwoShard` WHERE $count1 = $count2;

        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(StreamLookupForDataQuery, StreamLookupJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

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

        TExecDataQuerySettings querySettings;
        querySettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                $subquery = SELECT Key FROM `/Root/EightShard`;

                SELECT * FROM `/Root/KeyValue`
                WHERE Key IN $subquery ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx(), querySettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
            auto streamLookup = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
            UNIT_ASSERT(streamLookup.IsDefined());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

            if (StreamLookupJoin) {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);

                for (const auto& tableStat : stats.query_phases(0).table_access()) {
                    if (tableStat.name() == "/Root/EightShard") {
                        UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), 29);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(tableStat.name(), "/Root/KeyValue");
                        UNIT_ASSERT_VALUES_EQUAL(tableStat.reads().rows(), 2);
                    }
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/EightShard");
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/KeyValue");
                UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 2);
            }
        }

        {
            auto params = kikimr.GetTableClient().GetParamsBuilder()
                .AddParam("$keys").BeginList()
                    .AddListItem()
                        .Uint64(1)
                    .AddListItem()
                        .Uint64(2)
                    .EndList()
                    .Build()
                    .Build();

            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                DECLARE $keys AS List<Uint64>;

                SELECT * FROM `/Root/KeyValue`
                WHERE Key IN $keys ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx(), params, querySettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
            auto streamLookup = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT(streamLookup.IsDefined());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/KeyValue");
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).reads().rows(), 2);
        }
    }

    Y_UNIT_TEST(FlatmapLambdaMutiusedConnections) {
        NKikimrConfig::TAppConfig app;
        auto settings = TKikimrSettings()
            .SetAppConfig(app);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $values = SELECT Value2 AS Value FROM TwoShard;

            $values_filtered = SELECT * FROM $values WHERE Value < 5;

            SELECT Key FROM `/Root/EightShard`
            WHERE Data IN $values_filtered OR Data = 0
            ORDER BY Key;

            SELECT * FROM $values
            ORDER BY Value;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[101u]];[[202u]];[[303u]];[[401u]];[[502u]];[[603u]];[[701u]];[[802u]]])",
            FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[-1]];[[-1]];[[0]];[[0]];[[1]];[[1]]])",
            FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST(EmptyMapWithBroadcast) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT ts.Value1 AS c1, kv.Value AS c2, t.Name AS c3
            FROM TwoShard AS ts
            INNER JOIN KeyValue AS kv ON ts.Value2 = kv.Key
            INNER JOIN Test AS t ON ts.Key = t.Group
            WHERE ts.Key = 30;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(FlatMapLambdaInnerPrecompute) {
        NKikimrConfig::TAppConfig app;
        auto settings = TKikimrSettings()
            .SetAppConfig(app);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            $rows = SELECT * FROM KeyValue;
            $cnt = SELECT count(*) FROM $rows;
            $join = SELECT l.Value AS value FROM $rows as l LEFT JOIN EightShard AS r on l.Key = r.Key;

            $check = SELECT count(*) FROM $join;
            SELECT * FROM EightShard WHERE $check = $cnt;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DqSourceCount) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(1);
        evread.SetMaxRows(2);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(2);
        SetDefaultReadAckSettings(evreadack);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/EightShard`;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[24u]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DqSource) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key = 101 or (Key >= 202 and Key < 200+4) ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]];[[202u];[1]];[[203u];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DqSourceLiteralRange) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key >= 101 and Key < 103 ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]];[[102u];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto params = TParamsBuilder().AddParam("$param").Uint64(101).Build().Build();

            auto result = session.ExecuteDataQuery(R"(
                DECLARE $param as Uint64;
                SELECT Key, Data FROM `/Root/EightShard` WHERE Key >= $param and Key < 103 ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]];[[102u];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DqSourceLimit) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(2);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        SetDefaultReadAckSettings(evreadack);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Key, Value FROM `/Root/KeyValueLargePartition` WHERE Key >= 202 ORDER BY Key LIMIT 5;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[202u];["Value2"]];[[203u];["Value3"]];[[301u];["Value1"]];[[302u];["Value2"]];[[303u];["Value3"]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DqSourceSequentialLimit) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(2);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        SetDefaultReadAckSettings(evreadack);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` ORDER BY Key LIMIT 1;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT Key, Data FROM `/Root/EightShard` ORDER BY Key LIMIT 1;
            )", TTxControl::BeginTx(TTxSettings::OnlineRO(TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[101u];[1]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

    }

    Y_UNIT_TEST(DqSourceLocksEffects) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session3 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key <= 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        AssertSuccessResult(result);

        auto tx = result.GetTransaction();

        auto session2 = db.CreateSession().GetValueSync().GetSession();
        result = session2.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES(1, "NewValue2");
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        result = session1.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key,Value1) VALUES(1, "NewValue");
        )", TTxControl::Tx(*tx).CommitTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED));

        result = session2.ExecuteDataQuery(R"(
            SELECT Key, Value1 FROM `/Root/TwoShard` WHERE Key <= 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        AssertSuccessResult(result);

        CompareYson(R"([[[1u];["NewValue2"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(PrimaryView) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetIndexAutoChooseMode(NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_MAX_USED_PREFIX);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings querySettings;
        querySettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM `/Root/SecondaryKeys` VIEW PRIMARY KEY WHERE Fk <= 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()), querySettings).GetValueSync();
        AssertSuccessResult(result);
        AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 0);
    }

    Y_UNIT_TEST(AutoChooseIndex) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetIndexAutoChooseMode(NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_ONLY_POINTS);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings querySettings;
        querySettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT Fk, Key FROM `/Root/SecondaryKeys` WHERE Fk <= 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()), querySettings).GetValueSync();
        AssertSuccessResult(result);
        AssertTableReads(result, "/Root/SecondaryKeys/Index/indexImplTable", 1);
    }


    Y_UNIT_TEST(ComplexLookupLimit) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();

        {
            auto session = db.CreateSession().GetValueSync().GetSession();
            AssertSuccessResult(session.ExecuteSchemeQuery(R"(
                --!syntax_v1

                CREATE TABLE `/Root/Sample` (
                    A Uint64,
                    B Uint64,
                    C Uint64,
                    D Uint64,
                    E Uint64,
                    PRIMARY KEY (A, B, C)
                );

            )").GetValueSync());

            AssertSuccessResult(session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/Sample` (A, B, C, D, E) VALUES
                    (1, 1, 1, 1, 1),
                    (1, 2, 2, 2, 2),
                    (2, 2, 2, 2, 2),
                    (3, 3, 3, 3, 3),
                    (4, 4, 4, 4, 4),
                    (4, 4, 4, 4, 4),
                    (5, 5, 5, 5, 5);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());
        }

        auto params =
            TParamsBuilder()
                .AddParam("$lastCounterId").Uint64(1).Build()
                .AddParam("$lastId").Uint64(1).Build()
                .AddParam("$counterIds")
                    .BeginList()
                        .AddListItem().Uint64(1)
                        .AddListItem().Uint64(2)
                        .AddListItem().Uint64(3)
                    .EndList()
                    .Build()
                .Build();

        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings querySettings;
        querySettings.CollectQueryStats(ECollectQueryStatsMode::Profile);
        auto result = session.ExecuteDataQuery(R"(
            DECLARE $counterIds AS List<Uint64>;
            DECLARE $lastCounterId AS Uint64;
            DECLARE $lastId AS Uint64;
            SELECT A, B FROM
                    `/Root/Sample`
                    WHERE
                        A in $counterIds and
                        (A,B) > ($lastCounterId, $lastId)
                        ORDER BY A,B
                        LIMIT 2;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, querySettings).GetValueSync();

        AssertSuccessResult(result);
        AssertTableReads(result, "/Root/Sample", 2);
        CompareYson(R"([[[1u];[2u]];[[2u];[2u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }


    Y_UNIT_TEST(FullScanCount) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetExtractPredicateRangesLimit(4);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        {
            TAtomicBase before = counters.FullScansExecuted->GetAtomic();
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/EightShard` WHERE Key > 202 AND Key < 404 ORDER BY Key;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            AssertSuccessResult(result);
            UNIT_ASSERT_EQUAL(before, counters.FullScansExecuted->GetAtomic());
        }

        {
            TAtomicBase before = counters.FullScansExecuted->GetAtomic();
            auto result = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/EightShard`;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            AssertSuccessResult(result);
            UNIT_ASSERT_GT(counters.FullScansExecuted->GetAtomic(), before);
        }


        {
            auto req = R"(
                DECLARE $items AS List<Uint64>;
                SELECT Key FROM `/Root/EightShard` where Key in $items;
            )";

            auto params1 = TParamsBuilder()
                .AddParam("$items")
                .BeginList()
                    .AddListItem().Uint64(0)
                .EndList()
                .Build()
                .Build();

            TAtomicBase before = counters.FullScansExecuted->GetAtomic();
            auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params1).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            AssertSuccessResult(result);
            UNIT_ASSERT_EQUAL(counters.FullScansExecuted->GetAtomic(), before);

            auto params2 = TParamsBuilder()
                .AddParam("$items")
                .BeginList()
                    .AddListItem().Uint64(0)
                    .AddListItem().Uint64(1)
                    .AddListItem().Uint64(2)
                    .AddListItem().Uint64(3)
                    .AddListItem().Uint64(4)
                    .AddListItem().Uint64(5)
                .EndList()
                .Build()
                .Build();
            before = counters.FullScansExecuted->GetAtomic();
            auto result2 = session.ExecuteDataQuery(req, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params2).ExtractValueSync();
            result2.GetIssues().PrintTo(Cerr);
            AssertSuccessResult(result);
            UNIT_ASSERT_GT(counters.FullScansExecuted->GetAtomic(), before);
        }
    }



}

} // namespace NKikimr::NKqp
