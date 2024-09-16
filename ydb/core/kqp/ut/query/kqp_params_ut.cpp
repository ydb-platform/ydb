#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpParams) {
    Y_UNIT_TEST(RowsList) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = session.PrepareDataQuery(Q1_(R"(
            DECLARE $rows AS List<Struct<Group: Uint32?, Name: String?, Amount: Uint64?, Comment: String?>>;

            UPSERT INTO `/Root/Test`
            SELECT Group, Name, Amount FROM AS_TABLE($rows);
        )")).ExtractValueSync().GetQuery();

        auto params = query.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Amount").OptionalUint64(1000)
                        .AddMember("Comment").OptionalString("New")
                        .AddMember("Group").OptionalUint32(137)
                        .AddMember("Name").OptionalString("Sergey")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Amount").OptionalUint64(2000)
                        .AddMember("Comment").OptionalString("New")
                        .AddMember("Group").OptionalUint32(137)
                        .AddMember("Name").OptionalString("Boris")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = query.Execute(
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            std::move(params)).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 137;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([
            [[2000u];#;[137u];["Boris"]];
            [[1000u];#;[137u];["Sergey"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MissingParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $group AS Uint32;
            DECLARE $name AS String;

            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(BadParameterType) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .AddParam("$group")
                .Int32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $group AS Uint32;
            DECLARE $name AS String;

            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ImplicitParameterTypes) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .AddParam("$group")
                .Int32(1)
                .Build()
            .Build();

        // don't DECLARE parameter types in text query
        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CheckQueryCacheForPreparedQuery) {
        // All params are declared in the text
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q1_(R"(
            DECLARE $group AS Int32;
            DECLARE $name AS String;

            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )");

        auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .AddParam("$group")
                .Int32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
    }

    Y_UNIT_TEST(CheckQueryCacheForUnpreparedQuery) {
        // Some params are declared in text, some by user
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q1_(R"(
            DECLARE $group AS Int32;

            SELECT $group, $name;
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .AddParam("$group")
                .Int32(1)
                .Build()
            .AddParam("$phone")
                .String("80")
                .Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto firstQueryResult = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(firstQueryResult.GetStatus(), EStatus::SUCCESS, firstQueryResult.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*firstQueryResult.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

        {
            // The same query with the same params
            auto params = db.GetParamsBuilder()
                .AddParam("$name")
                    .String("Sergey")
                    .Build()
                .AddParam("$group")
                    .Int32(1)
                    .Build()
                .AddParam("$phone")
                    .String("80")
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            // The same query with different type of user param
            auto params = db.GetParamsBuilder()
                .AddParam("$name")
                    .Int64(2)
                    .Build()
                .AddParam("$group")
                    .Int32(1)
                    .Build()
                .AddParam("$phone")
                    .String("80")
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            // The same query with extra param
            auto params = db.GetParamsBuilder()
                .AddParam("$name")
                    .String("Sergey")
                    .Build()
                .AddParam("$group")
                    .Int32(1)
                    .Build()
                .AddParam("$phone")
                    .String("80")
                    .Build()
                .AddParam("$age")
                    .Int32(1)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            // The same query with less params
            auto params = db.GetParamsBuilder()
                .AddParam("$name")
                    .String("Sergey")
                    .Build()
                .AddParam("$group")
                    .Int32(1)
                    .Build()
                .Build();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }
    }

    Y_UNIT_TEST(CheckQueryCacheForExecuteAndPreparedQueries) {
        // All params are declared in the text
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q1_(R"(
            DECLARE $group AS Int32;
            DECLARE $name AS String;

            SELECT * FROM `/Root/Test` WHERE Group = $group AND Name = $name;
        )");

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String("Sergey")
                .Build()
            .AddParam("$group")
                .Int32(1)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

        auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(CheckCacheByAst) {
        auto query1 = Q1_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1 AND Name = "2";
        )");
        auto query2 = Q1_(R"(
            select * from `/Root/Test` where Group = 1 AND Name = "2";
        )");

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            // Check disable setting
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableAstCache(false);
            auto setting = NKikimrKqp::TKqpSetting();
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(true));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            result = session.ExecuteDataQuery(query2, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        {
            // Check 2 exec queries
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(true));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            result = session.ExecuteDataQuery(query2, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }
        {
            // Check Prepare and Exec queries
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(true));
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto prepareResult = session.PrepareDataQuery(query1).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());

            auto execResult = session.ExecuteDataQuery(query2, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*execResult.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }
    }

    Y_UNIT_TEST(CheckCacheWithRecompilationQuery) {
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(true));
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE `TestCacheWithRecompile` (
                version Int64,
                id Int64,
                PRIMARY KEY (version, id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto query = Q1_(R"(
            --!syntax_v1

            DECLARE $items as List<Struct<version:Int64,id:Int64>>;
            UPSERT INTO `/Root/TestCacheWithRecompile`
            SELECT `version`, `id` FROM AS_TABLE($items);
        )");

        auto prepareResult = session.PrepareDataQuery(query).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());

        auto params = prepareResult.GetQuery().GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("id").Int64(0)
                    .AddMember("version").Int64(1)
                .EndStruct()
            .EndList()
            .Build()
        .Build();

        auto execResult = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());
        auto stats = NYdb::TProtoAccessor::GetProto(*execResult.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

        schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            DROP TABLE TestCacheWithRecompile;
            CREATE TABLE `TestCacheWithRecompile` (
                version Int64,
                id Int64,
                PRIMARY KEY (version, id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        execResult = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());
        stats = NYdb::TProtoAccessor::GetProto(*execResult.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
    }

    Y_UNIT_TEST(ExplicitSameParameterTypesQueryCacheCheck) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // enable query cache
        NYdb::NTable::TExecDataQuerySettings execSettings{};
        execSettings.KeepInQueryCache(true);
        // enable extraction of cache status from the reply
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        for (int i = 0; i < 2; ++i) {
            auto params = db.GetParamsBuilder().AddParam("$group").Int32(1).Build().Build();
            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $group AS Int32;
                SELECT * FROM `/Root/Test` WHERE Group = $group;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), true);
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ImplicitSameParameterTypesQueryCacheCheck) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // enable query cache
        NYdb::NTable::TExecDataQuerySettings execSettings{};
        execSettings.KeepInQueryCache(true);
        // enable extraction of cache status from the reply
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        for (int i = 0; i < 2; ++i) {
            auto params = db.GetParamsBuilder().AddParam("$group").Int32(1).Build().Build();
            // don't DECLARE parameter type in text query
            auto result = session.ExecuteDataQuery(Q1_(R"(
                SELECT * FROM `/Root/Test` WHERE Group = $group;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), true);
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ImplicitDifferentParameterTypesQueryCacheCheck) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableImplicitQueryParameterTypes(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({NKikimrKqp::TKqpSetting()});
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // enable query cache
        NYdb::NTable::TExecDataQuerySettings execSettings{};
        execSettings.KeepInQueryCache(true);
        // enable extraction of cache status from the reply
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        // two queries differ only by parameter type
        for (const auto& params : { db.GetParamsBuilder().AddParam("$group").Int32(1).Build().Build(), db.GetParamsBuilder().AddParam("$group").Uint32(1).Build().Build() }) {
            // don't DECLARE parameter type in text query
            auto result = session.ExecuteDataQuery(Q1_(R"(
                SELECT * FROM `/Root/Test` WHERE Group = $group;
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), true);
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DefaultParameterValue) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$value1")
                .OptionalUint32(11)
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $value1 AS Uint32?;
            DECLARE $value2 AS String?;

            SELECT $value1, $value2;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[11u];#]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ParameterTypes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$ParamBool").Bool(true).Build()
            .AddParam("$ParamInt8").Int8(-5).Build()
            .AddParam("$ParamByte").Uint8(5).Build()
            .AddParam("$ParamInt16").Int16(-8).Build()
            .AddParam("$ParamUint16").Uint16(8).Build()
            .AddParam("$ParamInt32").Int32(-10).Build()
            .AddParam("$ParamUint32").Uint32(10).Build()
            .AddParam("$ParamInt64").Int64(-20).Build()
            .AddParam("$ParamUint64").Uint64(20).Build()
            .AddParam("$ParamFloat").Float(30.5).Build()
            .AddParam("$ParamDouble").Double(40.5).Build()
            .AddParam("$ParamDecimal").Decimal(TDecimalValue("50.5")).Build()
            .AddParam("$ParamDyNumber").DyNumber("60.5").Build()
            .AddParam("$ParamString").String("StringValue").Build()
            .AddParam("$ParamUtf8").Utf8("Utf8Value").Build()
            .AddParam("$ParamYson").Yson("[{Value=50}]").Build()
            .AddParam("$ParamJson").Json("[{\"Value\":60}]").Build()
            .AddParam("$ParamJsonDocument").JsonDocument("[{\"Value\":70}]").Build()
            .AddParam("$ParamDate").Date(TInstant::ParseIso8601("2020-01-10")).Build()
            .AddParam("$ParamDatetime").Datetime(TInstant::ParseIso8601("2020-01-11 15:04:53")).Build()
            .AddParam("$ParamTimestamp").Timestamp(TInstant::ParseIso8601("2020-01-12 21:18:37")).Build()
            .AddParam("$ParamInterval").Interval(3600).Build()
            .AddParam("$ParamTzDate").TzDate("2022-03-14,GMT").Build()
            .AddParam("$ParamTzDateTime").TzDatetime("2022-03-14T00:00:00,GMT").Build()
            .AddParam("$ParamTzTimestamp").TzTimestamp("2022-03-14T00:00:00.123,GMT").Build()
            .AddParam("$ParamOpt").OptionalString("Opt").Build()
            .AddParam("$ParamTuple")
                .BeginTuple()
                .AddElement().Utf8("Tuple0")
                .AddElement().Int32(1)
                .EndTuple()
                .Build()
            .AddParam("$ParamList")
                .BeginList()
                .AddListItem().Uint64(17)
                .AddListItem().Uint64(19)
                .EndList()
                .Build()
            .AddParam("$ParamEmptyList")
                .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Uint64).Build())
                .Build()
            .AddParam("$ParamStruct")
                .BeginStruct()
                .AddMember("Name").Utf8("Paul")
                .AddMember("Value").Int64(-5)
                .EndStruct()
                .Build()
            .AddParam("$ParamDict")
                .BeginDict()
                .AddDictItem()
                    .DictKey().String("Key1")
                    .DictPayload().Uint32(10)
                .AddDictItem()
                    .DictKey().String("Key2")
                    .DictPayload().Uint32(20)
                .EndDict()
                .Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $ParamBool AS Bool;
            DECLARE $ParamInt8 AS Int8;
            DECLARE $ParamByte AS Uint8;
            DECLARE $ParamInt16 AS Int16;
            DECLARE $ParamUint16 AS Uint16;
            DECLARE $ParamInt32 AS Int32;
            DECLARE $ParamUint32 AS Uint32;
            DECLARE $ParamInt64 AS Int64;
            DECLARE $ParamUint64 AS Uint64;
            DECLARE $ParamFloat AS Float;
            DECLARE $ParamDouble AS Double;
            DECLARE $ParamDecimal AS Decimal(22, 9);
            DECLARE $ParamDyNumber AS DyNumber;
            DECLARE $ParamString AS String;
            DECLARE $ParamUtf8 AS Utf8;
            DECLARE $ParamYson AS Yson;
            DECLARE $ParamJson AS Json;
            DECLARE $ParamJsonDocument AS JsonDocument;
            DECLARE $ParamDate AS Date;
            DECLARE $ParamDatetime AS Datetime;
            DECLARE $ParamTimestamp AS Timestamp;
            DECLARE $ParamInterval AS Interval;
            DECLARE $ParamTzDate AS TzDate;
            DECLARE $ParamTzDateTime AS TzDateTime;
            DECLARE $ParamTzTimestamp AS TzTimestamp;
            DECLARE $ParamOpt AS String?;
            DECLARE $ParamTuple AS Tuple<Utf8, Int32>;
            DECLARE $ParamList AS List<Uint64>;
            DECLARE $ParamEmptyList AS List<Uint64>;
            DECLARE $ParamStruct AS Struct<Name:Utf8,Value:Int64>;
            DECLARE $ParamDict AS Dict<String,Uint32>;

            SELECT
                $ParamBool AS ValueBool,
                $ParamInt8 AS ValueInt8,
                $ParamByte AS ValueByte,
                $ParamInt16 AS ValueInt16,
                $ParamUint16 AS ValueUint16,
                $ParamInt32 AS ValueInt32,
                $ParamUint32 AS ValueUint32,
                $ParamInt64 AS ValueInt64,
                $ParamUint64 AS ValueUint64,
                $ParamFloat AS ValueFloat,
                $ParamDouble AS ValueDouble,
                $ParamDecimal AS ValueDecimal,
                $ParamDyNumber AS ValueDyNumber,
                $ParamString AS ValueString,
                $ParamUtf8 AS ValueUtf8,
                $ParamYson AS ValueYson,
                $ParamJson AS ValueJson,
                $ParamJsonDocument AS ValueJsonDocument,
                $ParamDate AS ValueDate,
                $ParamDatetime AS ValueDatetime,
                $ParamTimestamp AS ValueTimestamp,
                $ParamInterval AS ValueInterval,
                $ParamTzDate AS ValueTzDate,
                $ParamTzDateTime AS ValueTzDateTime,
                $ParamTzTimestamp AS ValueTzTimestamp,
                $ParamOpt AS ValueOpt,
                $ParamTuple AS ValueTuple,
                $ParamList AS ValueList,
                $ParamEmptyList AS ValueEmptyList,
                $ParamStruct AS ValueStruct,
                $ParamDict AS ValueDict;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto actual = ReformatYson(FormatResultSetYson(result.GetResultSet(0)));
        auto expected1 = ReformatYson(R"([[
            %true;-5;5u;-8;8u;-10;10u;-20;20u;30.5;40.5;"50.5";".605e2";"StringValue";"Utf8Value";"[{Value=50}]";
            "[{\"Value\":60}]";"[{\"Value\":70}]";18271u;1578755093u;1578863917000000u;3600;"2022-03-14,GMT";
            "2022-03-14T00:00:00,GMT";"2022-03-14T00:00:00.123000,GMT";["Opt"];["Tuple0";1];[17u;19u];[];["Paul";-5];
            [["Key2";20u];["Key1";10u]]
        ]])");
        auto expected2 = ReformatYson(R"([[
            %true;-5;5u;-8;8u;-10;10u;-20;20u;30.5;40.5;"50.5";".605e2";"StringValue";"Utf8Value";"[{Value=50}]";
            "[{\"Value\":60}]";"[{\"Value\":70}]";18271u;1578755093u;1578863917000000u;3600;"2022-03-14,GMT";
            "2022-03-14T00:00:00,GMT";"2022-03-14T00:00:00.123000,GMT";["Opt"];["Tuple0";1];[17u;19u];[];["Paul";-5];
            [["Key1";10u];["Key2";20u]]
        ]])");

        UNIT_ASSERT_C(actual == expected1 || actual == expected2, "expected: " << expected1 << ", got: " << actual);
    }

    Y_UNIT_TEST(InvalidJson) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE TestJson (
                Key Int32,
                Value Json,
                PRIMARY KEY (Key)
            ) WITH (
                PARTITION_AT_KEYS = (10)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto params = kikimr.GetTableClient().GetParamsBuilder()
            .AddParam("$key1").Int32(5).Build()
            .AddParam("$value1").Json("{'bad': 5}").Build()
            .AddParam("$key2").Int32(15).Build()
            .AddParam("$value2").Json("{\"ok\": \"15\"}").Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key1 AS Int32;
            DECLARE $value1 AS Json;
            DECLARE $key2 AS Int32;
            DECLARE $value2 AS Json;

            UPSERT INTO TestJson (Key, Value) VALUES
                ($key1, $value1),
                ($key2, $value2);
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }
}

} // namespace NKqp
} // namespace NKikimr
