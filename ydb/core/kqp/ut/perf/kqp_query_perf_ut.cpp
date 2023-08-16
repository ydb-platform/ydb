#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

TParams BuildUpdateParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(101)
                    .AddMember("Text").String("New")
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(209)
                    .AddMember("Text").String("New")
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

TParams BuildInsertParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(109)
                    .AddMember("Text").String("New")
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(209)
                    .AddMember("Text").String("New")
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

TParams BuildDeleteParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(101)
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(209)
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

TParams BuildUpdateIndexParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").String("Primary1")
                    .AddMember("Index2").String("SecondaryNew1")
                    .AddMember("Value").String("ValueNew1")
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").String("Primary5")
                    .AddMember("Index2").String("SecondaryNew2")
                    .AddMember("Value").String("ValueNew2")
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

TParams BuildDeleteIndexParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").String("Primary1")
                .EndStruct()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").String("Primary5")
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

TParams BuildInsertIndexParams(TTableClient& client) {
    return client.GetParamsBuilder()
        .AddParam("$items")
            .BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("Key").String("Primary10")
                    .AddMember("Index2").String("SecondaryNew10")
                    .AddMember("Value").String("ValueNew10")
                .EndStruct()
            .EndList()
        .Build()
    .Build();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryPerf) {
    Y_UNIT_TEST_TWIN(KvRead, EnableSourceRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(EnableSourceRead);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(102).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Uint64;

            SELECT * FROM EightShard
            WHERE Key = $key;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Cerr << stats.query_plan() << Endl;

        AssertTableStats(result, "/Root/EightShard", {.ExpectedReads = 1,});

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 1);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), EnableSourceRead ? 1 : 2);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("TotalTasks").GetIntegerSafe();
        }

        UNIT_ASSERT_VALUES_EQUAL(totalTasks, EnableSourceRead ? 1 : 2);
    }

    Y_UNIT_TEST_TWIN(RangeLimitRead, EnableSourceRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(EnableSourceRead);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$from").Int32(1).Build()
            .AddParam("$to").Int32(5).Build()
            .AddParam("$limit").Uint64(3).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $from AS Int32;
            DECLARE $to AS Int32;
            DECLARE $limit AS Uint64;

            SELECT * FROM Join1
            WHERE Key >= $from AND Key < $to
            ORDER BY Key
            LIMIT $limit;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        // Cerr << stats.query_plan() << Endl;

        AssertTableStats(result, "/Root/Join1", {
            .ExpectedReads = 3,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2); // Precompute limit Min(1001,$limit),
        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 3);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("TotalTasks").GetIntegerSafe();
        }
        UNIT_ASSERT_VALUES_EQUAL(totalTasks, 3);
    }

    Y_UNIT_TEST_TWIN(RangeRead, EnableSourceRead) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(EnableSourceRead);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$from").Int32(2).Build()
            .AddParam("$to").Int32(7).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $from AS Int32;
            DECLARE $to AS Int32;

            SELECT * FROM Join1
            WHERE Key > $from AND Key <= $to
            ORDER BY Key;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        // Cerr << stats.query_plan() << Endl;

        AssertTableStats(result, "/Root/Join1", {
            .ExpectedReads = 5,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 2);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 2);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("TotalTasks").GetIntegerSafe();
        }
        UNIT_ASSERT_VALUES_EQUAL(totalTasks, EnableSourceRead ? 2 : 3);
    }

    Y_UNIT_TEST(Upsert) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = BuildUpdateParams(db);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            UPSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedUpdates = 2,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        // TODO: Get rid of additional precompute stage for adding optionality to row members
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST(Replace) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = BuildUpdateParams(db);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            REPLACE INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedUpdates = 2,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        // Single-phase REPLACE require additional runtime write callable
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST(UpdateOn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = BuildUpdateParams(db);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            UPDATE EightShard ON
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 1, // Non-existing keys don't count in reads
            .ExpectedUpdates = 1,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        // Two-phase UPDATE ON require more complex runtime callables
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST(Insert) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = BuildInsertParams(db);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            INSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 0, // Non-existing keys don't count in reads
            .ExpectedUpdates = 2,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        // Three-phase INSERT require more complex runtime callables
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST(DeleteOn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = BuildDeleteParams(db);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64>>;

            DELETE FROM EightShard ON
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedDeletes = 2,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        // TODO: Get rid of additional precompute stage for adding optionality to row members
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST(Update) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
        .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Uint64;

            UPDATE EightShard
            SET Data = Data + 1
            WHERE Key = $key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 1,
            .ExpectedUpdates = 1,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }
    }

    Y_UNIT_TEST(Delete) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
            .AddParam("$text").String("Value1").Build()
        .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Uint64;
            DECLARE $text AS String;

            DELETE FROM EightShard
            WHERE Key = $key AND Text = $text;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        AssertTableStats(result, "/Root/EightShard", {
            .ExpectedReads = 1,
            .ExpectedDeletes = 1,
        });

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }
    }

    Y_UNIT_TEST(IndexUpsert) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = BuildUpdateIndexParams(db);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            UPSERT INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
    }

    Y_UNIT_TEST(IndexReplace) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = BuildUpdateIndexParams(db);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            REPLACE INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
    }

    Y_UNIT_TEST(IndexUpdateOn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = BuildUpdateIndexParams(db);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            UPDATE SecondaryWithDataColumns ON
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
    }

    Y_UNIT_TEST(IndexDeleteOn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = BuildDeleteIndexParams(db);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String>>;

            DELETE FROM SecondaryWithDataColumns ON
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
    }

    Y_UNIT_TEST(IndexInsert) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = BuildInsertIndexParams(db);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            INSERT INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
    }

    Y_UNIT_TEST(IdxLookupJoin) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Int32(3).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Int32;

            SELECT *
            FROM Join1 AS t1
            INNER JOIN Join2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
            WHERE t1.Key = $key;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        }
    }

    Y_UNIT_TEST(IdxLookupJoinThreeWay) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Int32(3).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Int32;

            SELECT t1.Key, t3.Value
            FROM Join1 AS t1
            INNER JOIN Join2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
            INNER JOIN KeyValue2 AS t3 ON t2.Name = t3.Key
            WHERE t1.Key = $key;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
        }
    }

    Y_UNIT_TEST(ComputeLength) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT COUNT(*) FROM EightShard;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[24u]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
    }

    Y_UNIT_TEST(AggregateToScalar) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = TParamsBuilder()
            .AddParam("$group").Uint32(1).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $group AS Uint32;

            SELECT MIN(Name) AS MinName, SUM(Amount) AS TotalAmount
            FROM Test
            WHERE Group = $group;
        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[["Anna"];[3800u]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
    }

    Y_UNIT_TEST(MultiDeleteFromTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto params = TParamsBuilder()
            .AddParam("$key1_1").Uint32(101).Build()
            .AddParam("$key1_2").String("Two").Build()
            .AddParam("$key2_1").Uint32(105).Build()
            .AddParam("$key2_2").String("Two").Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key1_1 AS Uint32;
            DECLARE $key1_2 AS String;
            DECLARE $key2_1 AS Uint32;
            DECLARE $key2_2 AS String;

            $fetch1 = SELECT Key1, Key2 FROM Join2 WHERE Key1 = $key1_1 AND Key2 < $key1_2;
            $fetch2 = SELECT Key1, Key2 FROM Join2 WHERE Key1 = $key2_1 AND Key2 < $key2_2;

            DELETE FROM Join2 ON SELECT * FROM $fetch1;
            DELETE FROM Join2 ON SELECT * FROM $fetch2;

        )"), TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto checkResult = session.ExecuteDataQuery(Q1_(R"(
            SELECT COUNT(*) FROM Join2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(checkResult.IsSuccess(), checkResult.GetIssues().ToString());
        CompareYson(R"([[7u]])", FormatResultSetYson(checkResult.GetResultSet(0)));

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);

        AssertTableStats(result, "/Root/Join2", {
            .ExpectedReads = 3,
            .ExpectedDeletes = 3,
        });
    }

    Y_UNIT_TEST_TWIN(MultiRead, SourceRead) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(SourceRead);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto settings = NYdb::NTable::TExecDataQuerySettings().CollectQueryStats(ECollectQueryStatsMode::Full);
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/KeyValueLargePartition` WHERE Key > 101;
                SELECT * FROM `/Root/KeyValueLargePartition` Where Key < 201;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            size_t partitionsCount = 0;
            auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            for (auto& phase : stats.query_phases()) {
                for (auto& read : phase.table_access()) {
                    partitionsCount += read.partitions_count();
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(partitionsCount, SourceRead ? 2 : 1);
        }
    }
}

} // namespace NKikimr::NKqp
