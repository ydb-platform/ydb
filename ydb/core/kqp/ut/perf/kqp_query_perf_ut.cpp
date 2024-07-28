#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

TParams BuildUpdateParams() {
    return TParamsBuilder()
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

TParams BuildInsertParams() {
    return TParamsBuilder()
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

TParams BuildDeleteParams() {
    return TParamsBuilder()
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

TParams BuildUpdateIndexParams() {
    return TParamsBuilder()
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

TParams BuildDeleteIndexParams() {
    return TParamsBuilder()
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

TParams BuildInsertIndexParams() {
    return TParamsBuilder()
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

std::tuple<Ydb::TableStats::QueryStats, NYdb::TResultSets> ExecQuery(const TKikimrRunner& db, bool queryService,
    const TString& query, const NYdb::TParams& params)
{
    if (queryService) {
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .StatsMode(NYdb::NQuery::EStatsMode::Full);

        auto result = db.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
            params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        return std::make_tuple(stats, result.GetResultSets());
    } else {
        auto session = db.GetTableClient().CreateSession().GetValueSync().GetSession();

        auto settings = NYdb::NTable::TExecDataQuerySettings()
            .CollectQueryStats(ECollectQueryStatsMode::Full);

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
            params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        return std::make_tuple(stats, result.GetResultSets());
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryPerf) {
    Y_UNIT_TEST_TWIN(KvRead, QueryService) {
        TKikimrRunner kikimr;

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(102).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key AS Uint64;

            SELECT * FROM EightShard
            WHERE Key = $key;
        )"), params);

        // Cerr << stats.query_plan() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 1);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 2);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            if (stage.GetMapSafe().contains("Stats")) {
                totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(totalTasks, 1);
    }

    Y_UNIT_TEST_TWIN(RangeLimitRead, QueryService) {
        TKikimrRunner kikimr;

        auto params = TParamsBuilder()
            .AddParam("$from").Int32(1).Build()
            .AddParam("$to").Int32(5).Build()
            .AddParam("$limit").Uint64(3).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $from AS Int32;
            DECLARE $to AS Int32;
            DECLARE $limit AS Uint64;

            SELECT * FROM Join1
            WHERE Key >= $from AND Key < $to
            ORDER BY Key
            LIMIT $limit;
        )"), params);

        // Cerr << stats.query_plan() << Endl;

        AssertTableStats(stats, "/Root/Join1", {
            .ExpectedReads = 3,
        });

        // For data query, additional precompute for LIMIT: Min(1001,$limit)
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), QueryService ? 1 : 2);
        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        // TODO: Should be 2/3 stages?
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), QueryService ? 3 : 4);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            if (stage.GetMapSafe().contains("Stats")) {
                totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(totalTasks, QueryService ? 2 : 3);
    }

    Y_UNIT_TEST_TWIN(RangeRead, QueryService) {
        TKikimrRunner kikimr;

        auto params = TParamsBuilder()
            .AddParam("$from").Int32(2).Build()
            .AddParam("$to").Int32(7).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $from AS Int32;
            DECLARE $to AS Int32;

            SELECT * FROM Join1
            WHERE Key > $from AND Key <= $to
            ORDER BY Key;
        )"), params);


        // Cerr << stats.query_plan() << Endl;

        AssertTableStats(stats, "/Root/Join1", {
            .ExpectedReads = 5,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 2);

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(stats.query_plan(), &plan, true);

        auto stages = FindPlanStages(plan);
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 3);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            if (stage.GetMapSafe().contains("Stats")) {
                totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
            }
        }

        // Not implicit limit (1000 rows) for QueryService means no sequential reads.
        // TODO: Consider enabling sequential reads even without rows limit.
        UNIT_ASSERT_VALUES_EQUAL(totalTasks, QueryService ? 3 : 2);
    }

    Y_UNIT_TEST_QUAD(IndexLookupJoin, EnableStreamLookup, QueryService) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(false);
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(EnableStreamLookup);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};

        auto [stats, results] = ExecQuery(kikimr, QueryService, Q1_(R"(
            SELECT l.Key, r.Key1, r.Key2
            FROM `/Root/Join1` AS l
            INNER JOIN `/Root/Join2` AS r
               ON l.Fk21 = r.Key1
            ORDER BY l.Key, r.Key1, r.Key2;
        )"), TParamsBuilder().Build());

        CompareYson(R"([
            [[1];[101u];["One"]];
            [[1];[101u];["Three"]];
            [[1];[101u];["Two"]];
            [[2];[102u];["One"]];
            [[3];[103u];["One"]];
            [[4];[104u];["One"]];
            [[5];[105u];["One"]];
            [[5];[105u];["Two"]];
            [[6];[106u];["One"]];
            [[8];[108u];["One"]];
            [[9];[101u];["One"]];
            [[9];[101u];["Three"]];
            [[9];[101u];["Two"]]
        ])", FormatResultSetYson(results[0]));

        AssertTableStats(stats, "/Root/Join1", {
            .ExpectedReads = 9,
        });

        AssertTableStats(stats, "/Root/Join2", {
            .ExpectedReads = EnableStreamLookup ? 13 : 10,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), EnableStreamLookup ? 1 : 3);
    }

    Y_UNIT_TEST_TWIN(Upsert, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = BuildUpdateParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            UPSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), params);


        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedUpdates = 2,
        });


        // TODO: Get rid of additional precompute stage for adding optionality to row members
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST_TWIN(Replace, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = BuildUpdateParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            REPLACE INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedUpdates = 2,
        });

        // Single-phase REPLACE require additional runtime write callable
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST_TWIN(UpdateOn, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = BuildUpdateParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            UPDATE EightShard ON
            SELECT * FROM AS_TABLE($items);
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 1, // Non-existing keys don't count in reads
            .ExpectedUpdates = 1,
        });

        // Two-phase UPDATE ON require more complex runtime callables
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST_TWIN(Insert, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = BuildInsertParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64,'Text':String>>;

            INSERT INTO EightShard
            SELECT * FROM AS_TABLE($items);
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 0, // Non-existing keys don't count in reads
            .ExpectedUpdates = 2,
        });

        // Three-phase INSERT require more complex runtime callables
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST_TWIN(DeleteOn, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = BuildDeleteParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':Uint64>>;

            DELETE FROM EightShard ON
            SELECT * FROM AS_TABLE($items);
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 0,
            .ExpectedDeletes = 2,
        });

        // TODO: Get rid of additional precompute stage for adding optionality to row members
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 2);
        }
    }

    Y_UNIT_TEST_TWIN(Update, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key AS Uint64;

            UPDATE EightShard
            SET Data = Data + 1
            WHERE Key = $key;
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 1,
            .ExpectedUpdates = 1,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }
    }

    Y_UNIT_TEST_TWIN(Delete, QueryService) {
        auto kikimr = DefaultKikimrRunner();

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(201).Build()
            .AddParam("$text").String("Value1").Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key AS Uint64;
            DECLARE $text AS String;

            DELETE FROM EightShard
            WHERE Key = $key AND Text = $text;
        )"), params);

        AssertTableStats(stats, "/Root/EightShard", {
            .ExpectedReads = 1,
            .ExpectedDeletes = 1,
        });

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        for (const auto& phase : stats.query_phases()) {
            UNIT_ASSERT(phase.affected_shards() <= 1);
        }
    }

    Y_UNIT_TEST_TWIN(IndexUpsert, QueryService) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        auto params = BuildUpdateIndexParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            UPSERT INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), params);


        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
    }

    Y_UNIT_TEST_TWIN(IndexReplace, QueryService) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        auto params = BuildUpdateIndexParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            REPLACE INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), params);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
    }

    Y_UNIT_TEST_TWIN(IndexUpdateOn, QueryService) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        auto params = BuildUpdateIndexParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            UPDATE SecondaryWithDataColumns ON
            SELECT * FROM AS_TABLE($items);
        )"), params);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
    }

    Y_UNIT_TEST_TWIN(IndexDeleteOn, QueryService) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        auto params = BuildDeleteIndexParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String>>;

            DELETE FROM SecondaryWithDataColumns ON
            SELECT * FROM AS_TABLE($items);
        )"), params);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
    }

    Y_UNIT_TEST_TWIN(IndexInsert, QueryService) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        auto params = BuildInsertIndexParams();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $items AS List<Struct<'Key':String,'Index2':String,'Value':String>>;

            INSERT INTO SecondaryWithDataColumns
            SELECT * FROM AS_TABLE($items);
        )"), params);

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
    }

    Y_UNIT_TEST_TWIN(IdxLookupJoin, QueryService) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);

        auto params = TParamsBuilder()
            .AddParam("$key").Int32(3).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key AS Int32;

            SELECT *
            FROM Join1 AS t1
            INNER JOIN Join2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
            WHERE t1.Key = $key;
        )"), params);

        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        } else if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        }
    }

    Y_UNIT_TEST_TWIN(IdxLookupJoinThreeWay, QueryService) {
        TKikimrSettings settings;
        TKikimrRunner kikimr(settings);

        auto params = TParamsBuilder()
            .AddParam("$key").Int32(3).Build()
            .Build();

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key AS Int32;

            SELECT t1.Key, t3.Value
            FROM Join1 AS t1
            INNER JOIN Join2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
            INNER JOIN KeyValue2 AS t3 ON t2.Name = t3.Key
            WHERE t1.Key = $key;
        )"), params);

        if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamIdxLookupJoin()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        } else if (settings.AppConfig.GetTableServiceConfig().GetEnableKqpDataQueryStreamLookup()) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 5);
        }
    }

    Y_UNIT_TEST_TWIN(ComputeLength, QueryService) {
        TKikimrRunner kikimr;

        auto [stats, results] = ExecQuery(kikimr, QueryService, Q1_(R"(
            SELECT COUNT(*) FROM EightShard;
        )"), TParamsBuilder().Build());

        CompareYson(R"([[24u]])", FormatResultSetYson(results[0]));

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
    }

    Y_UNIT_TEST_TWIN(AggregateToScalar, QueryService) {
        TKikimrRunner kikimr;

        auto params = TParamsBuilder()
            .AddParam("$group").Uint32(1).Build()
            .Build();

        auto [stats, results] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $group AS Uint32;

            SELECT MIN(Name) AS MinName, SUM(Amount) AS TotalAmount
            FROM Test
            WHERE Group = $group;
        )"), params);

        CompareYson(R"([[["Anna"];[3800u]]])", FormatResultSetYson(results[0]));

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
    }

    Y_UNIT_TEST_TWIN(MultiDeleteFromTable, QueryService) {
        TKikimrRunner kikimr;

        auto params = TParamsBuilder()
            .AddParam("$key1_1").Uint32(101).Build()
            .AddParam("$key1_2").String("Two").Build()
            .AddParam("$key2_1").Uint32(105).Build()
            .AddParam("$key2_2").String("Two").Build()
            .Build();

        auto [stats, results] = ExecQuery(kikimr, QueryService, Q1_(R"(
            DECLARE $key1_1 AS Uint32;
            DECLARE $key1_2 AS String;
            DECLARE $key2_1 AS Uint32;
            DECLARE $key2_2 AS String;

            $fetch1 = SELECT Key1, Key2 FROM Join2 WHERE Key1 = $key1_1 AND Key2 < $key1_2;
            $fetch2 = SELECT Key1, Key2 FROM Join2 WHERE Key1 = $key2_1 AND Key2 < $key2_2;

            DELETE FROM Join2 ON SELECT * FROM $fetch1;
            DELETE FROM Join2 ON SELECT * FROM $fetch2;
        )"), params);

        auto [_, checkResults] = ExecQuery(kikimr, QueryService, Q1_(R"(
            SELECT COUNT(*) FROM Join2;
        )"), TParamsBuilder().Build());

        CompareYson(R"([[7u]])", FormatResultSetYson(checkResults[0]));

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        AssertTableStats(stats, "/Root/Join2", {
            .ExpectedReads = 3,
            .ExpectedDeletes = 3,
        });
    }

    Y_UNIT_TEST_TWIN(MultiRead, QueryService) {
        TKikimrRunner kikimr;

        auto [stats, _] = ExecQuery(kikimr, QueryService, Q1_(R"(
            SELECT * FROM `/Root/KeyValueLargePartition` WHERE Key > 101;
            SELECT * FROM `/Root/KeyValueLargePartition` Where Key < 201;
        )"), TParamsBuilder().Build());

        size_t partitionsCount = 0;
        for (auto& phase : stats.query_phases()) {
            for (auto& read : phase.table_access()) {
                partitionsCount += read.partitions_count();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(partitionsCount, 2);
    }
}

} // namespace NKikimr::NKqp
