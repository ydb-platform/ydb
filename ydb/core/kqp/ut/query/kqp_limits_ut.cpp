#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/core/tablet/resource_broker.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

using namespace NResourceBroker;

NKikimrResourceBroker::TResourceBrokerConfig MakeResourceBrokerTestConfig(ui32 multiplier = 1) {
    NKikimrResourceBroker::TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(4);

    queue = config.AddQueues();
    queue->SetName("queue_kqp_resource_manager");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(4);
    queue->MutableLimit()->AddResource(33554453 * multiplier);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    config.MutableResourceLimit()->AddResource(10);
    config.MutableResourceLimit()->AddResource(100'000);

    return config;
}

namespace {
    bool IsRetryable(const EStatus& status) {
        return status == EStatus::OVERLOADED;
    }
}

Y_UNIT_TEST_SUITE(KqpLimits) {
    Y_UNIT_TEST_TWIN(QSReplySizeEnsureMemoryLimits, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(true);

        TKikimrRunner kikimr(settings);
        CreateLargeTable(kikimr, 1'000, 100, 1'000, 1'000);

        auto db = kikimr.GetQueryClient();

        TControlWrapper mkqlInitialMemoryLimit;
        TControlWrapper mkqlMaxMemoryLimit;

        mkqlInitialMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlInitialMemoryLimit, "KqpSession.MkqlInitialMemoryLimit");
        mkqlMaxMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlMaxMemoryLimit, "KqpSession.MkqlMaxMemoryLimit");

        mkqlInitialMemoryLimit = 1_KB;
        mkqlMaxMemoryLimit = 1_KB;

        auto result = db.ExecuteQuery(R"(
            UPSERT INTO KeyValue2
            SELECT
                KeyText AS Key,
                DataText AS Value
            FROM `/Root/LargeTable`;
        )", NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(!to_lower(TString{result.GetIssues().ToString()}).Contains("query result"));
        if (useSink) {
            UNIT_ASSERT(result.GetIssues().ToString().contains("Stream write queries aren't allowed"));
        }
    }

    Y_UNIT_TEST(KqpMkqlMemoryLimitException) {
        TKikimrRunner kikimr;
        CreateLargeTable(kikimr, 10, 10, 1'000'000, 1);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        TControlWrapper mkqlInitialMemoryLimit;
        TControlWrapper mkqlMaxMemoryLimit;

        mkqlInitialMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlInitialMemoryLimit, "KqpSession.MkqlInitialMemoryLimit");
        mkqlMaxMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlMaxMemoryLimit, "KqpSession.MkqlMaxMemoryLimit");

        mkqlInitialMemoryLimit = 1_KB;
        mkqlMaxMemoryLimit = 1_KB;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(LargeParametersAndMkqlFailure) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000'000);

        TKikimrRunner kikimr(app);
        CreateLargeTable(kikimr, 0, 0, 0);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TControlWrapper mkqlInitialMemoryLimit;
        TControlWrapper mkqlMaxMemoryLimit;

        mkqlInitialMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlInitialMemoryLimit, "KqpSession.MkqlInitialMemoryLimit");
        mkqlMaxMemoryLimit = kikimr.GetTestServer().GetRuntime()->GetAppData().Icb->RegisterSharedControl(
            mkqlMaxMemoryLimit, "KqpSession.MkqlMaxMemoryLimit");


        mkqlInitialMemoryLimit = 1_KB;
        mkqlMaxMemoryLimit = 1_KB;

        auto paramsBuilder = db.GetParamsBuilder();
        auto& rowsParam = paramsBuilder.AddParam("$rows");

        rowsParam.BeginList();
        for (ui32 i = 0; i < 100; ++i) {
            rowsParam.AddListItem()
                .BeginStruct()
                .AddMember("Key")
                    .OptionalUint64(i)
                .AddMember("KeyText")
                    .OptionalString(TString(5000, '0' + i % 10))
                .AddMember("Data")
                    .OptionalInt64(i)
                .AddMember("DataText")
                    .OptionalString(TString(16, '0' + (i + 1) % 10))
                .EndStruct();
        }
        rowsParam.EndList();
        rowsParam.Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $rows AS List<Struct<Key: Uint64?, KeyText: String?, Data: Int64?, DataText: String?>>;

            UPSERT INTO `/Root/LargeTable`
            SELECT * FROM AS_TABLE($rows);
        )"), TTxControl::BeginTx().CommitTx(), paramsBuilder.Build()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_TWIN(ComputeActorMemoryAllocationFailure, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(10);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetQueryMemoryLimit(2000);

        app.MutableResourceBrokerConfig()->CopyFrom(MakeResourceBrokerTestConfig());

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        CreateLargeTable(kikimr, 0, 0, 0);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_C(result.GetIssues().ToString().contains("Mkql memory limit exceeded"), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ComputeActorMemoryAllocationFailureQueryService, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(10);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetQueryMemoryLimit(2000);

        app.MutableResourceBrokerConfig()->CopyFrom(MakeResourceBrokerTestConfig(4));

        app.MutableFeatureFlags()->SetEnableResourcePools(true);

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        CreateLargeTable(kikimr, 0, 0, 0);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetQueryClient();
        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.StatsMode(NYdb::NQuery::EStatsMode::Full);

        auto result = db.ExecuteQuery(Q1_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), NQuery::TTxControl::BeginTx().CommitTx(), querySettings).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        auto stats = result.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_C(result.GetIssues().ToString().contains("Mkql memory limit exceeded"), result.GetIssues().ToString());
        UNIT_ASSERT(stats.has_value());

        Cerr << stats->ToString(true) << Endl;
    }

    Y_UNIT_TEST_TWIN(DatashardProgramSize, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000'000);

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        CreateLargeTable(kikimr, 0, 0, 0);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto paramsBuilder = db.GetParamsBuilder();
        auto& rowsParam = paramsBuilder.AddParam("$rows");

        rowsParam.BeginList();
        for (ui32 i = 0; i < 10000; ++i) {
            rowsParam.AddListItem()
                .BeginStruct()
                .AddMember("Key")
                    .OptionalUint64(i)
                .AddMember("KeyText")
                    .OptionalString(TString(5000, '0' + i % 10))
                .AddMember("Data")
                    .OptionalInt64(i)
                .AddMember("DataText")
                    .OptionalString(TString(16, '0' + (i + 1) % 10))
                .EndStruct();
        }
        rowsParam.EndList();
        rowsParam.Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $rows AS List<Struct<Key: Uint64?, KeyText: String?, Data: Int64?, DataText: String?>>;

            UPSERT INTO `/Root/LargeTable`
            SELECT * FROM AS_TABLE($rows);
        )"), TTxControl::BeginTx().CommitTx(), paramsBuilder.Build()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        if (useSink) {
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
            UNIT_ASSERT(HasIssue(result.GetIssues(), NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED));
        }
    }

    Y_UNIT_TEST(DatashardReplySize) {
        auto app = NKikimrConfig::TAppConfig();

        auto& queryLimits = *app.MutableTableServiceConfig()->MutableQueryLimits();
        queryLimits.MutablePhaseLimits()->SetComputeNodeMemoryLimitBytes(1'000'000'000);
        TKikimrRunner kikimr(app);
        CreateLargeTable(kikimr, 100, 10, 1'000'000, 1, 2);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_RESULT_UNAVAILABLE));
    }

    Y_UNIT_TEST(QueryReplySize) {
        TKikimrRunner kikimr;
        CreateLargeTable(kikimr, 10, 10, 1'000'000, 1);

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_RESULT_UNAVAILABLE));
    }

    Y_UNIT_TEST(OutOfSpaceBulkUpsertFail) {
        TKikimrRunner kikimr(NFake::TStorage{
            .UseDisk = false,
            .SectorSize = 4096,
            .ChunkSize = 32_MB,
            .DiskSize = 8_GB
        });

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "LargeTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "DataText", Type: "String" }
            KeyColumnNames: ["Key"],
        )");

        auto client = kikimr.GetTableClient();

        const ui32 batchCount = 400;
        const ui32 dataTextSize = 1_MB;
        const ui32 rowsPerBatch = 30;

        auto session = client.CreateSession().GetValueSync().GetSession();

        bool failedToInsert = false;
        ui32 batchIdx = 0;
        ui32 cnt = 0;

        while (batchIdx < batchCount) {
            auto rowsBuilder = TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 0; i < rowsPerBatch; ++i) {
                TString dataText(dataTextSize, 'a' + RandomNumber<ui32>() % ('z' - 'a' + 1));
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(cnt++)
                    .AddMember("DataText")
                        .OptionalString(dataText)
                    .EndStruct();
            }
            rowsBuilder.EndList();

            auto result = client.BulkUpsert("/Root/LargeTable", rowsBuilder.Build()).ExtractValueSync();
            if (IsRetryable(result.GetStatus())) {
                continue;
            }
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAVAILABLE, result.GetIssues().ToString());
                failedToInsert = true;
                break;
            }
            ++batchIdx;
        }
        if (!failedToInsert) {
            UNIT_FAIL("Successfully inserted " << rowsPerBatch << " x " << batchCount << " lines, each of size " << dataTextSize << "bytes");
        }
    }

    Y_UNIT_TEST_TWIN(OutOfSpaceYQLUpsertFail, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000'000);

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(false)
            .SetStorage(NFake::TStorage{
                .UseDisk = false,
                .SectorSize = 4096,
                .ChunkSize = 32_MB,
                .DiskSize = 8_GB
            });

        TKikimrRunner kikimr(settings);

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "LargeTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "DataText", Type: "String" }
            KeyColumnNames: ["Key"],
        )");

        auto client = kikimr.GetTableClient();

        const ui32 batchCount = 400;
        const ui32 dataTextSize = 1_MB;
        const ui32 rowsPerBatch = 30;

        auto session = client.CreateSession().GetValueSync().GetSession();

        ui32 batchIdx = 0;
        ui32 cnt = 0;

        while (batchIdx < batchCount) {
            auto paramsBuilder = client.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            for (ui32 i = 0; i < rowsPerBatch; ++i) {
                TString dataText(dataTextSize, 'a' + RandomNumber<ui32>() % ('z' - 'a' + 1));
                rowsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(cnt++)
                    .AddMember("DataText")
                        .OptionalString(dataText)
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(Q1_(R"(
                DECLARE $rows AS List<Struct<Key: Uint64?, DataText: String?>>;

                UPSERT INTO `/Root/LargeTable`
                SELECT * FROM AS_TABLE($rows);
            )"), TTxControl::BeginTx().CommitTx(), paramsBuilder.Build()).ExtractValueSync();
 
            switch (result.GetStatus()) {
            case EStatus::SUCCESS:
                continue;
            case EStatus::OVERLOADED:
                if (result.GetIssues().ToString().contains("out of disk space")) {
                    UNIT_ASSERT(useSink);
                    Cerr << "Got out of space. Successfully inserted " << rowsPerBatch << " x " << batchIdx << " lines, each of size " << dataTextSize << "bytes";
                    return;
                } else {
                    continue;
                }
            case EStatus::UNAVAILABLE:
                if (result.GetIssues().ToString().contains("out of disk space")) {
                    UNIT_ASSERT(!useSink);
                    //TODO Should be also EStatus::OVERLOADED
                    Cerr << "Got out of space. Successfully inserted " << rowsPerBatch << " x " << batchIdx << " lines, each of size " << dataTextSize << "bytes";
                    return;
                } else if (result.GetIssues().ToString().contains("WRONG_SHARD_STATE")
                        || result.GetIssues().ToString().contains("wrong shard state")
                        || result.GetIssues().ToString().contains("can't deliver message to tablet")) {
                    // shards are allowed to split
                    continue;
                }
                UNIT_ASSERT_C(false, "Unexpected UNAVAILABLE status" << result.GetIssues().ToString());
            default:
                UNIT_ASSERT_C(false, "Unexpected status" << result.GetStatus() << result.GetIssues().ToString());
            }

            ++batchIdx;
        }
        UNIT_FAIL("Out of space is expected");
    }

    Y_UNIT_TEST_TWIN(TooBigQuery, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        app.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000'000);
        app.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(5).MilliSeconds());

        TKikimrRunner kikimr(app);
        CreateLargeTable(kikimr, 0, 0, 0);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_ERROR);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TStringBuilder query;
        query << R"(
            --!syntax_v1

            UPSERT INTO `/Root/LargeTable`
            SELECT * FROM AS_TABLE(AsList(
        )";

        ui32 count = 5000;
        for (ui32 i = 0; i < count; ++i) {
            query << "AsStruct("
                 << i << "UL AS Key, "
                 << "'" << CreateGuidAsString() << TString(5000, '0' + i % 10) << "' AS KeyText, "
                 << count + i << "L AS Data, "
                 << "'" << CreateGuidAsString() << "' AS DataText"
                 << ")";
            if (i + 1 != count) {
                query << ", ";
            }
        }
        query << "))";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        //UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        if (useSink) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED));
        }
    }

    Y_UNIT_TEST(BigParameter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `ManyColumns` (
                    Key Int32,
                    Str0 String, Str1 String, Str2 String, Str3 String, Str4 String,
                    Str5 String, Str6 String, Str7 String, Str8 String, Str9 String,
                    PRIMARY KEY (Key)
                )
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto query = Q1_(R"(
            DECLARE $str0 AS String;
            DECLARE $str1 AS String;
            DECLARE $str2 AS String;
            DECLARE $str3 AS String;
            DECLARE $str4 AS String;
            DECLARE $str5 AS String;
            DECLARE $str6 AS String;
            DECLARE $str7 AS String;
            DECLARE $str8 AS String;
            DECLARE $str9 AS String;

            UPSERT INTO `/Root/ManyColumns` (Key, Str0, Str1, Str2, Str3, Str4, Str5, Str6, Str7, Str8, Str9) VALUES
                (1, $str0, $str1, $str2, $str3, $str4, $str5, $str6, $str7, $str8, $str9)
        )");

        auto params = TParamsBuilder()
            .AddParam("$str0").String(TString(5_MB, 'd')).Build()
            .AddParam("$str1").String(TString(5_MB, 'o')).Build()
            .AddParam("$str2").String(TString(5_MB, 'n')).Build()
            .AddParam("$str3").String(TString(5_MB, 't')).Build()
            .AddParam("$str4").String(TString(5_MB, 'g')).Build()
            .AddParam("$str5").String(TString(5_MB, 'i')).Build()
            .AddParam("$str6").String(TString(5_MB, 'v')).Build()
            .AddParam("$str7").String(TString(5_MB, 'e')).Build()
            .AddParam("$str8").String(TString(5_MB, 'u')).Build()
            .AddParam("$str9").String(TString(1_MB, 'p')).Build()
            .Build();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(TooBigKey, useSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$group").Uint32(1000).Build()
            .AddParam("$name").String(TString(2_MB, 'n')).Build()
            .AddParam("$amount").Uint64(20).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $group AS Uint32;
            DECLARE $name AS Bytes;
            DECLARE $amount AS Uint64;

            UPSERT INTO Test (Group, Name, Amount) VALUES ($group, $name, $amount);
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), useSink ? NYql::TIssuesIds::KIKIMR_BAD_REQUEST : NYql::TIssuesIds::DEFAULT_ERROR,
            [&](const auto& issue) {
                if (useSink) {
                    return issue.GetMessage().contains("Row key size of")
                        && issue.GetMessage().contains("bytes is larger than the allowed threshold");
                } else {
                    return issue.GetMessage().contains("exceeds limit");
                }
        }), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(TooBigColumn, useSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1000).Build()
            .AddParam("$value").String(TString(20_MB, 'n')).Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $key AS Uint64;
            DECLARE $value AS Bytes;

            UPSERT INTO KeyValue (Key, Value) VALUES ($key, $value);
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        if (!useSink) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
        UNIT_ASSERT(HasIssue(result.GetIssues(), useSink ? NYql::TIssuesIds::KIKIMR_BAD_REQUEST : NYql::TIssuesIds::DEFAULT_ERROR,
                [] (const auto& issue) {
                    return issue.GetMessage().contains("larger than the allowed threshold");
            }));
    }

    Y_UNIT_TEST(AffectedShardsLimit) {
        NKikimrConfig::TAppConfig appConfig;
        auto& queryLimits = *appConfig.MutableTableServiceConfig()->MutableQueryLimits();
        queryLimits.MutablePhaseLimits()->SetAffectedShardsLimit(20);

        TKikimrRunner kikimr(appConfig);

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "ManyShard20"
            Columns { Name: "Key", Type: "Uint32" }
            Columns { Name: "Value1", Type: "String" }
            Columns { Name: "Value2", Type: "Int32" }
            KeyColumnNames: ["Key"]
            UniformPartitionsCount: 20
        )");

        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "ManyShard21"
            Columns { Name: "Key", Type: "Uint32" }
            Columns { Name: "Value1", Type: "String" }
            Columns { Name: "Value2", Type: "Int32" }
            KeyColumnNames: ["Key"]
            UniformPartitionsCount: 21
        )");

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT COUNT(*) FROM `/Root/ManyShard20`
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT COUNT(*) FROM `/Root/ManyShard21`
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED));
    }

    Y_UNIT_TEST(ReadsetCountLimit) {
        NKikimrConfig::TAppConfig appConfig;
        auto& queryLimits = *appConfig.MutableTableServiceConfig()->MutableQueryLimits();
        queryLimits.MutablePhaseLimits()->SetReadsetCountLimit(50);

        TKikimrRunner kikimr(appConfig);
        CreateLargeTable(kikimr, 10, 10, 100);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/LargeTable`
            SET Data = CAST(Key AS Int64) + 10
            WHERE Key < 7000000;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/LargeTable`
            SET Data = CAST(Key AS Int64) + 10;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        // TODO: KIKIMR-11134 (Fix readset limit)
        // UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        // UNIT_ASSERT(HasIssue(result.GetIssues(), NKikimrIssues::TIssuesIds::ENGINE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ComputeNodeMemoryLimit) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(1'000'000);
        auto& queryLimits = *appConfig.MutableTableServiceConfig()->MutableQueryLimits();
        queryLimits.MutablePhaseLimits()->SetComputeNodeMemoryLimitBytes(100'000'000);

        TKikimrRunner kikimr(appConfig);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT ToDict(
                ListMap(
                    ListFromRange(0ul, 5000000ul),
                    ($x) -> { RETURN AsTuple($x, $x + 1); }
                )
            );
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
            [] (const auto& issue) {
                return issue.GetMessage().contains("Memory limit exceeded");
            }));
    }

    Y_UNIT_TEST(QueryExecTimeoutCancel) {
        TKikimrRunner kikimr;
        CreateLargeTable(kikimr, 500000, 10, 100, 5000, 1);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        for (auto status : {EStatus::TIMEOUT, EStatus::CANCELLED}) {
            auto prepareResult = session.PrepareDataQuery(Q_(R"(
                SELECT COUNT(*) FROM `/Root/LargeTable` WHERE SUBSTRING(DataText, 50, 5) = "11111";
            )")).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
            auto dataQuery = prepareResult.GetQuery();

            auto settings = TExecDataQuerySettings();
            if (status == EStatus::TIMEOUT) {
                settings.OperationTimeout(TDuration::MilliSeconds(100));
            } else {
                settings.CancelAfter(TDuration::MilliSeconds(100));
            }

            auto result = dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync();

            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
        }
    }

    Y_UNIT_TEST_TWIN(CancelAfterRwTx, useSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        TKikimrRunner kikimr(appConfig);
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            int maxTimeoutMs = 500;

            auto createKey = [](int id) -> ui64 {
                return (1u << 29) + id;
            };

            auto createExpectedRow = [](ui64 key) -> TString {
                return Sprintf(R"([[100500];[%luu];["newrecords"]])", key);
            };

            TString expected;

            for (int i = 1; i <= maxTimeoutMs; i++) {
                auto params = db.GetParamsBuilder()
                    .AddParam("$id")
                        .Uint64(createKey(i))
                        .Build()
                    .Build();
                auto result = session.ExecuteDataQuery(R"(
                    DECLARE $id AS Uint64;
                    SELECT * FROM `/Root/EightShard` WHERE Text = "newrecords" ORDER BY Key;
                    UPSERT INTO `/Root/EightShard` (Key, Data, Text) VALUES ($id, 100500, "newrecords");
                )",
                TTxControl::BeginTx(
                    TTxSettings::SerializableRW()).CommitTx(),
                    params,
                    TExecDataQuerySettings().CancelAfter(TDuration::MilliSeconds(i))
                ).GetValueSync();

                if (result.IsSuccess()) {
                    auto yson = FormatResultSetYson(result.GetResultSet(0));
                    CompareYson(TString("[") + expected + "]", TString{yson});
                    expected += createExpectedRow(createKey(i));
                    if (i != maxTimeoutMs)
                        expected += ";";
                } else {
                    switch (result.GetStatus()) {
                        case EStatus::CANCELLED:
                            break;
                        default: {
                            auto msg = TStringBuilder()
                                << "unexpected status: " << result.GetStatus()
                                << " issues: " << result.GetIssues().ToString();
                            UNIT_ASSERT_C(false, msg.data());
                        }
                    }
                }
            }
        }

        WaitForZeroSessions(counters);
        WaitForZeroReadIterators(kikimr.GetTestServer(), "/Root/EightShard");
    }

    void DoCancelAfterRo(bool follower, bool dependedRead) {
        NKikimrConfig::TAppConfig appConfig;

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            int maxTimeoutMs = 500;
            bool wasCanceled = false;

            if (follower) {
                AssertSuccessResult(session.ExecuteSchemeQuery(R"(
                    --!syntax_v1
                    CREATE TABLE `/Root/OneShardWithFolower` (
                        Key Uint64,
                        Text String,
                        Data Int32,
                        PRIMARY KEY (Key)
                    )
                    WITH (
                        READ_REPLICAS_SETTINGS = "ANY_AZ:1"
                    );
                )").GetValueSync());

                AssertSuccessResult(session.ExecuteDataQuery(R"(
                    --!syntax_v1
                    REPLACE INTO `/Root/OneShardWithFolower` (Key, Text, Data) VALUES
                        (101u, "Value1",  1),
                        (201u, "Value1",  2),
                        (301u, "Value1",  3),
                        (401u, "Value1",  1),
                        (501u, "Value1",  2),
                        (601u, "Value1",  3),
                        (701u, "Value1",  1),
                        (801u, "Value1",  2),
                        (102u, "Value2",  3),
                        (202u, "Value2",  1),
                        (302u, "Value2",  2),
                        (402u, "Value2",  3),
                        (502u, "Value2",  1),
                        (602u, "Value2",  2),
                        (702u, "Value2",  3),
                        (802u, "Value2",  1),
                        (103u, "Value3",  2),
                        (203u, "Value3",  3),
                        (303u, "Value3",  1),
                        (403u, "Value3",  2),
                        (503u, "Value3",  3),
                        (603u, "Value3",  1),
                        (703u, "Value3",  2),
                        (803u, "Value3",  3);
                )", TTxControl::BeginTx().CommitTx()).GetValueSync());
            }

            const TString q = follower ?
                (dependedRead ?
                    TString(R"(
                        DECLARE $id AS Uint64;
                        --JOIN with same table to make depended read
                        SELECT t1.Data as Data, t1.Key as Key, t1.Text as Text FROM `/Root/OneShardWithFolower` as t1
                            INNER JOIN OneShardWithFolower as t2 ON t1.Key = t2.Key WHERE t1.Text = "Value1" ORDER BY t1.Key;
                    )"):
                    TString(R"(
                        DECLARE $id AS Uint64;
                        SELECT * FROM `/Root/OneShardWithFolower` WHERE Text = "Value1" ORDER BY Key
                    )")
                ):
                TString(R"(
                    DECLARE $id AS Uint64;
                    SELECT * FROM `/Root/EightShard` WHERE Text = "Value1" ORDER BY Key
                )");

            const auto txCtrl = follower ? TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx() :
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();

            for (int i = 1; i <= maxTimeoutMs; i++) {
                auto result = session.ExecuteDataQuery(q, txCtrl,
                    TExecDataQuerySettings().CancelAfter(TDuration::MilliSeconds(i))
                ).GetValueSync();

                if (result.IsSuccess()) {
                    CompareYson(EXPECTED_EIGHTSHARD_VALUE1, FormatResultSetYson(result.GetResultSet(0)));
                } else {
                    switch (result.GetStatus()) {
                        case EStatus::CANCELLED:
                            wasCanceled = true;
                            break;
                        default: {
                            auto msg = TStringBuilder()
                                << "unexpected status: " << result.GetStatus()
                                << " issues: " << result.GetIssues().ToString();
                            UNIT_ASSERT_C(false, msg.data());
                        }
                    }
                }
            }
            UNIT_ASSERT(wasCanceled);
        }
        WaitForZeroSessions(counters);

        WaitForZeroReadIterators(kikimr.GetTestServer(), "/Root/EightShard");
        if (follower) {
            WaitForZeroReadIterators(kikimr.GetTestServer(), "/Root/OneShardWithFolower");
        }
    }

    Y_UNIT_TEST(CancelAfterRoTx) {
        // false, false has no sense since we use TEvRead to read without followers
        DoCancelAfterRo(false, false);
    }

    Y_UNIT_TEST(CancelAfterRoTxWithFollowerStreamLookup) {
        DoCancelAfterRo(true, false);
    }

    Y_UNIT_TEST(CancelAfterRoTxWithFollowerStreamLookupDepededRead) {
        DoCancelAfterRo(true, true);
    }

    Y_UNIT_TEST(QueryExecTimeout) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlLightProgramMemoryLimit(10'000'000'000);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(300000);

        TKikimrRunner kikimr(appConfig);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto prepareSettings =
            TPrepareDataQuerySettings()
                .OperationTimeout(TDuration::Seconds(300));
        auto prepareResult = session.PrepareDataQuery(Q_(R"(
            SELECT DictLength(ToDict(
                ListMap(
                    ListFromRange(0ul, 10000000ul),
                    ($x) -> { RETURN AsTuple($x, $x + 1); }
                )
            ));
        )"), prepareSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
        auto dataQuery = prepareResult.GetQuery();

        auto settings = TExecDataQuerySettings()
            .OperationTimeout(TDuration::MilliSeconds(500));
        auto result = dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::TIMEOUT);
    }

    /* Scenario:
        - prepare and run query
        - observe first EvState event from CA to Executer and replace it with EvAbortExecution
        - count all EvState events from all CAs
        - wait for final event EvTxResponse from Executer
        - expect it to happen strictly after all EvState events
     */
    Y_UNIT_TEST(WaitCAsStateOnAbort) {
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false));
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        auto prepareResult = kikimr.RunCall([&] { return session.PrepareDataQuery(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TwoShard`;
            )")).GetValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
        auto dataQuery = prepareResult.GetQuery();

        bool firstEvState = false;
        ui32 totalEvState = 0;
        TActorId executerId;
        ui32 actorCount = 3; // TODO: get number of actors properly.

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                ++totalEvState;
                if (!firstEvState) {
                    executerId = ev->Recipient;
                    ev = new IEventHandle(ev->Recipient, ev->Sender,
                            new NKikimr::NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::UNSPECIFIED, NYql::TIssues()));
                    firstEvState = true;
                }
            } else if (ev->GetTypeRewrite() == NKikimr::NKqp::TEvKqpExecuter::TEvTxResponse::EventType && ev->Sender == executerId) {
                UNIT_ASSERT_C(totalEvState == actorCount*2, "Executer sent response before waiting for CAs");
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto settings = TExecDataQuerySettings().OperationTimeout(TDuration::MilliSeconds(500));
        kikimr.RunInThreadPool([&] { return dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync(); });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle& ev) {
            return ev.GetTypeRewrite() == NKikimr::NKqp::TEvKqpExecuter::TEvTxResponse::EventType
                && ev.Sender == executerId && totalEvState == actorCount*2;
        });

        UNIT_ASSERT(runtime.DispatchEvents(opts));
    }

    /* Scenario:
        - prepare and run query
        - observe first EvState event from CA to Executer and replace it with EvAbortExecution
        - count all EvState events from all CAs
        - drop final EvState event from last CA
        - wait for final event EvTxResponse from Executer after timeout poison
        - expect it to happen strictly after all EvState events
     */
    Y_UNIT_TEST(WaitCAsTimeout) {
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false));
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        auto prepareResult = kikimr.RunCall([&] { return session.PrepareDataQuery(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TwoShard`;
            )")).GetValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), EStatus::SUCCESS, prepareResult.GetIssues().ToString());
        auto dataQuery = prepareResult.GetQuery();

        bool firstEvState = false;
        bool timeoutPoison = false;
        ui32 totalEvState = 0;
        TActorId executerId;
        ui32 actorCount = 3; // TODO: get number of actors properly.

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                ++totalEvState;
                if (!firstEvState) {
                    executerId = ev->Recipient;
                    ev = new IEventHandle(ev->Recipient, ev->Sender,
                            new NKikimr::NKqp::TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::UNSPECIFIED, NYql::TIssues()));
                    firstEvState = true;
                } else {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType && totalEvState == actorCount*2 &&
                ev->Sender == executerId && ev->Recipient == executerId)
            {
                timeoutPoison = true;
            } else if (ev->GetTypeRewrite() == NKikimr::NKqp::TEvKqpExecuter::TEvTxResponse::EventType && ev->Sender == executerId) {
                UNIT_ASSERT_C(timeoutPoison, "Executer sent response before waiting for CAs");
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto settings = TExecDataQuerySettings().OperationTimeout(TDuration::Seconds(20));
        kikimr.RunInThreadPool([&] { return dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync(); });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle& ev) {
            return ev.GetTypeRewrite() == NKikimr::NKqp::TEvKqpExecuter::TEvTxResponse::EventType
                && ev.Sender == executerId && totalEvState == actorCount*2 && timeoutPoison;
        });

        UNIT_ASSERT(runtime.DispatchEvents(opts));
    }

    Y_UNIT_TEST(ReplySizeExceeded) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        const auto status = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableTest` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
        )").GetValueSync();
        UNIT_ASSERT(status.IsSuccess());

        auto replaceQuery = Q1_(R"(
            DECLARE $rows AS
                List<Struct<
                    Key: Uint64,
                    Value: String
                >>;

            REPLACE INTO `/Root/TableTest`
            SELECT * FROM AS_TABLE($rows);
        )");

        const ui32 BATCH_NUM = 4;
        const ui32 BATCH_ROWS = 100;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb

        for (ui64 i = 0; i < BATCH_NUM ; ++i) {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");
            rowsParam.BeginList();

            for (ui64 j = 0; j < BATCH_ROWS; ++j) {
                auto key = i * BATCH_ROWS + j;
                auto val = TString(BLOB_SIZE, '0' + key % 10);
                rowsParam.AddListItem()
                    .BeginStruct()
                        .AddMember("Key")
                            .Uint64(key)
                        .AddMember("Value")
                            .String(val)
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TableTest`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(counters.GetTxReplySizeExceededError()->Val(), 0);
        }

        auto paramsBuilder = session.GetParamsBuilder();
        auto& rowsParam = paramsBuilder.AddParam("$rows");
        rowsParam.BeginList();

        for (ui64 j = 0; j < BATCH_ROWS; ++j) {
            auto key = BATCH_NUM * BATCH_ROWS + j;
            auto val = TString(BLOB_SIZE, '0' + key % 10);
            rowsParam.AddListItem()
                .BeginStruct()
                    .AddMember("Key")
                        .Uint64(key)
                    .AddMember("Value")
                        .String(val)
                .EndStruct();
        }
        rowsParam.EndList();
        rowsParam.Build();

        auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
            paramsBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TableTest`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT_VALUES_EQUAL(counters.GetTxReplySizeExceededError()->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(counters.GetDataShardTxReplySizeExceededError()->Val(), 0);
        }
    }

    Y_UNIT_TEST(DataShardReplySizeExceeded) {
        auto app = NKikimrConfig::TAppConfig();
        TKikimrRunner kikimr(app);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        const auto status = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableTest` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
        )").GetValueSync();
        UNIT_ASSERT(status.IsSuccess());

        auto replaceQuery = Q1_(R"(
            DECLARE $rows AS
                List<Struct<
                    Key: Uint64,
                    Value: String
                >>;

            REPLACE INTO `/Root/TableTest`
            SELECT * FROM AS_TABLE($rows);
        )");

        const ui32 BATCH_NUM = 4;
        const ui32 BATCH_ROWS = 100;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb

        for (ui64 i = 0; i < BATCH_NUM ; ++i) {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");
            rowsParam.BeginList();

            for (ui64 j = 0; j < BATCH_ROWS; ++j) {
                auto key = i * BATCH_ROWS + j;
                auto val = TString(BLOB_SIZE, '0' + key % 10);
                rowsParam.AddListItem()
                    .BeginStruct()
                        .AddMember("Key")
                            .Uint64(key)
                        .AddMember("Value")
                            .String(val)
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TableTest`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(counters.GetDataShardTxReplySizeExceededError()->Val(), 0);
        }

        auto paramsBuilder = session.GetParamsBuilder();
        auto& rowsParam = paramsBuilder.AddParam("$rows");
        rowsParam.BeginList();

        for (ui64 j = 0; j < BATCH_ROWS; ++j) {
            auto key = BATCH_NUM * BATCH_ROWS + j;
            auto val = TString(BLOB_SIZE, '0' + key % 10);
            rowsParam.AddListItem()
                .BeginStruct()
                    .AddMember("Key")
                        .Uint64(key)
                    .AddMember("Value")
                        .String(val)
                .EndStruct();
        }
        rowsParam.EndList();
        rowsParam.Build();

        auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
            paramsBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TableTest`;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().contains("result size limit"), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(counters.GetTxReplySizeExceededError()->Val(), 1);
        }
    }

    Y_UNIT_TEST(ManyPartitions) {
        SetRandomSeed(42);

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        CreateManyShardsTable(kikimr, 1000, 100, 1000);

        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.StatsMode(NYdb::NQuery::EStatsMode::Full);

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM `/Root/ManyShardsTable`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        querySettings).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetPlan());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
        Cout << plan;

        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Tables"][0].GetStringSafe(), "ManyShardsTable");
        UNIT_ASSERT(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe() < 100);
        UNIT_ASSERT(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe() > 1);
    }

    Y_UNIT_TEST(ManyPartitionsSorting) {
        SetRandomSeed(42);

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        CreateManyShardsTable(kikimr, 1100, 100, 1000);

        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.StatsMode(NYdb::NQuery::EStatsMode::Full);

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT Key, Data FROM `/Root/ManyShardsTable` ORDER BY Key;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        querySettings).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        Cerr << result.GetStats()->ToString(true) << Endl;
        UNIT_ASSERT(result.GetStats()->GetPlan());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);

        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Node Type"].GetStringSafe(), "Query");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Node Type"].GetStringSafe(), "ResultSet");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "Stage");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "Merge");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["SortColumns"].GetArraySafe()[0], "Key (Asc)");

        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Tables"][0].GetStringSafe(), "ManyShardsTable");
        UNIT_ASSERT(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe() < 100);
        UNIT_ASSERT(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe() > 1);

        const auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1100);
        ui32 last = 0;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            const ui32 current = *parser.ColumnParser(0).GetOptionalUint32();
            UNIT_ASSERT(current >= last);
            last = current;
        }
    }

    Y_UNIT_TEST(ManyPartitionsSortingLimit) {
        SetRandomSeed(42);

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        CreateManyShardsTable(kikimr, 5000, 100, 1000);

        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.StatsMode(NYdb::NQuery::EStatsMode::Full);

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT Key, Data FROM `/Root/ManyShardsTable` ORDER BY Key LIMIT 1100;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        querySettings).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetPlan());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
        Cout << plan;

        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Node Type"].GetStringSafe(), "Query");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Node Type"].GetStringSafe(), "ResultSet");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "Limit");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "Merge");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["SortColumns"].GetArraySafe()[0], "Key (Asc)");

        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Node Type"].GetStringSafe(), "TableFullScan");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Tables"][0].GetStringSafe(), "ManyShardsTable");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);

        const auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1100);
        ui32 last = 0;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            const ui32 current = *parser.ColumnParser(0).GetOptionalUint32();
            UNIT_ASSERT(current >= last);
            const ui32 limit = (std::numeric_limits<ui32>::max() / 5000) * 1100 + 1;
            UNIT_ASSERT(current < limit);
            last = current;
        }
    }

    Y_UNIT_TEST_TWIN(QSReplySize, useSink) {
        auto app = NKikimrConfig::TAppConfig();
        app.MutableTableServiceConfig()->SetEnableOltpSink(useSink);

        auto settings = TKikimrSettings()
            .SetAppConfig(app)
            .SetWithSampleTables(true);

        TKikimrRunner kikimr(settings);
        CreateLargeTable(kikimr, 10'000, 100, 1'000, 1'000);

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            UPSERT INTO KeyValue2
            SELECT
                KeyText AS Key,
                DataText AS Value
            FROM `/Root/LargeTable`;
        )", NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(!to_lower(TString{result.GetIssues().ToString()}).Contains("query result"));
        if (useSink) {
            UNIT_ASSERT(result.GetIssues().ToString().contains("Stream write queries aren't allowed"));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
