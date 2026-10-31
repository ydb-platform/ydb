#include "public_events.h"
#include "replication.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {
namespace {

using TReplicationConfig = NKikimrSchemeOp::TTableReplicationConfig;

template <typename TPredicate>
void WaitFor(TPredicate&& predicate, TStringBuf description) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(60);
    do {
        if (predicate()) {
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    } while (TInstant::Now() < deadline);
    UNIT_FAIL("Timed out waiting for " << description);
}

template <typename TOperation>
void WaitForOperation(NYdb::NOperation::TOperationClient& client, const TOperation& operation) {
    UNIT_ASSERT_C(operation.Status().IsSuccess(), operation.Status().GetIssues().ToString());
    WaitFor([&] {
        const auto result = client.Get<TOperation>(operation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(result.Status().IsSuccess(), result.Status().GetIssues().ToString());
        return result.Ready();
    }, "S3 operation");
}

void TestBootstrap(bool withIndex, bool materialized, bool directImport) {
    InitAwsAPI();
    Y_DEFER { ShutdownAwsAPI(); };
    TPortManager ports;
    const auto port = ports.GetPort();
    NWrappers::NTestHelpers::TS3Mock s3({}, NWrappers::NTestHelpers::TS3Mock::TSettings(port));
    UNIT_ASSERT_C(s3.Start(), s3.GetError());
    TDataShardExportFactory exportFactory;

    NTestHelpers::TFeatureFlags flags;
    flags.FeatureFlags.SetEnableChangefeedsOnIndexTables(true);
    flags.FeatureFlags.SetEnableIndexMaterialization(true);
    flags.FeatureFlags.SetEnableChangefeedsExport(true);
    flags.FeatureFlags.SetEnableChangefeedsImport(true);
    flags.FeatureFlags.SetEnableDataShardDirectPartImport(directImport);
    NTestHelpers::TEnv env(flags);
    auto& runtime = env.GetRuntime();
    runtime.GetAppData().DataShardExportFactory = &exportFactory;
    const auto service = runtime.Register(CreateReplicationService());
    runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), service);

    NYdb::NTable::TTableClient tableClient(env.GetDriver());
    auto sessionResult = tableClient.CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();
    const auto scheme = [&](const TString& query) {
        const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    };
    const auto execute = [&](const TString& query) {
        auto result = session.ExecuteDataQuery(query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    };
    const auto read = [&](const TString& table, TStringBuf columns = "key, value, payload") {
        const auto result = execute(TStringBuilder() << "SELECT " << columns << " FROM " << table << " ORDER BY key;");
        return NYdb::FormatResultSetYson(result.GetResultSet(0));
    };

    scheme(Sprintf(R"(
        CREATE TABLE `/Root/source` (
            key Uint32, value Utf8, payload Utf8, PRIMARY KEY (key)%s
        );
    )", withIndex ? ", INDEX by_value GLOBAL SYNC ON (value)" : ""));

    NYdb::NTopic::TTopicClient topicClient(env.GetDriver());
    if (!withIndex) {
        scheme(R"(ALTER TABLE `/Root/source` ADD CHANGEFEED updates WITH (MODE='UPDATES', FORMAT='JSON');)");
        const auto result = topicClient.AlterTopic("/Root/source/updates", NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer().ConsumerName("consumer").EndAddConsumer()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    execute(R"(UPSERT INTO `/Root/source` (key, value, payload) VALUES
        (1u, "old"u, "keep"u), (2u, "deleted"u, "remove"u);)");
    const auto snapshot = read("`/Root/source`");

    scheme(Sprintf(R"(
        CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`
        WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin",
              CONSISTENCY_LEVEL="GLOBAL", COMMIT_INTERVAL=Interval("PT1S"), INITIAL_SCAN=FALSE);
    )", env.GetEndpoint().c_str()));

    const auto checkConfig = [&](const TString& path, auto state) {
        const auto description = env.GetDescription(path);
        const auto& config = description.GetPathDescription().GetTable().GetReplicationConfig();
        return config.GetMode() == TReplicationConfig::REPLICATION_MODE_READ_ONLY
            && config.GetConsistencyLevel() == TReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
            && config.GetImportState() == state;
    };
    WaitFor([&] {
        return checkConfig("/Root/replica", TReplicationConfig::WAITING_IMPORT)
            && (!withIndex || checkConfig("/Root/replica/by_value/indexImplTable", TReplicationConfig::WAITING_IMPORT));
    }, "WAITING_IMPORT placeholders");
    const auto placeholderId = env.GetPathId("/Root/replica");
    UNIT_ASSERT_VALUES_EQUAL(read("`/Root/replica`"), "[]");

    NYdb::NOperation::TOperationClient operationClient(env.GetDriver());
    NYdb::NExport::TExportClient exportClient(env.GetDriver());
    auto exportSettings = NYdb::NExport::TExportToS3Settings()
        .Endpoint(Sprintf("localhost:%u", port)).Scheme(NYdb::ES3Scheme::HTTP)
        .Bucket("bucket").AccessKey("key").SecretKey("secret")
        .IncludeIndexData(materialized);
    exportSettings.AppendItem({.Src = "/Root/source", .Dst = "snapshot"});
    WaitForOperation(operationClient, exportClient.ExportToS3(exportSettings).ExtractValueSync());

    // The backup contains the old value. UPDATES must preserve the untouched payload,
    // and index replication must remove the old index entry after attachment.
    execute(R"(UPDATE `/Root/source` SET value="new"u WHERE key=1u;
        DELETE FROM `/Root/source` WHERE key=2u;
        UPSERT INTO `/Root/source` (key, value, payload) VALUES (3u, "inserted"u, "fresh"u);)");

    NYdb::NImport::TImportClient importClient(env.GetDriver());
    auto importSettings = NYdb::NImport::TImportFromS3Settings()
        .Endpoint(Sprintf("localhost:%u", port)).Scheme(NYdb::ES3Scheme::HTTP)
        .Bucket("bucket").AccessKey("key").SecretKey("secret")
        .IndexPopulationMode(materialized ? NYdb::NImport::EIndexPopulationMode::Import
                                         : NYdb::NImport::EIndexPopulationMode::Build);
    importSettings.AppendItem({.Src = "snapshot", .Dst = "/Root/replica"});
    WaitForOperation(operationClient, importClient.ImportFromS3(importSettings).ExtractValueSync());

    UNIT_ASSERT(env.GetPathId("/Root/replica") != placeholderId);
    UNIT_ASSERT(checkConfig("/Root/replica", TReplicationConfig::IMPORTED));
    UNIT_ASSERT_VALUES_EQUAL(read("`/Root/replica`"), snapshot);
    const auto checkRestoredObjects = [&](TStringBuf phase) {
        if (withIndex) {
            const auto description = env.GetDescription("/Root/replica/by_value/indexImplTable");
            const auto& config = description.GetPathDescription().GetTable().GetReplicationConfig();
            UNIT_ASSERT_C(config.GetMode() == TReplicationConfig::REPLICATION_MODE_READ_ONLY
                && config.GetConsistencyLevel() == TReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
                && config.GetImportState() == TReplicationConfig::IMPORTED,
                phase << ": index replication config: " << config.DebugString());
            // Read only covered columns so a lookup into the base table cannot hide stale index entries.
            UNIT_ASSERT_VALUES_EQUAL(read("`/Root/replica` VIEW by_value", "key, value"),
                read("`/Root/replica`", "key, value"));
        } else {
            const auto table = session.DescribeTable("/Root/replica").ExtractValueSync();
            UNIT_ASSERT_C(table.IsSuccess(), table.GetIssues().ToString());
            bool found = false;
            for (const auto& feed : table.GetTableDescription().GetChangefeedDescriptions()) {
                if (feed.GetName() == "updates") {
                    found = true;
                    UNIT_ASSERT_VALUES_EQUAL(feed.GetMode(), NYdb::NTable::EChangefeedMode::Updates);
                    UNIT_ASSERT_VALUES_EQUAL(feed.GetFormat(), NYdb::NTable::EChangefeedFormat::Json);
                }
            }
            UNIT_ASSERT_C(found, "Imported table has no updates changefeed");
            const auto topic = topicClient.DescribeTopic("/Root/replica/updates").ExtractValueSync();
            UNIT_ASSERT_C(topic.IsSuccess(), topic.GetIssues().ToString());
            const auto& consumers = topic.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(consumers.front().GetConsumerName(), "consumer");
        }
    };
    checkRestoredObjects("after import");

    // The final ALTER must persist IMPORTED before import completion.
    // Attachment must observe IMPORTED even after SchemeShard restarts.
    const auto schemeShardId = env.GetPathId("/Root/replica").OwnerId;
    const auto previousSchemeShard = ResolveTablet(runtime, schemeShardId);
    env.SendAsync(schemeShardId, new TEvents::TEvPoisonPill());
    WaitFor([&] {
        InvalidateTabletResolverCache(runtime, schemeShardId);
        return ResolveTablet(runtime, schemeShardId) != previousSchemeShard;
    }, "SchemeShard restart after import");
    UNIT_ASSERT(checkConfig("/Root/replica", TReplicationConfig::IMPORTED));
    checkRestoredObjects("after SchemeShard restart");

    const auto replicationId = env.GetPathId("/Root/replication");
    const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
        .GetReplicationDescription().GetControllerId();
    scheme(R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE="STANDBY");)");
    const auto waitForData = [&] {
        const auto expected = read("`/Root/source`");
        WaitFor([&] {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            replicationId.ToProto(request->Record.MutablePathId());
            const auto description = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            const auto& record = description->Get()->Record;
            UNIT_ASSERT_C(!record.GetState().HasError(), record.DebugString());
            return record.GetState().HasStandBy() && read("`/Root/replica`") == expected;
        }, "replication to apply source changes");
        checkRestoredObjects("after replication");
    };
    waitForData();

    // Also exercise changes produced after attachment, rather than just replaying the backlog.
    execute(R"(UPDATE `/Root/source` SET value="after-standby"u WHERE key=1u;)");
    waitForData();
    UNIT_ASSERT(checkConfig("/Root/replica", TReplicationConfig::IMPORTED));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BootstrapImport) {
    Y_UNIT_TEST_QUAD(SourceWithIndex, Materialized, DirectImport) {
        TestBootstrap(true, Materialized, DirectImport);
    }

    Y_UNIT_TEST_TWIN(SourceWithCdc, DirectImport) {
        TestBootstrap(false, false, DirectImport);
    }
}

Y_UNIT_TEST_SUITE(AttachmentCompatibility) {
    Y_UNIT_TEST(LegacyAttachingDoesNotBlockOtherReplicationAndCanBeDropped) {
        NTestHelpers::TEnv env;
        auto& runtime = env.GetRuntime();
        const auto service = runtime.Register(CreateReplicationService());
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), service);
        NYdb::NTable::TTableClient client(env.GetDriver());
        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto scheme = [&](const TString& query) {
            const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        const auto execute = [&](const TString& query) {
            auto result = session.ExecuteDataQuery(query,
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };
        scheme(R"(CREATE TABLE `/Root/source` (key Uint32, value Utf8, PRIMARY KEY(key));)");
        execute(R"(UPSERT INTO `/Root/source` (key, value) VALUES (1u, "before"u);)");
        scheme(Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/legacy` FOR `/Root/source` AS `/Root/legacy_replica`
            WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin", INITIAL_SCAN=FALSE);
        )", env.GetEndpoint().c_str()));
        WaitFor([&] {
            return env.GetDescription("/Root/legacy_replica").GetPathDescription().GetTable()
                .GetReplicationConfig().GetImportState() == TReplicationConfig::WAITING_IMPORT;
        }, "legacy destination placeholder");

        const auto pathId = env.GetPathId("/Root/legacy");
        const auto destinationId = env.GetPathId("/Root/legacy_replica");
        const auto controllerId = env.GetDescription("/Root/legacy").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        const auto describe = [&] {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            pathId.ToProto(request->Record.MutablePathId());
            // A pipe waits for tablet activation; resolving the actor alone is insufficient after reboot.
            runtime.SendToPipe(controllerId, env.GetSender(), request.Release(), 0,
                NTabletPipe::TClientRetryPolicy::WithRetries());
            auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(env.GetSender());
            UNIT_ASSERT_C(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
                result->Get()->Record.DebugString());
            return result->Get()->Record;
        };
        const auto initial = describe();
        UNIT_ASSERT_VALUES_EQUAL(initial.TargetsSize(), 1);
        const auto targetId = initial.GetTargets(0).GetId();
        const auto readDestinationState = [&] {
            NKikimrMiniKQL::TResult result;
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, controllerId, Sprintf(R"((
                (let key '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
                (return (AsList (SetResult 'State (SelectRow 'Targets key '('DstState)))))
            ))", targetId), result), NKikimrProto::OK);
            return result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetUint32();
        };
        WaitFor([&] {
            return readDestinationState() == static_cast<ui32>(TReplication::EDstState::WaitingImport);
        }, "controller to persist destination creation");

        // Reproduce the on-disk layout of an older controller interrupted during attachment.
        // Use the old numeric values so changing the enum cannot silently change the fixture.
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, controllerId, Sprintf(R"((
            (let targetKey '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
            (let replicationKey '('('Id (Uint64 '1))))
            (return (AsList
                (UpdateRow 'Targets targetKey '(
                    '('DstState (Uint8 '6))
                    '('DstPathOwnerId (Uint64 '%lu))
                    '('DstPathLocalId (Uint64 '%lu))
                    '('PendingDstPathOwnerId (Uint64 '%lu))
                    '('PendingDstPathLocalId (Uint64 '%lu))))
                (UpdateRow 'Replications replicationKey '(
                    '('State (Uint8 '0)) '('DesiredState (Uint8 '0))))))
        ))", targetId, InvalidOwnerId, InvalidLocalPathId, destinationId.OwnerId, destinationId.LocalPathId), result),
            NKikimrProto::OK);

        const auto previousActor = ResolveTablet(runtime, controllerId);
        env.SendAsync(controllerId, new TEvents::TEvPoisonPill());
        WaitFor([&] {
            InvalidateTabletResolverCache(runtime, controllerId);
            return ResolveTablet(runtime, controllerId) != previousActor;
        }, "legacy controller restart");
        UNIT_ASSERT_C(describe().GetState().HasStandBy(), "Controller did not load the legacy replication");
        const auto restartedActor = ResolveTablet(runtime, controllerId);

        // A stranded legacy target must not interfere with other replications using the same service and source.
        scheme(Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/healthy` FOR `/Root/source` AS `/Root/healthy_replica`
            WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin", INITIAL_SCAN=TRUE);
        )", env.GetEndpoint().c_str()));
        WaitFor([&] {
            return env.GetDescription("/Root/healthy_replica").HasPathDescription();
        }, "healthy destination creation");
        const auto waitForData = [&] {
            const auto expected = NYdb::FormatResultSetYson(execute(
                "SELECT * FROM `/Root/source` ORDER BY key;").GetResultSet(0));
            WaitFor([&] {
                UNIT_ASSERT(describe().GetState().HasStandBy());
                return NYdb::FormatResultSetYson(execute(
                    "SELECT * FROM `/Root/healthy_replica` ORDER BY key;").GetResultSet(0)) == expected;
            }, "healthy replication data");
        };
        waitForData();
        execute(R"(UPDATE `/Root/source` SET value="after"u WHERE key=1u;)");
        waitForData();

        // The legacy target stays inert: no attachment, destination replacement, or restart loop.
        InvalidateTabletResolverCache(runtime, controllerId);
        UNIT_ASSERT_VALUES_EQUAL(ResolveTablet(runtime, controllerId), restartedActor);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/legacy_replica"), destinationId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/legacy_replica").GetPathDescription().GetTable()
            .GetReplicationConfig().GetImportState(), TReplicationConfig::WAITING_IMPORT);
        UNIT_ASSERT_VALUES_EQUAL(readDestinationState(), 6);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(execute(
            "SELECT * FROM `/Root/legacy_replica`;").GetResultSet(0)), "[]");

        scheme(R"(DROP ASYNC REPLICATION `/Root/legacy`;)");
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/legacy").GetSchemeStatus(),
            static_cast<int>(NKikimrScheme::StatusPathDoesNotExist));
        // Dropping one replication must not remove the other one's source stream.
        execute(R"(UPDATE `/Root/source` SET value="after-drop"u WHERE key=1u;)");
        const auto expected = NYdb::FormatResultSetYson(execute(
            "SELECT * FROM `/Root/source` ORDER BY key;").GetResultSet(0));
        WaitFor([&] {
            return NYdb::FormatResultSetYson(execute(
                "SELECT * FROM `/Root/healthy_replica` ORDER BY key;").GetResultSet(0)) == expected;
        }, "healthy replication after legacy DROP");
    }
}

} // namespace NKikimr::NReplication::NController
