#include "public_events.h"
#include "replication.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
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

void TestBootstrap(bool withIndex, bool materialized, bool directImport,
        bool globalConsistency = true, bool dropCascade = false) {
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
              CONSISTENCY_LEVEL="%s"%s, INITIAL_SCAN=FALSE);
    )", env.GetEndpoint().c_str(), globalConsistency ? "GLOBAL" : "ROW",
        globalConsistency ? ", COMMIT_INTERVAL=Interval(\"PT1S\")" : ""));

    const auto checkConfig = [&](const TString& path, auto state) {
        const auto description = env.GetDescription(path);
        const auto& config = description.GetPathDescription().GetTable().GetReplicationConfig();
        return config.GetMode() == TReplicationConfig::REPLICATION_MODE_READ_ONLY
            && config.GetConsistencyLevel() == (globalConsistency
                ? TReplicationConfig::CONSISTENCY_LEVEL_GLOBAL : TReplicationConfig::CONSISTENCY_LEVEL_ROW)
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

    if (dropCascade) {
        // A restored CDC stream is removed with its replica, but cannot be
        // removed independently while the table is attached.
        const auto dropCdc = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/replica` DROP CHANGEFEED updates;
        )").ExtractValueSync();
        UNIT_ASSERT(!dropCdc.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(dropCdc.GetIssues().ToString(), "path is an async replica table");

        scheme("DROP ASYNC REPLICATION `/Root/replication` CASCADE;");
        for (const auto* path : {
                "/Root/replication",
                "/Root/replica",
                "/Root/replica/updates",
                "/Root/replica/updates/streamImpl"}) {
            const auto description = env.GetDescription(path);
            UNIT_ASSERT_VALUES_EQUAL_C(
                description.GetSchemeStatus(), NKikimrScheme::StatusPathDoesNotExist,
                "CASCADE left a destination path: " << path << ": " << description.DebugString());
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BootstrapImport) {
    Y_UNIT_TEST_QUAD(SourceWithIndex, Materialized, DirectImport) {
        TestBootstrap(true, Materialized, DirectImport);
    }

    Y_UNIT_TEST_TWIN(SourceWithCdc, DirectImport) {
        TestBootstrap(false, false, DirectImport);
    }

    Y_UNIT_TEST_TWIN(DropCascadeWithDestinationCdc, GlobalConsistency) {
        TestBootstrap(false, false, false, GlobalConsistency, true);
    }
}

Y_UNIT_TEST_SUITE(DoneReplication) {
    Y_UNIT_TEST(CascadeDropKeepsDetachedTables) {
        // After DONE the replicas are plain tables holding the user's data. CASCADE drops replicas,
        // so it must leave them alone (manual switchover plan, C3).
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
        const auto read = [&](const TString& table) {
            const auto result = execute(TStringBuilder() << "SELECT key, value FROM " << table << " ORDER BY key;");
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };
        const auto replicaMode = [&](const TString& path) {
            return env.GetDescription(path).GetPathDescription().GetTable().GetReplicationConfig().GetMode();
        };

        scheme("CREATE TABLE `/Root/source` (key Uint32, value Utf8, PRIMARY KEY (key));");
        execute(R"(UPSERT INTO `/Root/source` (key, value) VALUES (1u, "one"u), (2u, "two"u);)");
        const auto expected = read("`/Root/source`");
        scheme(Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`
            WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin");
        )", env.GetEndpoint().c_str()));
        const auto tryRead = [&](const TString& table) -> TMaybe<TString> {
            auto result = session.ExecuteDataQuery(TStringBuilder() << "SELECT key, value FROM " << table << " ORDER BY key;",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            if (!result.IsSuccess()) {
                return Nothing();
            }
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };
        WaitFor([&] { return tryRead("`/Root/replica`") == expected; }, "initial scan");
        UNIT_ASSERT_VALUES_EQUAL(replicaMode("/Root/replica"), TReplicationConfig::REPLICATION_MODE_READ_ONLY);

        scheme(R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE="DONE");)");
        WaitFor([&] {
            return replicaMode("/Root/replica") == TReplicationConfig::REPLICATION_MODE_NONE;
        }, "replica detached by DONE");

        scheme("DROP ASYNC REPLICATION `/Root/replication` CASCADE;");
        WaitFor([&] {
            return env.GetDescription("/Root/replication").GetSchemeStatus() == NKikimrScheme::StatusPathDoesNotExist;
        }, "replication dropped");
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica").GetSchemeStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(read("`/Root/replica`"), expected);
        execute(R"(UPSERT INTO `/Root/replica` (key, value) VALUES (3u, "three"u);)");
    }
}

Y_UNIT_TEST_SUITE(ConsistentDone) {
    Y_UNIT_TEST_TWIN(GlobalDoneWaitsForAssignedTransactions, FlagOn) {
        // A GLOBAL replica stages rows under write transaction ids that commit at the next
        // heartbeat. DONE stops the workers at once, so rows of an interval that was assigned but
        // not yet committed are lost. With EnableConsistentReplicationDone a consistent DONE is
        // deferred until the commit loop has drained (manual switchover plan, C2).
        NTestHelpers::TFeatureFlags flags;
        flags.FeatureFlags.SetEnableConsistentReplicationDone(FlagOn);
        NTestHelpers::TEnv env(flags);
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
            // Changefeed creation and the detach bump the tables' schema versions; a data query
            // prepared against the old version is invalidated once and must be retried.
            for (int attempt = 0;; ++attempt) {
                auto result = session.ExecuteDataQuery(query,
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                if (result.IsSuccess() || attempt >= 10) {
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return result;
                }
                Sleep(TDuration::MilliSeconds(200));
            }
        };
        const auto read = [&](const TString& table) {
            const auto result = execute(TStringBuilder() << "SELECT key, value FROM " << table << " ORDER BY key;");
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };
        const auto replicaMode = [&](const TString& path) {
            return env.GetDescription(path).GetPathDescription().GetTable().GetReplicationConfig().GetMode();
        };

        scheme("CREATE TABLE `/Root/source` (key Uint32, value Utf8, PRIMARY KEY (key));");
        execute(R"(UPSERT INTO `/Root/source` (key, value) VALUES (1u, "one"u);)");
        // A long commit interval keeps the last interval assigned but uncommitted for a while.
        scheme(Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`
            WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin",
                  CONSISTENCY_LEVEL="GLOBAL", COMMIT_INTERVAL=Interval("PT30S"));
        )", env.GetEndpoint().c_str()));
        const auto tryRead = [&](const TString& table) -> TMaybe<TString> {
            auto result = session.ExecuteDataQuery(TStringBuilder() << "SELECT key, value FROM " << table << " ORDER BY key;",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            if (!result.IsSuccess()) {
                return Nothing();
            }
            return NYdb::FormatResultSetYson(result.GetResultSet(0));
        };
        WaitFor([&] { return tryRead("`/Root/replica`") == TMaybe<TString>(read("`/Root/source`")); }, "initial scan committed");

        // Rows of the current interval: staged on the replica, visible only after the next commit.
        execute(R"(UPSERT INTO `/Root/source` (key, value) VALUES (2u, "two"u), (3u, "three"u);)");
        const auto expected = read("`/Root/source`");
        const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        const auto replicationId = env.GetPathId("/Root/replication");
        const auto assignedTransactions = [&] {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            replicationId.ToProto(request->Record.MutablePathId());
            const auto description = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            return description->Get()->Record.GetTransactions().assigned_size();
        };
        WaitFor([&] { return assignedTransactions() > 0; }, "an assigned, uncommitted interval");

        scheme(R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE="DONE", FAILOVER_MODE="CONSISTENT");)");
        WaitFor([&] {
            return replicaMode("/Root/replica") == TReplicationConfig::REPLICATION_MODE_NONE;
        }, "replica detached by DONE");
        if (FlagOn) {
            // Detached only after the interval was committed: the staged rows are visible.
            // The detach bumps the schema version, so the first read may need a retry.
            WaitFor([&] { return tryRead("`/Root/replica`") == TMaybe<TString>(expected); },
                "committed rows visible on the detached replica");
            UNIT_ASSERT_VALUES_EQUAL(read("`/Root/replica`"), expected);
        } else {
            // The legacy DONE detaches at once; the rows of the open interval are not committed,
            // which is the loss the flag prevents. Only check that the replica is writable.
            WaitFor([&] {
                auto result = session.ExecuteDataQuery(R"(UPSERT INTO `/Root/replica` (key, value) VALUES (4u, "four"u);)",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                return result.IsSuccess();
            }, "detached replica writable");
        }
    }
}

Y_UNIT_TEST_SUITE(AttachmentCompatibility) {
    Y_UNIT_TEST_TWIN(LegacyAttachingFailsValidationAndCanBeDropped, MissingConfig) {
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

        if (MissingConfig) {
            scheme(R"(DROP TABLE `/Root/legacy_replica`;)");
            scheme(R"(CREATE TABLE `/Root/legacy_replica` (key Uint32, value Utf8, PRIMARY KEY(key));)");
        }
        const auto destinationId = env.GetPathId("/Root/legacy_replica");

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
        WaitFor([&] {
            return describe().GetState().HasError();
        }, "legacy attachment validation error");
        const auto error = describe().GetState().GetError().ShortDebugString();
        UNIT_ASSERT_STRING_CONTAINS(error, MissingConfig
            ? "replication configuration is missing"
            : "Destination table import is not complete (expected IMPORTED)");
        if (MissingConfig) {
            UNIT_ASSERT_STRING_CONTAINS(error, "/Root/legacy_replica");
            UNIT_ASSERT_STRING_CONTAINS(error, "Recreate the replication and import data");
        }
        const auto restartedActor = ResolveTablet(runtime, controllerId);

        // A failed legacy target must not interfere with other replications using the same service and source.
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
                UNIT_ASSERT(describe().GetState().HasError());
                return NYdb::FormatResultSetYson(execute(
                    "SELECT * FROM `/Root/healthy_replica` ORDER BY key;").GetResultSet(0)) == expected;
            }, "healthy replication data");
        };
        waitForData();
        execute(R"(UPDATE `/Root/source` SET value="after"u WHERE key=1u;)");
        waitForData();

        // Validation must not modify the destination or cause a controller restart loop.
        InvalidateTabletResolverCache(runtime, controllerId);
        UNIT_ASSERT_VALUES_EQUAL(ResolveTablet(runtime, controllerId), restartedActor);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/legacy_replica"), destinationId);
        const auto table = env.GetDescription("/Root/legacy_replica").GetPathDescription().GetTable();
        if (MissingConfig) {
            UNIT_ASSERT(!table.HasReplicationConfig());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(table.GetReplicationConfig().GetImportState(), TReplicationConfig::WAITING_IMPORT);
        }
        UNIT_ASSERT_VALUES_EQUAL(readDestinationState(), static_cast<ui32>(TReplication::EDstState::Error));
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
