#include "dst_creator.h"
#include "private_events.h"

#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/scope.h>

#include <atomic>
#include <memory>

namespace NKikimr::NReplication::NController {

namespace {

class TAttachmentAllocator: public TActorBootstrapped<TAttachmentAllocator> {
public:
    TAttachmentAllocator(TActorId txProxy, TActorId allocator)
        : TxProxy(txProxy)
        , Allocator(allocator)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        Send(ev->Forward(ev->GetTypeRewrite() == TEvTxUserProxy::TEvAllocateTxId::EventType
            ? Allocator : TxProxy));
    }

private:
    const TActorId TxProxy;
    const TActorId Allocator;
};

class TAttachmentEventCounter: public TActorBootstrapped<TAttachmentEventCounter> {
public:
    TAttachmentEventCounter(TActorId recipient, ui32 type, std::shared_ptr<std::atomic<ui32>> count)
        : Recipient(recipient)
        , Type(type)
        , Count(std::move(count))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == Type) {
            ++*Count;
        }
        Send(ev->Forward(Recipient));
    }

private:
    const TActorId Recipient;
    const ui32 Type;
    const std::shared_ptr<std::atomic<ui32>> Count;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(DstCreator) {
    using namespace NTestHelpers;

    void PrepareAttachDst(TEnv<>& env) {
        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
        UNIT_ASSERT(ev->Get()->DstPathId);
        env.GetRuntime().Send(ev->Sender, env.GetSender(), new TEvPrivate::TEvPrepareAttachDstResult());
    }

    void CheckTableReplica(
            const TTestTableDescription& tableDesc,
            const NKikimrSchemeOp::TTableDescription& replicatedDesc,
            EReplicationMode mode = EReplicationMode::ReadOnly,
            EConsistencyLevel consistency = EConsistencyLevel::Row)
    {
        UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.KeyColumnNamesSize(), tableDesc.KeyColumns.size());
        for (ui32 i = 0; i < replicatedDesc.KeyColumnNamesSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.GetKeyColumnNames(i), tableDesc.KeyColumns[i]);
        }

        UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.ColumnsSize(), tableDesc.Columns.size());
        for (ui32 i = 0; i < replicatedDesc.ColumnsSize(); ++i) {
            auto pred = [name = replicatedDesc.GetColumns(i).GetName()](const auto& column) {
                return name == column.Name;
            };

            UNIT_ASSERT(FindIfPtr(tableDesc.Columns, pred));
        }

        TString error;
        UNIT_ASSERT_C(CheckReplicationConfig(replicatedDesc.GetReplicationConfig(), mode, consistency, error), error);
    }

    void Basic(
            const TString& replicatedPath,
            EReplicationMode mode = EReplicationMode::ReadOnly,
            EConsistencyLevel consistency = EConsistencyLevel::Row)
    {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };

        env.CreateTable("/Root", *MakeTableDescription(tableDesc));
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", replicatedPath, mode, consistency
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto desc = env.GetDescription(replicatedPath);
        const auto& replicatedDesc = desc.GetPathDescription().GetTable();

        CheckTableReplica(tableDesc, replicatedDesc, mode, consistency);
    }

    Y_UNIT_TEST(Basic) {
        Basic("/Root/Replicated");
    }

    Y_UNIT_TEST(AttachTable) {
        TEnv env;
        const auto table = TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };
        env.CreateTable("/Root", *MakeTableDescription(table));
        auto dst = table;
        dst.Name = "Dst";
        env.CreateTable("/Root", *MakeTableDescription(dst));
        const auto pathId = env.GetPathId("/Root/Dst");

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
            "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Global, false));

        PrepareAttachDst(env);

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->DstPathId, pathId);
        CheckTableReplica(table, env.GetDescription("/Root/Dst").GetPathDescription().GetTable(),
            EReplicationMode::ReadOnly, EConsistencyLevel::Global);
    }

    void CreateTableWithVectorIndexes(TEnv<>& env, const TString& path, bool withSyncIndex = false) {
        using namespace NYdb::NTable;
        auto builder = TTableBuilder()
            .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("embedding", NYdb::EPrimitiveType::String)
            .SetPrimaryKeyColumn("key");
        for (const auto* name : {"vector_idx", "other_vector_idx"}) {
            builder.AddVectorKMeansTreeIndex(name, {"embedding"}, {TVectorIndexSettings{
                TVectorIndexSettings::EMetric::CosineDistance,
                TVectorIndexSettings::EVectorType::Float,
                2,
            }, 2, 1});
        }
        if (withSyncIndex) {
            builder.AddSyncSecondaryIndex("by_embedding", "embedding");
        }
        auto created = env.Send<TEvYdbProxy::TEvCreateTableResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvCreateTableRequest(path, builder.Build(), TCreateTableSettings()));
        UNIT_ASSERT_C(created->Get()->Result.IsSuccess(), created->Get()->Result.GetIssues().ToString());
    }

    TActorId StartVectorIndexAttachment(TEnv<>& env, const TPathId& pendingDstPathId = {},
            bool cleanup = false)
    {
        return env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
            "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Global,
            false /* initialScan */, pendingDstPathId, cleanup));
    }

    void AssertVectorIndexAttachment(TEnv<>& env, const TPathId& pathId) {
        // Also accept an early result so a schema mismatch fails the assertion
        // instead of timing out while waiting for the attachment handshake.
        for (;;) {
            TAutoPtr<IEventHandle> handle;
            auto [prepare, result] = env.GetRuntime().GrabEdgeEvents<
                TEvPrivate::TEvPrepareAttachDst, TEvPrivate::TEvCreateDstResult>(handle);
            if (prepare) {
                UNIT_ASSERT_VALUES_EQUAL(prepare->DstPathId, pathId);
                env.GetRuntime().Send(handle->Sender, env.GetSender(), new TEvPrivate::TEvPrepareAttachDstResult());
                continue;
            }

            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Status, NKikimrScheme::StatusSuccess, result->Error);
            UNIT_ASSERT_VALUES_EQUAL(result->DstPathId, pathId);
            break;
        }

        const auto desc = env.GetDescription("/Root/Dst");
        TString error;
        UNIT_ASSERT_C(CheckReplicationConfig(desc.GetPathDescription().GetTable().GetReplicationConfig(),
            EReplicationMode::ReadOnly, EConsistencyLevel::Global, error), error);
    }

    void AttachTableWithVectorIndex(bool withSyncIndex) {
        TEnv env;
        // A backup restores the source schema, including its vector indexes.
        CreateTableWithVectorIndexes(env, "/Root/Source", withSyncIndex);
        CreateTableWithVectorIndexes(env, "/Root/Dst", withSyncIndex);
        const auto pathId = env.GetPathId("/Root/Dst");

        StartVectorIndexAttachment(env);
        AssertVectorIndexAttachment(env, pathId);

        const auto dst = env.GetDescription("/Root/Dst");
        const auto& indexes = dst.GetPathDescription().GetTable().GetTableIndexes();
        UNIT_ASSERT_VALUES_EQUAL(indexes.size(), withSyncIndex ? 1 : 0);
        if (withSyncIndex) {
            UNIT_ASSERT_VALUES_EQUAL(indexes.Get(0).GetName(), "by_embedding");
            UNIT_ASSERT_VALUES_EQUAL(indexes.Get(0).GetType(), NKikimrSchemeOp::EIndexTypeGlobal);
        }
        const auto src = env.GetDescription("/Root/Source");
        UNIT_ASSERT_VALUES_EQUAL(src.GetPathDescription().GetTable().TableIndexesSize(), withSyncIndex ? 3 : 2);
        UNIT_ASSERT_VALUES_EQUAL(src.GetPathDescription().GetTable().GetReplicationConfig().GetMode(),
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
    }

    Y_UNIT_TEST(AttachTableWithVectorIndex) {
        AttachTableWithVectorIndex(false);
    }

    Y_UNIT_TEST(AttachTableWithVectorAndSyncIndexes) {
        AttachTableWithVectorIndex(true);
    }

    Y_UNIT_TEST(IncompatibleAttachmentPreservesVectorIndexes) {
        TEnv env;
        CreateTableWithVectorIndexes(env, "/Root/Source");
        CreateTableWithVectorIndexes(env, "/Root/Dst");
        const auto altered = env.Send<TEvYdbProxy::TEvAlterTableResponse>(env.GetYdbProxy(),
            new TEvYdbProxy::TEvAlterTableRequest("/Root/Dst", NYdb::NTable::TAlterTableSettings()
                .AppendAddColumns({"extra", NYdb::TTypeBuilder().BeginOptional()
                    .Primitive(NYdb::EPrimitiveType::Uint64).EndOptional().Build()})));
        UNIT_ASSERT_C(altered->Get()->Result.IsSuccess(), altered->Get()->Result.GetIssues().ToString());

        StartVectorIndexAttachment(env);
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusSchemeError);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Error, "Columns size mismatch");
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 2);
    }

    Y_UNIT_TEST(RecoverAttachmentAfterVectorIndexDrop) {
        TEnv env(true, ERuntimeMode::Simulated);
        CreateTableWithVectorIndexes(env, "/Root/Source");
        CreateTableWithVectorIndexes(env, "/Root/Dst");
        const auto pathId = env.GetPathId("/Root/Dst");
        const auto creator = StartVectorIndexAttachment(env);
        PrepareAttachDst(env);

        bool dropCompleted = false;
        auto observer = env.GetRuntime().AddObserver<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(
            [&](NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
                if (ev->Recipient == creator) {
                    dropCompleted = true;
                    ev.Reset();
                }
            });
        env.GetRuntime().WaitFor("first vector index removal", [&] { return dropCompleted; }, TDuration::Seconds(60));
        env.SendAsync(creator, new TEvents::TEvPoison());
        observer.Remove();
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 1);

        StartVectorIndexAttachment(env, pathId);
        AssertVectorIndexAttachment(env, pathId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Source").GetPathDescription().GetTable().TableIndexesSize(), 2);
    }

    Y_UNIT_TEST(DoneDuringAttachmentPreservesVectorIndexes) {
        TEnv env;
        CreateTableWithVectorIndexes(env, "/Root/Source");
        CreateTableWithVectorIndexes(env, "/Root/Dst");
        const auto pathId = env.GetPathId("/Root/Dst");

        StartVectorIndexAttachment(env, pathId, true /* cleanup */);
        AssertVectorIndexAttachment(env, pathId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 2);
    }

    Y_UNIT_TEST(CleanupBeforeVectorIndexDropPreservesIndexes) {
        TEnv env(true, ERuntimeMode::Simulated);
        CreateTableWithVectorIndexes(env, "/Root/Source");
        CreateTableWithVectorIndexes(env, "/Root/Dst");
        const auto pathId = env.GetPathId("/Root/Dst");
        const auto creator = StartVectorIndexAttachment(env);
        const auto prepare = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(prepare->Get()->DstPathId, pathId);

        // DONE/DROP can arrive while the controller persists the destination.
        // Finish attachment locally without submitting the prepared DROP INDEX.
        env.SendAsync(creator, new TEvPrivate::TEvCompleteDstAttachment());
        env.SendAsync(creator, new TEvPrivate::TEvPrepareAttachDstResult());
        AssertVectorIndexAttachment(env, pathId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 2);
    }

    Y_UNIT_TEST(StandbyDropsRestoredVectorIndexes) {
        TEnv env(true, ERuntimeMode::Simulated);
        CreateTableWithVectorIndexes(env, "/Root/Source");
        CreateTableWithVectorIndexes(env, "/Root/Dst");
        const auto pathId = env.GetPathId("/Root/Dst");
        env.GetRuntime().RegisterService(MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)),
            env.GetRuntime().Register(CreateReplicationMockService(env.GetSender())));

        bool streamCreated = false;
        auto streamObserver = env.GetRuntime().AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::EvCreateStreamResult && ev->HasEvent()) {
                if (const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase())) {
                    UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                    streamCreated = true;
                }
            }
        });
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint()).Database(env.GetDatabase()));
        const auto sessionResult = env.RunCall([&] { return client.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto created = env.RunCall([&] {
            return session.ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/Source` AS `/Root/Dst`
                WITH (INITIAL_SCAN = FALSE, TOKEN = "root@builtin",
                    CONNECTION_STRING = "grpc://)" << env.GetEndpoint() << R"(/?database=/Root");
            )").GetValueSync();
        });
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        const auto desc = env.GetDescription("/Root/replication");
        const auto& replication = desc.GetPathDescription().GetReplicationDescription();
        UNIT_ASSERT(replication.GetState().HasPaused());
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        env.SendAsync(replication.GetControllerId(), new TEvService::TEvStatus());
        // Bootstrap from backup starts after the source stream is ready.
        env.GetRuntime().WaitFor("source stream creation", [&] { return streamCreated; }, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Dst").GetPathDescription().GetTable().TableIndexesSize(), 2);

        const auto resumed = env.RunCall([&] {
            return session.ExecuteSchemeQuery(
                R"(ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = "STANDBY"))").GetValueSync();
        });
        UNIT_ASSERT_C(resumed.IsSuccess(), resumed.GetIssues().ToString());
        const auto worker = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender(), TDuration::Seconds(60));
        UNIT_ASSERT_C(worker, env.GetDescription("/Root/replication").DebugString());

        const auto dst = env.GetDescription("/Root/Dst");
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/Dst"), pathId);
        UNIT_ASSERT_VALUES_EQUAL(dst.GetPathDescription().GetTable().TableIndexesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(dst.GetPathDescription().GetTable().GetReplicationConfig().GetMode(),
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/Source").GetPathDescription().GetTable().TableIndexesSize(), 2);
    }

    enum class EAttachmentSchemaChange {
        None,
        Source,
        Destination,
    };

    void RecoverAttachment(bool attached, EAttachmentSchemaChange schemaChange, bool doneWithoutSource = false) {
        TEnv env;
        const auto table = TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };
        env.CreateTable("/Root", *MakeTableDescription(table));
        auto dst = table;
        dst.Name = "Dst";
        env.CreateTable("/Root", *MakeTableDescription(dst));

        auto createDstCreator = [&](const TPathId& pendingDstPathId) {
            const bool finishWithoutSource = pendingDstPathId && doneWithoutSource;
            return env.GetRuntime().Register(CreateDstCreator(
                env.GetSender(), env.GetSchemeshardId("/Root/Source"),
                finishWithoutSource ? TActorId{} : env.GetYdbProxy(),
                "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
                "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Global,
                false /* initialScan */, pendingDstPathId,
                finishWithoutSource));
        };

        const auto creator = createDstCreator({});
        auto prepare = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
        const auto pendingDstPathId = prepare->Get()->DstPathId;
        UNIT_ASSERT_VALUES_EQUAL(pendingDstPathId, env.GetPathId("/Root/Dst"));

        if (attached) {
            env.SendAsync(creator, new TEvPrivate::TEvPrepareAttachDstResult());
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrScheme::StatusSuccess, ev->Get()->Error);
        } else {
            // Restart after persisting the path ID, before SchemeShard alters the table.
            env.SendAsync(creator, new TEvents::TEvPoison());
        }

        if (schemaChange != EAttachmentSchemaChange::None) {
            const auto path = schemaChange == EAttachmentSchemaChange::Source ? "/Root/Source" : "/Root/Dst";
            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(env.GetYdbProxy(),
                new TEvYdbProxy::TEvAlterTableRequest(path, NYdb::NTable::TAlterTableSettings()
                    .AppendDropColumns("value")));
            UNIT_ASSERT_C(ev->Get()->Result.IsSuccess(), ev->Get()->Result.GetIssues().ToString());
        }

        createDstCreator(pendingDstPathId);
        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        const auto desc = env.GetDescription("/Root/Dst");
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/Dst"), pendingDstPathId);
        if (schemaChange == EAttachmentSchemaChange::None) {
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrScheme::StatusSuccess, ev->Get()->Error);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->DstPathId, pendingDstPathId);
            CheckTableReplica(table, desc.GetPathDescription().GetTable(),
                EReplicationMode::ReadOnly, EConsistencyLevel::Global);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSchemeError);
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Error, "Columns size mismatch");
            UNIT_ASSERT_VALUES_EQUAL(desc.GetPathDescription().GetTable().GetReplicationConfig().GetMode(),
                attached ? NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY
                         : NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
        }
    }

    Y_UNIT_TEST(RecoverAttachment) {
        RecoverAttachment(false, EAttachmentSchemaChange::None);
    }

    Y_UNIT_TEST(RecoverAttachedTable) {
        RecoverAttachment(true, EAttachmentSchemaChange::None);
    }

    Y_UNIT_TEST(RecoverAttachmentWithSourceSchemaDrift) {
        RecoverAttachment(false, EAttachmentSchemaChange::Source);
    }

    Y_UNIT_TEST(RecoverAttachmentWithDestinationSchemaDrift) {
        RecoverAttachment(false, EAttachmentSchemaChange::Destination);
    }

    Y_UNIT_TEST(RecoverAttachedTableWithSourceSchemaDrift) {
        RecoverAttachment(true, EAttachmentSchemaChange::Source);
    }

    Y_UNIT_TEST(RecoverAttachmentForDoneWithoutSource) {
        RecoverAttachment(false, EAttachmentSchemaChange::None, true);
    }

    Y_UNIT_TEST(RecoverAttachedTableForDoneWithoutSource) {
        RecoverAttachment(true, EAttachmentSchemaChange::None, true);
    }

    enum class EAttachmentStart {
        FreshWritable,
        FreshPublished,
        RecoveredPublished,
        RecoveredBeforeSubmission,
    };

    void CheckAttachmentCompletionBarrier(
            EAttachmentStart start,
            bool cleanup = false,
            bool acceptedBeforeRetry = false)
    {
        const bool recovery = start == EAttachmentStart::RecoveredPublished
            || start == EAttachmentStart::RecoveredBeforeSubmission;
        const bool beforeSubmission = start == EAttachmentStart::RecoveredBeforeSubmission;
        TEnv env;
        auto& runtime = env.GetRuntime();
        const auto table = TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        };
        env.CreateTable("/Root", *MakeTableDescription(table));
        auto dst = table;
        dst.Name = "Dst";
        env.CreateTable("/Root", *MakeTableDescription(dst));
        const auto pathId = env.GetPathId("/Root/Dst");
        const auto databasePathId = env.GetPathId("/Root");
        auto description = env.GetDescription("/Root/Dst");

        const auto allocator = runtime.AllocateEdgeActor();
        const auto pipeCache = runtime.AllocateEdgeActor();
        const auto results = std::make_shared<std::atomic<ui32>>(0);
        const auto requests = std::make_shared<std::atomic<ui32>>(0);
        const auto parent = runtime.Register(new TAttachmentEventCounter(
            env.GetSender(),
            TEvPrivate::TEvCreateDstResult::EventType,
            results));
        const auto countedPipeCache = runtime.Register(new TAttachmentEventCounter(
            pipeCache,
            TEvPipeCache::TEvForward::EventType,
            requests));
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), runtime.Register(new TAttachmentAllocator(txProxy, allocator)));
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        const auto creator = runtime.Register(CreateDstCreator(
            parent,
            pathId.OwnerId,
            cleanup ? TActorId{} : env.GetYdbProxy(),
            "/Root",
            databasePathId,
            1,
            1,
            TReplication::ETargetKind::Table,
            "/Root/Source",
            "/Root/Dst",
            EReplicationMode::ReadOnly,
            EConsistencyLevel::Row,
            false,
            recovery ? pathId : TPathId{},
            cleanup));

        auto allocate = [&](ui64 txId) {
            for (;;) {
                const auto request = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(allocator);
                if (request->Sender == creator) {
                    break;
                }
                runtime.Send(request->Forward(txProxy));
            }
            NTxProxy::TTxProxyServices services;
            services.LeaderPipeCache = countedPipeCache;
            runtime.Send(
                creator,
                allocator,
                new TEvTxUserProxy::TEvAllocateTxIdResult(txId, services, {}));
        };
        auto expectRequest = [&](ui32 type) {
            const auto request = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(request->Get()->Ev->Type(), type);
        };
        auto describe = [&](bool published) {
            expectRequest(NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);
            auto result = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            result->Record.SetStatus(NKikimrScheme::StatusSuccess);
            result->Record.SetPathId(pathId.LocalPathId);
            result->Record.SetPathOwnerId(pathId.OwnerId);
            *result->Record.MutablePathDescription() = description.GetPathDescription();
            if (published) {
                // SchemeShard has planned the ALTER but still awaits destination
                // shards. Its published mode alone cannot release workers.
                FillReplicationConfig(
                    *result->Record.MutablePathDescription()->MutableTable()->MutableReplicationConfig(),
                    EReplicationMode::ReadOnly,
                    EConsistencyLevel::Row);
                result->Record.MutablePathDescription()->MutableSelf()->SetPathState(
                    NKikimrSchemeOp::EPathStateAlter);
            }
            runtime.Send(creator, pipeCache, result.Release());
            Sleep(TDuration::MilliSeconds(100));
            UNIT_ASSERT_VALUES_EQUAL_C(results->load(), 0, "Attachment completed before shards");
        };
        auto proposeResult = [&](ui64 txId, NKikimrScheme::EStatus status) {
            expectRequest(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
            runtime.Send(
                creator,
                pipeCache,
                new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult(status, txId, pathId.OwnerId));
        };

        allocate(100);
        describe(start == EAttachmentStart::RecoveredPublished || start == EAttachmentStart::FreshPublished);
        if (!recovery) {
            runtime.GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
            env.SendAsync(creator, new TEvPrivate::TEvPrepareAttachDstResult());
        }
        allocate(101);
        if (!beforeSubmission) {
            if (!recovery) {
                // Lose the proposal response after it was submitted. The retry
                // must serialize behind that ALTER even after mode publication.
                if (acceptedBeforeRetry) {
                    proposeResult(101, NKikimrScheme::StatusAccepted);
                    expectRequest(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);
                } else {
                    expectRequest(NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
                }
                runtime.Send(creator, pipeCache, new TEvPipeCache::TEvDeliveryProblem(pathId.OwnerId, false));
                env.SendAsync(creator, new TEvents::TEvWakeup());
                allocate(102);
            }
            proposeResult(recovery ? 101 : 102, NKikimrScheme::StatusMultipleModifications);
            env.SendAsync(creator, new TEvents::TEvWakeup());
            describe(true);
            allocate(103);
        }

        const ui64 txId = beforeSubmission ? 101 : 103;
        proposeResult(txId, NKikimrScheme::StatusAccepted);
        expectRequest(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);
        const ui32 requestsBeforeStaleReplies = requests->load();
        // Replies to earlier proposals/subscriptions can survive a pipe retry.
        // Neither may replace the currently awaited transaction or complete it.
        runtime.Send(
            creator,
            pipeCache,
            new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult(
                NKikimrScheme::StatusAccepted,
                101 - beforeSubmission,
                pathId.OwnerId));
        runtime.Send(
            creator,
            pipeCache,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(101 - beforeSubmission));
        Sleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(requests->load(), requestsBeforeStaleReplies);
        UNIT_ASSERT_VALUES_EQUAL(results->load(), 0);
        // DONE/DROP may arrive after submission. They must retain this barrier
        // and complete attachment locally before detaching the retained table.
        env.SendAsync(creator, new TEvPrivate::TEvCompleteDstAttachment());
        runtime.Send(
            creator,
            pipeCache,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(txId));
        expectRequest(NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);
        auto completed = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        completed->Record.SetStatus(NKikimrScheme::StatusSuccess);
        completed->Record.SetPathId(pathId.LocalPathId);
        completed->Record.SetPathOwnerId(pathId.OwnerId);
        *completed->Record.MutablePathDescription() = description.GetPathDescription();
        FillReplicationConfig(
            *completed->Record.MutablePathDescription()->MutableTable()->MutableReplicationConfig(),
            EReplicationMode::ReadOnly,
            EConsistencyLevel::Row);
        runtime.Send(creator, pipeCache, completed.Release());
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstPathId, pathId);
    }

    Y_UNIT_TEST_TWIN(RecoveryWaitsForAttachmentShardCompletion, Cleanup) {
        CheckAttachmentCompletionBarrier(EAttachmentStart::RecoveredPublished, Cleanup);
    }

    Y_UNIT_TEST_TWIN(RecoveryBeforeAttachmentSubmission, Cleanup) {
        CheckAttachmentCompletionBarrier(EAttachmentStart::RecoveredBeforeSubmission, Cleanup);
    }

    Y_UNIT_TEST(AttachmentWaitsForCompletionAfterPipeRetry) {
        CheckAttachmentCompletionBarrier(EAttachmentStart::FreshWritable);
    }

    Y_UNIT_TEST(AttachmentIgnoresOldCompletionAfterPipeRetry) {
        CheckAttachmentCompletionBarrier(EAttachmentStart::FreshWritable, false, true);
    }

    Y_UNIT_TEST(FreshReadonlyAttachmentWaitsForShardCompletion) {
        CheckAttachmentCompletionBarrier(EAttachmentStart::FreshPublished);
    }

    Y_UNIT_TEST(MissingAttachmentTable) {
        TEnv env;
        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
            "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Row, false));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Error, "/Root/Dst");
    }

    Y_UNIT_TEST(IncompatibleAttachmentTable) {
        TEnv env;
        auto table = TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        };
        env.CreateTable("/Root", *MakeTableDescription(table));
        table.Name = "Dst";
        table.Columns[0].Type = "Utf8";
        env.CreateTable("/Root", *MakeTableDescription(table));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
            "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Row, false));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSchemeError);
        UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Error, "Column type mismatch");
        UNIT_ASSERT(!env.GetDescription("/Root/Dst").GetPathDescription().GetTable().HasReplicationConfig());
    }

    Y_UNIT_TEST_TWIN(AttachIndexTable, ConcurrentRecovery) {
        TFeatureFlags flags;
        flags.SetEnableChangefeedsOnIndexTables(true);
        TEnv env(flags);
        auto table = TTestTableDescription{
            .Name = "Source",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };
        env.CreateTableWithIndex("/Root", *MakeTableDescription(table),
            "by_value", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);
        table.Name = "Dst";
        env.CreateTableWithIndex("/Root", *MakeTableDescription(table),
            "by_value", TVector<TString>{"value"}, NKikimrSchemeOp::EIndexTypeGlobal);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 1, TReplication::ETargetKind::Table,
            "/Root/Source", "/Root/Dst", EReplicationMode::ReadOnly, EConsistencyLevel::Row, false));
        PrepareAttachDst(env);
        UNIT_ASSERT(env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender())->Get()->IsSuccess());

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Source"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"), 1, 2, TReplication::ETargetKind::IndexTable,
            "/Root/Source/by_value/indexImplTable", "/Root/Dst/by_value/indexImplTable",
            EReplicationMode::ReadOnly, EConsistencyLevel::Row, false));
        PrepareAttachDst(env);
        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, NKikimrScheme::StatusSuccess, ev->Get()->Error);
        const auto desc = env.GetDescription("/Root/Dst/by_value/indexImplTable");
        TString error;
        UNIT_ASSERT_C(CheckReplicationConfig(desc.GetPathDescription().GetTable().GetReplicationConfig(),
            EReplicationMode::ReadOnly, EConsistencyLevel::Row, error), error);

        if (ConcurrentRecovery) {
            const auto pathId = env.GetPathId("/Root/Dst/by_value/indexImplTable");
            const auto databasePathId = env.GetPathId("/Root");
            // Recovery and an uncertain pipe retry can overlap on the same
            // index. Both replacement ALTERs must serialize and finish safely.
            for (ui32 i = 0; i < 2; ++i) {
                env.GetRuntime().Register(CreateDstCreator(
                    env.GetSender(),
                    pathId.OwnerId,
                    env.GetYdbProxy(),
                    "/Root",
                    databasePathId,
                    1,
                    2,
                    TReplication::ETargetKind::IndexTable,
                    "/Root/Source/by_value/indexImplTable",
                    "/Root/Dst/by_value/indexImplTable",
                    EReplicationMode::ReadOnly,
                    EConsistencyLevel::Row,
                    false,
                    pathId));
            }
            for (ui32 i = 0; i < 2; ++i) {
                const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
                UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstPathId, pathId);
            }
        }
    }

    Y_UNIT_TEST(AttachmentAlterRejectsDroppedIndexAfterRestart) {
        TEnv env;
        auto& runtime = env.GetRuntime();
        env.CreateTableWithIndex(
            "/Root",
            *MakeTableDescription(TTestTableDescription{
                .Name = "Dst",
                .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Uint32"}},
                .ReplicationConfig = Nothing(),
            }),
            "by_value",
            TVector<TString>{"value"},
            NKikimrSchemeOp::EIndexTypeGlobal);
        const auto pathId = env.GetPathId("/Root/Dst/by_value/indexImplTable");
        const auto drop = env.Send<TEvYdbProxy::TEvDropTableResponse>(
            env.GetYdbProxy(),
            new TEvYdbProxy::TEvDropTableRequest("/Root/Dst", NYdb::NTable::TDropTableSettings()));
        UNIT_ASSERT_C(drop->Get()->Result.IsSuccess(), drop->Get()->Result.GetIssues().ToString());

        const auto alterDropped = [&] {
            const auto allocated = env.Send<TEvTxUserProxy::TEvAllocateTxIdResult>(
                MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId());
            auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(
                allocated->Get()->TxId,
                pathId.OwnerId);
            auto& tx = *request->Record.AddTransaction();
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            tx.SetInternal(true);
            pathId.ToProto(tx.MutableAlterTable()->MutablePathId());
            FillReplicationConfig(
                *tx.MutableAlterTable()->MutableReplicationConfig(),
                EReplicationMode::ReadOnly,
                EConsistencyLevel::Row);
            const auto result = env.Send<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(
                pathId.OwnerId,
                std::move(request));
            const auto status = result->Get()->Record.GetStatus();
            // A scavenged path is empty; a retained tombstone is deleted.
            UNIT_ASSERT_C(
                status == NKikimrScheme::StatusPathDoesNotExist
                    || status == NKikimrScheme::StatusNameConflict,
                result->Get()->Record.ShortDebugString());
        };
        alterDropped();

        // Tombstone paths remain resolved after restart, while their table and
        // index metadata may already be gone. Reject them before metadata lookup.
        const auto lookupEdge = runtime.AllocateEdgeActor();
        const auto generation = [&] {
            runtime.Send(new IEventHandle(
                MakeStateStorageProxyID(),
                lookupEdge,
                new TEvStateStorage::TEvLookup(pathId.OwnerId, 0)));
            const auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvInfo>(lookupEdge);
            return info->Get()->Status == NKikimrProto::OK ? info->Get()->CurrentGeneration : 0;
        };
        const auto oldGeneration = generation();
        UNIT_ASSERT_GT(oldGeneration, 0);
        env.SendAsync(pathId.OwnerId, new TEvents::TEvPoisonPill());
        for (ui32 attempt = 0; attempt < 100 && generation() <= oldGeneration; ++attempt) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_GT(generation(), oldGeneration);
        InvalidateTabletResolverCache(runtime, pathId.OwnerId);
        alterDropped();
    }

    Y_UNIT_TEST(ColumnFamilies) {
        TEnv env;
        auto source = MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        });
        source->MutableColumns(1)->SetFamilyName("archive");
        auto* family = source->MutablePartitionConfig()->AddColumnFamilies();
        family->SetName("archive");
        family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
        env.CreateTable("/Root", *source);

        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            env.GetRuntime().Register(CreateDstCreator(
                env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
                "/Root", env.GetPathId("/Root"),
                1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table,
                "/Root/Table", "/Root/Replicated"));
            const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrScheme::StatusSuccess, result->Get()->Error);
        }

        const auto description = env.GetDescription("/Root/Replicated");
        const auto& table = description.GetPathDescription().GetTable();
        ui32 archiveId = 0;
        for (const auto& item : table.GetPartitionConfig().GetColumnFamilies()) {
            if (item.GetName() == "archive") {
                archiveId = item.GetId();
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(item.GetColumnCodec()),
                    static_cast<ui32>(NKikimrSchemeOp::ColumnCodecLZ4));
            }
        }
        UNIT_ASSERT(archiveId);
        for (const auto& column : table.GetColumns()) {
            if (column.GetName() == "value") {
                UNIT_ASSERT_VALUES_EQUAL(column.GetFamily(), archiveId);
            }
        }
    }

    Y_UNIT_TEST(UnnamedNonDefaultDestinationFamily) {
        TEnv env;
        const auto table = TTestTableDescription{
            .Name = "Src",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        };
        env.CreateTable("/Root", *MakeTableDescription(table));

        auto destination = table;
        destination.Name = "Dst";
        destination.ReplicationConfig = TTestTableDescription::TReplicationConfig::Default();
        auto description = MakeTableDescription(destination);
        description->MutableColumns(1)->SetFamily(1);
        description->MutablePartitionConfig()->AddColumnFamilies()->SetId(1);
        env.CreateTable("/Root", *description);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Src"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table,
            "/Root/Src", "/Root/Dst"));
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusSchemeError);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Error, "Unnamed non-default destination column family: id: 1");
    }

    Y_UNIT_TEST(ImplicitDefaultFamilyFromProfile) {
        TEnv env;

        NKikimrConfig::TTableProfilesConfig profiles;
        auto* policy = profiles.AddStoragePolicies();
        policy->SetName("default");
        auto* family = policy->AddColumnFamilies();
        family->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("test");
        family->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("test");
        family->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("test");
        auto* profile = profiles.AddTableProfiles();
        profile->SetName("default");
        profile->SetStoragePolicy("default");
        env.ConfigureTableProfiles(profiles);

        env.CreateTable("/Root", *MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}},
            .ReplicationConfig = Nothing(),
        }));

        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            env.GetRuntime().Register(CreateDstCreator(
                env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
                "/Root", env.GetPathId("/Root"),
                1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table,
                "/Root/Table", "/Root/Replicated"));
            const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrScheme::StatusSuccess, result->Get()->Error);
        }
    }

    Y_UNIT_TEST(WithIntermediateDir) {
        Basic("/Root/Dir/Replicated");
    }

    Y_UNIT_TEST(GlobalConsistency) {
        Basic("/Root/Replicated", EReplicationMode::ReadOnly, EConsistencyLevel::Global);
    }

    void WithIndex(const TString& replicatedPath, NKikimrSchemeOp::EIndexType indexType, bool enableAsyncIndexReplication = false) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableChangefeedsOnIndexTables(true);
        if (enableAsyncIndexReplication) {
            featureFlags.SetEnableAsyncIndexReplication(true);
        }
        TEnv env(featureFlags);
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        const auto tableDesc = TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Uint32"},
            },
            .ReplicationConfig = Nothing(),
        };

        const TString indexName = "index_by_value";

        env.CreateTableWithIndex("/Root", *MakeTableDescription(tableDesc),
             indexName, TVector<TString>{"value"}, indexType);
        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", replicatedPath
        ));
        {
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        }

        auto desc = env.GetDescription(replicatedPath);
        const auto& replicatedDesc = desc.GetPathDescription().GetTable();

        CheckTableReplica(tableDesc, replicatedDesc);

        if (indexType == NKikimrSchemeOp::EIndexTypeGlobalAsync && !enableAsyncIndexReplication) {
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.TableIndexesSize(), 0);
            return;
        }

        switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.TableIndexesSize(), 1);
            break;
        default:
            UNIT_ASSERT_VALUES_EQUAL(replicatedDesc.TableIndexesSize(), 0);
            return;
        }

        if (indexType != NKikimrSchemeOp::EIndexTypeGlobalAsync) {
            env.GetRuntime().Register(CreateDstCreator(
                env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
                "/Root", env.GetPathId("/Root"),
                1 /* rid */, 2 /* tid */, TReplication::ETargetKind::IndexTable,
                "/Root/Table/" + indexName + "/indexImplTable", replicatedPath + "/" + indexName + "/indexImplTable"
            ));
            auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);
        }

        {
            auto desc = env.GetDescription(replicatedPath + "/" + indexName);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetPathDescription().GetTableIndex().GetName(), indexName);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetPathDescription().GetTableIndex().GetType(), indexType);
        }

        {
            auto desc = env.GetDescription(replicatedPath + "/" + indexName + "/indexImplTable");
            Cerr << desc.DebugString() << Endl;
            const auto& indexTableDesc = desc.GetPathDescription().GetTable();
            UNIT_ASSERT_VALUES_EQUAL(indexTableDesc.KeyColumnNamesSize(), 2);
            if (indexType == NKikimrSchemeOp::EIndexTypeGlobalAsync) {
                UNIT_ASSERT(!indexTableDesc.HasReplicationConfig());
            }
        }
    }

    Y_UNIT_TEST(WithSyncIndex) {
        WithIndex("/Root/Replicated", NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(WithSyncIndexAndIntermediateDir) {
        WithIndex("/Root/Dir/Replicated", NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(WithAsyncIndex) {
        WithIndex("/Root/Replicated", NKikimrSchemeOp::EIndexTypeGlobalAsync, true);
    }

    Y_UNIT_TEST(WithAsyncIndexDisabled) {
        WithIndex("/Root/Replicated", NKikimrSchemeOp::EIndexTypeGlobalAsync);
    }

    Y_UNIT_TEST(SameOwner) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.ModifyOwner("/", "Root", "user@builtin");
        env.CreateTable("/Root", *MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        }));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto desc = env.GetDescription("/Root/Replicated");
        const auto& replicatedSelf = desc.GetPathDescription().GetSelf();
        UNIT_ASSERT_VALUES_EQUAL(replicatedSelf.GetOwner(), "user@builtin");
    }

    Y_UNIT_TEST(SamePartitionCount) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription({
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
            .UniformPartitions = 2,
        }));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Table"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSuccess);

        auto originalDesc = env.GetDescription("/Root/Table");
        const auto& originalTable = originalDesc.GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(originalTable.TablePartitionsSize(), 2);

        auto replicatedDesc = env.GetDescription("/Root/Replicated");
        const auto& replicatedTable = replicatedDesc.GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(originalTable.TablePartitionsSize(), replicatedTable.TablePartitionsSize());
    }

    Y_UNIT_TEST(NonExistentSrc) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Table", "/Root/Replicated"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrScheme::StatusSchemeError);
    }

    template <typename T>
    void ExistingDst(NKikimrScheme::EStatus status, const TString& error, T&& mod, const TTestTableDescription& desc) {
        auto changeName = [](const TTestTableDescription& desc, const TString& name) {
            auto copy = desc;
            copy.Name = name;
            return copy;
        };

        auto clearConfig = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig.Clear();
            return copy;
        };

        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(clearConfig(changeName(desc, "Src"))));
        env.CreateTable("/Root", *MakeTableDescription(mod(changeName(desc, "Dst"))));

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table, "/Root/Src", "/Root/Dst"
        ));

        auto ev = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, status);
        if (error) {
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Error, error);
        }
    }

    template <typename T>
    void ExistingDstFamily(const TString& error, T&& modify) {
        TEnv env;
        auto source = MakeTableDescription({
            .Name = "Src",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        });
        source->MutableColumns(1)->SetFamilyName("archive");
        auto* family = source->MutablePartitionConfig()->AddColumnFamilies();
        family->SetName("archive");
        family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
        auto* data = family->MutableStorageConfig()->MutableData();
        data->SetPreferredPoolKind("test");
        data->SetAllowOtherKinds(false);
        env.CreateTable("/Root", *source);

        auto destination = *source;
        destination.SetName("Dst");
        TTestTableDescription::TReplicationConfig::Default().SerializeTo(*destination.MutableReplicationConfig());
        modify(destination);
        env.CreateTable("/Root", destination);

        env.GetRuntime().Register(CreateDstCreator(
            env.GetSender(), env.GetSchemeshardId("/Root/Src"), env.GetYdbProxy(),
            "/Root", env.GetPathId("/Root"),
            1 /* rid */, 1 /* tid */, TReplication::ETargetKind::Table,
            "/Root/Src", "/Root/Dst"));
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusSchemeError);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Error, error);
    }

    Y_UNIT_TEST(ColumnFamilyCodecMismatch) {
        ExistingDstFamily("Column family codec mismatch", [](auto& destination) {
            destination.MutablePartitionConfig()->MutableColumnFamilies(0)->SetColumnCodec(NKikimrSchemeOp::ColumnCodecPlain);
        });
    }

    Y_UNIT_TEST(ColumnFamilyCacheModeMismatch) {
        ExistingDstFamily("Column family cache mode mismatch", [](auto& destination) {
            destination.MutablePartitionConfig()->MutableColumnFamilies(0)->SetColumnCacheMode(
                NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory);
        });
    }

    Y_UNIT_TEST(ColumnFamilyMediaMismatch) {
        ExistingDstFamily("Column family media mismatch", [](auto& destination) {
            destination.MutablePartitionConfig()->MutableColumnFamilies(0)
                ->MutableStorageConfig()->MutableData()->SetAllowOtherKinds(true);
        });
    }

    Y_UNIT_TEST(ColumnFamilyAssignmentMismatch) {
        ExistingDstFamily("Column family mismatch", [](auto& destination) {
            destination.MutableColumns(1)->ClearFamilyName();
        });
    }

    Y_UNIT_TEST(ExistingDst) {
        auto nop = [](const TTestTableDescription& desc) {
            return desc;
        };

        ExistingDst(NKikimrScheme::StatusSuccess, "", nop, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(KeyColumnsSizeMismatch) {
        auto addKeyColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.KeyColumns.push_back("value");
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Key columns size mismatch", addKeyColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(KeyColumnNameMismatch) {
        auto changeKeyColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.KeyColumns = {"value"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Key column name mismatch", changeKeyColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ColumnsSizeMismatch) {
        auto addColumn = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns.push_back({.Name = "extra", .Type = "Utf8"});
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Columns size mismatch", addColumn, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(CannotFindColumn) {
        auto changeColumnName = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns[1] = {.Name = "value2", .Type = "Utf8"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Cannot find column", changeColumnName, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ColumnTypeMismatch) {
        auto changeColumnType = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.Columns[1] = {.Name = "value", .Type = "Uint32"};
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Column type mismatch", changeColumnType, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(EmptyReplicationConfig) {
        auto clearConfig = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig.Clear();
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Empty replication config", clearConfig, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ReplicationModeMismatch) {
        auto changeMode = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig->Mode = TTestTableDescription::TReplicationConfig::MODE_NONE;
            copy.ReplicationConfig->ConsistencyLevel = TTestTableDescription::TReplicationConfig::CONSISTENCY_LEVEL_UNKNOWN;
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Replication mode mismatch", changeMode, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        });
    }

    Y_UNIT_TEST(ReplicationConsistencyLevelMismatch) {
        auto changeConsistency = [](const TTestTableDescription& desc) {
            auto copy = desc;
            copy.ReplicationConfig->ConsistencyLevel = TTestTableDescription::TReplicationConfig::CONSISTENCY_LEVEL_GLOBAL;
            return copy;
        };

        ExistingDst(NKikimrScheme::StatusSchemeError, "Replication consistency level mismatch", changeConsistency, TTestTableDescription{
            .Name = "Table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = TTestTableDescription::TReplicationConfig{
                .Mode = TTestTableDescription::TReplicationConfig::MODE_READ_ONLY,
                .ConsistencyLevel = TTestTableDescription::TReplicationConfig::CONSISTENCY_LEVEL_ROW,
            },
        });
    }
}

}
