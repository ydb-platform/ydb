#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/engine/kikimr_program_builder.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_schema.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

NKikimrScheme::TEvDescribeSchemeResult DescribeImpl(
    TTestActorRuntime& runtime, ui64 schemeShard, const TString& path)
{
    auto result = DescribePath(runtime, schemeShard, path, TDescribeOptionsBuilder()
        .SetShowPrivateTable(true).SetReturnPartitioningInfo(true).SetReturnPartitionConfig(true));
    TestDescribeResult(result, {NLs::IsTable});
    UNIT_ASSERT(result.GetPathDescription().GetTable().HasPartitionConfig());
    return result;
}

// Reproduce the metadata persisted by old SchemeShard versions without changing
// the live DataShard schemas. A subsequent split creates shards with shadow data.
NKikimrSchemeOp::TPartitionConfig PersistLegacyConfig(
    TTestActorRuntime& runtime, ui64 schemeShard, const NKikimrScheme::TEvDescribeSchemeResult& describe)
{
    auto config = describe.GetPathDescription().GetTable().GetPartitionConfig();
    config.SetShadowData(true);
    config.MutableCompactionPolicy()->SetKeepEraseMarkers(true);
    config.MutableCompactionPolicy()->SetDefaultTaskPriority(17);
    const auto& self = describe.GetPathDescription().GetSelf();
    const bool migrated = self.GetSchemeshardId() != schemeShard;
    TString program;
    {
        // This legacy Utf8 column stores binary protobuf data. Keep the literal
        // binary and retag its bytes at runtime, without UTF-8 literal validation.
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TTypeEnvironment env(alloc);
        NMiniKQL::TKikimrProgramBuilder builder(env, *runtime.GetAppData().FunctionRegistry);
        TVector<NMiniKQL::TRuntimeNode> keys;
        if (migrated) {
            keys.push_back(builder.NewDataLiteral<ui64>(self.GetSchemeshardId()));
        }
        keys.push_back(builder.NewDataLiteral<ui64>(self.GetPathId()));
        TVector<NScheme::TTypeInfo> keyTypes(keys.size(), NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        auto update = builder.GetUpdateRowBuilder();
        const TString serialized = config.SerializeAsString();
        update.SetColumn(migrated ? Schema::MigratedTables::PartitionConfig::ColumnId : Schema::Tables::PartitionConfig::ColumnId,
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            builder.ToString<true>(builder.NewDataLiteral<NUdf::EDataSlot::String>(
                NUdf::TStringRef(serialized.data(), serialized.size()))));
        const TTableId tableId(schemeShard, migrated ? Schema::MigratedTables::TableId : Schema::Tables::TableId);
        program = NMiniKQL::SerializeRuntimeNode(builder.Build(builder.AsList(
            builder.UpdateRow(tableId, keyTypes, keys, update)),
            NMiniKQL::TKikimrProgramBuilder::TBindFlags::DisableOptimization), env);
    }
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetBin(program);
    ForwardToTablet(runtime, schemeShard, runtime.AllocateEdgeActor(), request.Release());
    TAutoPtr<IEventHandle> handle;
    const auto* response = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Record.GetStatus(), NKikimrProto::OK, response->Record.DebugString());
    config.ClearShadowData();
    config.MutableCompactionPolicy()->SetKeepEraseMarkers(false);
    return config;
}

NKikimrScheme::TEvDescribeSchemeResult WaitForRepair(
    TTestActorRuntime& runtime, TTestEnv& env, ui64 schemeShard, const TString& path)
{
    NKikimrScheme::TEvDescribeSchemeResult describe;
    // Allow the maximum 60-second retry delay plus a small margin.
    for (unsigned attempt = 0; attempt < 65; ++attempt) {
        describe = DescribeImpl(runtime, schemeShard, path);
        const auto& description = describe.GetPathDescription();
        const auto& config = description.GetTable().GetPartitionConfig();
        if (!config.HasShadowData() && !config.GetCompactionPolicy().GetKeepEraseMarkers()
            && description.GetSelf().GetPathState() == NKikimrSchemeOp::EPathStateNoChanges)
        {
            return describe;
        }
        env.SimulateSleep(runtime, TDuration::Seconds(1));
    }
    UNIT_FAIL("Index implementation table was not repaired: " << describe.ShortDebugString());
    return describe;
}

TVector<NKikimrSchemeOp::TPartitionConfig> ReadShardConfigs(
    TTestActorRuntime& runtime, ui64 schemeShard, const TString& path)
{
    TVector<NKikimrSchemeOp::TPartitionConfig> result;
    const auto describe = DescribeImpl(runtime, schemeShard, path);
    for (const auto& partition : describe.GetPathDescription().GetTablePartitions()) {
        runtime.SendToPipe(partition.GetDatashardId(), runtime.AllocateEdgeActor(),
            new TEvDataShard::TEvGetInfoRequest(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        const auto* response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.UserTablesSize(), 1);
        result.push_back(response->Record.GetUserTables(0).GetDescription().GetPartitionConfig());
    }
    UNIT_ASSERT(!result.empty());
    return result;
}

void CheckShardConfigs(const TVector<NKikimrSchemeOp::TPartitionConfig>& configs) {
    for (const auto& config : configs) {
        UNIT_ASSERT_C(!config.HasShadowData(), config.ShortDebugString());
        UNIT_ASSERT_C(!config.GetCompactionPolicy().GetKeepEraseMarkers(), config.ShortDebugString());
    }
}

struct TRepairTest {
    TTestBasicRuntime Runtime;
    TTestEnv Env{Runtime};
    ui64 TxId = 100;

    TRepairTest(NAudit::TAuditLogBackends&& auditLogBackends = {})
        : Runtime(1, 1, false, std::move(auditLogBackends))
    {
        TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env.TestWaitNotification(Runtime, TxId);
        for (ui32 key = 1; key <= 3; ++key) {
            WriteRow(Runtime, ++TxId, "/MyRoot/Table", 0, key, Sprintf("value%u", key));
        }
    }

    static TString IndexPath(const TString& name = "Index") {
        return "/MyRoot/Table/" + name;
    }

    static TString ImplPath(const TString& name = "Index") {
        return IndexPath(name) + "/indexImplTable";
    }

    void BuildIndex(const TString& name = "Index",
        NKikimrSchemeOp::EIndexType type = NKikimrSchemeOp::EIndexTypeGlobalUnique)
    {
        TestBuildIndex(Runtime, ++TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table",
            TBuildIndexConfig{name, type, {"value"}, {}, {}});
        Env.TestWaitNotification(Runtime, TxId);
        const auto build = TestGetBuildIndex(Runtime, TTestTxConfig::SchemeShard, "/MyRoot", TxId);
        UNIT_ASSERT_VALUES_EQUAL(build.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
    }

    NKikimrScheme::TEvDescribeSchemeResult Describe(const TString& name = "Index") {
        return DescribeImpl(Runtime, TTestTxConfig::SchemeShard, ImplPath(name));
    }

    NKikimrSchemeOp::TPartitionConfig PersistLegacyFlags(const TString& name = "Index") {
        return PersistLegacyConfig(Runtime, TTestTxConfig::SchemeShard, Describe(name));
    }

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::SchemeShard, Runtime.AllocateEdgeActor());
    }

    NKikimrScheme::TEvDescribeSchemeResult Wait(const TString& name = "Index") {
        auto result = WaitForRepair(Runtime, Env, TTestTxConfig::SchemeShard, ImplPath(name));
        TestDescribeResult(DescribePrivatePath(Runtime, IndexPath(name)), {
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)
        });
        return result;
    }

    TVector<NKikimrSchemeOp::TPartitionConfig> ShardConfigs(const TString& name = "Index") {
        return ReadShardConfigs(Runtime, TTestTxConfig::SchemeShard, ImplPath(name));
    }

    void CheckRows(const TString& name = "Index") {
        UNIT_ASSERT_VALUES_EQUAL(CountRows(Runtime, "/MyRoot/Table"), 3);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(Runtime, ImplPath(name)), 3);
    }

    void Split(unsigned partition, const TString& boundary) {
        const auto describe = Describe();
        TestSplitTable(Runtime, ++TxId, ImplPath(), Sprintf(R"(
            SourceTabletId: %lu
            SplitBoundary { KeyPrefix { Tuple { Optional { Text: "%s" } } } }
        )", describe.GetPathDescription().GetTablePartitions(partition).GetDatashardId(), boundary.c_str()));
        Env.TestWaitNotification(Runtime, TxId);
    }

    void SendRepair(const TString& path, bool internal, NKikimrScheme::EStatus status,
        bool extraConfig = false)
    {
        const auto describe = DescribePrivatePath(Runtime, path);
        const auto& self = describe.GetPathDescription().GetSelf();
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        request->Record.SetTxId(++TxId);
        auto& tx = *request->Record.AddTransaction();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpRepairIndexShadowData);
        tx.SetInternal(internal);
        auto& pathId = *tx.MutableAlterTable()->MutablePathId();
        pathId.SetOwnerId(self.GetSchemeshardId());
        pathId.SetLocalId(self.GetPathId());
        if (extraConfig) {
            tx.MutableAlterTable()->MutablePartitionConfig()->SetShadowData(false);
        }
        AsyncSend(Runtime, TTestTxConfig::SchemeShard, request.Release());
        TestModificationResult(Runtime, TxId, status);
    }
};

}

Y_UNIT_TEST_SUITE(IndexShadowDataRepairTest) {
    Y_UNIT_TEST(StartupRepairsLegacyFlags) {
        std::vector<std::string> auditLines;
        TRepairTest test(Tests::CreateTestAuditLogBackends(auditLines));
        test.BuildIndex();
        const auto expected = test.PersistLegacyFlags();
        CheckShardConfigs(test.ShardConfigs());
        test.Reboot();
        const auto repaired = test.Wait();
        UNIT_ASSERT_VALUES_EQUAL(repaired.GetPathDescription().GetTable().GetPartitionConfig().SerializeAsString(),
            expected.SerializeAsString());
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();

        test.Reboot();
        const auto restarted = test.Wait();
        UNIT_ASSERT_VALUES_EQUAL(restarted.GetPathDescription().GetTable().GetTableSchemaVersion(),
            repaired.GetPathDescription().GetTable().GetTableSchemaVersion());
        CheckShardConfigs(test.ShardConfigs());

        test.Env.SimulateSleep(test.Runtime, TDuration::MilliSeconds(1));
        UNIT_ASSERT_C(!auditLines.empty(), "Ordinary DDL must produce audit records");
        for (const auto& line : auditLines) {
            UNIT_ASSERT_C(line.find("REPAIR INDEX SHADOW DATA") == std::string::npos, line);
        }
    }

    Y_UNIT_TEST(RepairRefreshesCachedIndexSchemaVersion) {
        TRepairTest test;
        test.BuildIndex();
        test.PersistLegacyFlags();
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> repairs(test.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTransaction(0).GetOperationType()
                == NKikimrSchemeOp::ESchemeOpRepairIndexShadowData;
        });
        test.Reboot();
        test.Runtime.WaitFor("startup repair request", [&] { return !repairs.empty(); });
        const ui64 repairTxId = repairs.front()->Get()->Record.GetTxId();

        auto cachedImplVersion = [&] {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto request = MakeHolder<TNavigate>();
            auto& query = request->ResultSet.emplace_back();
            query.Path = SplitPath(TRepairTest::IndexPath());
            query.Operation = TNavigate::OpList;
            query.ShowPrivatePath = true;
            query.SyncVersion = true;
            const auto sender = test.Runtime.AllocateEdgeActor();
            test.Runtime.Send(new IEventHandle(MakeSchemeCacheID(), sender,
                new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())));
            auto response = test.Runtime.GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(sender);
            const auto& result = response->Get()->Request;
            const auto& entry = result->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, TNavigate::EStatus::Ok);
            UNIT_ASSERT(entry.ListNodeEntry);
            UNIT_ASSERT_VALUES_EQUAL(entry.ListNodeEntry->Children.size(), 1);
            const auto& child = entry.ListNodeEntry->Children.front();
            UNIT_ASSERT_VALUES_EQUAL(child.Name, "indexImplTable");
            return child.SchemaVersion;
        };
        const auto before = cachedImplVersion();
        UNIT_ASSERT_VALUES_EQUAL(before, test.Describe().GetPathDescription().GetTable().GetTableSchemaVersion());

        repairs.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, repairTxId);
        const auto after = test.Wait().GetPathDescription().GetTable().GetTableSchemaVersion();
        UNIT_ASSERT_VALUES_EQUAL(after, before + 1);
        // KQP uses the implementation schema version from the cached index's children.
        UNIT_ASSERT_VALUES_EQUAL(cachedImplVersion(), after);
    }

    Y_UNIT_TEST(RepairsSplitShards) {
        TRepairTest test;
        test.BuildIndex();
        test.Split(0, "value2");
        CheckShardConfigs(test.ShardConfigs());
        test.PersistLegacyFlags();
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> repairs(test.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTransaction(0).GetOperationType()
                == NKikimrSchemeOp::ESchemeOpRepairIndexShadowData;
        });
        test.Reboot();
        test.Runtime.WaitFor("startup repair request", [&] { return !repairs.empty(); });
        test.Split(1, "value3");
        const auto configs = test.ShardConfigs();
        UNIT_ASSERT_VALUES_EQUAL(configs.size(), 3);
        unsigned shadowShards = 0;
        for (const auto& config : configs) {
            shadowShards += config.GetShadowData();
        }
        UNIT_ASSERT_VALUES_EQUAL(shadowShards, 2);
        test.CheckRows();

        repairs.Stop().Unblock();
        const auto repaired = test.Wait();
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();
        for (const auto& partition : repaired.GetPathDescription().GetTablePartitions()) {
            RebootTablet(test.Runtime, partition.GetDatashardId(), test.Runtime.AllocateEdgeActor());
        }
        test.Reboot();
        test.Wait();
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();
    }

    Y_UNIT_TEST(BusyIndexDoesNotBlockOtherRepairs) {
        TRepairTest test;
        test.BuildIndex("First");
        test.BuildIndex("Second");
        test.PersistLegacyFlags("First");
        test.PersistLegacyFlags("Second");
        const ui64 lockId = ++test.TxId;
        TestLock(test.Runtime, lockId, TRepairTest::IndexPath("First"), "indexImplTable");
        test.Env.TestWaitNotification(test.Runtime, lockId);
        test.Reboot();
        test.Wait("Second");
        UNIT_ASSERT(test.Describe("First").GetPathDescription().GetTable().GetPartitionConfig().GetShadowData());
        TestUnlock(test.Runtime, ++test.TxId, lockId, TRepairTest::IndexPath("First"), "indexImplTable");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        test.Wait("First");
        CheckShardConfigs(test.ShardConfigs("First"));
        CheckShardConfigs(test.ShardConfigs("Second"));
    }

    Y_UNIT_TEST(RejectsInappropriateRequests) {
        TRepairTest test;
        test.BuildIndex();
        test.BuildIndex("Ordinary", NKikimrSchemeOp::EIndexTypeGlobal);
        test.SendRepair(TRepairTest::ImplPath(), false, NKikimrScheme::StatusInvalidParameter);
        test.SendRepair(TRepairTest::ImplPath(), true, NKikimrScheme::StatusInvalidParameter, true);
        test.SendRepair(TRepairTest::ImplPath("Ordinary"), true, NKikimrScheme::StatusInvalidParameter);
        test.SendRepair("/MyRoot/Table", true, NKikimrScheme::StatusInvalidParameter);
        test.SendRepair(TRepairTest::ImplPath(), true, NKikimrScheme::StatusAlreadyExists);
    }

    Y_UNIT_TEST(RestartResumesAcceptedRepair) {
        TRepairTest test;
        test.BuildIndex();
        test.PersistLegacyFlags();
        TBlockEvents<TEvDataShard::TEvProposeTransaction> proposals(test.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return false;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
            return tx.HasAlterTable() && tx.GetAlterTable().GetPartitionConfig().HasShadowData()
                && !tx.GetAlterTable().GetPartitionConfig().GetShadowData();
        });
        test.Reboot();
        test.Runtime.WaitFor("durable repair proposal", [&] { return !proposals.empty(); });
        test.Reboot();
        proposals.Stop().Unblock();
        test.Wait();
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();
    }

    Y_UNIT_TEST_FLAG(StopsWhenSchemeShardDies, BeforeBootstrap) {
        TRepairTest test;
        test.BuildIndex();
        test.PersistLegacyFlags();
        const ui64 lockId = ++test.TxId;
        TestLock(test.Runtime, lockId, TRepairTest::IndexPath(), "indexImplTable");
        test.Env.TestWaitNotification(test.Runtime, lockId);

        TActorId worker;
        TBlockEvents<TEvents::TEvBootstrap> bootstraps(test.Runtime, [&](const auto& ev) {
            if (!worker && test.Runtime.FindActorName(ev->Recipient).Contains("SHADOW_DATA_REPAIR")) {
                worker = ev->Recipient;
                return true;
            }
            return false;
        });
        TEvents::TEvWakeup::TPtr wakeup;
        auto wakeupObserver = test.Runtime.AddObserver<TEvents::TEvWakeup>([&](auto& ev) {
            if (ev->Recipient == worker) {
                wakeup = std::move(ev);
            }
        });
        TBlockEvents<TEvents::TEvPoisonPill> poison(test.Runtime, [&](const auto& ev) {
            return ev->Recipient == worker;
        });
        test.Reboot();
        test.Runtime.WaitFor("repairer bootstrap", [&] { return !bootstraps.empty(); });
        const auto owner = bootstraps.front()->Sender;
        if (!BeforeBootstrap) {
            bootstraps.Stop().Unblock();
            test.Runtime.WaitFor("busy repairer retry", [&] { return bool(wakeup); });
        }

        test.Reboot();
        test.Runtime.WaitFor("repairer poison", [&] { return !poison.empty(); });
        UNIT_ASSERT(!test.Runtime.FindActor(owner));
        UNIT_ASSERT(test.Runtime.FindActor(worker));
        // Deliver an earlier event while the dead owner's poison is still blocked.
        if (BeforeBootstrap) {
            bootstraps.Stop().Unblock();
        } else {
            wakeupObserver.Remove();
            test.Runtime.Send(wakeup.Release(), 0, true);
        }
        test.Runtime.WaitFor("orphaned repairer to stop", [&] { return !test.Runtime.FindActor(worker); },
            TDuration::Seconds(1));
        poison.Stop().Unblock();

        TestUnlock(test.Runtime, ++test.TxId, lockId, TRepairTest::IndexPath(), "indexImplTable");
        test.Env.TestWaitNotification(test.Runtime, test.TxId);
        test.Wait();
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();
    }

    Y_UNIT_TEST(RecoveredBuildFinishesBeforeRepair) {
        TRepairTest test;
        TBlockEvents<TEvDataShard::TEvValidateUniqueIndexRequest> validations(test.Runtime);
        const ui64 buildId = ++test.TxId;
        TestBuildIndex(test.Runtime, buildId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table",
            TBuildIndexConfig{"Index", NKikimrSchemeOp::EIndexTypeGlobalUnique, {"value"}, {}, {}});
        test.Runtime.WaitFor("unique index validation", [&] { return !validations.empty(); });
        test.PersistLegacyFlags();
        unsigned repairRequests = 0;
        auto observer = test.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](const auto& ev) {
            if (ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpRepairIndexShadowData) {
                ++repairRequests;
            }
        });
        test.Reboot();
        test.Env.SimulateSleep(test.Runtime, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(repairRequests, 0);
        UNIT_ASSERT(test.Describe().GetPathDescription().GetTable().GetPartitionConfig().GetShadowData());
        UNIT_ASSERT_VALUES_UNEQUAL(TestGetBuildIndex(test.Runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId)
            .GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
        validations.Stop().Unblock();
        test.Env.TestWaitNotification(test.Runtime, buildId);
        test.Wait();
        UNIT_ASSERT(repairRequests > 0);
        CheckShardConfigs(test.ShardConfigs());
        test.CheckRows();
    }

    Y_UNIT_TEST(RepairsReplicaImplementationTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(CreateIndexedTableRequest(100, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                ReplicationConfig {
                    Mode: REPLICATION_MODE_READ_ONLY
                    ConsistencyLevel: CONSISTENCY_LEVEL_GLOBAL
                }
            }
            IndexDescription {
                Name: "Index"
                Type: EIndexTypeGlobalUnique
                KeyColumnNames: ["value"]
                IndexImplTableDescriptions {
                    ReplicationConfig {
                        Mode: REPLICATION_MODE_READ_ONLY
                        ConsistencyLevel: CONSISTENCY_LEVEL_GLOBAL
                    }
                }
            }
        )")));
        TestModificationResult(runtime, 100);
        env.TestWaitNotification(runtime, 100);
        const auto before = DescribeImpl(runtime, TTestTxConfig::SchemeShard, TRepairTest::ImplPath());
        const auto expected = PersistLegacyConfig(runtime, TTestTxConfig::SchemeShard, before);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        const auto repaired = WaitForRepair(runtime, env, TTestTxConfig::SchemeShard, TRepairTest::ImplPath());
        const auto& table = repaired.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionConfig().SerializeAsString(), expected.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(table.GetReplicationConfig().SerializeAsString(),
            before.GetPathDescription().GetTable().GetReplicationConfig().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(table.GetTableSchemaVersion(),
            before.GetPathDescription().GetTable().GetTableSchemaVersion() + 1);
        TestDescribeResult(repaired, {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY)
        });
        CheckShardConfigs(ReadShardConfigs(runtime, TTestTxConfig::SchemeShard, TRepairTest::ImplPath()));
    }

    Y_UNIT_TEST(RepairsMigratedImplementationTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "Tenant")");
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "Tenant"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/Tenant", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "Index"
                Type: EIndexTypeGlobalUnique
                KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Tenant/Table/Index"), {
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)
        });
        TestUpgradeSubDomain(runtime, ++txId, "/MyRoot", "Tenant");
        env.TestWaitNotification(runtime, txId);
        TestUpgradeSubDomainDecision(runtime, ++txId, "/MyRoot", "Tenant", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);
        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Tenant"), {
            NLs::IsExternalSubDomain("Tenant"), NLs::ExtractTenantSchemeshard(&tenantSchemeShard)
        });
        const TString indexPath = "/MyRoot/Tenant/Table/Index";
        const TString implPath = indexPath + "/indexImplTable";
        const auto legacy = DescribeImpl(runtime, tenantSchemeShard, implPath);
        UNIT_ASSERT_VALUES_EQUAL(legacy.GetPathDescription().GetSelf().GetSchemeshardId(), TTestTxConfig::SchemeShard);
        UNIT_ASSERT_VALUES_UNEQUAL(tenantSchemeShard, TTestTxConfig::SchemeShard);
        const auto expected = PersistLegacyConfig(runtime, tenantSchemeShard, legacy);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        const auto repaired = WaitForRepair(runtime, env, tenantSchemeShard, implPath);
        UNIT_ASSERT_VALUES_EQUAL(repaired.GetPathDescription().GetTable().GetPartitionConfig().SerializeAsString(),
            expected.SerializeAsString());
        TestDescribeResult(DescribePrivatePath(runtime, tenantSchemeShard, indexPath), {
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)
        });
        CheckShardConfigs(ReadShardConfigs(runtime, tenantSchemeShard, implPath));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tenantSchemeShard, implPath), 0);
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        const auto restarted = WaitForRepair(runtime, env, tenantSchemeShard, implPath);
        UNIT_ASSERT_VALUES_EQUAL(restarted.GetPathDescription().GetTable().GetTableSchemaVersion(),
            repaired.GetPathDescription().GetTable().GetTableSchemaVersion());
    }
}
