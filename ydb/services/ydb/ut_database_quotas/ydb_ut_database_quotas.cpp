#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;


Y_UNIT_TEST_SUITE(TDatabaseQuotas) {

NKikimrConfig::TStoragePolicy CreateDefaultStoragePolicy(const TString& poolKind) {
    NKikimrSchemeOp::TStorageConfig config;
    config.MutableSysLog()->SetPreferredPoolKind(poolKind);
    config.MutableLog()->SetPreferredPoolKind(poolKind);
    config.MutableData()->SetPreferredPoolKind(poolKind);

    NKikimrConfig::TStoragePolicy policy;
    auto* family = policy.AddColumnFamilies();
    *family->MutableStorageConfig() = std::move(config);

    return policy;
}

NKikimrConfig::TTableProfilesConfig CreateDefaultTableProfilesConfig(const TString& poolKind) {
    constexpr const char* name = "default";
    NKikimrConfig::TTableProfilesConfig profiles;
    {
        auto* policy = profiles.AddStoragePolicies();
        *policy = CreateDefaultStoragePolicy(poolKind);
        policy->SetName(name);
    }
    {
        auto* profile = profiles.AddTableProfiles();
        profile->SetName(name);
        profile->SetStoragePolicy(name);
    }

    return profiles;
}

void CompactTableAndCheckResult(TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId) {
    auto compactionResult = CompactTable(runtime, shardId, tableId);
    UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
}

ui64 RunSchemeTx(
        TTestActorRuntimeBase& runtime,
        THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
        TActorId sender = {},
        bool viaActorSystem = false,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus
            = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress
) {
    if (!sender) {
        sender = runtime.AllocateEdgeActor();
    }

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

    return ev->Get()->Record.GetTxId();
}

THolder<TEvTxUserProxy::TEvProposeTransaction> SchemeTxTemplate(
        NKikimrSchemeOp::EOperationType type,
        const TString& workingDir
) {
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(type);
    tx.SetWorkingDir(workingDir);

    return request;
}

void WaitTxNotification(TServer::TPtr server, TActorId sender, ui64 txId) {
    auto& runtime = *server->GetRuntime();
    auto& settings = server->GetSettings();

    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void CreateSubdomain(TServer::TPtr server,
                     TActorId sender,
                     const TString& workingDir,
                     const NKikimrSubDomains::TSubDomainSettings& settings
) {
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateSubDomain, workingDir);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    *tx.MutableSubDomain() = settings;

    WaitTxNotification(server, sender, RunSchemeTx(*server->GetRuntime(), std::move(request), sender));
}

void AlterSubdomain(TServer::TPtr server,
                    TActorId sender,
                    const TString& workingDir,
                    const NKikimrSubDomains::TSubDomainSettings& settings
) {
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterSubDomain, workingDir);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    *tx.MutableSubDomain() = settings;

    // Don't wait for completion. It won't happen until the resources (i. e. dynamic nodes) are provided.
    RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

Y_UNIT_TEST(DisableWritesToDatabase) {
    TPortManager portManager;
    ui16 mbusPort = portManager.GetPort();
    TServerSettings serverSettings(mbusPort);
    serverSettings
        .SetUseRealThreads(false)
        .SetDynamicNodeCount(1)
        .SetDomainName("Root");

    TStoragePools storagePools = {{"/Root:ssd", "ssd"}, {"/Root:hdd", "hdd"}};
    for (const auto& pool : storagePools) {
        serverSettings.AddStoragePool(pool.GetKind(), pool.GetName());
    }
    NKikimrConfig::TAppConfig appConfig;
    // default table profile with a storage policy is needed to be able to create a table with families
    *appConfig.MutableTableProfilesConfig() = CreateDefaultTableProfilesConfig(storagePools[0].GetKind());
    appConfig.MutableDataShardConfig()->SetStatsReportIntervalSeconds(0);
    serverSettings.SetAppConfig(appConfig);

    TServer::TPtr server = new TServer(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
    NDataShard::gDbStatsDataSizeResolution = 1;
    NDataShard::gDbStatsRowCountResolution = 1;

    TString tenant = "tenant";
    TString tenantPath = Sprintf("/Root/%s", tenant.c_str());

    CreateSubdomain(server, sender, "/Root", GetSubDomainDeclarationSetting(tenant));

    auto subdomainSettings = GetSubDomainDefaultSetting(tenant, storagePools);
    auto* parsedQuotas = subdomainSettings.MutableDatabaseQuotas();
    constexpr const char* quotas = R"(
        storage_quotas {
            unit_kind: "hdd"
            data_size_hard_quota: 1
        }
    )";
    UNIT_ASSERT_C(NProtoBuf::TextFormat::ParseFromString(quotas, parsedQuotas), quotas);
    AlterSubdomain(server, sender, "/Root", subdomainSettings);

    TTenants tenants(server);
    tenants.Run(tenantPath, 1);

    TString table = Sprintf("%s/table", tenantPath.c_str());
    ExecSQL(server, sender, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8 FAMILY hdd,
                    PRIMARY KEY (Key),
                    FAMILY default (
                        DATA = "ssd",
                        COMPRESSION = "off"
                    ),
                    FAMILY hdd (
                        DATA = "hdd",
                        COMPRESSION = "lz4"
                    )
                );
            )", table.c_str()
        ), false
    );

    auto upsert = [&](
        const TString& table, const TString& row,
        Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS
    ) {
        ExecSQL(server, sender, Sprintf(R"(
                    UPSERT INTO `%s` (Key, Value) VALUES (%s);
                )", table.c_str(), row.c_str()
            ), true, expectedStatus
        );
    };

    upsert(table, "1u, \"Foo\"");

    auto shards = GetTableShards(server, sender, table);
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
    auto& datashard = shards[0];
    auto tableId = ResolveTableId(server, sender, table);
    // Compaction is a must. Table stats are missing channels usage statistics until the table is compacted at least once.
    CompactTableAndCheckResult(runtime, datashard, tableId);

    auto checkDatabaseState = [&](const TString& database, bool expectedQuotaExceeded) {
        auto schemeEntry = Navigate(
            runtime, sender, database, NSchemeCache::TSchemeCacheNavigate::EOp::OpPath
        )->ResultSet.at(0);
        UNIT_ASSERT_C(schemeEntry.DomainDescription, schemeEntry.ToString());
        auto& domainDescription = schemeEntry.DomainDescription->Description;
        bool quotaExceeded = domainDescription.GetDomainState().GetDiskQuotaExceeded();
        UNIT_ASSERT_VALUES_EQUAL_C(quotaExceeded, expectedQuotaExceeded, domainDescription.DebugString());
    };

    // try upsert when the feature flag is enabled
    {
        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(true);
        WaitTableStats(runtime, datashard, [](const NKikimrTableStats::TTableStats& stats) {
            return stats.GetPartCount() >= 1;
        });
        upsert(table, "2u, \"Bar\"", Ydb::StatusIds::UNAVAILABLE);
        checkDatabaseState(tenantPath, true);
    }

    // try upsert when the feature flag is disabled
    {
        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(false);
        WaitTableStats(runtime, datashard, [](const NKikimrTableStats::TTableStats& stats) {
            return stats.GetPartCount() >= 1;
        });
        upsert(table, "2u, \"Bar\"");
        checkDatabaseState(tenantPath, false);
    }
}

}

} // namespace NKikimr

