#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>  // for TSchemeShard
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>  // for MakeTestBlob
#include <ydb/core/scheme_types/scheme_type_info.h>  // for NTypeIds and TTypeInfo


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

NLs::TCheckFunc LsCheckSubDomainParamsAfterAlter(const TString name,
                                              ui64 descrVersion = 2,
                                              ui64 pathId = 2,
                                              TVector<ui64> coordinators = {TTestTxConfig::FakeHiveTablets},
                                              TVector<ui64> mediators = {TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2})
{
    return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
        NLs::PathExist(record);
        NLs::PathIdEqual(pathId)(record);
        NLs::IsSubDomain(name)(record);
        NLs::SubDomainVersion(descrVersion)(record);
        NLs::DomainKey(pathId, TTestTxConfig::SchemeShard)(record);
        NLs::DomainCoordinators(coordinators)(record);
        NLs::DomainMediators(mediators)(record);
    };
}

NLs::TCheckFunc LsCheckSubDomainParamsInCommonCase(const TString name,
                                        ui64 pathId = 2, ui64 schemeshardId = TTestTxConfig::SchemeShard,
                                        ui64 createTxId = 100, ui64 createStep = 5000001,
                                        ui64 parentPathId = 1, ui64 descrVersion = 1,
                                        ui32 planResolution = 50, ui32 timeCastBucketsPerMediator = 2,
                                        TVector<ui64> coordinators = {TTestTxConfig::FakeHiveTablets},
                                        TVector<ui64> mediators = {TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2})
{
    Y_UNUSED(createTxId);
    Y_UNUSED(schemeshardId);
    Y_UNUSED(createStep);
    Y_UNUSED(parentPathId);

    return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
        NLs::PathExist(record);
        NLs::IsSubDomain(name)(record);
        NLs::SubDomainVersion(descrVersion)(record);
        NLs::DomainSettings(planResolution, timeCastBucketsPerMediator)(record);
        NLs::DomainKey(pathId, TTestTxConfig::SchemeShard)(record);
        NLs::DomainCoordinators(coordinators)(record);
        NLs::DomainMediators(mediators)(record);
    };
}

NLs::TCheckFunc LsCheckSubDomainParamsInMassiveCase(const TString name = "",
                                        ui64 pathId = 2, ui64 schemeshardId = TTestTxConfig::SchemeShard,
                                        ui64 createTxId = 100, ui64 createStep = 5000001,
                                        ui64 parentPathId = 1, ui64 descrVersion = 1,
                                        ui32 planResolution = 10, ui32 timeCastBucketsPerMediator = 2,
                                        TVector<ui64> coordinators = {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 1, TTestTxConfig::FakeHiveTablets + 2},
                                        TVector<ui64> mediators = {TTestTxConfig::FakeHiveTablets + 3, TTestTxConfig::FakeHiveTablets + 4, TTestTxConfig::FakeHiveTablets + 5}) {

    Y_UNUSED(createTxId);
    Y_UNUSED(schemeshardId);
    Y_UNUSED(createStep);
    Y_UNUSED(parentPathId);

    return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
        NLs::PathExist(record);
        NLs::IsSubDomain(name)(record);
        NLs::SubDomainVersion(descrVersion)(record);
        NLs::DomainSettings(planResolution, timeCastBucketsPerMediator)(record);
        NLs::DomainKey(pathId, TTestTxConfig::SchemeShard)(record);
        NLs::DomainCoordinators(coordinators)(record);
        NLs::DomainMediators(mediators)(record);
    };
}

NLs::TCheckFunc LsCheckDiskQuotaExceeded(
    bool expectExceeded = true,
    const TString& debugHint = ""
) {
    return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
        auto& desc = record.GetPathDescription().GetDomainDescription();
        UNIT_ASSERT_VALUES_EQUAL_C(
            desc.GetDomainState().GetDiskQuotaExceeded(),
            expectExceeded,
            debugHint << ", subdomain's disk space usage:\n" << desc.GetDiskSpaceUsage().DebugString()
        );
    };
}

enum class EDiskUsageStatus {
    AboveHardQuota,
    InBetween,
    BelowSoftQuota,
};

template <>
void Out<EDiskUsageStatus>(IOutputStream& o, EDiskUsageStatus status) {
    o << static_cast<int>(status);
}

struct TQuotasPair {
    ui64 HardQuota = 0;
    ui64 SoftQuota = 0;
};

TMap<TString, EDiskUsageStatus> CheckStoragePoolsQuotas(const THashMap<TString, ui64>& storagePoolsUsage,
                                                        const THashMap<TString, TQuotasPair>& storagePoolsQuotas
) {
    TMap<TString, EDiskUsageStatus> exceeders;
    for (const auto& [poolKind, totalSize] : storagePoolsUsage) {
        if (const auto* quota = storagePoolsQuotas.FindPtr(poolKind)) {
            if (quota->HardQuota && totalSize > quota->HardQuota) {
                exceeders.emplace(poolKind, EDiskUsageStatus::AboveHardQuota);
            } else if (quota->SoftQuota && totalSize >= quota->SoftQuota) {
                exceeders.emplace(poolKind, EDiskUsageStatus::InBetween);
            }
        }
    }
    return exceeders;
}

ui64 GetTotalDiskUsage(const NKikimrSubDomains::TDiskSpaceUsage& usage) {
    const auto& tables = usage.GetTables();
    const auto& topics = usage.GetTopics();
    return tables.GetTotalSize() + topics.GetAccountSize();
}

constexpr const char* EntireDatabaseTag = "entire_database";

NLs::TCheckFunc LsCheckDiskQuotaExceeded(
    const TMap<TString, EDiskUsageStatus>& expectedExceeders,
    bool enableSeparateQuotasFeatureFlag,
    const TString& debugHint = ""
) {
    return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
        auto& desc = record.GetPathDescription().GetDomainDescription();
        UNIT_ASSERT_VALUES_EQUAL_C(
            desc.GetDomainState().GetDiskQuotaExceeded(),
            !expectedExceeders.empty(),
            debugHint << ", subdomain's disk space usage:\n" << desc.GetDiskSpaceUsage().DebugString()
        );

        if (!expectedExceeders.empty()) {
            const auto& receivedUsage = desc.GetDiskSpaceUsage();
            THashMap<TString, ui64> parsedUsage;
            for (const auto& poolUsage : receivedUsage.GetStoragePoolsUsage()) {
                parsedUsage.emplace(poolUsage.GetPoolKind(),
                                    poolUsage.GetDataSize() + poolUsage.GetIndexSize()
                );
            }
            UNIT_ASSERT_C(!parsedUsage.contains(EntireDatabaseTag), EntireDatabaseTag << " is reserved");
            parsedUsage.emplace(EntireDatabaseTag, GetTotalDiskUsage(receivedUsage));

            const auto& receivedQuotas = desc.GetDatabaseQuotas();
            THashMap<TString, TQuotasPair> parsedQuotas;
            for (const auto& poolQuotas : receivedQuotas.storage_quotas()) {
                parsedQuotas.emplace(poolQuotas.unit_kind(),
                                     TQuotasPair{poolQuotas.data_size_hard_quota(),
                                                 poolQuotas.data_size_soft_quota()
                                     }
                );
            }
            UNIT_ASSERT_C(!parsedQuotas.contains(EntireDatabaseTag), EntireDatabaseTag << " is reserved");
            parsedQuotas.emplace(EntireDatabaseTag,
                                 TQuotasPair{receivedQuotas.data_size_hard_quota(),
                                             receivedQuotas.data_size_soft_quota()
                                 }
            );

            TMap<TString, EDiskUsageStatus> exceeders = CheckStoragePoolsQuotas(parsedUsage, parsedQuotas);
            if (enableSeparateQuotasFeatureFlag) {
                // ignore the status of the overall quota
                exceeders.erase(EntireDatabaseTag);
            } else {
                // ignore the statuses of the separate storage pool quotas
                TMap<TString, EDiskUsageStatus> onlyOverallStatus;
                if (auto* overallStatus = exceeders.FindPtr(EntireDatabaseTag)) {
                    onlyOverallStatus.emplace(EntireDatabaseTag, *overallStatus);
                }
                std::swap(exceeders, onlyOverallStatus);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(exceeders, expectedExceeders,
                debugHint << ", subdomain's disk space usage:\n" << desc.GetDiskSpaceUsage().DebugString()
            );
        }
    };
}

void CheckQuotaExceedance(TTestActorRuntime& runtime,
                          ui64 schemeShard,
                          const TString& pathToSubdomain,
                          bool expectExceeded,
                          const TString& debugHint = ""
) {
    TestDescribeResult(DescribePath(runtime, schemeShard, pathToSubdomain), {
        LsCheckDiskQuotaExceeded(expectExceeded, debugHint)
    });
}

void CheckQuotaExceedance(TTestActorRuntime& runtime,
                          ui64 schemeShard,
                          const TString& pathToSubdomain,
                          const TMap<TString, EDiskUsageStatus>& expectedExceeders,
                          const TString& debugHint = ""
) {
    TestDescribeResult(DescribePath(runtime, schemeShard, pathToSubdomain), {
        LsCheckDiskQuotaExceeded(
            expectedExceeders,
            runtime.GetAppData().FeatureFlags.GetEnableSeparateDiskSpaceQuotas(),
            debugHint
        )
    });
}

TTableId ResolveTableId(TTestActorRuntime& runtime, const TString& path) {
    const auto response = Navigate(runtime, path);
    return response->ResultSet.at(0).TableId;
}

NKikimrTxDataShard::TEvPeriodicTableStats WaitTableStats(TTestActorRuntime& runtime, ui64 datashardId, ui64 minPartCount = 0) {
    NKikimrTxDataShard::TEvPeriodicTableStats stats;
    bool captured = false;

    auto observer = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>([&](const auto& event) {
            const auto& record = event->Get()->Record;
            if (record.GetDatashardId() == datashardId && record.GetTableStats().GetPartCount() >= minPartCount) {
                stats = record;
                captured = true;
            }
        }
    );

    for (int i = 0; i < 5 && !captured; ++i) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() { return captured; };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
    }

    observer.Remove();

    UNIT_ASSERT(captured);

    return stats;
}

void CompactTableAndCheckResult(TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId) {
    const auto compactionResult = CompactTable(runtime, shardId, tableId);
    UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
}

Y_UNIT_TEST_SUITE(TSchemeShardSubDomainTest) {
    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::PathExist});
    }

    Y_UNIT_TEST(RmDir) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, {100, 101});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist});

        TestRmDir(runtime, txId++, "/MyRoot", "USER_0", {NKikimrScheme::StatusPathIsNotDirectory});
    }

    Y_UNIT_TEST(CreateAndWait) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        AsyncMkDir(runtime, txId++, "MyRoot", "dir");
        TestCreateSubDomain(runtime, txId++,  "/MyRoot/dir",
                            "StoragePools { "
                            "  Name: \"/dc-1/users/tenant-1:hdd\" "
                            "  Kind: \"hdd\" "
                            "} "
                            "StoragePools { "
                            "  Name: \"/dc-1/users/tenant-1:hdd-1\" "
                            "  Kind: \"hdd-1\" "
                            "} "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, {100, 101});
        expectedDomainPaths += 2;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(LS) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsInCommonCase("USER_0", subdomainPathId),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(ConcurrentCreateSubDomainAndDescribe) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 10 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathVersionOneOf({2, 3}),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(6)});

        env.TestWaitNotification(runtime, txId - 1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsInMassiveCase("USER_0", subdomainPathId),
                            NLs::PathVersionEqual(3)});
    }


    Y_UNIT_TEST(CreateWithoutPlanResolution) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        env.TestWaitNotification(runtime, 100);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithoutTimeCastBuckets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        env.TestWaitNotification(runtime, 100);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithNoEqualName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::PathExist, NLs::IsSubDomain("USER_0")});

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"",
                            {NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});

        //########

        TestCreateTable(runtime, txId++, "/MyRoot",
                        "Name: \"USER_1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_1\"",
                            {NKikimrScheme::StatusNameConflict});

        //########

        TestMkDir(runtime, txId++, "/MyRoot", "USER_2");

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_2\"",
                            {NKikimrScheme::StatusNameConflict});

        //########

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_3\"");

        TestMkDir(runtime, txId++, "/MyRoot", "USER_3", {NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists});

        env.TestWaitNotification(runtime, {100, 101, 102, 103, 104, 105, 106, 107});

        TestMkDir(runtime, txId++, "/MyRoot", "USER_3", {NKikimrScheme::StatusAlreadyExists});


        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::IsSubDomain("USER_0")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
            {NLs::PathExist, NLs::Finished, NLs::NotInSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_2"),
            {NLs::PathExist, NLs::Finished, NLs::NotInSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_3"),
            {NLs::IsSubDomain("USER_3")});
    }

    Y_UNIT_TEST(CreateItemsInsideSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        TestMkDir(runtime, txId++, "/MyRoot/USER_0", "dir_0");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0/dir_0",
                        "Name: \"table_1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        env.TestWaitNotification(runtime, {100, 101, 102, 103});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(8),
                            NLs::PathsInsideDomain(3),
                            NLs::ShardsInsideDomain(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0"),
                           {NLs::InSubdomain,
                            NLs::PathVersionEqual(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0/table_1"),
                           {NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});
    }

    Y_UNIT_TEST(CreateItemsInsideSubdomainWithStoragePools) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\""
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-1\""
                            "  Kind: \"hdd-1\""
                            "}"
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-2\""
                            "  Kind: \"hdd-2\""
                            "}");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        TestMkDir(runtime, txId++, "/MyRoot/USER_0", "dir_0");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0/dir_0",
                        "Name: \"table_1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        env.TestWaitNotification(runtime, {100, 101, 102, 103});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0"),
                            NLs::SubdomainWithNoEmptyStoragePools,
                            NLs::PathsInsideDomain(3),
                            NLs::ShardsInsideDomain(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::InSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0"),
                           {NLs::InSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0/table_1"),
                           {NLs::InSubdomain});
    }

    Y_UNIT_TEST(CreateSubDomainWithoutTablets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(CreateSubDomainWithoutSomeTablets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Coordinators: 1 "
                            "Name: \"USER_1\"",
                            {NKikimrScheme::StatusInvalidParameter});

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Mediators: 1 "
                            "Name: \"USER_2\"",
                            {NKikimrScheme::StatusInvalidParameter});

        env.TestWaitNotification(runtime, {100, 101});

        TestLs(runtime, "/MyRoot/USER_1", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_2", false, NLs::PathNotExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(CreateSubDomainWithoutTabletsThenMkDir) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestMkDir(runtime, txId++, "/MyRoot/USER_0", "MyDir");

        env.TestWaitNotification(runtime, 101);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(0)});
        TestLs(runtime, "/MyRoot/USER_0/MyDir", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateSubDomainWithoutTabletsThenDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");

        env.TestWaitNotification(runtime, 101);
        expectedDomainPaths -= 1;

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
    }

    Y_UNIT_TEST(CreateSubDomainWithoutTabletsThenForceDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));

        TestForceDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");

        env.TestWaitNotification(runtime, 101);
        expectedDomainPaths -= 1;

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist, NLs::PathsInsideDomain(expectedDomainPaths), NLs::ShardsInsideDomain(0)});
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
    }

    Y_UNIT_TEST(CreateSubDomainsInSeparateDir) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestMkDir(runtime, txId++, "/MyRoot", "SubDomains");

        TestCreateSubDomain(runtime, txId++,  "/MyRoot/SubDomains",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestCreateSubDomain(runtime, txId++,  "/MyRoot/SubDomains",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_1\"");

        env.TestWaitNotification(runtime, {100, 101, 102});
        expectedDomainPaths += 3;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDomains/USER_0"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDomains/USER_1"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDomains"),
                           {NLs::PathExist,
                            NLs::NotInSubdomain,
                            NLs::PathVersionEqual(7),
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(SimultaneousCreateDelete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        AsyncCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        AsyncDropSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        TestModificationResult(runtime, txId-1);
        ui64 whatHappened = TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId, txId-1});

        if (whatHappened == NKikimrScheme::StatusAccepted) {
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::NotInSubdomain,
                                NLs::PathsInsideDomain(expectedDomainPaths)});
        } else {
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::NotInSubdomain,
                                NLs::PathsInsideDomain(expectedDomainPaths + 1),
                                NLs::ShardsInsideDomain(0)});
        }
    }

    Y_UNIT_TEST(SimultaneousCreateForceDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestForceDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");

        env.TestWaitNotification(runtime, {100, 101});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::NotInSubdomain,
                            NLs::PathVersionOneOf({13, 14}), // it is 13 if drop simultaneous with create
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomainOneOf({0, 1, 2, 3})});
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
    }

    Y_UNIT_TEST(ForceDropTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        env.TestWaitNotification(runtime, txId);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist});

        AsyncForceDropSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        AsyncForceDropSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");

        SkipModificationReply(runtime, 2);
        env.TestWaitNotification(runtime, {txId-1, txId});
        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(SimultaneousCreateForceDropTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        AsyncForceDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        AsyncForceDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");

        env.TestWaitNotification(runtime, {100, 101, 102});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomainOneOf({0, 1, 2, 3, 4, 5, 6})});
    }

    Y_UNIT_TEST(CopyRejects) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                            StoragePools {
                                Name: "/dc-1/users/tenant-1:hdd"
                                Kind: "hdd"
                                }
                            PlanResolution: 50
                            Coordinators: 1
                            Mediators: 1
                            TimeCastBucketsPerMediator: 2
                            Name: "USER_0"
                        )");

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_1\"");

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0",
                        "Name: \"src\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
            Name: "ex_blobs"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "String" }
            KeyColumnNames: ["key"]
            PartitionConfig {
                ColumnFamilies {
                    Id: 0
                    StorageConfig {
                        SysLog {
                            PreferredPoolKind: "hdd"
                        }
                        Log {
                            PreferredPoolKind: "hdd"
                        }
                        Data {
                            PreferredPoolKind: "hdd"
                        }
                        External {
                            PreferredPoolKind: "hdd"
                        }
                        ExternalThreshold: 524288
                    }
                }
            }
        )");

        env.TestWaitNotification(runtime, {100, 101, 102, 103, 104});

        TestCopyTable(runtime, ++txId, "/MyRoot/USER_1", "dst", "/MyRoot/USER_0/src", NKikimrScheme::StatusInvalidParameter);
        TestCopyTable(runtime, ++txId, "/MyRoot", "dst", "/MyRoot/USER_0/src", NKikimrScheme::StatusInvalidParameter);
        TestCopyTable(runtime, ++txId, "/MyRoot/USER_0", "ex_blobs_copy", "/MyRoot/USER_0/ex_blobs", NKikimrScheme::StatusPreconditionFailed);
    }

    Y_UNIT_TEST(ConsistentCopyRejects) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_1\"");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_1",
                        "Name: \"table\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        env.TestWaitNotification(runtime, {100, 101, 102, 103});
        expectedDomainPaths += 2;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1/table"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::NotInSubdomain,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestConsistentCopyTables(runtime, txId++, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/USER_0/table"
                DstPath: "/MyRoot/USER_1/dst"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/USER_1/table"
                DstPath: "/MyRoot/USER_0/dst"
            })", {NKikimrScheme::StatusInvalidParameter});

        TestConsistentCopyTables(runtime, txId++, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/USER_0/table"
                DstPath: "/MyRoot/USER_0/dst"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/USER_1/table"
                DstPath: "/MyRoot/USER_1/dst"
            })", {NKikimrScheme::StatusInvalidParameter});


        TestConsistentCopyTables(runtime, txId++, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/USER_0/table"
                DstPath: "/MyRoot/USER_0/dst"
            })");

        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dst"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::InSubdomain,
                            NLs::PathVersionEqual(7),
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(4)});
    }

    Y_UNIT_TEST(SimultaneousCreateTableForceDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "PlanResolution: 10 "
                             "Coordinators: 3 "
                             "Mediators: 3 "
                             "TimeCastBucketsPerMediator: 2 "
                             "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {NLs::PathExist});

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );
        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, {101, 102});
        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomainOneOf({0, 1, 2, 3, 4, 5, 6})});

        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
    }

    Y_UNIT_TEST(SimultaneousCreateTenantTableForceDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "PlanResolution: 10 "
                             "Coordinators: 3 "
                             "Mediators: 3 "
                             "TimeCastBucketsPerMediator: 2 "
                             "Name: \"USER_0\"");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");

        env.TestWaitNotification(runtime, {100, 101, 102});
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_0/table_0", false, NLs::PathNotExist);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathVersionOneOf({13, 14}), // version 13 if deletion is simultaneous with creation
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
    }

    Y_UNIT_TEST(SimultaneousCreateTenantTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "PlanResolution: 10 "
                             "Coordinators: 3 "
                             "Mediators: 3 "
                             "TimeCastBucketsPerMediator: 2 "
                             "Name: \"USER_0\"");

        AsyncCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        env.TestWaitNotification(runtime, {100, 101});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsInMassiveCase("USER_0", subdomainPathId),
                            NLs::PathVersionEqual(4),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(7)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::InSubdomain});
    }

    Y_UNIT_TEST(SimultaneousDeclareAndCreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "Name: \"USER_0\"");

        AsyncCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );
        TestModificationResult(runtime, txId - 2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId - 1, NKikimrScheme::StatusNameConflict);

        env.TestWaitNotification(runtime, {txId - 2, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(SimultaneousDefineAndCreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, txId - 1);

        AsyncAlterSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 10 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        AsyncCreateTable(runtime, txId++, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );
        TestModificationResult(runtime, txId - 2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId - 1, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId - 2, txId - 1});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsAfterAlter("USER_0", 2, subdomainPathId),
                            NLs::PathVersionEqual(6),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::PathExist});
    }

    Y_UNIT_TEST(SimultaneousCreateTenantDirTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "PlanResolution: 10 "
                             "Coordinators: 3 "
                             "Mediators: 3 "
                             "TimeCastBucketsPerMediator: 2 "
                             "Name: \"USER_0\"");

        AsyncMkDir(runtime, txId++, "/MyRoot/USER_0", "dir");

        AsyncCreateTable(runtime, txId++, "/MyRoot/USER_0/dir",
                        "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );

        env.TestWaitNotification(runtime, {100, 101, 102});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsInMassiveCase("USER_0", subdomainPathId),
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(7)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_0"),
                           {NLs::InSubdomain});

        TestForceDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, 103);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(CreateForceDropSolomon) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot", "PlanResolution: 50 "
                                                         "Coordinators: 1 "
                                                         "Mediators: 1 "
                                                         "TimeCastBucketsPerMediator: 2 "
                                                         "Name: \"USER_0\" ");

        TestCreateSolomon(runtime, txId++, "/MyRoot/USER_0", "Name: \"Solomon\" "
                                                             "PartitionCount: 40 ");
        env.TestWaitNotification(runtime, {txId-2, txId-1});

        TestLs(runtime, "/MyRoot/USER_0/Solomon", false, NLs::InSubdomain);

        // Already exists
        TestCreateSolomon(runtime, txId++, "/MyRoot/USER_0", "Name: \"Solomon\" "
                                                             "PartitionCount: 40 ",
            {NKikimrScheme::StatusAlreadyExists});

        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId-1);

        TestLs(runtime, "/MyRoot/USER_0/Solomon", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateDropSolomon) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot", "PlanResolution: 50 "
                                                         "Coordinators: 1 "
                                                         "Mediators: 1 "
                                                         "TimeCastBucketsPerMediator: 2 "
                                                         "Name: \"USER_0\" ");

        TLocalPathId solomonPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSolomon(runtime, txId++, "/MyRoot/USER_0", "Name: \"Solomon\" "
                                                             "PartitionCount: 40 ");
        env.TestWaitNotification(runtime, {txId-2, txId-1});
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", solomonPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SolomonVolumes", "PathId", solomonPathId));

        TestLs(runtime, "/MyRoot", false);
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::InSubdomain);
        TestLs(runtime, "/MyRoot/USER_0/Solomon", false, NLs::InSubdomain);

        // Already exists
        TestCreateSolomon(runtime, txId++, "/MyRoot/USER_0", "Name: \"Solomon\" "
                                                             "PartitionCount: 40 ",
            {NKikimrScheme::StatusAlreadyExists});

        TestDropSolomon(runtime, txId++, "/MyRoot/USER_0", "Solomon");
        env.TestWaitNotification(runtime, txId-1);
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SolomonVolumes", "PathId", solomonPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", solomonPathId));

        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId-1);
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));

        TestLs(runtime, "/MyRoot/USER_0/Solomon", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateDropNbs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\" "
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-1\""
                            "  Kind: \"storage-pool-number-1\""
                            "}"
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-2\""
                            "  Kind: \"storage-pool-number-2\""
                            "}");

        TLocalPathId bsVolumePathId = GetNextLocalPathId(runtime, txId);

        TestCreateBlockStoreVolume(runtime, txId++, "/MyRoot/USER_0",
                                   "Name: \"BSVolume\" "
                                   "VolumeConfig: { "
                                   " ExplicitChannelProfiles { PoolKind: \"storage-pool-number-1\"}"
                                   " ExplicitChannelProfiles { PoolKind: \"storage-pool-number-1\"}"
                                   " ExplicitChannelProfiles { PoolKind: \"storage-pool-number-1\"}"
                                   " ExplicitChannelProfiles { PoolKind: \"storage-pool-number-2\"}"
                                   " BlockSize: 4096 Partitions { BlockCount: 16 } } ");

        env.TestWaitNotification(runtime, {txId-2, txId-1});
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", bsVolumePathId));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "BlockStoreVolumes", "PathId", bsVolumePathId));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::SubdomainWithNoEmptyStoragePools});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/BSVolume"),
                           {NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(4)});

        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId-1);
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "BlockStoreVolumes", "PathId", bsVolumePathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", bsVolumePathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId));

        TestLs(runtime, "/MyRoot/USER_0/BSVolume", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateAlterNbsChannels) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, /* nchannels */ 3 + 4);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\" "
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-1\""
                            "  Kind: \"storage-pool-number-1\""
                            "}"
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-2\""
                            "  Kind: \"storage-pool-number-2\""
                            "}");

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        for (ui32 i = 0; i < 3; ++i) {
            auto* ecp = vc.AddExplicitChannelProfiles();
            ecp->SetPoolKind("storage-pool-number-1");
            ecp->SetSize(111);
        }

        {
            auto* ecp = vc.AddExplicitChannelProfiles();
            ecp->SetPoolKind("storage-pool-number-2");
            ecp->SetSize(222);
        }

        TestCreateBlockStoreVolume(runtime, txId++, "/MyRoot/USER_0", vdescr.DebugString());

        env.TestWaitNotification(runtime, {txId-2, txId-1});

        const TFakeHiveTabletInfo* tablet = nullptr;
        for (auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == TTabletTypes::BlockStorePartition) {
                tablet = &kv.second;
                break;
            }
        }
        UNIT_ASSERT(tablet != nullptr);

        // Tablet should be created with 3 + 1 storage pool bound channels
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[0].GetStoragePoolName(),
            "name_USER_0_kind_hdd-1"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[0].GetSize(), 111);
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[1].GetStoragePoolName(),
            "name_USER_0_kind_hdd-1"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[1].GetSize(), 111);
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[2].GetStoragePoolName(),
            "name_USER_0_kind_hdd-1"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[2].GetSize(), 111);
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[3].GetStoragePoolName(),
            "name_USER_0_kind_hdd-2"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[3].GetSize(), 222);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                            {NLs::SubdomainWithNoEmptyStoragePools});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/BSVolume"),
                            {NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(4)});

        vc.ClearBlockSize();
        vc.ClearPartitions();
        vc.SetVersion(1);
        vc.MutableExplicitChannelProfiles(3)->SetPoolKind("storage-pool-number-1");
        vc.MutableExplicitChannelProfiles(3)->SetSize(333);
        vc.SetPoolKindChangeAllowed(true);
        TestAlterBlockStoreVolume(runtime, txId++, "/MyRoot/USER_0", vdescr.DebugString());

        env.TestWaitNotification(runtime, txId-1);

        // Storage pool and size for channel 3 should be changed
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[3].GetStoragePoolName(),
            "name_USER_0_kind_hdd-1"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[3].GetSize(), 333);

        vc.ClearPoolKindChangeAllowed();
        vc.SetVersion(2);
        {
            auto* ecp = vc.AddExplicitChannelProfiles();
            ecp->SetPoolKind("storage-pool-number-2");
            ecp->SetSize(444);
        }
        TestAlterBlockStoreVolume(runtime, txId++, "/MyRoot/USER_0", vdescr.DebugString());

        env.TestWaitNotification(runtime, txId-1);

        // Tablet should be recreated with 3 + 2 storage pool bound channels
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(
            tablet->BoundChannels[4].GetStoragePoolName(),
            "name_USER_0_kind_hdd-2"
        );
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels[4].GetSize(), 444);

        vc.SetVersion(3);
        for (ui32 i = 0; i < 251; ++i) {
            vc.AddExplicitChannelProfiles()->SetPoolKind("storage-pool-number-2");
        }
        TestAlterBlockStoreVolume(runtime, txId++, "/MyRoot/USER_0", vdescr.DebugString());

        env.TestWaitNotification(runtime, txId-1);

        // Tablet should be recreated with 3 + 253 storage pool bound channels
        // Note that channel profile does not have that many entries
        UNIT_ASSERT_VALUES_EQUAL(tablet->BoundChannels.size(), 256u);

        TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId-1);

        TestLs(runtime, "/MyRoot/USER_0/BSVolume", false, NLs::PathNotExist);
        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(Restart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(RestartAtInFly) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(Delete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, txId - 1);
        expectedDomainPaths += 1;

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathExist);

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId - 1);
        expectedDomainPaths -= 1;

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomainOneOf({0, 1, 2, 3})});

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(DeleteAdd) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist});

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, 101);
        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 102);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(DeleteAndRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 2 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");

        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }

        env.TestWaitNotification(runtime, 101);
        expectedDomainPaths -= 1;

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);

        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }

        TestLs(runtime, "/MyRoot/USER_0", false, NLs::PathNotExist);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(RedefineErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);

        const auto describeResult = DescribePath(runtime, "/MyRoot/USER_0");
        const auto subDomainPathId = describeResult.GetPathId();
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsInCommonCase("USER_0", subDomainPathId),
                            NLs::PathVersionEqual(3)});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 2 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 102);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsAfterAlter("USER_0", 2, subDomainPathId),
                            NLs::PathVersionEqual(4)});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 10 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-1\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 104);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::StoragePoolsEqual({"pool-1", "pool-2", "pool-hdd-1"}),
                            NLs::PathVersionEqual(5)});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-2\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-1\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "StoragePools {"
                           "  Name: \"pool-hdd-2\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 106);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::StoragePoolsEqual({"pool-1", "pool-2", "pool-hdd-1", "pool-hdd-2"}),
                            NLs::PathVersionEqual(6)});

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 107);

        TestAlterSubDomain(runtime, txId++, "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-1\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "StoragePools {"
                           "  Name: \"pool-hdd-2\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 108);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::StoragePoolsEqual({"pool-1", "pool-2", "pool-hdd-1", "pool-hdd-2"}),
                            NLs::PathVersionEqual(8)});
    }

    Y_UNIT_TEST(SimultaneousDeclare) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, {100, 101});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
            {NLs::Finished, NLs::PathVersionEqual(3)});
    }


    Y_UNIT_TEST(SimultaneousDefine) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathVersionEqual(3)});

        AsyncAlterSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        AsyncAlterSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, {100, 101});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::SubDomainVersion(2), NLs::PathVersionEqual(4)});
    }

    Y_UNIT_TEST(SimultaneousDeclareAndDefine) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateSubDomain(runtime, txId++,  "/MyRoot",
                             "Name: \"USER_0\"");
        AsyncAlterSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 2 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"");
        TestModificationResult(runtime, 100, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, 101, NKikimrScheme::StatusMultipleModifications);

        env.TestWaitNotification(runtime, {100, 101});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0")});
    }

    Y_UNIT_TEST(DeclareAndDelete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0")});

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, 101);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DeclareAndForbidTableInside) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0")});

        TestMkDir(runtime, txId++, "/MyRoot/USER_0", "dir");

        TestCreateTable(runtime, txId++, "/MyRoot/USER_0/dir",
                        "Name: \"table_0\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusNameConflict});
    }

    Y_UNIT_TEST(DeclareDefineAndDelete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);

        const auto describeResult = DescribePath(runtime, "/MyRoot/USER_0");
        const auto subDomainPathId = describeResult.GetPathId();
        TestDescribeResult(describeResult, {NLs::IsSubDomain("USER_0")});
        TestLs(runtime, "/MyRoot", false);

        TestAlterSubDomain(runtime, txId++,  "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 101);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsAfterAlter("USER_0", 2, subDomainPathId)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist});

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, 102);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(Redefine) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TLocalPathId subdomainPathId = GetNextLocalPathId(runtime, txId);

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestAlterSubDomain(runtime, txId++,  "/MyRoot",
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 2 "
                           "TimeCastBucketsPerMediator: 2 "
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 101);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckSubDomainParamsAfterAlter("USER_0", 2, subdomainPathId),
                            NLs::PathVersionEqual(4),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestAlterSubDomain(runtime, txId++,  "/MyRoot",
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-1\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "StoragePools {"
                           "  Name: \"pool-hdd-2\""
                           "  Kind: \"hdd-2\""
                           "}"
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 102);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::StoragePoolsEqual({"pool-1", "pool-2", "pool-hdd-1", "pool-hdd-2"}),
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestAlterSubDomain(runtime, txId++,  "/MyRoot",
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools {"
                           "  Name: \"pool-hdd-1\""
                           "  Kind: \"hdd-1\""
                           "}"
                           "StoragePools {"
                           "  Name: \"pool-hdd-2\""
                           "  Kind: \"hdd-2\""
                           "}"
                           "StoragePools {"
                           "  Name: \"pool-hdd-3\""
                           "  Kind: \"hdd-2\""
                           "}"
                           "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, 103);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::StoragePoolsEqual({"pool-1", "pool-2", "pool-hdd-1", "pool-hdd-2", "pool-hdd-3"}),
                            NLs::PathVersionEqual(6),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        TestDropSubDomain(runtime, txId++,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, 104);
        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(SetSchemeLimits) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TSchemeLimits lowLimits;
        lowLimits.MaxPaths = 3;
        lowLimits.MaxShards = 3;
        lowLimits.MaxPQPartitions = 300;

        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist
                            , NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards)});

        TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\""
                            " DatabaseQuotas {"
                            "    data_stream_shards_quota: 3"
                            "}");
        env.TestWaitNotification(runtime, 100);
        expectedDomainPaths += 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist
                            , NLs::PathVersionEqual(3)
                            , NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions)
                            , NLs::PathsInsideDomain(0)
                            , NLs::ShardsInsideDomain(2)
                            , NLs::DatabaseQuotas(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist
                            , NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions)
                            , NLs::PathsInsideDomain(expectedDomainPaths)
                            , NLs::ShardsInsideDomain(0)
                            , NLs::DatabaseQuotas(0)});
    }

    Y_UNIT_TEST(SchemeLimitsRejects) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TSchemeLimits lowLimits;
        lowLimits.MaxDepth = 4;
        lowLimits.MaxPaths = 3;
        lowLimits.MaxChildrenInDir = 3;
        lowLimits.MaxAclBytesSize = 25;
        lowLimits.MaxTableColumns = 3;
        lowLimits.MaxTableColumnNameLength = 10;
        lowLimits.MaxTableKeyColumns = 1;
        lowLimits.MaxShards = 6;
        lowLimits.MaxShardsInPath = 4;
        lowLimits.MaxPQPartitions = 20;

        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions)});

        //create subdomain
        {
            TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                                "PlanResolution: 50 "
                                "Coordinators: 1 "
                                "Mediators: 1 "
                                "TimeCastBucketsPerMediator: 2 "
                                "Name: \"USER_0\""
                                " DatabaseQuotas {"
                                "    data_stream_shards_quota: 2"
                                "    data_stream_reserved_storage_quota: 200000"
                                "}");
            env.TestWaitNotification(runtime, txId - 1);
            expectedDomainPaths += 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(3),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2),
                                NLs::DatabaseQuotas(2)});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
        }

        //create nodes inside, depth limit
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            TestMkDir(runtime, txId++, "/MyRoot/USER_0/1", "2");
            TestMkDir(runtime, txId++, "/MyRoot/USER_0/1/2", "3", {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, {txId - 1, txId - 2, txId -3});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(6),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(2),
                                NLs::ShardsInsideDomain(2)});

            //create nodes inside, paths limit
            TestMkDir(runtime, txId++, "/MyRoot/USER_0/1", "3");
            TestMkDir(runtime, txId++, "/MyRoot/USER_0/1", "4", {NKikimrScheme::StatusResourceExhausted});
            env.TestWaitNotification(runtime, {txId - 1, txId - 2});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(6),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions),
                                NLs::PathsInsideDomain(3),
                                NLs::ShardsInsideDomain(2),
                                NLs::DatabaseQuotas(2)});
        }

        //clean
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(8),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }

        //create tables, shards limit
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "2"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 3
                )");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "3"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 2
                )", {NKikimrScheme::StatusResourceExhausted});
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "4"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 1
                )");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "5"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 1
                )",  {NKikimrScheme::StatusResourceExhausted});
        }

        //clean
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(12),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }


        //create tables, paths shards limit
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "2"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 1
                )");
            env.TestWaitNotification(runtime, {txId - 1, txId - 2});

            ui64 dataShardId = (ui64) -1;
            auto extractShards = [&dataShardId] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                UNIT_ASSERT_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);
                const auto& pathDescr = record.GetPathDescription();
                const auto& tableDest = pathDescr.GetTablePartitions();
                UNIT_ASSERT_EQUAL(tableDest.size(), 1);
                dataShardId = tableDest.begin()->GetDatashardId();
            };
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1/2", true),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(3),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(2),
                                NLs::ShardsInsideDomain(3),
                                extractShards});

            TestSplitTable(runtime, txId++, "/MyRoot/USER_0/1/2", Sprintf(R"(
                            SourceTabletId: %lu
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Uint64: 1000000 } }
                                }
                            } )", dataShardId));

            env.TestWaitNotification(runtime, txId - 1);

            env.TestWaitTabletDeletion(runtime, dataShardId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1/2"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(4),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(2),
                                NLs::ShardsInsideDomain(4)});

            TestSplitTable(runtime, txId++, "/MyRoot/USER_0/1/2", Sprintf(R"(
                            SourceTabletId: %lu
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Uint64: 500000 } }
                                }
                            } )", dataShardId + 1));
            TestSplitTable(runtime, txId++, "/MyRoot/USER_0/1/2", Sprintf(R"(
                            SourceTabletId: %lu
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Uint64: 2000000 } }
                                }
                            } )", dataShardId + 2), {NKikimrScheme::StatusResourceExhausted});
            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1/2"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(4),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(2),
                                NLs::ShardsInsideDomainOneOf({5, 6})});

            env.TestWaitTabletDeletion(runtime, dataShardId + 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1/2"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(5),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(2),
                                NLs::ShardsInsideDomain(5)});

            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "3"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 2
                )", {NKikimrScheme::StatusResourceExhausted});
        }

        //clear
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(17),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }

        //create tables, column limits
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            env.TestWaitNotification(runtime, txId - 1);

            // MaxTableColumns
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "2"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value0" Type: "Utf8" }
                            Columns { Name: "Value1" Type: "Utf8" }
                            Columns { Name: "Value2" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                )", {NKikimrScheme::StatusSchemeError});

            // MaxTableColumnNameLength
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "3"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "VeryLongColumnName" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                )", {NKikimrScheme::StatusSchemeError});

            // MaxTableKeyColumns
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "4"
                            Columns { Name: "RowId0" Type: "Uint64" }
                            Columns { Name: "RowId1" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId0", "RowId1"]
                )", {NKikimrScheme::StatusSchemeError});
        }

        //clear
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(21),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }

        //create dirs, acl size limit
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            env.TestWaitNotification(runtime, txId - 1);

            NACLib::TDiffACL tinyACL;
            tinyACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user0@builtin");
            TestModifyACL(runtime, txId++, "/MyRoot/USER_0", "1", tinyACL.SerializeAsString(), "user0@builtin");

            NACLib::TDiffACL hugeACL;
            for (ui32 i : xrange(100)) {
                hugeACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, Sprintf("user%" PRIu32 "@builtin", i));
            }
            TestModifyACL(runtime, txId++, "/MyRoot/USER_0", "1", hugeACL.SerializeAsString(), "user0@builtin",
                          NKikimrScheme::StatusInvalidParameter);
        }

        //clear
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(26),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }

        //create tables, consistent copy targets limit
        lowLimits.MaxPaths = 5;
        lowLimits.MaxChildrenInDir = 4;
        lowLimits.MaxConsistentCopyTargets = 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "2"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 1
                )");
            TestCreateTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                            Name: "3"
                            Columns { Name: "RowId" Type: "Uint64" }
                            Columns { Name: "Value" Type: "Utf8" }
                            KeyColumnNames: ["RowId"]
                            UniformPartitionsCount: 1
                )");
            env.TestWaitNotification(runtime, {txId - 1, txId - 2, txId - 3});

            TestConsistentCopyTables(runtime, txId++, "/", R"(
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/USER_0/1/2"
                    DstPath: "/MyRoot/USER_0/1/12"
                }
                CopyTableDescriptions {
                    SrcPath: "/MyRoot/USER_0/1/3"
                    DstPath: "/MyRoot/USER_0/1/13"
                })", {NKikimrScheme::StatusInvalidParameter});
        }

        //clear
        {
            auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/1"));
            TestForceDropUnsafe(runtime, txId++, dirVer.PathId.LocalPathId);
            env.TestWaitNotification(runtime, txId - 1);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(31),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
        }

        //databaseQuotas limits
        {
            // Stream shards(partitions) limit is 2. Trying to create 3.
            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 3
                            PartitionPerTablet: 2
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10  WriteSpeedInBytesPerSecond : 1000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(31),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});

            // Stream reserved storage limit is 200000. Trying to reserve 200001.
            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 1
                            PartitionPerTablet: 2
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 200001} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(31),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});

            // Stream reserved storage limit is 200000. Trying to reserve 100000 - fit in it!

            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 1
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )");

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(33),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(4)});

            // Stream reserved storage limit is 200000. Trying to reserve 200000 - fit in it!
            TestAlterPQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 2
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )");

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(33),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(5)});

            // Stream reserved storage limit is 200000. Trying to reserve 20002 - do not fit in it!
            TestAlterPQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 2
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100001} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(33),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(5)});


        }

        //clear subdomain
        {
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
            TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId - 1);
            expectedDomainPaths -= 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
        }
    }

    Y_UNIT_TEST(ColumnSchemeLimitsRejects) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits lowLimits;
        lowLimits.MaxDepth = 4;
        lowLimits.MaxPaths = 3;
        lowLimits.MaxChildrenInDir = 4;
        lowLimits.MaxAclBytesSize = 25;
        lowLimits.MaxTableColumns = 3;
        lowLimits.MaxColumnTableColumns = 3;
        lowLimits.MaxTableColumnNameLength = 10;
        lowLimits.MaxTableKeyColumns = 1;
        lowLimits.MaxShards = 6;
        lowLimits.MaxShardsInPath = 4;
        lowLimits.MaxPQPartitions = 20;


        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards, lowLimits.MaxPQPartitions)});

        {
            TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                                "PlanResolution: 50 "
                                "Coordinators: 1 "
                                "Mediators: 1 "
                                "TimeCastBucketsPerMediator: 2 "
                                "Name: \"USER_0\""
                                " DatabaseQuotas {"
                                "    data_stream_shards_quota: 2"
                                "    data_stream_reserved_storage_quota: 200000"
                                "}");

        }

        //create column tables, column limits
        {
            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "C");
            env.TestWaitNotification(runtime, txId - 1);

            // MaxColumnTableColumns
            TestCreateColumnTable(runtime, txId++, "/MyRoot/USER_0/C", R"(
                            Name: "C2"
                            ColumnShardCount: 1
                            Schema {
                                Columns { Name: "RowId" Type: "Uint64", NotNull: true }
                                Columns { Name: "Value0" Type: "Utf8" }
                                Columns { Name: "Value1" Type: "Utf8" }
                                KeyColumnNames: "RowId"
                            }
                )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId - 1);

            TestAlterColumnTable(runtime, txId++, "/MyRoot/USER_0/C", R"(
                Name: "C2"
                AlterSchema {
                    DropColumns {Name: "Value0"}
                }
            )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId - 1);

            TestAlterColumnTable(runtime, txId++, "/MyRoot/USER_0/C", R"(
                Name: "C2"
                AlterSchema {
                    DropColumns {Name: "Value1"}
                    AddColumns { Name: "Value2" Type: "Utf8" }
                    AddColumns { Name: "Value3" Type: "Utf8" }
                    AddColumns { Name: "Value4" Type: "Utf8" }
                }
            )", {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, txId - 1);

            TestCreateColumnTable(runtime, txId++, "/MyRoot/USER_0/C", R"(
                            Name: "C1"
                            ColumnShardCount: 1
                            Schema {
                                Columns { Name: "RowId" Type: "Uint64", NotNull: true }
                                Columns { Name: "Value0" Type: "Utf8" }
                                Columns { Name: "Value1" Type: "Utf8" }
                                Columns { Name: "Value2" Type: "Utf8" }
                                KeyColumnNames: "RowId"
                            }
                )", {NKikimrScheme::StatusSchemeError});

            TString olapSchema = R"(
                Name: "OlapStore1"
                ColumnShardCount: 1
                SchemaPresets {
                    Name: "default"
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                    }
                }
            )";

            TestCreateOlapStore(runtime, txId++, "/MyRoot", olapSchema, {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId - 1);

            TString olapSchemaBig = R"(
                Name: "OlapStoreBig"
                ColumnShardCount: 1
                SchemaPresets {
                    Name: "default"
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                        Columns { Name: "data" Type: "Utf8" }
                        Columns { Name: "data2" Type: "Utf8" }
                        Columns { Name: "data3" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                    }
                }
            )";

            TestCreateOlapStore(runtime, txId++, "/MyRoot", olapSchemaBig, {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, txId - 1);

            TestAlterOlapStore(runtime, txId++, "/MyRoot", R"(
                Name: "OlapStore1"
                AlterSchemaPresets {
                    Name: "default"
                    AlterSchema {
                        AddColumns { Name: "comment" Type: "Utf8" }
                    }
                }
            )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId - 1);

            TestAlterOlapStore(runtime, txId++, "/MyRoot", R"(
                Name: "OlapStore1"
                AlterSchemaPresets {
                    Name: "default"
                    AlterSchema {
                        AddColumns { Name: "comment2" Type: "Utf8" }
                    }
                }
            )", {NKikimrScheme::StatusSchemeError});
            env.TestWaitNotification(runtime, txId - 1);
        }
    }

    Y_UNIT_TEST(SchemeLimitsRejectsWithIndexedTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TSchemeLimits lowLimits;
        lowLimits.MaxDepth = 4;
        lowLimits.MaxPaths = 5;
        lowLimits.MaxChildrenInDir = 3;
        lowLimits.MaxTableIndices = 4;
        lowLimits.MaxShards = 7;
        lowLimits.MaxShardsInPath = 4;
        lowLimits.ExtraPathSymbolsAllowed = "_.-";

        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards)});

        //create subdomain
        {
            TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                                "PlanResolution: 50 "
                                "Coordinators: 1 "
                                "Mediators: 1 "
                                "TimeCastBucketsPerMediator: 2 "
                                "Name: \"USER_0\"");
            env.TestWaitNotification(runtime, txId - 1);
            expectedDomainPaths += 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(3),
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards),
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
        }

        //path inside limit
        {
            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table1"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Utf8" }
                  Columns { Name: "value1" Type: "Utf8" }
                  Columns { Name: "value2" Type: "Utf8" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["value0"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue1"
                  KeyColumnNames: ["value1"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue2"
                  KeyColumnNames: ["value2"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValues"
                  KeyColumnNames: ["value0", "value1"]
                }
            )", {NKikimrScheme::StatusResourceExhausted});

            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table2"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Uint64" }
                  Columns { Name: "value1" Type: "Uint64" }
                  Columns { Name: "value2" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 4
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["value0"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue1"
                  KeyColumnNames: ["value1"]
                }
            )", {NKikimrScheme::StatusResourceExhausted});

            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table3"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Uint64" }
                  Columns { Name: "value1" Type: "Uint64" }
                  Columns { Name: "value2" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                  UniformPartitionsCount: 6
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["value0"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue1"
                  KeyColumnNames: ["value1"]
                }
            )", {NKikimrScheme::StatusResourceExhausted});

            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table4"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Uint64" }
                  Columns { Name: "value1" Type: "Uint64" }
                  Columns { Name: "value2" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                }
            )", {NKikimrScheme::StatusInvalidParameter});

            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table5"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Uint64" }
                  Columns { Name: "value1" Type: "Uint64" }
                  Columns { Name: "value2" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "Index_@"
                  KeyColumnNames: ["value1"]
                }
            )", {NKikimrScheme::StatusSchemeError});

            TestMkDir(runtime, txId++, "/MyRoot/USER_0", "1");
            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0/1", R"(
                TableDescription {
                  Name: "Table6"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Uint64" }
                  Columns { Name: "value1" Type: "Uint64" }
                  Columns { Name: "value2" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["value0"]
                }
            )", {NKikimrScheme::StatusAccepted});

            env.TestWaitNotification(runtime, {txId - 1, txId - 2, txId - 3, txId - 4, txId - 5, txId - 6});
        }

        // MaxTableIndices
        {
            TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table7"
                  Columns { Name: "RowId" Type: "Uint64" }
                  Columns { Name: "Value0" Type: "Utf8" }
                  Columns { Name: "Value1" Type: "Utf8" }
                  Columns { Name: "Value2" Type: "Utf8" }
                  Columns { Name: "Value3" Type: "Utf8" }
                  Columns { Name: "Value4" Type: "Utf8" }
                  KeyColumnNames: ["RowId"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                  KeyColumnNames: ["Value0"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue1"
                  KeyColumnNames: ["Value1"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue2"
                  KeyColumnNames: ["Value2"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue3"
                  KeyColumnNames: ["Value3"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue4"
                  KeyColumnNames: ["Value4"]
                }
            )", {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);
        }
    }

    Y_UNIT_TEST(SchemeLimitsCreatePq) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits lowLimits;
        lowLimits.MaxDepth = 4;
        lowLimits.MaxPaths = 5;
        lowLimits.MaxChildrenInDir = 3;
        lowLimits.MaxTableIndices = 4;
        lowLimits.MaxShards = 7;
        lowLimits.MaxShardsInPath = 4;
        lowLimits.ExtraPathSymbolsAllowed = "_.-";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::DomainLimitsIs(lowLimits.MaxPaths, lowLimits.MaxShards)});

        // 1 balancer + 4 partitions = 5 (over path limit)
        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 40 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                        {NKikimrScheme::StatusResourceExhausted});

        // 1 balancer + 3 partitions = 4 (within path limit)
        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 30 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");

        // 1 balancer + 3 partitions = 4 (over tenant limit)
        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup_2\""
                        "TotalGroupCount: 30 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}",
                        {NKikimrScheme::StatusResourceExhausted});

        // 1 balancer + 2 partitions = 3 (within tenant limit)
        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup_2\""
                        "TotalGroupCount: 20 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
    }

    Y_UNIT_TEST(SchemeQuotas) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        // Subdomain with two quotas: once per minute and twice per 10 minutes
        TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_0"
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "hdd-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "hdd-2"
                        }
                        DeclaredSchemeQuotas {
                            SchemeQuotas {
                                BucketSize: 1
                                BucketSeconds: 60
                            }
                            SchemeQuotas {
                                BucketSize: 2
                                BucketSeconds: 600
                            }
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        // First table should succeed
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table1"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Second table should fail (out of per-minute quota)
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table2"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After a minute we should be able to create one more table
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table3"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table4"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After 1 more minute we should still fail because of per 10 minute quota
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table5"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After 3 more minutes we should succeed, because enough per 10 minute quota regenerates
        runtime.AdvanceCurrentTime(TDuration::Minutes(3));
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table6"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Quotas consumption is persistent, on reboot they should stay consumed
        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table7"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // Need 5 more minutes to create a table
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table7"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Reset quotas for the subdomain
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
                        Name: "USER_0"
                        DeclaredSchemeQuotas {
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        // Now two consecutive create table operations should succeed
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table8"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table9"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Quotas removal is persistent, on reboot they should not reactivate
        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table10"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table11"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
    }

    Y_UNIT_TEST_FLAGS(DiskSpaceUsage, DisableStatsBatching, EnablePersistentPartitionStats) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(DisableStatsBatching);
        opts.EnablePersistentPartitionStats(EnablePersistentPartitionStats);
        opts.EnableBackgroundCompaction(false);  // make sure background compaction will not interfere
        opts.DataShardStatsReportIntervalSeconds(0);  // make sure stats will be reported swiftly

        TSchemeShard* schemeshard = nullptr;
        TTestEnv env(runtime, opts,
            /*TSchemeShardFactory ssFactory*/
            [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
                schemeshard = new TSchemeShard(tablet, info);
                Cerr << "TEST create schemeshard, " << (void*)schemeshard << Endl;
                return schemeshard;
            }
        );

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        if (DisableStatsBatching == false) {
            runtime.GetAppData().SchemeShardConfig.SetStatsMaxBatchSize(2);
        }

        const auto sender = runtime.AllocateEdgeActor();

        auto waitForFullStatsUpdate = [&](const ui32 count) {
            ui64 statsCountBaseline = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                auto statsCount = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get() - statsCountBaseline;
                Cerr << "TEST waitForFullStatsUpdate, schemeshard " << (void*)schemeshard << ", stats written " << statsCount << Endl;
                return statsCount >= count;
            };
            runtime.DispatchEvents(options);
        };

        auto getDiskSpaceUsage = [&]() {
            NKikimrSubDomains::TDiskSpaceUsage result;

            TestDescribeResult(
                DescribePath(runtime, "/MyRoot"), {
                    NLs::PathExist,
                    NLs::Finished, [&result] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                        result = record.GetPathDescription().GetDomainDescription().GetDiskSpaceUsage();
                    }
                }
            );

            return result;
        };

        auto compareDiskSpaceUsage = [&](TString* diff, const NKikimrSubDomains::TDiskSpaceUsage& a, const NKikimrSubDomains::TDiskSpaceUsage& b) -> bool {
            using google::protobuf::util::MessageDifferencer;
            MessageDifferencer d;
            d.ReportDifferencesToString(diff);
            d.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
            return d.Compare(a, b);
        };

        ui64 tabletId = TTestTxConfig::FakeHiveTablets;
        ui64 txId = 100;

        // single-shard table
        {
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32"}
                Columns { Name: "value" Type: "Utf8"}
                KeyColumnNames: ["key"]
            )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId);

            UpdateRow(runtime, "Table1", 1, "value1", tabletId);
            waitForFullStatsUpdate(1);

            auto du = getDiskSpaceUsage();
            UNIT_ASSERT_C(du.GetTables().GetTotalSize() > 0, du.ShortDebugString());

            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
            waitForFullStatsUpdate(1);
            TString diff;
            UNIT_ASSERT_C(compareDiskSpaceUsage(&diff, du, getDiskSpaceUsage()), diff);
        }

        // multi-shard table
        {
            tabletId = tabletId + 1;

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Uint32"}
                Columns { Name: "value" Type: "Utf8"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId);

            UpdateRow(runtime, "Table2", 1, "value1", tabletId + 0);
            UpdateRow(runtime, "Table2", 2, "value2", tabletId + 1);
            const ui32 shardCount = 1 /* Table1 */ + 2 /* Table2 */;
            waitForFullStatsUpdate(shardCount);

            auto du = getDiskSpaceUsage();
            UNIT_ASSERT_C(du.GetTables().GetTotalSize() > 0, du.ShortDebugString());

            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
            waitForFullStatsUpdate(shardCount);
            TString diff;
            UNIT_ASSERT_C(compareDiskSpaceUsage(&diff, du, getDiskSpaceUsage()), diff);
        }
    }

    Y_UNIT_TEST_FLAG(DiskSpaceUsageWithPersistedLeftovers, DisableStatsBatching) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(DisableStatsBatching);
        opts.EnableBackgroundCompaction(false);  // make sure background compaction will not interfere
        opts.DataShardStatsReportIntervalSeconds(0);  // make sure stats will be reported swiftly

        TSchemeShard* schemeshard = nullptr;
        TTestEnv env(runtime, opts,
            /*TSchemeShardFactory ssFactory*/
            [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
                schemeshard = new TSchemeShard(tablet, info);
                Cerr << "TEST create schemeshard, " << (void*)schemeshard << Endl;
                return schemeshard;
            }
        );

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        if (DisableStatsBatching == false) {
            runtime.GetAppData().SchemeShardConfig.SetStatsMaxBatchSize(2);
        }

        const auto sender = runtime.AllocateEdgeActor();

        auto waitForFullStatsUpdate = [&](const ui32 count) {
            ui64 statsCountBaseline = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                auto statsCount = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get() - statsCountBaseline;
                Cerr << "TEST waitForFullStatsUpdate, schemeshard " << (void*)schemeshard << ", stats written " << statsCount << Endl;
                return statsCount >= count;
            };
            runtime.DispatchEvents(options);
        };

        auto getDiskSpaceUsage = [&]() {
            NKikimrSubDomains::TDiskSpaceUsage result;

            TestDescribeResult(
                DescribePath(runtime, "/MyRoot"), {
                    NLs::PathExist,
                    NLs::Finished, [&result] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                        result = record.GetPathDescription().GetDomainDescription().GetDiskSpaceUsage();
                    }
                }
            );

            return result;
        };

        auto compareDiskSpaceUsage = [&](TString* diff, const NKikimrSubDomains::TDiskSpaceUsage& a, const NKikimrSubDomains::TDiskSpaceUsage& b) -> bool {
            using google::protobuf::util::MessageDifferencer;
            MessageDifferencer d;
            d.ReportDifferencesToString(diff);
            d.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
            return d.Compare(a, b);
        };

        ui64 tabletId = TTestTxConfig::FakeHiveTablets;
        ui64 txId = 100;

        // multi-shard table
        {
            runtime.GetAppData().FeatureFlags.SetEnablePersistentPartitionStats(true);

            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Uint32"}
                Columns { Name: "value" Type: "Utf8"}
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )");
            env.TestWaitNotification(runtime, txId);

            UpdateRow(runtime, "Table2", 1, "value1", tabletId + 0);
            UpdateRow(runtime, "Table2", 2, "value2", tabletId + 1);

            const ui32 shardCount = 2;
            waitForFullStatsUpdate(shardCount);

            auto du = getDiskSpaceUsage();
            UNIT_ASSERT_C(du.GetTables().GetTotalSize() > 0, du.ShortDebugString());

            runtime.GetAppData().FeatureFlags.SetEnablePersistentPartitionStats(false);

            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

            waitForFullStatsUpdate(shardCount);
            TString diff;
            UNIT_ASSERT_C(compareDiskSpaceUsage(&diff, du, getDiskSpaceUsage()), diff);
        }
    }

    Y_UNIT_TEST_FLAGS(DiskSpaceUsageWithTable, DisableStatsBatching, EnablePersistentPartitionStats) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(DisableStatsBatching);
        opts.EnablePersistentPartitionStats(EnablePersistentPartitionStats);
        opts.EnableBackgroundCompaction(false);  // make sure background compaction will not interfere
        opts.DataShardStatsReportIntervalSeconds(0);  // make sure stats will be reported swiftly

        TSchemeShard* schemeshard = nullptr;
        TTestEnv env(runtime, opts,
            /*TSchemeShardFactory ssFactory*/
            [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
                schemeshard = new TSchemeShard(tablet, info);
                Cerr << "TEST create schemeshard, " << (void*)schemeshard << Endl;
                return schemeshard;
            }
        );

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        if (DisableStatsBatching == false) {
            runtime.GetAppData().SchemeShardConfig.SetStatsMaxBatchSize(2);
        }

        const auto sender = runtime.AllocateEdgeActor();

        auto waitForFullStatsUpdate = [&](const ui32 count) {
            ui64 statsCountBaseline = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                auto statsCount = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get() - statsCountBaseline;
                Cerr << "TEST waitForFullStatsUpdate, schemeshard " << (void*)schemeshard << ", stats written " << statsCount << Endl;
                return statsCount >= count;
            };
            runtime.DispatchEvents(options);
        };

        auto compareProto = [&](TString* diff, const auto& a, const auto& b) -> bool {
            using google::protobuf::util::MessageDifferencer;
            MessageDifferencer d;
            d.ReportDifferencesToString(diff);
            d.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
            return d.Compare(a, b);
        };

        ui64 txId = 100;

        // test body

        // 1. create object and fill it with data
        const ui32 shardCount = 2;
        {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32"}
                    Columns { Name: "value" Type: "Utf8"}
                    KeyColumnNames: ["key"]
                    UniformPartitionsCount: %d
                )", shardCount
            ));
            env.TestWaitNotification(runtime, txId);

            const ui64 tabletId = TTestTxConfig::FakeHiveTablets;
            UpdateRow(runtime, "Table", 1, "value1", tabletId + 0);
            UpdateRow(runtime, "Table", 2, "value2", tabletId + 1);
        }

        // 2. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 3. check that disk space usage at subdomain level and table level is the same
        auto getUsage = [](const auto& describe) {
            return std::make_pair(
                describe.GetPathDescription().GetDomainDescription().GetDiskSpaceUsage(),
                describe.GetPathDescription().GetTableStats()
            );
        };
        auto describeBefore = DescribePath(runtime, "/MyRoot/Table");
        const auto& [subdomainDiskUsageBefore, storeUsageBefore] = getUsage(describeBefore);
        UNIT_ASSERT_GT_C(subdomainDiskUsageBefore.GetTables().GetDataSize(), 0, subdomainDiskUsageBefore.DebugString());
        UNIT_ASSERT_GT_C(storeUsageBefore.GetDataSize(), 0, storeUsageBefore.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageBefore.GetTables().GetDataSize(), storeUsageBefore.GetDataSize());

        // 4. reboot schemeshard
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // 5. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 6. check that disk space usage levels is the same as before reboot
        auto describeAfter = DescribePath(runtime, "/MyRoot/Table");
        const auto& [subdomainDiskUsageAfter, storeUsageAfter] = getUsage(describeAfter);
        TString diff;
        UNIT_ASSERT_C(compareProto(&diff, subdomainDiskUsageBefore, subdomainDiskUsageAfter), diff);
        UNIT_ASSERT_C(compareProto(&diff, storeUsageBefore, storeUsageAfter), diff);
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageAfter.GetTables().GetDataSize(), storeUsageAfter.GetDataSize());
    }

    Y_UNIT_TEST_FLAGS(DiskSpaceUsageWithColumnTableInStore, DisableStatsBatching, EnablePersistentPartitionStats) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(DisableStatsBatching);
        opts.EnablePersistentPartitionStats(EnablePersistentPartitionStats);
        opts.EnableBackgroundCompaction(false);  // make sure background compaction will not interfere
        opts.DataShardStatsReportIntervalSeconds(0);  // make sure stats will be reported swiftly

        TSchemeShard* schemeshard = nullptr;
        TTestEnv env(runtime, opts,
            /*TSchemeShardFactory ssFactory*/
            [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
                schemeshard = new TSchemeShard(tablet, info);
                Cerr << "TEST create schemeshard, " << (void*)schemeshard << Endl;
                return schemeshard;
            }
        );

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        if (DisableStatsBatching == false) {
            runtime.GetAppData().SchemeShardConfig.SetStatsMaxBatchSize(2);
        }

        const auto sender = runtime.AllocateEdgeActor();

        auto waitForFullStatsUpdate = [&](const ui32 count) {
            ui64 statsCountBaseline = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                auto statsCount = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get() - statsCountBaseline;
                Cerr << "TEST waitForFullStatsUpdate, schemeshard " << (void*)schemeshard << ", stats written " << statsCount << Endl;
                return statsCount >= count;
            };
            runtime.DispatchEvents(options);
        };

        auto compareProto = [&](TString* diff, const auto& a, const auto& b) -> bool {
            using google::protobuf::util::MessageDifferencer;
            MessageDifferencer d;
            d.ReportDifferencesToString(diff);
            d.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
            return d.Compare(a, b);
        };

        ui64 txId = 100;

        // test body

        // 1. create object and fill it with data
        const ui32 shardCount = 1;
        {
            TestCreateOlapStore(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "Store"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                        }
                    }
                )", shardCount
            ));
            env.TestWaitNotification(runtime, txId);

            TestCreateColumnTable(runtime, ++txId, "/MyRoot/Store", Sprintf(R"(
                    Name: "ColumnTable"
                    ColumnShardCount: %d
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                    }
                )", shardCount
            ));
            env.TestWaitNotification(runtime, txId);

            ui64 pathId = 0;
            ui64 shardId = 0;
            {
                auto describe = DescribePath(runtime, "/MyRoot/Store/ColumnTable");
                TestDescribeResult(describe, {NLs::PathExist});
                pathId = describe.GetPathId();
                const auto& sharding = describe.GetPathDescription().GetColumnTableDescription().GetSharding();
                shardId = sharding.GetColumnShards()[0];
            }
            UNIT_ASSERT(shardId);

            {   // Write data directly into shard
                TActorId sender = runtime.AllocateEdgeActor();
                const ui32 rowsInBatch = 100000;

                const TVector<NArrow::NTest::TTestColumn> ydbSchema = {
                    NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
                    NArrow::NTest::TTestColumn("data", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) )
                };
                const auto& data = NTxUT::MakeTestBlob({ 0, rowsInBatch }, ydbSchema, {}, { "timestamp" });
                ui64 writeId = 0;
                std::vector<ui64> writeIds;
                ++txId;
                NTxUT::WriteData(runtime, sender, shardId, ++writeId, pathId, data, ydbSchema, &writeIds, NEvWrite::EModificationType::Upsert, txId);
                NTxUT::TPlanStep planStep = NTxUT::ProposeCommit(runtime, sender, shardId, txId, writeIds, txId);
                NTxUT::PlanCommit(runtime, sender, shardId, planStep, { txId });

            }
        }

        // 2. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 3. check that disk space usage at subdomain level and column store level is the same
        auto getUsage = [](const auto& describe) {
            return std::make_pair(
                describe.GetPathDescription().GetDomainDescription().GetDiskSpaceUsage(),
                describe.GetPathDescription().GetTableStats()
            );
        };
        auto describeBefore = DescribePath(runtime, "/MyRoot/Store");
        const auto& [subdomainDiskUsageBefore, storeUsageBefore] = getUsage(describeBefore);
        UNIT_ASSERT_GT_C(subdomainDiskUsageBefore.GetTables().GetDataSize(), 0, subdomainDiskUsageBefore.DebugString());
        UNIT_ASSERT_GT_C(storeUsageBefore.GetDataSize(), 0, storeUsageBefore.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageBefore.GetTables().GetDataSize(), storeUsageBefore.GetDataSize());

        // 4. reboot schemeshard
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // 5. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 6. check that disk space usage levels is the same as before reboot
        auto describeAfter = DescribePath(runtime, "/MyRoot/Store");
        const auto& [subdomainDiskUsageAfter, storeUsageAfter] = getUsage(describeAfter);
        TString diff;
        UNIT_ASSERT_C(compareProto(&diff, subdomainDiskUsageBefore, subdomainDiskUsageAfter), diff);
        UNIT_ASSERT_C(compareProto(&diff, storeUsageBefore, storeUsageAfter), diff);
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageAfter.GetTables().GetDataSize(), storeUsageAfter.GetDataSize());
    }

    Y_UNIT_TEST_FLAGS(DiskSpaceUsageWithStandaloneColumnTable, DisableStatsBatching, EnablePersistentPartitionStats) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(DisableStatsBatching);
        opts.EnablePersistentPartitionStats(EnablePersistentPartitionStats);
        opts.EnableBackgroundCompaction(false);  // make sure background compaction will not interfere
        opts.DataShardStatsReportIntervalSeconds(0);  // make sure stats will be reported swiftly

        TSchemeShard* schemeshard = nullptr;
        TTestEnv env(runtime, opts,
            /*TSchemeShardFactory ssFactory*/
            [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
                schemeshard = new TSchemeShard(tablet, info);
                Cerr << "TEST create schemeshard, " << (void*)schemeshard << Endl;
                return schemeshard;
            }
        );

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        if (DisableStatsBatching == false) {
            runtime.GetAppData().SchemeShardConfig.SetStatsMaxBatchSize(2);
        }

        const auto sender = runtime.AllocateEdgeActor();

        auto waitForFullStatsUpdate = [&](const ui32 count) {
            ui64 statsCountBaseline = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                auto statsCount = schemeshard->TabletCounters->Cumulative()[COUNTER_STATS_WRITTEN].Get() - statsCountBaseline;
                Cerr << "TEST waitForFullStatsUpdate, schemeshard " << (void*)schemeshard << ", stats written " << statsCount << Endl;
                return statsCount >= count;
            };
            runtime.DispatchEvents(options);
        };

        auto compareProto = [&](TString* diff, const auto& a, const auto& b) -> bool {
            using google::protobuf::util::MessageDifferencer;
            MessageDifferencer d;
            d.ReportDifferencesToString(diff);
            d.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
            return d.Compare(a, b);
        };

        ui64 txId = 100;

        // test body

        // 1. create object and fill it with data
        const ui32 shardCount = 1;
        {
            TestCreateColumnTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "ColumnTable"
                    ColumnShardCount: %d
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                    }
                )", shardCount
            ));
            env.TestWaitNotification(runtime, txId);

            ui64 pathId = 0;
            ui64 shardId = 0;
            {
                auto describe = DescribePath(runtime, "/MyRoot/ColumnTable");
                TestDescribeResult(describe, {NLs::PathExist});
                pathId = describe.GetPathId();
                const auto& sharding = describe.GetPathDescription().GetColumnTableDescription().GetSharding();
                shardId = sharding.GetColumnShards()[0];
            }
            UNIT_ASSERT(shardId);

            {   // Write data directly into shard
                TActorId sender = runtime.AllocateEdgeActor();
                const ui32 rowsInBatch = 100000;

                const TVector<NArrow::NTest::TTestColumn> ydbSchema = {
                    NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
                    NArrow::NTest::TTestColumn("data", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) )
                };
                const auto& data = NTxUT::MakeTestBlob({ 0, rowsInBatch }, ydbSchema, {}, { "timestamp" });
                ui64 writeId = 0;
                std::vector<ui64> writeIds;
                ++txId;
                NTxUT::WriteData(runtime, sender, shardId, ++writeId, pathId, data, ydbSchema, &writeIds, NEvWrite::EModificationType::Upsert, txId);
                NTxUT::TPlanStep planStep = NTxUT::ProposeCommit(runtime, sender, shardId, txId, writeIds, txId);
                NTxUT::PlanCommit(runtime, sender, shardId, planStep, { txId });

            }
        }

        // 2. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 3. check that disk space usage at subdomain level and column store level is the same
        auto getUsage = [](const auto& describe) {
            return std::make_pair(
                describe.GetPathDescription().GetDomainDescription().GetDiskSpaceUsage(),
                describe.GetPathDescription().GetTableStats()
            );
        };
        auto describeBefore = DescribePath(runtime, "/MyRoot/ColumnTable");
        const auto& [subdomainDiskUsageBefore, storeUsageBefore] = getUsage(describeBefore);
        UNIT_ASSERT_GT_C(subdomainDiskUsageBefore.GetTables().GetDataSize(), 0, subdomainDiskUsageBefore.DebugString());
        UNIT_ASSERT_GT_C(storeUsageBefore.GetDataSize(), 0, storeUsageBefore.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageBefore.GetTables().GetDataSize(), storeUsageBefore.GetDataSize());

        // 4. reboot schemeshard
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // 5. wait for all shard stats to be processed
        waitForFullStatsUpdate(shardCount);

        // 6. check that disk space usage levels is the same as before reboot
        auto describeAfter = DescribePath(runtime, "/MyRoot/ColumnTable");
        const auto& [subdomainDiskUsageAfter, storeUsageAfter] = getUsage(describeAfter);
        TString diff;
        UNIT_ASSERT_C(compareProto(&diff, subdomainDiskUsageBefore, subdomainDiskUsageAfter), diff);
        UNIT_ASSERT_C(compareProto(&diff, storeUsageBefore, storeUsageAfter), diff);
        UNIT_ASSERT_VALUES_EQUAL(subdomainDiskUsageAfter.GetTables().GetDataSize(), storeUsageAfter.GetDataSize());
    }

    //TODO: add DiskSpaceUsage test for topics

    Y_UNIT_TEST(TableDiskSpaceQuotas) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableTopicDiskSubDomainQuota(false);

        TTestEnv env(runtime, opts);
        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(false);

        ui64 txId = 100;

        auto waitForTableStats = [&](ui32 shards) {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvDataShard::EvPeriodicTableStats, shards));
            runtime.DispatchEvents(options);
        };

        auto waitForSchemaChanged = [&](ui32 shards) {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvDataShard::EvSchemaChanged, shards));
            runtime.DispatchEvents(options);
        };

        auto createTable = [&]() {
            TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table1"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId);
        };

        auto checkQuotaAndDropTable = [&]() {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {LsCheckDiskQuotaExceeded(true, "Table was created and data was written")});

            TestDropTable(runtime, ++txId, "/MyRoot/USER_0", "Table1");
            waitForSchemaChanged(1);
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {LsCheckDiskQuotaExceeded(false, "Table dropped")});
        };

        // Subdomain with a 1-byte data size quota
        TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_0"
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "hdd-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "hdd-2"
                        }
                        DatabaseQuotas {
                            data_size_hard_quota: 1
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {LsCheckDiskQuotaExceeded(false, "SubDomain created")});

        // UpdateRow
        {
            createTable();

            ui64 tabletId = TTestTxConfig::FakeHiveTablets + 2;  // skip a single coordinator and mediator
            UpdateRow(runtime, "Table1", 1, "value1", tabletId);
            waitForTableStats(1);

            checkQuotaAndDropTable();
        }

        // WriteRow
        {
            createTable();

            bool successIsExpected = true;
            WriteRow(runtime, ++txId, "/MyRoot/USER_0/Table1", 0, 1, "value1", successIsExpected);
            waitForTableStats(1);

            successIsExpected = false;
            WriteRow(runtime, ++txId, "/MyRoot/USER_0/Table1", 0, 1, "value1", successIsExpected);

            checkQuotaAndDropTable();
        }
    }

    Y_UNIT_TEST(SchemeDatabaseQuotaRejects) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist});

        // Create subdomain.
        {
            TestCreateSubDomain(runtime, txId++,  "/MyRoot",
                                "PlanResolution: 50 "
                                "Coordinators: 1 "
                                "Mediators: 1 "
                                "TimeCastBucketsPerMediator: 2 "
                                "Name: \"USER_0\""
                                " DatabaseQuotas {"
                                "    data_stream_shards_quota: 2"
                                "    data_stream_reserved_storage_quota: 200000"
                                "}");
            env.TestWaitNotification(runtime, txId - 1);
            expectedDomainPaths += 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathVersionEqual(3),
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2),
                                NLs::DatabaseQuotas(2)});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
        }

        {
            // Stream shards(partitions) limit is 2. Trying to create 3.
            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 3
                            PartitionPerTablet: 2
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10  WriteSpeedInBytesPerSecond : 1000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});

            // Stream reserved storage limit is 200000. Trying to reserve 200001.
            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 1
                            PartitionPerTablet: 2
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 200001} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(0),
                                NLs::ShardsInsideDomain(2)});

            // Stream reserved storage limit is 200000. Trying to reserve 100000 - fit in it!

            TestCreatePQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 1
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )");

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(4)});

            // Stream reserved storage limit is 200000. Trying to reserve 200000 - fit in it!
            TestAlterPQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 2
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100000} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )");

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(5)});

            // Stream reserved storage limit is 200000. Trying to reserve 20002 - do not fit in it!
            TestAlterPQGroup(runtime, txId++, "/MyRoot/USER_0/", R"(
                            Name: "Isolda"
                            TotalGroupCount: 2
                            PartitionPerTablet: 1
                            PQTabletConfig: {PartitionConfig { LifetimeSeconds : 1 WriteSpeedInBytesPerSecond : 100001} MeteringMode: METERING_MODE_RESERVED_CAPACITY}
                )",  {NKikimrScheme::StatusResourceExhausted});

            env.TestWaitNotification(runtime, txId - 1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(5)});


        }

        //clear subdomain
        {
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
            TestForceDropSubDomain(runtime, txId++, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId - 1);
            expectedDomainPaths -= 1;

            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::PathsInsideDomain(expectedDomainPaths),
                                NLs::ShardsInsideDomain(0)});
        }
    }

    Y_UNIT_TEST(TopicDiskSpaceQuotas) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableTopicDiskSubDomainQuota(true);

        TTestEnv env(runtime, opts);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NLog::PRI_TRACE);

        runtime.GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(false);

        ui64 txId = 100;

        // Subdomain with a 1-byte data size quota
        TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_1"
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "hdd-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "hdd-2"
                        }
                        DatabaseQuotas {
                            data_size_hard_quota: 1
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
                           {LsCheckDiskQuotaExceeded(false, "SubDomain was created")});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/USER_1", R"(
            Name: "Topic1"
            TotalGroupCount: 3
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 60
                }
                MeteringMode: METERING_MODE_REQUEST_UNITS
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
                           {LsCheckDiskQuotaExceeded(false, "Topic was created")});

        ui64 balancerId = DescribePath(runtime, "/MyRoot/USER_1/Topic1").GetPathDescription().GetPersQueueGroup().GetBalancerTabletID();

        auto stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(false, stats->Record.GetSubDomainOutOfSpace(), "SubDomainOutOfSpace from ReadBalancer");

        auto msg = TString(24_MB, '_');

        ui32 seqNo = 100;
        WriteToTopic(runtime, "/MyRoot/USER_1/Topic1", ++seqNo, msg);
        env.SimulateSleep(runtime, TDuration::Seconds(3)); // Wait TEvPeriodicTopicStats

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
                           {LsCheckDiskQuotaExceeded(true, "Message 0 was written")});

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(true, stats->Record.GetSubDomainOutOfSpace(), "SubDomainOutOfSpace from ReadBalancer after write");

        TestDropPQGroup(runtime, ++txId, "/MyRoot/USER_1", "Topic1");
        env.TestWaitNotification(runtime, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_1"),
                           {LsCheckDiskQuotaExceeded(false, "Topic1 was deleted")});
    }
}

Y_UNIT_TEST_SUITE(TStoragePoolsQuotasTest) {

#define DEBUG_HINT (TStringBuilder() << "at line " << __LINE__)

    Y_UNIT_TEST_FLAG(DisableWritesToDatabase, IsExternalSubdomain) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(true);

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        ui64 txId = 100;

        // step 1: create a subdomain with a quoted storage pool
        constexpr const char* databaseDescription = R"(
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "unquoted_storage_pool"
                Kind: "unquoted_storage_pool_kind"
            }
            StoragePools {
                Name: "quoted_storage_pool"
                Kind: "quoted_storage_pool_kind"
            }
            DatabaseQuotas {
                storage_quotas {
                    unit_kind: "quoted_storage_pool_kind"
                    data_size_hard_quota: 1
                }
            }
        )";
        if (IsExternalSubdomain) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                    Name: "SomeDatabase"
                )"
            );
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                    ExternalSchemeShard: true
                )" << databaseDescription
            );
        } else {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                )" << databaseDescription
            );
        }
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/SomeDatabase"), {
                NLs::PathExist,
                IsExternalSubdomain ? NLs::IsExternalSubDomain("SomeDatabase") : NLs::IsSubDomain("SomeDatabase"),
                LsCheckDiskQuotaExceeded(false, DEBUG_HINT)
            }
        );
        ui64 tenantSchemeShard = TTestTxConfig::SchemeShard;
        if (IsExternalSubdomain) {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/SomeDatabase"), {
                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)
                }
            );
        }

        // step 2: create a table inside the subdomain
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/SomeDatabase", R"(
                Name: "SomeTable"
                Columns { Name: "key"   Type: "Uint32" FamilyName: "default"}
                Columns { Name: "value" Type: "Utf8"   FamilyName: "quoted_family"}
                KeyColumnNames: ["key"]
                PartitionConfig {
                    ColumnFamilies {
                        Name: "default"
                        StorageConfig {
                            SysLog { PreferredPoolKind: "unquoted_storage_pool_kind" }
                            Log { PreferredPoolKind: "unquoted_storage_pool_kind" }
                            Data { PreferredPoolKind: "unquoted_storage_pool_kind" }
                        }
                    }
                    ColumnFamilies {
                        Name: "quoted_family"
                        StorageConfig {
                            Data { PreferredPoolKind: "quoted_storage_pool_kind" }
                        }
                    }
                }
            )"
        );
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", false, DEBUG_HINT);

        // step 3: insert data into the table
        const auto shards = GetTableShards(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase/SomeTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        UpdateRow(runtime, "SomeTable", 1, "some_value_for_the_key", shards[0]);
        {
            const auto tableStats = WaitTableStats(runtime, shards[0]).GetTableStats();
            // channels' usage statistics appears only after a table compaction
            UNIT_ASSERT_VALUES_EQUAL_C(tableStats.ChannelsSize(), 0, tableStats.DebugString());
        }
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", false, DEBUG_HINT);

        // step 4: compact the table (statistics by channels does not appear in the messages from datashards otherwise)
        const auto tableId = ResolveTableId(runtime, "/MyRoot/SomeDatabase/SomeTable");
        CompactTableAndCheckResult(runtime, shards[0], tableId);
        {
            const auto tableStats = WaitTableStats(runtime, shards[0]).GetTableStats();
            UNIT_ASSERT_GT_C(tableStats.ChannelsSize(), 0, tableStats.DebugString());
        }
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", true, DEBUG_HINT);

        // step 5: drop the table
        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/SomeDatabase", "SomeTable");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", false, DEBUG_HINT);
    }

    Y_UNIT_TEST_FLAG(QuoteNonexistentPool, IsExternalSubdomain) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        TTestEnvOptions opts;
        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        constexpr const char* databaseDescription = R"(
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            DatabaseQuotas {
                storage_quotas {
                    unit_kind: "nonexistent_storage_kind"
                    data_size_hard_quota: 1
                }
            }
        )";
        if (IsExternalSubdomain) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                    Name: "SomeDatabase"
                )"
            );
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                    ExternalSchemeShard: true
                )" << databaseDescription,
                {{ NKikimrScheme::StatusInvalidParameter }}
            );
        } else {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                    Name: "SomeDatabase"
                )"
            );
            TestAlterSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                )" << databaseDescription,
                {{ NKikimrScheme::StatusInvalidParameter }}
            );
        }
        env.TestWaitNotification(runtime, {txId - 1, txId});
    }

    // This test might start failing, because disk space usage of the created table might change
    // due to changes in the storage implementation.
    // To fix the test you need to update canonical quotas and the content of the table.
    Y_UNIT_TEST_FLAGS(DifferentQuotasInteraction, IsExternalSubdomain, EnableSeparateQuotas) {
        TTestBasicRuntime runtime;
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        ui64 txId = 100;

        // Warning: calculated empirically, might need an update if the test fails.
        // Test scenario that expects these particular quotas is described in the comments below.
        const TString canonicalQuotas = Sprintf(R"(
                DatabaseQuotas {
                    data_size_hard_quota: %d
                    data_size_soft_quota: %d
                    storage_quotas {
                        unit_kind: "fast_kind"
                        data_size_hard_quota: %d
                        data_size_soft_quota: %d
                    }
                    storage_quotas {
                        unit_kind: "large_kind"
                        data_size_hard_quota: %d
                        data_size_soft_quota: %d
                    }
                }
            )", 2800, 2200, 600, 500, 2200, 1700
        );

        // step 1: create a subdomain with a quoted storage pool
        const TString databaseDescription = TStringBuilder() << R"(
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "fast"
                Kind: "fast_kind"
            }
            StoragePools {
                Name: "large"
                Kind: "large_kind"
            }
        )" << canonicalQuotas;

        if (IsExternalSubdomain) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                    Name: "SomeDatabase"
                )"
            );
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                    ExternalSchemeShard: true
                )" << databaseDescription
            );
        } else {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder() << R"(
                    Name: "SomeDatabase"
                )" << databaseDescription
            );
        }
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/SomeDatabase"), {
                NLs::PathExist,
                IsExternalSubdomain ? NLs::IsExternalSubDomain("SomeDatabase") : NLs::IsSubDomain("SomeDatabase"),
                LsCheckDiskQuotaExceeded(false, DEBUG_HINT)
            }
        );
        ui64 tenantSchemeShard = TTestTxConfig::SchemeShard;
        if (IsExternalSubdomain) {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/SomeDatabase"), {
                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)
                }
            );
        }

        // step 2: create a table inside the subdomain
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/SomeDatabase", R"(
                Name: "SomeTable"
                Columns { Name: "key"   Type: "Uint32" FamilyName: "default"}
                Columns { Name: "value" Type: "Utf8"   FamilyName: "large"}
                KeyColumnNames: ["key"]
                PartitionConfig {
                    ColumnFamilies {
                        Name: "default"
                        StorageConfig {
                            SysLog { PreferredPoolKind: "fast_kind" }
                            Log { PreferredPoolKind: "fast_kind" }
                            Data { PreferredPoolKind: "fast_kind" }
                        }
                    }
                    ColumnFamilies {
                        Name: "large"
                        StorageConfig {
                            Data { PreferredPoolKind: "large_kind" }
                        }
                    }
                }
            )"
        );
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", false, DEBUG_HINT);

        // step 3: insert data into the table in several batches
        const auto shards = GetTableShards(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase/SomeTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        const auto tableId = ResolveTableId(runtime, "/MyRoot/SomeDatabase/SomeTable");

        const auto updateAndCheck = [&](ui32 rowsToUpdate,
                                        const TString& value,
                                        const TMap<TString, EDiskUsageStatus>& expectedExceeders,
                                        const TString& debugHint = ""
        ) {
            for (ui32 i = 0; i < rowsToUpdate; ++i) {
                UpdateRow(runtime, "SomeTable", i, value, shards[0]);
            }
            CompactTableAndCheckResult(runtime, shards[0], tableId);
            WaitTableStats(runtime, shards[0]);
            CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", expectedExceeders, debugHint);
        };

        // Warning: calculated empirically, might need an update if the test fails!
        constexpr ui32 lessRows = 37u;
        const ui32 moreRows = runtime.GetAppData().FeatureFlags.GetEnableLocalDBBtreeIndex() ? 60u : 50u;

        const TString longText = TString(64, 'a');;
        const TString mediumText = TString(32, 'a');
        const TString shortText = TString(16, 'a');
        const TString tinyText = TString(8, 'a');

        runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(EnableSeparateQuotas);
        if (!EnableSeparateQuotas) {
            // write a lot of data to break the overall hard quota
            updateAndCheck(lessRows, longText, {{EntireDatabaseTag, EDiskUsageStatus::AboveHardQuota}}, DEBUG_HINT);
        } else {
            // There are two columns in the table: key and value. Key is stored at the fast storage, value at the large storage.
            // We can:
            // - simultaneously increase the consumption of the both storage pools by increasing the number of rows in the table
            // - increase or decrease the consumption of the large storage by making the value longer / shorter
            // Test scenario:
            // 1) write a small number of rows (little fast storage consumption), but a long text that breaks the large kind hard quota
            // 2) the same small number of rows, but a medium text that gets the large kind storage consumption in between the soft and the large quotas
            // 3) the same small number of rows, but a short text that gets the large kind storage consumption below the soft quota
            // 4) a bigger number of rows, but a tiny text to break only the fast kind hard quota
            updateAndCheck(lessRows, longText, {{"large_kind", EDiskUsageStatus::AboveHardQuota}}, DEBUG_HINT);
            updateAndCheck(lessRows, mediumText, {{"large_kind", EDiskUsageStatus::InBetween}}, DEBUG_HINT);
            updateAndCheck(lessRows, shortText, {}, DEBUG_HINT);

            updateAndCheck(moreRows, tinyText, {{"fast_kind", EDiskUsageStatus::AboveHardQuota}}, DEBUG_HINT);
        }

        // step 4: drop the table
        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/SomeDatabase", "SomeTable");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);
        CheckQuotaExceedance(runtime, tenantSchemeShard, "/MyRoot/SomeDatabase", false, DEBUG_HINT);
    }

#undef DEBUG_HINT

}
