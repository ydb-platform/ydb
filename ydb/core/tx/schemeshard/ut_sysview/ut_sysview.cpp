#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {

    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;
    using NKikimrSysView::ESysViewType;

    void ExpectEqualSysViewDescription(const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
        const TString& sysViewName, const ESysViewType sysViewType, const TPathId& sourceObjectPathId)
    {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasSysViewDescription());
        const auto& sysViewDescription = describeResult.GetPathDescription().GetSysViewDescription();
        UNIT_ASSERT_VALUES_EQUAL(sysViewDescription.GetName(), sysViewName);
        UNIT_ASSERT(sysViewDescription.GetType() == sysViewType);
        UNIT_ASSERT_VALUES_EQUAL(TPathId::FromProto(sysViewDescription.GetSourceObject()), sourceObjectPathId);
    }

    ui64 TestCreateSysView(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &scheme,
        const TString &userToken, const TString &owner,
        const TVector<TExpectedResult> &expectedResults = {{NKikimrScheme::StatusAccepted}},
        const TApplyIf &applyIf = {})
    {
        THolder<TEvTx> request(CreateSysViewRequest(txId, parentPath, scheme, applyIf));
        auto& record = request->Record;
        record.SetUserToken(userToken);
        record.SetOwner(owner);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, request.Release());
        return TestModificationResults(runtime, txId, expectedResults);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSysViewTest) {
    Y_UNIT_TEST(CreateUnknownSysViewType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TVector<i32> invalidTypes = {
            0,
            -1,
            static_cast<i32>(NKikimrSysView::ESysViewType_MAX) + 1,
            Max<i32>(),
        };

        for (const i32 type : invalidTypes) {
            TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                              Sprintf(R"(
                                 Name: "unknown_sys_view"
                                 Type: %d
                                )", type),
                              {{NKikimrScheme::StatusSchemeError, "unsupported system view type"}});
            TestLs(runtime, "/MyRoot/.sys/unknown_sys_view", false, NLs::PathNotExist);
        }
    }

    Y_UNIT_TEST(CreateSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
            const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats, domainPathId);
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
            const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats, domainPathId);
        }
    }

    Y_UNIT_TEST(DropSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);

        TestDropSysView(runtime, ++txId, "/MyRoot/.sys", "new_sys_view");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(CreateExistingSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::ENodes)),
                          {EStatus::StatusSchemeError, EStatus::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);
        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
        const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats, domainPathId);
    }

    Y_UNIT_TEST(AsyncCreateDifferentSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           Sprintf(R"(
                              Name: "sys_view_1"
                              Type: %d
                             )", static_cast<i32>(ESysViewType::EPartitionStats)));
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           Sprintf(R"(
                              Name: "sys_view_2"
                              Type: %d
                             )", static_cast<i32>(ESysViewType::ENodes)));

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/sys_view_1");
            const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
            const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "sys_view_1", ESysViewType::EPartitionStats, domainPathId);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/sys_view_2");
            const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
            const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "sys_view_2", ESysViewType::ENodes, domainPathId);
        }
    }

    Y_UNIT_TEST(AsyncCreateDirWithSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableSystemNamesProtection(true));
        ui64 txId = 100;

        auto sysDirDesc = DescribePath(runtime, "/MyRoot/.sys");
        TestForceDropUnsafe(runtime, ++txId, sysDirDesc.GetPathId());
        env.TestWaitNotification(runtime, txId);

        AsyncMkDir(runtime, ++txId, "/MyRoot", ".sys");
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           Sprintf(R"(
                              Name: "new_sys_view"
                              Type: %d
                             )", static_cast<i32>(ESysViewType::EPartitionStats)));

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished});

        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
        const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats, domainPathId);
    }

    Y_UNIT_TEST(AsyncCreateSameSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           Sprintf(R"(
                              Name: "new_sys_view"
                              Type: %d
                             )", static_cast<i32>(ESysViewType::EPartitionStats)));
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           Sprintf(R"(
                              Name: "new_sys_view"
                              Type: %d
                             )", static_cast<i32>(ESysViewType::EPartitionStats)));
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusAlreadyExists};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        const auto& sysViewPath = describeResult.GetPathDescription().GetSelf();
        const auto domainPathId = TPathId(sysViewPath.GetSchemeshardId(), 1);
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats, domainPathId);
    }

    Y_UNIT_TEST(AsyncDropSameSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);

        AsyncDropSysView(runtime, ++txId, "/MyRoot/.sys", "new_sys_view");
        AsyncDropSysView(runtime, ++txId, "/MyRoot/.sys", "new_sys_view");
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusPathDoesNotExist};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)),
                          {{EStatus::StatusReadOnly}});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);
    }

    Y_UNIT_TEST(EmptyName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: ""
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)),
                          {{EStatus::StatusSchemeError, "error: path part shouldn't be empty"}});
        env.TestWaitNotification(runtime, txId);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSysViewsUpdateTest) {
    Y_UNIT_TEST(CreateDirWithDomainSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished, NLs::HasOwner("metadata@system")});

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats,
                                          describedPathId);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/ds_pdisks");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "ds_pdisks", ESysViewType::EPDisks, describedPathId);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/query_metrics_one_minute");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "query_metrics_one_minute", ESysViewType::EQueryMetricsOneMinute,
                                          describedPathId);
        }
    }

    Y_UNIT_TEST(RestoreAbsentSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestLs(runtime, "/MyRoot/.sys/partition_stats", false, NLs::PathExist);
        TestLs(runtime, "/MyRoot/.sys/ds_pdisks", false, NLs::PathExist);

        TestDropSysView(runtime, ++txId, "/MyRoot/.sys", "ds_pdisks");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/ds_pdisks", false, NLs::PathNotExist);

        env.AddSysViewsRosterUpdateObserver(runtime);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        env.WaitForSysViewsRosterUpdate(runtime);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats,
                                          describedPathId);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/ds_pdisks");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "ds_pdisks", ESysViewType::EPDisks, describedPathId);
        }
    }

    Y_UNIT_TEST(DeleteObsoleteSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestLs(runtime, "/MyRoot/.sys/partition_stats", false, NLs::PathExist);
        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_sys_view"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EShowCreate)));
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EShowCreate, describedPathId);
        }

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_ds_pdisks"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPDisks)),
                          NACLib::TSystemUsers::Metadata().SerializeAsString(),
                          "metadata@system");
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_ds_pdisks");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "new_ds_pdisks", ESysViewType::EPDisks, describedPathId);
        }

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          Sprintf(R"(
                             Name: "new_partition_stats"
                             Type: %d
                            )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_partition_stats");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_partition_stats", ESysViewType::EPartitionStats,
                                          describedPathId);
        }

        env.AddSysViewsRosterUpdateObserver(runtime);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        env.WaitForSysViewsRosterUpdate(runtime);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats,
                                          describedPathId);
        }

        // removed because had unsupported type for domain system view dir
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        // removed because owner was 'metadata@system' and had name not from domain system view reserved names
        TestLs(runtime, "/MyRoot/.sys/new_ds_pdisks", false, NLs::PathNotExist);

        // didn't touch user's system views with supported types
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_partition_stats");
            const auto& domainKey = describeResult.GetPathDescription().GetDomainDescription().GetDomainKey();
            const auto describedPathId = TPathId::FromDomainKey(domainKey);
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_partition_stats", ESysViewType::EPartitionStats,
                                          describedPathId);
        }
    }

    Y_UNIT_TEST(UnknownSysViewType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys", Sprintf(R"(
            Name: "new_sys_view"
            Type: %d
        )", static_cast<i32>(ESysViewType::EPartitionStats)));
        env.TestWaitNotification(runtime, txId);

        const auto pathId = DescribePath(runtime, "/MyRoot/.sys/new_sys_view")
            .GetPathDescription().GetSelf().GetPathId();
        const ui32 unknownType = static_cast<ui32>(NKikimrSysView::ESysViewType_MAX) + 1;
        UNIT_ASSERT(!NKikimrSysView::ESysViewType_IsValid(unknownType));
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('PathId (Uint64 '%lu))))
                (let update '('('SysViewType (Uint32 '%u))))
                (return (AsList (UpdateRow 'SysView key update)))
            )
        )", pathId, unknownType));

        // Keep the unknown view around to test describe and listing before cleanup.
        runtime.GetAppData().FeatureFlags.SetEnableRealSystemViewPaths(false);
        const auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SysView", "PathId", pathId));
        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetPathDescription().GetSysViewDescription().GetType(), unknownType);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::IsSysView);

        TVector<TString> children;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished, NLs::ExtractChildren(&children)});
        UNIT_ASSERT(Find(children, "new_sys_view") != children.end());

        // Reboot with roster updates enabled to remove the view with an unknown type.
        runtime.GetAppData().FeatureFlags.SetEnableRealSystemViewPaths(true);
        env.AddSysViewsRosterUpdateObserver(runtime);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        env.WaitForSysViewsRosterUpdate(runtime);

        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SysView", "PathId", pathId));
        const auto describeAfterCleanup = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        UNIT_ASSERT_VALUES_EQUAL(describeAfterCleanup.GetStatus(), NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT(!describeAfterCleanup.GetPathDescription().HasSysViewDescription());
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        children.clear();
        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished, NLs::ExtractChildren(&children)});
        UNIT_ASSERT(Find(children, "new_sys_view") == children.end());
    }
}
