#include <ydb/core/protos/sys_view_types.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace NKikimr::NSchemeShard {
    extern bool isSysDirCreateAllowed;
}
namespace {

    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;
    using NKikimrSysView::ESysViewType;

    void ExpectEqualSysViewDescription(const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
                                       const TString& sysViewName,
                                       const ESysViewType sysViewType) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasSysViewDescription());
        const auto& sysViewDescription = describeResult.GetPathDescription().GetSysViewDescription();
        UNIT_ASSERT_VALUES_EQUAL(sysViewDescription.GetName(), sysViewName);
        UNIT_ASSERT(sysViewDescription.GetType() == sysViewType);
    }

    class TSysDirCreateGuard : public TNonCopyable {
    public:
        TSysDirCreateGuard() {
            NKikimr::NSchemeShard::isSysDirCreateAllowed = true;
        }

        ~TSysDirCreateGuard() {
            NKikimr::NSchemeShard::isSysDirCreateAllowed = false;
        }
    };

    void WaitForSysViewsRosterUpdate(TTestActorRuntime &runtime) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NSysView::TEvSysView::EvRosterUpdateFinished);
        runtime.DispatchEvents(options, TDuration::Seconds(3));
    }

    ui64 TestCreateSysView(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &scheme,
                           const TString &userToken, const TString &owner,
                           const TVector<TExpectedResult> &expectedResults = {{NKikimrScheme::StatusAccepted}},
                           const TApplyIf &applyIf = {}) {

        THolder<TEvTx> request(CreateSysViewRequest(txId, parentPath, scheme, applyIf));
        auto& record = request->Record;
        record.SetUserToken(userToken);
        record.SetOwner(owner);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, request.Release());
        return TestModificationResults(runtime, txId, expectedResults);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSysViewTest) {
    Y_UNIT_TEST(CreateSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )");
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats);
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats);
        }
    }

    Y_UNIT_TEST(DropSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )");
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
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: ENodes
                            )",
                          {EStatus::StatusSchemeError, EStatus::StatusAlreadyExists});
        env.TestWaitNotification(runtime, txId);
        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats);
    }

    Y_UNIT_TEST(AsyncCreateDifferentSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           R"(
                              Name: "sys_view_1"
                              Type: EPartitionStats
                             )");
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           R"(
                              Name: "sys_view_2"
                              Type: ENodes
                             )");

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/sys_view_1");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "sys_view_1", ESysViewType::EPartitionStats);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/sys_view_2");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
            ExpectEqualSysViewDescription(describeResult, "sys_view_2", ESysViewType::ENodes);
        }
    }

    Y_UNIT_TEST(AsyncCreateDirWithSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(false));
        ui64 txId = 100;

        TSysDirCreateGuard sysDirCreateGuard;
        AsyncMkDir(runtime, ++txId, "/MyRoot", ".sys");
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           R"(
                              Name: "new_sys_view"
                              Type: EPartitionStats
                             )");

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished});

        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats);
    }

    Y_UNIT_TEST(AsyncCreateSameSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           R"(
                              Name: "new_sys_view"
                              Type: EPartitionStats
                             )");
        AsyncCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                           R"(
                              Name: "new_sys_view"
                              Type: EPartitionStats
                             )");
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusAlreadyExists};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView});
        ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EPartitionStats);
    }

    Y_UNIT_TEST(AsyncDropSameSysView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )");
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
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )",
                          {{EStatus::StatusReadOnly}});
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EPartitionStats
                            )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathExist);
    }

    Y_UNIT_TEST(EmptyName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;
        WaitForSysViewsRosterUpdate(runtime);

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: ""
                             Type: EPartitionStats
                            )",
                          {{EStatus::StatusSchemeError, "error: path part shouldn't be empty"}});
        env.TestWaitNotification(runtime, txId);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSysViewsUpdateTest) {
    Y_UNIT_TEST(CreateDirWithDomainSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));

        WaitForSysViewsRosterUpdate(runtime);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/.sys"), {NLs::Finished, NLs::HasOwner(BUILTIN_ACL_METADATA)});

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/ds_pdisks");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "ds_pdisks", ESysViewType::EPDisks);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/query_metrics_one_minute");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "query_metrics_one_minute", ESysViewType::EQueryMetricsOneMinute);
        }
    }

    Y_UNIT_TEST(RestoreAbsentSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;

        WaitForSysViewsRosterUpdate(runtime);

        TestLs(runtime, "/MyRoot/.sys/partition_stats", false, NLs::PathExist);
        TestLs(runtime, "/MyRoot/.sys/ds_pdisks", false, NLs::PathExist);

        TestDropSysView(runtime, ++txId, "/MyRoot/.sys", "ds_pdisks");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/.sys/ds_pdisks", false, NLs::PathNotExist);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        WaitForSysViewsRosterUpdate(runtime);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats);
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/ds_pdisks");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "ds_pdisks", ESysViewType::EPDisks);
        }
    }

    Y_UNIT_TEST(DeleteObsoleteSysViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableRealSystemViewPaths(true));
        ui64 txId = 100;

        WaitForSysViewsRosterUpdate(runtime);

        TestLs(runtime, "/MyRoot/.sys/partition_stats", false, NLs::PathExist);
        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_sys_view"
                             Type: EShowCreate
                            )");
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_sys_view");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_sys_view", ESysViewType::EShowCreate);
        }

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_ds_pdisks"
                             Type: EPDisks
                            )",
                          NACLib::TSystemUsers::Metadata().SerializeAsString(),
                          BUILTIN_ACL_METADATA);
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_ds_pdisks");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("metadata@system")});
            ExpectEqualSysViewDescription(describeResult, "new_ds_pdisks", ESysViewType::EPDisks);
        }

        TestCreateSysView(runtime, ++txId, "/MyRoot/.sys",
                          R"(
                             Name: "new_partition_stats"
                             Type: EPartitionStats
                            )");
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_partition_stats");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_partition_stats", ESysViewType::EPartitionStats);
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        WaitForSysViewsRosterUpdate(runtime);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/partition_stats");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner(BUILTIN_ACL_METADATA)});
            ExpectEqualSysViewDescription(describeResult, "partition_stats", ESysViewType::EPartitionStats);
        }

        // removed because had unsupported type for domain system view dir
        TestLs(runtime, "/MyRoot/.sys/new_sys_view", false, NLs::PathNotExist);

        // removed because owner was 'metadata@system' and had name not from domain system view reserved names
        TestLs(runtime, "/MyRoot/.sys/new_ds_pdisks", false, NLs::PathNotExist);

        // didn't touch user's system views with supported types
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/.sys/new_partition_stats");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSysView, NLs::HasOwner("root@builtin")});
            ExpectEqualSysViewDescription(describeResult, "new_partition_stats", ESysViewType::EPartitionStats);
        }
    }
}
