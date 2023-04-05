#include "cms_impl.h"
#include "cms_ut_common.h"
#include "info_collector.h"
#include "ut_helpers.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {
namespace NCmsTest {

using namespace NCms;
using namespace NKikimrCms;

Y_UNIT_TEST_SUITE(TCmsTenatsTest) {
    Y_UNIT_TEST(CollectInfo)
    {
        TCmsTestEnv env(8, 1);

        env.Register(CreateInfoCollector(env.GetSender(), TDuration::Minutes(1)));

        TAutoPtr<IEventHandle> handle;
        auto reply = env.GrabEdgeEventRethrow<TCms::TEvPrivate::TEvClusterInfo>(handle);
        UNIT_ASSERT(reply);
        const auto &info = *reply->Info;

        UNIT_ASSERT_VALUES_EQUAL(info.NodesCount(), env.GetNodeCount());
        UNIT_ASSERT_VALUES_EQUAL(info.PDisksCount(), env.GetNodeCount());
        UNIT_ASSERT_VALUES_EQUAL(info.VDisksCount(), env.GetNodeCount() * 4);
        UNIT_ASSERT_VALUES_EQUAL(info.BSGroupsCount(), 4);

        for (ui32 nodeIndex = 0; nodeIndex < env.GetNodeCount(); ++nodeIndex) {
            ui32 nodeId = env.GetNodeId(nodeIndex);
            UNIT_ASSERT(info.HasNode(nodeId));
            const auto &node = info.Node(nodeId);

            UNIT_ASSERT_VALUES_EQUAL(node.NodeId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(node.Host, "::1");
            UNIT_ASSERT_VALUES_EQUAL(node.State, NKikimrCms::UP);
            UNIT_ASSERT_VALUES_EQUAL(node.PDisks.size(), 1);
            UNIT_ASSERT(node.PDisks.contains(NCms::TPDiskID(nodeId, nodeId)));
            UNIT_ASSERT_VALUES_EQUAL(node.VDisks.size(), 4);
            UNIT_ASSERT(node.VDisks.contains(TVDiskID(0, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(node.VDisks.contains(TVDiskID(1, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(node.VDisks.contains(TVDiskID(2, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(node.VDisks.contains(TVDiskID(3, 1, 0, nodeIndex, 0)));

            UNIT_ASSERT(info.HasPDisk(NCms::TPDiskID(nodeId, nodeId)));

            UNIT_ASSERT(info.HasVDisk(TVDiskID(0, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(info.HasVDisk(TVDiskID(1, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(info.HasVDisk(TVDiskID(2, 1, 0, nodeIndex, 0)));
            UNIT_ASSERT(info.HasVDisk(TVDiskID(3, 1, 0, nodeIndex, 0)));
        }

        for (ui32 groupId = 0; groupId < 4; ++groupId) {
            UNIT_ASSERT(info.HasBSGroup(groupId));
            const auto &group = info.BSGroup(groupId);
            UNIT_ASSERT_VALUES_EQUAL(group.VDisks.size(), 8);
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 0, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 1, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 2, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 3, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 4, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 5, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 6, 0)));
            UNIT_ASSERT(group.VDisks.contains(TVDiskID(groupId, 1, 0, 7, 0)));
        }
    }

    Y_UNIT_TEST(TestNoneTenantPolicy)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(0, 10, 0, 0);

        for (ui32 i = 0; i < 8; ++i)
            env.CheckPermissionRequest("user", false, false, false, false, TStatus::ALLOW,
                                       MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(i), 60000000));
    }

    Y_UNIT_TEST(TestTenantLimit)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(2, 0, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.SetLimits(4, 0, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(4), 60000000));
    }

    Y_UNIT_TEST(TestDefaultTenantPolicyWithSingleTenantHost)
    {
        TNodeTenantsMap staticTenants;
        staticTenants[0] = {"user0"};

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(0, 10, 0, 0);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
    }

    Y_UNIT_TEST(TestTenantRatioLimit)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(0, 10, 0, 0);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", false, false, false,
                                        true, TStatus::ALLOW,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        env.SetLimits(0, 20, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        env.SetLimits(0, 30, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.CheckDonePermission("user", res1.second[0]);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));

        env.SetLimits(0, 0, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
    }

    Y_UNIT_TEST(TestClusterLimit)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(0, 0, 2, 0);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", false, false, false,
                                        false, TStatus::ALLOW,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.SetLimits(0, 0, 3, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));

        env.CheckDonePermission("user", res1.second[0]);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));

        env.SetLimits(0, 0, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
    }

    Y_UNIT_TEST(TestClusterRatioLimit)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);
        env.SetLimits(0, 0, 0, 10);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", false, false, false,
                                        false, TStatus::ALLOW,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        env.SetLimits(0, 0, 0, 20);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        env.SetLimits(0, 0, 0, 30);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.CheckDonePermission("user", res1.second[0]);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));

        env.SetLimits(0, 0, 0, 0);

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
    }

    Y_UNIT_TEST(TestLimitsWithDownNode)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);
        env.SetLimits(1, 10, 1, 10);

        // Mark node as down and try to lock it.
        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;

        env.CheckPermissionRequest("user", false, false, false,
                                   false, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        // Now node is locked and we cannot lock this or another node.
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false,
                                   true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));
    }

    void TestScheduledPermission(bool defaultPolicy)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(0, defaultPolicy ? 30 : 10, defaultPolicy ? 0 : 2, 0);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", true, false, true,
                                        defaultPolicy, TStatus::ALLOW_PARTIAL,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(4), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(5), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(6), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(7), 60000000)));
        UNIT_ASSERT_VALUES_EQUAL(res1.second.size(), 2);

        env.CheckRequest("user", res1.first, false, TStatus::DISALLOW_TEMP);

        env.CheckDonePermission("user", res1.second[0]);
        env.CheckDonePermission("user", res1.second[1]);

        env.CheckRequest("user", res1.first, false, TStatus::ALLOW_PARTIAL, 2);

        env.SetLimits(0, defaultPolicy ? 0 : 10, 0, 0);

        env.CheckRequest("user", res1.first, false, TStatus::ALLOW, 4);
    }

    Y_UNIT_TEST(TestScheduledPermissionWithNonePolicy)
    {
        TestScheduledPermission(false);
    }

    Y_UNIT_TEST(TestScheduledPermissionWithDefaultPolicy)
    {
        TestScheduledPermission(true);
    }

    void TestShutdownHost(bool usePolicy)
    {
        TNodeTenantsMap staticTenants;
        staticTenants[0].push_back("user0");
        staticTenants[8].push_back("user0");

        TCmsTestEnv env(16, 1, staticTenants);

        env.SetLimits(0, 10, 0, 0);

        env.CheckPermissionRequest("user", false, false, false, usePolicy,
                                   TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));

        env.CheckPermissionRequest("user", false, false, false, usePolicy,
                                   usePolicy ? TStatus::DISALLOW_TEMP : TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(8), 60000000));

        env.CheckPermissionRequest("user", false, false, false, usePolicy,
                                   TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(15), 60000000));
    }

    Y_UNIT_TEST(RequestShutdownHost) {
        TestShutdownHost(false);
    }

    Y_UNIT_TEST(RequestShutdownHostWithTenantPolicy) {
        TestShutdownHost(true);
    }

    Y_UNIT_TEST(RequestRestartServices) {
        TCmsTestEnv env(16, 1, TNodeTenantsMap{
            {8,  {"user0"}},
            {9,  {"user0"}},
            {10, {"user0"}},
            {11, {"user0"}},
            {12, {"user0"}},
            {13, {"user0"}},
            {14, {"user0"}},
            {15, {"user0"}},
        });

        // there is not storage on dynamic nodes
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::NO_SUCH_SERVICE,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(8), 60000000, "storage"));

        env.SetLimits(0, 50, 0, 0);
        // it is allowed to restart 50%
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(8),  60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(9),  60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(10), 60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(11), 60000000, "dynnode")
        );
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(12), 60000000, "dynnode"));

        env.SetLimits(0, 0, 0, 0);
        // it is allowed to restart them all
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(12), 60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(13), 60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(14), 60000000, "dynnode"),
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(15), 60000000, "dynnode")
        );

        // it doesn't affect storage nodes
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));

        // and fault model checks still work
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    }

    void TestLimitForceRestartMode(bool tenant, bool ratio)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(tenant && !ratio ? 1 : 0,
                      tenant && ratio ? 20 : 0,
                      !tenant && !ratio ? 1 : 0,
                      !tenant && ratio ? 20 : 0);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)));
        // Limit should work for any mode because we are restarting one node already.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false, true,
                                   MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false, true,
                                   MODE_FORCE_RESTART, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        env.CheckDonePermission("user", res1.second[0]);

        // Now shutdown one node and try various modes again. Only MODE_FORCE_RESTART
        // should allow to restart another node.
        {
            TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
            TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = false;
        }

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false, true,
                                   MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        env.CheckPermissionRequest("user", false, false, false, true,
                                   MODE_FORCE_RESTART, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
    }

    Y_UNIT_TEST(TestTenantLimitForceRestartMode) {
        TestLimitForceRestartMode(true, false);
    }

    Y_UNIT_TEST(TestTenantRatioLimitForceRestartMode) {
        TestLimitForceRestartMode(true, true);
    }

    Y_UNIT_TEST(TestClusterLimitForceRestartMode) {
        TestLimitForceRestartMode(false, false);
    }

    Y_UNIT_TEST(TestClusterRatioLimitForceRestartMode) {
        TestLimitForceRestartMode(false, true);
    }

    void TestLimitForceRestartModeScheduled(bool tenant, bool ratio)
    {
        TNodeTenantsMap staticTenants;
        for (ui32 i = 0; i < 8; ++i)
            staticTenants[i].push_back("user0");

        TCmsTestEnv env(8, staticTenants);

        env.SetLimits(tenant && !ratio ? 1 : 0,
                      tenant && ratio ? 20 : 0,
                      !tenant && !ratio ? 1 : 0,
                      !tenant && ratio ? 20 : 0);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", true, false, true, true, TStatus::ALLOW_PARTIAL,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000)));
        UNIT_ASSERT_VALUES_EQUAL(res1.second.size(), 1);
        // Limit should work for any mode because we are restarting one node already.
        env.CheckRequest("user", res1.first, false, TStatus::DISALLOW_TEMP, 0);
        env.CheckRequest("user", res1.first, false, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP, 0);
        env.CheckRequest("user", res1.first, false, MODE_FORCE_RESTART, TStatus::DISALLOW_TEMP, 0);

        env.CheckDonePermission("user", res1.second[0]);

        auto res2 = env.ExtractPermissions
            (env.CheckRequest("user", res1.first, false, MODE_FORCE_RESTART, TStatus::ALLOW_PARTIAL, 1));

        env.CheckDonePermission("user", res2.second[0]);

        // Now shutdown one node and try various modes again. Only MODE_FORCE_RESTART
        // should allow to restart another node.
        {
            TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
            TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = false;
        }
        env.CheckRequest("user", res1.first, false, TStatus::DISALLOW_TEMP, 0);
        env.CheckRequest("user", res1.first, false, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP, 0);
        env.CheckRequest("user", res1.first, false, MODE_FORCE_RESTART, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(TestTenantLimitForceRestartModeScheduled) {
        TestLimitForceRestartModeScheduled(true, false);
    }

    Y_UNIT_TEST(TestTenantRatioLimitForceRestartModeScheduled) {
        TestLimitForceRestartModeScheduled(true, true);
    }

    Y_UNIT_TEST(TestClusterLimitForceRestartModeScheduled) {
        TestLimitForceRestartModeScheduled(false, false);
    }

    Y_UNIT_TEST(TestClusterRatioLimitForceRestartModeScheduled) {
        TestLimitForceRestartModeScheduled(false, true);
    }
}

} // NCmsTest
} // NKikimr
