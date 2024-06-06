#include "cms_impl.h"
#include "info_collector.h"
#include "ut_helpers.h"
#include "walle.h"
#include "cms_ut_common.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr::NCmsTest {

using namespace NCms;
using namespace NNodeWhiteboard;
using namespace NKikimrWhiteboard;
using namespace NKikimrCms;
using namespace NKikimrBlobStorage;

namespace {

void CheckLoadLogRecord(const NKikimrCms::TLogRecord &rec,
                        const TString &host,
                        ui32 nodeId,
                        const TString &version)
{

    UNIT_ASSERT_VALUES_EQUAL(static_cast<TLogRecordData::EType>(rec.GetRecordType()), TLogRecordData::CMS_LOADED);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<TLogRecordData::EType>(rec.GetData().GetRecordType()), TLogRecordData::CMS_LOADED);
    auto &data = rec.GetData().GetCmsLoaded();
    UNIT_ASSERT_VALUES_EQUAL(data.GetHost(), host);
    UNIT_ASSERT_VALUES_EQUAL(data.GetNodeId(), nodeId);
    UNIT_ASSERT_VALUES_EQUAL(data.GetVersion(), version);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TCmsTest) {
    Y_UNIT_TEST(CollectInfo)
    {
        TCmsTestEnv env(8);

        auto before = env.GetCurrentTime();
        env.Register(CreateInfoCollector(env.GetSender(), TDuration::Minutes(1)));

        TAutoPtr<IEventHandle> handle;
        auto reply = env.GrabEdgeEventRethrow<TCms::TEvPrivate::TEvClusterInfo>(handle);
        UNIT_ASSERT(reply);
        const auto &info = *reply->Info;

        auto after = env.GetCurrentTime();
        UNIT_ASSERT_VALUES_EQUAL(info.NodesCount(), env.GetNodeCount());
        UNIT_ASSERT_VALUES_EQUAL(info.PDisksCount(), env.GetNodeCount());
        UNIT_ASSERT_VALUES_EQUAL(info.VDisksCount(), env.GetNodeCount() * 4);
        UNIT_ASSERT_VALUES_EQUAL(info.BSGroupsCount(), 4);
        UNIT_ASSERT(info.GetTimestamp() >= before && info.GetTimestamp() <= after);

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

    Y_UNIT_TEST(StateRequest)
    {
        TCmsTestEnv env(8);
        auto before = env.GetCurrentTime();
        auto state = env.RequestState();

        auto after = env.GetCurrentTime();
        UNIT_ASSERT_VALUES_EQUAL(state.HostsSize(), 8);
        UNIT_ASSERT(state.GetTimestamp() >= before.GetValue()
                    && state.GetTimestamp() <= after.GetValue());
        for (const auto &host : state.GetHosts()) {
            UNIT_ASSERT_VALUES_EQUAL(host.GetName(), "::1");
            UNIT_ASSERT_VALUES_EQUAL(host.GetState(), UP);
            UNIT_ASSERT(host.GetTimestamp() >= before.GetValue()
                        && host.GetTimestamp() <= after.GetValue());

            UNIT_ASSERT_VALUES_EQUAL(host.ServicesSize(), 1);
            const auto &service = host.GetServices(0);
            UNIT_ASSERT_VALUES_EQUAL(service.GetName(), "storage");
            UNIT_ASSERT_VALUES_EQUAL(service.GetState(), UP);
            UNIT_ASSERT_VALUES_EQUAL(service.GetVersion(), ToString(GetProgramSvnRevision()));
            UNIT_ASSERT(service.GetTimestamp() >= before.GetValue()
                        && service.GetTimestamp() <= after.GetValue());

            UNIT_ASSERT_VALUES_EQUAL(host.DevicesSize(), 5);
            int pdisks = 1;
            int vdisks = 4;
            for (size_t i = 0; i < host.DevicesSize(); ++i) {
                const auto &device = host.GetDevices(i);
                UNIT_ASSERT_VALUES_EQUAL(device.GetState(), UP);
                UNIT_ASSERT(device.GetTimestamp() >= before.GetValue()
                            && device.GetTimestamp() <= after.GetValue());

                if (device.GetName().StartsWith("vdisk-"))
                    --vdisks;
                else if (device.GetName().StartsWith("pdisk-"))
                    --pdisks;
                else
                    UNIT_FAIL("bad device name");
            }
            UNIT_ASSERT(!vdisks && !pdisks);
        }
    }

    Y_UNIT_TEST(StateRequestNode)
    {
        TCmsTestEnv env(8, TNodeTenantsMap{{1, {"user0"}}});

        THashMap<ui32, TString> nodeIdxToServiceName = {
            {0, "storage"},
            {1, "dynnode"},
        };

        for (const auto& [nodeIdx, serviceName] : nodeIdxToServiceName) {
            NKikimrCms::TClusterStateRequest request;
            request.AddHosts(ToString(env.GetNodeId(nodeIdx)));

            auto state = env.RequestState(request);
            UNIT_ASSERT_VALUES_EQUAL(state.HostsSize(), 1);
            const auto &host = state.GetHosts(0);
            UNIT_ASSERT_VALUES_EQUAL(host.GetName(), "::1");
            UNIT_ASSERT_VALUES_EQUAL(host.GetState(), UP);
            UNIT_ASSERT_VALUES_EQUAL(host.ServicesSize(), 1);
            const auto &service = host.GetServices(0);
            UNIT_ASSERT_VALUES_EQUAL(service.GetName(), serviceName);
            UNIT_ASSERT_VALUES_EQUAL(service.GetState(), UP);
            UNIT_ASSERT_VALUES_EQUAL(service.GetVersion(), ToString(GetProgramSvnRevision()));
        }
    }

    Y_UNIT_TEST(StateRequestUnknownNode)
    {
        TCmsTestEnv env(8);

        NKikimrCms::TClusterStateRequest request;
        request.AddHosts("0");
        env.RequestState(request, TStatus::NO_SUCH_HOST);
    }

    Y_UNIT_TEST(StateRequestUnknownMultipleNodes)
    {
        TCmsTestEnv env(8);

        NKikimrCms::TClusterStateRequest request;
        request.AddHosts(FQDNHostName() + ".com");
        env.RequestState(request, TStatus::NO_SUCH_HOST);
    }

    Y_UNIT_TEST(RequestRestartServicesOk)
    {
        TCmsTestEnv env(8);

        auto rec = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec.PermissionsSize(), 1);

        const auto &action1 = rec.GetPermissions(0).GetAction();
        UNIT_ASSERT_VALUES_EQUAL(action1.GetType(), TAction::RESTART_SERVICES);
        UNIT_ASSERT_VALUES_EQUAL(action1.GetHost(), ToString(env.GetNodeId(0)));
        UNIT_ASSERT_VALUES_EQUAL(action1.ServicesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(action1.GetServices(0), "storage");
        UNIT_ASSERT_VALUES_EQUAL(action1.GetDuration(), 60000000);
    }

    Y_UNIT_TEST(RequestRestartServicesReject)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    }

    Y_UNIT_TEST(RequestRestartServicesPartial)
    {
        TCmsTestEnv env(8);

        auto rec = env.CheckPermissionRequest
            ("user", true, false, false, true, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec.PermissionsSize(), 1);

        const auto &action1 = rec.GetPermissions(0).GetAction();
        UNIT_ASSERT_VALUES_EQUAL(action1.GetType(), TAction::RESTART_SERVICES);
        UNIT_ASSERT(action1.GetHost() == ToString(env.GetNodeId(0))
                    || action1.GetHost() == ToString(env.GetNodeId(1)));
        UNIT_ASSERT_VALUES_EQUAL(action1.ServicesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(action1.GetServices(0), "storage");
        UNIT_ASSERT_VALUES_EQUAL(action1.GetDuration(), 60000000);
    }

    Y_UNIT_TEST(RequestRestartServicesRejectSecond)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    }

    Y_UNIT_TEST(RequestRestartServicesWrongHost)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::NO_SUCH_HOST,
                                   MakeAction(TAction::RESTART_SERVICES, "host", 60000000, "storage"));
    }

    Y_UNIT_TEST(RequestRestartServicesMultipleNodes)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, "::1", 60000000, "storage"));
    }

    Y_UNIT_TEST(RequestRestartServicesNoUser)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("", false, false, false, true, TStatus::WRONG_REQUEST,
                                   MakeAction(TAction::RESTART_SERVICES, "::1", 60000000, "storage"));
    }

    Y_UNIT_TEST(RequestRestartServicesDryRun)
    {
        TCmsTestEnv env(8);

        auto rec1 = env.CheckPermissionRequest
            ("user", false, true, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rec1.GetPermissions(0).GetId(), "");

        auto rec2 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec2.PermissionsSize(), 1);
    }

    Y_UNIT_TEST(ManagePermissions)
    {
        TCmsTestEnv env(8);

        auto rec1 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);

        TString id = rec1.GetPermissions(0).GetId();
        ui64 deadline = rec1.GetPermissions(0).GetDeadline();

        // Get by ID.
        auto rec2 = env.CheckGetPermission("user", id);
        UNIT_ASSERT_VALUES_EQUAL(rec2.PermissionsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rec2.GetPermissions(0).GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(rec2.GetPermissions(0).GetDeadline(), deadline);
        const auto &action = rec2.GetPermissions(0).GetAction();
        UNIT_ASSERT_VALUES_EQUAL(action.GetType(), TAction::RESTART_SERVICES);
        UNIT_ASSERT_VALUES_EQUAL(action.GetHost(), ToString(env.GetNodeId(0)));
        UNIT_ASSERT_VALUES_EQUAL(action.ServicesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(action.GetServices(0), "storage");
        UNIT_ASSERT_VALUES_EQUAL(action.GetDuration(), 60000000);

        // List all.
        env.CheckListPermissions("user", 1);
        env.CheckListPermissions("user2", 0);

        // Done with permission.
        env.CheckDonePermission("user", id);

        // List all expecting empty list.
        env.CheckListPermissions("user", 0);

        // Get permission again.
        auto rec3 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec3.PermissionsSize(), 1);
        UNIT_ASSERT_VALUES_UNEQUAL(rec3.GetPermissions(0).GetId(), id);

        id = rec3.GetPermissions(0).GetId();

        // Reject permission.
        env.CheckRejectPermission("user", id);

        // List all expecting empty list.
        env.CheckListPermissions("user", 0);

        // Get permission again.
        auto rec4 = env.CheckPermissionRequest
            ("user", false, false, false, false, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rec4.PermissionsSize(), 1);
        UNIT_ASSERT_VALUES_UNEQUAL(rec4.GetPermissions(0).GetId(), id);

        id = rec4.GetPermissions(0).GetId();

        // Done with permission dry run.
        env.CheckDonePermission("user", id, true);

        // Reject permission dry run.
        env.CheckRejectPermission("user", id, true);

        // Get unmodified permission.
        auto rec5 = env.CheckGetPermission("user", id, true);
        UNIT_ASSERT_VALUES_EQUAL(rec5.PermissionsSize(), 1);
    }

    Y_UNIT_TEST(ManagePermissionWrongRequest)
    {
        TCmsTestEnv env(8);

        auto rec1 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        TString id = rec1.GetPermissions(0).GetId();

        // No user.
        env.CheckGetPermission("", id, false, TStatus::WRONG_REQUEST);

        // Wrong owner for GET.
        env.CheckGetPermission("user2", id, false, TStatus::WRONG_REQUEST);

        // Wrong owner for DONE.
        env.CheckDonePermission("user2", id, false, TStatus::WRONG_REQUEST);

        // Wrong owner for REJECT.
        env.CheckRejectPermission("user2", id, false, TStatus::WRONG_REQUEST);

        // Wrong ID for GET.
        env.CheckGetPermission("user", id + "-bad", false, TStatus::WRONG_REQUEST);

        // Wrong ID for DONE.
        env.CheckDonePermission("user", id + "-bad", false, TStatus::WRONG_REQUEST);

        // Wrong ID for REJECT.
        env.CheckRejectPermission("user", id + "-bad", false, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(RequestReplaceDevices)
    {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::NO_SUCH_DEVICE,
                                   MakeAction(TAction::REPLACE_DEVICES, "::1", 60000000, "/dev/bad/device/path"));

        auto rec1 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);
        auto id1 = rec1.GetPermissions(0).GetId();

        // Disallow conflicting PDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(1)));

        // Disallow locked PDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)));

        // Disallow VDisk on locked PDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, "vdisk-1-1-0-0-0"));

        // Disallow conflicting VDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, "vdisk-1-1-0-1-0"));

        // Unlock PDisk.
        env.CheckDonePermission("user", id1);

        // Lock VDisks.
        auto rec2 = env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::ALLOW,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                        "vdisk-0-1-0-0-0", "vdisk-1-1-0-1-0", "vdisk-2-1-0-2-0"));
        UNIT_ASSERT_VALUES_EQUAL(rec2.PermissionsSize(), 1);
        id1 = rec2.GetPermissions(0).GetId();

        // Disallow conflicting VDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, "vdisk-1-1-0-3-0"));

        // Disallow locked VDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, "vdisk-1-1-0-1-0"));

        // Disallow conflicting PDisk.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)));

        // Lock VDisks.
        auto rec3 = env.CheckPermissionRequest
            ("user", false, false, false,  true, TStatus::ALLOW,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, "vdisk-3-1-0-1-0"));
        UNIT_ASSERT_VALUES_EQUAL(rec3.PermissionsSize(), 1);
        auto id2 = rec3.GetPermissions(0).GetId();

        // Unlock VDisks.
        env.CheckDonePermission("user", false, TStatus::OK, id1, id2);

        // Impossible action.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                              env.PDiskName(0), env.PDiskName(1)));

        // Impossible action.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                              env.PDiskName(0), "vdisk-3-1-0-1-0"));

        // Impossible action.
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::DISALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                              "vdisk-3-1-0-1-0", "vdisk-3-1-0-5-0"));
    }

    Y_UNIT_TEST(RequestReplaceManyDevicesOnOneNode)
    {
        TCmsTestEnv env(16, 3);
        NKikimrCms::TCmsConfig config;
        config.MutableClusterLimits()->SetDisabledNodesLimit(3);
        config.MutableClusterLimits()->SetDisabledNodesRatioLimit(0);
        env.SetCmsConfig(config);

        auto rec1 = env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                               MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0),
                                                          60000000, env.PDiskName(0, 0),
                                                          env.PDiskName(0, 1), env.PDiskName(0, 2)));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);

        auto rec2 = env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                               MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(9),
                                                          60000000));
        UNIT_ASSERT_VALUES_EQUAL(rec2.PermissionsSize(), 1);
    }

    Y_UNIT_TEST(RequestReplaceBrokenDevices)
    {
        TCmsTestEnv env(8);

        // PDisk-0 on node-0 is down.
        auto &node = TFakeNodeWhiteboardService::Info[env.GetNodeId(0)];
        node.PDiskStateInfo[env.PDiskId(0).DiskId].SetState(NKikimrBlobStorage::TPDiskState::Initial);

        // OK to restart broken PDisk.
        env.CheckPermissionRequest("user", false, true, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)));

        // Not OK to replace PDisk from damaged group.
        env.CheckPermissionRequest("user", false, true, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(1)));
    }

    Y_UNIT_TEST(ManageRequestsWrong)
    {
        TCmsTestEnv env(8);

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", true, false, true, true, TStatus::ALLOW_PARTIAL,
                                        MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)),
                                        MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(1))));
        UNIT_ASSERT_VALUES_EQUAL(res1.second.size(), 1);
        UNIT_ASSERT(res1.first);
        auto id1 = res1.second[0];

        env.CheckGetRequest("", id1, false, TStatus::WRONG_REQUEST);
        env.CheckGetRequest("user", id1 + "-bad", false, TStatus::WRONG_REQUEST);
        env.CheckGetRequest("user1", id1, false, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ManageRequestsDry)
    {
        TCmsTestEnv env(8);

        auto rec1 = env.CheckPermissionRequest
            ("user", true, false, true, true, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)),
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(1)));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);
        auto pid1 = rec1.GetPermissions(0).GetId();
        auto rid1 = rec1.GetRequestId();

        env.CheckGetRequest("user", rid1, true);
        env.CheckRejectRequest("user", rid1, true);
        env.CheckGetRequest("user", rid1);
        env.CheckDonePermission("user", pid1);
        env.CheckRequest("user", rid1, true, TStatus::ALLOW, 1);
        env.CheckGetRequest("user", rid1);
        env.CheckRequest("user", rid1, false, TStatus::ALLOW, 1);
        env.CheckGetRequest("user", rid1, false, TStatus::WRONG_REQUEST);
    }

    Y_UNIT_TEST(ManageRequests)
    {
        TCmsTestEnv env(8, 4);

        // Schedule request-1.
        auto rec1 = env.CheckPermissionRequest
            ("user", true, false, true, true, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000),
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000),
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000),
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));
        UNIT_ASSERT_VALUES_EQUAL(rec1.PermissionsSize(), 1);
        auto pid1 = rec1.GetPermissions(0).GetId();
        auto rid1 = rec1.GetRequestId();

        // Schedule request-2.
        auto rec2 = env.CheckPermissionRequest
            ("user", true, false, true, true, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::REPLACE_DEVICES, 0, 60000000,
                        env.PDiskName(0, 0), env.PDiskName(0, 1)),
             MakeAction(TAction::REPLACE_DEVICES, 0, 60000000,
                        env.PDiskName(1, 2)));
        auto rid2 = rec2.GetRequestId();

        // List 2 events for user.
        env.CheckListRequests("user", 2);
        // List 0 requests for user1.
        env.CheckListRequests("user1", 0);

        // Get scheduled request-2.
        auto rec3 = env.CheckGetRequest("user", rid2);
        UNIT_ASSERT_VALUES_EQUAL(rec3.RequestsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rec3.GetRequests(0).GetRequestId(), rid2);
        UNIT_ASSERT_VALUES_EQUAL(rec3.GetRequests(0).GetOwner(), "user");
        UNIT_ASSERT_VALUES_EQUAL(rec3.GetRequests(0).ActionsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rec3.GetRequests(0).GetPartialPermissionAllowed(), true);
        UNIT_ASSERT_VALUES_EQUAL(rec3.GetRequests(0).GetAvailabilityMode(), MODE_MAX_AVAILABILITY);

        // Done with permission-1.
        env.CheckDonePermission("user", pid1);
        // Retry request-2.
        auto rec4 = env.CheckRequest("user", rid2, false, TStatus::ALLOW, 2);
        auto pid2 = rec4.GetPermissions(0).GetId();
        auto pid3 = rec4.GetPermissions(1).GetId();
        // Get permission-2 for request-1.
        env.CheckRequest("user", rid1, false, TStatus::DISALLOW_TEMP);

        // Get request-1 expecting 3 remaining actions.
        auto rec5 = env.CheckGetRequest("user", rid1);
        UNIT_ASSERT_VALUES_EQUAL(rec5.RequestsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rec5.GetRequests(0).ActionsSize(), 3);

        // Schedule request-3.
        auto rec6 = env.CheckPermissionRequest
            ("user1", true, false, true, true, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
        auto rid3 = rec6.GetRequestId();

        // Schedule request-4.
        auto rec7 = env.CheckPermissionRequest
            ("user1", true, false, true, true, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        auto rid4 = rec7.GetRequestId();

        // List 2 requests of user1.
        env.CheckListRequests("user1", 2);
        // Reject request-1.
        env.CheckRejectRequest("user", rid1);
        // Done with permissions 2 and 3.
        env.CheckDonePermission("user", pid2);
        env.CheckDonePermission("user", pid3);
        // Retry request-3.
        auto rec8 = env.CheckRequest("user1", rid3, false, TStatus::ALLOW, 1);
        auto pid4 = rec8.GetPermissions(0).GetId();
        // Retry request-4 and get reject (conflict with request-3).
        env.CheckRequest("user1", rid4, false, TStatus::DISALLOW_TEMP);

        // Restart CMS tablet.
        env.RestartCms();

        // Check there are no permissions for user.
        env.CheckListPermissions("user", 0);
        // Check permission of user1 is restored.
        auto rec9 = env.CheckListPermissions("user1", 1);
        UNIT_ASSERT_VALUES_EQUAL(rec9.GetPermissions(0).GetId(), pid4);
        // Done with permissions-4.
        env.CheckDonePermission("user1", pid4);
        // Check request for user1 is restored.
        env.CheckListRequests("user1", 1);
        // Retry request-4.
        auto rec10 = env.CheckRequest("user1", rid4, false, TStatus::ALLOW, 1);
        auto pid5 = rec10.GetPermissions(0).GetId();
        // Check there are no requests for user.
        env.CheckListRequests("user1", 0);
    }

    Y_UNIT_TEST(WalleTasks)
    {
        TCmsTestEnv env(24, 4);

        // Prepare existing hosts.
        env.CheckWalleCreateTask("task-1", "prepare", false, TStatus::OK,
                                 env.GetNodeId(0), env.GetNodeId(1), env.GetNodeId(2));
        // Deactivate existing host.
        env.CheckWalleCreateTask("task-2", "deactivate", false, TStatus::OK,
                                 env.GetNodeId(2));
        // Prepare unknown host.
        env.CheckWalleCreateTask("task-3", "prepare", false, TStatus::WRONG_REQUEST,
                                 "host1");
        // Deactivate Unknown host.
        env.CheckWalleCreateTask("task-4", "deactivate", false, TStatus::WRONG_REQUEST,
                                 "host1");
        // Lock node-0 for reboot.
        env.CheckWalleCreateTask("task-5", "change-disk", false, TStatus::ALLOW,
                                 env.GetNodeId(0));
        // List tasks.
        env.CheckWalleListTasks("task-5", "ok", env.GetNodeId(0));
        // Schedule node-1 reboot.
        env.CheckWalleCreateTask("task-6", "reboot", false, TStatus::DISALLOW_TEMP,
                                 env.GetNodeId(1));
        // List tasks.
        env.CheckWalleListTasks(2);
        // Check task.
        env.CheckWalleCheckTask("task-5", TStatus::ALLOW, env.GetNodeId(0));
        // Check scheduled task.
        env.CheckWalleCheckTask("task-6", TStatus::DISALLOW_TEMP, env.GetNodeId(1));
        // Check unknown task.
        env.CheckWalleCheckTask("task-7", TStatus::WRONG_REQUEST);
        // Check permissions for Wall-E user.
        env.CheckListPermissions(WALLE_CMS_USER, 1);
        // Try dry run creation.
        env.CheckWalleCreateTask("task-7", "reboot", true, TStatus::DISALLOW_TEMP,
                                 env.GetNodeId(1));
        // Tasks list shouldn't grow.
        env.CheckWalleListTasks(2);
        // Finish allowed task.
        env.CheckWalleRemoveTask("task-5");
        // Remove unknown task.
        env.CheckWalleRemoveTask("task-5", TStatus::WRONG_REQUEST);
        // Check modified tasks list.
        env.CheckWalleListTasks("task-6", "in-process", env.GetNodeId(1));
        // Lock node-0 for reboot, Task-6 scheduled, but permission was not issued.
        env.CheckWalleCreateTask("task-8", "change-disk", false, TStatus::ALLOW,
                                 env.GetNodeId(0));
        // Check tasks list.
        env.CheckWalleListTasks(2);
        // Task-6 still cannot be executed.
        env.CheckWalleCheckTask("task-6", TStatus::DISALLOW_TEMP, env.GetNodeId(1));

        // Kill CMS tablet to check Wall-E tasks are preserved correctly.
        env.RestartCms();

        // Check tasks list.
        env.CheckWalleListTasks(2);
        // Task-8 is allowed.
        env.CheckWalleCheckTask("task-8", TStatus::ALLOW, env.GetNodeId(0));
        // Task-6 is waiting.
        env.CheckWalleCheckTask("task-6", TStatus::DISALLOW_TEMP, env.GetNodeId(1));
        // Remove task-8.
        env.CheckWalleRemoveTask("task-8");
        // Task-6 now can be executed.
        env.CheckWalleCheckTask("task-6", TStatus::ALLOW, env.GetNodeId(1));
        // Check modified tasks list.
        env.CheckWalleListTasks("task-6", "ok", env.GetNodeId(1));
        // Remove task-6.
        env.CheckWalleRemoveTask("task-6");
        // Try task which should be rejected.
        env.CheckWalleCreateTask("task-9", "reboot", false, TStatus::DISALLOW,
                                 env.GetNodeId(0), env.GetNodeId(1));
        env.CheckWalleListTasks(0);

        // Create task for switchs maintenance
        env.CheckWalleCreateTask("task-10", "temporary-unreachable", false, TStatus::ALLOW,
                                 env.GetNodeId(1));
        // Wait 30 minutes till timeout
        env.AdvanceCurrentTime(TDuration::Minutes(30));
        env.CheckWalleCheckTask("task-10", TStatus::ALLOW, env.GetNodeId(1));
        // Remove Task
        env.CheckWalleRemoveTask("task-10");
        // Check tasks list
        env.CheckWalleListTasks(0);
    }

    Y_UNIT_TEST(WalleTasksWithNodeLimit)
    {
        TCmsTestEnv env(24, 4);

        // set limit
        NKikimrCms::TCmsConfig config;
        config.MutableClusterLimits()->SetDisabledNodesLimit(1);
        env.SetCmsConfig(config);

        // ok
        env.CheckWalleCreateTask("task-1", "reboot", false, TStatus::ALLOW, env.GetNodeId(0));
        // schedule
        env.CheckWalleCreateTask("task-2", "reboot", false, TStatus::DISALLOW_TEMP, env.GetNodeId(1));
        // remove first task
        env.CheckWalleRemoveTask("task-1");
        // ok for second task
        env.CheckWalleCheckTask("task-2", TStatus::ALLOW, env.GetNodeId(1));

        // check task is not removed
        env.AdvanceCurrentTime(TDuration::Minutes(20));
        env.CheckWalleCheckTask("task-2", TStatus::ALLOW, env.GetNodeId(1));
    }

    Y_UNIT_TEST(WalleRebootDownNode)
    {
        TCmsTestEnv env(8);

        // alllow
        env.CheckWalleCreateTask("task-1", "reboot", false, TStatus::ALLOW, env.GetNodeId(0));
        // disallow (up)
        env.CheckWalleCreateTask("task-2", "reboot", false, TStatus::DISALLOW_TEMP, env.GetNodeId(1));
        // allow (down)
        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        env.CheckWalleCheckTask("task-2", TStatus::ALLOW, env.GetNodeId(1));
    }

    Y_UNIT_TEST(Notifications)
    {
        TCmsTestEnv env(8);
        env.AdvanceCurrentTime(TDuration::Minutes(20));

        // User is not specified.
        env.CheckNotification(TStatus::WRONG_REQUEST, "", env.GetCurrentTime(),
                              MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
        // Too old.
        env.CheckNotification(TStatus::WRONG_REQUEST, "user", env.GetCurrentTime() - TDuration::Minutes(10),
                              MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000));
        // Store notification user-1.
        auto id1 = env.CheckNotification
            (TStatus::OK, "user", env.GetCurrentTime() + TDuration::Minutes(10),
             MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(1, 0)));

        // OK to replace the same device before notification start time.
        env.CheckPermissionRequest("user", false, true, true, true, TStatus::ALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(1, 0)));

        // Intersects with notification.
        env.CheckPermissionRequest("user", false, true, true, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 10 * 60000000, env.PDiskName(1, 0)));

        // Store notification user-2.
        auto id2 = env.CheckNotification(TStatus::OK, "user", env.GetCurrentTime(),
                                         MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(2, 0)));
        // Store notificaiton user1-3.
        auto id3 = env.CheckNotification(TStatus::OK, "user1", env.GetCurrentTime(),
                                         MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(3, 0)));
        // Get notification with no user.
        env.CheckGetNotification("", id1, TStatus::WRONG_REQUEST);
        // Get user-1.
        env.CheckGetNotification("user", id1, TStatus::OK);
        // Get with wrong user.
        env.CheckGetNotification("user1", id1, TStatus::WRONG_REQUEST);
        // Get with wrong id.
        env.CheckGetNotification("user", "wrong-id", TStatus::WRONG_REQUEST);
        // List notifications for user.
        env.CheckListNotifications("user", TStatus::OK, 2);
        // List notifications for user1.
        env.CheckListNotifications("user1", TStatus::OK, 1);
        // List with no user.
        env.CheckListNotifications("", TStatus::WRONG_REQUEST, 0);
        // Reject notification with no user.
        env.CheckRejectNotification("", id1, TStatus::WRONG_REQUEST);
        // Reject notification with wrong user.
        env.CheckRejectNotification("user1", id1, TStatus::WRONG_REQUEST);
        // Reject user-1 (dry run)
        env.CheckRejectNotification("user", id1, TStatus::OK, true);
        // Get user-1.
        env.CheckGetNotification("user", id1, TStatus::OK);
        // Reject user1-3.
        env.CheckRejectNotification("user1", id3, TStatus::OK);
        // Reject user-2.
        env.CheckRejectNotification("user", id2, TStatus::OK);
        // List notifications for user.
        env.CheckListNotifications("user", TStatus::OK, 1);
        // List notifications for user1.
        env.CheckListNotifications("user1", TStatus::OK, 0);
        // Get rejected user1-3.
        env.CheckGetNotification("user1", id3, TStatus::WRONG_REQUEST);
        // Get rejected user-2.
        env.CheckGetNotification("user", id2, TStatus::WRONG_REQUEST);
        // Get user-1.
        env.CheckGetNotification("user", id1, TStatus::OK);
   }

    Y_UNIT_TEST(PermissionDuration) {
        TCmsTestEnv env(8);

        // Store notification user-1.
        auto id1 = env.CheckNotification
            (TStatus::OK, "user", env.GetCurrentTime() + TDuration::Minutes(10),
             MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(1, 0)));

        // Intersects with notification.
        const TDuration _10minutes = TDuration::Minutes(10);
        env.CheckPermissionRequest("user", false, true, true, true, _10minutes, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), _10minutes.MicroSeconds(), env.PDiskName(1, 0)));

        // OK with default duration.
        env.CheckPermissionRequest("user", false, true, true, true, TStatus::ALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(1, 0)));
    }

    Y_UNIT_TEST(ActionWithZeroDuration) {
        TCmsTestEnv env(8);

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 0, env.PDiskName(1, 0)));

        env.AdvanceCurrentTime(TDuration::Minutes(10));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TCms::TEvPrivate::EvCleanupExpired);
        env.DispatchEvents(options);
    }

    Y_UNIT_TEST(DynamicConfig)
    {
        TCmsTestEnv env(1);

        NKikimrCms::TCmsConfig config;
        config.SetDefaultRetryTime(TDuration::Minutes(10).GetValue());
        config.SetDefaultPermissionDuration(TDuration::Minutes(11).GetValue());
        config.MutableTenantLimits()->SetDisabledNodesLimit(1);
        config.MutableTenantLimits()->SetDisabledNodesRatioLimit(10);
        config.MutableClusterLimits()->SetDisabledNodesLimit(2);
        config.MutableClusterLimits()->SetDisabledNodesRatioLimit(20);
        config.SetInfoCollectionTimeout(TDuration::Minutes(1).GetValue());
        env.SetCmsConfig(config);

        auto res = env.GetCmsConfig();
        UNIT_ASSERT_VALUES_EQUAL(res.GetDefaultRetryTime(), TDuration::Minutes(10).GetValue());
        UNIT_ASSERT_VALUES_EQUAL(res.GetDefaultPermissionDuration(), TDuration::Minutes(11).GetValue());
        UNIT_ASSERT_VALUES_EQUAL(res.GetTenantLimits().GetDisabledNodesLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.GetTenantLimits().GetDisabledNodesRatioLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(res.GetClusterLimits().GetDisabledNodesLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res.GetClusterLimits().GetDisabledNodesRatioLimit(), 20);
        UNIT_ASSERT_VALUES_EQUAL(res.GetInfoCollectionTimeout(), TDuration::Minutes(1).GetValue());
    }

    Y_UNIT_TEST(RestartNodeInDownState)
    {
        TCmsTestEnv env(8);

        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    }

    void TestAvailabilityMode(EAvailabilityMode mode, bool disconnectNodes)
    {
        Y_ABORT_UNLESS(mode == MODE_KEEP_AVAILABLE
                 || mode == MODE_FORCE_RESTART);

        TCmsTestEnv env(8);
        env.AdvanceCurrentTime(TDuration::Minutes(3));

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", false, false, false,
                                        true, mode, TStatus::ALLOW,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)));
        if (disconnectNodes) {
            TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = false;
        }

        env.CheckPermissionRequest("user", false, false, false,
                                   true, mode, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
        if (disconnectNodes) {
            TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        }

        env.CheckPermissionRequest("user", false, false, false,
                                   true, mode, mode == MODE_KEEP_AVAILABLE ? TStatus::DISALLOW_TEMP : TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000));
        if (mode != MODE_KEEP_AVAILABLE) {
            return;
        }

        env.CheckDonePermission("user", res1.second[0]);

        env.CheckPermissionRequest("user", false, false, false,
                                   true, mode, disconnectNodes ? TStatus::DISALLOW_TEMP: TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));
        if (!disconnectNodes) {
            return;
        }

        TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = true;

        env.CheckPermissionRequest("user", false, false, false,
                                   true, mode, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(3), 60000000));
    }

    Y_UNIT_TEST(TestKeepAvailableMode)
    {
        TestAvailabilityMode(MODE_KEEP_AVAILABLE, false);
    }

    Y_UNIT_TEST(TestForceRestartMode)
    {
        TestAvailabilityMode(MODE_FORCE_RESTART, false);
    }

    Y_UNIT_TEST(TestKeepAvailableModeDisconnects)
    {
        TestAvailabilityMode(MODE_KEEP_AVAILABLE, true);
    }

    Y_UNIT_TEST(TestForceRestartModeDisconnects)
    {
        TestAvailabilityMode(MODE_FORCE_RESTART, true);
    }

    void TestAvailabilityModeScheduled(EAvailabilityMode mode,  bool disconnectNodes)
    {
        Y_ABORT_UNLESS(mode == MODE_KEEP_AVAILABLE
                 || mode == MODE_FORCE_RESTART);

        TCmsTestEnv env(8);
        env.AdvanceCurrentTime(TDuration::Minutes(3));

        auto res1 = env.ExtractPermissions
            (env.CheckPermissionRequest("user", true, false, true,
                                        true, mode, TStatus::ALLOW_PARTIAL,
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000),
                                        MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(2), 60000000)));
        if (disconnectNodes) {
            TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = false;
        }

        env.CheckRequest("user", res1.first, false, mode, TStatus::ALLOW_PARTIAL, 1);
        if (disconnectNodes) {
            TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        }

        env.CheckRequest("user", res1.first, false, mode,
                         mode == MODE_KEEP_AVAILABLE ? TStatus::DISALLOW_TEMP : TStatus::ALLOW,
                         mode == MODE_KEEP_AVAILABLE ? 0 : 1);
        if (mode != MODE_KEEP_AVAILABLE) {
            return;
        }

        env.CheckDonePermission("user", res1.second[0]);

        env.CheckRequest("user", res1.first, false, mode,
                         disconnectNodes ? TStatus::DISALLOW_TEMP : TStatus::ALLOW,
                         disconnectNodes ? 0 : 1);
        if (!disconnectNodes) {
            return;
        }

        TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = true;

        env.CheckRequest("user", res1.first, false, mode, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(TestKeepAvailableModeScheduled)
    {
        TestAvailabilityModeScheduled(MODE_KEEP_AVAILABLE, false);
    }

    Y_UNIT_TEST(TestForceRestartModeScheduled)
    {
        TestAvailabilityModeScheduled(MODE_FORCE_RESTART, false);
    }

    Y_UNIT_TEST(TestKeepAvailableModeScheduledDisconnects)
    {
        TestAvailabilityModeScheduled(MODE_KEEP_AVAILABLE, true);
    }

    Y_UNIT_TEST(TestForceRestartModeScheduledDisconnects)
    {
        TestAvailabilityModeScheduled(MODE_FORCE_RESTART, true);
    }

    Y_UNIT_TEST(TestOutdatedState)
    {
        TCmsTestEnv env(8);

        env.DisableBSBaseConfig();

        env.RequestState({}, TStatus::ERROR_TEMP);
        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ERROR_TEMP,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));
        env.CheckNotification(TStatus::ERROR_TEMP, "user", env.GetCurrentTime() + TDuration::Minutes(10),
                              MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(1, 0)));

        env.EnableBSBaseConfig();

        auto rec1 = env.CheckPermissionRequest
            ("user", true, false, true, true, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)),
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(1)));
        auto rid1 = rec1.GetRequestId();

        env.DisableBSBaseConfig();

        env.CheckRequest("user", rid1, true, TStatus::ERROR_TEMP);
    }

    Y_UNIT_TEST(TestSetResetMarkers)
    {
        TCmsTestEnv env(8, 4);

        env.CheckSetMarker(MARKER_DISK_BROKEN, "", TStatus::ERROR,
                           "::1",
                           env.PDiskId(0, 0),
                           env.PDiskId(1, 1),
                           env.PDiskId(2, 2),
                           env.PDiskId(3, 3));

        env.CheckResetMarker(MARKER_DISK_BROKEN, "", TStatus::ERROR,
                             "::1",
                             env.PDiskId(0, 0),
                             env.PDiskId(1, 1));
    }

    Y_UNIT_TEST(TestLoadLog)
    {
        TCmsTestEnv env(1);

        ui64 ts[11];
        ts[0] = 0;
        for (size_t i = 0; i < 10; ++i) {
            auto log = env.GetLogTail();
            UNIT_ASSERT_VALUES_EQUAL(log.LogRecordsSize(), i + 1);
            ts[i + 1] = env.GetCurrentTime().GetValue();
            for (size_t j = 0; j <= i; ++j) {
                auto &rec = log.GetLogRecords(j);
                UNIT_ASSERT(rec.GetTimestamp() >= ts[j]);
                UNIT_ASSERT(rec.GetTimestamp() <= ts[j+1]);
                CheckLoadLogRecord(rec, FQDNHostName(), env.GetNodeId(0),
                                   Sprintf("%s:%s", GetArcadiaSourceUrl(),
                                           GetArcadiaLastChange()));
            }
            env.AdvanceCurrentTime(TDuration::Seconds(1));
            env.RestartCms();
        }
    }

    Y_UNIT_TEST(CheckUnreplicatedDiskPreventsRestart)
    {
        TCmsTestEnv env(8);

        // OK to restart node-1.
        env.CheckPermissionRequest("user", false, true, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));

        // Some VDisk on node-0 is not replicated.
        auto &node = TFakeNodeWhiteboardService::Info[env.GetNodeId(0)];
        node.VDiskStateInfo.begin()->second.SetReplicated(false);

        // Not OK to restart node-1.
        env.CheckPermissionRequest("user", false, true, false, true, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000));
    }

    Y_UNIT_TEST(StateStorageTwoRings)
    {
        TTestEnvOpts options(5);
        options.VDisks = 0;
        options.NRings = 2;
        options.RingSize = 2;
        options.NToSelect = 2;

        TCmsTestEnv env(options);
        env.AdvanceCurrentTime(TDuration::Minutes(3));

        env.CheckPermissionRequest("user", false, false, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));

        env.CheckPermissionRequest("user", false, false, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));
    }

    Y_UNIT_TEST(StateStorageNodesFromOneRing)
    {
        TTestEnvOpts options(5);
        options.VDisks = 0;
        options.NRings = 2;
        options.RingSize = 2;
        options.NToSelect = 2;

        TCmsTestEnv env(options);
        env.AdvanceCurrentTime(TDuration::Minutes(3));

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"));

        env.CheckPermissionRequest("user", false, false, false, true, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    }

    Y_UNIT_TEST(StateStorageAvailabilityMode)
    {
        TTestEnvOpts options(10);
        options.VDisks = 0;
        options.NRings = 5;
        options.RingSize = 2;
        options.NToSelect = 5;

        TCmsTestEnv env(options);

        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
    }

    Y_UNIT_TEST(StateStorageTwoBrokenRings)
    {
        TTestEnvOpts options(10);
        options.VDisks = 0;
        options.NRings = 5;
        options.RingSize = 2;
        options.NToSelect = 5;

        TCmsTestEnv env(options);

        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        TFakeNodeWhiteboardService::Info[env.GetNodeId(2)].Connected = false;

        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
    }

    Y_UNIT_TEST(StateStorageRollingRestart)
    {
        TTestEnvOpts options(20);
        options.VDisks = 0;
        options.NRings = 6;
        options.RingSize = 3;
        options.NToSelect = 6;

        TCmsTestEnv env(options);

        TIntrusiveConstPtr<TStateStorageInfo> info = env.GetStateStorageInfo();

        THashMap<ui32, ui32> NodeToRing;
        THashSet<ui32> StateStorageNodes;

        for (ui32 ring = 0; ring < info->Rings.size(); ++ring) {
            for (auto& replica : info->Rings[ring].Replicas) {
                ui32 nodeId = replica.NodeId();

                NodeToRing[nodeId] = ring;
                StateStorageNodes.insert(nodeId);
            }
        }

        THashSet<TString> restarted;

        while(restarted.size() < env.GetNodeCount()) {

            TAutoPtr<NCms::TEvCms::TEvPermissionRequest> event = new NCms::TEvCms::TEvPermissionRequest;
            event->Record.SetUser("user");
            event->Record.SetPartialPermissionAllowed(true);
            event->Record.SetDryRun(false);
            event->Record.SetSchedule(false);

            for (ui32 i = 0; i < env.GetNodeCount(); ++i) {
                if (!restarted.contains(TStringBuilder() << env.GetNodeId(i))) {
                    AddActions(event, MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(i), 0, "storage"));
                }
            }

            NKikimrCms::TPermissionResponse res;

            // In the last request comes the permission
            // for all nodes of the same ring
            if (env.GetNodeCount() - restarted.size() == options.RingSize) {
                res = env.CheckPermissionRequest(event, TStatus::ALLOW);
            } else {
                res = env.CheckPermissionRequest(event, TStatus::ALLOW_PARTIAL);
            }

            ui32 permRing = env.GetNodeCount() + 1;
            for (auto& perm : res.GetPermissions()) {
                auto &action = perm.GetAction();
                restarted.insert(action.GetHost());
                env.CheckDonePermission("user", perm.GetId());

                auto nodeId = std::stoi(action.GetHost());
                if (!StateStorageNodes.contains(nodeId)) {
                    continue;
                }

                // Check that all state storages in permissions
                // from the same ring
                ui32 curRing = NodeToRing.at(nodeId);
                if (permRing >= options.NRings) { // we have not met a state storage yet
                    permRing = curRing;
                }

                UNIT_ASSERT_VALUES_EQUAL(permRing, curRing);
            }
        }
    }

    Y_UNIT_TEST(WalleCleanupTest)
    {
        TCmsTestEnv env(8);
        env.RestartCms();

        TAutoPtr<NCms::TEvCms::TEvPermissionRequest> event = new NCms::TEvCms::TEvPermissionRequest;
        event->Record.SetUser(WALLE_CMS_USER);
        event->Record.SetPartialPermissionAllowed(true);
        event->Record.SetDryRun(false);
        event->Record.SetSchedule(false);

        AddActions(event, MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 6000000000, "storage"));

        NKikimrCms::TPermissionResponse res;
        res = env.CheckPermissionRequest(event, TStatus::ALLOW);

        // Check that permission is stored
        env.CheckListPermissions(WALLE_CMS_USER, 1);

        // Adbance time to run cleanup
        env.AdvanceCurrentTime(TDuration::Minutes(3));
        env.RestartCms();

        // TODO:: перенести внутрь TCmsTestEnv
        TAutoPtr<TEvCms::TEvStoreWalleTask> event_store = new TEvCms::TEvStoreWalleTask;
        event_store->Task.TaskId = "walle-test-task-1";
        event_store->Task.RequestId = res.GetRequestId();

        for (auto &permission : res.GetPermissions())
            event_store->Task.Permissions.insert(permission.GetId());

        env.CheckWalleStoreTaskIsFailed(event_store.Release());
    }

    Y_UNIT_TEST(SysTabletsNode)
    {
        TCmsTestEnv env(TTestEnvOpts(6 /* nodes */, 0 /* vdisks */));
        env.EnableSysNodeChecking();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(0)].Connected = false;
        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(2)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(3)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(4)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                   MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));
    }

    Y_UNIT_TEST(Mirror3dcPermissions)
    {
        TTestEnvOpts options(18);
        options.UseMirror3dcErasure = true;
        options.VDisks = 9;
        options.DataCenterCount = 3;

        TCmsTestEnv env(options);

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));
        // 3dc disabled
        TFakeNodeWhiteboardService::Info[env.GetNodeId(7)].Connected = false;
        TFakeNodeWhiteboardService::Info[env.GetNodeId(4)].Connected = false;
        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));

        // 2dc disabled
        TFakeNodeWhiteboardService::Info[env.GetNodeId(7)].Connected = true;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(7), 60000000, "storage"));


        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));

        TFakeNodeWhiteboardService::Info[env.GetNodeId(5)].Connected = false;
        env.RestartCms();

        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"));
    }

    Y_UNIT_TEST(StateStorageLockedNodes)
    {
        TTestEnvOpts options(10);
        options.VDisks = 0;
        options.NRings = 9;
        options.RingSize = 1;
        options.NToSelect = 9;

        TCmsTestEnv env(options);

        TFakeNodeWhiteboardService::Info[env.GetNodeId(1)].Connected = false;
        TFakeNodeWhiteboardService::Info[env.GetNodeId(2)].Connected = false;

        // Node downtime simulation
        env.UpdateNodeStartTime(3, env.GetTimeProvider()->Now() + TDuration::Minutes(1));
        env.UpdateNodeStartTime(4, env.GetTimeProvider()->Now() + TDuration::Minutes(1));

        env.AdvanceCurrentTime(TDuration::Seconds(90));
        env.RestartCms();

        // Cant allow to restart ring when 2 restart 2 locked
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW_TEMP,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));
        // Can get node from locked ring
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"));
    }

    Y_UNIT_TEST(TestTwoOrMoreDisksFromGroupAtTheSameRequestBlock42)
    {
        TCmsTestEnv env(8);

        // It is impossible to get two or more permissions for one group in one request
        env.CheckPermissionRequest("user", true, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
        env.CheckPermissionRequest("user", true, true, false, true, MODE_FORCE_RESTART, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
        env.CheckPermissionRequest("user", true, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        // It's ok to get two permissions for one group if PartialPermissionAllowed is set to false
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
        env.CheckPermissionRequest("user", false, true, false, true, MODE_FORCE_RESTART, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        // Icorrect requests
        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"));
    }

    Y_UNIT_TEST(TestTwoOrMoreDisksFromGroupAtTheSameRequestMirror3dc)
    {
        TTestEnvOpts options(9);
        options.UseMirror3dcErasure = true;
        options.VDisks = 9;
        options.DataCenterCount = 3;

        TCmsTestEnv env(options);

        // It is impossible to get two or more permissions for one group in one request
        env.CheckPermissionRequest("user", true, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));
        env.CheckPermissionRequest("user", true, true, false, true, MODE_FORCE_RESTART, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(6), 60000000, "storage"));
        env.CheckPermissionRequest("user", true, true, false, true, MODE_MAX_AVAILABILITY, TStatus::ALLOW_PARTIAL,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(2), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"));

        // It's ok to get two permissions for one group if PartialPermissionAllowed is set to false
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));
        env.CheckPermissionRequest("user", false, true, false, true, MODE_FORCE_RESTART, TStatus::ALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));

        // Incorrect requests
        env.CheckPermissionRequest("user", false, true, false, true, MODE_KEEP_AVAILABLE, TStatus::DISALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(6), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(7), 60000000, "storage"));
        env.CheckPermissionRequest("user", false, true, false, true, MODE_MAX_AVAILABILITY, TStatus::DISALLOW,
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(3), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(4), 60000000, "storage"),
                                    MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(5), 60000000, "storage"));
    }

    Y_UNIT_TEST(TestProcessingQueue)
    {
        const ui32 nodes = 8;
        TCmsTestEnv env(nodes);
        env.CreateDefaultCmsPipe();

        auto makeRequest = [&env](ui32 nodeId) {
            auto ev = MakePermissionRequest("user", false, true, false,
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(nodeId), 60000000, "storage")
            );
            ev->Record.SetDuration(60000000);
            ev->Record.SetAvailabilityMode(MODE_FORCE_RESTART);
            env.SendToCms(ev.Release());
        };

        TActorId cmsActorId;
        {
            makeRequest(0);
            auto ev = env.GrabEdgeEvent<TEvCms::TEvPermissionResponse>(env.GetSender());
            cmsActorId = ev->Sender;
        }

        THolder<IEventHandle> delayedClusterInfo;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == cmsActorId && ev->GetTypeRewrite() == TCms::TEvPrivate::EvClusterInfo) {
                delayedClusterInfo.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // We need to send messages in fixed order
        for (ui32 i = 0; i < nodes; ++i) {
            makeRequest(i);
        }

        if (!delayedClusterInfo) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedClusterInfo](IEventHandle&) {
                return bool(delayedClusterInfo);
            });
            env.DispatchEvents(opts);
        }

        ui32 processQueueCount = 0;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == cmsActorId && ev->GetTypeRewrite() == TCms::TEvPrivate::EvProcessQueue) {
                ++processQueueCount;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.Send(delayedClusterInfo.Release(), 0, true);

        // Check responses order
        for (ui32 i = 0; i < nodes; ++i) {
            auto ev = env.GrabEdgeEvent<TEvCms::TEvPermissionResponse>(env.GetSender());
            const auto &rec = ev->Get()->Record;

            UNIT_ASSERT_VALUES_EQUAL(rec.permissions_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rec.permissions()[0].GetAction().GetHost(), ToString(env.GetNodeId(i)));
        }

        // first request processed without EvProcessQueue
        UNIT_ASSERT_VALUES_EQUAL(processQueueCount, nodes - 1);
        env.DestroyDefaultCmsPipe();
    }

    Y_UNIT_TEST(TestLogOperationsRollback)
    {
        TCmsTestEnv env(24);

        const ui32 requestsCount = 8;

        env.SetLimits(0, 0, 3, 0);
        env.CreateDefaultCmsPipe();
        for (ui32 i = 0; i < requestsCount; ++i) {
            auto req = MakePermissionRequest("user", false, true, false,
                            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(i), 60000000, "storage"),
                            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(i + 8), 60000000, "storage"),
                            MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(i + 16), 60000000, "storage"));
            req->Record.SetDuration(60000000);
            req->Record.SetAvailabilityMode(MODE_KEEP_AVAILABLE);

            env.SendToCms(req.Release());
        }
        env.DestroyDefaultCmsPipe();

        // Check responses order
        for (ui32 i = 0; i < requestsCount; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto reply = env.GrabEdgeEventRethrow<TEvCms::TEvPermissionResponse>(handle);
            const auto &rec = reply->Record;

            UNIT_ASSERT_VALUES_EQUAL(rec.permissions_size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(rec.status().code(), TStatus::ALLOW);
        }
    }

    Y_UNIT_TEST(RacyStartCollecting)
    {
        TCmsTestEnv env(8);
        env.CreateDefaultCmsPipe();

        auto makeRequest = [&env]() {
            auto ev = MakePermissionRequest("user", false, true, false,
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage")
            );
            ev->Record.SetDuration(60000000);
            ev->Record.SetAvailabilityMode(MODE_MAX_AVAILABILITY);
            env.SendToCms(ev.Release());
        };

        TActorId cmsActorId;
        {
            makeRequest();
            auto ev = env.GrabEdgeEvent<TEvCms::TEvPermissionResponse>(env.GetSender());
            cmsActorId = ev->Sender;
        }

        THolder<IEventHandle> delayedClusterInfo;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TCms::TEvPrivate::EvClusterInfo) {
                if (ev->Recipient == cmsActorId) {
                    delayedClusterInfo.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        makeRequest();
        if (!delayedClusterInfo) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedClusterInfo](IEventHandle&) {
                return bool(delayedClusterInfo);
            });
            env.DispatchEvents(opts);
        }

        bool requestCaptured = false;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvCms::EvPermissionRequest) {
                requestCaptured = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        makeRequest();
        if (!requestCaptured) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&requestCaptured](IEventHandle&) {
                return requestCaptured;
            });
            env.DispatchEvents(opts);
        }

        env.Send(delayedClusterInfo.Release(), 0, true);
        bool processQueueCaptured = false;
        bool clusterInfoCaptured = false;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TCms::TEvPrivate::EvProcessQueue) {
                if (ev->Sender == cmsActorId || ev->Recipient == cmsActorId) {
                    processQueueCaptured = true;
                }
            } else if (ev->GetTypeRewrite() == TCms::TEvPrivate::EvClusterInfo) {
                if (ev->Recipient == cmsActorId) {
                    clusterInfoCaptured = true;
                    if (processQueueCaptured) {
                        delayedClusterInfo.Reset(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        if (!delayedClusterInfo || !clusterInfoCaptured) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayedClusterInfo, &clusterInfoCaptured](IEventHandle&) {
                return delayedClusterInfo || clusterInfoCaptured;
            });
            env.DispatchEvents(opts);
        }

        requestCaptured = false;
        THolder<IEventHandle> delayedStartCollecting;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvCms::EvPermissionRequest) {
                requestCaptured = true;
            } else if (ev->GetTypeRewrite() == TCms::TEvPrivate::EvStartCollecting) {
                if (ev->Sender == cmsActorId || ev->Recipient == cmsActorId) {
                    delayedStartCollecting.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        makeRequest();
        if (!requestCaptured || !delayedStartCollecting) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&requestCaptured, &delayedStartCollecting](IEventHandle&) {
                return requestCaptured && delayedStartCollecting;
            });
            env.DispatchEvents(opts);
        }

        bool startCollecting = false;
        env.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TCms::TEvPrivate::EvStartCollecting) {
                if (ev->Sender == cmsActorId || ev->Recipient == cmsActorId) {
                    startCollecting = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        if (delayedClusterInfo) {
            env.Send(delayedClusterInfo.Release(), 0, true);
        }
        env.Send(delayedStartCollecting.Release(), 0, true);

        if (!startCollecting) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&startCollecting](IEventHandle&) {
                return startCollecting;
            });
            env.DispatchEvents(opts);
        }

        env.DestroyDefaultCmsPipe();
    }

    Y_UNIT_TEST(VDisksEvictionShouldFailWhileSentinelIsDisabled)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithoutSentinel());
        env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000)
            ),
            TStatus::ERROR
        );
    }

    Y_UNIT_TEST(VDisksEvictionShouldFailOnUnsupportedAction)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithSentinel());
        env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::REPLACE_DEVICES, env.GetNodeId(0), 60000000, env.PDiskName(0))
            ),
            TStatus::WRONG_REQUEST
        );
    }

    Y_UNIT_TEST(VDisksEvictionShouldFailOnMultipleActions)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithSentinel());
        env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(0), 60000000),
                MakeAction(TAction::SHUTDOWN_HOST, env.GetNodeId(1), 60000000)
            ),
            TStatus::WRONG_REQUEST
        );
    }

    Y_UNIT_TEST(VDisksEviction)
    {
        auto opts = TTestEnvOpts(8).WithSentinel();
        TCmsTestEnv env(opts);
        env.SetLogPriority(NKikimrServices::CMS, NLog::PRI_DEBUG);

        // ok
        auto request1 = env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 600000000, "storage")
            ),
            TStatus::DISALLOW_TEMP
        );
        // forbid another prepare request for same host
        env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 600000000, "storage")
            ),
            TStatus::WRONG_REQUEST
        );

        // "move" vdisks
        auto& node = TFakeNodeWhiteboardService::Info[env.GetNodeId(0)];
        node.VDisksMoved = true;
        node.VDiskStateInfo.clear();
        env.RegenerateBSConfig(TFakeNodeWhiteboardService::Config.MutableResponse()->MutableStatus(0)->MutableBaseConfig(), opts);

        // prepared
        auto permission1 = env.CheckRequest("user", request1.GetRequestId(), false, TStatus::ALLOW, 1);
        env.CheckRejectRequest("user", request1.GetRequestId(), false, TStatus::WRONG_REQUEST);
        env.CheckDonePermission("user", permission1.GetPermissions(0).GetId());

        // allow immediately
        auto request2 = env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 600000000, "storage")
            ),
            TStatus::ALLOW
        );
        UNIT_ASSERT_VALUES_EQUAL(request2.PermissionsSize(), 1);

        // check markers after restart
        env.RestartCms();
        env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 600000000, "storage")
            ),
            TStatus::WRONG_REQUEST
        );

        env.CheckRejectRequest("user", request2.GetRequestId(), false, TStatus::WRONG_REQUEST);
        env.CheckDonePermission("user", request2.GetPermissions(0).GetId());

        // restore vdisks
        node.VDisksMoved = false;
        env.RegenerateBSConfig(TFakeNodeWhiteboardService::Config.MutableResponse()->MutableStatus(0)->MutableBaseConfig(), opts);

        // prepare
        auto request3 = env.CheckPermissionRequest(
            MakePermissionRequest(TRequestOptions("user").WithEvictVDisks(),
                MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 600000000, "storage")
            ),
            TStatus::DISALLOW_TEMP
        );

        // reject until prepared
        env.CheckRejectRequest("user", request3.GetRequestId());
    }

    Y_UNIT_TEST(EmergencyDuringRollingRestart)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Emergency request
        auto emergency = env.CheckPermissionRequest
            ("user", true, false, true, true, -100, TStatus::ALLOW,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    
        // Rolling restart is blocked by emergency request
        env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::DISALLOW_TEMP, 0);

        // Done with emergency request
        env.CheckDonePermission("user", emergency.GetPermissions(0).GetId());

        // Rolling restart can continue
        env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(ScheduledEmergencyDuringRollingRestart)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Emergency request
        auto emergency = env.CheckPermissionRequest
            ("user", true, false, true, true, -100, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    
        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Rolling restart is blocked by emergency request
        env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::DISALLOW_TEMP, 0);

        // Emergency request is not blocked
        emergency = env.CheckRequest("user", emergency.GetRequestId(), false, TStatus::ALLOW, 1);

        // Done with emergency request
        env.CheckDonePermission("user", emergency.GetPermissions(0).GetId());

        // Rolling restart can continue
        env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(WalleRequestDuringRollingRestart)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Wall-E task is blocked by rolling restart
        env.CheckWalleCreateTask("task-1", "reboot", false, TStatus::DISALLOW_TEMP, env.GetNodeId(1));
    
        // Rolling restart is not blocked
        rollingRestart = env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting second node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Wall-E task can continue
        env.CheckWalleCheckTask("task-1", TStatus::ALLOW, env.GetNodeId(1));
    }

    Y_UNIT_TEST(ScheduledWalleRequestDuringRollingRestart)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Wall-E task is blocked by rolling restart
        env.CheckWalleCreateTask("task-1", "reboot", false, TStatus::DISALLOW_TEMP, env.GetNodeId(1));

        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Wall-E task is stil blocked
        env.CheckWalleCheckTask("task-1", TStatus::DISALLOW_TEMP, env.GetNodeId(1));

        // Rolling restart is not blocked
        rollingRestart = env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting second node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Wall-E task can continue
        env.CheckWalleCheckTask("task-1", TStatus::ALLOW, env.GetNodeId(1));
    }

    Y_UNIT_TEST(EnableCMSRequestPrioritiesFeatureFlag)
    {
        TCmsTestEnv env(8);
        // Start rolling restart with specified priority
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::WRONG_REQUEST,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));

        const TString expectedReason = "Unsupported: feature flag EnableCMSRequestPriorities is off";
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.GetStatus().GetReason(), expectedReason);
    }

    Y_UNIT_TEST(SamePriorityRequest)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Issue same priority request
        auto samePriorityRequest = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    
        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Rolling restart is not blocked by same priority request
        rollingRestart = env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting second node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Same priority can continue
        env.CheckRequest("user", samePriorityRequest.GetRequestId(), false, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(SamePriorityRequest2)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Issue same priority request
        auto samePriorityRequest = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
    
        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Request is not blocked by rolling restart of same priority
        samePriorityRequest = env.CheckRequest("user", samePriorityRequest.GetRequestId(), false, TStatus::ALLOW, 1);
        UNIT_ASSERT_VALUES_EQUAL(samePriorityRequest.PermissionsSize(), 1);

        // Done with same priority request permissions
        env.CheckDonePermission("user", samePriorityRequest.GetPermissions(0).GetId());

        // Rolling restart can continue
        env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
    }

    Y_UNIT_TEST(PriorityRange)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        const TString expectedReason = "Priority value is out of range";
        
        // Out of range priority
        auto request = env.CheckPermissionRequest
            ("user", true, false, true, true, -101, TStatus::WRONG_REQUEST,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(request.GetStatus().GetReason(), expectedReason);
        
        // Out of range priority
        request = env.CheckPermissionRequest
            ("user", true, false, true, true, 101, TStatus::WRONG_REQUEST,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(request.GetStatus().GetReason(), expectedReason);
    }

    Y_UNIT_TEST(WalleTasksDifferentPriorities)
    {
        TCmsTestEnv env(TTestEnvOpts(8).WithEnableCMSRequestPriorities());

        // Without node limits
        NKikimrCms::TCmsConfig config;
        config.MutableClusterLimits()->SetDisabledNodesLimit(0);
        config.MutableClusterLimits()->SetDisabledNodesRatioLimit(0);
        env.SetCmsConfig(config);

        // Start rolling restart
        auto rollingRestart = env.CheckPermissionRequest
            ("user", true, false, true, true, -80, TStatus::ALLOW_PARTIAL,
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(0), 60000000, "storage"),
             MakeAction(TAction::RESTART_SERVICES, env.GetNodeId(1), 60000000, "storage"));
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Done with restarting first node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());
    
        // Rolling restart is continue
        rollingRestart = env.CheckRequest("user", rollingRestart.GetRequestId(), false, TStatus::ALLOW, 1);
        UNIT_ASSERT_VALUES_EQUAL(rollingRestart.PermissionsSize(), 1);

        // Wall-E soft maintainance task is blocked by rolling restart
        env.CheckWalleCreateTask("task-1", "temporary-unreachable", false, TStatus::DISALLOW_TEMP, env.GetNodeId(2));

        // Wall-E reboot task is blocked by rolling restart
        env.CheckWalleCreateTask("task-2", "reboot", false, TStatus::DISALLOW_TEMP, env.GetNodeId(1));

        // Done with restarting second node
        env.CheckDonePermission("user", rollingRestart.GetPermissions(0).GetId());

        // Wall-E soft maintainance task is blocked by Wall-E reboot task
        env.CheckWalleCheckTask("task-1", TStatus::DISALLOW_TEMP, env.GetNodeId(2));

        // Wall-E reboot task can continue
        env.CheckWalleCheckTask("task-2", TStatus::ALLOW, env.GetNodeId(1));

        // Done with Wall-E reboot task
        env.CheckWalleRemoveTask("task-2");

        // Wall-E soft maintainance task can continue
        env.CheckWalleCheckTask("task-1", TStatus::ALLOW, env.GetNodeId(2));
    }
}

}
