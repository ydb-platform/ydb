#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/cms/cms_ut_common.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_events_private.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <util/generic/algorithm.h>

#include <initializer_list>

namespace NKikimr::NCmsTest {
namespace {

namespace NDbsc = NYdb::NBS::NBlockStore::NStorage::NDbsController;
using TEvCms = NCms::TEvCms;
using TEvDbsc = NDbsc::TEvDbsControllerPrivate;
using TPermissionRequest = TEvDbsc::TEvNodeMaintenancePermissionRequest;
using TPermissionResponse = TEvDbsc::TEvNodeMaintenancePermissionResponse;
using TActionState = Ydb::Maintenance::ActionState;

constexpr ui64 PartitionId = 70001;

// Real CMS and DBSC with TCmsTestEnv's mocked cluster; observers only record traffic.
class TRealDbsControllerEnv : public TCmsTestEnv {
public:
    TRealDbsControllerEnv()
        : TCmsTestEnv(8, 0)
    {
        for (ui32 i = 0; i < GetNodeCount(); ++i) {
            GetAppData(i).NbsEnabled = true;
            GetAppData(i).FeatureFlags.SetEnableCmsNbs2MaintenanceChecks(true);
        }

        const auto bootstrapper = CreateTestBootstrapper(*this,
            CreateTestTabletInfo(MakeDbsControllerID(), TTabletTypes::DbsController),
            &NDbsc::CreateDbsControllerTablet);
        EnableScheduleForActor(bootstrapper);
        WaitForDbsController();

        RequestObserver = AddObserver<TPermissionRequest>([this](TPermissionRequest::TPtr& ev) {
            Requests.push_back(ev->Get()->Record);
        });
        ResponseObserver = AddObserver<TPermissionResponse>([this](TPermissionResponse::TPtr& ev) {
            Responses.push_back(ev->Get()->Record);
        });
    }

    void WaitForDbsController() {
        // Unlike ResolveTablet, a retrying pipe waits for asynchronous bootstrap/reboot.
        const auto edge = AllocateEdgeActor();
        const auto pipe = ConnectToPipe(MakeDbsControllerID(), edge, 0, GetPipeConfigWithRetries());
        const auto connected = GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(edge, TDuration::Seconds(30));
        ClosePipe(pipe, edge, 0);
        UNIT_ASSERT_C(connected, "DBSC did not become ready within 30 seconds");
        UNIT_ASSERT_C(connected->Get()->Status == NKikimrProto::OK, connected->Get()->ToString());
    }

    void UpdateMap(ui64 partitionId, std::initializer_list<ui32> nodeIndexes) {
        auto request = MakeHolder<TEvDbsc::TEvUpdateDDiskMapRequest>();
        request->Record.SetTabletId(partitionId);
        auto* group = request->Record.MutablePartitionDDisks()->AddDirectBlockGroupsDDisks();
        for (const ui32 index : nodeIndexes) {
            auto* disks = group->AddDDiskIds();
            disks->SetHealth(NDbsc::NProto::ONLINE);
            auto* ddisk = disks->MutableDDisk();
            ddisk->SetNodeId(GetNodeId(index));
            ddisk->SetPDiskId(1);
            ddisk->SetDDiskSlotId(1);
            auto* buffer = disks->MutablePersistentBuffer();
            buffer->SetNodeId(GetNodeId(index));
            buffer->SetPDiskId(1);
            buffer->SetDDiskSlotId(2);
        }

        const auto edge = AllocateEdgeActor();
        SendToPipe(MakeDbsControllerID(), edge, request.Release(), 0, GetPipeConfigWithRetries());
        const auto response = GrabEdgeEventRethrow<TEvDbsc::TEvUpdateDDiskMapResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "DBSC did not acknowledge the DDisk map update");
        UNIT_ASSERT_C(!NYdb::NBS::HasError(response->Get()->Record.GetError()),
            response->Get()->Record.ShortDebugString());
    }

    Ydb::Maintenance::ActionGroup LockGroup(ui32 nodeIndex) const {
        return MakeActionGroup(MakeLockAction(GetNodeId(nodeIndex), TDuration::Minutes(10)));
    }

    void CheckDbsc(size_t count, std::initializer_list<ui32> nodeIndexes,
            NDbsc::NProto::EDecision decision, const TVector<ui64>& blockingPartitions = {}) const
    {
        // One request per attempt; no checks of subsets or injected replies.
        UNIT_ASSERT_VALUES_EQUAL(Requests.size(), count);
        UNIT_ASSERT_VALUES_EQUAL(Responses.size(), count);
        const auto& nodes = Requests.back().GetNodeIds();
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>(nodes.begin(), nodes.end()), NodeIds(nodeIndexes));
        const auto& response = Responses.back();
        UNIT_ASSERT_C(!NYdb::NBS::HasError(response.GetError()), response.ShortDebugString());
        UNIT_ASSERT_C(response.GetDecision() == decision, response.ShortDebugString());
        const auto& partitions = response.GetBlockingPartitionIds();
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui64>(partitions.begin(), partitions.end()), blockingPartitions);
    }

    template <typename TResult>
    void CheckActions(const TResult& result, std::initializer_list<ui32> nodeIndexes,
            bool allowed, const TString& reason = {}) const
    {
        UNIT_ASSERT_VALUES_EQUAL(result.action_group_states_size(), nodeIndexes.size());
        TVector<ui32> nodes;
        for (const auto& group : result.action_group_states()) {
            UNIT_ASSERT_VALUES_EQUAL(group.action_states_size(), 1);
            const auto& action = group.action_states(0);
            UNIT_ASSERT_VALUES_EQUAL(action.status(), allowed
                ? TActionState::ACTION_STATUS_PERFORMED : TActionState::ACTION_STATUS_PENDING);
            nodes.push_back(action.action().lock_action().scope().node_id());
            if (!reason.empty()) {
                UNIT_ASSERT_C(TString(action.details()).Contains(reason), action.ShortDebugString());
            }
        }
        Sort(nodes);
        UNIT_ASSERT_VALUES_EQUAL(nodes, NodeIds(nodeIndexes));
    }

private:
    TVector<ui32> NodeIds(std::initializer_list<ui32> indexes) const {
        TVector<ui32> nodes;
        for (const ui32 index : indexes) {
            nodes.push_back(GetNodeId(index));
        }
        Sort(nodes);
        return nodes;
    }

    TVector<NDbsc::NProto::TNodeMaintenancePermissionRequest> Requests;
    TVector<NDbsc::NProto::TNodeMaintenancePermissionResponse> Responses;
    NActors::TTestActorRuntimeBase::TEventObserverHolder RequestObserver;
    NActors::TTestActorRuntimeBase::TEventObserverHolder ResponseObserver;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TCmsNbs2RealDbsControllerTest) {
    Y_UNIT_TEST(DecisionsFromDiskMap) {
        TRealDbsControllerEnv env;

        // Before any partition reports its map, the real controller allows
        // maintenance. Dry-run must not leave a task or permissions behind.
        auto request = MakeHolder<TEvCms::TEvCreateMaintenanceTaskRequest>();
        request->Record.SetUserSID("test-user");
        auto& apiRequest = *request->Record.MutableRequest();
        auto& options = *apiRequest.mutable_task_options();
        options.set_task_uid("empty-map");
        options.set_availability_mode(Ydb::Maintenance::AVAILABILITY_MODE_FORCE);
        options.set_dry_run(true);
        AddActionGroups(apiRequest, env.LockGroup(0), env.LockGroup(1));
        env.SendToCms(request.Release());
        const auto dryRun = env.GrabEdgeEventRethrow<TEvCms::TEvMaintenanceTaskResponse>(
            env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT_C(dryRun, "CMS did not reply to dry-run with an empty DBSC map");
        UNIT_ASSERT_VALUES_EQUAL_C(dryRun->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS,
            dryRun->Get()->Record.ShortDebugString());
        env.CheckDbsc(1, {0, 1}, NDbsc::NProto::ALLOW);
        env.CheckActions(dryRun->Get()->Record.GetResult(), {0, 1}, true);
        env.CheckListPermissions("test-user", 0);
        env.CheckMaintenanceTaskGet("empty-map", Ydb::StatusIds::BAD_REQUEST);

        // A complete DBG has five healthy logical nodes. Losing one is safe;
        // losing two is not, even if CMS could grant their actions in sequence.
        env.UpdateMap(PartitionId, {0, 1, 2, 3, 4});
        const auto single = env.CheckMaintenanceTaskCreate("single-node", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(0));
        env.CheckDbsc(2, {0}, NDbsc::NProto::ALLOW);
        env.CheckActions(single, {0}, true);
        env.CheckListPermissions("test-user", 1);
        env.CheckCompleteAction(single.action_group_states(0).action_states(0).action_uid(), Ydb::StatusIds::SUCCESS);
        env.CheckListPermissions("test-user", 0);

        const auto batch = env.CheckMaintenanceTaskCreate("two-nodes", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(0), env.LockGroup(1));
        env.CheckDbsc(3, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
        env.CheckActions(batch, {0, 1}, false, ToString(PartitionId));
        env.CheckListPermissions("test-user", 0);
        const auto stored = env.CheckMaintenanceTaskGet("two-nodes", Ydb::StatusIds::SUCCESS);
        env.CheckActions(stored, {0, 1}, false, ToString(PartitionId));
        env.CheckDbsc(3, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
    }

    Y_UNIT_TEST(ManualApprovalWithRealDbsController) {
        TRealDbsControllerEnv env;
        env.UpdateMap(PartitionId, {0, 1, 2, 3, 4});

        const auto first = env.CheckMaintenanceTaskCreate("manual-blocker", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(0));
        env.CheckDbsc(1, {0}, NDbsc::NProto::ALLOW);
        env.CheckActions(first, {0}, true);

        const auto pending = env.CheckMaintenanceTaskCreate("manual-task", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(1));
        env.CheckDbsc(2, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
        env.CheckActions(pending, {1}, false, ToString(PartitionId));
        const auto scheduled = env.CheckListRequests("test-user", 1);
        const auto requestId = scheduled.GetRequests(0).GetRequestId();

        auto config = env.GetCmsConfig();
        config.SetDisableMaintenance(true);
        env.SetCmsConfig(config);

        // Manual approval bypasses DisableMaintenance, but not NBS2 safety.
        const auto denied = env.CheckApproveRequest("test-user", requestId, false, NKikimrCms::TStatus::DISALLOW_TEMP);
        env.CheckDbsc(3, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
        UNIT_ASSERT_VALUES_EQUAL(denied.ManuallyApprovedPermissionsSize(), 0);
        UNIT_ASSERT_C(denied.GetStatus().GetReason().Contains(ToString(PartitionId)), denied.ShortDebugString());
        env.CheckListPermissions("test-user", 1);
        env.CheckActions(env.CheckMaintenanceTaskGet("manual-task", Ydb::StatusIds::SUCCESS),
            {1}, false, ToString(PartitionId));
        const auto unchanged = env.CheckGetRequest("test-user", requestId);
        UNIT_ASSERT_VALUES_EQUAL(scheduled.GetRequests(0).SerializeAsString(), unchanged.GetRequests(0).SerializeAsString());

        env.CheckCompleteAction(first.action_group_states(0).action_states(0).action_uid(), Ydb::StatusIds::SUCCESS);
        const auto allowed = env.CheckApproveRequest("test-user", requestId);
        env.CheckDbsc(4, {1}, NDbsc::NProto::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(allowed.ManuallyApprovedPermissionsSize(), 1);
        const auto& permission = allowed.GetManuallyApprovedPermissions(0);
        UNIT_ASSERT_VALUES_EQUAL(permission.GetAction().GetHost(), ToString(env.GetNodeId(1)));
        UNIT_ASSERT(permission.GetDeadline() > env.GetCurrentTime().MicroSeconds());
        const auto stored = env.CheckMaintenanceTaskGet("manual-task", Ydb::StatusIds::SUCCESS);
        env.CheckActions(stored, {1}, true);
        UNIT_ASSERT_VALUES_EQUAL(stored.action_group_states(0).action_states(0).action_uid().action_id(), permission.GetId());
        env.CheckListPermissions("test-user", 1);

        // Manually approved permissions survive CMS restart.
        env.RestartCms();
        env.CheckActions(env.CheckMaintenanceTaskGet("manual-task", Ydb::StatusIds::SUCCESS), {1}, true);
        env.CheckListPermissions("test-user", 1);
        env.CheckDbsc(4, {1}, NDbsc::NProto::ALLOW);
    }

    Y_UNIT_TEST(ExistingLocksRefreshAndRestart) {
        TRealDbsControllerEnv env;
        env.UpdateMap(PartitionId, {0, 1, 2, 3, 4});

        const auto first = env.CheckMaintenanceTaskCreate("task-a", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(0));
        env.CheckDbsc(1, {0}, NDbsc::NProto::ALLOW);
        env.CheckActions(first, {0}, true);
        const auto firstAction = first.action_group_states(0).action_states(0).action_uid();
        env.CheckListPermissions("test-user", 1);

        // Node 0 is still physically healthy in the map, but its issued CMS
        // permission must be included in the check for another task's node 1.
        const auto second = env.CheckMaintenanceTaskCreate("task-b", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE, 1u, env.LockGroup(1));
        env.CheckDbsc(2, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
        env.CheckActions(second, {1}, false, ToString(PartitionId));
        env.CheckListPermissions("test-user", 1);

        const auto oldDbsc = ResolveTablet(env, MakeDbsControllerID());
        const auto oldCms = ResolveTablet(env, env.CmsId);
        RebootTablet(env, MakeDbsControllerID(), env.GetSender());
        env.WaitForDbsController();
        UNIT_ASSERT(ResolveTablet(env, MakeDbsControllerID()) != oldDbsc);
        env.RestartCms();
        UNIT_ASSERT(ResolveTablet(env, env.CmsId) != oldCms);

        const auto persistedFirst = env.CheckMaintenanceTaskGet("task-a", Ydb::StatusIds::SUCCESS);
        env.CheckActions(persistedFirst, {0}, true);
        UNIT_ASSERT_VALUES_EQUAL(persistedFirst.action_group_states(0).action_states(0).action_uid().action_id(),
            firstAction.action_id());
        const auto persistedSecond = env.CheckMaintenanceTaskGet("task-b", Ydb::StatusIds::SUCCESS);
        env.CheckActions(persistedSecond, {1}, false, ToString(PartitionId));
        env.CheckListPermissions("test-user", 1);

        // Do not repopulate DBSC after reboot: DENY proves that both its map
        // and CMS's existing permission survived, not just the pending task.
        const auto denied = env.CheckMaintenanceTaskRefresh("task-b", Ydb::StatusIds::SUCCESS);
        env.CheckDbsc(3, {0, 1}, NDbsc::NProto::DENY, {PartitionId});
        env.CheckActions(denied, {1}, false, ToString(PartitionId));
        env.CheckListPermissions("test-user", 1);

        env.CheckCompleteAction(firstAction, Ydb::StatusIds::SUCCESS);
        env.CheckListPermissions("test-user", 0);
        const auto allowed = env.CheckMaintenanceTaskRefresh("task-b", Ydb::StatusIds::SUCCESS);
        env.CheckDbsc(4, {1}, NDbsc::NProto::ALLOW);
        env.CheckActions(allowed, {1}, true);
        env.CheckListPermissions("test-user", 1);
        const auto stored = env.CheckMaintenanceTaskGet("task-b", Ydb::StatusIds::SUCCESS);
        env.CheckActions(stored, {1}, true);
        UNIT_ASSERT_VALUES_EQUAL(stored.action_group_states(0).action_states(0).action_uid().action_id(),
            allowed.action_group_states(0).action_states(0).action_uid().action_id());
        env.CheckDbsc(4, {1}, NDbsc::NProto::ALLOW);
    }
}

} // namespace NKikimr::NCmsTest
