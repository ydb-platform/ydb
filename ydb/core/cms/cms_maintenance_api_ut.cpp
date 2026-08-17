#include "cms_ut_common.h"
#include "cms_maintenance_api_ut_enums.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/hive.h>

#include <util/generic/algorithm.h>
#include <util/random/fast.h>

namespace NKikimr::NCmsTest {

using namespace Ydb::Maintenance;

namespace {

Ydb::Maintenance::AvailabilityMode ToAvailabilityMode(EAvailabilityCase mode) {
    switch (mode) {
        case EAvailabilityCase::Strong:
            return Ydb::Maintenance::AVAILABILITY_MODE_STRONG;
        case EAvailabilityCase::KeepAvailable:
            return Ydb::Maintenance::AVAILABILITY_MODE_WEAK;
        case EAvailabilityCase::Force:
            return Ydb::Maintenance::AVAILABILITY_MODE_FORCE;
    }
}

void RunRollingRestartSimulation(ui32 totalNodes, ui32 cap, EAvailabilityCase availabilityCase) {
    Y_ABORT_UNLESS(totalNodes > 0);
    Y_ABORT_UNLESS(cap > 0);

    const auto availabilityMode = ToAvailabilityMode(availabilityCase);

    TCmsTestEnv env(totalNodes);

    auto ev = std::make_unique<NCms::TEvCms::TEvCreateMaintenanceTaskRequest>();
    ev->Record.SetUserSID("test-user");
    auto* req = ev->Record.MutableRequest();
    req->mutable_task_options()->set_task_uid("task-1");
    req->mutable_task_options()->set_availability_mode(availabilityMode);
    req->mutable_task_options()->set_max_inflight_actions(cap);
    for (ui32 i = 0; i < totalNodes; ++i) {
        auto group = MakeActionGroup(
            MakeLockAction(env.GetNodeId(i), TDuration::Minutes(10)));
        req->add_action_groups()->CopyFrom(group);
    }
    env.SendToCms(ev.release());
    auto createReply = env.GrabEdgeEvent<NCms::TEvCms::TEvMaintenanceTaskResponse>();
    UNIT_ASSERT_VALUES_EQUAL(createReply->Record.GetStatus(), Ydb::StatusIds::SUCCESS);

    auto split = [](const Ydb::Maintenance::MaintenanceTaskResult& r) {
        TVector<Ydb::Maintenance::ActionUid> performed;
        ui32 pending = 0;
        for (const auto& group : r.action_group_states()) {
            UNIT_ASSERT_VALUES_EQUAL(group.action_states().size(), 1);
            const auto& s = group.action_states(0);
            switch (s.status()) {
                case ActionState::ACTION_STATUS_PERFORMED:
                    performed.push_back(s.action_uid());
                    break;
                case ActionState::ACTION_STATUS_PENDING:
                    ++pending;
                    break;
                default:
                    UNIT_FAIL("Unexpected action status: " << static_cast<int>(s.status()));
            }
        }
        return std::make_pair(std::move(performed), pending);
    };

    Ydb::Maintenance::MaintenanceTaskResult current = createReply->Record.GetResult();
    TReallyFastRng32 rng(42);
    ui32 totalCompleted = 0;
    ui32 iterations = 0;

    while (totalCompleted < totalNodes) {
        auto [performed, pending] = split(current);
        const ui32 total = performed.size() + pending;

        UNIT_ASSERT_VALUES_EQUAL_C(total, totalNodes - totalCompleted,
            "iter=" << iterations << " mode=" << ToString(availabilityCase));

        if (availabilityCase == EAvailabilityCase::Force) {
            UNIT_ASSERT_VALUES_EQUAL_C(performed.size(), Min<ui32>(cap, total),
                "iter=" << iterations << " mode=" << ToString(availabilityCase));
        } else {
            UNIT_ASSERT_C(performed.size() <= Min<ui32>(cap, total),
                "cap exceeded: performed=" << performed.size()
                << " cap=" << cap << " total=" << total
                << " iter=" << iterations
                << " mode=" << ToString(availabilityCase));
        }
        UNIT_ASSERT_C(!performed.empty(),
            "no progress: nothing PERFORMED at iter=" << iterations
            << " mode=" << ToString(availabilityCase));

        const ui32 toComplete = 1 + rng.Uniform(performed.size());
        for (ui32 i = 0; i < toComplete; ++i) {
            env.CheckCompleteAction(performed[i], Ydb::StatusIds::SUCCESS);
        }
        totalCompleted += toComplete;

        if (totalCompleted < totalNodes) {
            current = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);
        }

        ++iterations;
        UNIT_ASSERT_C(iterations <= totalNodes,
            "too many iterations: " << iterations
            << " mode=" << ToString(availabilityCase));
    }

    UNIT_ASSERT_VALUES_EQUAL(totalCompleted, totalNodes);
}

} // namespace

Y_UNIT_TEST_SUITE(TMaintenanceApiTest) {
    Y_UNIT_TEST(ManyActionGroupsWithSingleAction) {
        TCmsTestEnv env(8);

        auto response = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a1 = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(1).action_states().size(), 1);
        const auto &a2 = response.action_group_states(1).action_states(0);

        bool hasPerformedAction = a1.status() == ActionState::ACTION_STATUS_PERFORMED
            || a2.status() == ActionState::ACTION_STATUS_PERFORMED;
        bool hasPendingAction = a1.status() == ActionState::ACTION_STATUS_PENDING
            || a2.status() == ActionState::ACTION_STATUS_PENDING;
        UNIT_ASSERT(hasPerformedAction && hasPendingAction);
    }

    Y_UNIT_TEST(SingleCompositeActionGroup) {
        TCmsTestEnv env(16);

        // lock a node to prevent task-2 from taking a lock on a node from the same storage group
        env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );
        
        auto response = env.CheckMaintenanceTaskCreate("task-2", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10)),
                MakeLockAction(env.GetNodeId(9), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 2);
        const auto &a1 = response.action_group_states(0).action_states(0);
        const auto &a2 = response.action_group_states(0).action_states(1);

        bool allActionsPending = a1.status() == ActionState::ACTION_STATUS_PENDING
            && a2.status() == ActionState::ACTION_STATUS_PENDING;
        UNIT_ASSERT(allActionsPending);
    }

    Y_UNIT_TEST(CompositeActionGroupSameStorageGroup) {
        TCmsTestEnv env(8);

        env.CheckMaintenanceTaskCreate("task-2", Ydb::StatusIds::BAD_REQUEST,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10)),
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );
    }

    Y_UNIT_TEST(ActionReason) {
        TCmsTestEnv env(8);

        auto response = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a1 = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a1.status(), ActionState::ACTION_STATUS_PERFORMED);
        UNIT_ASSERT_VALUES_EQUAL(a1.reason(), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT(a1.details().empty());

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(1).action_states().size(), 1);
        const auto &a2 = response.action_group_states(1).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a2.status(), ActionState::ACTION_STATUS_PENDING);
        UNIT_ASSERT_VALUES_EQUAL(a2.reason(), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
        UNIT_ASSERT(a2.details().Contains("too many unavailable vdisks"));
    }

    Y_UNIT_TEST(SimplifiedMirror3DC) {
        TTestEnvOpts options(3);
        options.UseMirror3dcErasure = true;
        options.DataCenterCount = 3;
        TCmsTestEnv env(options);

        auto response = env.CheckMaintenanceTaskCreate(
            "task-1",
            Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_WEAK,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a.status(), ActionState::ACTION_STATUS_PERFORMED);
    }

    Y_UNIT_TEST(CreateTime) {
        TCmsTestEnv env(8);

        // Move time to make it different from initial time
        env.AdvanceCurrentTime(TDuration::MilliSeconds(5500));

        // Create sets create time
        auto createResult = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );
        UNIT_ASSERT_VALUES_UNEQUAL(createResult.create_time().seconds(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(createResult.create_time().nanos(), 0);

        // Move time to make it different from create time
        env.AdvanceCurrentTime(TDuration::MilliSeconds(5500));

        // Get doesn't update create time
        auto getResult = env.CheckMaintenanceTaskGet("task-1", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(getResult.create_time().seconds(), createResult.create_time().seconds());
        UNIT_ASSERT_VALUES_EQUAL(getResult.create_time().nanos(), createResult.create_time().nanos());

        // Refresh doesn't update create time
        auto refreshResult = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(refreshResult.create_time().seconds(), createResult.create_time().seconds());
        UNIT_ASSERT_VALUES_EQUAL(refreshResult.create_time().nanos(), createResult.create_time().nanos());
    }

    Y_UNIT_TEST(LastRefreshTime) {
        TCmsTestEnv env(8);

        // Move time to make it different from initial time
        env.AdvanceCurrentTime(TDuration::MilliSeconds(5500));

        // Create includes refresh, so create sets last refresh time
        auto createResult = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );
        UNIT_ASSERT_VALUES_UNEQUAL(createResult.last_refresh_time().seconds(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(createResult.last_refresh_time().nanos(), 0);

        // Move time to make it different from create time
        env.AdvanceCurrentTime(TDuration::MilliSeconds(5500));

        // Get doesn't update last refresh time
        auto getResult = env.CheckMaintenanceTaskGet("task-1", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(getResult.last_refresh_time().seconds(), createResult.last_refresh_time().seconds());
        UNIT_ASSERT_VALUES_EQUAL(getResult.last_refresh_time().nanos(), createResult.last_refresh_time().nanos());

        // Refresh updates last refresh time
        auto refreshResult = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_UNEQUAL(refreshResult.last_refresh_time().seconds(), createResult.last_refresh_time().seconds());
        UNIT_ASSERT_VALUES_UNEQUAL(refreshResult.last_refresh_time().nanos(), createResult.last_refresh_time().nanos());
    }

    Y_UNIT_TEST(RequestReplaceDevicePDisk) {
        std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)> serializeIdFn = [](NCms::TPDiskID& pdiskId, Ydb::Maintenance::ActionScope_PDisk* pdisk) {
            auto* id = pdisk->mutable_pdisk_id();
            id->set_node_id(pdiskId.NodeId);
            id->set_pdisk_id(pdiskId.DiskId);
        };

        std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)> deserializeIdFn = [](const Ydb::Maintenance::ActionScope_PDisk& pdisk, NCms::TPDiskID& pdiskId) {
            auto& id = pdisk.Getpdisk_id();
            pdiskId.NodeId = id.node_id();
            pdiskId.DiskId = id.pdisk_id();
        };

        std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)> serializeLocationFn = [](NCms::TPDiskID& pdiskId, Ydb::Maintenance::ActionScope_PDisk* pdisk) {
            auto* location = pdisk->mutable_pdisk_location();
            location->set_host("::1"); // All nodes live on this host.
            location->set_path("/" + std::to_string(pdiskId.NodeId) + "/pdisk-" + std::to_string(pdiskId.DiskId) + ".data");
        };

        std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)> deserializeLocationFn = [](const Ydb::Maintenance::ActionScope_PDisk& pdisk, NCms::TPDiskID& pdiskId) {
            auto& location = pdisk.Getpdisk_location();
            sscanf(location.path().c_str(), "/%d/pdisk-%d.data", &pdiskId.NodeId, &pdiskId.DiskId);
        };

        std::vector<std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)>> serializeFns = {serializeIdFn, serializeLocationFn};
        
        std::vector<std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)>> deserializeFns = {deserializeIdFn, deserializeLocationFn};

        for (size_t i = 0; i < serializeFns.size(); i++) {
            auto serializeFn = serializeFns[i];
            auto deserializeFn = deserializeFns[i];

            TTestEnvOpts envOpts(8);
            envOpts.WithSentinel();

            TCmsTestEnv env(envOpts);

            auto req = std::make_unique<NKikimr::NCms::TEvCms::TEvCreateMaintenanceTaskRequest>();

            Ydb::Maintenance::CreateMaintenanceTaskRequest* rec = req->Record.MutableRequest();

            req->Record.SetUserSID("user");

            auto* options = rec->mutable_task_options();
            options->set_availability_mode(::Ydb::Maintenance::AVAILABILITY_MODE_STRONG);

            auto* actionGroup = rec->add_action_groups();
            auto* action = actionGroup->Addactions();
            auto* lockAction = action->mutable_lock_action();

            auto* scope = lockAction->mutable_scope();

            auto* pdisk = scope->mutable_pdisk();
            NCms::TPDiskID disk = env.PDiskId(0);
            serializeFn(disk, pdisk);
            lockAction->mutable_duration()->set_seconds(60);

            env.SendToCms(req.release());

            auto response = env.GrabEdgeEvent<NCms::TEvCms::TEvMaintenanceTaskResponse>();

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), Ydb::StatusIds_StatusCode_SUCCESS);
            
            {
                auto& result = response->Record.GetResult();
                UNIT_ASSERT_VALUES_EQUAL(result.action_group_states().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result.action_group_states(0).action_states().size(), 1);
                const auto &actionState = result.action_group_states(0).action_states(0);

                UNIT_ASSERT(actionState.has_action());
                const auto& action = actionState.action();
                UNIT_ASSERT(action.has_lock_action());
                const auto& lockAction = action.lock_action();

                UNIT_ASSERT(lockAction.has_scope());
                const auto& scope = lockAction.scope();
                UNIT_ASSERT(scope.has_pdisk());
                const auto& pdisk = scope.pdisk();
                
                NCms::TPDiskID resultDisk;
                deserializeFn(pdisk, resultDisk);
                UNIT_ASSERT_VALUES_EQUAL(resultDisk, disk);
            }

            env.CheckRequest("user", "user-r-1", false, NKikimrCms::TStatus::TStatus::WRONG_REQUEST);
        }
    }

    Y_UNIT_TEST(ForceAvailabilityMode) {
        TCmsTestEnv env(8);

        NKikimrCms::TCmsConfig config;
        config.MutableClusterLimits()->SetDisabledNodesLimit(1);
        config.MutableClusterLimits()->SetDisabledNodesRatioLimit(30);
        config.MutableTenantLimits()->SetDisabledNodesLimit(1);
        config.MutableTenantLimits()->SetDisabledNodesRatioLimit(30);
        env.SetCmsConfig(config);

        auto response = env.CheckMaintenanceTaskCreate(
            "task-1",
            Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(2), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(3), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(4), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(5), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(6), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(7), TDuration::Minutes(10))
            )
        );

        // Everything is allowed to perform maintenance regardless of the failure model
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 8);
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(i).action_states().size(), 1);
            const auto& a = response.action_group_states(i).action_states(0);
            UNIT_ASSERT_VALUES_EQUAL(a.status(), ActionState::ACTION_STATUS_PERFORMED);
        }
    }

    Y_UNIT_TEST(TestDrainAction) {
        TCmsTestEnv env(8);

        ui64 requestDrainCount = 0;
        auto observer = env.AddObserver([&](auto&& ev) {
            if (ev->Type == TEvHive::EvDrainNode) {
                env.Send(new IEventHandle(ev->Sender, env.GetSender(), new TEvHive::TEvDrainNodeAck(), 0, ev->Cookie), 0);
            }
            if (ev->Type == TEvHive::EvRequestDrainInfo) {
                auto response = std::make_unique<TEvHive::TEvResponseDrainInfo>();
                response->Record.SetNodeId(env.GetNodeId(1));
                response->Record.SetDrainSeqNo(requestDrainCount++);
                response->Record.MutableDrainInProgress();
                env.Send(new IEventHandle(ev->Sender, env.GetSender(), response.release(), 0, ev->Cookie), 0);
            }
        });

        env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeDrainAction(env.GetNodeId(1))
            )
        );

        auto getResult1 = env.CheckMaintenanceTaskGet("task-1", Ydb::StatusIds::SUCCESS);
        auto status1 = getResult1.action_group_states(0).action_states(0).status();
        UNIT_ASSERT(status1 == ActionState::ACTION_STATUS_IN_PROGRESS);
        auto getResult2 = env.CheckMaintenanceTaskGet("task-1", Ydb::StatusIds::SUCCESS);
        auto status2 = getResult2.action_group_states(0).action_states(0).status();
        UNIT_ASSERT(status2 == ActionState::ACTION_STATUS_PERFORMED);
    }

    Y_UNIT_TEST(TestCordonAction) {
        TCmsTestEnv env(8);

        struct TMockHive {
            TCmsTestEnv& Runtime;
            bool Alive = true;

            TMockHive(TCmsTestEnv& env) : Runtime(env) {}

            void Observe(IEventHandle::TPtr& ev) {
                if (ev->Type == TEvHive::EvSetDown) {
                    if (Alive) {
                        Runtime.Send(new IEventHandle(ev->Sender, Runtime.GetSender(), new TEvHive::TEvSetDownReply(), 0, ev->Cookie), 0);
                    } else {
                        Runtime.Send(new IEventHandle(ev->Sender, Runtime.GetSender(), new TEvTabletPipe::TEvClientDestroyed(0, {}, {}), 0, ev->Cookie), 0);
                    }
                }
            }
        };

        TMockHive hive(env);
        auto observer = env.AddObserver([&](auto&& ev) { hive.Observe(ev); });

        env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeCordonAction(env.GetNodeId(1))
            )
        );

        auto getResult = env.CheckMaintenanceTaskGet("task-1", Ydb::StatusIds::SUCCESS);
        const auto& actionState = getResult.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(actionState.status(), ActionState::ACTION_STATUS_PERFORMED);
        UNIT_ASSERT(actionState.action().has_cordon_action());

        hive.Alive = false;
        env.CheckMaintenanceTaskDrop("task-1", Ydb::StatusIds::UNAVAILABLE);
        hive.Alive = true;
        env.CheckMaintenanceTaskDrop("task-1", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(MaxInflightActionsBlocksRefresh) {
        TCmsTestEnv env(8);

        auto createResult = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE,
            /* maxInflightActions = */ 3u,
            MakeActionGroup(MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(2), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(3), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(4), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(5), TDuration::Minutes(10)))
        );

        ui32 createPerformed = 0;
        for (const auto& group : createResult.action_group_states()) {
            if (group.action_states(0).status() == ActionState::ACTION_STATUS_PERFORMED) {
                ++createPerformed;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(createPerformed, 3);

        auto refreshResult = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);

        ui32 refreshPerformed = 0;
        ui32 refreshPending = 0;
        for (const auto& group : refreshResult.action_group_states()) {
            const auto& state = group.action_states(0);
            if (state.status() == ActionState::ACTION_STATUS_PERFORMED) {
                ++refreshPerformed;
            } else if (state.status() == ActionState::ACTION_STATUS_PENDING) {
                ++refreshPending;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(refreshPerformed, 3);
        UNIT_ASSERT_VALUES_EQUAL(refreshPending, 3);
    }

    Y_UNIT_TEST(MaxInflightActionsReleasesQuotaOnCompleteAction) {
        TCmsTestEnv env(8);

        auto createResult = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE,
            /* maxInflightActions = */ 3u,
            MakeActionGroup(MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(2), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(3), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(4), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(5), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(6), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(7), TDuration::Minutes(10)))
        );

        TVector<Ydb::Maintenance::ActionUid> performed;
        for (const auto& group : createResult.action_group_states()) {
            const auto& state = group.action_states(0);
            if (state.status() == ActionState::ACTION_STATUS_PERFORMED) {
                performed.push_back(state.action_uid());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(performed.size(), 3);

        env.CheckCompleteAction(performed[0], Ydb::StatusIds::SUCCESS);
        env.CheckCompleteAction(performed[1], Ydb::StatusIds::SUCCESS);

        auto refreshResult = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);

        ui32 refreshPerformed = 0;
        ui32 refreshPending = 0;
        for (const auto& group : refreshResult.action_group_states()) {
            const auto& state = group.action_states(0);
            if (state.status() == ActionState::ACTION_STATUS_PERFORMED) {
                ++refreshPerformed;
            } else if (state.status() == ActionState::ACTION_STATUS_PENDING) {
                ++refreshPending;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(refreshPerformed, 3);
        UNIT_ASSERT_VALUES_EQUAL(refreshPending, 5 - 2);
    }

    Y_UNIT_TEST(MaxInflightActionsRollingRestartBatchNormal, EAvailabilityCase) {
        RunRollingRestartSimulation(100, 5, Arg<0>());
    }

    Y_UNIT_TEST(MaxInflightActionsRollingRestartBatchMin, EAvailabilityCase) {
        RunRollingRestartSimulation(100, 1, Arg<0>());
    }

    Y_UNIT_TEST(MaxInflightActionsRollingRestartBatchFull, EAvailabilityCase) {
        RunRollingRestartSimulation(100, 100, Arg<0>());
    }

    Y_UNIT_TEST(MaxInflightActionsRollingRestartBatchOverCap, EAvailabilityCase) {
        RunRollingRestartSimulation(10, 20, Arg<0>());
    }

    Y_UNIT_TEST(MaxInflightActionsZeroMeansUnlimited) {
        TCmsTestEnv env(8);

        auto response = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE,
            /* maxInflightActions = */ 0u,
            MakeActionGroup(MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(2), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(3), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(4), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(5), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(6), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(7), TDuration::Minutes(10)))
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 8);
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(i).action_states().size(), 1);
            const auto& state = response.action_group_states(i).action_states(0);
            UNIT_ASSERT_VALUES_EQUAL(state.status(), ActionState::ACTION_STATUS_PERFORMED);
        }
    }

    Y_UNIT_TEST(MaxInflightActionsSurvivesCmsRestart) {
        TCmsTestEnv env(8);

        auto createResult = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_FORCE,
            /* maxInflightActions = */ 3u,
            MakeActionGroup(MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(2), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(3), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(4), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(5), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(6), TDuration::Minutes(10))),
            MakeActionGroup(MakeLockAction(env.GetNodeId(7), TDuration::Minutes(10)))
        );

        auto countStatuses = [](const Ydb::Maintenance::MaintenanceTaskResult& r) {
            TVector<Ydb::Maintenance::ActionUid> performed;
            ui32 pending = 0;
            for (const auto& group : r.action_group_states()) {
                UNIT_ASSERT_VALUES_EQUAL(group.action_states().size(), 1);
                const auto& state = group.action_states(0);
                if (state.status() == ActionState::ACTION_STATUS_PERFORMED) {
                    performed.push_back(state.action_uid());
                } else if (state.status() == ActionState::ACTION_STATUS_PENDING) {
                    ++pending;
                }
            }
            return std::make_pair(std::move(performed), pending);
        };

        auto [createPerformed, createPending] = countStatuses(createResult);
        UNIT_ASSERT_VALUES_EQUAL(createPerformed.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(createPending, 5);

        env.RestartCms();

        auto refreshAfterRestart = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);
        auto [postRestartPerformed, postRestartPending] = countStatuses(refreshAfterRestart);
        UNIT_ASSERT_VALUES_EQUAL(postRestartPerformed.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(postRestartPending, 5);

        env.CheckCompleteAction(postRestartPerformed[0], Ydb::StatusIds::SUCCESS);
        env.CheckCompleteAction(postRestartPerformed[1], Ydb::StatusIds::SUCCESS);

        auto refreshAfterCompletion = env.CheckMaintenanceTaskRefresh("task-1", Ydb::StatusIds::SUCCESS);
        auto [completionPerformed, completionPending] = countStatuses(refreshAfterCompletion);
        UNIT_ASSERT_VALUES_EQUAL(completionPerformed.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(completionPending, 3);
    }

    Y_UNIT_TEST(DisableMaintenance) {
        TCmsTestEnv env(16);

        auto r1 = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );
        UNIT_ASSERT_VALUES_EQUAL(r1.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r1.action_group_states(0).action_states().size(), 1);
        const auto &a1 = r1.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a1.status(), ActionState::ACTION_STATUS_PERFORMED);

        // Pending task
        auto r2 = env.CheckMaintenanceTaskCreate("task-2", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );
        UNIT_ASSERT_VALUES_EQUAL(r2.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r2.action_group_states(0).action_states().size(), 1);
        const auto &a2 = r2.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a2.status(), ActionState::ACTION_STATUS_PENDING);

        // Disable maintenance
        NKikimrCms::TCmsConfig config;
        config.SetDisableMaintenance(true);
        env.SetCmsConfig(config);

        env.CheckCompleteAction(a1.action_uid(), Ydb::StatusIds::SUCCESS);

        // Requests should fail
        env.CheckMaintenanceTaskCreate("task-3", Ydb::StatusIds::UNAVAILABLE,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );
        env.CheckMaintenanceTaskRefresh("task-2", Ydb::StatusIds::UNAVAILABLE);

        // Enable maintenance back
        config.SetDisableMaintenance(false);
        env.SetCmsConfig(config);

        // Requests should be ok
        auto r3 = env.CheckMaintenanceTaskCreate("task-3", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(9), TDuration::Minutes(10))
            )
        );
        UNIT_ASSERT_VALUES_EQUAL(r3.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r3.action_group_states(0).action_states().size(), 1);
        const auto &a3 = r3.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a3.status(), ActionState::ACTION_STATUS_PERFORMED);

        auto r4 = env.CheckMaintenanceTaskRefresh("task-2", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(r4.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(r4.action_group_states(0).action_states().size(), 1);
        const auto &a4 = r4.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a4.status(), ActionState::ACTION_STATUS_PERFORMED);
    }
}

} // namespace NKikimr::NCmsTest 
