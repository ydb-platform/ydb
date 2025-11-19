#include "cms_ut_common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCmsTest {

using namespace Ydb::Maintenance;

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
        UNIT_ASSERT(a1.reason_details().empty());

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(1).action_states().size(), 1);
        const auto &a2 = response.action_group_states(1).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a2.status(), ActionState::ACTION_STATUS_PENDING);
        UNIT_ASSERT_VALUES_EQUAL(a2.reason(), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
        UNIT_ASSERT(a2.reason_details().Contains("too many unavailable vdisks"));
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
}

} // namespace NKikimr::NCmsTest 
