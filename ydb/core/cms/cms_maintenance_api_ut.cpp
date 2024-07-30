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
}

} // namespace NKikimr::NCmsTest 
