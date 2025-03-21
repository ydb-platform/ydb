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
}

} // namespace NKikimr::NCmsTest 
