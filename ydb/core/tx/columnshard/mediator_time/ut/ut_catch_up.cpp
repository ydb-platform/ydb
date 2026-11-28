#include <ydb/core/tx/columnshard/mediator_time/catch_up.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NColumnShard;

Y_UNIT_TEST_SUITE(TMediatorTimeCatchUp) {
    using EStatus = TMediatorTimeCatchUp::EStatus;
    Y_UNIT_TEST(WaitsForFirstCoordinator) {
        TMediatorTimeCatchUp catchUp({ 1, 2 });
        UNIT_ASSERT(catchUp.Check(Max<ui64>()).Status == EStatus::WaitingForCoordinator);
    }

    Y_UNIT_TEST(TargetIsMinOverAnsweredCoordinators) {
        TMediatorTimeCatchUp catchUp({ 1, 2, 3 });
        catchUp.OnNextReadStep(1, 200);
        UNIT_ASSERT_VALUES_EQUAL(catchUp.Check(0).TargetStep, 200);
        catchUp.OnNextReadStep(2, 150);
        UNIT_ASSERT_VALUES_EQUAL(catchUp.Check(0).TargetStep, 150);
        catchUp.OnNextReadStep(3, 300);
        UNIT_ASSERT_VALUES_EQUAL(catchUp.Check(0).TargetStep, 150);
    }

    Y_UNIT_TEST(ReachedWhenMediatorTimeIsAtTarget) {
        TMediatorTimeCatchUp catchUp({ 1 });
        catchUp.OnNextReadStep(1, 200);
        UNIT_ASSERT(catchUp.Check(199).Status == EStatus::WaitingForStep);
        UNIT_ASSERT(catchUp.Check(200).Status == EStatus::CaughtUp);
    }

    Y_UNIT_TEST(EmptyCoordinatorSetIsAlreadyCaughtUp) {
        TMediatorTimeCatchUp catchUp({});
        UNIT_ASSERT(catchUp.Check(0).Status == EStatus::CaughtUp);
    }

    Y_UNIT_TEST(IgnoresDuplicateCoordinatorReply) {
        TMediatorTimeCatchUp catchUp({ 1 });
        catchUp.OnNextReadStep(1, 200);
        catchUp.OnNextReadStep(1, 100);
        UNIT_ASSERT_VALUES_EQUAL(catchUp.Check(0).TargetStep, 200);
    }
}
