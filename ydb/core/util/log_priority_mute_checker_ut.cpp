#include <library/cpp/testing/unittest/registar.h>

#include "log_priority_mute_checker.h"

using namespace NKikimr;
using namespace NActors::NLog;

TInstant GetTime(ui64 seconds) {
    return TInstant::Zero() + TDuration::Seconds(seconds);
}

void CheckPriority(EPriority expected, TMaybe<EPriority> prio) {
    UNIT_ASSERT(prio);
    UNIT_ASSERT_EQUAL(*prio, expected);
}

void CheckPriority(EPriority expected, EPriority prio) {
    UNIT_ASSERT_EQUAL(prio, expected);
}

Y_UNIT_TEST_SUITE(TLogPriorityMuteTests) {

    template <template <NActors::NLog::EPriority, NActors::NLog::EPriority> typename TMuteChecker>
    void MakeMuteUntilTest(){
        TMuteChecker<PRI_ERROR, PRI_DEBUG> checker;
        TInstant muteDeadline = GetTime(3);
        checker.MuteUntil(muteDeadline);
        for (ui64 i = 0; i < 3; ++i) {
            CheckPriority(PRI_DEBUG, checker.CheckPriority(GetTime(i)));
        }
        for (ui64 i = 3; i < 6; ++i) {
            CheckPriority(PRI_ERROR, checker.CheckPriority(GetTime(i)));
        }
    }
    Y_UNIT_TEST(MuteUntilTest) {
        MakeMuteUntilTest<TLogPriorityMuteChecker>();
    }
    Y_UNIT_TEST(AtomicMuteUntilTest) {
        MakeMuteUntilTest<TAtomicLogPriorityMuteChecker>();
    }

    template <template <NActors::NLog::EPriority, NActors::NLog::EPriority> typename TMuteChecker>
    void MakeUnmuteTest() {
        TMuteChecker<PRI_ERROR, PRI_DEBUG> checker;
        TInstant muteDeadline = GetTime(3);
        checker.MuteUntil(muteDeadline);
        for (ui64 i = 0; i < 3; ++i) {
            CheckPriority(PRI_DEBUG, checker.CheckPriority(GetTime(i)));
        }
        checker.Unmute();
        for (ui64 i = 0; i < 6; ++i) {
            CheckPriority(PRI_ERROR, checker.CheckPriority(GetTime(i)));
        }
    }
    Y_UNIT_TEST(UnmuteTest) {
        MakeUnmuteTest<TLogPriorityMuteChecker>();
    }
    Y_UNIT_TEST(AtomicUnmuteTest) {
        MakeUnmuteTest<TAtomicLogPriorityMuteChecker>();
    }

    template <template <NActors::NLog::EPriority, NActors::NLog::EPriority> typename TMuteChecker>
    void MakeCheckPriorityWithSetMuteTest() {
        TLogPriorityMuteChecker<PRI_ERROR, PRI_DEBUG> checker;
        ui64 secondsForDeadlines[3] = {3, 6, 12};

        TInstant fakeDeadline = TInstant::Max();
        ui64 currentTime = 0;
        for (ui64 deadlineIdx = 0; deadlineIdx < 3; ++deadlineIdx) {
            TInstant realDeadline = GetTime(secondsForDeadlines[deadlineIdx]);
            CheckPriority(PRI_ERROR, checker.Register(GetTime(currentTime++), realDeadline));
            while (currentTime < secondsForDeadlines[deadlineIdx]) {
                CheckPriority(PRI_DEBUG, checker.Register(GetTime(currentTime++), fakeDeadline));
            }
        }
        CheckPriority(PRI_ERROR, checker.CheckPriority(GetTime(currentTime)));
    }
    Y_UNIT_TEST(CheckPriorityWithSetMuteTest) {
        MakeCheckPriorityWithSetMuteTest<TLogPriorityMuteChecker>();
    }
    Y_UNIT_TEST(AtomicCheckPriorityWithSetMuteTest) {
        MakeCheckPriorityWithSetMuteTest<TAtomicLogPriorityMuteChecker>();
    }

    template <template <NActors::NLog::EPriority, NActors::NLog::EPriority> typename TMuteChecker>
    void MakeCheckPriorityWithSetMuteDurationTest() {
        TLogPriorityMuteChecker<PRI_ERROR, PRI_DEBUG> checker;
        ui64 secondsForDurations[3] = {3, 6, 12};

        TDuration fakeDurations = TDuration::Seconds(600);
        ui64 currentTime = 0;
        ui64 deadline = 0;
        for (ui64 durationIdx = 0; durationIdx < 3; ++durationIdx) {
            TDuration realDuration = TDuration::Seconds(secondsForDurations[durationIdx]);
            deadline += secondsForDurations[durationIdx];
            CheckPriority(PRI_ERROR, checker.Register(GetTime(currentTime++), realDuration));
            while (currentTime < deadline) {
                CheckPriority(PRI_DEBUG, checker.Register(GetTime(currentTime++), fakeDurations));
            }
        }
        CheckPriority(PRI_ERROR, checker.CheckPriority(GetTime(currentTime)));
    }
    Y_UNIT_TEST(CheckPriorityWithSetMuteDurationTest) {
        MakeCheckPriorityWithSetMuteDurationTest<TLogPriorityMuteChecker>();
    }
    Y_UNIT_TEST(AtomicCheckPriorityWithSetMuteDurationTest) {
        MakeCheckPriorityWithSetMuteDurationTest<TAtomicLogPriorityMuteChecker>();
    }

}
