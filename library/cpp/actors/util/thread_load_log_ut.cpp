#include "thread_load_log.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>
#include <util/system/hp_timer.h>
#include <util/system/thread.h>
#include <util/system/types.h>
#include <util/system/sanitizers.h>

#include <limits>

Y_UNIT_TEST_SUITE(ThreadLoadLog) {

    Y_UNIT_TEST(TThreadLoad8BitSlotType) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using TSlotType = std::uint8_t;
        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, TSlotType>;

        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeWindowLengthNs(), timeWindowLengthNs);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotLengthNs(), timeSlotLengthNs);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotCount(), timeSlotCount);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotMaxValue(), std::numeric_limits<TSlotType>::max());
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotPartCount(), (ui64)std::numeric_limits<TSlotType>::max() + 1);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotPartLengthNs(), T::GetTimeSlotLengthNs() / T::GetTimeSlotPartCount());
    }

    Y_UNIT_TEST(TThreadLoad16BitSlotType) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using TSlotType = std::uint16_t;
        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, TSlotType>;

        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeWindowLengthNs(), timeWindowLengthNs);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotLengthNs(), timeSlotLengthNs);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotCount(), timeSlotCount);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotMaxValue(), std::numeric_limits<TSlotType>::max());
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotPartCount(), (ui64)std::numeric_limits<TSlotType>::max() + 1);
        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotPartLengthNs(), T::GetTimeSlotLengthNs() / T::GetTimeSlotPartCount());
    }

    Y_UNIT_TEST(TThreadLoad8BitSlotTypeWindowBusy) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using TSlotType = std::uint8_t;
        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, TSlotType>;

        T threadLoad;
        threadLoad.RegisterBusyPeriod(T::GetTimeWindowLengthNs());

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), T::GetTimeWindowLengthNs());
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), T::GetTimeSlotMaxValue());
        }
    }

    Y_UNIT_TEST(TThreadLoad16BitSlotTypeWindowBusy) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using TSlotType = std::uint16_t;
        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, TSlotType>;

        T threadLoad;
        threadLoad.RegisterBusyPeriod(T::GetTimeWindowLengthNs());

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), T::GetTimeWindowLengthNs());
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), T::GetTimeSlotMaxValue());
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTimeSlot1) {
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTimeSlot2) {
        using T = TThreadLoad<38400>;

        ui32 startNs = 2 * T::GetTimeSlotPartLengthNs();
        T threadLoad(startNs);

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 3 * T::GetTimeSlotPartLengthNs() - 1;
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);

        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTimeSlot3) {
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTimeSlot4) {
        using T = TThreadLoad<38400>;

        ui32 startNs = 2 * T::GetTimeSlotPartLengthNs();
        T threadLoad(startNs);

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 3 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), (timeNs - startNs) / T::GetTimeSlotPartLengthNs());

        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTwoTimeSlots1) { 
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 2 * threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstTwoTimeSlots2) { 
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 2 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstThreeTimeSlots1) { 
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 3 * threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 3u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstThreeTimeSlots2) { 
        TThreadLoad<38400> threadLoad;

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());

        for (auto slotIndex = 3u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterBusyPeriodFirstThreeTimeSlots3) { 
        using T = TThreadLoad<38400>;

        ui32 startNs = 3 * T::GetTimeSlotPartLengthNs();
        T threadLoad(startNs);

        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 0;
        threadLoad.RegisterBusyPeriod(timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTimeSlot1) {
        using T = TThreadLoad<38400>;

        ui64 timeNs = T::GetTimeSlotPartLengthNs();
        T threadLoad(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 3 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 0);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 4 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTimeSlot2) {
        using T = TThreadLoad<38400>;

        ui64 timeNs = T::GetTimeSlotPartLengthNs();
        T threadLoad(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 3 * T::GetTimeSlotPartLengthNs() - 1;
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 4 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 3);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTimeSlot3) {
        using T = TThreadLoad<38400>;

        ui64 timeNs = T::GetTimeSlotPartLengthNs();
        T threadLoad(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 3 * T::GetTimeSlotPartLengthNs() - 1;
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 4 * T::GetTimeSlotPartLengthNs() - 2;
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 5 * T::GetTimeSlotPartLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 3);
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTwoTimeSlots1) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTwoTimeSlots2) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstTwoTimeSlots3) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = 2 * threadLoad.GetTimeSlotLengthNs() - 1;
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstThreeTimeSlots1) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        timeNs = 2 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        timeNs = 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 3u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstThreeTimeSlots2) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        timeNs = 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 1u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstThreeTimeSlots3) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        timeNs = 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 3u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstThreeTimeSlots4) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = threadLoad.GetTimeSlotLengthNs() + 2 * threadLoad.GetTimeSlotPartLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        timeNs = 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotPartCount() - 2);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 3u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodFirstThreeTimeSlots5) {
        using T = TThreadLoad<38400>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 2 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 2u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = timeNs + threadLoad.GetTimeWindowLengthNs() + threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(TThreadLoadRegisterIdlePeriodOverTimeWindow) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint8_t>;

        T threadLoad;

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), 0);
        for (auto slotIndex = 0u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        ui64 timeNs = 5 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterBusyPeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[3].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[4].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 5u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }

        timeNs = timeNs + threadLoad.GetTimeWindowLengthNs() - 3 * threadLoad.GetTimeSlotLengthNs();
        threadLoad.RegisterIdlePeriod(timeNs);

        UNIT_ASSERT_VALUES_EQUAL(threadLoad.LastTimeNs.load(), timeNs);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[0].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[1].load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[2].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[3].load(), threadLoad.GetTimeSlotMaxValue());
        UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[4].load(), threadLoad.GetTimeSlotMaxValue());
        for (auto slotIndex = 5u; slotIndex < threadLoad.GetTimeSlotCount(); ++slotIndex) {
            UNIT_ASSERT_VALUES_EQUAL(threadLoad.TimeSlots[slotIndex].load(), 0);
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsZeroShiftNs) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        UNIT_ASSERT_VALUES_EQUAL(T::GetTimeSlotPartCount(), (ui64)std::numeric_limits<std::uint16_t>::max() + 1);

        T *threadLoads[2];
        threadLoads[0] = new T;
        threadLoads[1] = new T;

        for (ui64 i = 1; i < timeSlotCount; i += 2) {
            threadLoads[0]->RegisterIdlePeriod(i * T::GetTimeSlotLengthNs());
            threadLoads[0]->RegisterBusyPeriod((i + 1) * T::GetTimeSlotLengthNs());
        }

        for (ui64 i = 1; i < timeSlotCount; i += 2) {
            threadLoads[1]->RegisterBusyPeriod(i * T::GetTimeSlotLengthNs());
            threadLoads[1]->RegisterIdlePeriod((i + 1) * T::GetTimeSlotLengthNs());
        }

        TMinusOneThreadEstimator estimator;
        ui64 value = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, 2, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());
        UNIT_ASSERT_VALUES_EQUAL(value, 0);

        delete threadLoads[0];
        delete threadLoads[1];
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsOneTimeSlotShift1) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 2;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        T *threadLoads[threadCount];

        for (auto t = 0u; t < threadCount; ++t) {
            threadLoads[t] = new T;

            for (ui64 i = 2; i < threadLoads[t]->GetTimeSlotCount(); i += 2) {
                threadLoads[t]->RegisterIdlePeriod((i - 1) * T::GetTimeSlotLengthNs());
                threadLoads[t]->RegisterBusyPeriod(i * T::GetTimeSlotLengthNs());
            }

            threadLoads[t]->RegisterIdlePeriod((threadLoads[t]->GetTimeSlotCount() - 1) * T::GetTimeSlotLengthNs());
            threadLoads[t]->RegisterBusyPeriod(threadLoads[t]->GetTimeSlotCount() * T::GetTimeSlotLengthNs());

            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 2 == 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        TMinusOneThreadEstimator estimator;
        auto result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());

        for (ui64 t = 0; t < threadCount; ++t) {
            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 2 == 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(result, T::GetTimeSlotLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            delete threadLoads[t];
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsOneTimeSlotShift2) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 2;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        T *threadLoads[threadCount];

        for (auto t = 0u; t < threadCount; ++t) {
            threadLoads[t] = new T;

            for (ui64 i = 2; i < threadLoads[t]->GetTimeSlotCount(); i += 2) {
                threadLoads[t]->RegisterBusyPeriod((i - 1) * T::GetTimeSlotLengthNs());
                threadLoads[t]->RegisterIdlePeriod(i * T::GetTimeSlotLengthNs());
            }

            threadLoads[t]->RegisterBusyPeriod((threadLoads[t]->GetTimeSlotCount() - 1) * T::GetTimeSlotLengthNs());
            threadLoads[t]->RegisterIdlePeriod(threadLoads[t]->GetTimeSlotCount() * T::GetTimeSlotLengthNs());

            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 2 == 0) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        TMinusOneThreadEstimator estimator;
        auto result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());

        for (ui64 t = 0; t < threadCount; ++t) {
            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 2 == 0) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(result, T::GetTimeSlotLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            delete threadLoads[t];
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsTwoTimeSlotsShift1) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 2;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        T *threadLoads[threadCount];

        for (auto t = 0u; t < threadCount; ++t) {
            threadLoads[t] = new T;

            for (ui64 i = 4; i < threadLoads[t]->GetTimeSlotCount(); i += 4) {
                threadLoads[t]->RegisterIdlePeriod((i - 2) * T::GetTimeSlotLengthNs());
                threadLoads[t]->RegisterBusyPeriod(i * T::GetTimeSlotLengthNs());
            }

            threadLoads[t]->RegisterIdlePeriod((threadLoads[t]->GetTimeSlotCount() - 2) * T::GetTimeSlotLengthNs());
            threadLoads[t]->RegisterBusyPeriod(threadLoads[t]->GetTimeSlotCount() * T::GetTimeSlotLengthNs());

            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 4 == 2 || s % 4 == 3) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        TMinusOneThreadEstimator estimator;
        auto result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());

        for (ui64 t = 0; t < threadCount; ++t) {
            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 4 == 2 || s % 4 == 3) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->TimeSlots[s].load(), 0);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(result, 2 * T::GetTimeSlotLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            delete threadLoads[t];
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsTwoTimeSlotsShift2) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 2;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        T *threadLoads[threadCount];

        for (auto t = 0u; t < threadCount; ++t) {
            threadLoads[t] = new T;

            for (ui64 i = 4; i < threadLoads[t]->GetTimeSlotCount(); i += 4) {
                threadLoads[t]->RegisterBusyPeriod((i - 2) * T::GetTimeSlotLengthNs());
                threadLoads[t]->RegisterIdlePeriod(i * T::GetTimeSlotLengthNs());
            }

            threadLoads[t]->RegisterBusyPeriod((threadLoads[t]->GetTimeSlotCount() - 2) * T::GetTimeSlotLengthNs());
            threadLoads[t]->RegisterIdlePeriod(threadLoads[t]->GetTimeSlotCount() * T::GetTimeSlotLengthNs());

            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 4 == 0 || s % 4 == 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        TMinusOneThreadEstimator estimator;
        auto result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());

        for (ui64 t = 0; t < threadCount; ++t) {
            for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                if (s % 4 == 0 || s % 4 == 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(result, 2 * T::GetTimeSlotLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            delete threadLoads[t];
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimatorTwoThreadLoadsTwoTimeSlotsShift3) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 2;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        T *threadLoads[threadCount];

        for (auto t = 0u; t < threadCount; ++t) {
            threadLoads[t] = new T;

            auto timeNs = T::GetTimeWindowLengthNs() - 1.5 * T::GetTimeSlotLengthNs();
            threadLoads[t]->RegisterIdlePeriod(timeNs);
            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->LastTimeNs.load(), timeNs);

            timeNs = T::GetTimeWindowLengthNs();
            threadLoads[t]->RegisterBusyPeriod(timeNs);
            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->LastTimeNs.load(), timeNs);

            for (ui64 s = 0; s + 2 < threadLoads[t]->GetTimeSlotCount(); ++s) {
                UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
            }

            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->TimeSlots[timeSlotCount - 2].load(), T::GetTimeSlotPartCount() / 2);
            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->TimeSlots[timeSlotCount - 1].load(), T::GetTimeSlotMaxValue());
        }

        TMinusOneThreadEstimator estimator;
        auto result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            for (ui64 s = 0; s + 2 < threadLoads[t]->GetTimeSlotCount(); ++s) {
                UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), 0, ToString(s).c_str());
            }

            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->TimeSlots[timeSlotCount - 2].load(), T::GetTimeSlotPartCount() / 2);
            UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->TimeSlots[timeSlotCount - 1].load(), T::GetTimeSlotMaxValue());
        }

        UNIT_ASSERT_VALUES_EQUAL(result, 2 * T::GetTimeSlotLengthNs());

        for (auto t = 0u; t < threadCount; ++t) {
            delete threadLoads[t];
        }
    }

    Y_UNIT_TEST(MinusOneThreadEstimator16ThreadLoadsAllTimeSlots) {
        constexpr auto timeWindowLengthNs = 5368709120ull; // 5 * 2 ^ 30 ~5 sec
        constexpr auto timeSlotLengthNs = 524288ull; // 2 ^ 19 ns ~ 512 usec
        constexpr auto timeSlotCount = timeWindowLengthNs / timeSlotLengthNs;
        constexpr auto threadCount = 16;
        constexpr auto estimatesCount = 16;

        using T = TThreadLoad<timeSlotCount, timeSlotLengthNs, std::uint16_t>;

        for (auto e = 0u; e < estimatesCount; ++e) {
            T *threadLoads[threadCount];

            for (auto t = 0u; t < threadCount; ++t) {
                threadLoads[t] = new T;
                auto timeNs = threadLoads[t]->GetTimeWindowLengthNs();
                threadLoads[t]->RegisterBusyPeriod(timeNs);

                UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->LastTimeNs.load(), timeNs);
                for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                }
            }

            ui64 result = 0;
            {
                THPTimer timer;
                TMinusOneThreadEstimator estimator;
                result = estimator.MaxLatencyIncreaseWithOneLessCpu(threadLoads, threadCount, T::GetTimeWindowLengthNs(), T::GetTimeWindowLengthNs());
                // output in microseconds
                auto passed = timer.Passed() * 1000000;
                Y_UNUSED(passed);
                // Cerr << "timer : " << passed << " " << __LINE__ << Endl;
            }

            for (ui64 t = 0; t < threadCount; ++t) {
                UNIT_ASSERT_VALUES_EQUAL(threadLoads[t]->LastTimeNs.load(), T::GetTimeWindowLengthNs());
                for (ui64 s = 0; s < threadLoads[t]->GetTimeSlotCount(); ++s) {
                    UNIT_ASSERT_VALUES_EQUAL_C(threadLoads[t]->TimeSlots[s].load(), T::GetTimeSlotMaxValue(), ToString(s).c_str());
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(result, T::GetTimeWindowLengthNs());

            for (auto t = 0u; t < threadCount; ++t) {
                delete threadLoads[t];
            }
        }
    }
}
