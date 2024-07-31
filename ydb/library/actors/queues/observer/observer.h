#pragma once

#include "defs.h"

namespace NActors {

    struct TStatsObserver {

        struct TStats {
            ui64 SuccessPush = 0;
            ui64 SuccessSlowPush = 0;
            ui64 SuccessFastPush = 0;

            ui64 FailedPush = 0;
            ui64 FailedSlowPushAttempt = 0;

            ui64 ChangeFastPushToSlowPush = 0;
            ui64 ChangeReallySlowPopToSlowPop = 0;

            ui64 SuccessPop = 0;
            ui64 SuccessOvertakenPop = 0;
            ui64 SuccessSingleConsumerPop = 0;
            ui64 SuccessSlowPop = 0;
            ui64 SuccessFastPop = 0;
            ui64 SuccessReallyFastPop = 0;

            ui64 FailedPop = 0;
            ui64 FailedSingleConsumerPop = 0;
            ui64 FailedSingleConsumerPopAttempt = 0;
            ui64 FailedReallySlowPop = 0;
            ui64 FailedSlowPop = 0;
            ui64 FailedSlowPopAttempt = 0;
            ui64 FailedFastPop = 0;
            ui64 FailedFastPopAttempt = 0;
            ui64 FailedReallyFastPop = 0;
            ui64 FailedReallyFastPopAttempt = 0;


            ui64 InvalidatedSlot = 0;
            ui64 InvalidatedSlotInSlowPop = 0;
            ui64 InvalidatedSlotInFastPop = 0;
            ui64 InvalidatedSlotInReallyFastPop = 0;

            ui64 MoveTailBecauseHeadOvertakesInReallySlowPop = 0;
            ui64 MoveTailBecauseHeadOvertakesInSlowPop = 0;
            ui64 MoveTailBecauseHeadOvertakesInFastPop = 0;

            ui64 FoundOldSlot = 0;
            ui64 FoundOldSlotInSlowPush = 0;
            ui64 FoundOldSlotInFastPush = 0;
            ui64 FoundOldSlotInSlowPop = 0;
            ui64 FoundOldSlotInFastPop = 0;
            ui64 FoundOldSlotInReallyFastPop = 0;

            ui64 LongPush10It = 0;
            ui64 LongSlowPop10It = 0;
            ui64 LongFastPop10It = 0;
            ui64 LongReallyFastPop10It = 0;

            ui64 LongPush100It = 0;
            ui64 LongSlowPop100It = 0;
            ui64 LongFastPop100It = 0;
            ui64 LongReallyFastPop100It = 0;

            ui64 LongPush1000It = 0;
            ui64 LongSlowPop1000It = 0;
            ui64 LongFastPop1000It = 0;
            ui64 LongReallyFastPop1000It = 0;

            TStats& operator += (const TStats &other) {
                SuccessPush += other.SuccessPush;
                SuccessSlowPush += other.SuccessSlowPush;
                SuccessFastPush += other.SuccessFastPush;
                ChangeFastPushToSlowPush += other.ChangeFastPushToSlowPush;
                ChangeReallySlowPopToSlowPop += other.ChangeReallySlowPopToSlowPop;
                FailedPush += other.FailedPush;
                FailedSlowPushAttempt += other.FailedSlowPushAttempt;

                SuccessPop += other.SuccessPop;
                SuccessOvertakenPop += other.SuccessOvertakenPop;
                SuccessSingleConsumerPop += other.SuccessSingleConsumerPop;
                SuccessSlowPop += other.SuccessSlowPop;
                SuccessFastPop += other.SuccessFastPop;
                SuccessReallyFastPop += other.SuccessReallyFastPop;

                FailedPop += other.FailedPop;
                FailedSingleConsumerPop += other.FailedSingleConsumerPop;
                FailedSingleConsumerPopAttempt += other.FailedSingleConsumerPopAttempt;
                FailedReallySlowPop += other.FailedReallySlowPop;
                FailedSlowPop += other.FailedSlowPop;
                FailedSlowPopAttempt += other.FailedSlowPopAttempt;
                FailedFastPop += other.FailedFastPop;
                FailedFastPopAttempt += other.FailedFastPopAttempt;
                FailedReallyFastPop += other.FailedReallyFastPop;
                FailedReallyFastPopAttempt += other.FailedReallyFastPopAttempt;

                MoveTailBecauseHeadOvertakesInReallySlowPop += other.MoveTailBecauseHeadOvertakesInReallySlowPop;
                MoveTailBecauseHeadOvertakesInSlowPop += other.MoveTailBecauseHeadOvertakesInSlowPop;
                MoveTailBecauseHeadOvertakesInFastPop += other.MoveTailBecauseHeadOvertakesInFastPop;

                InvalidatedSlot += other.InvalidatedSlot;
                InvalidatedSlotInSlowPop += other.InvalidatedSlotInSlowPop;
                InvalidatedSlotInFastPop += other.InvalidatedSlotInFastPop;
                InvalidatedSlotInReallyFastPop += other.InvalidatedSlotInReallyFastPop;

                FoundOldSlot += other.FoundOldSlot;
                FoundOldSlotInSlowPush += other.FoundOldSlotInSlowPush;
                FoundOldSlotInFastPush += other.FoundOldSlotInFastPush;
                FoundOldSlotInSlowPop += other.FoundOldSlotInSlowPop;
                FoundOldSlotInFastPop += other.FoundOldSlotInFastPop;
                FoundOldSlotInReallyFastPop += other.FoundOldSlotInReallyFastPop;

                LongPush10It += other.LongPush10It;
                LongSlowPop10It += other.LongSlowPop10It;
                LongFastPop10It += other.LongFastPop10It;
                LongReallyFastPop10It += other.LongReallyFastPop10It;

                LongPush100It += other.LongPush100It;
                LongSlowPop100It += other.LongSlowPop100It;
                LongFastPop100It += other.LongFastPop100It;
                LongReallyFastPop100It += other.LongReallyFastPop100It;

                LongPush1000It += other.LongPush1000It;
                LongSlowPop100It += other.LongSlowPop1000It;
                LongFastPop1000It += other.LongFastPop1000It;
                LongReallyFastPop1000It += other.LongReallyFastPop1000It;

                return *this;
            }
        };
        static thread_local TStats Stats;

        template <typename  ...TArgs>
        static void IncrementMetrics(TArgs& ...args) {
            auto dummy = [](...) {};
            dummy(++args...);
        }

    #define DEFINE_INCREMENT_STATS_1(M1)                \
        static void Observe ## M1() {                      \
            IncrementMetrics(Stats.M1);                 \
        }                                               \
    // end DEFINE_INCREMENT_STATS_1

    #define DEFINE_INCREMENT_STATS_2(M1, M2)            \
        static void Observe ## M1() {                      \
            IncrementMetrics(Stats.M1, Stats.M2);       \
        }                                               \
    // end DEFINE_INCREMENT_STATS_2

        DEFINE_INCREMENT_STATS_2(SuccessSlowPush, SuccessPush)
        DEFINE_INCREMENT_STATS_2(SuccessFastPush, SuccessPush)
        DEFINE_INCREMENT_STATS_1(ChangeFastPushToSlowPush)
        DEFINE_INCREMENT_STATS_1(FailedPush)
        DEFINE_INCREMENT_STATS_1(FailedSlowPushAttempt)
        DEFINE_INCREMENT_STATS_2(SuccessSingleConsumerPop, SuccessPop)
        DEFINE_INCREMENT_STATS_2(FailedSingleConsumerPop, FailedPop)
        DEFINE_INCREMENT_STATS_1(FailedSingleConsumerPopAttempt)
        DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInReallySlowPop)
        DEFINE_INCREMENT_STATS_2(FailedReallySlowPop, FailedPop)
        DEFINE_INCREMENT_STATS_2(SuccessSlowPop, SuccessPop)
        DEFINE_INCREMENT_STATS_2(FailedSlowPop, FailedPop)
        DEFINE_INCREMENT_STATS_1(FailedSlowPopAttempt)
        DEFINE_INCREMENT_STATS_1(ChangeReallySlowPopToSlowPop)
        DEFINE_INCREMENT_STATS_2(SuccessFastPop, SuccessPop)
        DEFINE_INCREMENT_STATS_2(FailedFastPop, FailedPop)
        DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInFastPop)
        DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInSlowPop)
        DEFINE_INCREMENT_STATS_1(FailedFastPopAttempt)
        DEFINE_INCREMENT_STATS_2(SuccessReallyFastPop, SuccessPop)
        DEFINE_INCREMENT_STATS_2(FailedReallyFastPop, FailedPop)
        DEFINE_INCREMENT_STATS_1(FailedReallyFastPopAttempt)
        DEFINE_INCREMENT_STATS_2(InvalidatedSlotInSlowPop, InvalidatedSlot)
        DEFINE_INCREMENT_STATS_2(InvalidatedSlotInFastPop, InvalidatedSlot)
        DEFINE_INCREMENT_STATS_2(InvalidatedSlotInReallyFastPop, InvalidatedSlot)
        
        DEFINE_INCREMENT_STATS_2(FoundOldSlotInSlowPush, FoundOldSlot)
        DEFINE_INCREMENT_STATS_2(FoundOldSlotInFastPush, FoundOldSlot)
        DEFINE_INCREMENT_STATS_2(FoundOldSlotInSlowPop, FoundOldSlot)
        DEFINE_INCREMENT_STATS_2(FoundOldSlotInFastPop, FoundOldSlot)
        DEFINE_INCREMENT_STATS_2(FoundOldSlotInReallyFastPop, FoundOldSlot)

        DEFINE_INCREMENT_STATS_1(LongPush10It)
        DEFINE_INCREMENT_STATS_1(LongSlowPop10It)
        DEFINE_INCREMENT_STATS_1(LongFastPop10It)
        DEFINE_INCREMENT_STATS_1(LongReallyFastPop10It)

        DEFINE_INCREMENT_STATS_1(LongPush100It)
        DEFINE_INCREMENT_STATS_1(LongSlowPop100It)
        DEFINE_INCREMENT_STATS_1(LongFastPop100It)
        DEFINE_INCREMENT_STATS_1(LongReallyFastPop100It)

        DEFINE_INCREMENT_STATS_1(LongPush1000It)
        DEFINE_INCREMENT_STATS_1(LongSlowPop1000It)
        DEFINE_INCREMENT_STATS_1(LongFastPop1000It)
        DEFINE_INCREMENT_STATS_1(LongReallyFastPop1000It)

        DEFINE_INCREMENT_STATS_2(SuccessOvertakenPop, SuccessPop)
    #undef DEFINE_INCREMENT_STATS_1
    #undef DEFINE_INCREMENT_STATS_2

        static TStats GetLocalStats() {
            return Stats;
        }
    };

}