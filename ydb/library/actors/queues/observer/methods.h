#pragma once


namespace NActors {

#define DEFINE_HAS_STATIC_METHOD_CONCEPT(STATIC_METHOD)   \
    template <typename T>                                 \
    concept HasStaticMethod ## STATIC_METHOD = requires { \
        { T:: STATIC_METHOD } -> std::same_as<void(&)()>; \
    };                                                    \
// DEFINE_HAS_STATIC_METHOD_CONCEPT
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessFastPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveChangeFastPushToSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedSlowPushAttempt)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessSingleConsumerPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedSingleConsumerPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedSingleConsumerPopAttempt)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveMoveTailBecauseHeadOvertakesInReallySlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedReallySlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedSlowPopAttempt)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveMoveTailBecauseHeadOvertakesInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveChangeReallySlowPopToSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessOvertakenPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveMoveTailBecauseHeadOvertakesInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedFastPopAttempt)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveSuccessReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFailedReallyFastPopAttempt)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveInvalidatedSlotInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveInvalidatedSlotInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveInvalidatedSlotInReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFoundOldSlotInSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFoundOldSlotInFastPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFoundOldSlotInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFoundOldSlotInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveFoundOldSlotInReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongPush10It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongSlowPop10It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongFastPop10It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongReallyFastPop10It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongPush100It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongSlowPop100It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongFastPop100It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongReallyFastPop100It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongPush1000It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongSlowPop1000It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongFastPop1000It)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveLongReallyFastPop1000It)

    // FOR DEBUG PURPOSES
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveAfterReserveSlotInFastPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveAfterReserveSlotInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveAfterReserveSlotInReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(ObserveAfterIncorectlyChangeSlotGenerationInReallyFastPop)
#undef DEFINE_HAS_STATIC_METHOD_CONCEPT

}