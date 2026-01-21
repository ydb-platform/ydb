#include "context.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCallContext)
{
    void CheckCalcRequestTime(
        ui64 cycles,
        TCallContextBasePtr context,
        TRequestTime requestTimeAnswer)
    {
        auto requestTime = context->CalcRequestTime(cycles);
        UNIT_ASSERT_VALUES_EQUAL(requestTimeAnswer.ExecutionTime,
            requestTime.ExecutionTime);
        UNIT_ASSERT_VALUES_EQUAL(requestTimeAnswer.TotalTime,
            requestTime.TotalTime);
    }

    Y_UNIT_TEST(CheckCalcRequestTime)
    {
        auto callContext = MakeIntrusive<TCallContextBase>(static_cast<ui64>(0));
        callContext->SetRequestStartedCycles(5);
        CheckCalcRequestTime(
            3, callContext, {TDuration::Zero(), TDuration::Zero()});

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime {
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(15)});

        callContext->SetResponseSentCycles(13);

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime {
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(7)});

        callContext->SetResponseSentCycles(0);

        CheckCalcRequestTime(
            20,
            callContext,
            TRequestTime {
                .TotalTime = CyclesToDurationSafe(15),
                .ExecutionTime = CyclesToDurationSafe(2)});
    }
}

}   // namespace NYdb::NBS
