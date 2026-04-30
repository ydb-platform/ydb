#include "host_stat.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THostStatTest)
{
    Y_UNIT_TEST(NoErrors)
    {
        THostStat stat;
        TInstant now = TInstant::Now();
        auto errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ErrorCount);

        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ErrorCount);
    }

    Y_UNIT_TEST(InflightTracking)
    {
        THostStat stat;
        TInstant now = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            stat.InflightCount(EOperation::WriteToManyPBuffers));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            stat.InflightCount(EOperation::WriteToPBuffer));

        stat.OnRequest(EOperation::WriteToManyPBuffers);
        stat.OnRequest(EOperation::WriteToManyPBuffers);
        stat.OnRequest(EOperation::WriteToPBuffer);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            stat.InflightCount(EOperation::WriteToManyPBuffers));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            stat.InflightCount(EOperation::WriteToPBuffer));

        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToManyPBuffers);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            stat.InflightCount(EOperation::WriteToManyPBuffers));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            stat.InflightCount(EOperation::WriteToPBuffer));

        stat.OnError(now, EOperation::WriteToManyPBuffers);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            stat.InflightCount(EOperation::WriteToManyPBuffers));

        // Decrement is clamped at 0 - extra OnSuccess/OnError calls are
        // safe and do not cause underflow.
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToManyPBuffers);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            stat.InflightCount(EOperation::WriteToManyPBuffers));
    }

    Y_UNIT_TEST(HasErrors)
    {
        THostStat stat;

        TInstant now = TInstant::Now();

        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);

        auto errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ErrorCount);

        now += TDuration::Seconds(1);
        stat.OnError(now, EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(2, errorsInfo.ErrorCount);

        now += TDuration::Seconds(1);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(3),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(2, errorsInfo.ErrorCount);
    }

    Y_UNIT_TEST(ResetErrorsOnSuccess)
    {
        THostStat stat;

        TInstant now = TInstant::Now();

        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);

        auto errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ErrorCount);

        now += TDuration::Seconds(1);
        stat.OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ErrorCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
