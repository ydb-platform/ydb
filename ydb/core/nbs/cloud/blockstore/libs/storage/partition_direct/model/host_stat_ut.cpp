#include "host_stat.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THostStatTest)
{
    Y_UNIT_TEST(NoErrors)
    {
        THostStat stat;
        TInstant now = TInstant::Now();
        size_t errorCount = 0;
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration(),
            stat.ErrorsDuration(now, &errorCount));
        UNIT_ASSERT_VALUES_EQUAL(0, errorCount);

        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration(),
            stat.ErrorsDuration(now + TDuration::Seconds(1), &errorCount));
        UNIT_ASSERT_VALUES_EQUAL(0, errorCount);
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

        size_t errorCount = 0;
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            stat.ErrorsDuration(now, &errorCount));
        UNIT_ASSERT_VALUES_EQUAL(1, errorCount);

        now += TDuration::Seconds(1);
        stat.OnError(now + TDuration::Seconds(1), EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            stat.ErrorsDuration(now, &errorCount));
        UNIT_ASSERT_VALUES_EQUAL(2, errorCount);

        now += TDuration::Seconds(1);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(3),
            stat.ErrorsDuration(now, &errorCount));
        UNIT_ASSERT_VALUES_EQUAL(2, errorCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
