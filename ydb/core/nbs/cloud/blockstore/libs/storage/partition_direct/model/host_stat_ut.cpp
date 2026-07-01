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
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(TDuration(), errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
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

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnRequest(EOperation::WriteToPBuffer);
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
        UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);

        now += TDuration::Seconds(1);
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(2, errorsInfo.ConsecutiveErrorCount);

        now += TDuration::Seconds(1);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(3),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(2, errorsInfo.ConsecutiveErrorCount);
    }

    Y_UNIT_TEST(ResetErrorsOnSuccess)
    {
        THostStat stat;

        TInstant now = TInstant::Now();

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(100),
            EOperation::WriteToPBuffer);
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);

        auto errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);

        now += TDuration::Seconds(1);
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);
        errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
    }

    Y_UNIT_TEST(DontUpdateStatOnCancelled)
    {
        THostStat stat;

        TInstant now = TInstant::Now();

        stat.OnRequest(EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);
        stat.OnError(now, EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);

        stat.OnRequest(EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);
        stat.OnCancelled(now, EOperation::WriteToPBuffer);
        now += TDuration::Seconds(1);

        auto errorsInfo = stat.GetErrorsInfo(now);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(3),
            errorsInfo.FromFirstError);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(3),
            errorsInfo.FromLastError);
        UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);
    }

    Y_UNIT_TEST(SuccessCountInitiallyZero)
    {
        THostStat stat;
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetSuccessCount());

        auto errorsInfo = stat.GetErrorsInfo(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.SuccessCount);
    }

    Y_UNIT_TEST(SuccessCountIncrementsOnSuccess)
    {
        THostStat stat;
        TInstant now = TInstant::Now();

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetErrorsInfo(now).SuccessCount);

        stat.OnRequest(EOperation::ReadFromDDisk);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(20),
            EOperation::ReadFromDDisk);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).SuccessCount);

        stat.OnRequest(EOperation::Flush);
        stat.OnSuccess(now, TDuration::MilliSeconds(30), EOperation::Flush);
        UNIT_ASSERT_VALUES_EQUAL(3, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stat.GetErrorsInfo(now).SuccessCount);
    }

    Y_UNIT_TEST(SuccessCountResetsOnError)
    {
        THostStat stat;
        TInstant now = TInstant::Now();

        // Build up a success streak.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToPBuffer);
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).SuccessCount);

        // An error resets the counter to 0.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).SuccessCount);
    }

    Y_UNIT_TEST(SuccessCountNotAffectedByCancelled)
    {
        THostStat stat;
        TInstant now = TInstant::Now();

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToPBuffer);
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(
            now,
            TDuration::MilliSeconds(10),
            EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).SuccessCount);

        // OnCancelled should not change SuccessCount.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnCancelled(now, EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).SuccessCount);
    }

    Y_UNIT_TEST(SuccessCountAndErrorCountAreSymmetric)
    {
        THostStat stat;
        TInstant now = TInstant::Now();

        // Initially both are 0.
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).SuccessCount);

        // Success: SuccessCount grows, ErrorCount stays 0.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetErrorsInfo(now).SuccessCount);

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).SuccessCount);

        // Error: ErrorCount grows, SuccessCount resets.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).SuccessCount);

        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnError(now, EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).SuccessCount);

        // Success again: SuccessCount resumes, ErrorCount resets.
        stat.OnRequest(EOperation::WriteToPBuffer);
        stat.OnSuccess(now, TDuration(), EOperation::WriteToPBuffer);
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetSuccessCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stat.GetErrorsInfo(now).ErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(1, stat.GetErrorsInfo(now).SuccessCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
