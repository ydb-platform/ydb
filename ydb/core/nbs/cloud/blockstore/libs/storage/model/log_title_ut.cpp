#include "log_title.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogTitleTest)
{
    Y_UNIT_TEST(GetPartitionPrefixTest)
    {
        ui64 tabletId = 12345;

        {
            ui32 partitionIndex = 1;
            ui32 partitionCount = 1;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL(result, "p:12345");
        }

        {
            ui32 partitionIndex = 0;
            ui32 partitionCount = 2;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL("p0:12345", result);
        }

        {
            ui32 partitionIndex = 1;
            ui32 partitionCount = 2;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL("p1:12345", result);
        }
    }

    Y_UNIT_TEST(GetForVolume)
    {
        TLogTitle logTitle1(
            GetCycleCount(),
            TLogTitle::TVolume{.TabletId = 12345, .DiskId = ""});

        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:? d:???]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetDiskId("disk1");
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:? d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:5 d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.GetWithTime(),
            "[v:12345 g:5 d:disk1 t:");
    }

    Y_UNIT_TEST(GetForPartition)
    {
        TLogTitle logTitle1(
            GetCycleCount(),
            TLogTitle::TPartitionDirect{.TabletId = 12345, .DiskId = "disk1"});

        UNIT_ASSERT_STRINGS_EQUAL(
            "[pd:12345 g:? d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[pd:12345 g:5 d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.GetWithTime(),
            "[pd:12345 g:5 d:disk1 t:");
    }

    Y_UNIT_TEST(GetForDirectBlockGroup)
    {
        TLogTitle logTitle{
            GetCycleCount(),
            TLogTitle::TDirectBlockGroup{.DiskId = "disk1"}};

        UNIT_ASSERT_STRINGS_EQUAL(
            "[dbg:disk1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(logTitle.GetWithTime(), "[dbg:disk1 t:");
    }

    Y_UNIT_TEST(GetChildLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(
            startTime,
            TLogTitle::TVolume{.TabletId = 12345, .DiskId = "disk1"});
        logTitle1.SetGeneration(5);

        auto childLogTitle =
            logTitle1.GetChild(startTime + GetCyclesPerMillisecond() * 1001);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:12345 g:5 d:disk1 t:1.001s + 1.");
    }

    Y_UNIT_TEST(GetChildWithTagsLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(
            startTime,
            TLogTitle::TVolume{.TabletId = 12345, .DiskId = "disk1"});
        logTitle1.SetGeneration(5);

        std::pair<TString, TString> tags[] = {{"cp", "123"}};

        auto childLogTitle = logTitle1.GetChildWithTags(
            startTime + GetCyclesPerMillisecond() * 1001,
            tags);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:12345 g:5 d:disk1 cp:123 t:1.001s + 1.");
    }
}

}   // namespace NYdb::NBS
