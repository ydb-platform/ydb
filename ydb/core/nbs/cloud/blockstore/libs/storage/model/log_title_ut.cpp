#include "log_title.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/stream/output.h>

namespace NYdb::NBS {

namespace {

enum class ETestMode: ui32
{
    IndirectWrite,
    DirectWrite,
};

IOutputStream& operator<<(IOutputStream& out, ETestMode mode)
{
    switch (mode) {
        case ETestMode::IndirectWrite:
            return out << "IndirectWrite";
        case ETestMode::DirectWrite:
            return out << "DirectWrite";
    }
    Y_ABORT("unexpected ETestMode");
}

struct TTestRange
{
    ui64 Start = 0;
    ui64 End = 0;
};

IOutputStream& operator<<(IOutputStream& out, TTestRange range)
{
    return out << "[" << range.Start << ".." << range.End << "]";
}

}   // namespace

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
            TLogTitle::TVolume{.DiskId = "", .TabletId = 12345});

        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:??? tbl:12345/0]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetDiskId("disk1");
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:disk1 tbl:12345/0]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:disk1 tbl:12345/5]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.GetWithTime(),
            "[v:disk1 tbl:12345/5 t:");
    }

    Y_UNIT_TEST(GetForPartition)
    {
        TLogTitle logTitle1(
            GetCycleCount(),
            TLogTitle::TPartitionDirect{.DiskId = "disk1", .TabletId = 12345});

        UNIT_ASSERT_STRINGS_EQUAL(
            "[pd:disk1 tbl:12345/?]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[pd:disk1 tbl:12345/5]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.GetWithTime(),
            "[pd:disk1 tbl:12345/5 t:");
    }

    Y_UNIT_TEST(GetForDirectBlockGroup)
    {
        TLogTitle logTitle{
            GetCycleCount(),
            TLogTitle::TDirectBlockGroup{.DiskId = "disk1"}};

        UNIT_ASSERT_STRINGS_EQUAL(
            "[dbg:disk1/0 tbl:0/0]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle.GetWithTime(),
            "[dbg:disk1/0 tbl:0/0 t:");
    }

    Y_UNIT_TEST(GetChildLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(
            startTime,
            TLogTitle::TVolume{.DiskId = "disk1", .TabletId = 12345});
        logTitle1.SetGeneration(5);

        auto childLogTitle =
            logTitle1.GetChild(startTime + GetCyclesPerMillisecond() * 1001);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:disk1 tbl:12345/5 t:1.001s + 1.");
    }

    Y_UNIT_TEST(GetChildWithTagsLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(
            startTime,
            TLogTitle::TVolume{.DiskId = "disk1", .TabletId = 12345});
        logTitle1.SetGeneration(5);

        TLogParam tags[] = {{"cp", "123"}};

        auto childLogTitle = logTitle1.GetChildWithTags(
            startTime + GetCyclesPerMillisecond() * 1001,
            tags);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:disk1 tbl:12345/5 cp:123 t:1.001s + 1.");
    }

    Y_UNIT_TEST(GetChildWithTypedTags)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle(
            startTime,
            TLogTitle::TVolume{.DiskId = "disk1", .TabletId = 12345});
        logTitle.SetGeneration(5);

        const ui64 lsn = 42;
        const auto mode = ETestMode::DirectWrite;
        const TTestRange range{.Start = 10, .End = 19};

        auto childLogTitle = logTitle.GetChildWithTags(
            startTime + GetCyclesPerMillisecond() * 1001,
            {{"lsn", lsn}, {"t", mode}, {"r", range}});

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:disk1 tbl:12345/5 lsn:42 t:DirectWrite r:[10..19] t:1.001s + "
            "1.");
    }
}

}   // namespace NYdb::NBS
