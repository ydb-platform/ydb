#ifdef __linux__
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TProcTest, TestParseMemoryMappings)
{
    const TString rawSMaps =
        "7fbb7b24d000-7fbb7b251000 rw-s 00000000 00:00 0 \n"
        "Size:                  1 kB\n"
        "KernelPageSize:        2 kB\n"
        "MMUPageSize:           3 kB\n"
        "Rss:                   4 kB\n"
        "Pss:                   5 kB\n"
        "Shared_Clean:          6 kB\n"
        "Shared_Dirty:          7 kB\n"
        "Private_Clean:         8 kB\n"
        "Private_Dirty:         9 kB\n"
        "Referenced:           10 kB\n"
        "Anonymous:            11 kB\n"
        "LazyFree:             12 kB\n"
        "AnonHugePages:        13 kB\n"
        "ShmemPmdMapped:       14 kB\n"
        "Shared_Hugetlb:       15 kB\n"
        "Private_Hugetlb:      16 kB\n"
        "Swap:                 17 kB\n"
        "SwapPss:              18 kB\n"
        "Locked:               19 kB\n"
        "ProtectionKey:        20\n"
        "VmFlags: rd wr mg\n"
        "7fbb7b251000-7fbb7b278000 r-xp 000000ff 00:13d 406536                     /lib/x86_64-linux-gnu/ld-2.28.so (deleted)\n"
        "Size:                156 kB\n"
        "KernelPageSize:        4 kB\n"
        "MMUPageSize:           4 kB\n"
        "ProtectionKey:         0\n"
        "VmFlags: \n";

    auto smaps = ParseMemoryMappings(rawSMaps);

    EXPECT_EQ(std::ssize(smaps), 2);

    EXPECT_EQ(smaps[0].Start, 0x7fbb7b24d000u);
    EXPECT_EQ(smaps[0].End, 0x7fbb7b251000u);
    EXPECT_EQ(smaps[0].Permissions, EMemoryMappingPermission::Read | EMemoryMappingPermission::Write | EMemoryMappingPermission::Shared);
    EXPECT_EQ(smaps[0].Offset, 0u);
    EXPECT_EQ(static_cast<bool>(smaps[0].DeviceId), false);
    EXPECT_EQ(static_cast<bool>(smaps[0].INode), false);
    EXPECT_EQ(static_cast<bool>(smaps[0].Path), false);
    EXPECT_EQ(smaps[0].Statistics.Size, 1_KB);
    EXPECT_EQ(smaps[0].Statistics.KernelPageSize, 2_KB);
    EXPECT_EQ(smaps[0].Statistics.MMUPageSize, 3_KB);
    EXPECT_EQ(smaps[0].Statistics.Rss, 4_KB);
    EXPECT_EQ(smaps[0].Statistics.Pss, 5_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedClean, 6_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedDirty, 7_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateClean, 8_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateDirty, 9_KB);
    EXPECT_EQ(smaps[0].Statistics.Referenced, 10_KB);
    EXPECT_EQ(smaps[0].Statistics.Anonymous, 11_KB);
    EXPECT_EQ(smaps[0].Statistics.LazyFree, 12_KB);
    EXPECT_EQ(smaps[0].Statistics.AnonHugePages, 13_KB);
    EXPECT_EQ(smaps[0].Statistics.ShmemPmdMapped, 14_KB);
    EXPECT_EQ(smaps[0].Statistics.SharedHugetlb, 15_KB);
    EXPECT_EQ(smaps[0].Statistics.PrivateHugetlb, 16_KB);
    EXPECT_EQ(smaps[0].Statistics.Swap, 17_KB);
    EXPECT_EQ(smaps[0].Statistics.SwapPss, 18_KB);
    EXPECT_EQ(smaps[0].Statistics.Locked, 19_KB);
    EXPECT_EQ(smaps[0].ProtectionKey, 20u);
    EXPECT_EQ(smaps[0].VMFlags, EVMFlag::RD | EVMFlag::WR | EVMFlag::MG);

    EXPECT_EQ(smaps[1].Start, 0x7fbb7b251000u);
    EXPECT_EQ(smaps[1].End, 0x7fbb7b278000u);
    EXPECT_EQ(smaps[1].Permissions, EMemoryMappingPermission::Read | EMemoryMappingPermission::Execute | EMemoryMappingPermission::Private);
    EXPECT_EQ(smaps[1].Offset, 0xffu);
    EXPECT_EQ(smaps[1].DeviceId, 1048637);
    EXPECT_EQ(*smaps[1].INode, 406536u);
    EXPECT_EQ(*smaps[1].Path, "/lib/x86_64-linux-gnu/ld-2.28.so");
    EXPECT_EQ(smaps[1].Statistics.Size, 156_KB);
    EXPECT_EQ(smaps[1].Statistics.KernelPageSize, 4_KB);
    EXPECT_EQ(smaps[1].Statistics.MMUPageSize, 4_KB);
    EXPECT_EQ(smaps[1].Statistics.Rss, 0_KB);
    EXPECT_EQ(smaps[1].ProtectionKey, 0u);
    EXPECT_EQ(smaps[1].VMFlags, EVMFlag::None);
}

TEST(TProcTest, TestGetSelfMemoryMappings)
{
    auto pid = GetCurrentProcessId();
    auto memoryMappings = GetProcessMemoryMappings(pid);

    TMemoryMappingStatistics statistics;
    for (const auto& mapping : memoryMappings) {
        statistics += mapping.Statistics;
    }

    auto memoryUsage = GetProcessMemoryUsage();

    // Memory usage could change slightly between measurings.
    EXPECT_LE(statistics.Rss, 1.1 * memoryUsage.Rss);
    EXPECT_GE(statistics.Rss, 0.9 * memoryUsage.Rss);
}

TEST(TProcTest, CgroupList)
{
    auto cgroups = GetProcessCgroups();
    ASSERT_FALSE(cgroups.empty());

    for (const auto& group : cgroups) {
        if (group.HierarchyId == 0) {
            continue;
        }

        ASSERT_FALSE(group.Controllers.empty());
        ASSERT_NE(group.Path, "");

        for (const auto& controller : group.Controllers) {
            if (controller == "cpu") {
                GetCgroupCpuStat(group.ControllersName, group.Path);
            }
        }
    }
}

TEST(TProcTest, DiskStat)
{
    {
        auto parsed = ParseDiskStat("259       1 nvme0n1 372243 70861 50308550 175935 635314 559065 105338106 2777004 0 415304 3236956 38920 4 80436632 905059");
        EXPECT_EQ(parsed.MajorNumber, 259);
        EXPECT_EQ(parsed.MinorNumber, 1);
        EXPECT_EQ(parsed.DeviceName, "nvme0n1");

        EXPECT_EQ(parsed.ReadsCompleted, 372243);
        EXPECT_EQ(parsed.ReadsMerged, 70861);
        EXPECT_EQ(parsed.SectorsRead, 50308550);
        EXPECT_EQ(parsed.TimeSpentReading, TDuration::MilliSeconds(175935));

        EXPECT_EQ(parsed.WritesCompleted, 635314);

        EXPECT_EQ(parsed.DiscardsCompleted, 38920);
        EXPECT_EQ(parsed.DiscardsMerged, 4);
        EXPECT_EQ(parsed.SectorsDiscarded, 80436632);
        EXPECT_EQ(parsed.TimeSpentDiscarding, TDuration::MilliSeconds(905059));
    }
    {
        auto parsed = ParseDiskStat("259       1 nvme0n1 372243 trash 50308550 trash");
        EXPECT_EQ(parsed.MajorNumber, 259);
        EXPECT_EQ(parsed.MinorNumber, 1);
        EXPECT_EQ(parsed.DeviceName, "nvme0n1");

        EXPECT_EQ(parsed.ReadsCompleted, 372243);
        EXPECT_EQ(parsed.ReadsMerged, 0);
        EXPECT_EQ(parsed.SectorsRead, 50308550);
        EXPECT_EQ(parsed.TimeSpentReading, TDuration::MilliSeconds(0));
    }
    {
        auto stats = GetDiskStats();
        for (const TString& disk : ListDisks()) {
            EXPECT_TRUE(IsIn(stats, disk));
        }
    }
}

TEST(TProcTest, BlockDeviceStat)
{
    {
        auto stat = ParseBlockDeviceStat("509883438 87421933 206345875260 1643399993 1892382802 4495364138 837307482336 2391271400 0 2964131914 3304110410 0 0 0 0 81921472 3564406312");
        EXPECT_EQ(stat.ReadsCompleted, 509883438ll);
        EXPECT_EQ(stat.ReadsMerged, 87421933ll);
        EXPECT_EQ(stat.SectorsRead, 206345875260ll);
        EXPECT_EQ(stat.TimeSpentReading, TDuration::MilliSeconds(1643399993ul));
        EXPECT_EQ(stat.WritesCompleted, 1892382802ll);
        EXPECT_EQ(stat.WritesMerged, 4495364138ll);
        EXPECT_EQ(stat.SectorsWritten, 837307482336ll);
        EXPECT_EQ(stat.TimeSpentWriting, TDuration::MilliSeconds(2391271400ul));
        EXPECT_EQ(stat.IOCurrentlyInProgress, 0ll);
        EXPECT_EQ(stat.TimeSpentDoingIO, TDuration::MilliSeconds(2964131914ul));
        EXPECT_EQ(stat.WeightedTimeSpentDoingIO, TDuration::MilliSeconds(3304110410ul));
        EXPECT_EQ(stat.DiscardsCompleted, 0ll);
        EXPECT_EQ(stat.DiscardsMerged, 0ll);
        EXPECT_EQ(stat.SectorsDiscarded, 0ll);
        EXPECT_EQ(stat.TimeSpentDiscarding, TDuration::MilliSeconds(0ul));
        EXPECT_EQ(stat.FlushesCompleted, 81921472ll);
        EXPECT_EQ(stat.TimeSpentFlushing, TDuration::MilliSeconds(3564406312ul));
    }
    {
        for (const TString& disk : ListDisks()) {
            auto stat = GetBlockDeviceStat(disk);
            EXPECT_TRUE(stat);
        }
    }
}

TEST(TProcTest, FileDescriptorCount)
{
    auto initialCount = GetFileDescriptorCount();
    EXPECT_GE(initialCount, 3);

    int newDescriptorCount = 139;
    std::vector<TFile> files;
    for (int i = 0; i < newDescriptorCount; ++i) {
        files.emplace_back("/dev/null", EOpenModeFlag::WrOnly);
    }
    EXPECT_EQ(GetFileDescriptorCount(), initialCount + newDescriptorCount);

    files.clear();
    EXPECT_EQ(GetFileDescriptorCount(), initialCount);
}

TEST(TProcTest, SelfIO)
{
    GetSelfThreadTaskDiskStatistics();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
#endif
