#include "../device_test_tool_pdisk_test.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPDisk {

extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;

} // namespace NKikimr::NPDisk

namespace {

constexpr ui32 TestChunkSize = 32 << 20;
constexpr ui32 TestInFlight = 2;
using TPDiskTest = NKikimr::TPDiskTest<TestChunkSize>;

struct TReport {
    TString TestType;
    TString Device;
    ui32 InFlight = 0;
    NKikimr::TSpeedAndIops Performance;
};

struct TRunResults {
    TVector<TReport> Reports;
    ui64 LogBytesWritten = 0;
    ui64 LogWritesCompleted = 0;
};

class TReportPrinter : public NKikimr::IResultPrinter {
    TReport Current;

public:
    TVector<TReport> Reports;

    void AddResult(const TString& name, const TString& value) override {
        if (name == "Device") {
            Current.Device = value;
        }
    }

    void AddGlobalParam(const TString&, const TString&) override {
    }

    void AddSpeedAndIops(const NKikimr::TSpeedAndIops& performance) override {
        Current.Performance = performance;
    }

    void SetTestType(const TString& testType) override {
        Current.TestType = testType;
    }

    void SetInFlight(ui32 inFlight) override {
        Current.InFlight = inFlight;
    }

    void PrintResults() override {
        Reports.push_back(Current);
        Current = {};
    }

    void EndTest() override {
    }
};

template<typename TLoad>
void ConfigureChunkLoad(TLoad* load) {
    load->SetDurationSeconds(2);
    load->SetDelayBeforeMeasurementsSeconds(0);
    load->SetSequential(true);
    auto* vdisk = load->MutableVDiskId();
    vdisk->SetGroupID(1);
    vdisk->SetGroupGeneration(1);
    auto* chunk = load->AddChunks();
    chunk->SetSlots(128);
    chunk->SetWeight(1);
}

NDevicePerfTest::TPDiskTest MakeReadTest() {
    NDevicePerfTest::TPDiskTest test;
    auto* load = test.AddPDiskTestList()->MutablePDiskReadLoad();
    ConfigureChunkLoad(load);
    load->SetInFlightReads(TestInFlight);
    return test;
}

NDevicePerfTest::TPDiskTest MakeWriteTest() {
    NDevicePerfTest::TPDiskTest test;
    auto* load = test.AddPDiskTestList()->MutablePDiskWriteLoad();
    ConfigureChunkLoad(load);
    load->SetInFlightWrites(TestInFlight);
    load->SetLogMode(NKikimr::TEvLoadTestRequest::LOG_SEQUENTIAL);
    return test;
}

NDevicePerfTest::TPDiskTest MakeLogTest() {
    NDevicePerfTest::TPDiskTest test;
    auto* load = test.AddPDiskTestList()->MutablePDiskLogLoad();
    load->SetDurationSeconds(1);
    load->SetDelayBeforeMeasurementsSeconds(0);
    auto* worker = load->AddWorkers();
    worker->MutableVDiskId()->SetGroupID(1);
    worker->MutableVDiskId()->SetGroupGeneration(1);
    worker->SetMaxInFlight(1);
    worker->SetSizeIntervalMin(4096);
    worker->SetSizeIntervalMax(4096);
    worker->SetBurstInterval(65536);
    worker->SetBurstSize(65536);
    worker->SetStorageDuration(1048576);
    return test;
}

TRunResults RunTest(
    const NDevicePerfTest::TPDiskTest& testProto,
    const TVector<TString>& paths = {"SectorMap:portable:64"},
    ui32 runs = 1)
{
    NKikimr::TPerfTestConfig config(paths, "sector-map", "SSD", "human", "0", false);
    for (const auto& sectorMap : config.SectorMaps) {
        UNIT_ASSERT(sectorMap);
        UNIT_ASSERT_VALUES_EQUAL(sectorMap->DeviceSize, 64ull << 30);
    }

    TRunResults result;
    auto printer = MakeIntrusive<TReportPrinter>();
    for (ui32 run = 0; run < runs; ++run) {
        TPDiskTest test(config, testProto);
        test.InitialSleep = TDuration::MilliSeconds(100);
        test.SetPrinter(printer);
        test.RunTest();
        auto pdiskCounters = NKikimr::GetServiceCounters(test.Counters, "pdisks")
            ->FindSubgroup("pdisk", "000000001");
        UNIT_ASSERT(pdiskCounters);
        auto media = pdiskCounters->FindSubgroup("media", "ssd");
        UNIT_ASSERT(media);
        auto subsystem = media->FindSubgroup("subsystem", "pdisk");
        UNIT_ASSERT(subsystem);
        auto logCounters = subsystem->FindSubgroup("req", "WriteLog");
        UNIT_ASSERT(logCounters);
        auto bytes = logCounters->FindCounter("Bytes");
        auto completed = logCounters->FindCounter("Results");
        UNIT_ASSERT(bytes);
        UNIT_ASSERT(completed);
        result.LogBytesWritten += bytes->Val();
        result.LogWritesCompleted += completed->Val();
    }
    printer->EndTest();
    result.Reports = std::move(printer->Reports);
    return result;
}

void AssertChunkReport(const TReport& report, const TString& testType) {
    UNIT_ASSERT_VALUES_EQUAL(report.TestType, testType);
    UNIT_ASSERT_VALUES_EQUAL(report.InFlight, TestInFlight);
    UNIT_ASSERT_C(report.Performance.SpeedMBps > 0, "No measured PDisk throughput");
    UNIT_ASSERT_C(report.Performance.Iops > 0, "No measured PDisk operations");
}

} // namespace

Y_UNIT_TEST_SUITE(TStressToolSectorMap) {
    Y_UNIT_TEST(PathDefaultsAndModes) {
        const auto defaults = NKikimr::TPerfTestConfig::TryParseSectorMap("SectorMap:defaults");
        UNIT_ASSERT(defaults);
        UNIT_ASSERT_VALUES_EQUAL(defaults->DeviceSize, 100ull << 30);
        UNIT_ASSERT(!defaults->GetDiskModeParams());

        for (const TString mode : {"NONE", "HDD", "SSD", "NVME"}) {
            const auto sectorMap = NKikimr::TPerfTestConfig::TryParseSectorMap("SectorMap:explicit:64:" + mode);
            UNIT_ASSERT(sectorMap);
            UNIT_ASSERT_VALUES_EQUAL(sectorMap->DeviceSize, 64ull << 30);
            UNIT_ASSERT_STRING_CONTAINS(sectorMap->ToString(), "DiskMode# " + mode + "\n");
        }
        const auto maxSize = NKikimr::TPerfTestConfig::TryParseSectorMap("SectorMap:large:4294967295");
        UNIT_ASSERT_VALUES_EQUAL(maxSize->DeviceSize, ui64(Max<ui32>()) << 30);
    }

    Y_UNIT_TEST(InvalidMapPaths) {
        for (const TString path : {"SectorMap:", "SectorMap::64", "SectorMap:map:", "SectorMap:map::64",
                "SectorMap:map:0", "SectorMap:map:-1", "SectorMap:map:+64", "SectorMap:map:64.5",
                "SectorMap:map:no", "SectorMap:map:4294967296", "SectorMap:map: 64",
                "SectorMap:map:64:", "SectorMap:map:64:hdd", "SectorMap:map:64:nvme",
                "SectorMap:map:64:unknown", "SectorMap:map:64:SSD:extra"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(NKikimr::TPerfTestConfig::TryParseSectorMap(path), yexception, path);
        }
    }

    Y_UNIT_TEST(RealPathsRemainUnchanged) {
        for (const TString path : {"/dev/disk0", "disk-file", "PCIe:device"}) {
            UNIT_ASSERT(!NKikimr::TPerfTestConfig::TryParseSectorMap(path));
        }
    }

    Y_UNIT_TEST(RejectSmallMapsBeforeInitialization) {
        const auto testProto = MakeReadTest();
        for (const TString path : {"SectorMap:small:8", "SectorMap:boundary:25"}) {
            const NKikimr::TPerfTestConfig config(path, "capacity", "SSD", "human", "0", false);
            UNIT_ASSERT_EXCEPTION_CONTAINS((NKikimr::TPDiskTest<>(config, testProto)), yexception,
                "201 chunks of 134217728 bytes");
        }
        const NKikimr::TPerfTestConfig config("SectorMap:enough:26", "capacity", "SSD", "human", "0", false);
        NKikimr::TPDiskTest<> test(config, testProto);
    }

    Y_UNIT_TEST(CapacityUsesEffectiveChunkSize) {
        const auto testProto = MakeReadTest();
        NKikimr::TPerfTestConfig config("SectorMap:small-chunks:7", "capacity", "SSD", "human", "0", false);
        TPDiskTest smallChunks(config, testProto);
        config.PhysicalChunkSize = 128 << 20;
        UNIT_ASSERT_EXCEPTION_CONTAINS((TPDiskTest(config, testProto)), yexception,
            "201 chunks of 134217728 bytes");
        config.PhysicalChunkSize = TestChunkSize;
        NKikimr::TPDiskTest<> physicalSmallChunks(config, testProto);
    }

    Y_UNIT_TEST(CapacityChecksEveryMap) {
        const auto testProto = MakeReadTest();
        const NKikimr::TPerfTestConfig config(
            TVector<TString>{"SectorMap:first:64", "SectorMap:second:8"}, "capacity", "SSD", "human", "0", false);
        UNIT_ASSERT_EXCEPTION_CONTAINS((NKikimr::TPDiskTest<>(config, testProto)), yexception,
            "Sector map 'SectorMap:second:8' is too small");
    }

    Y_UNIT_TEST(Read) {
        const auto reports = RunTest(MakeReadTest()).Reports;
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
        AssertChunkReport(reports.front(), "PDiskReadLoad");
    }

    Y_UNIT_TEST(Write) {
        const auto reports = RunTest(MakeWriteTest()).Reports;
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
        AssertChunkReport(reports.front(), "PDiskWriteLoad");
    }

    Y_UNIT_TEST(Log) {
        const auto result = RunTest(MakeLogTest());
        UNIT_ASSERT_VALUES_EQUAL(result.Reports.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Reports.front().TestType, "PDiskLogLoad");
        // Log load reports do not populate throughput; check completed PDisk log I/O directly.
        UNIT_ASSERT_C(result.LogBytesWritten > 0, "No PDisk log bytes written");
        UNIT_ASSERT_C(result.LogWritesCompleted > 0, "No PDisk log writes completed");
    }

    Y_UNIT_TEST(MultipleSectorMaps) {
        const auto reports = RunTest(MakeReadTest(), {"SectorMap:first:64", "SectorMap:second:64"}).Reports;
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 3);
        for (const auto& report : reports) {
            AssertChunkReport(report, "PDiskReadLoad");
        }
        UNIT_ASSERT_VALUES_EQUAL(reports[0].Device, "0");
        UNIT_ASSERT_VALUES_EQUAL(reports[1].Device, "1");
        UNIT_ASSERT_VALUES_EQUAL(reports[2].Device, "SUM");
        UNIT_ASSERT_DOUBLES_EQUAL(reports[2].Performance.SpeedMBps,
            reports[0].Performance.SpeedMBps + reports[1].Performance.SpeedMBps, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(reports[2].Performance.Iops,
            reports[0].Performance.Iops + reports[1].Performance.Iops, 1e-9);
    }

    Y_UNIT_TEST(RepeatedRuns) {
        const auto reports = RunTest(MakeReadTest(), {"SectorMap:repeated:64"}, 2).Reports;
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 2);
        for (const auto& report : reports) {
            AssertChunkReport(report, "PDiskReadLoad");
        }
    }
}
