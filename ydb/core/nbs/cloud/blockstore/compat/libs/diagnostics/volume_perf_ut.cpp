#include "volume_perf.h"

#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/monitoring.h>
#include <ydb/core/nbs/cloud/storage/core/libs/throttling/helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;
using NYdb::NBS::CostPerIO;
using NYdb::NBS::CreateMonitoringServiceStub;
using NYdb::NBS::DefaultBlockSize;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NonreplDefaultIops = 35114;
constexpr ui64 NonreplDefaultBw = 843000000;

constexpr ui64 SsdDefaultIops = 4200;
constexpr ui64 SsdDefaultBw = 342000000;

constexpr ui64 HddDefaultIops = 1023;
constexpr ui64 HddDefaultBw = 129000000;

constexpr ui64 Mirror2DefaultIops = SsdDefaultIops;
constexpr ui64 Mirror2DefaultBw = SsdDefaultBw;

constexpr ui64 Mirror3DefaultIops = SsdDefaultIops;
constexpr ui64 Mirror3DefaultBw = SsdDefaultBw;

constexpr ui64 LocalSSDDefaultIops = NonreplDefaultIops;
constexpr ui64 LocalSSDDefaultBw = NonreplDefaultBw;

////////////////////////////////////////////////////////////////////////////////

const NProto::TVolumePerfSettings& GetPerfSettings(
    const NProto::TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind type)
{
    switch (type) {
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            return config.GetNonreplPerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            return config.GetHddNonreplPerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            return config.GetSsdPerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            return config.GetMirror2PerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            return config.GetMirror3PerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            return config.GetLocalSSDPerfSettings();
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: {
            return config.GetLocalHDDPerfSettings();
            break;
        }
        default: {
            return config.GetHddPerfSettings();
        }
    }
}

void CreateDefaultPerformanceSettings(
    NProto::TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind type)
{
    NProto::TVolumePerfSettings settings;
    auto& read = *settings.MutableRead();
    auto& write = *settings.MutableWrite();

    switch (type) {
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            read.SetIops(NonreplDefaultIops);
            read.SetBandwidth(NonreplDefaultBw);

            write.SetIops(NonreplDefaultIops);
            write.SetBandwidth(NonreplDefaultBw);

            *config.MutableNonreplPerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            // same params as for ssd nonrepl - for simplicity
            read.SetIops(NonreplDefaultIops);
            read.SetBandwidth(NonreplDefaultBw);

            write.SetIops(NonreplDefaultIops);
            write.SetBandwidth(NonreplDefaultBw);

            *config.MutableHddNonreplPerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            read.SetIops(SsdDefaultIops);
            read.SetBandwidth(SsdDefaultBw);

            write.SetIops(SsdDefaultIops);
            write.SetBandwidth(SsdDefaultBw);

            *config.MutableSsdPerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            read.SetIops(Mirror2DefaultIops);
            read.SetBandwidth(Mirror2DefaultBw);

            write.SetIops(Mirror2DefaultIops);
            write.SetBandwidth(Mirror2DefaultBw);

            *config.MutableMirror2PerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            read.SetIops(Mirror3DefaultIops);
            read.SetBandwidth(Mirror3DefaultBw);

            write.SetIops(Mirror3DefaultIops);
            write.SetBandwidth(Mirror3DefaultBw);

            *config.MutableMirror3PerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            read.SetIops(LocalSSDDefaultIops);
            read.SetBandwidth(LocalSSDDefaultBw);

            write.SetIops(LocalSSDDefaultIops);
            write.SetBandwidth(LocalSSDDefaultBw);

            *config.MutableLocalSSDPerfSettings() = settings;
            break;
        }
        default: {
            read.SetIops(HddDefaultIops);
            read.SetBandwidth(HddDefaultBw);

            write.SetIops(HddDefaultIops);
            write.SetBandwidth(HddDefaultBw);

            *config.MutableHddPerfSettings() = settings;
        }
    }
    config.SetExpectedIoParallelism(32);
}

NProto::TDiagnosticsConfig CreateDefaultPerformanceSettings()
{
    using namespace NCloud::NProto;

    NProto::TDiagnosticsConfig config;

    for (ui32 i = EStorageMediaKind_MIN; i <= EStorageMediaKind_MAX; ++i) {
        EStorageMediaKind kind = static_cast<EStorageMediaKind>(i);
        CreateDefaultPerformanceSettings(config, kind);
    }

    config.SetExpectedIoParallelism(32);
    return config;
}

NProto::TDiagnosticsConfig CreateDefaultPerformanceSettings(
    NCloud::NProto::EStorageMediaKind type)
{
    NProto::TDiagnosticsConfig config;
    CreateDefaultPerformanceSettings(config, type);
    return config;
}

NProto::TDiagnosticsConfig CreatePerformanceSettings(
    NCloud::NProto::EStorageMediaKind type,
    ui32 readIops,
    ui64 readBandwidth,
    ui32 writeIops,
    ui64 writeBandwidth,
    ui32 minReadIops,
    ui64 minReadBandwidth,
    ui32 minWriteIops,
    ui64 minWriteBandwidth)
{
    NProto::TDiagnosticsConfig config;
    NProto::TVolumePerfSettings settings;

    auto& read = *settings.MutableRead();
    auto& write = *settings.MutableWrite();
    read.SetIops(readIops);
    read.SetBandwidth(readBandwidth);
    write.SetIops(writeIops);
    write.SetBandwidth(writeBandwidth);

    settings.SetThrottlerOvercommit(1);

    auto& minRead = *settings.MutableMinRead();
    auto& minWrite = *settings.MutableMinWrite();
    minRead.SetIops(minReadIops);
    minRead.SetBandwidth(minReadBandwidth);
    minWrite.SetIops(minWriteIops);
    minWrite.SetBandwidth(minWriteBandwidth);

    switch (type) {
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            *config.MutableNonreplPerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            *config.MutableHddNonreplPerfSettings() = settings;
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            *config.MutableSsdPerfSettings() = settings;
            break;
        }
        default: {
            *config.MutableHddPerfSettings() = settings;
        }
    }

    return config;
}

NProto::TVolume CreateVolume(
    const TString& name,
    NCloud::NProto::EStorageMediaKind kind,
    ui32 readIops,
    ui32 readBw,
    ui32 writeIops,
    ui32 writeBw)
{
    NProto::TVolume volume;
    volume.SetDiskId(name);
    volume.SetStorageMediaKind(kind);
    volume.SetBlockSize(DefaultBlockSize);

    auto& profile = *volume.MutablePerformanceProfile();
    profile.SetMaxReadIops(readIops);
    profile.SetMaxReadBandwidth(readBw);
    profile.SetMaxWriteIops(writeIops);
    profile.SetMaxWriteBandwidth(writeBw);

    return volume;
}

NProto::TVolume CreateVolumeWithDiagnosticsProfile(
    const TString& name,
    NCloud::NProto::EStorageMediaKind kind,
    const NProto::TDiagnosticsConfig& config)
{
    NProto::TVolume volume;
    volume.SetDiskId(name);
    volume.SetStorageMediaKind(kind);
    volume.SetBlockSize(DefaultBlockSize);

    auto& profile = *volume.MutablePerformanceProfile();
    NProto::TVolumePerfSettings settings = GetPerfSettings(config, kind);

    profile.SetMaxReadIops(settings.GetRead().GetIops());
    profile.SetMaxReadBandwidth(settings.GetRead().GetBandwidth());
    profile.SetMaxWriteIops(settings.GetWrite().GetIops());
    profile.SetMaxWriteBandwidth(settings.GetWrite().GetBandwidth());

    return volume;
}

TDynamicCountersPtr CreateVolumeCounters(
    TDynamicCountersPtr root,
    const TString& name)
{
    auto hostGroup = root->GetSubgroup("host", "cluster");
    return hostGroup->GetSubgroup("volume", name);
}

void Wait(TVolumePerformanceCalculator& calc, TDuration duration)
{
    for (ui32 i = 0; i < duration.Seconds(); ++i) {
        calc.UpdateStats();
    }
}

void TestSuffer(
    TVolumePerformanceCalculator& calc,
    TDynamicCountersPtr counters,
    TDuration requestTime,
    TDuration waitDuration,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    calc.OnRequestCompleted(
        EBlockStoreRequest::WriteBlocks,
        0,
        DurationToCyclesSafe(requestTime),
        0,
        1024 * 1024);

    Wait(calc, waitDuration);

    auto sufferCounter = counters->GetCounter("Suffer", false);
    UNIT_ASSERT_VALUES_EQUAL(expected, sufferCounter->Val());
    auto smoothSufferCounter = counters->GetCounter("SmoothSuffer", false);
    UNIT_ASSERT_VALUES_EQUAL(expectedSmooth, smoothSufferCounter->Val());
    auto criticalSufferCounter = counters->GetCounter("CriticalSuffer", false);
    UNIT_ASSERT_VALUES_EQUAL(expectedCritical, criticalSufferCounter->Val());
}

void TestSuffer(
    TVolumePerformanceCalculator& calc,
    TDynamicCountersPtr counters,
    TDuration waitDuration,
    bool slowRequest,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    TDuration extraTime;
    if (slowRequest) {
        extraTime = TDuration::MicroSeconds(10);
    }

    auto requestTime = calc.GetExpectedWriteCost(1024 * 1024) + extraTime;

    TestSuffer(
        calc,
        counters,
        requestTime,
        waitDuration,
        expected,
        expectedSmooth,
        expectedCritical);
}

void TestSufferCount(
    TVolumePerformanceCalculator& calc,
    TDuration requestTime,
    TDuration waitDuration,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    calc.OnRequestCompleted(
        EBlockStoreRequest::WriteBlocks,
        0,
        DurationToCyclesSafe(requestTime),
        0,
        1024 * 1024);

    Wait(calc, waitDuration);

    UNIT_ASSERT_VALUES_EQUAL(expected, calc.GetSufferCount());
    UNIT_ASSERT_VALUES_EQUAL(expectedSmooth, calc.GetSmoothSufferCount());
    UNIT_ASSERT_VALUES_EQUAL(expectedCritical, calc.GetCriticalSufferCount());
}

void TestSufferCount(
    TVolumePerformanceCalculator& calc,
    TDuration requestTime,
    TDuration postponedTime,
    TDuration waitDuration,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    calc.OnRequestCompleted(
        EBlockStoreRequest::WriteBlocks,
        0,
        DurationToCyclesSafe(requestTime),
        DurationToCyclesSafe(postponedTime),
        1024 * 1024);

    Wait(calc, waitDuration);

    UNIT_ASSERT_VALUES_EQUAL(expected, calc.GetSufferCount());
    UNIT_ASSERT_VALUES_EQUAL(expectedSmooth, calc.GetSmoothSufferCount());
    UNIT_ASSERT_VALUES_EQUAL(expectedCritical, calc.GetCriticalSufferCount());
}

void TestSufferCount(
    TVolumePerformanceCalculator& calc,
    TDuration waitDuration,
    bool slowRequest,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    TDuration extraTime;
    if (slowRequest) {
        extraTime = TDuration::MicroSeconds(10);
    }

    auto requestTime = calc.GetExpectedWriteCost(1024 * 1024) + extraTime;

    TestSufferCount(
        calc,
        requestTime,
        waitDuration,
        expected,
        expectedSmooth,
        expectedCritical);
}

void TestSufferCountPostponed(
    TVolumePerformanceCalculator& calc,
    TDuration waitDuration,
    ui32 expected,
    ui32 expectedSmooth,
    ui32 expectedCritical)
{
    TDuration extraTime = TDuration::MicroSeconds(10);

    auto requestTime = calc.GetExpectedWriteCost(1024 * 1024) + extraTime;

    TestSufferCount(
        calc,
        requestTime,
        extraTime,
        waitDuration,
        expected,
        expectedSmooth,
        expectedCritical);
}

void CheckServerSufferCounters(
    TDynamicCountersPtr root,
    NCloud::NProto::EStorageMediaKind kind,
    i64 cnt)
{
    auto serverGroup = root->GetSubgroup("counters", "blockstore")
                           ->GetSubgroup("component", "server");

    switch (kind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            auto perType = serverGroup->GetSubgroup("type", "ssd")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            auto perType = serverGroup->GetSubgroup("type", "ssd_nonrepl")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            auto perType = serverGroup->GetSubgroup("type", "hdd_nonrepl")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            auto perType = serverGroup->GetSubgroup("type", "ssd_mirror2")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            auto perType = serverGroup->GetSubgroup("type", "ssd_mirror3")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            auto perType = serverGroup->GetSubgroup("type", "ssd_local")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: {
            auto perType = serverGroup->GetSubgroup("type", "hdd_local")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
        default: {
            auto perType = serverGroup->GetSubgroup("type", "hdd")
                               ->GetCounter("DisksSuffer", false);
            UNIT_ASSERT_VALUES_EQUAL(cnt, perType->Val());
            break;
        }
    }
}

void TestSufferWithMetrics(NCloud::NProto::EStorageMediaKind diskKind)
{
    using namespace NCloud::NProto;

    auto monitoring = CreateMonitoringServiceStub();
    auto counters = monitoring->GetCounters();

    auto config = CreateDefaultPerformanceSettings();

    auto volume = CreateVolumeWithDiagnosticsProfile("test1", diskKind, config);
    auto volumeCounters = CreateVolumeCounters(counters, "test1");

    auto calc = TVolumePerformanceCalculator(
        volume,
        std::make_shared<TDiagnosticsConfig>(std::move(config)));

    calc.Register(*volumeCounters, volume);

    TSufferCounters sufferCounters(
        "DisksSuffer",
        counters->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server"));

    auto commonKind = [&](const auto& kind) -> auto
    {
        if (kind == NProto::STORAGE_MEDIA_DEFAULT ||
            kind == NProto::STORAGE_MEDIA_HYBRID ||
            kind == NProto::STORAGE_MEDIA_HDD)
        {
            return NProto::STORAGE_MEDIA_HYBRID;
        } else {
            return kind;
        }
    };

    auto runIO =
        [&](bool slow, i64 expected, i64 expectedSmooth, i64 expectedCritical)
    {
        TestSufferCount(
            calc,
            TDuration::Seconds(15),
            slow,
            expected,
            expectedSmooth,
            expectedCritical);

        if (calc.IsSuffering()) {
            sufferCounters.OnDiskSuffer(diskKind);
        }
        sufferCounters.PublishCounters();

        auto serverGroup = counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "server");

        auto total = serverGroup->GetCounter("DisksSuffer", false);
        UNIT_ASSERT_VALUES_EQUAL(expected, total->Val());

        for (ui32 i = EStorageMediaKind_MIN; i <= EStorageMediaKind_MAX; ++i) {
            EStorageMediaKind kind = static_cast<EStorageMediaKind>(i);
            CheckServerSufferCounters(
                counters,
                kind,
                (commonKind(kind) == commonKind(diskKind) ? expected : 0));
        }
    };

    // run slow request
    runIO(true, 1, 1, 0);
    // run fast request
    runIO(false, 0, 0, 0);
}

};   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumePerfTest)
{
    Y_UNIT_TEST(ShouldNotReportSufferCounterIfThereAreNoSettings)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();
        auto config = CreatePerformanceSettings(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0);

        auto volume = CreateVolume(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            0,
            0,
            0,
            0);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>());

        volumePerfCalc.Register(*volumeCounters, volume);

        TStringStream strStream;
        volumeCounters->OutputPlainText(strStream);
        bool sufferFound = strStream.Str().find("Suffer") != std::string::npos;

        UNIT_ASSERT(!sufferFound);
    }

    Y_UNIT_TEST(ShouldCreateSufferCounter)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config =
            CreateDefaultPerformanceSettings(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        volumePerfCalc.Register(*volumeCounters, volume);

        TStringStream strStream;
        volumeCounters->OutputPlainText(strStream);
        bool sufferFound = strStream.Str().find("Suffer") != std::string::npos;

        UNIT_ASSERT(sufferFound);
    }

    Y_UNIT_TEST(ShouldNotSetSufferIfLoadIsBelowThresholds)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config =
            CreateDefaultPerformanceSettings(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        volumePerfCalc.Register(*volumeCounters, volume);

        volumePerfCalc.OnRequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            0,
            1,
            0,
            1024 * 1024);

        for (ui32 i = 0; i < 15; ++i) {
            volumePerfCalc.UpdateStats();
        }

        auto sufferCounter = counters->GetSubgroup("host", "cluster")
                                 ->FindSubgroup("volume", "test1")
                                 ->GetCounter("Suffer", false);
        UNIT_ASSERT_VALUES_EQUAL(0, sufferCounter->Val());
    }

    Y_UNIT_TEST(ShouldUse15SecondWindowForSmoothSuffer)
    {
        constexpr ui32 RequestBytes = 1024 * 1024;

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config = CreatePerformanceSettings(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            1,        // readIops
            128_KB,   // readBandwidth
            1,        // writeIops
            128_KB,   // writeBandwidth
            0,        // minReadIops
            0,        // minReadBandwidth
            0,        // minWriteIops
            0         // minWriteBandwidth
        );
        config.SetExpectedIoParallelism(1);
        config.MutableSsdPerfSettings()->SetCriticalFactor(100);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test_window_critical",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);

        auto volumeCounters =
            CreateVolumeCounters(counters, volume.GetDiskId());
        TVolumePerformanceCalculator calc(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));
        calc.Register(*volumeCounters, volume);

        const auto expectedCost = calc.GetExpectedWriteCost(RequestBytes);
        UNIT_ASSERT(expectedCost > TDuration::Seconds(8));
        UNIT_ASSERT(expectedCost < TDuration::Seconds(15));

        const auto requestTime = expectedCost + TDuration::Seconds(1);
        // expectedCost > 1s => per-second Suffer must stay off,
        // but for the 15s window SmoothSuffer should still be set.
        TestSufferCount(
            calc,
            requestTime,
            TDuration::Seconds(15),
            0,    // expected Suffer
            1,    // expected SmoothSuffer
            0);   // expected CriticalSuffer
    }

    Y_UNIT_TEST(ShouldUse15SecondWindowForCriticalSuffer)
    {
        constexpr ui32 RequestBytes = 1024 * 1024;

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config = CreatePerformanceSettings(
            NCloud::NProto::STORAGE_MEDIA_SSD,
            1,        // readIops
            512_KB,   // readBandwidth
            1,        // writeIops
            512_KB,   // writeBandwidth
            0,        // minReadIops
            0,        // minReadBandwidth
            0,        // minWriteIops
            0         // minWriteBandwidth
        );
        config.SetExpectedIoParallelism(1);
        config.MutableSsdPerfSettings()->SetCriticalFactor(2);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test_window_critical",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);

        auto volumeCounters =
            CreateVolumeCounters(counters, volume.GetDiskId());
        TVolumePerformanceCalculator calc(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));
        calc.Register(*volumeCounters, volume);

        const auto expectedCost = calc.GetExpectedWriteCost(RequestBytes);
        UNIT_ASSERT(expectedCost > TDuration::Seconds(1));
        UNIT_ASSERT(expectedCost < TDuration::Seconds(7));

        const auto requestTime =
            expectedCost + expectedCost + TDuration::Seconds(1);

        TestSufferCount(
            calc,
            requestTime,
            TDuration::Seconds(15),
            0,    // expected Suffer
            1,    // expected SmoothSuffer
            1);   // expected CriticalSuffer
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForSsd)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_SSD);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForHdd)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_HDD);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForHybrid)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_HYBRID);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForDefault)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_DEFAULT);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForNonReplicated)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForHddNonReplicated)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForMirror2)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForMirror3)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);
    }

    Y_UNIT_TEST(ShouldSetUnsetSufferForLocal)
    {
        TestSufferWithMetrics(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL);
    }

    Y_UNIT_TEST(ShouldUpdatePerformanceSettings)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config =
            CreateDefaultPerformanceSettings(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        volumePerfCalc.Register(*volumeCounters, volume);

        // run slow request
        TestSuffer(
            volumePerfCalc,
            volumeCounters,
            TDuration::Seconds(15),
            true,
            1,
            1,
            0);

        auto oldTime = volumePerfCalc.GetExpectedWriteCost(1024 * 1024);

        auto updatedVolume = CreateVolume(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config.GetSsdPerfSettings().GetRead().GetIops() * 10,
            config.GetSsdPerfSettings().GetRead().GetBandwidth() * 10,
            config.GetSsdPerfSettings().GetWrite().GetIops() * 10,
            config.GetSsdPerfSettings().GetWrite().GetBandwidth() * 10);

        volumePerfCalc.Register(*volumeCounters, updatedVolume);

        // run previously "slow" request and make sure it is "fast enough" now.
        TestSuffer(
            volumePerfCalc,
            volumeCounters,
            oldTime,
            TDuration::Seconds(15),
            0,
            0,
            0);
    }

    Y_UNIT_TEST(ShouldOverridePerformanceProfileLimitsWithMinLimits)
    {
        constexpr ui32 ConfigReadIops = 10000;
        constexpr ui32 ConfigWriteIops = 10000;
        constexpr ui32 ConfigReadBw = 300_MB;
        constexpr ui32 ConfigWriteBw = 300_MB;

        constexpr ui32 ProfileReadIops = 100;
        constexpr ui32 ProfileWriteIops = 100;
        constexpr ui32 ProfileReadBw = 1_MB;
        constexpr ui32 ProfileWriteBw = 1_MB;

        const auto config = CreatePerformanceSettings(
            NProto::STORAGE_MEDIA_SSD,
            ConfigReadIops,    // readIops
            ConfigReadBw,      // readBandwidth
            ConfigWriteIops,   // writeIops
            ConfigWriteBw,     // writeBandwidth
            0,                 // minReadIops
            0,                 // minReadBandwidth
            0,                 // minWriteIops
            0                  // minWriteBandwidth
        );

        // Create a config that overrides the performance profile limits.
        const auto configWithMin = CreatePerformanceSettings(
            NProto::STORAGE_MEDIA_SSD,
            ConfigReadIops,    // readIops
            ConfigReadBw,      // readBandwidth
            ConfigWriteIops,   // writeIops
            ConfigWriteBw,     // writeBandwidth
            ConfigReadIops,    // minReadIops
            ConfigReadBw,      // minReadBandwidth
            ConfigWriteIops,   // minWriteIops
            ConfigWriteBw      // minWriteBandwidth
        );

        constexpr ui32 RequestBytes = 1024 * 1024;

        {
            auto monitoring = CreateMonitoringServiceStub();
            auto counters = monitoring->GetCounters();

            auto volume = CreateVolume(
                "test_clamped",
                NProto::STORAGE_MEDIA_SSD,
                ProfileReadIops,
                ProfileReadBw,
                ProfileWriteIops,
                ProfileWriteBw);
            auto volumeCounters =
                CreateVolumeCounters(counters, volume.GetDiskId());

            TVolumePerformanceCalculator calc(
                volume,
                std::make_shared<TDiagnosticsConfig>(config));
            calc.Register(*volumeCounters, volume);

            // Since config is clamped by profile limits, expectedScore becomes
            // too large for the 1s window — per-second Suffer cannot fire even
            // for "slow" requests. SmoothSuffer still fires because the 15s
            // window has 15x more capacity.
            TestSuffer(
                calc,
                volumeCounters,
                TDuration::Seconds(15),
                true,   // slowRequest
                0,      // expected Suffer
                1,      // expected SmoothSuffer
                0);     // expected CriticalSuffer
        }

        {
            auto monitoring = CreateMonitoringServiceStub();
            auto counters = monitoring->GetCounters();

            auto volume = CreateVolume(
                "test_ignore",
                NProto::STORAGE_MEDIA_SSD,
                ProfileReadIops,
                ProfileReadBw,
                ProfileWriteIops,
                ProfileWriteBw);
            auto volumeCounters =
                CreateVolumeCounters(counters, volume.GetDiskId());

            TVolumePerformanceCalculator calc(
                volume,
                std::make_shared<TDiagnosticsConfig>(std::move(configWithMin)));

            const auto before = calc.GetExpectedWriteCost(RequestBytes);
            calc.Register(*volumeCounters, volume);
            const auto after = calc.GetExpectedWriteCost(RequestBytes);

            UNIT_ASSERT_VALUES_EQUAL(before, after);

            TestSuffer(
                calc,
                volumeCounters,
                TDuration::Seconds(15),
                true,   // slowRequest
                1,      // expected Suffer
                1,      // expected SmoothSuffer
                0);     // expected CriticalSuffer
        }
    }

    Y_UNIT_TEST(ShouldApplyThrottlerOvercommitToPerformanceProfileLimits)
    {
        constexpr double NoOvercommit = 1.0;
        constexpr double Overcommit = 2.0;

        constexpr ui32 ConfigReadIops = 10000;
        constexpr ui32 ConfigWriteIops = 20000;
        constexpr ui64 ConfigReadBw = 300_MB;
        constexpr ui64 ConfigWriteBw = 400_MB;

        constexpr ui32 ProfileReadIops = 100;
        constexpr ui32 ProfileWriteIops = 200;
        constexpr ui32 ProfileReadBw = 5_MB;
        constexpr ui32 ProfileWriteBw = 10_MB;

        constexpr ui32 RequestBytes = 1024 * 1024;

        auto baseConfig = CreatePerformanceSettings(
            NProto::STORAGE_MEDIA_SSD,
            ConfigReadIops,    // readIops
            ConfigReadBw,      // readBandwidth
            ConfigWriteIops,   // writeIops
            ConfigWriteBw,     // writeBandwidth
            0,                 // minReadIops
            0,                 // minReadBandwidth
            0,                 // minWriteIops
            0                  // minWriteBandwidth
        );
        baseConfig.SetExpectedIoParallelism(1);

        auto overcommit1Config = baseConfig;
        overcommit1Config.MutableSsdPerfSettings()->SetThrottlerOvercommit(
            NoOvercommit);

        auto overcommit2Config = baseConfig;
        overcommit2Config.MutableSsdPerfSettings()->SetThrottlerOvercommit(
            Overcommit);

        const auto volume = CreateVolume(
            "test_overcommit",
            NProto::STORAGE_MEDIA_SSD,
            ProfileReadIops,
            ProfileReadBw,
            ProfileWriteIops,
            ProfileWriteBw);

        TVolumePerformanceCalculator calcOvercommit1(
            volume,
            std::make_shared<TDiagnosticsConfig>(overcommit1Config));
        calcOvercommit1.Register(volume);

        TVolumePerformanceCalculator calcOvercommit2(
            volume,
            std::make_shared<TDiagnosticsConfig>(overcommit2Config));
        calcOvercommit2.Register(volume);

        // With overcommit=1, perf settings are clamped by profile limits.
        UNIT_ASSERT_VALUES_EQUAL(
            CostPerIO(ProfileReadIops, ProfileReadBw, RequestBytes),
            calcOvercommit1.GetExpectedReadCost(RequestBytes));
        UNIT_ASSERT_VALUES_EQUAL(
            CostPerIO(ProfileWriteIops, ProfileWriteBw, RequestBytes),
            calcOvercommit1.GetExpectedWriteCost(RequestBytes));

        // With overcommit=2, profile limits are scaled up before clamping.
        UNIT_ASSERT_VALUES_EQUAL(
            CostPerIO(
                static_cast<ui64>(ProfileReadIops) * 2,
                static_cast<ui64>(ProfileReadBw) * 2,
                RequestBytes),
            calcOvercommit2.GetExpectedReadCost(RequestBytes));
        UNIT_ASSERT_VALUES_EQUAL(
            CostPerIO(
                static_cast<ui64>(ProfileWriteIops) * 2,
                static_cast<ui64>(ProfileWriteBw) * 2,
                RequestBytes),
            calcOvercommit2.GetExpectedWriteCost(RequestBytes));

        UNIT_ASSERT(
            calcOvercommit2.GetExpectedWriteCost(RequestBytes) <
            calcOvercommit1.GetExpectedWriteCost(RequestBytes));
    }

    Y_UNIT_TEST(
        ShouldCorrectlyUpdatePerformanceSettingsIfClientPerfSettingsIncreased)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config =
            CreateDefaultPerformanceSettings(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volumeNoSettings = CreateVolume(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            0,
            0,
            0,
            0);

        auto volume = CreateVolume(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config.GetSsdPerfSettings().GetRead().GetIops() / 10,
            config.GetSsdPerfSettings().GetRead().GetBandwidth() / 10,
            config.GetSsdPerfSettings().GetWrite().GetIops() / 10,
            config.GetSsdPerfSettings().GetWrite().GetBandwidth() / 10);

        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(config));

        volumePerfCalc.Register(*volumeCounters, volumeNoSettings);

        // run slow request for default settings
        TestSuffer(
            volumePerfCalc,
            volumeCounters,
            TDuration::Seconds(15),
            true,
            1,
            1,
            0);
        auto oldTime = volumePerfCalc.GetExpectedWriteCost(1024 * 1024) +
                       TDuration::MicroSeconds(10);

        // mount with settings
        volumePerfCalc.Register(*volumeCounters, volume);
        // make sure that previously slow request becomes fast
        TestSuffer(
            volumePerfCalc,
            volumeCounters,
            oldTime,
            TDuration::Seconds(15),
            0,
            0,
            0);

        auto updatedVolume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);
        volumePerfCalc.Register(*volumeCounters, updatedVolume);

        TestSuffer(
            volumePerfCalc,
            volumeCounters,
            oldTime,
            TDuration::Seconds(15),
            1,
            1,
            0);
    }

    Y_UNIT_TEST(ShouldTrackNumberOfSufferSeconds)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config = CreateDefaultPerformanceSettings(
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        volumePerfCalc.Register(*volumeCounters, volume);

        TestSufferCount(volumePerfCalc, TDuration::Seconds(1), true, 1, 1, 0);
        TestSufferCount(volumePerfCalc, TDuration::Seconds(1), true, 2, 1, 0);

        Wait(volumePerfCalc, TDuration::Seconds(13));
        TestSufferCount(volumePerfCalc, TDuration::Seconds(1), false, 1, 1, 0);
    }

    Y_UNIT_TEST(ShouldNotTurnOnSufferIfDiskIsOverloaded)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config =
            CreateDefaultPerformanceSettings(NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NCloud::NProto::STORAGE_MEDIA_SSD,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto volumePerfCalc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        volumePerfCalc.Register(*volumeCounters, volume);

        for (ui32 i = 0; i < 33; ++i) {
            volumePerfCalc.OnRequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                0,
                DurationToCyclesSafe(
                    volumePerfCalc.GetExpectedWriteCost(4096) +
                    TDuration::Seconds(1)),
                0,
                4096);
        }

        Wait(volumePerfCalc, TDuration::Seconds(1));

        auto sufferCounter = volumeCounters->GetCounter("Suffer", false);
        UNIT_ASSERT_VALUES_EQUAL(0, sufferCounter->Val());
    }

    Y_UNIT_TEST(ShouldTakePostponedTimeIntoAccount)
    {
        using namespace NCloud::NProto;

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config = CreateDefaultPerformanceSettings();

        auto volume = CreateVolumeWithDiagnosticsProfile(
            "test1",
            NProto::STORAGE_MEDIA_SSD,
            config);
        auto volumeCounters = CreateVolumeCounters(counters, "test1");

        auto calc = TVolumePerformanceCalculator(
            volume,
            std::make_shared<TDiagnosticsConfig>(std::move(config)));

        calc.Register(*volumeCounters, volume);

        TestSufferCountPostponed(calc, TDuration::Seconds(15), 0, 0, 0);

        UNIT_ASSERT_VALUES_EQUAL(false, calc.IsSuffering());
    }

    Y_UNIT_TEST(ShouldReportZeroSufferCounters)
    {
        using namespace NCloud::NProto;

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters();

        auto config = CreateDefaultPerformanceSettings();

        auto serverGroup = counters->GetSubgroup("counters", "blockstore")
                               ->GetSubgroup("component", "server");

        TSufferCounters sufferCounters("DisksSuffer", serverGroup);

        sufferCounters.PublishCounters();

        auto perType = serverGroup->GetSubgroup("type", "ssd_mirror3")
                           ->FindCounter("DisksSuffer");
        UNIT_ASSERT(perType);
        UNIT_ASSERT_VALUES_EQUAL(0, perType->Val());
    }
}

}   // namespace NCloud::NBlockStore
