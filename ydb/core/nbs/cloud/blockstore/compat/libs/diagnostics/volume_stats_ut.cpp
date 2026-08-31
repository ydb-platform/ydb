#include "volume_stats.h"

#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/compat/libs/common/media.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer_test.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/monitoring.h>
#include <ydb/core/nbs/cloud/storage/core/libs/throttling/helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>

#include <tuple>

namespace NCloud::NBlockStore {

using NYdb::NBS::CostPerIO;
using NYdb::NBS::CreateMonitoringServiceStub;
using NYdb::NBS::CreateWallClockTimer;
using NYdb::NBS::DefaultBlockSize;
using NYdb::NBS::TTestTimer;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultCloudId = "cloud_id";
const TString DefaultFolderId = "folder_id";

////////////////////////////////////////////////////////////////////////////////

class TLabelKeeper: public NMonitoring::IMetricConsumer
{
private:
    std::vector<std::pair<TString, TString>> Labels;
    THashMap<TString, TString> ValueMap;
    TString CurrentLabel;

public:
    size_t FindLabel(TStringBuf name, TStringBuf value)
    {
        return CountIf(
            Labels,
            [name, value](const auto& labels) {
                return labels.first == name.data() &&
                       labels.second == value.data();
            });
    }

    TString GetValue(const TString labelName) const
    {
        auto it = ValueMap.find(labelName);
        return it == ValueMap.end() ? TString() : it->second;
    }

    void OnStreamBegin() override
    {}

    void OnStreamEnd() override
    {}

    void OnCommonTime(TInstant) override
    {}

    void OnMetricBegin(NMonitoring::EMetricType) override
    {}

    void OnMetricEnd() override
    {}

    void OnLabelsBegin() override
    {
        CurrentLabel.clear();
    }

    void OnLabelsEnd() override
    {}

    void OnLabel(TStringBuf name, TStringBuf value) override
    {
        Labels.emplace_back(name.data(), value.data());
        if (!CurrentLabel.empty()) {
            CurrentLabel += ".";
        }
        CurrentLabel += value;
    }

    void OnLabel(ui32, ui32) override
    {}

    void OnDouble(TInstant, double value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnInt64(TInstant, i64 value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnUint64(TInstant, ui64 value) override
    {
        ValueMap[CurrentLabel] = ToString(value);
    }

    void OnHistogram(TInstant, NMonitoring::IHistogramSnapshotPtr) override
    {}

    void OnLogHistogram(
        TInstant,
        NMonitoring::TLogHistogramSnapshotPtr) override
    {}

    void OnSummaryDouble(
        TInstant,
        NMonitoring::ISummaryDoubleSnapshotPtr) override
    {}
};

void Mount(
    IVolumeStatsPtr volumeStats,
    const TString& name,
    const TString& client,
    const TString& instance,
    NCloud::NProto::EStorageMediaKind mediaKind =
        NCloud::NProto::STORAGE_MEDIA_SSD)
{
    NProto::TVolume volume;
    volume.SetDiskId(name);
    volume.SetStorageMediaKind(mediaKind);
    volume.SetBlockSize(DefaultBlockSize);
    volume.SetCloudId(DefaultCloudId);
    volume.SetFolderId(DefaultFolderId);

    volumeStats->MountVolume(volume, client, instance);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeStatsTest)
{
    Y_TEST_HOOK_BEFORE_RUN(InitTest)
    {
        // NHPTimer warmup, see issue #2830 for more information
        Y_UNUSED(GetCyclesPerMillisecond());
    }

    Y_UNIT_TEST(ShouldTrackRequestsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&](auto volume, auto instance, auto mediaType)
        {
            return counters->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", volume)
                ->GetSubgroup("instance", instance)
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("type", mediaType);
        };

        auto writeData = [](auto volume, auto type)
        {
            auto started = volume->RequestStarted(type, 1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        };

        auto readData = [](auto volume, auto type)
        {
            auto started = volume->RequestStarted(type, 1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_HDD);
        Mount(
            volumeStats,
            "test2",
            "client3",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_HDD);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");
        auto volume3 = volumeStats->GetVolumeInfo("test2", "client3");

        auto volume1Counters = getCounters("test1", "instance1", "ssd");
        auto volume1WriteCount =
            volume1Counters->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("Count");
        auto volume1ReadCount =
            volume1Counters->GetSubgroup("request", "ReadBlocks")
                ->GetCounter("Count");

        auto volume2Counters = getCounters("test2", "instance1", "hdd");
        auto volume2WriteCount =
            volume2Counters->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("Count");

        auto volume3Counters = getCounters("test2", "instance2", "hdd");
        auto volume3WriteCount =
            volume3Counters->GetSubgroup("request", "WriteBlocks")
                ->GetCounter("Count");

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume1, EBlockStoreRequest::WriteBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 0);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume2, EBlockStoreRequest::WriteBlocks);
        readData(volume1, EBlockStoreRequest::ReadBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 0);

        writeData(volume3, EBlockStoreRequest::WriteBlocks);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 1);

        writeData(volume1, EBlockStoreRequest::WriteBlocksLocal);
        readData(volume1, EBlockStoreRequest::ReadBlocksLocal);

        UNIT_ASSERT_EQUAL(volume1WriteCount->Val(), 2);
        UNIT_ASSERT_EQUAL(volume1ReadCount->Val(), 2);
        UNIT_ASSERT_EQUAL(volume2WriteCount->Val(), 1);
        UNIT_ASSERT_EQUAL(volume3WriteCount->Val(), 1);
    }

    Y_UNIT_TEST(ShouldRegisterAndUnregisterCountersPerVolume)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        // Mount:
        //    volume1(client1 + instance1)
        //    volume2(client2 + instance2)
        Mount(
            volumeStats,
            "volume1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Mount(
            volumeStats,
            "volume2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume1")
                        ->GetSubgroup("instance", "instance1")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume2")
                        ->GetSubgroup("instance", "instance2")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        // Since the timeout has not expired, both clients remain connected.
        Timer->AdvanceTime(inactivityTimeout * 0.5);
        volumeStats->TrimVolumes();

        // Mount new client to volume2 (client3 + instance3)
        Mount(
            volumeStats,
            "volume2",
            "client3",
            "instance3",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        // All three clients send metrics.
        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume1")
                        ->GetSubgroup("instance", "instance1")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume2")
                        ->GetSubgroup("instance", "instance2")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume2")
                        ->GetSubgroup("instance", "instance3")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        // Since the timeout for the first two clients has expired, only the new
        // client sends the metrics.
        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                         ->FindSubgroup("volume", "volume1"));

        UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                         ->GetSubgroup("volume", "volume2")
                         ->FindSubgroup("instance", "instance2"));

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume2")
                        ->GetSubgroup("instance", "instance3")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        // Mount new copied disk volume2-copy (client4 + instance4)
        Mount(
            volumeStats,
            "volume2-copy",
            "client4",
            "instance4",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        // Since the timeout has not expired, both clients remain connected.
        Timer->AdvanceTime(inactivityTimeout * 0.3);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "volume2")
                        ->GetSubgroup("instance", "instance3")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(
            counters->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "volume2")   // volume2-copy -> volume2
                ->GetSubgroup("instance", "instance4")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->FindSubgroup("folder", DefaultFolderId));

        // Timeout for instance3 expired.
        Timer->AdvanceTime(inactivityTimeout * 0.3);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                         ->GetSubgroup("volume", "volume2")
                         ->GetSubgroup("instance", "instance3")
                         ->GetSubgroup("cloud", DefaultCloudId)
                         ->FindSubgroup("folder", DefaultFolderId));

        UNIT_ASSERT(
            counters->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "volume2")   // volume2-copy -> volume2
                ->GetSubgroup("instance", "instance4")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->FindSubgroup("folder", DefaultFolderId));

        // Timeout for instance4 expired.
        Timer->AdvanceTime(inactivityTimeout * 0.5);
        volumeStats->TrimVolumes();
        UNIT_ASSERT(
            !counters->GetSubgroup("host", "cluster")
                 ->GetSubgroup("volume", "volume2")   // volume2-copy -> volume2
                 ->GetSubgroup("instance", "instance4")
                 ->GetSubgroup("cloud", DefaultCloudId)
                 ->FindSubgroup("folder", DefaultFolderId));
    }

    Y_UNIT_TEST(ShouldTrackSilentErrorsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&](auto volume, auto instance, auto mediaType)
        {
            auto volumeCounters = counters->GetSubgroup("host", "cluster")
                                      ->GetSubgroup("volume", volume)
                                      ->GetSubgroup("instance", instance)
                                      ->GetSubgroup("cloud", DefaultCloudId)
                                      ->GetSubgroup("folder", DefaultFolderId)
                                      ->GetSubgroup("type", mediaType)
                                      ->GetSubgroup("request", "WriteBlocks");

            return std::make_pair(
                volumeCounters->GetCounter("Errors/Fatal"),
                volumeCounters->GetCounter("Errors/Silent"));
        };

        auto shoot = [](auto volume, auto errorKind)
        {
            auto started = volume->RequestStarted(
                EBlockStoreRequest::WriteBlocks,
                1024 * 1024);

            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                started,
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1024 * 1024,
                errorKind,
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_HDD);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");

        auto [volume1Errors, volume1Silent] =
            getCounters("test1", "instance1", "ssd");
        auto [volume2Errors, volume2Silent] =
            getCounters("test2", "instance2", "hdd");

        UNIT_ASSERT_VALUES_EQUAL(0, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume1, EDiagnosticsErrorKind::ErrorFatal);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume2, EDiagnosticsErrorKind::ErrorFatal);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume1, EDiagnosticsErrorKind::ErrorSilent);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, volume2Silent->Val());

        shoot(volume2, EDiagnosticsErrorKind::ErrorSilent);

        UNIT_ASSERT_VALUES_EQUAL(1, volume1Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume1Silent->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, volume2Silent->Val());
    }

    Y_UNIT_TEST(ShouldTrackHwProblemsPerVolume)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto getCounters = [&](auto volume, auto instance, auto mediaKind)
        {
            auto volumeCounters =
                counters->GetSubgroup("host", "cluster")
                    ->GetSubgroup("volume", volume)
                    ->GetSubgroup("instance", instance)
                    ->GetSubgroup("cloud", DefaultCloudId)
                    ->GetSubgroup("folder", DefaultFolderId)
                    ->GetSubgroup("type", MediaKindToStatsString(mediaKind));

            return std::make_tuple(
                volumeCounters->GetSubgroup("request", "WriteBlocks")
                    ->GetCounter("Errors/Fatal"),
                volumeCounters->GetCounter("HwProblems"));
        };

        auto shoot = [](auto volume, auto errorKind, ui32 errorFlags)
        {
            auto started = volume->RequestStarted(
                EBlockStoreRequest::WriteBlocks,
                1024 * 1024);

            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                started,
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1024 * 1024,
                errorKind,
                errorFlags,
                false,
                0);
        };

        auto mount = [&volumeStats, &getCounters](
                         const TString& name,
                         NCloud::NProto::EStorageMediaKind mediaKind)
        {
            const auto client = name + "Client";
            const auto instance = name + "Instance";

            Mount(volumeStats, name, client, instance, mediaKind);

            auto stats = volumeStats->GetVolumeInfo(name, client);
            auto [errors, hwProblems] = getCounters(name, instance, mediaKind);

            return std::make_tuple(
                std::move(stats),
                std::move(errors),
                std::move(hwProblems));
        };

        auto [localStats, localErrors, localHwProblems] =
            mount("local", NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL);
        auto [nonreplStats, nonreplErrors, nonreplHwProblems] =
            mount("nonrepl", NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        auto [hddNonreplStats, hddNonreplErrors, hddNonreplHwProblems] = mount(
            "hdd_nonrepl",
            NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
        auto [ssdStats, ssdErrors, ssdHwProblems] =
            mount("ssd", NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT_VALUES_EQUAL(0, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            localStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NYdb::NBS::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            nonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NYdb::NBS::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            hddNonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NYdb::NBS::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            ssdStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NYdb::NBS::NProto::EF_NONE);

        UNIT_ASSERT_VALUES_EQUAL(1, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            localStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            nonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            hddNonreplStats,
            EDiagnosticsErrorKind::ErrorSilent,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        shoot(
            ssdStats,
            EDiagnosticsErrorKind::ErrorFatal,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());

        // Check if mirror disks are forgotten.
        auto [mirror2Stats, mirror2Errors, mirror2HwProblems] =
            mount("mirror2", NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2);
        auto [mirror3Stats, mirror3Errors, mirror3HwProblems] =
            mount("mirror3", NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);

        shoot(
            mirror2Stats,
            EDiagnosticsErrorKind::ErrorSilent,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);
        shoot(
            mirror3Stats,
            EDiagnosticsErrorKind::ErrorFatal,
            NYdb::NBS::NProto::EF_HW_PROBLEMS_DETECTED);

        UNIT_ASSERT_VALUES_EQUAL(2, localErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, localHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, nonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddNonreplErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, hddNonreplHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, ssdErrors->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, ssdHwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mirror2Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror2HwProblems->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror3Errors->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirror2HwProblems->Val());
    }

    void DoTestShouldReportSufferMetrics(
        const TVector<TString>& strictSLACloudIds,
        bool reportStrictSLA)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        NProto::TDiagnosticsConfig cfg;
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetBandwidth(342000000);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetBandwidth(342000000);
        for (const auto& cloudId: strictSLACloudIds) {
            *cfg.AddCloudIdsWithStrictSLA() = cloudId;
        }
        auto diagConfig = std::make_shared<TDiagnosticsConfig>(std::move(cfg));

        auto volumeStats = CreateVolumeStats(
            monitoring,
            diagConfig,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto volume = volumeStats->GetVolumeInfo("test1", "client1");

        auto requestDuration =
            TDuration::MilliSeconds(400) +
            diagConfig->GetExpectedIoParallelism() *
                CostPerIO(
                    diagConfig->GetSsdPerfSettings().Write.Iops,
                    diagConfig->GetSsdPerfSettings().Write.Bandwidth,
                    1_MB);
        auto durationInCycles = DurationToCyclesSafe(requestDuration);
        auto now = GetCycleCount();

        volume->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            now - Min(now, durationInCycles),
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1_MB,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);

        volumeStats->UpdateStats(false);

        auto sufferArray = volumeStats->GatherVolumePerfStatuses();
        UNIT_ASSERT_VALUES_EQUAL(1, sufferArray.size());
        UNIT_ASSERT_VALUES_EQUAL("test1", sufferArray[0].first);
        UNIT_ASSERT_VALUES_EQUAL(1, sufferArray[0].second);

        auto disksSufferCounter = counters->GetCounter("DisksSuffer", false);
        auto ssdDisksSufferCounter = counters->GetSubgroup("type", "ssd")
                                         ->GetCounter("DisksSuffer", false);
        auto hddDisksSufferCounter = counters->GetSubgroup("type", "hdd")
                                         ->GetCounter("DisksSuffer", false);
        auto smoothDisksSufferCounter =
            counters->GetCounter("SmoothDisksSuffer", false);
        auto smoothSsdDisksSufferCounter =
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("SmoothDisksSuffer", false);
        auto smoothHddDisksSufferCounter =
            counters->GetSubgroup("type", "hdd")
                ->GetCounter("SmoothDisksSuffer", false);
        auto criticalDisksSufferCounter =
            counters->GetCounter("CriticalDisksSuffer", false);
        auto criticalSsdDisksSufferCounter =
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("CriticalDisksSuffer", false);
        auto criticalHddDisksSufferCounter =
            counters->GetSubgroup("type", "hdd")
                ->GetCounter("CriticalDisksSuffer", false);

        auto strictSLADisksSufferCounter =
            counters->GetCounter("StrictSLADisksSuffer", false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            reportStrictSLA ? 1 : 0,
            strictSLADisksSufferCounter->Val());

        // a bunch of fast requests
        const auto fastRequestCyclesCount =
            DurationToCyclesSafe(TDuration::MilliSeconds(10));
        for (ui32 i = 0; i < 5; ++i) {
            now = GetCycleCount();
            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                now - Min(now, fastRequestCyclesCount),
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1_MB,
                {},
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        }

        volumeStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, strictSLADisksSufferCounter->Val());

        // a bunch of slow but not critically slow requests
        const auto slowRequestCyclesCount =
            DurationToCyclesSafe(TDuration::MilliSeconds(110));
        for (ui32 i = 0; i < 20; ++i) {
            now = GetCycleCount();
            volume->RequestCompleted(
                EBlockStoreRequest::WriteBlocks,
                now - Min(now, slowRequestCyclesCount),
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1_MB,
                {},
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        }

        volumeStats->UpdateStats(false);

        UNIT_ASSERT_VALUES_EQUAL(1, disksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, ssdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, hddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(1, smoothDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, smoothSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, smoothHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, criticalDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalSsdDisksSufferCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, criticalHddDisksSufferCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            reportStrictSLA ? 1 : 0,
            strictSLADisksSufferCounter->Val());
    }

    Y_UNIT_TEST(ShouldReportSufferMetrics)
    {
        DoTestShouldReportSufferMetrics({"something"}, false);
    }

    Y_UNIT_TEST(ShouldReportSufferMetricsWithStrictSLAFilter)
    {
        DoTestShouldReportSufferMetrics({DefaultCloudId}, true);
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculatePossiblePostponeTimeForVolume)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto volumeStats = CreateVolumeStats(
            CreateMonitoringServiceStub(),
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TVector<TString> clients = {"client1", "client2"};
        TVector<TString> volumes = {"test1", "test2"};
        TVector<IVolumeInfoPtr> volumeInfos;

        for (size_t i = 0; i < Min(clients.size(), volumes.size()); ++i) {
            Mount(
                volumeStats,
                volumes[i],
                clients[i],
                "instance" + std::to_string(i),
                NCloud::NProto::STORAGE_MEDIA_SSD);
            volumeInfos.push_back(
                volumeStats->GetVolumeInfo(volumes[i], clients[i]));
        }

        const auto postponeDuration = TDuration::Seconds(1);

        volumeInfos[0]->RequestStarted(EBlockStoreRequest::WriteBlocks, 1024);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        volumeInfos[0]->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            postponeDuration,
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1024,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());

        volumeInfos[1]->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            postponeDuration,
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1024,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            postponeDuration,
            volumeInfos[1]->GetPossiblePostponeDuration());

        timer->AdvanceTime(config->GetPostponeTimePredictorInterval() / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[0]->GetPossiblePostponeDuration());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            volumeInfos[1]->GetPossiblePostponeDuration());
    }

    Y_UNIT_TEST(ShouldTrackDownDisksForCompletedRequests)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TString client{"client1"};
        TString volume{"test1"};
        IVolumeInfoPtr volumeInfo;

        Mount(
            volumeStats,
            volume,
            client,
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        volumeInfo = volumeStats->GetVolumeInfo(volume, client);

        timer->AdvanceTime(TDuration::Seconds(15));

        volumeInfo->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1024,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);

        volumeStats->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "test1")
                ->GetSubgroup("instance", "instance")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("type", "ssd")
                ->GetCounter("HasDowntime")
                ->Val());

        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "test1")
                ->GetSubgroup("instance", "instance")
                ->GetSubgroup("cloud", DefaultCloudId)
                ->GetSubgroup("folder", DefaultFolderId)
                ->GetSubgroup("type", "ssd")
                ->GetCounter("HasDowntime")
                ->Val());
    }

    Y_UNIT_TEST(ShouldTrackDownDisksForIncompleteRequests)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        TString client{"client1"};
        TString volume{"test1"};
        IVolumeInfoPtr volumeInfo;

        Mount(
            volumeStats,
            volume,
            client,
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        volumeInfo = volumeStats->GetVolumeInfo(volume, client);

        volumeInfo->AddIncompleteStats(
            EBlockStoreRequest::WriteBlocks,
            TRequestTime{
                .TotalTime = TDuration::Seconds(15),
                .ExecutionTime = TDuration::Seconds(15)});

        timer->AdvanceTime(TDuration::Seconds(15));

        volumeStats->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")
                ->Val());

        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counters->GetCounter("DownDisks")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counters->GetSubgroup("type", "ssd")
                ->GetCounter("DownDisks")
                ->Val());
    }

    Y_UNIT_TEST(ShouldTrackAvailabilityCounters)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");
        auto available = availabilityCounters->GetCounter("AvailableSeconds");
        auto healthy = availabilityCounters->GetCounter("HealthySeconds");

        // A healthy, served volume advances all three counters by the real time
        // elapsed since it was mounted (seeded at mount time).
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(15, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());

        // Accumulation happens on every tick, not only on the publish tick, so
        // updateIntervalFinished == false must still advance the counters.
        timer->AdvanceTime(TDuration::Seconds(1));
        volumeStats->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(16, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(16, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(16, healthy->Val());

        // A tick with no elapsed time credits nothing.
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(16, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(16, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(16, healthy->Val());

        timer->AdvanceTime(TDuration::Seconds(14));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(30, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, healthy->Val());
    }

    Y_UNIT_TEST(ShouldCreditAvailabilityFromMountTime)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        // The service has already ticked for a while on the first volume.
        timer->AdvanceTime(TDuration::Seconds(10));
        volumeStats->UpdateStats(true);

        // A second volume is mounted mid-interval, well after the previous
        // tick.
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto observed2 = monitoring->GetCounters()
                             ->GetSubgroup("counters", "blockstore")
                             ->GetSubgroup("component", "sli_volume")
                             ->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "test2")
                             ->GetSubgroup("instance", "instance2")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->GetSubgroup("folder", DefaultFolderId)
                             ->GetSubgroup("type", "network-ssd")
                             ->GetCounter("ObservedSeconds");

        // Five seconds later the new volume must be credited only for the 5s it
        // was actually served, not for the whole tick interval.
        timer->AdvanceTime(TDuration::Seconds(5));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(5, observed2->Val());
    }

    Y_UNIT_TEST(ShouldNotCreditLargeGapAsAvailability)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");

        // A gap larger than the publish interval (e.g. stats were not updated
        // for a long time / the clock jumped forward) must not be credited as
        // availability: the increment is dropped and the timestamp resynced.
        timer->AdvanceTime(TDuration::Seconds(60));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(0, observed->Val());

        // A normal tick after the resync is accounted as usual.
        timer->AdvanceTime(TDuration::Seconds(10));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(10, observed->Val());
    }

    Y_UNIT_TEST(ShouldNotAdvanceAvailableSecondsDuringDowntime)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        auto volumeInfo = volumeStats->GetVolumeInfo("test1", "client1");

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");
        auto available = availabilityCounters->GetCounter("AvailableSeconds");
        auto healthy = availabilityCounters->GetCounter("HealthySeconds");

        // One healthy interval: everything advances.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(15, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());

        // Force downtime: a completed request whose measured duration exceeds
        // the downtime threshold (requestStarted set far in the past in cycle
        // terms), same trick as the DownDisks tests above.
        volumeInfo->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            timer->Now().MicroSeconds(),
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1024,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);

        // During downtime observed still advances, but available and healthy
        // freeze.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(30, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());
    }

    Y_UNIT_TEST(ShouldFreezeHealthySecondsDuringCriticalSuffering)
    {
        auto timer = std::make_shared<TTestTimer>();

        NProto::TDiagnosticsConfig cfg;
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableWrite()->SetBandwidth(342000000);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetIops(4200);
        cfg.MutableSsdPerfSettings()->MutableRead()->SetBandwidth(342000000);
        auto config = std::make_shared<TDiagnosticsConfig>(std::move(cfg));

        auto monitoring = CreateMonitoringServiceStub();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        auto volumeInfo = volumeStats->GetVolumeInfo("test1", "client1");

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");
        auto available = availabilityCounters->GetCounter("AvailableSeconds");
        auto healthy = availabilityCounters->GetCounter("HealthySeconds");

        // One healthy interval: everything advances.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(15, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());

        // A single, heavily over-latency write drives critical suffering (but
        // stays well below the 5s SSD downtime threshold, so the volume is
        // still "available").
        auto requestDuration =
            TDuration::MilliSeconds(400) +
            config->GetExpectedIoParallelism() *
                CostPerIO(
                    config->GetSsdPerfSettings().Write.Iops,
                    config->GetSsdPerfSettings().Write.Bandwidth,
                    1_MB);
        auto durationInCycles = DurationToCyclesSafe(requestDuration);
        auto now = GetCycleCount();
        volumeInfo->RequestCompleted(
            EBlockStoreRequest::WriteBlocks,
            now - Min(now, durationInCycles),
            TDuration::Zero(),   // postponedTime
            TDuration::Zero(),   // backoffTime
            TDuration::Zero(),   // shapingTime
            1_MB,
            {},
            NYdb::NBS::NProto::EF_NONE,
            false,
            0);

        // During critical suffering observed and available advance, but
        // healthy freezes.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(30, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());
    }

    Y_UNIT_TEST(ShouldStopAccruingAvailabilityAfterVolumeTrimmed)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        // Finite inactivity timeout so that TrimVolumes() actually removes the
        // instance. NOTE: UnmountVolume() is intentionally a no-op for this
        // class (an instance may back several endpoints / live migration), so a
        // volume keeps accruing during the grace period until it is trimmed —
        // this mirrors the existing HasDowntime/RequestCounters behaviour.
        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Seconds(10),
            EVolumeStatsType::EServerStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");

        // One accounted interval while served.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(15, observed->Val());

        // Unmount alone is a no-op for this class: the instance (and therefore
        // its counter) is not removed until TrimVolumes fires, so it must
        // still be present and still accrue on the next tick.
        volumeStats->UnmountVolume("test1", "client1");
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT(availabilityCounters->FindCounter("ObservedSeconds"));
        UNIT_ASSERT_VALUES_EQUAL(30, observed->Val());

        // The instance has now been inactive longer than the timeout
        // (now - LastRemountTime = 30s > 10s), so TrimVolumes removes it and
        // the counter stops advancing.
        volumeStats->TrimVolumes();
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(30, observed->Val());
    }

    Y_UNIT_TEST(ShouldTrackClientAvailabilityCountersInSliVolume)
    {
        auto timer = std::make_shared<TTestTimer>();
        const auto config =
            std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());

        auto monitoring = CreateMonitoringServiceStub();

        // Client-side stats must place the cumulative availability counters in
        // the same narrow component=sli_volume tree as the server, so a
        // monitoring agent on the compute host can scrape only these sensors.
        auto volumeStats = CreateVolumeStats(
            monitoring,
            config,
            TDuration::Max(),
            EVolumeStatsType::EClientStats,
            timer);

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        auto availabilityCounters = monitoring->GetCounters()
                                        ->GetSubgroup("counters", "blockstore")
                                        ->GetSubgroup("component", "sli_volume")
                                        ->GetSubgroup("host", "cluster")
                                        ->GetSubgroup("volume", "test1")
                                        ->GetSubgroup("instance", "instance")
                                        ->GetSubgroup("cloud", DefaultCloudId)
                                        ->GetSubgroup("folder", DefaultFolderId)
                                        ->GetSubgroup("type", "network-ssd");

        auto observed = availabilityCounters->GetCounter("ObservedSeconds");
        auto available = availabilityCounters->GetCounter("AvailableSeconds");
        auto healthy = availabilityCounters->GetCounter("HealthySeconds");

        // A healthy, served volume advances all three counters by the real
        // time elapsed since it was mounted, exactly like the server path.
        timer->AdvanceTime(TDuration::Seconds(15));
        volumeStats->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(15, observed->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, available->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, healthy->Val());

        // The counters must live in the narrow sli_volume tree, not nested in
        // the wide component=client_volume per-volume group.
        auto clientVolume = monitoring->GetCounters()
                                ->GetSubgroup("counters", "blockstore")
                                ->GetSubgroup("component", "client_volume")
                                ->GetSubgroup("host", "cluster")
                                ->GetSubgroup("volume", "test1")
                                ->GetSubgroup("instance", "instance")
                                ->GetSubgroup("cloud", DefaultCloudId)
                                ->GetSubgroup("folder", DefaultFolderId);
        UNIT_ASSERT(!clientVolume->FindCounter("ObservedSeconds"));
    }

    Y_UNIT_TEST(ShouldAlterVolume)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        NProto::TVolume volume;
        volume.SetDiskId("volume-1");
        volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        volume.SetBlockSize(DefaultBlockSize);

        volume.SetCloudId("cloud-1");
        volume.SetFolderId("folder-1");
        volumeStats->MountVolume(volume, "client-1", "instance-1");

        {
            TLabelKeeper keeper;
            volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("project", "cloud-1"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("cluster", "folder-1"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("project", "cloud-2"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("cluster", "folder-2"), 0);
        }

        volume.SetCloudId("cloud-2");
        volume.SetFolderId("folder-2");
        volumeStats->MountVolume(volume, "client-1", "instance-1");

        {
            TLabelKeeper keeper;
            volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

            UNIT_ASSERT_EQUAL(keeper.FindLabel("project", "cloud-1"), 0);
            UNIT_ASSERT_EQUAL(keeper.FindLabel("cluster", "folder-1"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("project", "cloud-2"), 0);
            UNIT_ASSERT_UNEQUAL(keeper.FindLabel("cluster", "folder-2"), 0);
        }
    }

    Y_UNIT_TEST(ShouldRemoveVolumeInfoByTimeoutIfNotPinned)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "disk-1",
            "client-1",
            "instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));
        }

        Timer->AdvanceTime(inactivityTimeout * 0.5);

        Mount(
            volumeStats,
            "disk-2",
            "client-2",
            "instance-2",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        {
            UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "disk-1")
                             ->GetSubgroup("instance", "instance-1")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        {
            UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "disk-2")
                             ->GetSubgroup("instance", "instance-2")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }
    }

    Y_UNIT_TEST(ShouldNotRemoveVolumeInfoByTimeoutIfPinned)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        NProto::TDiagnosticsConfig config;
        config.SetEnableDurableVolumeInfo(true);

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(config),
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "disk-1",
            "client-1",
            "instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));
        }

        auto pin1_1 = volumeStats->PinVolumeInfo("disk-1", "client-1");

        Timer->AdvanceTime(inactivityTimeout * 0.5);

        Mount(
            volumeStats,
            "disk-2",
            "client-2",
            "instance-2",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        auto pin2_1 = volumeStats->PinVolumeInfo("disk-2", "client-2");

        // Must not remove pinned VolumeInfos by timeout
        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        // Must not remove pinned VolumeInfos by timeout
        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        // Must not remove pinned VolumeInfos
        volumeStats->UnmountVolume("disk-1", "client-1");
        volumeStats->UnmountVolume("disk-2", "client-2");
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        // Must not remove multiple pinned VolumeInfos by timeout
        auto pin1_2 = volumeStats->PinVolumeInfo("disk-1", "client-1");
        auto pin1_3 = volumeStats->PinVolumeInfo("disk-1", "client-1");
        auto pin2_2 = volumeStats->PinVolumeInfo("disk-2", "client-2");
        auto pin2_3 = volumeStats->PinVolumeInfo("disk-2", "client-2");
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        // Remounts must not affect Pin count (next checks)
        for (size_t i = 0; i < 10; i++) {
            Mount(
                volumeStats,
                "disk-1",
                "client-1",
                "instance-1",
                NCloud::NProto::STORAGE_MEDIA_SSD);
            Mount(
                volumeStats,
                "disk-2",
                "client-2",
                "instance-2",
                NCloud::NProto::STORAGE_MEDIA_SSD);
        }
        Timer->AdvanceTime(inactivityTimeout * 1.1);

        // Must not remove partially unpinned VolumeInfos regardless of
        // pin/unpin order
        pin1_1.Reset();
        pin1_2.Reset();
        pin2_3.Reset();
        pin2_2.Reset();
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-2")
                            ->GetSubgroup("instance", "instance-2")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }

        // Must remove fully unpinned VolumeInfos by timeout
        pin1_3.Reset();
        pin2_1.Reset();
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "disk-1")
                             ->GetSubgroup("instance", "instance-1")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));

            UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "disk-2")
                             ->GetSubgroup("instance", "instance-2")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-2"));
        }
    }

    Y_UNIT_TEST(ShouldRespectEnableDurableVolumeInfoConfig)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        NProto::TDiagnosticsConfig config;
        // false by default until ensured safe
        UNIT_ASSERT(!config.GetEnableDurableVolumeInfo());

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(config),
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "disk-1",
            "client-1",
            "instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        {
            UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                            ->GetSubgroup("volume", "disk-1")
                            ->GetSubgroup("instance", "instance-1")
                            ->GetSubgroup("cloud", DefaultCloudId)
                            ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));
        }

        auto pin = volumeStats->PinVolumeInfo("disk-1", "client-1");

        Timer->AdvanceTime(inactivityTimeout * 0.5);
        // Timeout not expired - must not remove VolumeInfo
        volumeStats->TrimVolumes();

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        // Timeout expired - must remove VolumeInfo despite pinned
        volumeStats->TrimVolumes();
        {
            UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                             ->GetSubgroup("volume", "disk-1")
                             ->GetSubgroup("instance", "instance-1")
                             ->GetSubgroup("cloud", DefaultCloudId)
                             ->FindSubgroup("folder", DefaultFolderId));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
        }
    }

    Y_UNIT_TEST(ShouldMountSeveralClientsOnOneInstance)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-1",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-2",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "Disk-1")
                        ->GetSubgroup("instance", "Instance-1")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        {
            auto client1Info = volumeStats->GetVolumeInfo("Disk-1", "Client-1");
            auto client2Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            UNIT_ASSERT_EQUAL(client1Info.get(), client2Info.get());
            UNIT_ASSERT(client1Info);
        }

        Timer->AdvanceTime(inactivityTimeout * 0.5);

        Mount(
            volumeStats,
            "Disk-1",
            "Client-2",
            "Instance-1",
            NCloud::NProto::STORAGE_MEDIA_SSD);

        Timer->AdvanceTime(inactivityTimeout * 0.6);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(counters->GetSubgroup("host", "cluster")
                        ->GetSubgroup("volume", "Disk-1")
                        ->GetSubgroup("instance", "Instance-1")
                        ->GetSubgroup("cloud", DefaultCloudId)
                        ->FindSubgroup("folder", DefaultFolderId));

        {
            auto client1Info = volumeStats->GetVolumeInfo("Disk-1", "Client-1");
            auto client2Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            // Note: information about all volume clients is kept until volume
            // is unmounted (trimmed) even if there are no mount requests form
            // some clients during inactivityTimeout, and VolumeInfo can be
            // obtained for such inactive client while exists
            UNIT_ASSERT_EQUAL(client1Info.get(), client2Info.get());
            UNIT_ASSERT(client1Info);
        }

        Timer->AdvanceTime(inactivityTimeout * 1.1);
        volumeStats->TrimVolumes();

        UNIT_ASSERT(!counters->GetSubgroup("host", "cluster")
                         ->GetSubgroup("volume", "Disk-1")
                         ->GetSubgroup("instance", "Instance-1")
                         ->GetSubgroup("cloud", DefaultCloudId)
                         ->FindSubgroup("folder", DefaultFolderId));

        {
            auto client1Info = volumeStats->GetVolumeInfo("Disk-1", "Client-1");
            auto client2Info = volumeStats->GetVolumeInfo("Disk-1", "Client-2");
            UNIT_ASSERT(!client1Info);
            UNIT_ASSERT(!client2Info);
        }
    }

    Y_UNIT_TEST(ShouldProperlyAccountSeveralVolumesForSameClient)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        NProto::TDiagnosticsConfig config;

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(config),
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        // clang-format off

        // Initial mount
        {
            Mount(volumeStats, "disk-1", "client-1", "instance-1");
            Mount(volumeStats, "disk-2", "client-1", "instance-2");
            Mount(volumeStats, "disk-3", "client-1", "instance-3");

            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-3", "client-1"));
        }

        // Remove volumes one by one by timeout, the rest must remain

        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-2", "client-1", "instance-1");
            Mount(volumeStats, "disk-3", "client-1", "instance-1");

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-1"));
        }

        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-2", "client-1", "instance-1");

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-1"));
        }

        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-1"));
        }
        // clang-format on
    }

    Y_UNIT_TEST(ShouldProperlyAccountSeveralVolumesForSameAndDifferentClients)
    {
        auto inactivityTimeout = TDuration::MilliSeconds(10);

        auto monitoring = CreateMonitoringServiceStub();
        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        NProto::TDiagnosticsConfig config;

        std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(config),
            inactivityTimeout,
            EVolumeStatsType::EServerStats,
            Timer);

        // clang-format off

        // Initial mount
        {
            Mount(volumeStats, "disk-1", "client-1", "" );
            Mount(volumeStats, "disk-2", "client-1", "instance-1" );
            Mount(volumeStats, "disk-3", "client-1", "instance-2" );

            Mount(volumeStats, "disk-1", "client-2", "" );
            Mount(volumeStats, "disk-2", "client-2", "instance-1" );
            Mount(volumeStats, "disk-3", "client-2", "instance-2" );

            Mount(volumeStats, "disk-1", "client-3", "instance-1" );
            Mount(volumeStats, "disk-2", "client-3", "instance-1" );
            Mount(volumeStats, "disk-3", "client-3", "instance-1" );

            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT(volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // Remove some volumes by timeout,
        // every still existing [diskId, instanceId] combination must remain

        // Note: information about all volume clients is kept until volume
        // is unmounted (trimmed) even if there are no mount requests form
        // some clients during inactivityTimeout, and VolumeInfo can be obtained
        // for such inactive client while exists

        // Keep:
        //  - all
        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-1", "client-1", "" );
            Mount(volumeStats, "disk-3", "client-1", "instance-2" );

            Mount(volumeStats, "disk-1", "client-2", ""           );
            Mount(volumeStats, "disk-2", "client-2", "instance-1" );
            Mount(volumeStats, "disk-3", "client-2", "instance-2" );

            Mount(volumeStats, "disk-1", "client-3", "instance-1" );
            Mount(volumeStats, "disk-2", "client-3", "instance-1" );
            Mount(volumeStats, "disk-3", "client-3", "instance-1" );

            volumeStats->TrimVolumes();

            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // Keep:
        //  - [disk-3, instance-2]
        //  - [disk-2, instance-1]
        //  - [disk-1, instance-1]
        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-3", "client-1", "instance-2" );

            Mount(volumeStats, "disk-2", "client-2", "instance-1" );
            Mount(volumeStats, "disk-3", "client-2", "instance-2" );

            Mount(volumeStats, "disk-1", "client-3", "instance-1" );
            Mount(volumeStats, "disk-2", "client-3", "instance-1" );

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // Keep:
        //  - [disk-3, instance-2]
        //  - [disk-2, instance-1]
        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-3", "client-1", "instance-2" );

            Mount(volumeStats, "disk-2", "client-2", "instance-1" );
            Mount(volumeStats, "disk-3", "client-2", "instance-2" );

            Mount(volumeStats, "disk-2", "client-3", "instance-1" );

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // Keep:
        //  - [disk-3, instance-2]
        //  - [disk-2, instance-1]
        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            Mount(volumeStats, "disk-3", "client-1", "instance-2" );

            Mount(volumeStats, "disk-2", "client-2", "instance-1" );

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT( volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // Keep none
        {
            Timer->AdvanceTime(inactivityTimeout * 1.1);

            volumeStats->TrimVolumes();

            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-1"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-2"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-2"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-2"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-1", "client-3"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-2", "client-3"));
            UNIT_ASSERT(!volumeStats->GetVolumeInfo("disk-3", "client-3"));
        }

        // clang-format on
    }

    Y_UNIT_TEST(ShouldSkipReportingZeroBlocksMetricsForYDBBasedDisks)
    {
        auto monitoring = CreateMonitoringServiceStub();
        NProto::TDiagnosticsConfig diagnostics;
        diagnostics.SetSkipReportingZeroBlocksMetricsForYDBBasedDisks(true);

        auto counters = monitoring->GetCounters()
                            ->GetSubgroup("counters", "blockstore")
                            ->GetSubgroup("component", "server_volume");

        auto volumeStats = CreateVolumeStats(
            monitoring,
            std::make_shared<TDiagnosticsConfig>(diagnostics),
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        auto sendRequest = [](auto volume, auto type)
        {
            auto started = volume->RequestStarted(type, 1024 * 1024);

            volume->RequestCompleted(
                type,
                started,
                TDuration::Zero(),   // postponedTime
                TDuration::Zero(),   // backoffTime
                TDuration::Zero(),   // shapingTime
                1024 * 1024,
                EDiagnosticsErrorKind::Success,
                NYdb::NBS::NProto::EF_NONE,
                false,
                0);
        };

        Mount(
            volumeStats,
            "test1",
            "client1",
            "instance1",
            NCloud::NProto::STORAGE_MEDIA_SSD);
        Mount(
            volumeStats,
            "test2",
            "client2",
            "instance2",
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3);

        auto volume1 = volumeStats->GetVolumeInfo("test1", "client1");
        auto volume2 = volumeStats->GetVolumeInfo("test2", "client2");

        sendRequest(volume1, EBlockStoreRequest::WriteBlocks);
        sendRequest(volume1, EBlockStoreRequest::ZeroBlocks);
        sendRequest(volume2, EBlockStoreRequest::WriteBlocks);
        sendRequest(volume2, EBlockStoreRequest::ZeroBlocks);

        TLabelKeeper keeper;
        volumeStats->GetUserCounters()->Append(TInstant::Now(), &keeper);

        UNIT_ASSERT_EQUAL(
            keeper.GetValue(
                "compute.cloud_id.folder_id.test1.instance1.disk.write_ops"),
            "1");
        UNIT_ASSERT_EQUAL(
            keeper.GetValue(
                "compute.cloud_id.folder_id.test2.instance2.disk.write_ops"),
            "2");
    }
}

}   // namespace NCloud::NBlockStore
