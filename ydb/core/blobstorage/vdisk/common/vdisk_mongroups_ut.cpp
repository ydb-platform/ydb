#include "vdisk_histograms.h"
#include "vdisk_mongroups.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/split.h>

namespace NKikimr::NMonGroup {
namespace {

    // Exercise the same feature-flag lookup as production actor constructors.
    struct TMonitoringContext {
        TAppData Data{0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr};
        THolder<NActors::TActorSystem> System;
        NActors::TMailbox Mailbox;
        THolder<NActors::TExecutorThread> Thread;
        THolder<NActors::TActorContext> Context;
        NActors::TActivationContext* Previous = NActors::TlsActivationContext;

        explicit TMonitoringContext(bool extended) {
            Data.FeatureFlags.SetExtendedVDiskCounters(extended);
            THolder<NActors::TActorSystemSetup> setup(new NActors::TActorSystemSetup);
            System.Reset(new NActors::TActorSystem(setup, &Data));
            Thread.Reset(new NActors::TExecutorThread(0, System.Get(), nullptr, "monitoring-test"));
            Context.Reset(new NActors::TActorContext(Mailbox, *Thread, GetCycleCountFast(), TActorId()));
            NActors::TlsActivationContext = Context.Get();
        }

        ~TMonitoringContext() {
            NActors::TlsActivationContext = Previous;
        }
    };

    TString Export(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters) {
        TStringStream out;
        auto encoder = NMonitoring::CreateEncoder(&out, NMonitoring::EFormat::PROMETHEUS);
        counters->Accept({}, {}, *encoder);
        return out.Str();
    }

    size_t CountSeries(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters) {
        size_t count = 0;
        const TString exported = Export(counters);
        for (TStringBuf line : StringSplitter(exported).Split('\n').SkipEmpty()) {
            count += !line.StartsWith('#');
        }
        return count;
    }

} // namespace

Y_UNIT_TEST_SUITE(TVDiskMonitoring) {
    Y_UNIT_TEST(ExtendedCountersRemainEnabledByDefault) {
        NKikimrConfig::TFeatureFlags flags;
        UNIT_ASSERT(flags.GetExtendedVDiskCounters());
    }

    Y_UNIT_TEST(SortedLevelAggregatesResetAfterCompaction) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TLsmAllLevelsStat stat(counters);
            std::array<TLsmLevelStats, TLsmAllLevelsStat::MaxCounterLevels> levels;
            levels[0] = {1, 2, 3, 4, 5, 6};
            levels[1] = {10, 20, 30, 40, 50, 60};
            levels[23] = {100, 200, 300, 400, 500, 600};
            stat.Update(levels, {110, 220, 330, 440, 550, 660});

            UNIT_ASSERT_VALUES_EQUAL(stat.Levels[0]->SstNum(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.SstNum(), 110);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItems(), 220);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItemsInplaced(), 330);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItemsHuge(), 440);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.DataInplaced(), 550);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.DataHuge(), 660);
            UNIT_ASSERT(!stat.SortedLevels.GetGroup()->FindCounter("NumItems"));
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.GetGroup()->FindCounter("SortedNumItems")->Val(), 220);
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 150 : 12);

            levels[1] = {};
            levels[23] = {};
            stat.Update(levels, {});
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.SstNum(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.DataHuge(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stat.Levels[23]->NumItems(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stat.Levels[0]->DataInplaced(), 5);
        }
    }

    Y_UNIT_TEST(StatusTotalsMatchHandleClassBreakdown) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TResponseStatusGroup mon(counters);
            for (auto status : {NKikimrProto::ERROR, NKikimrProto::RACE, NKikimrProto::BLOCKED,
                    NKikimrProto::OUT_OF_SPACE, NKikimrProto::DEADLINE, NKikimrProto::NOTREADY,
                    NKikimrProto::VDISK_ERROR_STATE, NKikimrProto::OK}) {
                mon.Count(status, NKikimrBlobStorage::FastRead);
                mon.Count(status, NKikimrBlobStorage::TabletLog);
                mon.Count(status);
                auto& group = mon.GetStatusGroup(status);
                UNIT_ASSERT_VALUES_EQUAL(group.TotalResponses(), 3);
                UNIT_ASSERT_VALUES_EQUAL(group.GetFast(), 1);
                UNIT_ASSERT_VALUES_EQUAL(group.PutTabletLog(), 1);
                UNIT_ASSERT_VALUES_EQUAL(group.Undefined(), 1);
                // Looking up a detailed counter must not count another response.
                UNIT_ASSERT_VALUES_EQUAL(mon.GetCounter(status).Val(), 1);
                UNIT_ASSERT_VALUES_EQUAL(group.TotalResponses(), 3);
            }
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 72 : 8);
        }
    }

    Y_UNIT_TEST(DeepScrubbingRegistersOnlyUsedErasureOnFirstCheck) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            for (auto erasure : {TErasureType::ErasureNone, TErasureType::Erasure4Plus2Block,
                    TErasureType::ErasureMirror3of4, TErasureType::ErasureMirror3dc}) {
                auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                TDeepScrubbingSubgroups mon(counters, erasure);
                UNIT_ASSERT(!counters->FindSubgroup("subsystem", "deepScrubbing"));
                UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), 0);
                auto& small = mon.GetCounters(false);
                small.BlobsChecked() += 2;
                ++small.CheckIntegritySuccesses();
                ++small.CheckIntegrityErrors();
                auto& huge = mon.GetCounters(true);
                ++huge.BlobsChecked();
                ++huge.CheckIntegritySuccesses();
                UNIT_ASSERT(&mon.GetCounters(false) == &small);
                UNIT_ASSERT_VALUES_EQUAL(small.BlobsChecked(),
                    small.CheckIntegritySuccesses() + small.CheckIntegrityErrors());
                UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 14 : 12);

                auto group = counters->FindSubgroup("subsystem", "deepScrubbing");
                for (const auto* blobSize : {"huge", "small"}) {
                    auto sizes = group->FindSubgroup("blobSize", blobSize);
                    size_t erasures = 0;
                    sizes->EnumerateSubgroups([&](const TString& label, const TString& value) {
                        UNIT_ASSERT_VALUES_EQUAL(label, "erasure");
                        UNIT_ASSERT_VALUES_EQUAL(value, TErasureType::ErasureSpeciesName(erasure));
                        ++erasures;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(erasures, 1);
                }
            }
        }
    }

    Y_UNIT_TEST(HugeBlobThresholdHasBoundedRegularExport) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TVDiskIFaceGroup mon(counters);
            for (ui32 size : {65'536, 131'072, 65'536}) {
                mon.MinHugeBlobInBytes(size);
                UNIT_ASSERT_VALUES_EQUAL(counters->FindCounter("MinHugeBlobInBytes")->Val(), size);
            }
            UNIT_ASSERT(!counters->FindCounter("MinHugeBlobInBytes")->ForDerivative());
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 24 : 1);
            auto old = counters->FindSubgroup("MinHugeBlobInBytes", "131072");
            UNIT_ASSERT_VALUES_EQUAL(old->FindCounter("count")->Val(), 0);
        }
    }

    Y_UNIT_TEST(LegacyNumericStateDoesNotAffectRegularStateIndicators) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TVDiskStateGroup mon(counters);
            mon.VDiskState(NKikimrWhiteboard::OK);
            mon.VDiskState(NKikimrWhiteboard::PDiskError);
            UNIT_ASSERT(mon.VDiskState() == NKikimrWhiteboard::PDiskError);
            UNIT_ASSERT_VALUES_EQUAL(counters->FindCounter("VDiskState_OK")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->FindCounter("VDiskState_PDiskError")->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 10 : 8);
        }
    }

    Y_UNIT_TEST(LatencyHistogramCountReplacesRegularCompletedCounter) {
        for (bool extended : {false, true}) {
            TMonitoringContext context(extended);
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto asyncCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            NVDiskMon::THistograms mon(counters, asyncCounters, NPDisk::DEVICE_TYPE_ROT);
            auto histogram = mon.GetHistogram(NKikimrBlobStorage::FastRead);
            histogram->Collect(TDuration::MilliSeconds(1), 123);
            histogram->Collect(TDuration::Seconds(120), 456);
            auto latency = counters->FindSubgroup("handleclass", "GetFast")
                ->FindSubgroup("subsystem", "latency_histo");
            auto snapshot = latency->FindHistogram("LatencyMs")->Snapshot();
            ui64 count = 0;
            for (ui32 i = 0; i < snapshot->Count(); ++i) {
                count += snapshot->Value(i);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, 2);
            UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("LatencyCompletedCount")->Val(), count);
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), extended ? 54 : 51);
            UNIT_ASSERT_VALUES_EQUAL(CountSeries(asyncCounters), extended ? 56 : 52);
        }
    }

    Y_UNIT_TEST(EssentialHealthAndCostCountersRemainInRegularExport) {
        TMonitoringContext context(false);
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TDskOutOfSpaceGroup space(counters, "subsystem", "outofspace");
        TReplGroup repl(counters, "subsystem", "repl");
        TSyncerGroup syncer(counters, "subsystem", "syncer");
        TSkeletonOverloadGroup overload(counters, "subsystem", "emergency");
        TCostTrackerGroup cost(counters, "subsystem", "advancedCost");
        const TString exported = Export(counters);
        for (const auto* name : {"HugeCanBeFreedChunks", "ReplUnreplicatedVDisks", "ReplMadeNoProgress",
                "ReplTotalBlobsWithProblems", "SyncerUnsyncedDisks", "FreshSatisfactionRankPercent",
                "LevelSatisfactionRankPercent", "ThrottlingIsActive", "ThrottlingCurrentSpeedLimit",
                "DiskTimeAvailableCtr", "DiskTimeFairShareNs", "UserDiskCost", "CompactionDiskCost",
                "ScrubDiskCost", "DefragDiskCost", "InternalDiskCost"}) {
            UNIT_ASSERT_C(exported.Contains(TString(name) + "{"), name);
        }
        UNIT_ASSERT_VALUES_EQUAL(CountSeries(counters), 46);
    }
}

} // namespace NKikimr::NMonGroup
