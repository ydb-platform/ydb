#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>

#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>
#include <library/cpp/json/writer/json_value.h>

#include <cmath>

namespace NKikimr {
    enum {
        EvStopTest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvUpdateQuantile,
        EvUpdateMonitoring,
    };

    struct TEvStopTest : TEventLocal<TEvStopTest, EvStopTest>
    {};
    constexpr TDuration DelayBeforeMeasurements = TDuration::Seconds(15);

    struct TEvUpdateQuantile : TEventLocal<TEvUpdateQuantile, EvUpdateQuantile>
    {};

    constexpr ui64 MonitoringUpdateCycleMs = 1000;

    struct TEvUpdateMonitoring : TEventLocal<TEvUpdateMonitoring, EvUpdateMonitoring>
    {};


    class TLoadActorException : public yexception {
    };

    NActors::IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    NActors::IActor *CreateWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TLoadStart& cmd,
            const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

    NActors::IActor *CreatePDiskWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TPDiskLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreatePDiskLogWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TPDiskLogLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreatePDiskReaderTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TPDiskReadLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateVDiskWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TVDiskLoadStart& cmd,
            const NActors::TActorId& parent, ui64 tag);

    NActors::IActor *CreateKeyValueWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TKeyValueLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateKqpLoadActor(const NKikimrBlobStorage::TEvTestLoadRequest::TKqpLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateMemoryTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TMemoryLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    struct TLoadReport : public TThrRefBase {
        enum ELoadType {
            LOAD_READ,
            LOAD_WRITE,
            LOAD_LOG_WRITE,
        };

        TDuration Duration;
        ui64 Size;
        ui32 InFlight;
        TVector<ui64> RwSpeedBps;
        ELoadType LoadType;
        NMonitoring::TPercentileTrackerLg<10, 4, 1> LatencyUs; // Upper threshold of this tracker is ~134 seconds, size is 256kB
        TMap<double, ui64> DeviceLatency;

        double GetAverageSpeed() const {
            if (RwSpeedBps.size() < 1) {
                return 0;
            }
            double avg = 0;
            for (const ui64& speed : RwSpeedBps) {
                avg += speed;
            }
            avg /= RwSpeedBps.size();
            return avg;
        }

        double GetSpeedDeviation() const {
            if (RwSpeedBps.size() <= 1) {
                return 0;
            }
            i64 avg = (i64)GetAverageSpeed();
            double sd = 0;
            for (const ui64& speed : RwSpeedBps) {
                sd += ((i64)speed - avg) * ((i64)speed - avg);
            }
            sd /= RwSpeedBps.size();
            return std::sqrt(sd);
        }

        TString LoadTypeName() const {
            switch (LoadType) {
            case LOAD_READ:
                return "read";
            case LOAD_WRITE:
                return "write";
            case LOAD_LOG_WRITE:
                return "log_write";
            }
        }
    };

    struct TEvTestLoadFinished : public TEventLocal<TEvTestLoadFinished, TEvBlobStorage::EvTestLoadFinished> {
        ui64 Tag;
        TIntrusivePtr<TLoadReport> Report; // nullptr indicates error
        TString ErrorReason;
        TString LastHtmlPage;
        NJson::TJsonValue JsonResult;

        TEvTestLoadFinished(ui64 tag, TIntrusivePtr<TLoadReport> report, TString errorReason)
            : Tag(tag)
            , Report(report)
            , ErrorReason(errorReason)
        {}
    };

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadActorException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

#define VERIFY_PARAM(NAME) VERIFY_PARAM2(cmd, NAME)

} // NKikimr
