#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>

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

    class TLoadActorException : public yexception
    {};

    NActors::IActor *CreateLoadTestActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    NActors::IActor *CreateWriterLoadTest(const NKikimr::TEvLoadTestRequest::TLoadStart& cmd,
            const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

    NActors::IActor *CreatePDiskWriterLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreatePDiskLogWriterLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskLogLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreatePDiskReaderLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskReadLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateVDiskWriterLoadTest(const NKikimr::TEvLoadTestRequest::TVDiskLoadStart& cmd,
            const NActors::TActorId& parent, ui64 tag);

    NActors::IActor *CreateKeyValueWriterLoadTest(const NKikimr::TEvLoadTestRequest::TKeyValueLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateKqpLoadActor(const NKikimr::TEvLoadTestRequest::TKqpLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

    NActors::IActor *CreateMemoryLoadTest(const NKikimr::TEvLoadTestRequest::TMemoryLoadStart& cmd,
            const NActors::TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            ui64 index, ui64 tag);

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadActorException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

#define VERIFY_PARAM(NAME) VERIFY_PARAM2(cmd, NAME)

} // NKikimr
