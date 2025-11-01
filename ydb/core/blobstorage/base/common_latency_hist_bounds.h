#pragma once

#include "defs.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/config_metrics.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

namespace NKikimr {

static inline NMonitoring::TBucketBounds GetCommonLatencyHistBounds(NPDisk::EDeviceType type, TActorSystem* actorSystem = nullptr) {
    static TMetricsConfig defaultMetricsConfig; // this is for tests only

    TMetricsConfig* metricsConfig;
    if (!actorSystem) {
        if (NActors::TlsActivationContext) {
            actorSystem = NActors::TActivationContext::ActorSystem();
        }
    }

    if (actorSystem) {
        auto appData = AppData(actorSystem);
        metricsConfig = &appData->MetricsConfig;
    } else {
        metricsConfig = &defaultMetricsConfig;
    }

    switch (type) {
        case NPDisk::DEVICE_TYPE_UNKNOWN:
            return metricsConfig->GetCommonLatencyHistBounds().Unknown;
        case NPDisk::DEVICE_TYPE_ROT:
            return metricsConfig->GetCommonLatencyHistBounds().Rot;
        case NPDisk::DEVICE_TYPE_SSD:
            return metricsConfig->GetCommonLatencyHistBounds().Ssd;
        case NPDisk::DEVICE_TYPE_NVME:
            return metricsConfig->GetCommonLatencyHistBounds().Nvme;
        default:
            return metricsConfig->GetCommonLatencyHistBounds().Unknown;
    }
}

} // NKikimr
