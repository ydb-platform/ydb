#pragma once

#include "defs.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/config/metrics.pb.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

namespace NKikimr {

static inline NMonitoring::TBucketBounds GetCommonLatencyHistBounds(NPDisk::EDeviceType type, TActorSystem* actorSystem = nullptr) {
    NMonitoring::TBucketBounds bounds = {
        8, 16, 32, 64, 128, 256, 512,   // ms
        1'024, 4'096,                                       // s
        65'536                                                  // minutes
    };

    const NKikimrConfig::TCommonLatencyHistBounds* commonLatencyHistBounds = nullptr;
    if (!actorSystem) {
        if (NActors::TlsActivationContext) {
            actorSystem = NActors::TActivationContext::ActorSystem();
        }
    }
    if (actorSystem) {
        auto appData = AppData(actorSystem);
        if (appData->MetricsConfig.HasCommonLatencyHistBounds()) {
            commonLatencyHistBounds = &appData->MetricsConfig.GetCommonLatencyHistBounds();
        }
    }

    switch (type) {
        case NPDisk::DEVICE_TYPE_UNKNOWN:
            // Use default
            break;
        case NPDisk::DEVICE_TYPE_ROT:
            if (commonLatencyHistBounds && commonLatencyHistBounds->RotSize() > 0) {
                bounds = {};
                for (size_t i = 0; i < commonLatencyHistBounds->RotSize(); ++i) {
                    bounds.push_back(commonLatencyHistBounds->GetRot(i));
                }
            }
            // Use default otherwise
            break;
        case NPDisk::DEVICE_TYPE_SSD:
            if (commonLatencyHistBounds && commonLatencyHistBounds->SsdSize() > 0) {
                bounds = {};
                for (size_t i = 0; i < commonLatencyHistBounds->SsdSize(); ++i) {
                    bounds.push_back(commonLatencyHistBounds->GetSsd(i));
                }
            } else {
                bounds = {
                    0.5,                                        // us
                    1, 2, 8, 32, 128, 512,  // ms
                    1'024, 4'096,                           // s
                    65'536                                      // minutes
                };
            }
            break;
        case NPDisk::DEVICE_TYPE_NVME:
            if (commonLatencyHistBounds && commonLatencyHistBounds->NvmeSize() > 0) {
                bounds = {};
                for (size_t i = 0; i < commonLatencyHistBounds->NvmeSize(); ++i) {
                    bounds.push_back(commonLatencyHistBounds->GetNvme(i));
                }
            } else {
                bounds = {
                    0.25, 0.5,                              // us
                    1, 2, 4, 8, 32, 128,    // ms
                    1'024,                                      // s
                    65'536                                      // minutes
                };
            }
            break;
    }
    return bounds;
}

} // NKikimr
