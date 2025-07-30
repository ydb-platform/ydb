#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

namespace NKikimr {

static inline NMonitoring::TBucketBounds GetCommonLatencyHistBounds(NPDisk::EDeviceType type) {
    NMonitoring::TBucketBounds bounds = {
        8, 16, 32, 64, 128, 256, 512,       // ms
        1'024, 4'096,                       // s
        65'536                              // minutes
    };
    switch (type) {
        case NPDisk::DEVICE_TYPE_UNKNOWN:
            // Use default
            break;
        case NPDisk::DEVICE_TYPE_ROT:
            // Use default
            break;
        case NPDisk::DEVICE_TYPE_SSD:
            bounds = {
                0.5,                                // us
                1, 2, 8, 32, 128, 512,              // ms
                1'024, 4'096,                       // s
                65'536                              // minutes
            };
            break;
        case NPDisk::DEVICE_TYPE_NVME:
            bounds = {
                0.125, 0.25, 0.5,                   // us
                1, 2, 8, 32, 128,                   // ms
                1'000, 10'000                       // s
            };
            break;
    }
    return bounds;
}

} // NKikimr
