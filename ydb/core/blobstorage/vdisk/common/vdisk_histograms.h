#pragma once

#include "defs.h"
#include "vdisk_histogram_latency.h"
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {
    namespace NVDiskMon {

        /////////////////////////////////////////////////////////////////////////////////////////
        // THistograms -- all VDisk histograms in one place
        /////////////////////////////////////////////////////////////////////////////////////////
        class THistograms {
        public:
            THistograms(
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                NPDisk::EDeviceType type);
            const NVDiskMon::TLtcHistoPtr &GetHistogram(NKikimrBlobStorage::EGetHandleClass handleClass) const;
            const NVDiskMon::TLtcHistoPtr &GetHistogram(NKikimrBlobStorage::EPutHandleClass handleClass) const;

            NVDiskMon::TLtcHistoPtr VGetDiscoverLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VGetFastLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VGetAsyncLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VGetLowLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VPutTabletLogLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VPutUserDataLatencyHistogram;
            NVDiskMon::TLtcHistoPtr VPutAsyncBlobLatencyHistogram;
        };

    } // NVDiskMon
} // NKikimr

