#include "vdisk_histograms.h"

namespace NKikimr {
    namespace NVDiskMon {

        THistograms::THistograms(
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                NPDisk::EDeviceType type)
        {
            for (const auto& item : {
                    std::make_pair(&VGetAsyncLatencyHistogram,     "GetAsync"    ),
                    std::make_pair(&VGetFastLatencyHistogram,      "GetFast"     ),
                    std::make_pair(&VGetDiscoverLatencyHistogram,  "GetDiscover" ),
                    std::make_pair(&VGetLowLatencyHistogram,       "GetLow" ),
                    std::make_pair(&VPutTabletLogLatencyHistogram, "PutTabletLog"),
                    std::make_pair(&VPutUserDataLatencyHistogram,  "PutUserData" ),
                    std::make_pair(&VPutAsyncBlobLatencyHistogram, "PutAsyncBlob")
                    }) {
                *item.first = std::make_shared<NVDiskMon::TLtcHisto>(counters, "handleclass", item.second, type);
            }
        }

        const NVDiskMon::TLtcHistoPtr &THistograms::GetHistogram(NKikimrBlobStorage::EGetHandleClass handleClass) const {
            switch (handleClass) {
                case NKikimrBlobStorage::AsyncRead:
                    return VGetAsyncLatencyHistogram;
                case NKikimrBlobStorage::FastRead:
                    return VGetFastLatencyHistogram;
                case NKikimrBlobStorage::Discover:
                    return VGetDiscoverLatencyHistogram;
                case NKikimrBlobStorage::LowRead:
                    return VGetLowLatencyHistogram;
            }
        }

        const NVDiskMon::TLtcHistoPtr &THistograms::GetHistogram(NKikimrBlobStorage::EPutHandleClass handleClass) const {
            switch (handleClass) {
                case NKikimrBlobStorage::TabletLog:
                    return VPutTabletLogLatencyHistogram;
                case NKikimrBlobStorage::AsyncBlob:
                    return VPutAsyncBlobLatencyHistogram;
                case NKikimrBlobStorage::UserData:
                    return VPutUserDataLatencyHistogram;
            }
        }

    } // NVDiskMon
} // NKikimr
