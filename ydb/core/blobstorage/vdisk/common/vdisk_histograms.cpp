#include "vdisk_histograms.h"

namespace NKikimr {
    namespace NVDiskMon {

        THistograms::THistograms(
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& asyncCounters,
                NPDisk::EDeviceType type)
        {
            for (const auto& item : {
                    std::make_pair(&VGetFastLatencyHistogram,      "GetFast"     ),
                    std::make_pair(&VPutTabletLogLatencyHistogram, "PutTabletLog"),
                    std::make_pair(&VPutUserDataLatencyHistogram,  "PutUserData" ),
                    std::make_pair(&VGetAsyncLatencyHistogram,     "GetAsync"    ),
                    std::make_pair(&VGetDiscoverLatencyHistogram,  "GetDiscover" ),
                    std::make_pair(&VGetLowLatencyHistogram,       "GetLow"      ),
                    std::make_pair(&VPutAsyncBlobLatencyHistogram, "PutAsyncBlob")
                    }) {
                if (IsAsyncHandleClass(item.second)) {
                    *item.first = std::make_shared<TLtcHisto>(asyncCounters, "handleclass", item.second,
                        GetAsyncLatencyHistBounds());
                } else {
                    *item.first = std::make_shared<TLtcHisto>(counters, "handleclass", item.second, type);
                }
            }
        }

        bool THistograms::IsAsyncHandleClass(TStringBuf handleClass) {
            return handleClass == "GetAsync" || handleClass == "GetDiscover"
                || handleClass == "GetLow" || handleClass == "PutAsyncBlob";
        }

        NMonitoring::TBucketBounds THistograms::GetAsyncLatencyHistBounds() {
            // Background bounds are fixed independently of the media's foreground configuration.
            return {1, 8, 32, 128, 1'024, 65'536}; // ms
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

        void THistograms::UpdateCounters(TInstant now) {
            for (const auto& histogram : {
                     VGetAsyncLatencyHistogram,
                     VGetFastLatencyHistogram,
                     VGetDiscoverLatencyHistogram,
                     VGetLowLatencyHistogram,
                     VPutTabletLogLatencyHistogram,
                     VPutUserDataLatencyHistogram,
                     VPutAsyncBlobLatencyHistogram}) {
                histogram->UpdateCounters(now);
            }
        }

    } // NVDiskMon
} // NKikimr
