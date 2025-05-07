#include "vdisk_histogram_latency.h"

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

namespace NKikimr {
    namespace NVDiskMon {

        TLtcHisto::TLtcHisto(
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                const TString &name,
                const TString &value,
                NPDisk::EDeviceType type)
        {
            auto group = counters->GetSubgroup(name, value);
            ThroughputBytes = group->GetCounter("requestBytes", true);

            // Set up Histo
            TIntrusivePtr<::NMonitoring::TDynamicCounters> histoGroup;
            histoGroup = group->GetSubgroup("subsystem", "latency_histo");

            auto h = NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(type));
            Histo = histoGroup->GetHistogram("LatencyMs", std::move(h));
        }

        void TLtcHisto::Collect(TDuration d, ui64 size) {
            if (Histo) {
                Histo->Collect(d.MillisecondsFloat());
            }
            if (size) {
                ThroughputBytes->Add(size);
            }
        }

    } // NKikimr
} // NKikimr
