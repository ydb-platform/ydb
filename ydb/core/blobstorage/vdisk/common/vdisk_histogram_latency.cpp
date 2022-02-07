#include "vdisk_histogram_latency.h"

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

namespace NKikimr {
    namespace NVDiskMon {

        TLtcHisto::TLtcHisto(
                const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                const TString &name,
                const TString &value,
                TPDiskCategory::EDeviceType type)
        {
            auto throughput = counters->GetSubgroup(name, value);
            ThroughputBytes = throughput->GetCounter("requestBytes", true);

            // Set up Histo
            TIntrusivePtr<NMonitoring::TDynamicCounters> histoGroup;
            histoGroup = counters->GetSubgroup("subsystem", "latency_histo");

            auto h = NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(type));
            Histo = histoGroup->GetNamedHistogram(name, value, std::move(h));
        }

        void TLtcHisto::Collect(TDuration d, ui64 size) {
            if (Histo) {
                Histo->Collect(d.MilliSeconds());
            }
            if (size) {
                ThroughputBytes->Add(size);
            }
        }

    } // NKikimr
} // NKikimr

