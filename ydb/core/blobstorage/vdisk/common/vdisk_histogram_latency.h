#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

#include <library/cpp/monlib/dynamic_counters/percentile/percentile.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NKikimr {
    namespace NVDiskMon {
        /////////////////////////////////////////////////////////////////////////////////////////
        // TLtcHisto - Latency Histogram
        // This class abstracts interface for Latency Histograms from its implementation.
        // NOTE:
        // 1. REMOVED. Historically we have TPercentileHistogram for calculation percentiles locally,
        //    unfortunately percentiles are not additive and we can't build aggregates
        //    for the whole cluster using Solomon.
        // 2. So IHistogramCollectorPtr is added to have additive histograms (i.e. buckets based)
        /////////////////////////////////////////////////////////////////////////////////////////
        class TLtcHisto {
        public:
            TLtcHisto(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                    const TString &name,
                    const TString &value,
                    NPDisk::EDeviceType type);

            // update histogram with with an operation with duration 'd'
            void Collect(TDuration d, ui64 size = 0);

        private:
            NMonitoring::THistogramPtr Histo;
            ::NMonitoring::TDynamicCounters::TCounterPtr ThroughputBytes;
        };

        using TLtcHistoPtr = std::shared_ptr<TLtcHisto>;

    } // NVDiskMon
} // NKikimr

