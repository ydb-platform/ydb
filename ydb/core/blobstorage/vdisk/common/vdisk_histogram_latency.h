#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/util/max_tracker.h>

#include <library/cpp/monlib/dynamic_counters/percentile/percentile.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <util/generic/hash.h>

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
                      const TString& name,
                      const TString& value,
                      NPDisk::EDeviceType type);

            // update histogram with with an operation with duration 'd'
            void Collect(TDuration d, ui64 size = 0);
            void AddInFlightRequest(ui64 requestId, TInstant receivedTime);
            void RemoveInFlightRequest(ui64 requestId);
            void UpdateCounters(TInstant now);

        private:
            NMonitoring::THistogramPtr Histo;
            ::NMonitoring::TDynamicCounters::TCounterPtr ThroughputBytes;
            TMaxTracker LatencyMsMax;
            ::NMonitoring::TDynamicCounters::TCounterPtr LatencyMsCompletedSum;
            ::NMonitoring::TDynamicCounters::TCounterPtr LatencyCompletedCount;
            ::NMonitoring::TDynamicCounters::TCounterPtr InFlightLatencyMsSum;
            ::NMonitoring::TDynamicCounters::TCounterPtr InFlightCount;
            THashMap<ui64, TInstant> InFlightRequests;
        };

        using TLtcHistoPtr = std::shared_ptr<TLtcHisto>;

        // Owns one in-flight latency record. Construction/destruction updates the tracked
        // request set immediately; visible monitoring counters are snapshot-published by
        // TLtcHisto::UpdateCounters().
        class TInFlightLatencyGuard {
        public:
            TInFlightLatencyGuard() = default;
            TInFlightLatencyGuard(TLtcHistoPtr histogram, ui64 requestId, TInstant receivedTime);
            ~TInFlightLatencyGuard();

            TInFlightLatencyGuard(const TInFlightLatencyGuard&) = delete;
            TInFlightLatencyGuard& operator=(const TInFlightLatencyGuard&) = delete;

            TInFlightLatencyGuard(TInFlightLatencyGuard&& other) noexcept;
            TInFlightLatencyGuard& operator=(TInFlightLatencyGuard&& other) noexcept;

            ui64 GetRequestId() const {
                return RequestId;
            }

            void Reset();

        private:
            TLtcHistoPtr Histogram;
            ui64 RequestId = 0;
        };

    } // namespace NVDiskMon
} // namespace NKikimr
