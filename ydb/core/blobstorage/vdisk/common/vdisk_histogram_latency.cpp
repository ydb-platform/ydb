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
            LatencyUsMax.Init(histoGroup->GetCounter("LatencyUsMax", false));
            LatencyUsCompletedSum = histoGroup->GetCounter("LatencyUsCompletedSum", true);
            LatencyCompletedCount = histoGroup->GetCounter("LatencyCompletedCount", true);
            InFlightLatencyUsSum = histoGroup->GetCounter("InFlightLatencyUsSum", false);
            InFlightCount = histoGroup->GetCounter("InFlightCount", false);
        }

        void TLtcHisto::Collect(TDuration d, ui64 size) {
            const ui64 durationUs = d.MicroSeconds();
            if (Histo) {
                Histo->Collect(d.MillisecondsFloat());
            }
            LatencyUsMax.Collect(durationUs);
            LatencyUsCompletedSum->Add(durationUs);
            LatencyCompletedCount->Inc();
            if (size) {
                ThroughputBytes->Add(size);
            }
        }

        void TLtcHisto::AddInFlightRequest(ui64 requestId, TInstant receivedTime) {
            InFlightRequests.emplace(requestId, receivedTime);
        }

        void TLtcHisto::RemoveInFlightRequest(ui64 requestId) {
            InFlightRequests.erase(requestId);
        }

        void TLtcHisto::UpdateCounters(TInstant now) {
            ui64 latencyUsSum = 0;
            ui64 latencyUsMax = 0;
            for (const auto& [requestId, receivedTime] : InFlightRequests) {
                Y_UNUSED(requestId);
                const ui64 latencyUs = now > receivedTime ? (now - receivedTime).MicroSeconds() : 0;
                latencyUsSum += latencyUs;
                latencyUsMax = Max(latencyUsMax, latencyUs);
            }

            InFlightLatencyUsSum->Set(latencyUsSum);
            InFlightCount->Set(InFlightRequests.size());
            LatencyUsMax.Collect(latencyUsMax);
            LatencyUsMax.Update();
        }

        TInFlightLatencyGuard::TInFlightLatencyGuard(TLtcHistoPtr histogram, ui64 requestId, TInstant receivedTime)
            : Histogram(std::move(histogram))
            , RequestId(requestId)
        {
            if (Histogram) {
                Histogram->AddInFlightRequest(RequestId, receivedTime);
            }
        }

        TInFlightLatencyGuard::~TInFlightLatencyGuard() {
            Reset();
        }

        TInFlightLatencyGuard::TInFlightLatencyGuard(TInFlightLatencyGuard&& other) noexcept
            : Histogram(std::move(other.Histogram))
            , RequestId(other.RequestId)
        {
            other.RequestId = 0;
        }

        TInFlightLatencyGuard& TInFlightLatencyGuard::operator=(TInFlightLatencyGuard&& other) noexcept {
            if (this != &other) {
                Reset();
                Histogram = std::move(other.Histogram);
                RequestId = other.RequestId;
                other.RequestId = 0;
            }
            return *this;
        }

        void TInFlightLatencyGuard::Reset() {
            if (Histogram) {
                Histogram->RemoveInFlightRequest(RequestId);
                Histogram.reset();
            }
            RequestId = 0;
        }

    } // NKikimr
} // NKikimr
