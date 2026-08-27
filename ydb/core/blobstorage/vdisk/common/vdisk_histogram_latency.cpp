#include "vdisk_histogram_latency.h"

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

namespace NKikimr {
    namespace NVDiskMon {

        TLtcHisto::TLtcHisto(
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            const TString& name,
            const TString& value,
            NPDisk::EDeviceType type)
        {
            auto group = counters->GetSubgroup(name, value);
            ThroughputBytes = group->GetCounter("requestBytes", true);

            // Set up Histo
            TIntrusivePtr<::NMonitoring::TDynamicCounters> histoGroup;
            histoGroup = group->GetSubgroup("subsystem", "latency_histo");

            auto h = NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(type));
            Histo = histoGroup->GetHistogram("LatencyMs", std::move(h));
            LatencyMsMax.Init(histoGroup->GetCounter("LatencyMsMax", false));
            LatencyMsCompletedSum = histoGroup->GetCounter("LatencyMsCompletedSum", true);
            LatencyCompletedCount = histoGroup->GetCounter("LatencyCompletedCount", true);
            InFlightLatencyMsSum = histoGroup->GetCounter("InFlightLatencyMsSum", false);
            InFlightCount = histoGroup->GetCounter("InFlightCount", false);
        }

        void TLtcHisto::Collect(TDuration d, ui64 size) {
            const auto durationMs = d.MillisecondsFloat();
            if (Histo) {
                Histo->Collect(d.MillisecondsFloat());
            }
            LatencyMsMax.Collect(durationMs);
            LatencyMsCompletedSum->Add(durationMs);
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
            ui64 latencyMsSum = 0;
            ui64 latencyMsMax = 0;
            for (const auto& [requestId, receivedTime] : InFlightRequests) {
                Y_UNUSED(requestId);
                const ui64 latencyMs = now > receivedTime ? (now - receivedTime).MilliSeconds() : 0;
                latencyMsSum += latencyMs;
                latencyMsMax = Max(latencyMsMax, latencyMs);
            }

            InFlightLatencyMsSum->Set(latencyMsSum);
            InFlightCount->Set(InFlightRequests.size());
            LatencyMsMax.Collect(latencyMsMax);
            LatencyMsMax.Update();
        }

    } // namespace NVDiskMon
} // namespace NKikimr
