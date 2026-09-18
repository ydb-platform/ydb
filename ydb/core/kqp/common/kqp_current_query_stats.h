#pragma once

#include <util/system/types.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

// CPU includes compute and reported storage CPU; memory is compute quota without channel quota.
// Table/source bytes may overlap and do not distinguish local storage from S3.
struct TCurrentExecStats {
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
    ui64 ComputeMemoryBytes = 0;
    ui64 TableReadBytes = 0;
    ui64 ReadIngressBytes = 0;
    ui64 ObservedPeakComputeMemoryBytes = 0;
};

struct TCurrentExecStatsReport {
    TCurrentExecStats Stats;
    ui64 SequenceNo = 0;
};

class TCurrentQueryStats {
public:
    using TSnapshot = TCurrentExecStats;

    void Update(TCurrentExecStats current, TCurrentExecStats& previous) {
        current.CpuTimeUs = std::max(current.CpuTimeUs, previous.CpuTimeUs);
        current.TableReadBytes = std::max(current.TableReadBytes, previous.TableReadBytes);
        current.ReadIngressBytes = std::max(current.ReadIngressBytes, previous.ReadIngressBytes);
        Total.CpuTimeUs += current.CpuTimeUs - previous.CpuTimeUs;
        Total.TableReadBytes += current.TableReadBytes - previous.TableReadBytes;
        Total.ReadIngressBytes += current.ReadIngressBytes - previous.ReadIngressBytes;
        Total.ComputeMemoryBytes -= previous.ComputeMemoryBytes;
        Total.ComputeMemoryBytes += current.ComputeMemoryBytes;
        Total.ObservedPeakComputeMemoryBytes = std::max({Total.ObservedPeakComputeMemoryBytes,
            Total.ComputeMemoryBytes, current.ObservedPeakComputeMemoryBytes});
        previous = current;
        HasReports = true;
    }

    std::optional<TSnapshot> Get() const {
        return HasReports ? std::make_optional(Total) : std::nullopt;
    }

private:
    TSnapshot Total;
    bool HasReports = false;
};

} // namespace NKikimr::NKqp
