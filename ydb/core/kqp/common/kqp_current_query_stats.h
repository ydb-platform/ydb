#pragma once

#include <util/system/mutex.h>
#include <util/system/types.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

// Latest task reports for one physical execution.
// CPU includes compute and reported storage CPU; memory is compute quota without channel quota.
// Table/source bytes may overlap and do not distinguish local storage from S3.
struct TCurrentExecStats {
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
    ui64 ComputeMemoryBytes = 0;
    ui64 TableReadBytes = 0;
    ui64 SourceReadBytes = 0;
};

// One instance per request, shared by the local session and its executers.
// Duration is measured from request start, not summed across executions.
class TCurrentQueryStats {
public:
    struct TSnapshot : TCurrentExecStats {
        // Sampled maximum; allocations between reports can be missed.
        ui64 ObservedPeakComputeMemoryBytes = 0;
    };

    void Update(TCurrentExecStats current, TCurrentExecStats& previous) {
        current.CpuTimeUs = std::max(current.CpuTimeUs, previous.CpuTimeUs);
        current.TableReadBytes = std::max(current.TableReadBytes, previous.TableReadBytes);
        current.SourceReadBytes = std::max(current.SourceReadBytes, previous.SourceReadBytes);
        TGuard<TMutex> guard(Lock);
        Total.CpuTimeUs += current.CpuTimeUs - previous.CpuTimeUs;
        Total.TableReadBytes += current.TableReadBytes - previous.TableReadBytes;
        Total.SourceReadBytes += current.SourceReadBytes - previous.SourceReadBytes;
        Total.ComputeMemoryBytes -= previous.ComputeMemoryBytes;
        Total.ComputeMemoryBytes += current.ComputeMemoryBytes;
        Total.ObservedPeakComputeMemoryBytes = std::max(Total.ObservedPeakComputeMemoryBytes, Total.ComputeMemoryBytes);
        previous = current;
        HasReports = true;
    }

    std::optional<TSnapshot> Get() const {
        TGuard<TMutex> guard(Lock);
        return HasReports ? std::make_optional(Total) : std::nullopt;
    }

private:
    mutable TMutex Lock;
    TSnapshot Total;
    bool HasReports = false;
};

} // namespace NKikimr::NKqp
