#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <mutex>

namespace NKvVolumeStress {

struct TRunStatsSnapshot {
    THashMap<TString, ui64> ActionRuns;
    THashMap<TString, ui64> ErrorsByKind;
    TVector<TString> SampleErrors;
    ui64 TotalErrors = 0;
};

class TRunStats {
public:
    void RecordAction(const TString& actionName);
    void RecordError(const TString& kind, const TString& message);
    ui64 GetTotalErrors() const;
    TRunStatsSnapshot Snapshot() const;
    void PrintSummary(double elapsedSeconds) const;

private:
    mutable std::mutex Mutex_;
    THashMap<TString, ui64> ActionRuns_;
    THashMap<TString, ui64> ErrorsByKind_;
    TVector<TString> SampleErrors_;
    ui64 TotalErrors_ = 0;
};

} // namespace NKvVolumeStress
