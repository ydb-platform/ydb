#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/histogram/hdr/histogram.h>

#include <unordered_map>


namespace NYdbWorkload {
namespace NTPCC {

struct TStatistics {
    NHdr::THistogram Hist;

    ui64 Retries = 0;
    ui64 Failes = 0;
    ui64 Successes = 0;

    std::unordered_map<NYdb::EStatus, ui32> RetriesWithStatus;
    std::unordered_map<NYdb::EStatus, ui32> FailesWithStatus;

    TStatistics(i64 highestTrackableValue, i32 numberOfSignificantValueDigits)
        : Hist(highestTrackableValue, numberOfSignificantValueDigits)
    {
    }

    void Add(const std::unique_ptr<TStatistics>& other) {
        Retries += other->Retries;
        Failes += other->Failes;
        Successes += other->Successes;

        Hist.Add(other->Hist);

        for (auto [status, retries]: other->RetriesWithStatus) {
            RetriesWithStatus[status] += retries;
        }
        for (auto [status, failes]: other->FailesWithStatus) {
            FailesWithStatus[status] += failes;
        }
    }
};

}
}
