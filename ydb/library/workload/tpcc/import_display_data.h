#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#include <util/generic/string.h>

#include <stop_token>

namespace NYdb::NTPCC {

struct TIndexBuildState {
    TOperation::TOperationId Id;
    TString Table;
    TString Name;
    double Progress = 0.0;

    TIndexBuildState(TOperation::TOperationId id, const TString& table, const TString& name)
        : Id(id), Table(table), Name(name) {}
};

struct TImportState {
    enum ELoadState {
        ELOAD_INDEXED_TABLES = 0,
        ELOAD_TABLES_BUILD_INDICES,
        EWAIT_INDICES,
        ESUCCESS
    };

    explicit TImportState(std::stop_token stopToken)
        : State(ELOAD_INDEXED_TABLES)
        , StopToken(stopToken)
    {
    }

    // copy constructor and assignment are used to share data with TUI only,
    // so it's OK to ommit StopToken

    TImportState(const TImportState& other)
        : State(other.State)
        , StopToken() // can't copy: leave unset
        , DataSizeLoaded(other.DataSizeLoaded.load(std::memory_order_relaxed))
        , IndexedRangesLoaded(other.IndexedRangesLoaded.load(std::memory_order_relaxed))
        , RangesLoaded(other.RangesLoaded.load(std::memory_order_relaxed))
        , IndexBuildStates(other.IndexBuildStates)
        , CurrentIndex(other.CurrentIndex)
        , ApproximateDataSize(other.ApproximateDataSize)
    {
    }

    TImportState& operator=(const TImportState& other) {
        if (this != &other) {
            State = other.State;
            // StopToken is not copyable — intentionally omitted
            DataSizeLoaded.store(other.DataSizeLoaded.load(std::memory_order_relaxed), std::memory_order_relaxed);
            IndexedRangesLoaded.store(other.IndexedRangesLoaded.load(std::memory_order_relaxed), std::memory_order_relaxed);
            RangesLoaded.store(other.RangesLoaded.load(std::memory_order_relaxed), std::memory_order_relaxed);
            IndexBuildStates = other.IndexBuildStates;
            CurrentIndex = other.CurrentIndex;
            ApproximateDataSize = other.ApproximateDataSize;
        }
        return *this;
    }

    ELoadState State;

    // shared with loader threads

    std::stop_token StopToken;

    std::atomic<size_t> DataSizeLoaded{0};

    std::atomic<size_t> IndexedRangesLoaded{0};
    std::atomic<size_t> RangesLoaded{0};

    // single threaded

    std::vector<TIndexBuildState> IndexBuildStates;
    size_t CurrentIndex = 0;
    size_t ApproximateDataSize = 0;
};

struct TImportStatusData {
    size_t CurrentDataSizeLoaded = 0;
    double PercentLoaded = 0.0;
    double InstantSpeedMiBs = 0.0;
    double AvgSpeedMiBs = 0.0;
    int ElapsedMinutes = 0;
    int ElapsedSeconds = 0;
    int EstimatedTimeLeftMinutes = 0;
    int EstimatedTimeLeftSeconds = 0;
    bool IsWaitingForIndices = false;
    bool IsLoadingTablesAndBuildingIndices = false;
};

struct TImportDisplayData {
    TImportDisplayData(const TImportState& importState)
        : ImportState(importState)
    {
    }

    TImportDisplayData(const TImportDisplayData&) = default;
    TImportDisplayData& operator=(const TImportDisplayData&) = default;
    TImportDisplayData(TImportDisplayData&&) = default;
    TImportDisplayData& operator=(TImportDisplayData&&) = default;

    TImportState ImportState;
    TImportStatusData StatusData;
};

} // namespace NYdb::NTPCC
