#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>

namespace NKikimr::NOlap::NReader {

class TScanIteratorBase {
protected:
    virtual void DoOnSentDataFromInterval(const ui32 /*intervalIdx*/) const {

    }
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(const std::shared_ptr<IApplyAction>& /*task*/) {

    }

    virtual TConclusionStatus Start() = 0;

    virtual const TReadStats& GetStats() const;

    void OnSentDataFromInterval(const std::optional<ui32> intervalIdx) const {
        if (intervalIdx) {
            DoOnSentDataFromInterval(*intervalIdx);
        }
    }

    virtual std::optional<ui32> GetAvailableResultsCount() const {
        return {};
    }
    virtual bool Finished() const = 0;
    virtual TConclusion<std::shared_ptr<TPartialReadResult>> GetBatch() = 0;
    virtual void PrepareResults() {

    }
    virtual TConclusion<bool> ReadNextInterval() { return false; }
    virtual TString DebugString(const bool verbose = false) const {
        Y_UNUSED(verbose);
        return "NO_DATA";
    }
};

}
