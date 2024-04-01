#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>

namespace NKikimr::NOlap::NReader {

class TScanIteratorBase {
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(IDataTasksProcessor::ITask::TPtr /*processor*/) {

    }

    virtual const TReadStats& GetStats() const;

    virtual std::optional<ui32> GetAvailableResultsCount() const {
        return {};
    }
    virtual bool Finished() const = 0;
    virtual TConclusion<std::optional<TPartialReadResult>> GetBatch() = 0;
    virtual void PrepareResults() {

    }
    virtual TConclusion<bool> ReadNextInterval() { return false; }
    virtual TString DebugString(const bool verbose = false) const {
        Y_UNUSED(verbose);
        return "NO_DATA";
    }
};

}
