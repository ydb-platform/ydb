#pragma once

#include "blob_cache.h"
#include "engines/reader/conveyor_task.h"
#include "engines/indexed_read_data.h"

namespace NKikimr::NColumnShard {

class TScanIteratorBase {
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(IDataTasksProcessor::ITask::TPtr /*processor*/) {

    }
    virtual void AddData(const NBlobCache::TBlobRange& /*blobRange*/, TString /*data*/) {}
    virtual bool HasWaitingTasks() const = 0;
    virtual bool Finished() const = 0;
    virtual NOlap::TPartialReadResult GetBatch() = 0;
    virtual NBlobCache::TBlobRange GetNextBlobToRead() { return NBlobCache::TBlobRange(); }
    virtual size_t ReadyResultsCount() const = 0;
};

}
