#pragma once

#include "blob_cache.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NColumnShard {

class TScanIteratorBase {
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(IDataPreparationTask::TPtr /*processor*/) {

    }
    virtual void AddData(const NBlobCache::TBlobRange& /*blobRange*/, TString /*data*/, IDataTasksProcessor::TPtr /*processor*/) {}
    virtual bool Finished() const = 0;
    virtual NOlap::TPartialReadResult GetBatch() = 0;
    virtual NBlobCache::TBlobRange GetNextBlobToRead() { return NBlobCache::TBlobRange(); }
    virtual size_t ReadyResultsCount() const = 0;
};

}
