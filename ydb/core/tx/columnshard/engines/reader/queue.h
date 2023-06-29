#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

class TBatchBlobRange {
private:
    const ui64 GranuleId;
    const TBlobRange Range;
public:
    ui64 GetGranuleId() const {
        return GranuleId;
    }

    const TBlobRange& GetRange() const {
        return Range;
    }

    TBatchBlobRange(const ui64 granuleId, const TBlobRange range)
        : GranuleId(granuleId)
        , Range(range)
    {

    }
};

class TFetchBlobsQueue {
private:
    bool StoppedFlag = false;
    std::deque<TBatchBlobRange> IteratorBlobsSequential;
public:
    const std::deque<TBatchBlobRange>& GetIteratorBlobsSequential() const noexcept {
        return IteratorBlobsSequential;
    }

    bool IsStopped() const {
        return StoppedFlag;
    }

    void Stop() {
        IteratorBlobsSequential.clear();
        StoppedFlag = true;
    }

    bool empty() const {
        return IteratorBlobsSequential.empty();
    }

    size_t size() const {
        return IteratorBlobsSequential.size();
    }

    const TBatchBlobRange* front() const {
        if (!IteratorBlobsSequential.size()) {
            return nullptr;
        }
        return &IteratorBlobsSequential.front();
    }
    TBlobRange pop_front();

    void emplace_back(const ui64 granuleId, const TBlobRange& range);
};

}
