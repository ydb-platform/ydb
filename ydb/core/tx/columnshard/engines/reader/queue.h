#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

class TFetchBlobsQueue {
private:
    bool StoppedFlag = false;
    std::deque<TBlobRange> IteratorBlobsSequential;
public:
    const std::deque<TBlobRange>& GetIteratorBlobsSequential() const noexcept {
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

    TBlobRange pop_front();

    void emplace_front(const TBlobRange& range);
    void emplace_back(const TBlobRange& range);
};

}
