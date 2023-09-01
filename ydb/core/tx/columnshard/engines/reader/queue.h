#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

class TBatchBlobRange {
private:
    const ui64 ObjectId;
    const TBlobRange Range;
public:
    ui64 GetObjectId() const {
        return ObjectId;
    }

    const TBlobRange& GetRange() const {
        return Range;
    }

    TBatchBlobRange(const ui64 objectId, const TBlobRange range)
        : ObjectId(objectId)
        , Range(range)
    {
        Y_VERIFY(range.BlobId.IsValid());
    }
};

template <class TFetchTask>
class TFetchBlobsQueueImpl {
private:
    bool StoppedFlag = false;
    std::deque<TFetchTask> IteratorBlobsSequential;
public:
    const std::deque<TFetchTask>& GetIteratorBlobsSequential() const noexcept {
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

    const TFetchTask* front() const {
        if (!IteratorBlobsSequential.size()) {
            return nullptr;
        }
        return &IteratorBlobsSequential.front();
    }

    std::optional<TBlobRange> pop_front() {
        if (!StoppedFlag && IteratorBlobsSequential.size()) {
            auto result = IteratorBlobsSequential.front();
            IteratorBlobsSequential.pop_front();
            return result.GetRange();
        } else {
            return {};
        }
    }

    void emplace_back(const ui64 objectId, const TBlobRange& range) {
        Y_VERIFY(!StoppedFlag);
        IteratorBlobsSequential.emplace_back(objectId, range);
    }

};

using TFetchBlobsQueue = TFetchBlobsQueueImpl<TBatchBlobRange>;

}
