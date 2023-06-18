#include "queue.h"

namespace NKikimr::NOlap {

NKikimr::NOlap::TBlobRange TFetchBlobsQueue::pop_front() {
    if (!StoppedFlag && IteratorBlobsSequential.size()) {
        auto result = IteratorBlobsSequential.front();
        IteratorBlobsSequential.pop_front();
        return result.GetRange();
    } else {
        return TBlobRange();
    }
}

void TFetchBlobsQueue::emplace_front(const ui64 granuleId, const TBlobRange& range) {
    Y_VERIFY(!StoppedFlag);
    IteratorBlobsSequential.emplace_front(granuleId, range);
}

void TFetchBlobsQueue::emplace_back(const ui64 granuleId, const TBlobRange& range) {
    Y_VERIFY(!StoppedFlag);
    IteratorBlobsSequential.emplace_back(granuleId, range);
}

}
