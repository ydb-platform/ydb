#include "queue.h"

namespace NKikimr::NOlap {

NKikimr::NOlap::TBlobRange TFetchBlobsQueue::pop_front() {
    if (!StoppedFlag && IteratorBlobsSequential.size()) {
        TBlobRange result = IteratorBlobsSequential.front();
        IteratorBlobsSequential.pop_front();
        return result;
    } else {
        return TBlobRange();
    }
}

void TFetchBlobsQueue::emplace_front(const TBlobRange& range) {
    Y_VERIFY(!StoppedFlag);
    IteratorBlobsSequential.emplace_front(range);
}

void TFetchBlobsQueue::emplace_back(const TBlobRange& range) {
    Y_VERIFY(!StoppedFlag);
    IteratorBlobsSequential.emplace_back(range);
}

}
