#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>

namespace NKikimr::NOlap {

class TBatchReadTask {
private:
    const ui64 ObjectId;
    const std::shared_ptr<NBlobOperations::NRead::ITask> ReadTask;
public:
    ui64 GetObjectId() const {
        return ObjectId;
    }

    const std::shared_ptr<NBlobOperations::NRead::ITask>& GetTask() const {
        return ReadTask;
    }

    TBatchReadTask(const ui64 objectId, const std::shared_ptr<NBlobOperations::NRead::ITask>& readTask)
        : ObjectId(objectId)
        , ReadTask(readTask)
    {
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

    std::optional<std::shared_ptr<NBlobOperations::NRead::ITask>> pop_front() {
        if (!StoppedFlag && IteratorBlobsSequential.size()) {
            auto result = IteratorBlobsSequential.front();
            IteratorBlobsSequential.pop_front();
            return result.GetTask();
        } else {
            return {};
        }
    }

    void emplace_back(const ui64 objectId, const std::shared_ptr<NBlobOperations::NRead::ITask>& task) {
        Y_ABORT_UNLESS(!StoppedFlag);
        Y_ABORT_UNLESS(task);
        IteratorBlobsSequential.emplace_back(objectId, task);
    }

};

using TFetchBlobsQueue = TFetchBlobsQueueImpl<TBatchReadTask>;

}
