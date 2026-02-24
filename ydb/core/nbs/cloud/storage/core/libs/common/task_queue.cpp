#include "task_queue.h"

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTaskQueueStub final
    : public ITaskQueue
{
public:
    void Start() override
    {
    }

    void Stop() override
    {
    }

    void Enqueue(ITaskPtr task) override
    {
        task->Execute();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITaskQueuePtr CreateTaskQueueStub()
{
    return std::make_shared<TTaskQueueStub>();
}

}   // namespace NYdb::NBS
