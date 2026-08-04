#include <ydb/library/drr/drr.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <array>
#include <deque>
#include <memory>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxDrrQueues = 8;

class TDrrQueue;

struct TDrrTask {
    NScheduling::TUCost Cost = 1;
    ui32 QueueId = 0;

    NScheduling::TUCost GetCost() const {
        return Cost;
    }
};

class TDrrQueue : public NScheduling::TDRRQueue {
public:
    using TTask = TDrrTask;

    TDrrQueue(ui32 id, NScheduling::TWeight weight, NScheduling::TUCost maxBurst)
        : NScheduling::TDRRQueue(weight, maxBurst)
        , Id(id)
    {}

    void Push(NScheduling::TUCost cost, bool front) {
        auto task = std::make_unique<TDrrTask>();
        task->Cost = cost;
        task->QueueId = Id;
        if (Tasks.empty() && GetScheduler()) {
            GetScheduler()->ActivateQueue(this);
        }
        if (front) {
            Tasks.push_front(std::move(task));
            if (GetScheduler()) {
                GetScheduler()->DropCache(this);
            }
        } else {
            Tasks.push_back(std::move(task));
        }
    }

    void OnSchedulerAttach() {
        if (!Tasks.empty()) {
            GetScheduler()->ActivateQueue(this);
        }
    }

    TTask* PeekTask() {
        Y_ABORT_UNLESS(!Tasks.empty());
        return Tasks.front().get();
    }

    void PopTask() {
        Y_ABORT_UNLESS(!Tasks.empty());
        Tasks.pop_front();
    }

    bool Empty() const {
        return Tasks.empty();
    }

    size_t Size() const {
        return Tasks.size();
    }

private:
    ui32 Id = 0;
    std::deque<std::unique_ptr<TDrrTask>> Tasks;
};

using TDrrScheduler = NScheduling::TDRRScheduler<TDrrQueue, ui32>;

void CheckDrrQueues(const std::array<std::shared_ptr<TDrrQueue>, MaxDrrQueues>& queues, const std::array<size_t, MaxDrrQueues>& model) {
    for (size_t i = 0; i < MaxDrrQueues; ++i) {
        if (queues[i]) {
            Y_ABORT_UNLESS(queues[i]->Size() == model[i]);
            Y_ABORT_UNLESS(queues[i]->GetScheduler() != nullptr);
        } else {
            Y_ABORT_UNLESS(model[i] == 0);
        }
    }
}

void ExerciseDrr(FuzzedDataProvider& fdp) {
    TDrrScheduler scheduler(128);
    std::array<std::shared_ptr<TDrrQueue>, MaxDrrQueues> queues;
    std::array<size_t, MaxDrrQueues> model = {};
    size_t totalTasks = 0;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 id = fdp.ConsumeIntegralInRange<ui32>(0, MaxDrrQueues - 1);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 6)) {
            case 0:
                if (!queues[id]) {
                    const auto weight = fdp.ConsumeIntegralInRange<NScheduling::TWeight>(1, 8);
                    const auto maxBurst = fdp.ConsumeIntegralInRange<NScheduling::TUCost>(32, 256);
                    queues[id] = std::make_shared<TDrrQueue>(id, weight, maxBurst);
                    Y_ABORT_UNLESS(scheduler.AddQueue(id, queues[id]));
                }
                break;
            case 1:
                if (queues[id]) {
                    scheduler.DropCache(queues[id].get());
                    scheduler.DeleteQueue(id);
                    totalTasks -= model[id];
                    model[id] = 0;
                    queues[id].reset();
                }
                break;
            case 2:
            case 3:
                if (queues[id] && model[id] < 32 && totalTasks < 128) {
                    queues[id]->Push(fdp.ConsumeIntegralInRange<NScheduling::TUCost>(1, 64), fdp.ConsumeBool());
                    ++model[id];
                    ++totalTasks;
                }
                break;
            case 4:
                if (queues[id]) {
                    scheduler.UpdateQueueWeight(queues[id].get(), fdp.ConsumeIntegralInRange<NScheduling::TWeight>(1, 8));
                }
                break;
            case 5: {
                TDrrTask* first = scheduler.PeekTask();
                TDrrTask* second = scheduler.PeekTask();
                Y_ABORT_UNLESS(first == second);
                if (first) {
                    Y_ABORT_UNLESS(first->QueueId < MaxDrrQueues);
                    Y_ABORT_UNLESS(model[first->QueueId] > 0);
                }
                break;
            }
            default: {
                TDrrTask* task = scheduler.PeekTask();
                if (task) {
                    const ui32 queueId = task->QueueId;
                    scheduler.PopTask();
                    Y_ABORT_UNLESS(model[queueId] > 0);
                    --model[queueId];
                    --totalTasks;
                } else {
                    Y_ABORT_UNLESS(totalTasks == 0);
                }
                break;
            }
        }
        CheckDrrQueues(queues, model);
    }

    for (size_t i = 0; i < totalTasks + MaxDrrQueues; ++i) {
        TDrrTask* task = scheduler.PeekTask();
        if (!task) {
            break;
        }
        const ui32 queueId = task->QueueId;
        scheduler.PopTask();
        Y_ABORT_UNLESS(model[queueId] > 0);
        --model[queueId];
    }
    CheckDrrQueues(queues, model);
}

void ExerciseDrrFairness(FuzzedDataProvider& fdp) {
    const auto quantum = fdp.ConsumeIntegralInRange<NScheduling::TUCost>(64, 512);
    TDrrScheduler scheduler(quantum);
    std::array<std::shared_ptr<TDrrQueue>, MaxDrrQueues> queues;
    std::array<NScheduling::TWeight, MaxDrrQueues> weights = {};
    const size_t queueCount = fdp.ConsumeIntegralInRange<size_t>(2, MaxDrrQueues);
    const NScheduling::TUCost taskCost = 1;

    for (size_t i = 0; i < queueCount; ++i) {
        weights[i] = fdp.ConsumeIntegralInRange<NScheduling::TWeight>(1, 8);
        queues[i] = std::make_shared<TDrrQueue>(static_cast<ui32>(i), weights[i], quantum * 4);
        Y_ABORT_UNLESS(scheduler.AddQueue(static_cast<ui32>(i), queues[i]));
        for (size_t j = 0; j < 8; ++j) {
            queues[i]->Push(taskCost, false);
        }
        Y_ABORT_UNLESS(queues[i]->GetQuantumFraction() > 0);
        Y_ABORT_UNLESS(queues[i]->GetQuantumFraction() <= quantum);
    }

    const size_t steps = fdp.ConsumeIntegralInRange<size_t>(queueCount * 16, queueCount * 96);
    for (size_t step = 0; step < steps; ++step) {
        TDrrTask* task = scheduler.PeekTask();
        Y_ABORT_UNLESS(task);
        Y_ABORT_UNLESS(task->QueueId < queueCount);
        const ui32 queueId = task->QueueId;
        scheduler.PopTask();
        queues[queueId]->Push(taskCost, false);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    if (fdp.ConsumeBool()) {
        ExerciseDrr(fdp);
    } else {
        ExerciseDrrFairness(fdp);
    }

    return 0;
}
