#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mailbox_queue_revolving.h>
#include <ydb/library/actors/core/scheduler_queue.h>
#include <ydb/library/actors/core/thread_context.h>
#include <ydb/library/actors/queues/activation_queue.h>
#include <ydb/library/actors/queues/mpmc_ring_queue.h>
#include <ydb/library/actors/util/unordered_cache.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <array>
#include <deque>
#include <map>
#include <memory>
#include <optional>

namespace {

constexpr size_t MaxOps = 192;
void ExerciseSchedulerQueue(FuzzedDataProvider& fdp) {
    NActors::NSchedulerQueue::TQueueType queue;
    std::deque<ui64> model;
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);

    for (size_t i = 0; i < ops; ++i) {
        if (fdp.ConsumeBool()) {
            ui64 instant = fdp.ConsumeIntegral<ui64>();
            if (instant == 0) {
                instant = 1;
            }
            queue.Writer.Push(instant, nullptr, nullptr);
            model.push_back(instant);
        } else {
            auto* entry = queue.Reader.Pop();
            if (model.empty()) {
                Y_ABORT_UNLESS(entry == nullptr);
            } else {
                Y_ABORT_UNLESS(entry != nullptr);
                Y_ABORT_UNLESS(entry->InstantMicroseconds == model.front());
                Y_ABORT_UNLESS(entry->Ev == nullptr);
                Y_ABORT_UNLESS(entry->Cookie == nullptr);
                model.pop_front();
            }
        }
    }
}

void ExerciseRevolvingMailboxQueue(FuzzedDataProvider& fdp) {
    using TQueue = NActors::TRevolvingMailboxQueue<ui64, 3, 128>;
    TQueue::TReader reader;
    TQueue::TWriter writer(reader);
    std::deque<ui64> model;
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);

    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 3)) {
            case 0: {
                const ui64 value = 1 + fdp.ConsumeIntegralInRange<ui64>(0, 1024);
                writer.Push(value);
                model.push_back(value);
                break;
            }
            case 1: {
                const ui64 value = reader.Head();
                Y_ABORT_UNLESS(value == (model.empty() ? 0 : model.front()));
                break;
            }
            case 2: {
                auto it = reader.Iterator();
                for (ui64 expected : model) {
                    Y_ABORT_UNLESS(it.Next() == expected);
                }
                Y_ABORT_UNLESS(it.Next() == 0);
                break;
            }
            default: {
                const ui64 value = reader.Pop();
                Y_ABORT_UNLESS(value == (model.empty() ? 0 : model.front()));
                if (!model.empty()) {
                    model.pop_front();
                }
                break;
            }
        }
    }

    while (!model.empty()) {
        Y_ABORT_UNLESS(reader.Pop() == model.front());
        model.pop_front();
    }
}

void AddMultiset(std::map<ui32, size_t>& model, ui32 value) {
    Y_ABORT_UNLESS(value != 0);
    ++model[value];
}

void RemoveMultiset(std::map<ui32, size_t>& model, ui32 value) {
    Y_ABORT_UNLESS(value != 0);
    auto it = model.find(value);
    Y_ABORT_UNLESS(it != model.end());
    if (--it->second == 0) {
        model.erase(it);
    }
}

void ExerciseUnorderedCache(FuzzedDataProvider& fdp) {
    TUnorderedCache<ui32, 64, 1> cache;
    std::map<ui32, size_t> model;
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);

    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 2)) {
            case 0: {
                const ui32 value = 1 + fdp.ConsumeIntegralInRange<ui32>(0, 255);
                cache.Push(value, fdp.ConsumeIntegral<ui64>());
                AddMultiset(model, value);
                break;
            }
            case 1: {
                std::array<ui32, 8> values;
                const ui32 count = fdp.ConsumeIntegralInRange<ui32>(0, values.size());
                for (ui32 j = 0; j < count; ++j) {
                    values[j] = 1 + fdp.ConsumeIntegralInRange<ui32>(0, 255);
                    AddMultiset(model, values[j]);
                }
                cache.PushBulk(values.data(), count, fdp.ConsumeIntegral<ui64>());
                break;
            }
            default: {
                const ui32 value = cache.Pop(fdp.ConsumeIntegral<ui64>());
                if (value) {
                    RemoveMultiset(model, value);
                } else {
                    Y_ABORT_UNLESS(model.empty());
                }
                break;
            }
        }
    }

    while (!model.empty()) {
        const ui32 value = cache.Pop(0);
        Y_ABORT_UNLESS(value != 0);
        RemoveMultiset(model, value);
    }
}

template <class TQueue>
void ExerciseFifoQueue(TQueue& queue, FuzzedDataProvider& fdp) {
    std::deque<ui32> model;
    const bool singleConsumer = fdp.ConsumeBool();
    typename TQueue::EPopMode mode = TQueue::EPopMode::ReallySlow;
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);

    for (size_t i = 0; i < ops; ++i) {
        if (fdp.ConsumeBool()) {
            const ui32 value = 1 + fdp.ConsumeIntegralInRange<ui32>(0, 4095);
            const bool pushed = queue.TryPush(value);
            if (pushed) {
                model.push_back(value);
            }
        } else {
            std::optional<ui32> value;
            if (singleConsumer) {
                value = queue.TryPopSingleConsumer();
            } else {
                value = queue.TryPop(mode);
            }
            if (value) {
                Y_ABORT_UNLESS(!model.empty());
                Y_ABORT_UNLESS(*value == model.front());
                model.pop_front();
            } else {
                Y_ABORT_UNLESS(model.empty());
            }
        }
    }
}

void ExerciseActorQueues(FuzzedDataProvider& fdp) {
    NActors::TMPMCRingQueue<4> ring;
    ExerciseFifoQueue(ring, fdp);

    NActors::TRingActivationQueue activation(1);
    std::deque<ui32> activationModel;
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        if (fdp.ConsumeBool()) {
            const ui32 value = 1 + fdp.ConsumeIntegralInRange<ui32>(0, 4095);
            activation.Push(value, fdp.ConsumeIntegral<ui64>());
            activationModel.push_back(value);
        } else {
            const ui32 value = activation.Pop(fdp.ConsumeIntegral<ui64>());
            if (value) {
                Y_ABORT_UNLESS(!activationModel.empty());
                Y_ABORT_UNLESS(value == activationModel.front());
                activationModel.pop_front();
            } else {
                Y_ABORT_UNLESS(activationModel.empty());
            }
        }
    }
}

struct TFuzzRunnable : public NActors::TActorRunnableItem::TImpl<TFuzzRunnable> {
    ui32* Runs = nullptr;

    explicit TFuzzRunnable(ui32* runs)
        : Runs(runs)
    {}

    void DoRun(NActors::IActor*) noexcept {
        ++*Runs;
    }
};

void ExerciseActorRunnableQueue(FuzzedDataProvider& fdp) {
    ui32 runs = 0;
    ui32 expectedRuns = 0;
    std::array<std::unique_ptr<TFuzzRunnable>, 8> items;
    std::array<bool, 8> pending = {};
    for (auto& item : items) {
        item = std::make_unique<TFuzzRunnable>(&runs);
    }

    {
        NActors::TActorRunnableQueue queue(nullptr);
        const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
        for (size_t i = 0; i < ops; ++i) {
            const size_t id = fdp.ConsumeIntegralInRange<size_t>(0, items.size() - 1);
            switch (fdp.ConsumeIntegralInRange<unsigned>(0, 2)) {
                case 0:
                    if (!pending[id]) {
                        NActors::TActorRunnableQueue::Schedule(items[id].get());
                        pending[id] = true;
                    }
                    break;
                case 1:
                    if (pending[id]) {
                        NActors::TActorRunnableQueue::Cancel(items[id].get());
                        pending[id] = false;
                    }
                    break;
                default:
                    queue.Execute();
                    for (bool& value : pending) {
                        if (value) {
                            ++expectedRuns;
                            value = false;
                        }
                    }
                    Y_ABORT_UNLESS(runs == expectedRuns);
                    break;
            }
        }
        for (bool value : pending) {
            if (value) {
                ++expectedRuns;
            }
        }
    }

    Y_ABORT_UNLESS(runs == expectedRuns);
}

void ExerciseRuntimeUtilities(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<unsigned>(0, 4)) {
        case 0:
            ExerciseSchedulerQueue(fdp);
            break;
        case 1:
            ExerciseRevolvingMailboxQueue(fdp);
            break;
        case 2:
            ExerciseUnorderedCache(fdp);
            break;
        case 3:
            ExerciseActorQueues(fdp);
            break;
        default:
            ExerciseActorRunnableQueue(fdp);
            break;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseRuntimeUtilities(fdp);

    return 0;
}
