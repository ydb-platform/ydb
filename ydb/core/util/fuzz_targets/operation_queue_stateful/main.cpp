#include <ydb/core/util/circular_queue.h>
#include <ydb/core/util/operation_queue.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/datetime/base.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <deque>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;

struct TPriorityItem {
    ui32 Id = 0;
    int Priority = 0;

    bool operator==(const TPriorityItem& rhs) const {
        return Id == rhs.Id;
    }

    explicit operator size_t() const {
        return Id;
    }
};

struct TPriorityLess {
    bool operator()(const TPriorityItem& lhs, const TPriorityItem& rhs) const {
        if (lhs.Priority != rhs.Priority) {
            return lhs.Priority < rhs.Priority;
        }
        return lhs.Id < rhs.Id;
    }
};

void CheckPriorityQueue(NKikimr::NOperationQueue::TQueueWithPriority<TPriorityItem, TPriorityLess>& queue, const std::map<ui32, int>& model) {
    Y_ABORT_UNLESS(queue.Size() == model.size());
    Y_ABORT_UNLESS(queue.Empty() == model.empty());
    for (ui32 id = 0; id < 16; ++id) {
        Y_ABORT_UNLESS(queue.Contains({id, 0}) == model.contains(id));
    }
    if (!model.empty()) {
        auto best = std::min_element(model.begin(), model.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.second != rhs.second) {
                return lhs.second < rhs.second;
            }
            return lhs.first < rhs.first;
        });
        const auto& front = queue.Front();
        Y_ABORT_UNLESS(front.Id == best->first);
        Y_ABORT_UNLESS(front.Priority == best->second);
    }
}

void ExercisePriorityQueue(FuzzedDataProvider& fdp) {
    NKikimr::NOperationQueue::TQueueWithPriority<TPriorityItem, TPriorityLess> queue;
    std::map<ui32, int> model;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 id = fdp.ConsumeIntegralInRange<ui32>(0, 15);
        const int priority = fdp.ConsumeIntegralInRange<int>(-64, 64);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 5)) {
            case 0: {
                const bool inserted = queue.Enqueue(TPriorityItem{id, priority});
                const bool expected = !model.contains(id);
                Y_ABORT_UNLESS(inserted == expected);
                if (inserted) {
                    model[id] = priority;
                }
                break;
            }
            case 1: {
                const bool removed = queue.Remove({id, 0});
                const bool expected = model.erase(id) != 0;
                Y_ABORT_UNLESS(removed == expected);
                break;
            }
            case 2: {
                const bool updated = queue.UpdateIfFound({id, priority});
                const bool expected = model.contains(id);
                Y_ABORT_UNLESS(updated == expected);
                if (updated) {
                    model[id] = priority;
                }
                break;
            }
            case 3:
                if (!model.empty()) {
                    auto best = std::min_element(model.begin(), model.end(), [](const auto& lhs, const auto& rhs) {
                        if (lhs.second != rhs.second) {
                            return lhs.second < rhs.second;
                        }
                        return lhs.first < rhs.first;
                    });
                    Y_ABORT_UNLESS(queue.Front().Id == best->first);
                    queue.PopFront();
                    model.erase(best);
                }
                break;
            case 4:
                queue.Clear();
                model.clear();
                break;
            default:
                break;
        }
        CheckPriorityQueue(queue, model);
    }
}

class TFuzzTimer : public NKikimr::NOperationQueue::ITimer {
public:
    TMonotonic Now() override {
        return Now_;
    }

    void SetWakeupTimer(TDuration delta) override {
        Y_ABORT_UNLESS(delta >= TDuration::Zero());
        Wakeups.push_back(Now_ + delta);
    }

    void Move(TDuration delta) {
        Now_ += delta;
    }

    std::vector<TMonotonic> Wakeups;

private:
    TMonotonic Now_;
};

class TFuzzStarter : public NKikimr::NOperationQueue::IStarter<ui32> {
public:
    NKikimr::NOperationQueue::EStartStatus StartOperation(const ui32& item) override {
        Started.push_back(item);
        return Status;
    }

    void OnTimeout(const ui32& item) override {
        TimedOut.push_back(item);
    }

    NKikimr::NOperationQueue::EStartStatus Status = NKikimr::NOperationQueue::EStartStatus::EOperationRunning;
    std::vector<ui32> Started;
    std::vector<ui32> TimedOut;
};

struct TOperationItem {
    ui32 Id = 0;
    int Priority = 0;

    bool operator==(const TOperationItem& rhs) const {
        return Id == rhs.Id;
    }

    size_t Hash() const {
        return THash<ui32>()(Id);
    }

    explicit operator size_t() const {
        return Hash();
    }
};

struct TOperationItemPriorityLess {
    bool operator()(const TOperationItem& lhs, const TOperationItem& rhs) const {
        if (lhs.Priority != rhs.Priority) {
            return lhs.Priority > rhs.Priority;
        }
        return lhs.Id < rhs.Id;
    }
};

class TPriorityFuzzStarter : public NKikimr::NOperationQueue::IStarter<TOperationItem> {
public:
    NKikimr::NOperationQueue::EStartStatus StartOperation(const TOperationItem& item) override {
        Started.push_back(item);
        return Status;
    }

    void OnTimeout(const TOperationItem& item) override {
        TimedOut.push_back(item);
    }

    NKikimr::NOperationQueue::EStartStatus Status = NKikimr::NOperationQueue::EStartStatus::EOperationRunning;
    std::vector<TOperationItem> Started;
    std::vector<TOperationItem> TimedOut;
};

struct TOperationModel {
    NKikimr::NOperationQueue::TConfig Config;
    bool Running = false;
    std::deque<ui32> Ready;
    std::deque<std::pair<ui32, TMonotonic>> RunningItems;
    std::deque<std::pair<ui32, TMonotonic>> Waiting;
};

bool ContainsReady(const TOperationModel& model, ui32 item) {
    return std::find(model.Ready.begin(), model.Ready.end(), item) != model.Ready.end();
}

bool ContainsAll(const TOperationModel& model, ui32 item) {
    if (ContainsReady(model, item)) {
        return true;
    }
    for (const auto& [value, ts] : model.RunningItems) {
        Y_UNUSED(ts);
        if (value == item) {
            return true;
        }
    }
    for (const auto& [value, ts] : model.Waiting) {
        Y_UNUSED(ts);
        if (value == item) {
            return true;
        }
    }
    return false;
}

bool EnqueueReady(TOperationModel& model, ui32 item) {
    if (ContainsReady(model, item)) {
        return false;
    }
    model.Ready.push_back(item);
    return true;
}

bool RemoveFromDeque(std::deque<ui32>& items, ui32 item) {
    auto it = std::find(items.begin(), items.end(), item);
    if (it == items.end()) {
        return false;
    }
    items.erase(it);
    return true;
}

bool RemoveTimed(std::deque<std::pair<ui32, TMonotonic>>& items, ui32 item, std::optional<TMonotonic>* ts = nullptr) {
    auto it = std::find_if(items.begin(), items.end(), [&](const auto& value) {
        return value.first == item;
    });
    if (it == items.end()) {
        return false;
    }
    if (ts) {
        *ts = it->second;
    }
    items.erase(it);
    return true;
}

void ProcessModel(TOperationModel& model, TFuzzStarter& starter, TMonotonic now) {
    if (model.Config.Timeout) {
        while (!model.RunningItems.empty() && model.RunningItems.front().second + model.Config.Timeout <= now) {
            const ui32 item = model.RunningItems.front().first;
            model.RunningItems.pop_front();
            starter.TimedOut.push_back(item);
            if (model.Config.IsCircular) {
                if (model.Config.MinOperationRepeatDelay) {
                    model.Waiting.push_back({item, now});
                } else {
                    EnqueueReady(model, item);
                }
            }
        }
    }

    while (!model.Waiting.empty() && model.Waiting.front().second + model.Config.MinOperationRepeatDelay <= now) {
        const ui32 item = model.Waiting.front().first;
        model.Waiting.pop_front();
        EnqueueReady(model, item);
    }

    if (!model.Running) {
        return;
    }
    if ((model.Ready.empty() && model.Waiting.empty()) || model.RunningItems.size() == model.Config.InflightLimit) {
        return;
    }

    const size_t maxTries = model.Ready.size();
    for (size_t tries = 0; model.RunningItems.size() != model.Config.InflightLimit && tries < maxTries; ++tries) {
        const ui32 item = model.Ready.front();
        model.Ready.pop_front();
        starter.Started.push_back(item);
        switch (starter.Status) {
            case NKikimr::NOperationQueue::EStartStatus::EOperationRunning:
                model.RunningItems.push_back({item, now});
                break;
            case NKikimr::NOperationQueue::EStartStatus::EOperationRetry:
                EnqueueReady(model, item);
                break;
            case NKikimr::NOperationQueue::EStartStatus::EOperationRemove:
                break;
        }
    }
}

template <class TQueue>
void CheckOperationQueue(const TQueue& queue, const TOperationModel& model, const TFuzzStarter& starter, const TFuzzStarter& starterModel) {
    Y_ABORT_UNLESS(queue.Size() == model.Ready.size() + model.Waiting.size());
    Y_ABORT_UNLESS(queue.RunningSize() == model.RunningItems.size());
    Y_ABORT_UNLESS(queue.WaitingSize() == model.Waiting.size());

    const auto ready = queue.GetQueue();
    Y_ABORT_UNLESS(ready.size() == model.Ready.size());
    for (size_t i = 0; i < ready.size(); ++i) {
        Y_ABORT_UNLESS(ready[i] == model.Ready[i]);
    }

    const auto running = queue.GetRunning();
    Y_ABORT_UNLESS(running.size() == model.RunningItems.size());
    for (size_t i = 0; i < running.size(); ++i) {
        Y_ABORT_UNLESS(running[i].Item == model.RunningItems[i].first);
        Y_ABORT_UNLESS(running[i].Timestamp == model.RunningItems[i].second);
    }

    const auto waiting = queue.GetWaiting();
    Y_ABORT_UNLESS(waiting.size() == model.Waiting.size());
    for (size_t i = 0; i < waiting.size(); ++i) {
        Y_ABORT_UNLESS(waiting[i].Item == model.Waiting[i].first);
        Y_ABORT_UNLESS(waiting[i].Timestamp == model.Waiting[i].second);
    }

    Y_ABORT_UNLESS(starter.Started == starterModel.Started);
    Y_ABORT_UNLESS(starter.TimedOut == starterModel.TimedOut);
}

void ExerciseOperationQueue(FuzzedDataProvider& fdp) {
    using TQueue = NKikimr::NOperationQueue::TOperationQueue<ui32, NKikimr::TFifoQueue<ui32>>;

    TFuzzTimer timer;
    TFuzzStarter starter;
    TFuzzStarter starterModel;

    NKikimr::NOperationQueue::TConfig config;
    config.InflightLimit = fdp.ConsumeIntegralInRange<ui32>(1, 4);
    config.IsCircular = fdp.ConsumeBool();
    config.Timeout = fdp.ConsumeBool() ? TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5)) : TDuration::Zero();
    config.WakeupInterval = TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5));
    config.MinWakeupInterval = TDuration::Zero();
    config.MinOperationRepeatDelay = fdp.ConsumeBool() ? TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5)) : TDuration::Zero();

    TQueue queue(config, starter, timer);
    TOperationModel model;
    model.Config = config;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 item = fdp.ConsumeIntegralInRange<ui32>(0, 15);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 9)) {
            case 0:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRunning;
                break;
            case 1:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRetry;
                break;
            case 2:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRemove;
                break;
            case 3:
                if (!ContainsAll(model, item)) {
                    const bool inserted = queue.Enqueue(item);
                    const bool expected = EnqueueReady(model, item);
                    Y_ABORT_UNLESS(inserted == expected);
                    ProcessModel(model, starterModel, timer.Now());
                }
                break;
            case 4:
            {
                const bool wasRunning = model.Running;
                queue.Start();
                model.Running = true;
                if (!wasRunning) {
                    ProcessModel(model, starterModel, timer.Now());
                }
                break;
            }
            case 5:
                queue.Stop();
                model.Running = false;
                break;
            case 6: {
                const bool removed = queue.Remove(item);
                bool expected = RemoveFromDeque(model.Ready, item);
                std::optional<TMonotonic> ignored;
                const bool removedRunning = RemoveTimed(model.RunningItems, item, &ignored);
                if (removedRunning) {
                    ProcessModel(model, starterModel, timer.Now());
                }
                expected = RemoveTimed(model.Waiting, item, nullptr) || expected;
                Y_ABORT_UNLESS(removed == expected);
                break;
            }
            case 7: {
                std::optional<TMonotonic> ts;
                TDuration expectedDuration = TDuration::Zero();
                const bool removedRunning = RemoveTimed(model.RunningItems, item, &ts);
                if (removedRunning) {
                    expectedDuration = timer.Now() - *ts;
                    if (model.Config.IsCircular) {
                        if (model.Config.MinOperationRepeatDelay) {
                            model.Waiting.push_back({item, timer.Now()});
                        } else {
                            EnqueueReady(model, item);
                        }
                    }
                }
                const TDuration actualDuration = queue.OnDone(item);
                if (removedRunning) {
                    ProcessModel(model, starterModel, timer.Now());
                }
                Y_ABORT_UNLESS(actualDuration == expectedDuration);
                break;
            }
            case 8:
                timer.Move(TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(0, 7)));
                queue.Wakeup();
                ProcessModel(model, starterModel, timer.Now());
                break;
            default:
                queue.Clear();
                model.Ready.clear();
                model.RunningItems.clear();
                model.Waiting.clear();
                break;
        }

        CheckOperationQueue(queue, model, starter, starterModel);
    }
}

struct TPriorityOperationModel {
    NKikimr::NOperationQueue::TConfig Config;
    bool Running = false;
    bool WasRunning = false;
    std::vector<TOperationItem> ItemsToShuffle;
    std::vector<TOperationItem> Ready;
    std::deque<std::pair<TOperationItem, TMonotonic>> RunningItems;
    std::deque<std::pair<TOperationItem, TMonotonic>> Waiting;
    NKikimr::TTokenBucketBase<TMonotonic> TokenBucket;
    double Rate = 0.0;
};

bool SameOperationItemVector(const std::vector<TOperationItem>& lhs, const std::vector<TOperationItem>& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (!(lhs[i] == rhs[i]) || lhs[i].Priority != rhs[i].Priority) {
            return false;
        }
    }
    return true;
}

bool ContainsReady(const TPriorityOperationModel& model, ui32 id) {
    return std::find_if(model.Ready.begin(), model.Ready.end(), [&](const auto& item) {
        return item.Id == id;
    }) != model.Ready.end();
}

bool ContainsAll(const TPriorityOperationModel& model, ui32 id) {
    for (const auto& item : model.ItemsToShuffle) {
        if (item.Id == id) {
            return true;
        }
    }
    if (ContainsReady(model, id)) {
        return true;
    }
    for (const auto& [item, ts] : model.RunningItems) {
        Y_UNUSED(ts);
        if (item.Id == id) {
            return true;
        }
    }
    for (const auto& [item, ts] : model.Waiting) {
        Y_UNUSED(ts);
        if (item.Id == id) {
            return true;
        }
    }
    return false;
}

size_t TotalPriorityQueueSize(const TPriorityOperationModel& model) {
    const size_t size = model.ItemsToShuffle.size() + model.Ready.size() + model.Waiting.size();
    return model.Config.IsCircular ? size + model.RunningItems.size() : size;
}

void UpdatePriorityRate(TPriorityOperationModel& model, TMonotonic now) {
    if (!model.Config.MaxRate && !model.Config.RoundInterval) {
        model.Rate = 0.0;
        model.TokenBucket.SetUnlimited();
        return;
    }

    model.Rate = model.Config.MaxRate;
    if (model.Config.RoundInterval && TotalPriorityQueueSize(model) > 0) {
        double rateByInterval = TotalPriorityQueueSize(model) / static_cast<double>(model.Config.RoundInterval.Seconds());
        if (model.Config.MaxRate) {
            rateByInterval = Min(rateByInterval, model.Config.MaxRate);
        }
        model.Rate = rateByInterval;
    }

    if (model.Rate) {
        model.TokenBucket.SetCapacity(model.Config.InflightLimit);
        model.TokenBucket.SetRate(model.Rate);
        model.TokenBucket.Fill(now);
    }
}

bool EnqueueReady(TPriorityOperationModel& model, const TOperationItem& item, TMonotonic now) {
    if (ContainsReady(model, item.Id)) {
        return false;
    }
    model.Ready.push_back(item);
    UpdatePriorityRate(model, now);
    return true;
}

bool RemoveReady(TPriorityOperationModel& model, ui32 id) {
    auto it = std::find_if(model.Ready.begin(), model.Ready.end(), [&](const auto& item) {
        return item.Id == id;
    });
    if (it == model.Ready.end()) {
        return false;
    }
    model.Ready.erase(it);
    return true;
}

bool RemoveTimed(std::deque<std::pair<TOperationItem, TMonotonic>>& items, ui32 id, std::optional<TMonotonic>* ts = nullptr) {
    auto it = std::find_if(items.begin(), items.end(), [&](const auto& value) {
        return value.first.Id == id;
    });
    if (it == items.end()) {
        return false;
    }
    if (ts) {
        *ts = it->second;
    }
    items.erase(it);
    return true;
}

bool UpdateTimed(std::deque<std::pair<TOperationItem, TMonotonic>>& items, const TOperationItem& item) {
    auto it = std::find_if(items.begin(), items.end(), [&](const auto& value) {
        return value.first.Id == item.Id;
    });
    if (it == items.end()) {
        return false;
    }
    it->first = item;
    return true;
}

bool UpdatePriorityModel(TPriorityOperationModel& model, const TOperationItem& item) {
    auto ready = std::find_if(model.Ready.begin(), model.Ready.end(), [&](const auto& value) {
        return value.Id == item.Id;
    });
    if (ready != model.Ready.end()) {
        *ready = item;
        return true;
    }
    return UpdateTimed(model.Waiting, item) || UpdateTimed(model.RunningItems, item);
}

void ReEnqueuePriorityNoStart(TPriorityOperationModel& model, const TOperationItem& item, TMonotonic now) {
    if (model.Config.MinOperationRepeatDelay) {
        model.Waiting.push_back({item, now});
    } else {
        EnqueueReady(model, item, now);
    }
}

std::vector<TOperationItem>::iterator BestPriorityReady(TPriorityOperationModel& model) {
    return std::min_element(model.Ready.begin(), model.Ready.end(), TOperationItemPriorityLess());
}

void ProcessPriorityModel(TPriorityOperationModel& model, TPriorityFuzzStarter& starter, TMonotonic now) {
    if (model.Config.Timeout) {
        while (!model.RunningItems.empty() && model.RunningItems.front().second + model.Config.Timeout <= now) {
            const auto item = model.RunningItems.front().first;
            model.RunningItems.pop_front();
            starter.TimedOut.push_back(item);
            if (model.Config.IsCircular) {
                ReEnqueuePriorityNoStart(model, item, now);
            }
        }
    }

    while (!model.Waiting.empty() && model.Waiting.front().second + model.Config.MinOperationRepeatDelay <= now) {
        const auto item = model.Waiting.front().first;
        EnqueueReady(model, item, now);
        model.Waiting.pop_front();
    }

    if (!model.Running) {
        return;
    }
    if ((model.Ready.empty() && model.Waiting.empty()) || model.RunningItems.size() == model.Config.InflightLimit) {
        return;
    }

    model.TokenBucket.Fill(now);
    const size_t maxTries = model.Ready.size();
    for (size_t tries = 0; model.RunningItems.size() != model.Config.InflightLimit && tries < maxTries && model.TokenBucket.Available() >= 0; ++tries) {
        auto it = BestPriorityReady(model);
        auto item = *it;
        model.Ready.erase(it);
        starter.Started.push_back(item);
        switch (starter.Status) {
            case NKikimr::NOperationQueue::EStartStatus::EOperationRunning:
                model.TokenBucket.Take(1);
                model.RunningItems.push_back({item, now});
                break;
            case NKikimr::NOperationQueue::EStartStatus::EOperationRetry:
                if (!ContainsReady(model, item.Id)) {
                    model.Ready.push_back(item);
                }
                break;
            case NKikimr::NOperationQueue::EStartStatus::EOperationRemove:
                break;
        }
    }
}

template <class TQueue>
void CheckPriorityOperationQueue(const TQueue& queue, const TPriorityOperationModel& model, const TPriorityFuzzStarter& starter, const TPriorityFuzzStarter& starterModel) {
    Y_ABORT_UNLESS(queue.Size() == model.ItemsToShuffle.size() + model.Ready.size() + model.Waiting.size());
    Y_ABORT_UNLESS(queue.RunningSize() == model.RunningItems.size());
    Y_ABORT_UNLESS(queue.WaitingSize() == model.Waiting.size());
    Y_ABORT_UNLESS(queue.GetRate() == model.Rate);

    const auto running = queue.GetRunning();
    Y_ABORT_UNLESS(running.size() == model.RunningItems.size());
    for (size_t i = 0; i < running.size(); ++i) {
        Y_ABORT_UNLESS(running[i].Item == model.RunningItems[i].first);
        Y_ABORT_UNLESS(running[i].Item.Priority == model.RunningItems[i].first.Priority);
        Y_ABORT_UNLESS(running[i].Timestamp == model.RunningItems[i].second);
    }

    const auto waiting = queue.GetWaiting();
    Y_ABORT_UNLESS(waiting.size() == model.Waiting.size());
    for (size_t i = 0; i < waiting.size(); ++i) {
        Y_ABORT_UNLESS(waiting[i].Item == model.Waiting[i].first);
        Y_ABORT_UNLESS(waiting[i].Item.Priority == model.Waiting[i].first.Priority);
        Y_ABORT_UNLESS(waiting[i].Timestamp == model.Waiting[i].second);
    }

    Y_ABORT_UNLESS(SameOperationItemVector(starter.Started, starterModel.Started));
    Y_ABORT_UNLESS(SameOperationItemVector(starter.TimedOut, starterModel.TimedOut));
}

TOperationItem MakeOperationItem(ui32 id, ui32 priorityLowBits) {
    return {id, static_cast<int>(id * 32 + (priorityLowBits & 15))};
}

void ExercisePriorityOperationQueue(FuzzedDataProvider& fdp) {
    using TQueue = NKikimr::NOperationQueue::TOperationQueue<
        TOperationItem,
        NKikimr::NOperationQueue::TQueueWithPriority<TOperationItem, TOperationItemPriorityLess>>;

    TFuzzTimer timer;
    TPriorityFuzzStarter starter;
    TPriorityFuzzStarter starterModel;

    NKikimr::NOperationQueue::TConfig config;
    config.InflightLimit = fdp.ConsumeIntegralInRange<ui32>(1, 4);
    config.IsCircular = fdp.ConsumeBool();
    config.Timeout = fdp.ConsumeBool() ? TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5)) : TDuration::Zero();
    config.WakeupInterval = TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5));
    config.MinWakeupInterval = TDuration::Zero();
    config.MinOperationRepeatDelay = fdp.ConsumeBool() ? TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 5)) : TDuration::Zero();
    config.ShuffleOnStart = fdp.ConsumeBool();
    if (fdp.ConsumeBool()) {
        config.MaxRate = fdp.ConsumeIntegralInRange<ui32>(1, 4);
    }
    if (fdp.ConsumeBool()) {
        config.RoundInterval = TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(1, 6));
    }

    TQueue queue(config, starter, timer);
    TPriorityOperationModel model;
    model.Config = config;
    UpdatePriorityRate(model, timer.Now());

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 id = fdp.ConsumeIntegralInRange<ui32>(0, 15);
        const auto item = MakeOperationItem(id, fdp.ConsumeIntegral<ui32>());
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 10)) {
            case 0:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRunning;
                break;
            case 1:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRetry;
                break;
            case 2:
                starter.Status = starterModel.Status = NKikimr::NOperationQueue::EStartStatus::EOperationRemove;
                break;
            case 3:
                if (!ContainsAll(model, id)) {
                    const bool inserted = queue.Enqueue(item);
                    bool expected = true;
                    if (!model.WasRunning && model.Config.ShuffleOnStart) {
                        model.ItemsToShuffle.push_back(item);
                    } else {
                        expected = EnqueueReady(model, item, timer.Now());
                        ProcessPriorityModel(model, starterModel, timer.Now());
                    }
                    Y_ABORT_UNLESS(inserted == expected);
                }
                break;
            case 4:
                queue.Start();
                if (!model.Running) {
                    for (const auto& shuffled : model.ItemsToShuffle) {
                        EnqueueReady(model, shuffled, timer.Now());
                    }
                    model.ItemsToShuffle.clear();
                    model.Running = true;
                    model.WasRunning = true;
                    ProcessPriorityModel(model, starterModel, timer.Now());
                }
                break;
            case 5:
                queue.Stop();
                model.Running = false;
                break;
            case 6: {
                const bool removed = queue.Remove(item);
                bool expected = RemoveReady(model, id);
                std::optional<TMonotonic> ignored;
                if (RemoveTimed(model.RunningItems, id, &ignored)) {
                    ProcessPriorityModel(model, starterModel, timer.Now());
                }
                expected = RemoveTimed(model.Waiting, id, nullptr) || expected;
                auto shuffleIt = std::find_if(model.ItemsToShuffle.begin(), model.ItemsToShuffle.end(), [&](const auto& value) {
                    return value.Id == id;
                });
                bool removedFromShuffle = false;
                if (shuffleIt != model.ItemsToShuffle.end()) {
                    model.ItemsToShuffle.erase(shuffleIt);
                    expected = true;
                    removedFromShuffle = true;
                }
                if (expected && !removedFromShuffle) {
                    UpdatePriorityRate(model, timer.Now());
                }
                Y_ABORT_UNLESS(removed == expected);
                break;
            }
            case 7: {
                std::optional<TMonotonic> ts;
                TDuration expectedDuration = TDuration::Zero();
                const bool removedRunning = RemoveTimed(model.RunningItems, id, &ts);
                if (removedRunning) {
                    expectedDuration = timer.Now() - *ts;
                    if (model.Config.IsCircular) {
                        ReEnqueuePriorityNoStart(model, item, timer.Now());
                    }
                }
                const TDuration actualDuration = queue.OnDone(item);
                if (removedRunning) {
                    ProcessPriorityModel(model, starterModel, timer.Now());
                }
                Y_ABORT_UNLESS(actualDuration == expectedDuration);
                break;
            }
            case 8:
                timer.Move(TDuration::Seconds(fdp.ConsumeIntegralInRange<ui32>(0, 7)));
                queue.Wakeup();
                ProcessPriorityModel(model, starterModel, timer.Now());
                break;
            case 9: {
                const bool updated = queue.Update(item);
                const bool expected = UpdatePriorityModel(model, item);
                Y_ABORT_UNLESS(updated == expected);
                break;
            }
            default:
                queue.Clear();
                model.ItemsToShuffle.clear();
                model.Ready.clear();
                model.RunningItems.clear();
                model.Waiting.clear();
                break;
        }

        CheckPriorityOperationQueue(queue, model, starter, starterModel);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    switch (fdp.ConsumeIntegralInRange<unsigned>(0, 2)) {
        case 0:
            ExercisePriorityQueue(fdp);
            break;
        case 1:
            ExercisePriorityOperationQueue(fdp);
            break;
        default:
            ExerciseOperationQueue(fdp);
            break;
    }

    return 0;
}
