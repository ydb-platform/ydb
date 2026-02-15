#include "worker.h"

#include "utils.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <exception>
#include <random>

namespace NKvVolumeStress {

using namespace std::chrono_literals;

namespace {

constexpr ui32 DefaultActionMaxInFlight = 30;

ui64 ToLatencyMs(std::chrono::steady_clock::duration duration) {
    const ui64 latencyUs = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    if (latencyUs == 0) {
        return 0;
    }
    return (latencyUs + 999) / 1000;
}

} // namespace

TWorker::TExecutionContext::TExecutionContext(
    ui64 executionId,
    TString actionName,
    std::shared_ptr<TExecutionContext> parent,
    TKeyBucket* workerStorage)
    : ExecutionId(executionId)
    , ActionName(std::move(actionName))
    , Parent(std::move(parent))
    , WorkerStorage_(workerStorage)
{
}

TWorker::TExecutionContext::~TExecutionContext() {
    if (!WorkerStorage_) {
        return;
    }

    const TVector<std::pair<TString, TKeyInfo>> remaining = Keys_.Drain();
    if (!remaining.empty()) {
        WorkerStorage_->AddKeys(remaining);
    }
}

void TWorker::TExecutionContext::AddKey(const TString& key, const TKeyInfo& keyInfo) {
    Keys_.AddKey(key, keyInfo);
}

void TWorker::TExecutionContext::AddKeys(const TVector<std::pair<TString, TKeyInfo>>& keys) {
    Keys_.AddKeys(keys);
}

TVector<std::pair<TString, TKeyInfo>> TWorker::TExecutionContext::PickKeys(ui32 count, bool erase) {
    return Keys_.PickKeys(count, erase);
}

TWorker::TWorker(
    ui32 workerId,
    const TOptions& options,
    const NKikimrKeyValue::KeyValueVolumeStressLoad& config,
    const TString& hostPort,
    const TString& volumePath,
    TRunStats& stats,
    TInitialLoadProgress* initialLoadProgress,
    TWorkerLoadTracker* workerLoadTracker,
    const volatile std::sig_atomic_t* stopSignal)
    : WorkerId_(workerId)
    , Options_(options)
    , Config_(config)
    , VolumePath_(volumePath)
    , Stats_(stats)
    , InitialLoadProgress_(initialLoadProgress)
    , WorkerLoadTracker_(workerLoadTracker)
    , StopSignal_(stopSignal)
    , Client_(MakeKeyValueClient(hostPort, options.Version))
{
    ui32 actionIndex = 0;
    for (const auto& action : Config_.actions()) {
        ActionsByName_[action.name()] = TActionEntry{&action, actionIndex};
        ActionCapacity_ += GetActionLimit(action);
        if (action.has_parent_action() && !action.parent_action().empty()) {
            ChildrenByParent_[action.parent_action()].push_back(action.name());
        }
        ++actionIndex;
    }
    ActionPoolSize_ = GetActionPoolSize();

    InitialContext_ = std::make_shared<TExecutionContext>(0, "__initial__", nullptr, &WorkerDataStorage_);

    if (WorkerLoadTracker_) {
        WorkerLoadTracker_->SetWorkerCapacity(WorkerId_, ActionPoolSize_);
    }

    if (static_cast<int>(Config_.partition_mode()) == 1 && Config_.volume_config().partition_count() > 0) {
        FixedPartitionId_ = WorkerId_ % Config_.volume_config().partition_count();
    }
}

ui32 TWorker::GetActionLimit(const NKikimrKeyValue::Action& action) const {
    return action.has_worker_max_in_flight() && action.worker_max_in_flight() > 0
        ? action.worker_max_in_flight()
        : DefaultActionMaxInFlight;
}

ui32 TWorker::GetActionPoolSize() const {
    const ui32 hardwareThreads = std::max<ui32>(1, std::thread::hardware_concurrency());
    const ui32 desired = std::max<ui32>(1, hardwareThreads * 2);
    const ui32 capacity = std::max<ui32>(1, ActionCapacity_);
    return std::min(capacity, desired);
}

TWorker::TExecutionContextPtr TWorker::CreateExecutionContext(const TString& actionName, TExecutionContextPtr parentContext) {
    const ui64 executionId = ExecutionIdCounter_.fetch_add(1, std::memory_order_relaxed);
    return std::make_shared<TExecutionContext>(executionId, actionName, std::move(parentContext), &WorkerDataStorage_);
}

TWorker::TExecutionContextPtr TWorker::FindNearestAncestorAction(
    const TExecutionContextPtr& context,
    const TString& actionName) const
{
    TExecutionContextPtr current = context ? context->Parent : nullptr;
    while (current) {
        if (current->ActionName == actionName) {
            return current;
        }
        current = current->Parent;
    }
    return nullptr;
}

TVector<std::pair<TString, TKeyInfo>> TWorker::PickSourceKeys(
    const NKikimrKeyValue::Action& action,
    const TExecutionContextPtr& context,
    ui32 count,
    bool erase)
{
    if (count == 0) {
        return {};
    }

    auto pickFromWorker = [this, count, erase]() {
        return WorkerDataStorage_.PickKeys(count, erase);
    };

    auto pickFromInitial = [this, count, erase, &pickFromWorker]() {
        if (InitialContext_) {
            TVector<std::pair<TString, TKeyInfo>> keys = InitialContext_->PickKeys(count, erase);
            if (!keys.empty()) {
                return keys;
            }
        }
        return pickFromWorker();
    };

    if (!action.has_action_data_mode()) {
        return pickFromInitial();
    }

    const auto& mode = action.action_data_mode();
    switch (mode.Mode_case()) {
        case NKikimrKeyValue::ActionDataMode::kWorker:
            return pickFromInitial();
        case NKikimrKeyValue::ActionDataMode::kFromPrevActions: {
            if (mode.from_prev_actions().action_name_size() == 0) {
                return {};
            }

            const TString& sourceAction = mode.from_prev_actions().action_name(0);
            if (const TExecutionContextPtr sourceContext = FindNearestAncestorAction(context, sourceAction)) {
                return sourceContext->PickKeys(count, erase);
            }

            return pickFromWorker();
        }
        case NKikimrKeyValue::ActionDataMode::MODE_NOT_SET:
            return pickFromInitial();
    }

    return pickFromInitial();
}

void TWorker::LoadInitialData() {
    try {
        WriteInitialDataImpl();
    } catch (const std::exception& e) {
        Stats_.RecordError("worker_exception", e.what());
    }
}

void TWorker::Run(std::chrono::steady_clock::time_point endAt) {
    bool actionPoolStarted = false;
    try {
        StartActionPool();
        actionPoolStarted = true;

        for (const auto& action : Config_.actions()) {
            if (action.has_parent_action() && !action.parent_action().empty()) {
                continue;
            }

            if (action.has_period_us() && action.period_us() > 0) {
                Schedulers_.emplace_back([this, endAt, name = action.name(), period = action.period_us()] {
                    PeriodicLoop(name, period, endAt);
                });
            } else {
                ScheduleAction(action.name());
            }
        }

        while (!IsStopped() && std::chrono::steady_clock::now() < endAt) {
            std::this_thread::sleep_for(100ms);
        }

        StopRequested_.store(true, std::memory_order_relaxed);
        StopSchedulers();
        WaitForActions();
        StopActionPool();
    } catch (const std::exception& e) {
        StopRequested_.store(true, std::memory_order_relaxed);
        StopSchedulers();
        if (actionPoolStarted) {
            WaitForActions();
            StopActionPool();
        }
        Stats_.RecordError("worker_exception", e.what());
    }
}

void TWorker::StartActionPool() {
    ActionQueueStopRequested_.store(false, std::memory_order_relaxed);
    NextActionQueueIndex_.store(0, std::memory_order_relaxed);

    ActionQueues_.clear();
    ActionQueues_.reserve(ActionPoolSize_);
    ActionWorkers_.clear();
    ActionWorkers_.reserve(ActionPoolSize_);

    try {
        for (ui32 i = 0; i < ActionPoolSize_; ++i) {
            ActionQueues_.push_back(std::make_unique<TActionQueue>());
            ActionWorkers_.emplace_back([this, i] {
                ActionWorkerLoop(i);
            });
        }
    } catch (...) {
        ActionQueueStopRequested_.store(true, std::memory_order_relaxed);
        for (const auto& queue : ActionQueues_) {
            if (queue) {
                queue->Cv.notify_all();
            }
        }
        for (auto& workerThread : ActionWorkers_) {
            if (workerThread.joinable()) {
                workerThread.join();
            }
        }
        ActionWorkers_.clear();
        ActionQueues_.clear();
        throw;
    }
}

void TWorker::StopActionPool() {
    ActionQueueStopRequested_.store(true, std::memory_order_relaxed);
    for (const auto& queue : ActionQueues_) {
        if (queue) {
            queue->Cv.notify_all();
        }
    }

    for (auto& workerThread : ActionWorkers_) {
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }
    ActionWorkers_.clear();
    ActionQueues_.clear();
}

void TWorker::ActionWorkerLoop(ui32 queueIndex) {
    if (queueIndex >= ActionQueues_.size() || !ActionQueues_[queueIndex]) {
        return;
    }
    TActionQueue& queue = *ActionQueues_[queueIndex];

    while (true) {
        TActionTask task;
        {
            std::unique_lock lock(queue.Mutex);
            queue.Cv.wait(lock, [this, &queue] {
                return ActionQueueStopRequested_.load(std::memory_order_relaxed) || !queue.Pending.empty();
            });

            if (queue.Pending.empty()) {
                if (ActionQueueStopRequested_.load(std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }

            task = std::move(queue.Pending.front());
            queue.Pending.pop_front();
        }

        try {
            ExecuteAction(task.ActionName, task.ExecutionContext);
        } catch (const std::exception& e) {
            std::optional<ui32> actionStatsIndex;
            if (const auto actionIt = ActionsByName_.find(task.ActionName); actionIt != ActionsByName_.end()) {
                actionStatsIndex = actionIt->second.StatsIndex;
            }
            Stats_.RecordError("action_exception", e.what(), actionStatsIndex);
        } catch (...) {
            std::optional<ui32> actionStatsIndex;
            if (const auto actionIt = ActionsByName_.find(task.ActionName); actionIt != ActionsByName_.end()) {
                actionStatsIndex = actionIt->second.StatsIndex;
            }
            Stats_.RecordError("action_exception", "unknown exception", actionStatsIndex);
        }

        {
            std::lock_guard lock(RunningByActionMutex_);
            auto it = RunningByAction_.find(task.ActionName);
            if (it != RunningByAction_.end() && it->second) {
                --it->second;
            }
        }

        if (ActiveActions_.fetch_sub(1, std::memory_order_relaxed) == 1) {
            std::lock_guard lock(ActiveActionsMutex_);
            ActiveActionsCv_.notify_all();
        }

        if (WorkerLoadTracker_) {
            WorkerLoadTracker_->AddActive(WorkerId_, -1);
        }
    }
}

bool TWorker::IsStopped() const {
    if (StopRequested_.load(std::memory_order_relaxed)) {
        return true;
    }
    return StopSignal_ && *StopSignal_ != 0;
}

void TWorker::StopSchedulers() {
    for (auto& scheduler : Schedulers_) {
        if (scheduler.joinable()) {
            scheduler.join();
        }
    }
    Schedulers_.clear();
}

void TWorker::WaitForActions() {
    std::unique_lock lock(ActiveActionsMutex_);
    const bool completed = ActiveActionsCv_.wait_for(lock, 30s, [this] {
        return ActiveActions_.load(std::memory_order_relaxed) == 0;
    });

    if (!completed) {
        Stats_.RecordError("worker_shutdown_timeout", "waiting for active actions timed out");
    }
}

void TWorker::PeriodicLoop(const TString& actionName, ui32 periodUs, std::chrono::steady_clock::time_point endAt) {
    auto period = std::chrono::microseconds(std::max<ui32>(1, periodUs));
    auto nextRun = std::chrono::steady_clock::now();

    while (!IsStopped() && nextRun < endAt) {
        ScheduleAction(actionName);
        nextRun += period;
        std::this_thread::sleep_until(nextRun);
    }
}

void TWorker::ScheduleAction(const TString& actionName, TExecutionContextPtr parentContext) {
    if (IsStopped()) {
        return;
    }

    const auto actionIt = ActionsByName_.find(actionName);
    if (actionIt == ActionsByName_.end()) {
        Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
        return;
    }

    const auto* action = actionIt->second.Action;
    const ui32 actionStatsIndex = actionIt->second.StatsIndex;
    const ui32 limit = GetActionLimit(*action);

    {
        std::lock_guard lock(RunningByActionMutex_);
        ui32& running = RunningByAction_[actionName];
        if (running >= limit) {
            return;
        }
        ++running;
    }

    auto rollbackRunning = [this, &actionName] {
        std::lock_guard lock(RunningByActionMutex_);
        auto it = RunningByAction_.find(actionName);
        if (it != RunningByAction_.end() && it->second) {
            --it->second;
        }
    };

    TExecutionContextPtr executionContext;
    try {
        executionContext = CreateExecutionContext(actionName, std::move(parentContext));
    } catch (const std::exception& e) {
        rollbackRunning();
        Stats_.RecordError("execution_context_create_failed", e.what(), actionStatsIndex);
        return;
    }

    if (IsStopped()) {
        rollbackRunning();
        return;
    }

    TActionQueue* queue = nullptr;
    try {
        if (ActionQueueStopRequested_.load(std::memory_order_relaxed) || IsStopped()) {
            rollbackRunning();
            return;
        }

        if (ActionQueues_.empty()) {
            rollbackRunning();
            Stats_.RecordError("action_enqueue_failed", "action queue is not initialized", actionStatsIndex);
            return;
        }

        const ui32 queueIndex = static_cast<ui32>(
            NextActionQueueIndex_.fetch_add(1, std::memory_order_relaxed) % ActionQueues_.size());
        queue = ActionQueues_[queueIndex].get();
        if (!queue) {
            rollbackRunning();
            Stats_.RecordError("action_enqueue_failed", "action queue is null", actionStatsIndex);
            return;
        }

        std::lock_guard lock(queue->Mutex);
        if (ActionQueueStopRequested_.load(std::memory_order_relaxed) || IsStopped()) {
            rollbackRunning();
            return;
        }

        queue->Pending.push_back(TActionTask{actionName, std::move(executionContext)});
        ActiveActions_.fetch_add(1, std::memory_order_relaxed);
        if (WorkerLoadTracker_) {
            WorkerLoadTracker_->AddActive(WorkerId_, +1);
        }
    } catch (const std::exception& e) {
        rollbackRunning();
        Stats_.RecordError("action_enqueue_failed", e.what(), actionStatsIndex);
        return;
    }

    queue->Cv.notify_one();
}

void TWorker::ExecuteAction(const TString& actionName, TExecutionContextPtr executionContext) {
    const auto actionIt = ActionsByName_.find(actionName);
    if (actionIt == ActionsByName_.end()) {
        Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
        return;
    }

    const auto& action = *actionIt->second.Action;
    const ui32 actionStatsIndex = actionIt->second.StatsIndex;

    for (const auto& command : action.action_command()) {
        switch (command.Command_case()) {
            case NKikimrKeyValue::ActionCommand::kPrint: {
                Cerr << "[worker=" << WorkerId_ << "][action=" << actionName << "] " << command.print().msg() << Endl;
                break;
            }
            case NKikimrKeyValue::ActionCommand::kRead: {
                ExecuteReadCommand(action, actionStatsIndex, executionContext, command.read());
                break;
            }
            case NKikimrKeyValue::ActionCommand::kWrite: {
                (void)ExecuteWriteCommand(actionName, command.write(), actionStatsIndex, executionContext);
                break;
            }
            case NKikimrKeyValue::ActionCommand::kDelete: {
                ExecuteDeleteCommand(action, actionStatsIndex, executionContext, command.delete_());
                break;
            }
            case NKikimrKeyValue::ActionCommand::COMMAND_NOT_SET: {
                Stats_.RecordError("empty_command", TStringBuilder() << "Action " << actionName << " has empty command", actionStatsIndex);
                break;
            }
        }
    }

    Stats_.RecordAction(actionStatsIndex);

    if (!IsStopped()) {
        const auto childrenIt = ChildrenByParent_.find(actionName);
        if (childrenIt != ChildrenByParent_.end()) {
            for (const TString& childName : childrenIt->second) {
                ScheduleAction(childName, executionContext);
            }
        }
    }
}

void TWorker::WriteInitialDataImpl() {
    for (const auto& writeCommand : Config_.initial_data().write_commands()) {
        if (IsStopped()) {
            break;
        }

        const bool success = ExecuteWriteCommand("__initial__", writeCommand, std::nullopt, InitialContext_);
        if (InitialLoadProgress_) {
            const ui64 bytes = static_cast<ui64>(writeCommand.size()) * writeCommand.count();
            InitialLoadProgress_->OnCommandFinished(bytes, success);
        }
    }
}

bool TWorker::ExecuteWriteCommand(
    const TString& actionName,
    const NKikimrKeyValue::ActionCommand_Write& writeCommand,
    std::optional<ui32> actionStatsIndex,
    const TExecutionContextPtr& executionContext)
{
    if (writeCommand.count() == 0) {
        return true;
    }

    const ui32 partitionId = SelectPartitionId();

    TVector<std::pair<TString, TString>> pairs;
    pairs.reserve(writeCommand.count());

    for (ui32 i = 0; i < writeCommand.count(); ++i) {
        const ui64 keyId = WriteKeyCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
        TString key = TStringBuilder() << actionName << "_" << WorkerId_ << "_" << keyId;
        TString value = GetPatternData(writeCommand.size());
        pairs.emplace_back(key, value);
    }

    TString error;
    const auto startedAt = std::chrono::steady_clock::now();
    const bool ok = Client_->Write(VolumePath_, partitionId, pairs, writeCommand.channel(), &error);
    const ui64 latencyMs = ToLatencyMs(std::chrono::steady_clock::now() - startedAt);

    if (actionStatsIndex) {
        Stats_.RecordLatency(*actionStatsIndex, latencyMs);
    }

    if (!ok) {
        Stats_.RecordError("write_failed", error, actionStatsIndex);
        return false;
    }

    ui64 bytesWritten = 0;
    TVector<std::pair<TString, TKeyInfo>> writtenKeys;
    writtenKeys.reserve(pairs.size());

    for (const auto& [key, value] : pairs) {
        const TKeyInfo keyInfo{partitionId, static_cast<ui32>(value.size())};
        writtenKeys.emplace_back(key, keyInfo);
        bytesWritten += value.size();
    }

    executionContext->AddKeys(writtenKeys);

    if (actionStatsIndex) {
        Stats_.RecordWriteBytes(*actionStatsIndex, bytesWritten);
    }

    if (Options_.Verbose) {
        Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << pairs.size() << Endl;
    }

    return true;
}

void TWorker::ExecuteReadCommand(
    const NKikimrKeyValue::Action& action,
    ui32 actionStatsIndex,
    const TExecutionContextPtr& executionContext,
    const NKikimrKeyValue::ActionCommand_Read& readCommand)
{
    if (readCommand.count() == 0) {
        return;
    }

    const auto keys = PickSourceKeys(action, executionContext, readCommand.count(), false);

    for (const auto& [key, info] : keys) {
        if (IsStopped()) {
            break;
        }

        const ui32 maxOffset = info.KeySize > readCommand.size() ? info.KeySize - readCommand.size() : 0;
        ui32 offset = 0;
        if (maxOffset > 0) {
            std::uniform_int_distribution<ui32> distribution(0, maxOffset);
            offset = distribution(RandomEngine());
        }

        TString value;
        TString error;
        const auto startedAt = std::chrono::steady_clock::now();
        const bool ok = Client_->Read(VolumePath_, info.PartitionId, key, offset, readCommand.size(), &value, &error);
        const ui64 latencyMs = ToLatencyMs(std::chrono::steady_clock::now() - startedAt);
        Stats_.RecordLatency(actionStatsIndex, latencyMs);

        if (!ok) {
            Stats_.RecordError("read_failed", error, actionStatsIndex);
            continue;
        }

        Stats_.RecordReadBytes(actionStatsIndex, value.size());

        if (readCommand.verify_data()) {
            TString expected = GetPatternData(info.KeySize);
            expected = expected.substr(offset, readCommand.size());
            if (value != expected) {
                Stats_.RecordError(
                    "verify_failed",
                    TStringBuilder()
                        << "key=" << key
                        << " partition=" << info.PartitionId
                        << " offset=" << offset
                        << " requested_size=" << readCommand.size()
                        << " expected_len=" << expected.size()
                        << " actual_len=" << value.size(),
                    actionStatsIndex);
            }
        }
    }
}

void TWorker::ExecuteDeleteCommand(
    const NKikimrKeyValue::Action& action,
    ui32 actionStatsIndex,
    const TExecutionContextPtr& executionContext,
    const NKikimrKeyValue::ActionCommand_Delete& deleteCommand)
{
    if (deleteCommand.count() == 0) {
        return;
    }

    const auto keys = PickSourceKeys(action, executionContext, deleteCommand.count(), true);

    for (const auto& [key, info] : keys) {
        if (IsStopped()) {
            break;
        }

        TString error;
        const auto startedAt = std::chrono::steady_clock::now();
        const bool ok = Client_->DeleteKey(VolumePath_, info.PartitionId, key, &error);
        const ui64 latencyMs = ToLatencyMs(std::chrono::steady_clock::now() - startedAt);
        Stats_.RecordLatency(actionStatsIndex, latencyMs);

        if (!ok) {
            Stats_.RecordError("delete_failed", error, actionStatsIndex);
        }
    }
}

ui32 TWorker::SelectPartitionId() {
    if (FixedPartitionId_) {
        return *FixedPartitionId_;
    }

    const ui32 partitionCount = std::max<ui32>(1, Config_.volume_config().partition_count());
    std::uniform_int_distribution<ui32> distribution(0, partitionCount - 1);
    return distribution(RandomEngine());
}

TString TWorker::GetPatternData(ui32 size) {
    std::lock_guard lock(PatternCacheMutex_);
    auto it = PatternCache_.find(size);
    if (it == PatternCache_.end()) {
        it = PatternCache_.emplace(size, GeneratePatternData(size)).first;
    }
    return it->second;
}

} // namespace NKvVolumeStress
