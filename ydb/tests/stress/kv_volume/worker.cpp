#include "worker.h"

#include "utils.h"

#include <util/system/datetime.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <exception>
#include <random>
#include <stdexcept>

namespace NKvVolumeStress {

using namespace std::chrono_literals;

namespace {

constexpr ui32 DefaultActionMaxInFlight = 30;
constexpr auto MaxNanoSleepChunk = std::chrono::milliseconds(1);

ui64 ToLatencyMs(std::chrono::steady_clock::duration duration) {
    const ui64 latencyUs = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    if (latencyUs == 0) {
        return 0;
    }
    return (latencyUs + 999) / 1000;
}

} // namespace

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
    , Client_(MakeKeyValueClient(hostPort, options))
{
    const ui32 actionsCount = Config_.actions_size();
    Actions_.reserve(actionsCount);
    ActionIdByName_.reserve(actionsCount);
    ChildrenByActionId_.resize(actionsCount);
    RunningByAction_ = std::make_unique<TAlignedActionCounter[]>(actionsCount);
    ActionSlotSync_ = std::make_unique<TActionSlotSync[]>(actionsCount);

    ui32 actionIndex = 0;
    for (const auto& action : Config_.actions()) {
        const ui32 limit = GetActionLimit(action);
        ActionCapacity_ += limit;

        Actions_.push_back(TActionEntry{
            .Action = &action,
            .Name = action.name(),
            .ActionId = actionIndex,
            .StatsIndex = actionIndex,
            .Limit = limit,
            .ParentActionId = std::nullopt,
            .SourceActionId = std::nullopt,
        });
        ActionIdByName_[action.name()] = actionIndex;
        ++actionIndex;
    }

    for (auto& actionEntry : Actions_) {
        const auto& action = *actionEntry.Action;

        if (action.has_parent_action() && !action.parent_action().empty()) {
            if (const auto parentIt = ActionIdByName_.find(action.parent_action()); parentIt != ActionIdByName_.end()) {
                actionEntry.ParentActionId = parentIt->second;
                ChildrenByActionId_[*actionEntry.ParentActionId].push_back(actionEntry.ActionId);
            }
        }

        if (action.has_action_data_mode()
            && action.action_data_mode().Mode_case() == NKikimrKeyValue::ActionDataMode::kFromPrevActions)
        {
            const auto& fromPrev = action.action_data_mode().from_prev_actions();
            if (fromPrev.action_name_size() > 0) {
                if (const auto sourceIt = ActionIdByName_.find(fromPrev.action_name(0)); sourceIt != ActionIdByName_.end()) {
                    actionEntry.SourceActionId = sourceIt->second;
                }
            }
        }
    }

    ActionPoolSize_ = GetActionPoolSize();
    ActionPool_ = std::make_unique<TActionPool>(ActionPoolSize_);

    if (WorkerLoadTracker_) {
        WorkerLoadTracker_->SetWorkerCapacity(WorkerId_, ActionPoolSize_);
    }

    if (static_cast<int>(Config_.partition_mode()) == 1 && Config_.volume_config().partition_count() > 0) {
        FixedPartitionId_ = WorkerId_ % Config_.volume_config().partition_count();
    }
}

TWorker::~TWorker() {
}

ui32 TWorker::GetActionLimit(const NKikimrKeyValue::Action& action) const {
    return action.has_worker_max_in_flight() && action.worker_max_in_flight() > 0
        ? action.worker_max_in_flight()
        : DefaultActionMaxInFlight;
}

ui32 TWorker::GetActionPoolSize() const {
    if (Options_.ActionPoolSize > 0) {
        return Options_.ActionPoolSize;
    }

    const ui32 hardwareThreads = std::max<ui32>(1, std::thread::hardware_concurrency());
    const ui32 desired = std::max<ui32>(1, hardwareThreads * 2);
    const ui32 capacity = std::max<ui32>(1, ActionCapacity_);
    return std::min(capacity, desired);
}

TWorker::TExecutionContextPtr TWorker::CreateExecutionContext(ui32 actionId, TExecutionContextPtr parentContext) {
    if (actionId >= Actions_.size()) {
        throw std::runtime_error(TStringBuilder() << "invalid action id " << actionId);
    }

    const auto& actionEntry = Actions_[actionId];
    const ui64 executionId = ExecutionIdCounter_.fetch_add(1, std::memory_order_relaxed);
    return std::make_shared<TExecutionContext>(
        executionId,
        actionEntry.ActionId,
        actionEntry.Name,
        std::move(parentContext),
        &WorkerDataStorage_);
}

TWorker::TExecutionContextPtr TWorker::FindNearestAncestorAction(
    const TExecutionContextPtr& context,
    ui32 actionId) const
{
    TExecutionContextPtr current = context ? context->Parent : nullptr;
    while (current) {
        if (current->ActionId == actionId) {
            return current;
        }
        current = current->Parent;
    }
    return nullptr;
}

TVector<std::pair<TString, TKeyInfo>> TWorker::PickSourceKeys(
    const TActionEntry& actionEntry,
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

    const auto& action = *actionEntry.Action;
    if (!action.has_action_data_mode()) {
        return pickFromWorker();
    }

    const auto& mode = action.action_data_mode();
    switch (mode.Mode_case()) {
        case NKikimrKeyValue::ActionDataMode::kWorker:
            return pickFromWorker();
        case NKikimrKeyValue::ActionDataMode::kFromPrevActions: {
            if (actionEntry.SourceActionId) {
                if (const TExecutionContextPtr sourceContext = FindNearestAncestorAction(context, *actionEntry.SourceActionId)) {
                    return sourceContext->PickKeys(count, erase);
                }
            }

            return pickFromWorker();
        }
        case NKikimrKeyValue::ActionDataMode::MODE_NOT_SET:
            return pickFromWorker();
    }

    return pickFromWorker();
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
        ActionPool_->Start();
        actionPoolStarted = true;

        for (const auto& actionEntry : Actions_) {
            const auto& action = *actionEntry.Action;
            if (actionEntry.ParentActionId) {
                continue;
            }

            if (action.has_period_us() && action.period_us() > 0) {
                Schedulers_.emplace_back([this, endAt, actionId = actionEntry.ActionId, period = action.period_us()] {
                    PeriodicLoop(actionId, period, endAt);
                });
            } else {
                ScheduleAction(actionEntry.ActionId);
            }
        }

        while (!IsStopped() && std::chrono::steady_clock::now() < endAt) {
            std::this_thread::sleep_for(100ms);
        }

        StopRequested_.store(true, std::memory_order_relaxed);
        StopSchedulers();
        WaitForActions();
        ActionPool_->Stop();
    } catch (const std::exception& e) {
        StopRequested_.store(true, std::memory_order_relaxed);
        StopSchedulers();
        if (actionPoolStarted) {
            WaitForActions();
            ActionPool_->Stop();
        }
        Stats_.RecordError("worker_exception", e.what());
    }
}

bool TWorker::IsStopped() const {
    if (StopRequested_.load(std::memory_order_relaxed)) {
        return true;
    }
    return StopSignal_ && *StopSignal_ != 0;
}

void TWorker::StopSchedulers() {
    for (ui32 actionId = 0; actionId < Actions_.size(); ++actionId) {
        ActionSlotSync_[actionId].Cv.notify_all();
    }

    for (auto& scheduler : Schedulers_) {
        if (scheduler.joinable()) {
            scheduler.join();
        }
    }
    Schedulers_.clear();
}

void TWorker::WaitForActions() {
    {
        std::unique_lock lock(ActiveActionsMutex_);
        const bool completed = ActiveActionsCv_.wait_for(lock, 30s, [this] {
            return ActiveActions_.load(std::memory_order_acquire) == 0;
        });
        if (!completed) {
            Stats_.RecordError("worker_shutdown_timeout", "waiting for active actions timed out");
        }
    }

    const bool queueCompleted = ActionPool_->WaitForIdle(30s);
    if (!queueCompleted) {
        Stats_.RecordError("worker_shutdown_timeout", "waiting for action queue to drain timed out");
    }
}

void TWorker::PeriodicLoop(ui32 actionId, ui32 periodUs, std::chrono::steady_clock::time_point endAt) {
    auto period = std::chrono::microseconds(std::max<ui32>(1, periodUs));
    auto nextRun = std::chrono::steady_clock::now();
    const auto maxSleepChunk = std::chrono::duration_cast<std::chrono::steady_clock::duration>(MaxNanoSleepChunk);

    while (!IsStopped() && nextRun < endAt) {
        while (!IsStopped()) {
            const EScheduleResult result = TryScheduleAction(actionId);
            if (result == EScheduleResult::Scheduled) {
                break;
            }

            if (result != EScheduleResult::LimitReached) {
                return;
            }

            if (!WaitForActionSlot(actionId, endAt)) {
                return;
            }
        }

        if (IsStopped()) {
            return;
        }

        nextRun += period;

        while (!IsStopped()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= nextRun) {
                break;
            }

            const auto remaining = nextRun - now;
            const auto sleepFor = std::min(remaining, maxSleepChunk);
            const auto sleepNs = std::chrono::duration_cast<std::chrono::nanoseconds>(sleepFor).count();
            if (sleepNs == 0) {
                break;
            }

            NanoSleep(static_cast<ui64>(sleepNs));
        }
    }
}

TWorker::EScheduleResult TWorker::TryScheduleAction(ui32 actionId, TExecutionContextPtr parentContext) {
    if (IsStopped()) {
        return EScheduleResult::Stopped;
    }

    if (actionId >= Actions_.size()) {
        Stats_.RecordError("invalid_action_id", TStringBuilder() << "invalid action id " << actionId);
        return EScheduleResult::Failed;
    }

    const auto& actionEntry = Actions_[actionId];
    const ui32 actionStatsIndex = actionEntry.StatsIndex;
    const ui32 limit = actionEntry.Limit;

    auto& running = RunningByAction_[actionId].Value;
    ui32 runningNow = running.load(std::memory_order_relaxed);
    while (true) {
        if (runningNow >= limit) {
            return EScheduleResult::LimitReached;
        }

        if (running.compare_exchange_weak(
                runningNow,
                runningNow + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            break;
        }
    }

    auto rollbackRunning = [this, actionId] {
        RunningByAction_[actionId].Value.fetch_sub(1, std::memory_order_acq_rel);
    };

    TExecutionContextPtr executionContext;
    try {
        executionContext = CreateExecutionContext(actionId, std::move(parentContext));
    } catch (const std::exception& e) {
        rollbackRunning();
        Stats_.RecordError("execution_context_create_failed", e.what(), actionStatsIndex);
        return EScheduleResult::Failed;
    }

    ActiveActions_.fetch_add(1, std::memory_order_acq_rel);

    try {
        if (WorkerLoadTracker_) {
            WorkerLoadTracker_->AddActive(WorkerId_, +1);
        }

        ActionPool_->Enqueue([this, actionId, executionContext, actionStatsIndex] {
            const auto actionStartedAt = std::chrono::steady_clock::now();
            try {
                ExecuteActionAsync(actionId, actionStatsIndex, executionContext, actionStartedAt);
            } catch (const std::exception& e) {
                Stats_.RecordError("action_exception", e.what(), actionStatsIndex);
                auto state = std::make_shared<TActionExecutionState>();
                state->ActionId = actionId;
                state->ActionStatsIndex = actionStatsIndex;
                state->ExecutionContext = executionContext;
                state->StartedAt = actionStartedAt;
                FinishAction(state, false);
            } catch (...) {
                Stats_.RecordError("action_exception", "unknown exception", actionStatsIndex);
                auto state = std::make_shared<TActionExecutionState>();
                state->ActionId = actionId;
                state->ActionStatsIndex = actionStatsIndex;
                state->ExecutionContext = executionContext;
                state->StartedAt = actionStartedAt;
                FinishAction(state, false);
            }
        });
    } catch (const std::exception& e) {
        if (WorkerLoadTracker_) {
            WorkerLoadTracker_->AddActive(WorkerId_, -1);
        }
        DecreaseActiveActions();
        rollbackRunning();
        Stats_.RecordError("action_enqueue_failed", e.what(), actionStatsIndex);
        return EScheduleResult::Failed;
    }

    return EScheduleResult::Scheduled;
}

void TWorker::ScheduleAction(ui32 actionId, TExecutionContextPtr parentContext) {
    (void)TryScheduleAction(actionId, std::move(parentContext));
}

bool TWorker::WaitForActionSlot(ui32 actionId, std::chrono::steady_clock::time_point endAt) {
    if (actionId >= Actions_.size()) {
        return false;
    }

    TActionSlotSync& slotSync = ActionSlotSync_[actionId];
    std::unique_lock lock(slotSync.Mutex);
    slotSync.Waiter.store(true, std::memory_order_release);

    const bool hasSlot = slotSync.Cv.wait_until(lock, endAt, [this, actionId] {
        if (IsStopped()) {
            return true;
        }
        const ui32 running = RunningByAction_[actionId].Value.load(std::memory_order_acquire);
        return running < Actions_[actionId].Limit;
    });

    slotSync.Waiter.store(false, std::memory_order_release);
    if (IsStopped()) {
        return false;
    }
    return hasSlot;
}

void TWorker::ExecuteActionAsync(
    ui32 actionId,
    ui32 actionStatsIndex,
    TExecutionContextPtr executionContext,
    std::chrono::steady_clock::time_point startedAt)
{
    auto state = std::make_shared<TActionExecutionState>();
    state->ActionId = actionId;
    state->ActionStatsIndex = actionStatsIndex;
    state->ExecutionContext = std::move(executionContext);
    state->StartedAt = startedAt;
    ContinueAction(state);
}

void TWorker::ContinueAction(const std::shared_ptr<TActionExecutionState>& state) {
    if (!state || state->Finished) {
        return;
    }

    if (state->ActionId >= Actions_.size()) {
        Stats_.RecordError("invalid_action_id", TStringBuilder() << "invalid action id " << state->ActionId, state->ActionStatsIndex);
        FinishAction(state, false);
        return;
    }

    const auto& actionEntry = Actions_[state->ActionId];
    const auto& action = *actionEntry.Action;
    const TString& actionName = actionEntry.Name;

    try {
        if (state->NextCommandIndex >= static_cast<size_t>(action.action_command_size())) {
            FinishAction(state, true);
            return;
        }

        const auto commandIndex = state->NextCommandIndex;
        ++state->NextCommandIndex;
        const auto& command = action.action_command(static_cast<int>(commandIndex));

        switch (command.Command_case()) {
            case NKikimrKeyValue::ActionCommand::kPrint: {
                Cerr << "[worker=" << WorkerId_ << "][action=" << actionName << "] " << command.print().msg() << Endl;
                ContinueAction(state);
                return;
            }
            case NKikimrKeyValue::ActionCommand::kRead: {
                ExecuteReadCommandAsync(
                    actionEntry,
                    state->ActionStatsIndex,
                    state->ExecutionContext,
                    command.read(),
                    [this, state] {
                        ContinueAction(state);
                    });
                return;
            }
            case NKikimrKeyValue::ActionCommand::kWrite: {
                ExecuteWriteCommandAsync(
                    actionName,
                    command.write(),
                    state->ActionStatsIndex,
                    state->ExecutionContext,
                    [this, state] {
                        ContinueAction(state);
                    });
                return;
            }
            case NKikimrKeyValue::ActionCommand::kDelete: {
                ExecuteDeleteCommandAsync(
                    actionEntry,
                    state->ActionStatsIndex,
                    state->ExecutionContext,
                    command.delete_(),
                    [this, state] {
                        ContinueAction(state);
                    });
                return;
            }
            case NKikimrKeyValue::ActionCommand::COMMAND_NOT_SET: {
                Stats_.RecordError(
                    "empty_command",
                    TStringBuilder() << "Action " << actionName << " has empty command",
                    state->ActionStatsIndex);
                ContinueAction(state);
                return;
            }
        }
    } catch (const std::exception& e) {
        Stats_.RecordError("action_exception", e.what(), state->ActionStatsIndex);
        FinishAction(state, false);
        return;
    } catch (...) {
        Stats_.RecordError("action_exception", "unknown exception", state->ActionStatsIndex);
        FinishAction(state, false);
        return;
    }

    Stats_.RecordError(
        "action_exception",
        TStringBuilder() << "unknown command type in action " << actionName,
        state->ActionStatsIndex);
    FinishAction(state, false);
}

void TWorker::FinishAction(const std::shared_ptr<TActionExecutionState>& state, bool recordAction) {
    if (!state || state->Finished) {
        return;
    }
    state->Finished = true;

    if (recordAction) {
        Stats_.RecordAction(state->ActionStatsIndex);

        if (!IsStopped() && state->ActionId < ChildrenByActionId_.size()) {
            for (ui32 childActionId : ChildrenByActionId_[state->ActionId]) {
                ScheduleAction(childActionId, state->ExecutionContext);
            }
        }
    }

    Stats_.RecordLatency(state->ActionStatsIndex, ToLatencyMs(std::chrono::steady_clock::now() - state->StartedAt));

    if (state->ActionId < Actions_.size()) {
        RunningByAction_[state->ActionId].Value.fetch_sub(1, std::memory_order_acq_rel);
        if (ActionSlotSync_[state->ActionId].Waiter.load(std::memory_order_acquire)) {
            ActionSlotSync_[state->ActionId].Cv.notify_one();
        }
    }

    if (WorkerLoadTracker_) {
        WorkerLoadTracker_->AddActive(WorkerId_, -1);
    }

    DecreaseActiveActions();
}

void TWorker::DecreaseActiveActions() {
    if (ActiveActions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        std::lock_guard lock(ActiveActionsMutex_);
        ActiveActionsCv_.notify_all();
    }
}

void TWorker::WriteInitialDataImpl() {
    for (const auto& writeCommand : Config_.initial_data().write_commands()) {
        if (IsStopped()) {
            break;
        }

        const bool success = ExecuteWriteCommand("__initial__", writeCommand, std::nullopt, nullptr);
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
    const bool ok = Client_->Write(VolumePath_, partitionId, pairs, writeCommand.channel(), &error);

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

    if (executionContext) {
        executionContext->AddKeys(writtenKeys);
    } else {
        WorkerDataStorage_.AddKeys(writtenKeys);
    }

    if (actionStatsIndex) {
        Stats_.RecordWriteBytes(*actionStatsIndex, bytesWritten);
    }

    if (Options_.Verbose) {
        Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << pairs.size() << Endl;
    }

    return true;
}

void TWorker::ExecuteWriteCommandAsync(
    const TString& actionName,
    const NKikimrKeyValue::ActionCommand_Write& writeCommand,
    std::optional<ui32> actionStatsIndex,
    const TExecutionContextPtr& executionContext,
    TCommandDone done)
{
    if (writeCommand.count() == 0) {
        done();
        return;
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

    ui64 bytesWritten = 0;
    TVector<std::pair<TString, TKeyInfo>> writtenKeys;
    writtenKeys.reserve(pairs.size());

    for (const auto& [key, value] : pairs) {
        const TKeyInfo keyInfo{partitionId, static_cast<ui32>(value.size())};
        writtenKeys.emplace_back(key, keyInfo);
        bytesWritten += value.size();
    }

    Client_->WriteAsync(
        VolumePath_,
        partitionId,
        pairs,
        writeCommand.channel(),
        [this,
         actionName,
         actionStatsIndex,
         executionContext,
         bytesWritten,
         writtenKeys = std::move(writtenKeys),
         done = std::move(done)](bool ok, TString error) mutable {
            try {
                if (!ok) {
                    Stats_.RecordError("write_failed", error, actionStatsIndex);
                    done();
                    return;
                }

                if (executionContext) {
                    executionContext->AddKeys(writtenKeys);
                } else {
                    WorkerDataStorage_.AddKeys(writtenKeys);
                }

                if (actionStatsIndex) {
                    Stats_.RecordWriteBytes(*actionStatsIndex, bytesWritten);
                }

                if (Options_.Verbose) {
                    Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << writtenKeys.size() << Endl;
                }

                done();
            } catch (const std::exception& e) {
                Stats_.RecordError("write_callback_exception", e.what(), actionStatsIndex);
                done();
            } catch (...) {
                Stats_.RecordError("write_callback_exception", "unknown exception", actionStatsIndex);
                done();
            }
        });
}

void TWorker::ExecuteReadCommandAsync(
    const TActionEntry& actionEntry,
    ui32 actionStatsIndex,
    const TExecutionContextPtr& executionContext,
    const NKikimrKeyValue::ActionCommand_Read& readCommand,
    TCommandDone done)
{
    if (readCommand.count() == 0) {
        done();
        return;
    }

    const auto keys = PickSourceKeys(actionEntry, executionContext, readCommand.count(), false);
    if (keys.empty()) {
        done();
        return;
    }

    auto state = std::make_shared<TReadCommandState>();
    state->ActionStatsIndex = actionStatsIndex;
    state->ReadSize = readCommand.size();
    state->VerifyData = readCommand.verify_data();
    state->Keys = keys;
    state->Done = std::move(done);

    ContinueReadCommand(state);
}

void TWorker::ContinueReadCommand(const std::shared_ptr<TReadCommandState>& state) {
    if (!state) {
        return;
    }

    if (state->NextKeyIndex >= state->Keys.size() || IsStopped()) {
        state->Done();
        return;
    }

    const auto& [key, info] = state->Keys[state->NextKeyIndex];
    const ui32 maxOffset = info.KeySize > state->ReadSize ? info.KeySize - state->ReadSize : 0;
    ui32 offset = 0;
    if (maxOffset > 0) {
        std::uniform_int_distribution<ui32> distribution(0, maxOffset);
        offset = distribution(RandomEngine());
    }

    Client_->ReadAsync(
        VolumePath_,
        info.PartitionId,
        key,
        offset,
        state->ReadSize,
        [this, state, key, info, offset](bool ok, TString value, TString error) mutable {
            try {
                if (!ok) {
                    Stats_.RecordError("read_failed", error, state->ActionStatsIndex);
                } else {
                    Stats_.RecordReadBytes(state->ActionStatsIndex, value.size());

                    if (state->VerifyData) {
                        TString expected = GetPatternData(info.KeySize);
                        expected = expected.substr(offset, state->ReadSize);
                        if (value != expected) {
                            Stats_.RecordError(
                                "verify_failed",
                                TStringBuilder()
                                    << "key=" << key
                                    << " partition=" << info.PartitionId
                                    << " offset=" << offset
                                    << " requested_size=" << state->ReadSize
                                    << " expected_len=" << expected.size()
                                    << " actual_len=" << value.size(),
                                state->ActionStatsIndex);
                        }
                    }
                }

                ++state->NextKeyIndex;
                ContinueReadCommand(state);
            } catch (const std::exception& e) {
                Stats_.RecordError("read_callback_exception", e.what(), state->ActionStatsIndex);
                ++state->NextKeyIndex;
                ContinueReadCommand(state);
            } catch (...) {
                Stats_.RecordError("read_callback_exception", "unknown exception", state->ActionStatsIndex);
                ++state->NextKeyIndex;
                ContinueReadCommand(state);
            }
        });
}

void TWorker::ExecuteDeleteCommandAsync(
    const TActionEntry& actionEntry,
    ui32 actionStatsIndex,
    const TExecutionContextPtr& executionContext,
    const NKikimrKeyValue::ActionCommand_Delete& deleteCommand,
    TCommandDone done)
{
    if (deleteCommand.count() == 0) {
        done();
        return;
    }

    auto state = std::make_shared<TDeleteCommandState>();
    state->ActionStatsIndex = actionStatsIndex;
    state->Keys = PickSourceKeys(actionEntry, executionContext, deleteCommand.count(), true);
    state->Done = std::move(done);

    if (state->Keys.empty()) {
        state->Done();
        return;
    }

    ContinueDeleteCommand(state);
}

void TWorker::ContinueDeleteCommand(const std::shared_ptr<TDeleteCommandState>& state) {
    if (!state) {
        return;
    }

    if (state->NextKeyIndex >= state->Keys.size() || IsStopped()) {
        state->Done();
        return;
    }

    const auto& [key, info] = state->Keys[state->NextKeyIndex];
    Client_->DeleteKeyAsync(
        VolumePath_,
        info.PartitionId,
        key,
        [this, state](bool ok, TString error) mutable {
            try {
                if (!ok) {
                    Stats_.RecordError(
                        "delete_failed",
                        error,
                        state->ActionStatsIndex);
                }

                ++state->NextKeyIndex;
                ContinueDeleteCommand(state);
            } catch (const std::exception& e) {
                Stats_.RecordError("delete_callback_exception", e.what(), state->ActionStatsIndex);
                ++state->NextKeyIndex;
                ContinueDeleteCommand(state);
            } catch (...) {
                Stats_.RecordError("delete_callback_exception", "unknown exception", state->ActionStatsIndex);
                ++state->NextKeyIndex;
                ContinueDeleteCommand(state);
            }
        });
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
