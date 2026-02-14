#include "worker.h"

#include "utils.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <exception>
#include <random>
#include <system_error>

namespace NKvVolumeStress {

using namespace std::chrono_literals;

namespace {

constexpr ui32 DefaultActionMaxInFlight = 30;
const TString InitialActionName = "__initial__";

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
    TInitialLoadProgress* initialLoadProgress)
    : WorkerId_(workerId)
    , Options_(options)
    , Config_(config)
    , VolumePath_(volumePath)
    , Stats_(stats)
    , InitialLoadProgress_(initialLoadProgress)
    , Client_(MakeKeyValueClient(hostPort, options.Version))
{
    for (const auto& action : Config_.actions()) {
        ActionsByName_[action.name()] = &action;
        if (action.has_parent_action() && !action.parent_action().empty()) {
            ChildrenByParent_[action.parent_action()].push_back(action.name());
        }
    }

    if (static_cast<int>(Config_.partition_mode()) == 1 && Config_.volume_config().partition_count() > 0) {
        FixedPartitionId_ = WorkerId_ % Config_.volume_config().partition_count();
    }
}

void TWorker::LoadInitialData() {
    try {
        WriteInitialDataImpl();
    } catch (const std::exception& e) {
        Stats_.RecordError("worker_exception", e.what());
    }
}

void TWorker::Run(std::chrono::steady_clock::time_point endAt) {
    try {
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

        while (std::chrono::steady_clock::now() < endAt) {
            std::this_thread::sleep_for(100ms);
        }

        StopRequested_.store(true);

        for (auto& scheduler : Schedulers_) {
            scheduler.join();
        }

        WaitForActions();
    } catch (const std::exception& e) {
        Stats_.RecordError("worker_exception", e.what());
    }
}

bool TWorker::IsStopped() const {
    return StopRequested_.load(std::memory_order_relaxed);
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

void TWorker::ScheduleAction(const TString& actionName) {
    if (IsStopped()) {
        return;
    }

    const auto actionIt = ActionsByName_.find(actionName);
    if (actionIt == ActionsByName_.end()) {
        Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
        return;
    }

    const auto* action = actionIt->second;
    const ui32 limit = action->has_worker_max_in_flight() && action->worker_max_in_flight() > 0
        ? action->worker_max_in_flight()
        : DefaultActionMaxInFlight;

    {
        std::lock_guard lock(RunningByActionMutex_);
        ui32& running = RunningByAction_[actionName];
        if (running >= limit) {
            return;
        }
        ++running;
    }

    ActiveActions_.fetch_add(1, std::memory_order_relaxed);

    try {
        std::thread([this, actionName] {
            ExecuteAction(actionName);

            {
                std::lock_guard lock(RunningByActionMutex_);
                auto it = RunningByAction_.find(actionName);
                if (it != RunningByAction_.end() && it->second) {
                    --it->second;
                }
            }

            if (ActiveActions_.fetch_sub(1, std::memory_order_relaxed) == 1) {
                std::lock_guard lock(ActiveActionsMutex_);
                ActiveActionsCv_.notify_all();
            }
        }).detach();
    } catch (const std::system_error& e) {
        {
            std::lock_guard lock(RunningByActionMutex_);
            auto it = RunningByAction_.find(actionName);
            if (it != RunningByAction_.end() && it->second) {
                --it->second;
            }
        }

        if (ActiveActions_.fetch_sub(1, std::memory_order_relaxed) == 1) {
            std::lock_guard lock(ActiveActionsMutex_);
            ActiveActionsCv_.notify_all();
        }

        Stats_.RecordError("thread_create_failed", e.what());
    }
}

void TWorker::ExecuteAction(const TString& actionName) {
    const auto actionIt = ActionsByName_.find(actionName);
    if (actionIt == ActionsByName_.end()) {
        Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
        return;
    }

    const auto& action = *actionIt->second;

    for (const auto& command : action.action_command()) {
        switch (command.Command_case()) {
            case NKikimrKeyValue::ActionCommand::kPrint: {
                Cerr << "[worker=" << WorkerId_ << "][action=" << actionName << "] " << command.print().msg() << Endl;
                break;
            }
            case NKikimrKeyValue::ActionCommand::kRead: {
                ExecuteReadCommand(action, command.read());
                break;
            }
            case NKikimrKeyValue::ActionCommand::kWrite: {
                (void)ExecuteWriteCommand(actionName, command.write());
                break;
            }
            case NKikimrKeyValue::ActionCommand::kDelete: {
                ExecuteDeleteCommand(action, command.delete_());
                break;
            }
            case NKikimrKeyValue::ActionCommand::COMMAND_NOT_SET: {
                Stats_.RecordError("empty_command", TStringBuilder() << "Action " << actionName << " has empty command");
                break;
            }
        }
    }

    Stats_.RecordAction(actionName);

    if (!IsStopped()) {
        const auto childrenIt = ChildrenByParent_.find(actionName);
        if (childrenIt != ChildrenByParent_.end()) {
            for (const TString& childName : childrenIt->second) {
                ScheduleAction(childName);
            }
        }
    }
}

void TWorker::WriteInitialDataImpl() {
    for (const auto& writeCommand : Config_.initial_data().write_commands()) {
        const bool success = ExecuteWriteCommand("__initial__", writeCommand);
        if (InitialLoadProgress_) {
            const ui64 bytes = static_cast<ui64>(writeCommand.size()) * writeCommand.count();
            InitialLoadProgress_->OnCommandFinished(bytes, success);
        }
    }
}

bool TWorker::ExecuteWriteCommand(const TString& actionName, const NKikimrKeyValue::ActionCommand_Write& writeCommand) {
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

    if (actionName != InitialActionName) {
        Stats_.RecordLatency(actionName, latencyMs);
    }

    if (!ok) {
        Stats_.RecordError("write_failed", error);
        return false;
    }

    for (const auto& [key, value] : pairs) {
        DataStorage_.AddKey(actionName, key, TKeyInfo{partitionId, static_cast<ui32>(value.size())});
    }

    if (Options_.Verbose) {
        Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << pairs.size() << Endl;
    }

    return true;
}

void TWorker::ExecuteReadCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand_Read& readCommand) {
    if (readCommand.count() == 0) {
        return;
    }

    const TString actionName = action.name();
    const TVector<TString> sources = ResolveSources(action);
    const auto keys = DataStorage_.PickKeys(sources, readCommand.count(), false);

    for (const auto& [key, info] : keys) {
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
        Stats_.RecordLatency(actionName, latencyMs);

        if (!ok) {
            Stats_.RecordError("read_failed", error);
            continue;
        }

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
                        << " actual_len=" << value.size());
            }
        }
    }
}

void TWorker::ExecuteDeleteCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand_Delete& deleteCommand) {
    if (deleteCommand.count() == 0) {
        return;
    }

    const TString actionName = action.name();
    const TVector<TString> sources = ResolveSources(action);
    const auto keys = DataStorage_.PickKeys(sources, deleteCommand.count(), true);

    for (const auto& [key, info] : keys) {
        TString error;
        const auto startedAt = std::chrono::steady_clock::now();
        const bool ok = Client_->DeleteKey(VolumePath_, info.PartitionId, key, &error);
        const ui64 latencyMs = ToLatencyMs(std::chrono::steady_clock::now() - startedAt);
        Stats_.RecordLatency(actionName, latencyMs);

        if (!ok) {
            Stats_.RecordError("delete_failed", error);
        }
    }
}

TVector<TString> TWorker::ResolveSources(const NKikimrKeyValue::Action& action) const {
    if (!action.has_action_data_mode()) {
        return {"__initial__"};
    }

    const auto& mode = action.action_data_mode();
    switch (mode.Mode_case()) {
        case NKikimrKeyValue::ActionDataMode::kWorker:
            return {"__initial__"};
        case NKikimrKeyValue::ActionDataMode::kFromPrevActions: {
            TVector<TString> names;
            for (const auto& name : mode.from_prev_actions().action_name()) {
                names.push_back(name);
            }
            return names;
        }
        case NKikimrKeyValue::ActionDataMode::MODE_NOT_SET:
            return {"__initial__"};
    }

    return {"__initial__"};
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
