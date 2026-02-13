#include "worker.h"

#include "utils.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <exception>
#include <limits>
#include <random>

namespace NKvVolumeStress {

using namespace std::chrono_literals;

TWorker::TWorker(
    ui32 workerId,
    const TOptions& options,
    const NKikimrKeyValue::KeyValueVolumeStressLoad& config,
    const TString& hostPort,
    const TString& volumePath,
    TRunStats& stats)
    : WorkerId_(workerId)
    , Options_(options)
    , Config_(config)
    , VolumePath_(volumePath)
    , Stats_(stats)
    , Client_(hostPort)
    , EndAt_(std::chrono::steady_clock::now() + std::chrono::seconds(options.Duration))
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

void TWorker::Run() {
    try {
        WriteInitialData();

        for (const auto& action : Config_.actions()) {
            if (action.has_parent_action() && !action.parent_action().empty()) {
                continue;
            }

            if (action.has_period_us() && action.period_us() > 0) {
                Schedulers_.emplace_back([this, name = action.name(), period = action.period_us()] {
                    PeriodicLoop(name, period);
                });
            } else {
                ScheduleAction(action.name());
            }
        }

        while (std::chrono::steady_clock::now() < EndAt_) {
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

void TWorker::PeriodicLoop(const TString& actionName, ui32 periodUs) {
    auto period = std::chrono::microseconds(std::max<ui32>(1, periodUs));
    auto nextRun = std::chrono::steady_clock::now();

    while (!IsStopped() && nextRun < EndAt_) {
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
        : std::numeric_limits<ui32>::max();

    {
        std::lock_guard lock(RunningByActionMutex_);
        ui32& running = RunningByAction_[actionName];
        if (running >= limit) {
            return;
        }
        ++running;
    }

    ActiveActions_.fetch_add(1, std::memory_order_relaxed);

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
                ExecuteWriteCommand(actionName, command.write());
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

void TWorker::WriteInitialData() {
    for (const auto& writeCommand : Config_.initial_data().write_commands()) {
        ExecuteWriteCommand("__initial__", writeCommand);
    }
}

void TWorker::ExecuteWriteCommand(const TString& actionName, const NKikimrKeyValue::ActionCommand_Write& writeCommand) {
    if (writeCommand.count() == 0) {
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

    TString error;
    if (!Client_.Write(VolumePath_, partitionId, pairs, writeCommand.channel(), &error)) {
        Stats_.RecordError("write_failed", error);
        return;
    }

    for (const auto& [key, value] : pairs) {
        DataStorage_.AddKey(actionName, key, TKeyInfo{partitionId, static_cast<ui32>(value.size())});
    }

    if (Options_.Verbose) {
        Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << pairs.size() << Endl;
    }
}

void TWorker::ExecuteReadCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand_Read& readCommand) {
    if (readCommand.count() == 0) {
        return;
    }

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
        if (!Client_.Read(VolumePath_, info.PartitionId, key, offset, readCommand.size(), &value, &error)) {
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

    const TVector<TString> sources = ResolveSources(action);
    const auto keys = DataStorage_.PickKeys(sources, deleteCommand.count(), true);

    for (const auto& [key, info] : keys) {
        TString error;
        if (!Client_.DeleteKey(VolumePath_, info.PartitionId, key, &error)) {
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
