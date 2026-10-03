#include "category.h"

#include <algorithm>
#include <ranges>

namespace NKikimr::NConveyorComposite {

TProcessCategory::TProcessCategory(const NConfig::TCategory& config, TCounters& counters)
    : Category(config.GetCategory()) {
    Counters = counters.GetCategorySignals(Category);
    RegisterProcess(0, RegisterScope("DEFAULT", TCPULimitsConfig(1000, 1000)), kServiceQueryIdentity);
    Counters->WaitingQueueSizeLimit->Set(config.GetQueueSizeLimit());
}

TProcessCategory::~TProcessCategory() {
    Y_UNUSED(UnregisterProcess(0));
}

void TProcessCategory::RegisterProcess(const ui64 internalProcessId, std::shared_ptr<TProcessScope>&& scope,
    const TSchedulerQueryIdentity& schedulerQueryIdentity) {
    scope->IncProcesses();
    AFL_VERIFY(Processes.emplace(internalProcessId,
        std::make_shared<TProcess>(internalProcessId, std::move(scope), WaitingTasksCount, schedulerQueryIdentity)).second);
    AFL_VERIFY(ProcessesByIdentity[schedulerQueryIdentity].insert(internalProcessId).second);
}

TSchedulerQueryIdentity TProcessCategory::UnregisterProcess(const ui64 processId) {
    auto it = Processes.find(processId);
    AFL_VERIFY(it != Processes.end());
    if (const auto tasksCount = it->second->GetTasksCount()) {
        AFL_WARN(NKikimrServices::TX_CONVEYOR)
        ("event", "unregister_process_with_queued_tasks")("process_id", processId)("category", ::ToString(Category))("tasks_count", tasksCount);
    }
    const auto identity = it->second->GetSchedulerQueryIdentity();
    auto identityIt = ProcessesByIdentity.find(identity);
    AFL_VERIFY(identityIt != ProcessesByIdentity.end());
    AFL_VERIFY(identityIt->second.erase(processId) == 1);
    if (identityIt->second.empty()) {
        ProcessesByIdentity.erase(identityIt);
    }
    Y_UNUSED(RemoveWeightedProcess(it->second));
    if (it->second->GetScope()->DecProcesses()) {
        AFL_VERIFY(Scopes.erase(it->second->GetScope()->GetScopeId()));
    }
    Processes.erase(it);
    return identity;
}

bool TProcessCategory::HasTasks() const {
    return WeightedProcesses.size();
}

ui64 TProcessCategory::MoveProcessesToService(const TSchedulerQueryIdentity& identity) {
    AFL_VERIFY(!identity.IsServiceQuery);
    auto it = ProcessesByIdentity.find(identity);
    if (it == ProcessesByIdentity.end()) {
        return 0;
    }
    auto processIds = std::move(it->second);
    ProcessesByIdentity.erase(it);
    auto& serviceIds = ProcessesByIdentity[kServiceQueryIdentity];
    for (const auto processId : processIds) {
        Processes.at(processId)->MoveToServiceQuery();
        AFL_VERIFY(serviceIds.insert(processId).second);
    }
    return processIds.size();
}

bool TProcessCategory::HasTasks(const TSchedulerQueryIdentity& identity) const {
    if (WeightedProcesses.empty()) {
        return false;
    }
    const auto& first = WeightedProcesses.begin()->second.front();
    if (first->GetSchedulerQueryIdentity() == identity && first->GetScope()->CheckToRun()) {
        return true;
    }
    if (ProcessesByIdentity.size() == 1) {
        return first->GetSchedulerQueryIdentity() == identity &&
            std::ranges::any_of(WeightedProcesses | std::views::values | std::views::join,
                [](const auto& process) { return process->GetScope()->CheckToRun(); });
    }
    const auto it = ProcessesByIdentity.find(identity);
    return it != ProcessesByIdentity.end() && std::ranges::any_of(it->second, [&](ui64 id) {
        const auto& process = Processes.at(id);
        return process->GetTasksCount() && process->GetScope()->CheckToRun();
    });
}

bool TProcessCategory::HasProcesses(const TSchedulerQueryIdentity& identity) const {
    return ProcessesByIdentity.contains(identity);
}

std::optional<TDuration> TProcessCategory::GetMinProcessUsage(const TSchedulerQueryIdentity& identity, const ui64 workerIdx,
    const std::vector<NConfig::THeavyLimit>& heavyLimits) const {
    auto ordered = WeightedProcesses | std::views::values | std::views::join;
    auto current = ordered.begin();
    const auto end = ordered.end();
    if (current == end) {
        return std::nullopt;
    }
    const auto canRun = [&](const auto& process) {
        return process->CanRunOnWorker(workerIdx, heavyLimits) && process->GetScope()->CheckToRun();
    };
    const auto& first = *current++;
    if (first->GetSchedulerQueryIdentity() == identity && canRun(first)) {
        return first->GetWeightedUsage();
    }
    if (ProcessesByIdentity.size() == 1) {
        if (first->GetSchedulerQueryIdentity() != identity) {
            return std::nullopt;
        }
        const auto match = std::ranges::find_if(current, end, canRun);
        return match == end ? std::nullopt : std::make_optional((*match)->GetWeightedUsage());
    }
    const auto it = ProcessesByIdentity.find(identity);
    if (it == ProcessesByIdentity.end()) {
        return std::nullopt;
    }
    std::optional<TDuration> minimum;
    for (const auto id : it->second) {
        if (current == end) {
            return std::nullopt;
        }
        const auto& candidate = *current++;
        if (candidate->GetSchedulerQueryIdentity() == identity && canRun(candidate)) {
            return candidate->GetWeightedUsage();
        }
        const auto& process = Processes.at(id);
        if (process->GetTasksCount() && canRun(process)) {
            const auto usage = process->GetWeightedUsage();
            if (!minimum || usage < *minimum) {
                minimum = usage;
            }
        }
    }
    return minimum;
}

void TProcessCategory::ApplyConfig(const NConfig::TCategory& config) {
    Y_ENSURE(config.GetCategory() == Category, "category config type mismatch");
    Counters->WaitingQueueSizeLimit->Set(config.GetQueueSizeLimit());
}

std::optional<TWorkerTask> TProcessCategory::ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& counters,
    THashSet<TString>& scopeIds, const TSchedulerQueryIdentity& identity,
    const ui64 workerIdx, const std::vector<NConfig::THeavyLimit>& heavyLimits) {
    std::shared_ptr<TProcess> pMin;
    for (auto it = WeightedProcesses.begin(); it != WeightedProcesses.end(); ++it) {
        for (ui32 i = 0; i < it->second.size(); ++i) {
            if (it->second[i]->GetSchedulerQueryIdentity() != identity ||
                !it->second[i]->CanRunOnWorker(workerIdx, heavyLimits) || !it->second[i]->GetScope()->CheckToRun()) {
                continue;
            }
            pMin = it->second[i];
            std::swap(it->second[i], it->second.back());
            it->second.pop_back();
            if (it->second.empty()) {
                WeightedProcesses.erase(it);
            }
            break;
        }
        if (pMin) {
            break;
        }
    }
    if (!pMin) {
        return std::nullopt;
    }
    auto result = pMin->ExtractTaskWithPrediction(counters);
    if (pMin->GetTasksCount()) {
        WeightedProcesses[pMin->GetWeightedUsage()].emplace_back(pMin);
    }
    if (scopeIds.emplace(pMin->GetScope()->GetScopeId()).second) {
        pMin->GetScope()->IncInFlight();
    }
    Counters->WaitingQueueSize->Set(WaitingTasksCount->Val());
    return result;
}

TProcessScope& TProcessCategory::MutableProcessScope(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    AFL_VERIFY(it != Scopes.end())("cat", GetCategory())("scope", scopeName);
    return *it->second;
}

std::shared_ptr<TProcessScope> TProcessCategory::GetProcessScopePtrVerified(const TString& scopeName) const {
    auto it = Scopes.find(scopeName);
    AFL_VERIFY(it != Scopes.end());
    return it->second;
}

TProcessScope* TProcessCategory::MutableProcessScopeOptional(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    if (it != Scopes.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

std::shared_ptr<TProcessScope> TProcessCategory::RegisterScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    TCPUGroup::TPtr cpuGroup = std::make_shared<TCPUGroup>(processCpuLimits.GetCPUGroupThreadsLimitDef(256));
    auto info = Scopes.emplace(scopeId, std::make_shared<TProcessScope>(scopeId, std::move(cpuGroup), CPUUsage));
    AFL_VERIFY(info.second);
    return info.first->second;
}

std::shared_ptr<TProcessScope> TProcessCategory::UpdateScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    auto scope = GetProcessScopePtrVerified(scopeId);
    scope->UpdateLimits(processCpuLimits);
    return scope;
}

std::shared_ptr<TProcessScope> TProcessCategory::UpsertScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    if (Scopes.contains(scopeId)) {
        return UpdateScope(scopeId, processCpuLimits);
    } else {
        return RegisterScope(scopeId, processCpuLimits);
    }
}

void TProcessCategory::UnregisterScope(const TString& name) {
    auto it = Scopes.find(name);
    AFL_VERIFY(it != Scopes.end());
    Scopes.erase(it);
}

void TProcessCategory::PutTaskResult(TWorkerTaskResult&& result, THashSet<TString>& scopeIds) {
    const ui64 internalProcessId = result.GetProcessId();
    auto it = Processes.find(internalProcessId);
    if (scopeIds.emplace(result.GetScope()->GetScopeId()).second) {
        result.GetScope()->DecInFlight();
    }
    if (it == Processes.end()) {
        return;
    }
    Y_UNUSED(RemoveWeightedProcess(it->second));
    it->second->PutTaskResult(std::move(result));
    if (it->second->GetTasksCount()) {
        WeightedProcesses[it->second->GetWeightedUsage()].emplace_back(it->second);
    }
}

bool TProcessCategory::RemoveWeightedProcess(const std::shared_ptr<TProcess>& process) {
    if (!process->GetTasksCount()) {
        return false;
    }
    AFL_VERIFY(WeightedProcesses.size());
    auto itW = WeightedProcesses.find(process->GetWeightedUsage());
    AFL_VERIFY(itW != WeightedProcesses.end())("weight", process->GetWeightedUsage().GetValue())("size", WeightedProcesses.size())(
                        "first", WeightedProcesses.begin()->first.GetValue());
    for (ui32 i = 0; i < itW->second.size(); ++i) {
        if (itW->second[i]->GetProcessId() != process->GetProcessId()) {
            continue;
        }
        itW->second[i] = itW->second.back();
        if (itW->second.size() == 1) {
            WeightedProcesses.erase(itW);
        } else {
            itW->second.pop_back();
        }
        return true;
    }
    AFL_VERIFY(false);
    return false;
}

}   // namespace NKikimr::NConveyorComposite
