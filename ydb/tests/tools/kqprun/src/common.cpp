#include "common.h"


namespace NKqpRun {

NKikimrServices::EServiceKikimr GetLogService(const TString& serviceName) {
    NKikimrServices::EServiceKikimr service;
    if (!NKikimrServices::EServiceKikimr_Parse(serviceName, &service)) {
        ythrow yexception() << "Invalid kikimr service name " << serviceName;
    }
    return service;
}

void ModifyLogPriorities(std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> logPriorities, NKikimrConfig::TLogConfig& logConfig) {
    for (auto& entry : *logConfig.MutableEntry()) {
        const auto it = logPriorities.find(GetLogService(entry.GetComponent()));
        if (it != logPriorities.end()) {
            entry.SetLevel(it->second);
            logPriorities.erase(it);
        }
    }
    for (const auto& [service, priority] : logPriorities) {
        auto* entry = logConfig.AddEntry();
        entry->SetComponent(NKikimrServices::EServiceKikimr_Name(service));
        entry->SetLevel(priority);
    }
}

}  // namespace NKqpRun
