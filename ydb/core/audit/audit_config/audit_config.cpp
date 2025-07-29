#include "audit_config.h"

namespace NKikimr::NAudit {

namespace {

NACLibProto::ESubjectType ToSubjectType(NKikimrConfig::TAuditConfig::TLogClassConfig::EAccountType accountType) {
    switch (accountType) {
    case NKikimrConfig::TAuditConfig::TLogClassConfig::Anonymous:
        return NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    case NKikimrConfig::TAuditConfig::TLogClassConfig::User:
        return NACLibProto::SUBJECT_TYPE_USER;
    case NKikimrConfig::TAuditConfig::TLogClassConfig::Service:
        return NACLibProto::SUBJECT_TYPE_SERVICE;
    case NKikimrConfig::TAuditConfig::TLogClassConfig::ServiceImpersonatedFromUser:
        return NACLibProto::SUBJECT_TYPE_SERVICE_IMPERSONATED_FROM_USER;
    }
}

}

TAuditConfig::TAuditConfig(const NKikimrConfig::TAuditConfig& cfg)
    : NKikimrConfig::TAuditConfig(cfg)
{
    ResetLogClassMap();
}

TAuditConfig& TAuditConfig::operator=(const NKikimrConfig::TAuditConfig& cfg) {
    NKikimrConfig::TAuditConfig::operator=(cfg);
    ResetLogClassMap();
    return *this;
}

bool TAuditConfig::EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass logClass, NACLibProto::ESubjectType subjectType) const {
    auto cfg = LogClassMap.find(logClass);
    if (cfg == LogClassMap.end()) {
        cfg = LogClassMap.find(NKikimrConfig::TAuditConfig::TLogClassConfig::Default);
        Y_ENSURE(cfg != LogClassMap.end());
    }
    if (!cfg->second.EnableLogging) {
        return false;
    }
    for (NACLibProto::ESubjectType type : cfg->second.ExcludeSubjectTypes) {
        if (type == subjectType) {
            return false;
        }
    }
    return true;
}

void TAuditConfig::ResetLogClassMap() {
    LogClassMap.clear();
    for (const NKikimrConfig::TAuditConfig::TLogClassConfig& cfg : GetLogClassConfig()) {
        Y_ENSURE(cfg.HasLogClass());
        auto [it, inserted] = LogClassMap.emplace(cfg.GetLogClass(), TLogClassSettings{
            .LogClass = cfg.GetLogClass(),
            .EnableLogging = cfg.GetEnableLogging()});
        Y_ENSURE(inserted, "Duplicated log class in audit config");
        it->second.ExcludeSubjectTypes.reserve(cfg.GetExcludeAccountType().size());
        for (int accountType : cfg.GetExcludeAccountType()) {
            it->second.ExcludeSubjectTypes.push_back(ToSubjectType(static_cast<NKikimrConfig::TAuditConfig::TLogClassConfig::EAccountType>(accountType)));
        }
    }

    auto cfg = LogClassMap.find(NKikimrConfig::TAuditConfig::TLogClassConfig::Default);
    if (cfg == LogClassMap.end()) {
        LogClassMap.emplace(NKikimrConfig::TAuditConfig::TLogClassConfig::Default, TLogClassSettings{
            .LogClass = NKikimrConfig::TAuditConfig::TLogClassConfig::Default,
            .EnableLogging = false});
    }
}

} // namespace NKikimr::NAudit
