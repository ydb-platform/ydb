#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/protos/aclib.pb.h>

#include <util/generic/hash.h>

namespace NKikimr::NAudit {

class TAuditConfig : public NKikimrConfig::TAuditConfig {
private:
    struct TLogClassSettings {
        NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass LogClass;
        std::vector<NACLibProto::ESubjectType> ExcludeSubjectTypes;
        bool EnableLogging;
    };

public:
    TAuditConfig();
    TAuditConfig(const NKikimrConfig::TAuditConfig&);
    TAuditConfig& operator=(const NKikimrConfig::TAuditConfig&);

    bool EnableLogging(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass logClass, NACLibProto::ESubjectType subjectType) const;
    bool EnableLogPhase(NKikimrConfig::TAuditConfig::ELogPhase logPhase);

private:
    using TLogClassMap = THashMap<NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass, TLogClassSettings>;

    void ResetLogClassMap();

private:
    TLogClassMap LogClassMap;
};

} // namespace NKikimr::NAudit
