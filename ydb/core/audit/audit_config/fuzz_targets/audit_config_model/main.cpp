#include <ydb/core/audit/audit_config/audit_config.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/yassert.h>

namespace {

using TLogClassConfig = NKikimrConfig::TAuditConfig::TLogClassConfig;
using ELogClass = TLogClassConfig::ELogClass;
using ELogPhase = TLogClassConfig::ELogPhase;
using EAccountType = TLogClassConfig::EAccountType;

constexpr ui32 MaxClassConfigs = 8;

const ELogClass LogClasses[] = {
    TLogClassConfig::ClusterAdmin,
    TLogClassConfig::DatabaseAdmin,
    TLogClassConfig::Login,
    TLogClassConfig::NodeRegistration,
    TLogClassConfig::Ddl,
    TLogClassConfig::Dml,
    TLogClassConfig::Operations,
    TLogClassConfig::ExportImport,
    TLogClassConfig::Acl,
    TLogClassConfig::AuditHeartbeat,
    TLogClassConfig::Default,
};

const ELogPhase LogPhases[] = {
    TLogClassConfig::Received,
    TLogClassConfig::Completed,
};

const EAccountType AccountTypes[] = {
    TLogClassConfig::Anonymous,
    TLogClassConfig::User,
    TLogClassConfig::Service,
    TLogClassConfig::ServiceImpersonatedFromUser,
};

NACLibProto::ESubjectType Subject(EAccountType accountType) {
    switch (accountType) {
        case TLogClassConfig::Anonymous:
            return NACLibProto::SUBJECT_TYPE_ANONYMOUS;
        case TLogClassConfig::User:
            return NACLibProto::SUBJECT_TYPE_USER;
        case TLogClassConfig::Service:
            return NACLibProto::SUBJECT_TYPE_SERVICE;
        case TLogClassConfig::ServiceImpersonatedFromUser:
            return NACLibProto::SUBJECT_TYPE_SERVICE_IMPERSONATED_FROM_USER;
    }
    Y_ABORT("unexpected account type");
}

struct TExpectedClass {
    bool Present = false;
    bool Enable = false;
    THashSet<NACLibProto::ESubjectType> ExcludedSubjects;
    THashSet<ELogPhase> Phases;
};

void AddExpectedDefaults(THashMap<ELogClass, TExpectedClass>& expected) {
    for (auto& [_, cfg] : expected) {
        if (cfg.Present && cfg.Phases.empty()) {
            cfg.Phases.insert(TLogClassConfig::Completed);
        }
    }
    expected.emplace(TLogClassConfig::Default, TExpectedClass{});
}

bool ExpectedEnable(
    const THashMap<ELogClass, TExpectedClass>& expected,
    ELogClass logClass,
    ELogPhase phase,
    NACLibProto::ESubjectType subject) {
    auto it = expected.find(logClass);
    if (it == expected.end()) {
        it = expected.find(TLogClassConfig::Default);
    }
    Y_ABORT_UNLESS(it != expected.end());
    const auto& cfg = it->second;
    return cfg.Enable && !cfg.ExcludedSubjects.contains(subject) && cfg.Phases.contains(phase);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    NKikimrConfig::TAuditConfig proto;
    THashMap<ELogClass, TExpectedClass> expected;
    THashSet<ELogClass> used;

    const ui32 configs = provider.ConsumeIntegralInRange<ui32>(0, MaxClassConfigs);
    for (ui32 i = 0; i < configs; ++i) {
        const ELogClass logClass = LogClasses[provider.ConsumeIntegralInRange<size_t>(0, std::size(LogClasses) - 1)];
        if (!used.insert(logClass).second) {
            continue;
        }

        auto* cfg = proto.add_logclassconfig();
        cfg->SetLogClass(logClass);
        cfg->SetEnableLogging(provider.ConsumeBool());

        auto& expectedClass = expected[logClass];
        expectedClass.Present = true;
        expectedClass.Enable = cfg->GetEnableLogging();

        const ui32 excluded = provider.ConsumeIntegralInRange<ui32>(0, std::size(AccountTypes));
        for (ui32 j = 0; j < excluded; ++j) {
            const EAccountType accountType = AccountTypes[provider.ConsumeIntegralInRange<size_t>(0, std::size(AccountTypes) - 1)];
            cfg->AddExcludeAccountType(accountType);
            expectedClass.ExcludedSubjects.insert(Subject(accountType));
        }

        const ui32 phases = provider.ConsumeIntegralInRange<ui32>(0, std::size(LogPhases));
        for (ui32 j = 0; j < phases; ++j) {
            const ELogPhase phase = LogPhases[provider.ConsumeIntegralInRange<size_t>(0, std::size(LogPhases) - 1)];
            cfg->AddLogPhase(phase);
            expectedClass.Phases.insert(phase);
        }
    }

    AddExpectedDefaults(expected);

    NKikimr::NAudit::TAuditConfig config = proto;
    NKikimr::NAudit::TAuditConfig assigned;
    assigned = proto;

    for (const ELogClass logClass : LogClasses) {
        for (const ELogPhase phase : LogPhases) {
            for (const EAccountType accountType : AccountTypes) {
                const auto subject = Subject(accountType);
                const bool expectedValue = ExpectedEnable(expected, logClass, phase, subject);
                Y_ABORT_UNLESS(config.EnableLogging(logClass, phase, subject) == expectedValue);
                Y_ABORT_UNLESS(assigned.EnableLogging(logClass, phase, subject) == expectedValue);
            }
        }
    }

    return 0;
}
