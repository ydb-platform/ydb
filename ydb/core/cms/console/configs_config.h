#pragma once

#include <ydb/core/protos/console_config.pb.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NConsole {

struct TConfigsConfig {
    static constexpr ui64 MAX_CONFIG_CHECKS_LIMIT = 100000;

    TConfigsConfig(const NKikimrConsole::TConfigsConfig &config = {});

    void Clear();
    void Parse(const NKikimrConsole::TConfigsConfig &config);

    static bool Check(const NKikimrConsole::TConfigsConfig &config,
                      TString &error);

    THashSet<ui32> AllowedNodeIdScopeKinds;
    THashSet<ui32> AllowedHostScopeKinds;
    THashSet<ui32> DisallowedDomainScopeKinds;

    NKikimrConsole::EValidationLevel ValidationLevel;
    ui32 MaxConfigChecksPerModification;
    bool FailOnExceededConfigChecksLimit;
    bool EnableValidationOnNodeConfigRequest;
    bool TreatWarningAsError;
};

} // namespace NKikimr::NConsole
