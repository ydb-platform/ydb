#pragma once

#include "validator.h"

namespace NKikimr::NConsole {

class TCompositeConveyorConfigValidator: public IConfigValidator {
public:
    TCompositeConveyorConfigValidator();

    TString GetDescription() const override;
    bool CheckConfig(const NKikimrConfig::TAppConfig& oldConfig,
        const NKikimrConfig::TAppConfig& newConfig,
        TVector<Ydb::Issue::IssueMessage>& issues) const override;
};

}   // namespace NKikimr::NConsole
