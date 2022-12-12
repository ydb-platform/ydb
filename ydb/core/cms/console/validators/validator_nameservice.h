#pragma once

#include "validator.h"

namespace NKikimr::NConsole {

class TNameserviceConfigValidator : public IConfigValidator {
public:
    TNameserviceConfigValidator();

    TString GetDescription() const override;
    bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                     const NKikimrConfig::TAppConfig &newConfig,
                     TVector<Ydb::Issue::IssueMessage> &issues) const override;

private:
};

} // namespace NKikimr::NConsole
