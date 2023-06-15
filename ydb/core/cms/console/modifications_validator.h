#pragma once

#include "configs_config.h"

#include <ydb/core/cms/console/util/config_index.h>

namespace NKikimr::NConsole {

class TModificationsValidatorTests;

class TModificationsValidator {
public:
    TModificationsValidator(const TConfigIndex &index,
                            const TConfigModifications &diff,
                            const TConfigsConfig &config);

    bool ApplyValidators();

    const TVector<Ydb::Issue::IssueMessage> &GetIssues() const;
    const TString &GetErrorMessage() const;
    ui64 GetChecksDone() const;

    THashSet<TTenantAndNodeType> ComputeAffectedConfigs(const TDynBitMap &kinds,
                                                        bool requireCheck) const;
    void BuildConfigs(const TDynBitMap &kinds,
                      const TString &tenant,
                      const TString &nodeType,
                      NKikimrConfig::TAppConfig &oldConfig,
                      NKikimrConfig::TAppConfig &newConfig) const;

private:
    void BuildConfigIndexForValidation();
    TConfigModifications BuildModificationsForValidation(const TConfigModifications &diff);
    void CollectModifiedItems(const TConfigModifications &diff);

    bool IsValidationRequired(TConfigItem::TPtr item) const;

    void ValidateModifications();
    bool ValidateConfigs(const TDynBitMap &kinds);
    bool ValidateConfig(const TDynBitMap &kinds,
                        const TString &tenant,
                        const TString &nodeType);
    void ExtractErrorMessage();
    void AddLimitExceededIssue();

    TConfigIndex Index;
    TConfigItems ModifiedItems;
    const TConfigIndex &OldIndex;

    NKikimrConsole::EValidationLevel ValidationLevel;
    ui64 ChecksLimit;
    bool FailOnExceededChecksLimit;
    bool TreatWarningAsError;

    bool Validated;
    bool ValidationResult;
    TString Error;
    TVector<Ydb::Issue::IssueMessage> Issues;
    ui64 ChecksDone;

    friend class TModificationsValidatorTests;
};

} // namespace NKikimr::NConsole
