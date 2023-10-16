#include "modifications_validator.h"

#include <ydb/core/cms/console/validators/registry.h>

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

namespace NKikimr::NConsole {

TModificationsValidator::TModificationsValidator(const TConfigIndex &index,
                                                 const TConfigModifications &diff,
                                                 const TConfigsConfig &config)
    : OldIndex(index)
    , ValidationLevel(config.ValidationLevel)
    , ChecksLimit(config.MaxConfigChecksPerModification)
    , FailOnExceededChecksLimit(config.FailOnExceededConfigChecksLimit)
    , TreatWarningAsError(config.TreatWarningAsError)
    , Validated(false)
    , ValidationResult(false)
{
    Y_UNUSED(ChecksLimit);
    Y_UNUSED(FailOnExceededChecksLimit);

    BuildConfigIndexForValidation();

    TConfigModifications importantDiff = BuildModificationsForValidation(diff);

    CollectModifiedItems(importantDiff);

    importantDiff.ApplyTo(Index);
}

bool TModificationsValidator::ApplyValidators()
{
    if (!Validated)
        ValidateModifications();

    if (ValidationResult && TreatWarningAsError) {
        for (auto &issue : Issues) {
            if (issue.severity() == NYql::TSeverityIds::S_WARNING) {
                ValidationResult = false;
                break;
            }
        }
    }

    Validated = true;

    return ValidationResult;
}

const TVector<Ydb::Issue::IssueMessage> &TModificationsValidator::GetIssues() const
{
    return Issues;
}

const TString &TModificationsValidator::GetErrorMessage() const
{
    return Error;
}

ui64 TModificationsValidator::GetChecksDone() const
{
    return ChecksDone;
}

void TModificationsValidator::BuildConfigIndexForValidation()
{
    if (ValidationLevel == NKikimrConsole::VALIDATE_NONE)
        return;

    TConfigItems items;
    OldIndex.CollectItemsByTenantAndNodeType("", "", TDynBitMap(), items);
    if (ValidationLevel == NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES) {
        OldIndex.CollectItemsWithTenantScope({}, items);
        OldIndex.CollectItemsWithNodeTypeScope({}, items);
    } else if (ValidationLevel == NKikimrConsole::VALIDATE_TENANTS) {
        OldIndex.CollectItemsWithPureTenantScope({}, items);
    }

    for (auto &item : items)
        Index.AddItem(item);
}

TConfigModifications TModificationsValidator::BuildModificationsForValidation(const TConfigModifications &diff)
{
    TConfigModifications result;

    for (auto &[id, item] : diff.RemovedItems) {
        if (Index.GetItem(id))
            result.RemovedItems.emplace(id, item);
    }

    ui64 newId = Max<ui64>();
    for (auto &pr : diff.ModifiedItems) {
        if (Index.GetItem(pr.first)) {
            if (IsValidationRequired(pr.second)) {
                TConfigItem::TPtr newItem = new TConfigItem(*pr.second);
                ++newItem->Generation;
                result.ModifiedItems.emplace(pr.first, newItem);
            } else {
                result.RemovedItems.emplace(pr.first, pr.second);
            }
        } else if (IsValidationRequired(pr.second)) {
            TConfigItem::TPtr newItem = new TConfigItem(*pr.second);
            ++newItem->Generation;
            result.AddedItems.push_back(newItem);
        }
    }

    for (auto &item : diff.AddedItems) {
        if (IsValidationRequired(item)) {
            TConfigItem::TPtr newItem = new TConfigItem(*item);
            newItem->Id = newId--;
            newItem->Generation = 1;
            result.AddedItems.push_back(newItem);
        }
    }

    return result;
}

void TModificationsValidator::CollectModifiedItems(const TConfigModifications &diff)
{
    for (auto &[id, _] : diff.RemovedItems)
        ModifiedItems.insert(Index.GetItem(id));
    for (auto &pr : diff.ModifiedItems) {
        ModifiedItems.insert(Index.GetItem(pr.first));
        ModifiedItems.insert(pr.second);
    }
    for (auto item : diff.AddedItems)
        ModifiedItems.insert(item);
}

bool TModificationsValidator::IsValidationRequired(TConfigItem::TPtr item) const
{
    if (!item->UsageScope.NodeIds.empty() || !item->UsageScope.Hosts.empty())
        return false;

    if (item->UsageScope.NodeType)
        return ValidationLevel == NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES;

    if (item->UsageScope.Tenant)
        return (ValidationLevel == NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES
                || ValidationLevel == NKikimrConsole::VALIDATE_TENANTS);

    return ValidationLevel != NKikimrConsole::VALIDATE_NONE;
}

void TModificationsValidator::ValidateModifications()
{
    if (ModifiedItems.empty()) {
        ValidationResult = true;
        return;
    }

    ChecksDone = 0;

    TDynBitMap modifiedKinds;
    for (auto item : ModifiedItems)
        modifiedKinds.Set(item->Kind);

    auto &itemClasses = TValidatorsRegistry::Instance()->GetValidatorClasses();
    for (auto &cl : itemClasses) {
        if (cl.Empty() || modifiedKinds.HasAny(cl)) {
            if (ChecksLimit && (ChecksDone >= ChecksLimit)) {
                AddLimitExceededIssue();
                ValidationResult = !FailOnExceededChecksLimit;
                return;
            }

            if (!ValidateConfigs(cl)) {
                ValidationResult = false;
                return;
            }
        }
    }

    ValidationResult = true;
}

bool TModificationsValidator::ValidateConfigs(const TDynBitMap &kinds)
{
    auto requiredChecks = ComputeAffectedConfigs(kinds, true);

    for (auto &pr : requiredChecks) {
        if (ChecksLimit && (ChecksDone >= ChecksLimit)) {
            AddLimitExceededIssue();
            return !FailOnExceededChecksLimit;
        }
        ++ChecksDone;

        if (!ValidateConfig(kinds, pr.Tenant, pr.NodeType))
            return false;
    }

    return true;
}

THashSet<TTenantAndNodeType>
TModificationsValidator::ComputeAffectedConfigs(const TDynBitMap &kinds,
                                                bool requireCheck) const
{
    THashSet<TTenantAndNodeType> affected;
    if (ValidationLevel == NKikimrConsole::VALIDATE_NONE)
        return affected;

    THashSet<TString> allTenants;
    THashSet<TString> allNodeTypes;
    THashSet<TTenantAndNodeType> allTenantAndNodeTypes;
    Index.CollectTenantAndNodeTypeUsageScopes(kinds, allTenants, allNodeTypes,
                                              allTenantAndNodeTypes);

    THashSet<TString> affectedTenants;
    THashSet<TString> affectedNodeTypes;
    THashSet<TTenantAndNodeType> affectedTenantAndNodeTypes;
    bool domainAffected = false;

    for (auto item : ModifiedItems) {
        if (!kinds.Empty() && !kinds.Get(item->Kind))
            continue;

        Y_ASSERT(item->UsageScope.NodeIds.empty()
                 && item->UsageScope.Hosts.empty());

        if (item->UsageScope.Tenant) {
            if (item->UsageScope.NodeType)
                affectedTenantAndNodeTypes.insert({item->UsageScope.Tenant,
                                                   item->UsageScope.NodeType});
            else
                affectedTenants.insert(item->UsageScope.Tenant);
        } else if (item->UsageScope.NodeType) {
            affectedNodeTypes.insert(item->UsageScope.NodeType);
        } else {
            Y_ABORT_UNLESS(item->UsageScope.NodeIds.empty());
            Y_ABORT_UNLESS(item->UsageScope.Hosts.empty());
            domainAffected = true;
        }
    }

    // Affected tenants and types with no more configs should still
    // be included into the result if all affected configs are
    // requested.
    if (!requireCheck) {
        allTenants.insert(affectedTenants.begin(), affectedTenants.end());
        allNodeTypes.insert(affectedNodeTypes.begin(), affectedNodeTypes.end());
        allTenantAndNodeTypes.insert(affectedTenantAndNodeTypes.begin(),
                                     affectedTenantAndNodeTypes.end());
    }

    if (domainAffected) {
        affected.insert({TString(), TString()});
        if (ValidationLevel == NKikimrConsole::VALIDATE_DOMAIN) {
            Y_ASSERT(affected.size() == 1);
        } else {
            for (auto &tenant : allTenants)
                affected.insert({tenant, TString()});

            if (ValidationLevel == NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES) {
                for (auto &type : allNodeTypes) {
                    affected.insert({TString(), type});
                    for (auto &tenant : allTenants)
                        affected.insert({tenant, type});
                }

                for (auto &type : allTenantAndNodeTypes)
                    affected.insert(type);
            }
        }
    } else {
        if (ValidationLevel == NKikimrConsole::VALIDATE_DOMAIN) {
            Y_ABORT("Trying to validate unmodified kinds");
        } else {
            // If tenant was modified but it has no more config items in
            // the resulting config index then its config is equal to
            // domain one (which is unmodified) and shouldn't be checked.
            for (auto &tenant : affectedTenants) {
                if (allTenants.contains(tenant))
                    affected.insert({tenant, TString()});
            }

            if (ValidationLevel == NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES) {
                // Affected node types should be checked with all tenants.
                // Filter out those types which have no more config items
                // in the resulting config index.
                for (auto &type : affectedNodeTypes) {
                    if (allNodeTypes.contains(type)) {
                        affected.insert({TString(), type});

                        for (auto &tenant : allTenants)
                            affected.insert({tenant, type});
                    }
                }
                // Similarly to affected node types affected tenants should be
                // checked with all node types.
                for (auto &tenant : affectedTenants) {
                    if (allTenants.contains(tenant)) {
                        for (auto &type : allNodeTypes)
                            affected.insert({tenant, type});
                    }
                }

                // Modified tenant and node type pair should be checked in case
                // the resulting config index holds items for both tenant and
                // node type.
                for (auto &type : affectedTenantAndNodeTypes) {
                    if (allTenantAndNodeTypes.contains(type)
                        || (allTenants.contains(type.Tenant)
                            && allNodeTypes.contains(type.NodeType)))
                        affected.insert(type);
                }
            }
        }
    }

    return affected;
}

bool TModificationsValidator::ValidateConfig(const TDynBitMap &kinds,
                                             const TString &tenant,
                                             const TString &nodeType)
{
    NKikimrConfig::TAppConfig oldConfig;
    NKikimrConfig::TAppConfig newConfig;
    BuildConfigs(kinds, tenant, nodeType, oldConfig, newConfig);

    if (!TValidatorsRegistry::Instance()->CheckConfig(oldConfig, newConfig, kinds, Issues)) {
        ExtractErrorMessage();
        return false;
    }

    return true;
}

void TModificationsValidator::BuildConfigs(const TDynBitMap &kinds,
                                           const TString &tenant,
                                           const TString &nodeType,
                                           NKikimrConfig::TAppConfig &oldConfig,
                                           NKikimrConfig::TAppConfig &newConfig) const
{
    auto oldScopedConfig = OldIndex.BuildConfig(0, "", tenant, nodeType, kinds);
    oldScopedConfig->ComputeConfig(oldConfig);
    auto newScopedConfig = Index.BuildConfig(0, "", tenant, nodeType, kinds);
    newScopedConfig->ComputeConfig(newConfig);
}

void TModificationsValidator::ExtractErrorMessage()
{
    for (auto &issue : Issues) {
        if (issue.severity() == NYql::TSeverityIds::S_ERROR
            || issue.severity() == NYql::TSeverityIds::S_FATAL) {
            Error = issue.message();
            break;
        }
    }
}

void TModificationsValidator::AddLimitExceededIssue()
{
    auto msg = Sprintf("Config checks limit (%" PRIu64 ") exceeded during validation",
                       ChecksLimit);
    Ydb::Issue::IssueMessage issue;
    issue.set_message(msg);
    if (FailOnExceededChecksLimit) {
        issue.set_severity(NYql::TSeverityIds::S_ERROR);
        Error = msg;
    } else {
        issue.set_severity(NYql::TSeverityIds::S_INFO);
    }
    Issues.push_back(issue);
}

} // namespace NKikimr::NConsole
