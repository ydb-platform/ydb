#include "registry.h"

namespace NKikimr::NConsole {

TValidatorsRegistry::TPtr TValidatorsRegistry::Instance_;
TMutex TValidatorsRegistry::Mutex;

TValidatorsRegistry::TPtr TValidatorsRegistry::Instance()
{
    auto res = Instance_;
    if (!res) {
        TGuard<TMutex> guard(Mutex);
        if (!Instance_)
            Instance_ = new TValidatorsRegistry();
        res = Instance_;
    }
    return res;
}

void TValidatorsRegistry::DropInstance()
{
    TGuard<TMutex> guard(Mutex);
    Instance_ = nullptr;
}

TValidatorsRegistry::TValidatorsRegistry()
    : Locked(0)
{
}

bool TValidatorsRegistry::AddValidator(IConfigValidator::TPtr validator)
{
    TGuard<TMutex> guard(Mutex);

    if (IsLocked() || Validators.contains(validator->GetName()))
        return false;

    Validators.emplace(validator->GetName(), validator);
    AddToValidatorClasses(validator);

    return true;
}

void TValidatorsRegistry::LockValidators()
{
    TGuard<TMutex> guard(Mutex);
    Locked = 1;
}

bool TValidatorsRegistry::IsLocked() const
{
    return Locked;
}

bool TValidatorsRegistry::CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                                      const NKikimrConfig::TAppConfig &newConfig,
                                      const TDynBitMap &validatorClass,
                                      TVector<Ydb::Issue::IssueMessage> &issues) const
{
    Y_ABORT_UNLESS(Locked);
    if (!ItemClasses.contains(validatorClass))
        return true;

    for (auto &validator : ValidatorsByItemClass.at(validatorClass))
        if (validator->IsEnabled() && !validator->CheckConfig(oldConfig, newConfig, issues))
            return false;

    return true;
}

bool TValidatorsRegistry::CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                                      const NKikimrConfig::TAppConfig &newConfig,
                                      TVector<Ydb::Issue::IssueMessage> &issues) const
{
    Y_ABORT_UNLESS(Locked);
    for (auto &pr : Validators) {
        if (pr.second->IsEnabled() && !pr.second->CheckConfig(oldConfig, newConfig, issues))
            return false;
    }

    return true;
}

bool TValidatorsRegistry::EnableValidator(const TString &name)
{
    Y_ABORT_UNLESS(Locked);
    auto validator = GetValidator(name);
    if (!validator)
        return false;

    if (!validator->IsEnabled()) {
        validator->Enable();
        AddToValidatorClasses(validator);
    }

    return true;
}

bool TValidatorsRegistry::DisableValidator(const TString &name)
{
    Y_ABORT_UNLESS(Locked);
    auto validator = GetValidator(name);
    if (!validator)
        return false;

    if (validator->IsEnabled()) {
        validator->Disable();
        RemoveFromValidatorClasses(validator);
    }

    return true;
}

bool TValidatorsRegistry::IsValidatorEnabled(const TString &name) const
{
    Y_ABORT_UNLESS(Locked);
    auto validator = GetValidator(name);
    return validator ? validator->IsEnabled() : false;
}

void TValidatorsRegistry::EnableValidators()
{
    for (auto &pr : Validators) {
        if (!pr.second->IsEnabled()) {
            pr.second->Enable();
            AddToValidatorClasses(pr.second);
        }
    }
}

void TValidatorsRegistry::DisableValidators()
{
    for (auto &pr : Validators) {
        if (pr.second->IsEnabled()) {
            pr.second->Disable();
            RemoveFromValidatorClasses(pr.second);
        }
    }
}

IConfigValidator::TPtr TValidatorsRegistry::GetValidator(const TString &name) const
{
    Y_ABORT_UNLESS(Locked);
    if (Validators.contains(name))
        return Validators.at(name);
    return nullptr;
}

const TValidatorsRegistry::TValidators &TValidatorsRegistry::GetValidators() const
{
    Y_ABORT_UNLESS(Locked);
    return Validators;
}

const THashSet<TDynBitMap> &TValidatorsRegistry::GetValidatorClasses() const
{
    Y_ABORT_UNLESS(Locked);
    return ItemClasses;
}

void TValidatorsRegistry::AddToValidatorClasses(IConfigValidator::TPtr validator)
{
    TDynBitMap kinds;
    for (auto kind : validator->GetCheckedConfigItemKinds())
        kinds.Set(kind);
    ValidatorsByItemClass[kinds].insert(validator);
    ItemClasses.insert(kinds);
}

void TValidatorsRegistry::RemoveFromValidatorClasses(IConfigValidator::TPtr validator)
{
    TDynBitMap kinds;
    for (auto kind : validator->GetCheckedConfigItemKinds())
        kinds.Set(kind);
    auto it = ValidatorsByItemClass.find(kinds);
    it->second.erase(validator);
    if (it->second.empty()) {
        ItemClasses.erase(it->first);
        ValidatorsByItemClass.erase(it);
    }
}

} // namespace NKikimr::NConsole
