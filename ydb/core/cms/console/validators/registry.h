#pragma once

#include "validator.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/bitmap.h>
#include <util/system/mutex.h>

namespace NKikimr::NConsole {

class TValidatorsRegistry  : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TValidatorsRegistry>;
    using TValidators = THashMap<TString, IConfigValidator::TPtr>;
    using TValidatorsSet = THashSet<IConfigValidator::TPtr>;

    static TPtr Instance();
    // Drop is currently used by tests to re-initialize registry.
    static void DropInstance();

    bool AddValidator(IConfigValidator::TPtr validator);

    // Don't allow to add any more validators.
    void LockValidators();
    bool IsLocked() const;

    bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                     const NKikimrConfig::TAppConfig &newConfig,
                     const TDynBitMap &validatorClass,
                     TVector<Ydb::Issue::IssueMessage> &issues) const;

    bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                     const NKikimrConfig::TAppConfig &newConfig,
                     TVector<Ydb::Issue::IssueMessage> &issues) const;

    bool EnableValidator(const TString &name);
    bool DisableValidator(const TString &name);
    bool IsValidatorEnabled(const TString &name) const;

    void EnableValidators();
    void DisableValidators();

    IConfigValidator::TPtr GetValidator(const TString &name) const;

    const TValidators &GetValidators() const;

    // All validators are split into classes. Each class corresponds
    // to a set of checked config items. Config variations to check
    // are examined within each class to reduce total number of
    // variations to check. This method returns vector of such
    // classes represented as a bitmap holding numbers of included
    // item kinds. Empty bitmap corresponds to global validator
    // covering all config items.
    // Currently we don't merge validators for various kinds into
    // single class but this can be changed in the future. E.g. we
    // might want to split all validators into non-intersecting
    // classes.
    const THashSet<TDynBitMap> &GetValidatorClasses() const;

private:
    TValidatorsRegistry();

    void AddToValidatorClasses(IConfigValidator::TPtr validator);
    void RemoveFromValidatorClasses(IConfigValidator::TPtr validator);

    TValidators Validators;
    THashSet<TDynBitMap> ItemClasses;
    THashMap<TDynBitMap, TValidatorsSet> ValidatorsByItemClass;

    TAtomic Locked;

    static TMutex Mutex;
    static TPtr Instance_;
};

} // namespace NKikimr::NConsole
