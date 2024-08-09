#include "checker_access.h"
#include "checker_secret.h"
#include "manager.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

void TAccessManager::DoPrepareObjectsBeforeModification(std::vector<TAccess>&& patchedObjects, NModifications::IAlterPreparationController<TAccess>::TPtr controller,
    const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    if (context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
        controller->OnPreparationProblem("access object cannot be modified");
        return;
    }
    if (!!context.GetExternalData().GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetExternalData().GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secret access");
                return;
            }
        }
    }
    TActivationContext::Register(new TAccessPreparationActor(std::move(patchedObjects), controller, context));
}

NModifications::TOperationParsingResult TAccessManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const {
    NInternal::TTableRecord result;
    TStringBuf sb(settings.GetObjectId().data(), settings.GetObjectId().size());
    TStringBuf l;
    TStringBuf r;
    if (!sb.TrySplit(':', l, r)) {
        return TConclusionStatus::Fail("incorrect objectId format (secretId:accessSID)");
    }
    result.SetColumn(TAccess::TDecoder::SecretId, NInternal::TYDBValue::Utf8(l));
    result.SetColumn(TAccess::TDecoder::AccessSID, NInternal::TYDBValue::Utf8(r));
    if (!context.GetExternalData().GetUserToken()) {
        auto fValue = settings.GetFeaturesExtractor().Extract(TAccess::TDecoder::OwnerUserId);
        if (fValue) {
            result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(*fValue));
        } else {
            return TConclusionStatus::Fail("OwnerUserId not defined");
        }
    } else {
        result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetExternalData().GetUserToken()->GetUserSID()));
    }
    return result;
}

NModifications::TOperationParsingResult TSecretManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const {
    static const TString ExtraPathSymbolsAllowed = "!\"#$%&'()*+,-.:;<=>?@[\\]^_`{|}~";
    NInternal::TTableRecord result;
    if (!context.GetExternalData().GetUserToken()) {
        auto fValue = settings.GetFeaturesExtractor().Extract(TSecret::TDecoder::OwnerUserId);
        if (fValue) {
            result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(*fValue));
        } else {
            return TConclusionStatus::Fail("OwnerUserId not defined");
        }
    } else {
        result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetExternalData().GetUserToken()->GetUserSID()));
    }
    for (auto&& c : settings.GetObjectId()) {
        if (c >= '0' && c <= '9') {
            continue;
        }
        if (c >= 'a' && c <= 'z') {
            continue;
        }
        if (c >= 'A' && c <= 'Z') {
            continue;
        }
        if (ExtraPathSymbolsAllowed.Contains(c)) {
            continue;
        }
        return TConclusionStatus::Fail("incorrect character for secret id: '" + TString(c) + "'");
    }
    {
        result.SetColumn(TSecret::TDecoder::SecretId, NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    }
    {
        auto fValue = settings.GetFeaturesExtractor().Extract(TSecret::TDecoder::Value);
        if (fValue) {
            result.SetColumn(TSecret::TDecoder::Value, NInternal::TYDBValue::Utf8(*fValue));
        }
    }
    return result;
}

void TSecretManager::DoPrepareObjectsBeforeModification(std::vector<TSecret>&& patchedObjects, NModifications::IAlterPreparationController<TSecret>::TPtr controller,
    const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    if (!!context.GetExternalData().GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetExternalData().GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secrets");
                return;
            }
        }
    }
    TActivationContext::Register(new TSecretPreparationActor(std::move(patchedObjects), controller, context));
}

}
