#include "checker_access.h"
#include "checker_secret.h"
#include "manager.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

void TAccessManager::DoPrepareObjectsBeforeModification(std::vector<TAccess>&& patchedObjects, NModifications::IAlterPreparationController<TAccess>::TPtr controller, const NModifications::IOperationsManager::TModificationContext& context) const {
    if (context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
        controller->OnPreparationProblem("access object cannot be modified");
        return;
    }
    if (!!context.GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secret access");
                return;
            }
        }
    }
    TActivationContext::Register(new TAccessPreparationActor(std::move(patchedObjects), controller, context));
}

NModifications::TOperationParsingResult TAccessManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, const NModifications::IOperationsManager::TModificationContext& context) const {
    NInternal::TTableRecord result;
    TStringBuf sb(settings.GetObjectId().data(), settings.GetObjectId().size());
    TStringBuf l;
    TStringBuf r;
    if (!sb.TrySplit(':', l, r)) {
        return "incorrect objectId format (secretId:accessSID)";
    }
    result.SetColumn(TAccess::TDecoder::SecretId, NInternal::TYDBValue::Utf8(l));
    result.SetColumn(TAccess::TDecoder::AccessSID, NInternal::TYDBValue::Utf8(r));
    if (!context.GetUserToken()) {
        auto it = settings.GetFeatures().find(TAccess::TDecoder::OwnerUserId);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(it->second));
        } else {
            return "OwnerUserId not defined";
        }
    } else {
        result.SetColumn(TAccess::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetUserToken()->GetUserSID()));
    }
    return result;
}

NModifications::TOperationParsingResult TSecretManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, const NModifications::IOperationsManager::TModificationContext& context) const {
    NInternal::TTableRecord result;
    if (!context.GetUserToken()) {
        auto it = settings.GetFeatures().find(TSecret::TDecoder::OwnerUserId);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(it->second));
        } else {
            return "OwnerUserId not defined";
        }
    } else {
        result.SetColumn(TSecret::TDecoder::OwnerUserId, NInternal::TYDBValue::Utf8(context.GetUserToken()->GetUserSID()));
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
        if (c == '_') {
            continue;
        }
        return "incorrect character for secret id: '" + TString(c) + "'";
    }
    {
        result.SetColumn(TSecret::TDecoder::SecretId, NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    }
    {
        auto it = settings.GetFeatures().find(TSecret::TDecoder::Value);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TSecret::TDecoder::Value, NInternal::TYDBValue::Utf8(it->second));
        }
    }
    return result;
}

void TSecretManager::DoPrepareObjectsBeforeModification(std::vector<TSecret>&& patchedObjects, NModifications::IAlterPreparationController<TSecret>::TPtr controller, const NModifications::IOperationsManager::TModificationContext& context) const {
    if (!!context.GetUserToken()) {
        for (auto&& i : patchedObjects) {
            if (i.GetOwnerUserId() != context.GetUserToken()->GetUserSID()) {
                controller->OnPreparationProblem("no permissions for modify secrets");
                return;
            }
        }
    }
    TActivationContext::Register(new TSecretPreparationActor(std::move(patchedObjects), controller, context));
}

}
