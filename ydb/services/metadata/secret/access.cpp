#include "access.h"
#include "checker_access.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

TString TAccess::GetInternalStorageTablePath() {
    return "secrets/access";
}

bool TAccess::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetOwnerUserIdIdx(), OwnerUserId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetSecretIdIdx(), SecretId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetAccessSIDIdx(), AccessSID, rawValue)) {
        return false;
    }
    return true;
}

NMetadataManager::TTableRecord TAccess::SerializeToRecord() const {
    NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::OwnerUserId, NMetadataManager::TYDBValue::Bytes(OwnerUserId));
    result.SetColumn(TDecoder::SecretId, NMetadataManager::TYDBValue::Bytes(SecretId));
    result.SetColumn(TDecoder::AccessSID, NMetadataManager::TYDBValue::Bytes(AccessSID));
    return result;
}

void TAccess::AlteringPreparation(std::vector<TAccess>&& objects,
    NMetadataManager::IAlterPreparationController<TAccess>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context) {
    if (context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
        controller->PreparationProblem("access object cannot be modified");
        return;
    }
    if (!!context.GetUserToken()) {
        for (auto&& i : objects) {
            if (i.GetOwnerUserId() != context.GetUserToken()->GetUserSID()) {
                controller->PreparationProblem("no permissions for modify secret access");
                return;
            }
        }
    }
    TActivationContext::Register(new TAccessPreparationActor(std::move(objects), controller, context));
}

NMetadata::TOperationParsingResult TAccess::BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    const NMetadata::IOperationsManager::TModificationContext& context)
{
    NKikimr::NMetadataManager::TTableRecord result;
    TStringBuf sb(settings.GetObjectId().data(), settings.GetObjectId().size());
    TStringBuf l;
    TStringBuf r;
    if (!sb.TrySplit(':', l, r)) {
        return "incorrect objectId format (secretId:accessSID)";
    }
    result.SetColumn(TDecoder::SecretId, NMetadataManager::TYDBValue::Bytes(l));
    result.SetColumn(TDecoder::AccessSID, NMetadataManager::TYDBValue::Bytes(r));
    if (!context.GetUserToken()) {
        auto it = settings.GetFeatures().find(TDecoder::OwnerUserId);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::OwnerUserId, NMetadataManager::TYDBValue::Bytes(it->second));
        } else {
            return "OwnerUserId not defined";
        }
    } else {
        result.SetColumn(TDecoder::OwnerUserId, NMetadataManager::TYDBValue::Bytes(context.GetUserToken()->GetUserSID()));
    }
    return result;
}

std::vector<Ydb::Column> TAccess::TDecoder::GetColumns() {
    return {
        NMetadataManager::TYDBColumn::Bytes(OwnerUserId),
        NMetadataManager::TYDBColumn::Bytes(SecretId),
        NMetadataManager::TYDBColumn::Bytes(AccessSID)
    };
}

std::vector<Ydb::Column> TAccess::TDecoder::GetPKColumns() {
    return GetColumns();
}

std::vector<TString> TAccess::TDecoder::GetPKColumnIds() {
    return { OwnerUserId, SecretId, AccessSID };
}

}
