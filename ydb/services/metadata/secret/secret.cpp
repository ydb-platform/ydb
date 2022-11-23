#include "secret.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NSecret {

TString TSecret::GetStorageTablePath() {
    return "/" + AppData()->TenantName + "/.metadata/secrets";
}

bool TSecret::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetOwnerUserIdIdx(), OwnerUserId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetSecretIdIdx(), SecretId, rawValue)) {
        return false;
    }
    if (!decoder.Read(decoder.GetValueIdx(), Value, rawValue)) {
        return false;
    }
    return true;
}

NMetadataManager::TTableRecord TSecret::SerializeToRecord() const {
    NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::OwnerUserId, NMetadataManager::TYDBValue::Bytes(OwnerUserId));
    result.SetColumn(TDecoder::SecretId, NMetadataManager::TYDBValue::Bytes(SecretId));
    result.SetColumn(TDecoder::Value, NMetadataManager::TYDBValue::Bytes(Value));
    return result;
}

void TSecret::AlteringPreparation(std::vector<TSecret>&& objects,
    NMetadataManager::IAlterPreparationController<TSecret>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context) {
    if (!!context.GetUserToken()) {
        for (auto&& i : objects) {
            if (i.GetOwnerUserId() != context.GetUserToken()->GetUserSID()) {
                controller->PreparationProblem("no permissions for modify secrets");
                return;
            }
        }
    }
    controller->PreparationFinished(std::move(objects));
}

NMetadata::TOperationParsingResult TSecret::BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    const NMetadata::IOperationsManager::TModificationContext& context) {
    NKikimr::NMetadataManager::TTableRecord result;
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
    {
        result.SetColumn(TDecoder::SecretId, NMetadataManager::TYDBValue::Bytes(settings.GetObjectId()));
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::Value);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::Value, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    return result;
}

std::vector<Ydb::Column> TSecret::TDecoder::GetColumns() {
    return {
        NMetadataManager::TYDBColumn::Bytes(OwnerUserId),
        NMetadataManager::TYDBColumn::Bytes(SecretId),
        NMetadataManager::TYDBColumn::Bytes(Value)
    };
}

std::vector<Ydb::Column> TSecret::TDecoder::GetPKColumns() {
    return {
        NMetadataManager::TYDBColumn::Bytes(OwnerUserId),
        NMetadataManager::TYDBColumn::Bytes(SecretId)
    };
}

std::vector<TString> TSecret::TDecoder::GetPKColumnIds() {
    return { OwnerUserId, SecretId };
}

}
