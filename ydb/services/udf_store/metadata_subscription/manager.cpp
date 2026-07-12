#include "manager.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NUdfStore {

NModifications::TOperationParsingResult TUdfManager::DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& /*settings*/,
    TInternalModificationContext& /*context*/) const {
    NInternal::TTableRecord result;
    // result.SetColumn(TUdfMeta::TDecoder::Md5, NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    // {
    //     auto fValue = settings.GetFeaturesExtractor().Extract(TUdfMeta::TDecoder::Name);
    //     if (fValue) {
    //         result.SetColumn(TUdfMeta::TDecoder::Name, NInternal::TYDBValue::Utf8(*fValue));
    //     }
    // }
    // {
    //     auto fValue = settings.GetFeaturesExtractor().Extract(TUdfMeta::TDecoder::Type);
    //     if (fValue) {
    //         result.SetColumn(TUdfMeta::TDecoder::Type, NInternal::TYDBValue::Utf8(*fValue));
    //     }
    // }
    // {
    //     auto fValue = settings.GetFeaturesExtractor().Extract(TUdf::TDecoder::Manifest);
    //     if (fValue) {
    //         // JSON is serialized as text_value, same wire format as Utf8
    //         result.SetColumn(TUdfMeta::TDecoder::Manifest, NInternal::TYDBValue::Utf8(*fValue));
    //     }
    // }
    return result;
}

void TUdfManager::DoPrepareObjectsBeforeModification(std::vector<TUdfMeta>&& patchedObjects,
    NModifications::IAlterPreparationController<TUdfMeta>::TPtr controller,
    const TInternalModificationContext& /*context*/, const NMetadata::NModifications::TAlterOperationContext& /*alterContext*/) const {
    // Read-only manager: UDF bodies are not written to the KV tablet here.
    // The metadata table stores md5, size, name, type and manifest.
    // Bodies are uploaded to the KV tablet by an external process.
    controller->OnPreparationFinished(std::move(patchedObjects));
}

} // namespace NKikimr::NUdfStore
