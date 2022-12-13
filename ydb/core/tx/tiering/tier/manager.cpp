#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

NMetadata::NModifications::TOperationParsingResult TTiersManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings,
    const NMetadata::NModifications::IOperationsManager::TModificationContext& /*context*/) const
{
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTierConfig::TDecoder::TierName, NMetadata::NInternal::TYDBValue::Bytes(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TTierConfig::TDecoder::TierConfig);
        if (it != settings.GetFeatures().end()) {
            NKikimrSchemeOp::TStorageTierConfig proto;
            if (!::google::protobuf::TextFormat::ParseFromString(it->second, &proto)) {
                return "incorrect proto format";
            } else {
                result.SetColumn(TTierConfig::TDecoder::TierConfig, NMetadata::NInternal::TYDBValue::Bytes(it->second));
            }
        }
    }
    return result;
}

void TTiersManager::DoPrepareObjectsBeforeModification(std::vector<TTierConfig>&& patchedObjects,
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TModificationContext& context) const
{
    TActivationContext::Register(new TTierPreparationActor(std::move(patchedObjects), controller, context));
}

}
