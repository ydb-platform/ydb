#include "manager.h"
#include "initializer.h"
#include "checker.h"

namespace NKikimr::NColumnShard::NTiers {

NMetadata::NModifications::TOperationParsingResult TTiersManager::DoBuildPatchFromSettings(
    const NYql::TObjectSettingsImpl& settings,
    TInternalModificationContext& context) const
{
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TTierConfig::TDecoder::TierName, NMetadata::NInternal::TYDBValue::Utf8(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TTierConfig::TDecoder::TierConfig);
        if (it != settings.GetFeatures().end()) {
            NKikimrSchemeOp::TStorageTierConfig proto;
            if (!::google::protobuf::TextFormat::ParseFromString(it->second, &proto)) {
                return "incorrect proto format";
            } else if (proto.HasObjectStorage()) {
                TString defaultUserId;
                if (context.GetExternalData().GetUserToken()) {
                    defaultUserId = context.GetExternalData().GetUserToken()->GetUserSID();
                }

                if (proto.GetObjectStorage().HasSecretableAccessKey()) {
                    auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromProto(proto.GetObjectStorage().GetSecretableAccessKey(), defaultUserId);
                    if (!accessKey) {
                        return "AccessKey description is incorrect";
                    }
                    *proto.MutableObjectStorage()->MutableSecretableAccessKey() = accessKey->SerializeToProto();
                } else if (proto.GetObjectStorage().HasAccessKey()) {
                    auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(proto.GetObjectStorage().GetAccessKey(), defaultUserId);
                    if (!accessKey) {
                        return "AccessKey is incorrect";
                    }
                    *proto.MutableObjectStorage()->MutableAccessKey() = accessKey->SerializeToString();
                } else {
                    return "AccessKey not configured";
                }

                if (proto.GetObjectStorage().HasSecretableSecretKey()) {
                    auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromProto(proto.GetObjectStorage().GetSecretableSecretKey(), defaultUserId);
                    if (!secretKey) {
                        return "SecretKey description is incorrect";
                    }
                    *proto.MutableObjectStorage()->MutableSecretableSecretKey() = secretKey->SerializeToProto();
                } else if (proto.GetObjectStorage().HasSecretKey()) {
                    auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(proto.GetObjectStorage().GetSecretKey(), defaultUserId);
                    if (!secretKey) {
                        return "SecretKey is incorrect";
                    }
                    *proto.MutableObjectStorage()->MutableSecretKey() = secretKey->SerializeToString();
                } else {
                    return "SecretKey not configured";
                }
            }
            result.SetColumn(TTierConfig::TDecoder::TierConfig, NMetadata::NInternal::TYDBValue::Utf8(proto.DebugString()));
        }
    }
    return result;
}

void TTiersManager::DoPrepareObjectsBeforeModification(std::vector<TTierConfig>&& patchedObjects,
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
    const TInternalModificationContext& context) const
{
    TActivationContext::Register(new TTierPreparationActor(std::move(patchedObjects), controller, context));
}

}
