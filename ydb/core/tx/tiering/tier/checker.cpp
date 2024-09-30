#include "checker.h"

#include <ydb/core/tx/tiering/rule/ss_checker.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTierPreprocessingActor::StartChecker() {
    if (!Secrets) {
        return;
    }
    auto g = PassAwayGuard();
    if (const TString* serializedConfig = Settings.ReadFeature(TTierConfig::TDecoder::TierConfig)) {
        NKikimrSchemeOp::TStorageTierConfig config;
        if (!config.ParseFromString(*serializedConfig)) {
            Controller->OnPreprocessingProblem("Can't deserialize tier config");
            return;
        }
        if (config.HasObjectStorage()) {
            if (auto status = PreprocessObjectStorage(*config.MutableObjectStorage()); status.IsFail()) {
                Controller->OnPreprocessingProblem(status.GetErrorMessage());
                return;
            }
        }
        Settings.UpdateFeatures({{TTierConfig::TDecoder::TierConfig, config.SerializeAsString()}});
    }
    Controller->OnPreprocessingFinished(std::move(Settings));
}

void TTierPreprocessingActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

TConclusionStatus TTierPreprocessingActor::PreprocessObjectStorage(NKikimrSchemeOp::TS3Settings& config) const {
    TString defaultUserId;
    if (Context.GetExternalData().GetUserToken()) {
        defaultUserId = Context.GetExternalData().GetUserToken()->GetUserSID();
    }

    if (config.HasSecretableAccessKey()) {
        auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromProto(config.GetSecretableAccessKey(), defaultUserId);
        if (!accessKey) {
            return TConclusionStatus::Fail("AccessKey description is incorrect");
        }
        *config.MutableSecretableAccessKey() = accessKey->SerializeToProto();
    } else if (config.HasAccessKey()) {
        auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(config.GetAccessKey(), defaultUserId);
        if (!accessKey) {
            return TConclusionStatus::Fail("AccessKey is incorrect: " + config.GetAccessKey() + " for userId: " + defaultUserId);
        }
        *config.MutableAccessKey() = accessKey->SerializeToString();
    } else {
        return TConclusionStatus::Fail("AccessKey not configured");
    }

    if (config.HasSecretableSecretKey()) {
        auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromProto(config.GetSecretableSecretKey(), defaultUserId);
        if (!secretKey) {
            return TConclusionStatus::Fail("SecretKey description is incorrect");
        }
        *config.MutableSecretableSecretKey() = secretKey->SerializeToProto();
    } else if (config.HasSecretKey()) {
        auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(config.GetSecretKey(), defaultUserId);
        if (!secretKey) {
            return TConclusionStatus::Fail("SecretKey is incorrect");
        }
        *config.MutableSecretKey() = secretKey->SerializeToString();
    } else {
        return TConclusionStatus::Fail("SecretKey not configured");
    }
    return TConclusionStatus::Success();
}

void TTierPreprocessingActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TTierPreprocessingActor::TTierPreprocessingActor(NYql::TObjectSettingsImpl settings, IController::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Settings(std::move(settings))
    , Controller(controller)
    , Context(context) {
}
}   // namespace NKikimr::NColumnShard::NTiers
