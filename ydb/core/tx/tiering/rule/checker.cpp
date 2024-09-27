#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>

#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

void TRulePreprocessingActor::StartChecker() {
    if (!Tierings || !Secrets) {
        return;
    }
    auto g = PassAwayGuard();

    TTieringRule rule;
    if (const TString* description = Settings.ReadFeature(TTieringRule::TDecoder::Description)) {
        NJson::TJsonValue descriptionJson;
        if (!NJson::ReadJsonFastTree(*description, &descriptionJson)) {
            Controller->OnPreprocessingProblem("Cannot parse JSON string at description");
        }
        const auto intervalsConclusion = TTieringRule::TDecoder::DeserializeIntervalsFromJson(descriptionJson);
        if (intervalsConclusion.IsFail()) {
            Controller->OnPreprocessingProblem("Cannot parse description: " + intervalsConclusion.GetErrorMessage());
        }
        const auto intervals = intervalsConclusion.GetResult();
        for (auto&& interval : intervals) {
            auto tier = Tierings->GetTierById(interval.GetTierName());
            if (!tier) {
                Controller->OnPreprocessingProblem("unknown tier usage: " + interval.GetTierName());
                return;
            } else if (!Secrets->CheckSecretAccess(tier->GetAccessKey(), Context.GetExternalData().GetUserToken())) {
                Controller->OnPreprocessingProblem("no access for secret: " + tier->GetAccessKey().DebugString());
                return;
            } else if (!Secrets->CheckSecretAccess(tier->GetSecretKey(), Context.GetExternalData().GetUserToken())) {
                Controller->OnPreprocessingProblem("no access for secret: " + tier->GetSecretKey().DebugString());
                return;
            }
        }
    }
    Controller->OnPreprocessingFinished(Settings);
}

void TRulePreprocessingActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TConfigsSnapshot>()) {
        Tierings = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

void TRulePreprocessingActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TSnapshotConstructor>()));
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TRulePreprocessingActor::TRulePreprocessingActor(NYql::TObjectSettingsImpl settings, IController::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Settings(std::move(settings))
    , Controller(controller)
    , Context(context) {
}
}
