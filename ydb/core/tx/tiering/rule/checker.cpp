#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/tiering/fetcher/list.h>
#include <ydb/core/tx/tiering/rule/ss_checker.h>

#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTieringRulePreparationActor::StartChecker() {
    if (!Tiers || !Secrets) {
        return;
    }
    auto g = PassAwayGuard();
    if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
        ReplySuccess();
        return;
    }
    for (const auto& interval : TieringRule.GetTiers().GetIntervals()) {
        const TString& tierName = interval.GetTierName();
        const auto* tier = Tiers->GetTierConfigs().FindPtr(tierName);
        if (!tier) {
            Controller->OnBuildProblem("Unknown tier: " + tierName);
            return;
        }
        if (!Secrets->CheckSecretAccess(tier->GetAccessKey(), Context.GetExternalData().GetUserToken())) {
            Controller->OnBuildProblem("no access for secret: " + tier->GetAccessKey().DebugString());
            return;
        } else if (!Secrets->CheckSecretAccess(tier->GetSecretKey(), Context.GetExternalData().GetUserToken())) {
            Controller->OnBuildProblem("no access for secret: " + tier->GetSecretKey().DebugString());
            return;
        }
    }
    ReplySuccess();
}

void TTieringRulePreparationActor::ReplySuccess() {
    NKikimrSchemeOp::TMetadataObjectProperties properties;
    *properties.MutableTieringRule() = std::move(TieringRule);
    Controller->OnBuildFinished(std::move(properties));
}

void TTieringRulePreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TTiersSnapshot>()) {
        Tiers = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

void TTieringRulePreparationActor::Bootstrap() {
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TTierSnapshotConstructor>()));
    Become(&TThis::StateFetchTiering);
}

TTieringRulePreparationActor::TTieringRulePreparationActor(NKikimrSchemeOp::TTieringRuleProperties tieringRule,
    NMetadata::NModifications::IBuildRequestController::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : TieringRule(std::move(tieringRule))
    , Controller(controller)
    , Context(context) {
}

}   // namespace NKikimr::NColumnShard::NTiers
