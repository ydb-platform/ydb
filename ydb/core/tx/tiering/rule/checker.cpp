#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/tiering/fetcher/list.h>
#include <ydb/core/tx/tiering/rule/ss_checker.h>

#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTieringRulePreparationActor::StartChecker() {
    AFL_VERIFY(Tiers && Secrets);
    auto g = PassAwayGuard();
    if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
        ReplySuccess();
        return;
    }
    for (const auto& interval : GetTieringRule().GetIntervals().GetIntervals()) {
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

void TTieringRulePreparationActor::AdvanceCheckerState() {
    switch (State) {
        case INITIAL:
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TTierSnapshotConstructor>()));
            State = FETCH_TIERING;
            break;
        case FETCH_TIERING:
            if (Tiers && Secrets) {
                StartChecker();
                State = MAKE_RESULT;
            }
            break;
        case MAKE_RESULT:
            AFL_VERIFY(false);
    }
}

void TTieringRulePreparationActor::ReplySuccess() {
    Controller->OnBuildFinished(std::move(Request), UserToken);
}

NKikimrSchemeOp::TTieringRuleDescription TTieringRulePreparationActor::GetTieringRule() const {
    switch (Context.GetActivityType()) {
        case NMetadata::NModifications::IOperationsManager::EActivityType::Undefined:
        case NMetadata::NModifications::IOperationsManager::EActivityType::Upsert:
        case NMetadata::NModifications::IOperationsManager::EActivityType::Drop:
            AFL_VERIFY(false);
        case NMetadata::NModifications::IOperationsManager::EActivityType::Create:
        case NMetadata::NModifications::IOperationsManager::EActivityType::Alter:
            return Request.GetCreateTieringRule();
    }
}

void TTieringRulePreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TTiersSnapshot>()) {
        Tiers = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    AdvanceCheckerState();
}

void TTieringRulePreparationActor::Bootstrap() {
    AdvanceCheckerState();
    Become(&TThis::StateMain);
}

TTieringRulePreparationActor::TTieringRulePreparationActor(NKikimrSchemeOp::TModifyScheme request, std::optional<NACLib::TUserToken> userToken,
    NMetadata::NModifications::IBuildRequestController::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Request(std::move(request))
    , UserToken(std::move(userToken))
    , Controller(controller)
    , Context(context) {
}

}   // namespace NKikimr::NColumnShard::NTiers
