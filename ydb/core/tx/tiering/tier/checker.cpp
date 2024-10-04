#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/tiering/fetcher/list.h>
#include <ydb/core/tx/tiering/rule/ss_checker.h>

#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTierPreparationActor::StartChecker() {
    AFL_VERIFY(Tiers && Secrets && SSCheckResult && TieringRules);
    auto g = PassAwayGuard();
    if (!SSCheckResult->GetContent().GetOperationAllow()) {
        Controller->OnPreparationProblem(SSCheckResult->GetContent().GetDenyReason());
        return;
    }
    for (auto&& tier : Objects) {
        if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
            std::set<TString> tieringsWithTiers;
            for (auto&& i : *TieringRules) {
                if (i.second.ContainsTier(tier.GetTierName())) {
                    tieringsWithTiers.emplace(i.first);
                    if (tieringsWithTiers.size() > 10) {
                        break;
                    }
                }
            }
            if (tieringsWithTiers.size()) {
                Controller->OnPreparationProblem("tier in usage for tierings: " + JoinSeq(", ", tieringsWithTiers));
                return;
            }
        }
        if (!Secrets->CheckSecretAccess(tier.GetAccessKey(), Context.GetExternalData().GetUserToken())) {
            Controller->OnPreparationProblem("no access for secret: " + tier.GetAccessKey().DebugString());
            return;
        } else if (!Secrets->CheckSecretAccess(tier.GetSecretKey(), Context.GetExternalData().GetUserToken())) {
            Controller->OnPreparationProblem("no access for secret: " + tier.GetSecretKey().DebugString());
            return;
        }
    }
    Controller->OnPreparationFinished(std::move(Objects));
}

void TTierPreparationActor::StartSSFetcher() {
    AFL_VERIFY(Tiers && TieringRules);
    std::set<TString> tieringIds;
    std::set<TString> tiersChecked;
    for (auto&& tier : Objects) {
        if (!tiersChecked.emplace(tier.GetTierName()).second) {
            continue;
        }
        for (const auto& [tieringId, config] : *TieringRules) {
            if (config.ContainsTier(tier.GetTierName())) {
                tieringIds.emplace(tieringId);
            }
        }
    }
    {
        SSFetcher = std::make_shared<TFetcherCheckUserTieringPermissions>();
        SSFetcher->SetUserToken(Context.GetExternalData().GetUserToken());
        SSFetcher->SetActivityType(Context.GetActivityType());
        SSFetcher->MutableTieringRuleIds() = tieringIds;
        Register(new TSSFetchingActor(SSFetcher, std::make_shared<TSSFetchingController>(SelfId()), TDuration::Seconds(10)));
    }
}

void TTierPreparationActor::AdvanceCheckerState() {
    switch (State) {
        case INITIAL: {
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
            Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
                new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TTierSnapshotConstructor>()));
            Register(MakeListTieringRulesActor(SelfId()).Release());
            State = FETCH_TIERING;
        }
        case FETCH_TIERING: {
            if (Tiers && TieringRules) {
                StartSSFetcher();
                State = CHECK_PERMISSIONS;
            }
        }
        case CHECK_PERMISSIONS: {
            if (Tiers && Secrets && SSCheckResult && TieringRules) {
                StartChecker();
                State = MAKE_RESULT;
            }
        }
        case MAKE_RESULT: {
            AFL_VERIFY(false);
        }
    }
}

void TTierPreparationActor::Handle(NSchemeShard::TEvSchemeShard::TEvProcessingResponse::TPtr& ev) {
    auto& proto = ev->Get()->Record;
    if (proto.HasError()) {
        Controller->OnPreparationProblem(proto.GetError().GetErrorMessage());
        PassAway();
    } else if (proto.HasContent()) {
        SSCheckResult = SSFetcher->UnpackResult(ev->Get()->Record.GetContent().GetData());
        if (!SSCheckResult) {
            Controller->OnPreparationProblem("cannot unpack ss-fetcher result for class " + SSFetcher->GetClassName());
            PassAway();
        } else {
            AdvanceCheckerState();
        }
    } else {
        Y_ABORT_UNLESS(false);
    }
}

void TTierPreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TTiersSnapshot>()) {
        Tiers = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    AdvanceCheckerState();
}

void TTierPreparationActor::Handle(TEvListTieringRulesResult::TPtr& ev) {
    const auto& result = ev->Get()->GetResult();
    if (result.IsFail()) {
        Controller->OnPreparationProblem(result.GetErrorMessage());
        PassAway();
        return;
    }
    TieringRules.emplace(result.GetResult());
    AdvanceCheckerState();
}

void TTierPreparationActor::Bootstrap() {
    AdvanceCheckerState();
    Become(&TThis::StateMain);
}

TTierPreparationActor::TTierPreparationActor(std::vector<TTierConfig>&& objects,
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
