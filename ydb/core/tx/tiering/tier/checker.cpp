#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/tiering/rule/ss_checker.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTierPreparationActor::StartChecker() {
    if (!Tierings || !Secrets || !SSCheckResult) {
        return;
    }
    auto g = PassAwayGuard();
    if (!SSCheckResult->GetContent().GetOperationAllow()) {
        Controller->OnPreparationProblem(SSCheckResult->GetContent().GetDenyReason());
        return;
    }
    for (auto&& tier : Objects) {
        if (Context.GetActivityType() == NMetadata::NModifications::IOperationsManager::EActivityType::Drop) {
            std::set<TString> tieringsWithTiers;
            for (auto&& i : Tierings->GetTableTierings()) {
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
            StartChecker();
        }
    } else {
        Y_ABORT_UNLESS(false);
    }
}

void TTierPreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TConfigsSnapshot>()) {
        Tierings = snapshot;
        std::set<TString> tieringIds;
        std::set<TString> tiersChecked;
        for (auto&& tier : Objects) {
            if (!tiersChecked.emplace(tier.GetTierName()).second) {
                continue;
            }
            auto tIds = Tierings->GetTieringIdsForTier(tier.GetTierName());
            if (tieringIds.empty()) {
                tieringIds = std::move(tIds);
            } else {
                tieringIds.insert(tIds.begin(), tIds.end());
            }
        }
        {
            SSFetcher = std::make_shared<TFetcherCheckUserTieringPermissions>();
            SSFetcher->SetUserToken(Context.GetExternalData().GetUserToken());
            SSFetcher->SetActivityType(Context.GetActivityType());
            SSFetcher->MutableTieringRuleIds() = tieringIds;
            Register(new TSSFetchingActor(SSFetcher, std::make_shared<TSSFetchingController>(SelfId()), TDuration::Seconds(10)));
        }
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

void TTierPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TSnapshotConstructor>()));
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
