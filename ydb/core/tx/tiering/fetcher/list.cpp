#include "list.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/rule/object.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

#ifdef __wip__
class TListTieringActor: public TSchemeActorBase<TListTieringActor> {
private:
    using TResult = TConclusion<THashMap<TString, NTiers::TTieringRule>>;
    NThreading::TPromise<TResult> Promise;

private:
    THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(const TVector<TVector<TString>>& paths) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(NACLib::TSystemUsers::Metadata());

        for (const auto& pathComponents : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.ShowPrivatePath = true;
            entry.Path = pathComponents;
        }

        return request;
    }

    void StartListRequest() {
        auto event = BuildSchemeCacheNavigateRequest({ { NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath() } });
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnPathFetched(const TVector<TString> pathComponents, const TPathId& pathId, NSchemeCache::TSchemeCacheNavigate::EKind kind) {
        switch (kind) {
            case NSchemeCache::TSchemeCacheNavigate::KindTieringRule:
                break;
            default:
                OnObjectResolutionFailure(pathComponents, NTiers::TBaseEvObjectResolutionFailed::UNEXPECTED_KIND);
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId), IEventHandle::FlagTrackDelivery);
    }

    void OnPathNotFound(const TVector<TString>& path) {
        OnObjectResolutionFailure(path, NTiers::TEvTieringRuleResolutionFailed::EReason::NOT_FOUND);
    }

    void OnLookupError(const TVector<TString>& path) {
        OnObjectResolutionFailure(path, NTiers::TEvTieringRuleResolutionFailed::EReason::LOOKUP_ERROR);
    }

    void OnObjectResolutionFailure(const TVector<TString>& pathComponents, const NTiers::TEvTieringRuleResolutionFailed::EReason reason) {
        const TString path = JoinPath(pathComponents);
        const TString storageDirectory = TString(ExtractParent(path));
        const TString objectId = TString(ExtractBase(path));
        if (IsEqualPaths(storageDirectory, NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath())) {
            WatchedTieringRules.erase(objectId);
            Send(Owner, new NTiers::TEvTieringRuleResolutionFailed(objectId, reason));
        } else {
            AFL_VERIFY(false)("storage_dir", storageDirectory)("object_id", objectId);
        }
    }

    void OnTieringRuleFetched(const TString& name, const NKikimrSchemeOp::TTieringRuleDescription& description) {
        NTiers::TTieringRule config;
        AFL_VERIFY(config.DeserializeFromProto(description))("name", name)("proto", description.DebugString());
        Send(Owner, new NTiers::TEvNotifyTieringRuleUpdated(name, config));
    }

    void OnConfigsFetched(NSchemeCache::TSchemeCacheNavigate::TListNodeEntry& listResult) {

    }

    void ReplyAndDie(TResult result) {
        TPassAwayGuard g(this);
        Promise.SetValue(std::move(result));
    }

public:
    TListTieringActor(NThreading::TPromise<TConclusion<THashMap<TString, NTiers::TTieringRule>>> promise)
        : Promise(promise) {
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Bootstrap() {
        StartListRequest();
        Become(&TListTieringActor::StateMain);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;

        const auto& result = results[0];
        switch (result.Status) {
            case EStatus::Unknown:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
            case EStatus::RedirectLookupError:
                ReplyAndDie(TResult(Ydb::StatusIds::BAD_REQUEST));
                return;
            case EStatus::AccessDenied:
                ReplyAndDie(TResult(Ydb::StatusIds::UNAUTHORIZED));
                return;
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                ReplyAndDie(TResult(Ydb::StatusIds::NOT_FOUND));
                return;
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    ReplyAndDie(TResult(Ydb::StatusIds::UNAVAILABLE));
                }
                return;
            case EStatus::Ok:
                OnConfigsFetched(result.ListNodeEntry);
                return;
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_ERROR(NKikimrServices::TX_TIERING)("issue", "event_undelivered_to_local_service")("reason", ev->Get()->Reason);
    }
};
#endif

THolder<IActor> MakeListTieringRulesActor(TActorId recipient) {
    // Not implemented
    TActivationContext::ActorSystem()->Send(recipient, new NTiers::TEvListTieringRulesResult(THashMap<TString, NTiers::TTieringRule>()));
    return {};
}

}   // namespace NKikimr::NColumnShard
