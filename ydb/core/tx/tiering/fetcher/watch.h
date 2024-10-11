#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/rule/object.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TEvWatchTieringRules: public TEventLocal<TEvWatchTieringRules, NTiers::EvWatchTieringRules> {
private:
    YDB_READONLY_DEF(std::vector<TString>, Names);

public:
    TEvWatchTieringRules(std::vector<TString> names)
        : Names(std::move(names)) {
    }
};

class TEvNotifyTieringRuleUpdated: public TEventLocal<TEvNotifyTieringRuleUpdated, NTiers::EvNotifyTieringRuleUpdated> {
private:
    YDB_READONLY_DEF(TString, Id);
    YDB_READONLY_DEF(NTiers::TTieringRule, Config);

public:
    TEvNotifyTieringRuleUpdated(const TString& id, NTiers::TTieringRule config)
        : Id(id)
        , Config(std::move(config)) {
    }
};

class TEvNotifyTieringRuleDeleted: public TEventLocal<TEvNotifyTieringRuleDeleted, NTiers::EvNotifyTieringRuleDeleted> {
private:
    YDB_READONLY_DEF(TString, Name);

public:
    TEvNotifyTieringRuleDeleted(TString name)
        : Name(std::move(name)) {
    }
};

class TBaseEvObjectResolutionFailed {
public:
    enum EReason {
        NOT_FOUND = 0,
        LOOKUP_ERROR = 1,
        UNEXPECTED_KIND = 2,
    };

private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(EReason, Reason);

public:
    TBaseEvObjectResolutionFailed(TString name, const EReason reason)
        : Name(std::move(name))
        , Reason(reason) {
    }
};

class TEvTieringRuleResolutionFailed: public TBaseEvObjectResolutionFailed,
                                      public TEventLocal<TEvTieringRuleResolutionFailed, NTiers::EvTieringRuleResulutionFailed> {
public:
    using TBaseEvObjectResolutionFailed::TBaseEvObjectResolutionFailed;
};

}   // namespace NTiers

class TTieringWatcher: public TActorBootstrapped<TTieringWatcher> {
private:
    TActorId Owner;
    THashSet<TString> WatchedTieringRules;

private:
    void WatchTieringRules(const std::vector<TString>& tieringRules) {
        std::vector<TString> newTieringRules;
        for (const TString& tieringRuleId : tieringRules) {
            if (WatchedTieringRules.emplace(tieringRuleId).second) {
                newTieringRules.emplace_back(tieringRuleId);
            }
        }
        RequestPaths(newTieringRules, NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath());
    }

    THolder<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigateRequest(
        const TVector<TVector<TString>>& paths, TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        if (userToken && !userToken->GetSerializedToken().empty()) {
            request->UserToken = userToken;
        }

        for (const auto& pathComponents : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.ShowPrivatePath = true;
            entry.Path = pathComponents;
        }

        return request;
    }

    void RequestPaths(const std::vector<TString>& tieringRules, const TString& storagePath) {
        TVector<TVector<TString>> paths;
        for (const TString& tieringRuleId : tieringRules) {
            paths.emplace_back(SplitPath(storagePath));
            paths.back().emplace_back(tieringRuleId);
        }

        auto event =
            BuildSchemeCacheNavigateRequest(std::move(paths), MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnPathFetched(const TVector<TString> pathComponents, const TPathId& pathId, NSchemeCache::TSchemeCacheNavigate::EKind kind) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "path_fetched")("path", JoinPath(pathComponents));
        switch (kind) {
            case NSchemeCache::TSchemeCacheNavigate::KindTieringRule:
                break;
            default:
                OnObjectResolutionFailure(pathComponents, NTiers::TBaseEvObjectResolutionFailed::UNEXPECTED_KIND);
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId), IEventHandle::FlagTrackDelivery);
    }

    void OnPathNotFound(const TVector<TString>& path) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "path_not_found")("path", JoinPath(path));
        OnObjectResolutionFailure(path, NTiers::TEvTieringRuleResolutionFailed::EReason::NOT_FOUND);
    }

    void OnLookupError(const TVector<TString>& path) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "lookup_error")("path", JoinPath(path));
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

    void OnTieringRuleDeleted(const TString& name) {
        Send(Owner, new NTiers::TEvNotifyTieringRuleDeleted(name));
    }

public:
    TTieringWatcher(TActorId owner)
        : Owner(owner) {
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable, Handle);
            hFunc(NTiers::TEvWatchTieringRules, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        Become(&TTieringWatcher::StateMain);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        for (auto entry : result->ResultSet) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    OnPathFetched(entry.Path, entry.TableId.PathId, entry.Kind);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                    OnPathNotFound(entry.Path);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                    OnLookupError(entry.Path);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                    AFL_VERIFY(false)("entry", entry.ToString());
            }
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "object_fetched")("path", ev->Get()->Path);
        const auto& describeResult = *ev->Get()->Result;
        const auto& pathDescription = describeResult.GetPathDescription();
        const TString& name = pathDescription.GetSelf().GetName();

        switch (pathDescription.GetSelf().GetPathType()) {
            case NKikimrSchemeOp::EPathTypeTieringRule:
                OnTieringRuleFetched(name, pathDescription.GetTieringRuleDescription());
                break;
            default:
                AFL_VERIFY(false)("issue", "invalid_object_kind")("kind", pathDescription.GetSelf().GetPathType());
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto& record = ev->Get();
        const TString name = TString(ExtractBase(record->Path));
        const TString storageDir = TString(ExtractParent(record->Path));
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "object_deleted")("path", record->Path);
        if (IsEqualPaths(storageDir, NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath())) {
            WatchedTieringRules.erase(name);
            Send(Owner, new NTiers::TEvNotifyTieringRuleDeleted(name));
        } else {
            AFL_VERIFY(false)("storage_dir", storageDir)("object_id", name);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr& ev) {
        const auto& record = ev->Get();
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "object_unavailable")("path", record->Path);
    }

    void Handle(NTiers::TEvWatchTieringRules::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_watcher")("event", "watch_tiering_rules")(
            "names", JoinStrings(ev->Get()->GetNames().begin(), ev->Get()->GetNames().end(), ","));
        WatchTieringRules(ev->Get()->GetNames());
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_CRIT(NKikimrServices::TX_TIERING)("error", "event_undelivered_to_scheme_cache")("reason", ev->Get()->Reason);
        for (const TString& tieringRuleId : WatchedTieringRules) {
            Send(Owner, new NTiers::TEvTieringRuleResolutionFailed(tieringRuleId, NTiers::TBaseEvObjectResolutionFailed::LOOKUP_ERROR));
        }
        WatchedTieringRules.clear();
    }
};

}   // namespace NKikimr::NColumnShard
