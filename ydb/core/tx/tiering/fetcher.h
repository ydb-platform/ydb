#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TEvWatchSchemeObject: public TEventLocal<TEvWatchSchemeObject, NTiers::EvWatchSchemeObject> {
private:
    YDB_READONLY_DEF(std::vector<TString>, ObjectIds);

public:
    TEvWatchSchemeObject(std::vector<TString> names)
        : ObjectIds(std::move(names)) {
    }
};

class TEvNotifySchemeObjectUpdated: public TEventLocal<TEvNotifySchemeObjectUpdated, NTiers::EvNotifySchemeObjectUpdated> {
private:
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(NKikimrSchemeOp::TPathDescription, Description);

public:
    TEvNotifySchemeObjectUpdated(const TString& path, NKikimrSchemeOp::TPathDescription description)
        : ObjectId(path)
        , Description(std::move(description)) {
    }
};

class TEvNotifySchemeObjectDeleted: public TEventLocal<TEvNotifySchemeObjectDeleted, NTiers::EvNotifySchemeObjectDeleted> {
private:
    YDB_READONLY_DEF(TString, ObjectId);

public:
    TEvNotifySchemeObjectDeleted(TString name)
        : ObjectId(std::move(name)) {
    }
};

class TEvSchemeObjectResolutionFailed: public TEventLocal<TEvSchemeObjectResolutionFailed, NTiers::EvSchemeObjectResulutionFailed> {
public:
    enum EReason {
        NOT_FOUND = 0,
        LOOKUP_ERROR = 1
    };

private:
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(EReason, Reason);

public:
    TEvSchemeObjectResolutionFailed(TString name, const EReason reason)
        : ObjectId(std::move(name))
        , Reason(reason) {
    }
};

}   // namespace NTiers

class TSchemeObjectWatcher: public TActorBootstrapped<TSchemeObjectWatcher> {
private:
    TActorId Owner;
    THashSet<TPathId> WatchedPathIds;

private:
    void WatchObjects(const std::vector<TString>& objectIds) {
        RequestPaths(objectIds);
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

    void RequestPaths(const std::vector<TString>& paths) {
        TVector<TVector<TString>> splitPaths;
        for (const TString& path : paths) {
            splitPaths.emplace_back(SplitPath(path));
        }

        auto event =
            BuildSchemeCacheNavigateRequest(std::move(splitPaths), MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnPathFetched(const TVector<TString> pathComponents, const TPathId& pathId) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "path_fetched")("path", JoinPath(pathComponents));
        if (WatchedPathIds.emplace(pathId).second) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId), IEventHandle::FlagTrackDelivery);
        } else {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "skip_watch_path_id")("reason", "already_subscribed")("path", JoinPath(pathComponents));
        }
    }

    void OnPathNotFound(const TVector<TString>& path) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "path_not_found")("path", JoinPath(path));
        OnObjectResolutionFailure(path, NTiers::TEvSchemeObjectResolutionFailed::EReason::NOT_FOUND);
    }

    void OnLookupError(const TVector<TString>& path) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "lookup_error")("path", JoinPath(path));
        OnObjectResolutionFailure(path, NTiers::TEvSchemeObjectResolutionFailed::EReason::LOOKUP_ERROR);
    }

    void OnObjectResolutionFailure(const TVector<TString>& pathComponents, const NTiers::TEvSchemeObjectResolutionFailed::EReason reason) {
        Send(Owner, new NTiers::TEvSchemeObjectResolutionFailed(JoinPath(pathComponents), reason));
    }

    void OnObjectFetched(const NKikimrSchemeOp::TPathDescription& description, const TString& path) {
        Send(Owner, new NTiers::TEvNotifySchemeObjectUpdated(path, description));
    }

    void OnObjectDeleted(const TString& path, const TPathId& pathId) {
        AFL_VERIFY(WatchedPathIds.erase(pathId));
        Send(Owner, new NTiers::TEvNotifySchemeObjectDeleted(path));
    }

public:
    TSchemeObjectWatcher(TActorId owner)
        : Owner(owner) {
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable, Handle);
            hFunc(NTiers::TEvWatchSchemeObject, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        Become(&TSchemeObjectWatcher::StateMain);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        for (auto entry : result->ResultSet) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    OnPathFetched(entry.Path, entry.TableId.PathId);
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
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "object_fetched")("path", ev->Get()->Path);
        const auto& describeResult = *ev->Get()->Result;
        OnObjectFetched(describeResult.GetPathDescription(), describeResult.GetPath());
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto& record = ev->Get();
        const TString name = TString(ExtractBase(record->Path));
        const TString storageDir = TString(ExtractParent(record->Path));
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "object_deleted")("path", record->Path);
        OnObjectDeleted(record->Path, record->PathId);
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr& ev) {
        const auto& record = ev->Get();
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "object_unavailable")("path", record->Path);
    }

    void Handle(NTiers::TEvWatchSchemeObject::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "watch_scheme_objects")(
            "names", JoinStrings(ev->Get()->GetObjectIds().begin(), ev->Get()->GetObjectIds().end(), ","));
        WatchObjects(ev->Get()->GetObjectIds());
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_WARN(NKikimrServices::TX_TIERING)("error", "event_undelivered_to_scheme_cache")("reason", ev->Get()->Reason);
    }
};

}   // namespace NKikimr::NColumnShard
