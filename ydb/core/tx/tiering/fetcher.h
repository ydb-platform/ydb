#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TEvWatchSchemeObject: public TEventLocal<TEvWatchSchemeObject, NTiers::EvWatchSchemeObject> {
private:
    YDB_READONLY_DEF(std::vector<TString>, ObjectPaths);

public:
    TEvWatchSchemeObject(std::vector<TString> paths)
        : ObjectPaths(std::move(paths)) {
    }
};

class TEvNotifySchemeObjectUpdated: public TEventLocal<TEvNotifySchemeObjectUpdated, NTiers::EvNotifySchemeObjectUpdated> {
private:
    YDB_READONLY_DEF(TString, ObjectPath);
    YDB_READONLY_DEF(NKikimrSchemeOp::TPathDescription, Description);

public:
    TEvNotifySchemeObjectUpdated(const TString& path, NKikimrSchemeOp::TPathDescription description)
        : ObjectPath(path)
        , Description(std::move(description)) {
    }
};

class TEvNotifySchemeObjectDeleted: public TEventLocal<TEvNotifySchemeObjectDeleted, NTiers::EvNotifySchemeObjectDeleted> {
private:
    YDB_READONLY_DEF(TString, ObjectPath);

public:
    TEvNotifySchemeObjectDeleted(TString path)
        : ObjectPath(std::move(path)) {
    }
};

class TEvSchemeObjectResolutionFailed: public TEventLocal<TEvSchemeObjectResolutionFailed, NTiers::EvSchemeObjectResulutionFailed> {
public:
    enum EReason {
        NOT_FOUND = 0,
        LOOKUP_ERROR = 1
    };

private:
    YDB_READONLY_DEF(TString, ObjectPath);
    YDB_READONLY_DEF(EReason, Reason);

public:
    TEvSchemeObjectResolutionFailed(TString path, const EReason reason)
        : ObjectPath(std::move(path))
        , Reason(reason) {
    }
};

}   // namespace NTiers

class TSchemeObjectWatcher: public TActorBootstrapped<TSchemeObjectWatcher> {
private:
    TActorId Owner;

private:
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

    void WatchObjects(const std::vector<TString>& paths) {
        TVector<TVector<TString>> splitPaths;
        for (const TString& path : paths) {
            splitPaths.emplace_back(SplitPath(path));
        }

        auto event = BuildSchemeCacheNavigateRequest(
            std::move(splitPaths), MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void WatchPathId(const TPathId& pathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        for (auto entry : result->ResultSet) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "TSchemeObjectWatcher")("event", ev->ToString())("path", JoinPath(entry.Path));
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    WatchPathId(entry.TableId.PathId);
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                    Send(Owner, new NTiers::TEvSchemeObjectResolutionFailed(
                                    JoinPath(entry.Path), NTiers::TEvSchemeObjectResolutionFailed::EReason::NOT_FOUND));
                    break;

                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                    Send(Owner, new NTiers::TEvSchemeObjectResolutionFailed(
                                    JoinPath(entry.Path), NTiers::TEvSchemeObjectResolutionFailed::EReason::LOOKUP_ERROR));
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
        Send(Owner, new NTiers::TEvNotifySchemeObjectUpdated(describeResult.GetPath(), describeResult.GetPathDescription()));
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto& record = ev->Get();
        const TString name = TString(ExtractBase(record->Path));
        const TString storageDir = TString(ExtractParent(record->Path));
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "object_deleted")("path", record->Path);
        Send(Owner, new NTiers::TEvNotifySchemeObjectDeleted(record->Path));
    }

    void Handle(NTiers::TEvWatchSchemeObject::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "watch_scheme_objects")(
            "names", JoinStrings(ev->Get()->GetObjectPaths().begin(), ev->Get()->GetObjectPaths().end(), ","));
        WatchObjects(ev->Get()->GetObjectPaths());
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_WARN(NKikimrServices::TX_TIERING)("error", "event_undelivered_to_scheme_cache")("reason", ev->Get()->Reason);
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
            IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
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
};

}   // namespace NKikimr::NColumnShard
