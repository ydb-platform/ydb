#pragma once

#include "common.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/rule/object.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

enum ESchemeObject {
    UNKNOWN = 0,
    TIER = 1,
    TIERING = 2,
};

class TEvWatchSchemeObjects: public TEventLocal<TEvWatchSchemeObjects, NTiers::EvWatchSchemeObjects> {
private:
    YDB_READONLY_DEF(std::vector<TString>, Tierings);
    YDB_READONLY_DEF(std::vector<TString>, Tiers);

public:
    TEvWatchSchemeObjects(std::vector<TString> tierings, std::vector<TString> tiers)
        : Tierings(std::move(tierings))
        , Tiers(std::move(tiers)) {
    }
};

class TEvNotifyTieringUpdated: public TEventLocal<TEvNotifyTieringUpdated, NTiers::EvNotifyTieringUpdated> {
private:
    YDB_READONLY_DEF(TString, Id);
    YDB_READONLY_DEF(NTiers::TTieringRule, Config);

public:
    TEvNotifyTieringUpdated(const TString& id, NTiers::TTieringRule config)
        : Id(id)
        , Config(std::move(config)) {
    }
};

class TEvNotifyTierUpdated: public TEventLocal<TEvNotifyTierUpdated, NTiers::EvNotifyTierUpdated> {
private:
    YDB_READONLY_DEF(TString, Id);
    YDB_READONLY_DEF(NTiers::TTierConfig, Config);

public:
    TEvNotifyTierUpdated(const TString& id, NTiers::TTierConfig config)
        : Id(id)
        , Config(std::move(config)) {
    }
};

class TEvNotifyObjectDeleted: public TEventLocal<TEvNotifyObjectDeleted, NTiers::EvNotifyObjectDeleted> {
private:
    YDB_READONLY_DEF(ESchemeObject, ObjectType);
    YDB_READONLY_DEF(TString, ObjectId);

public:
    TEvNotifyObjectDeleted(ESchemeObject type, const TString& id)
        : ObjectType(type)
        , ObjectId(std::move(id)) {
    }
};

class TEvObjectResolutionFailed: public TEventLocal<TEvObjectResolutionFailed, NTiers::EvObjectResolutionFailed> {
public:
    enum EReason {
        NOT_FOUND = 0,
        LOOKUP_ERROR = 1,
    };

private:
    YDB_READONLY_DEF(ESchemeObject, ObjectType);
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(EReason, Reason);

public:
    TEvObjectResolutionFailed(ESchemeObject type, const TString& id, EReason reason)
        : ObjectType(type)
        , ObjectId(std::move(id))
        , Reason(reason) {
    }
};

}   // namespace NTiers

class TTieringFetcher: public TActorBootstrapped<TTieringFetcher> {
private:
    TActorId Owner;
    THashSet<TString> WatchedTiers;
    THashSet<TString> WatchedTierings;

private:
    template <typename T>
    static T DeserializeObject(const NKikimrSchemeOp::TAbstractObjectProperties& properties) {
        NMetadata::NInternal::TTableRecord record;
        AFL_VERIFY(record.DeserializeFromProto(properties.GetProperties()))("proto", properties.DebugString());

        const auto object = NMetadata::NModifications::TObjectManager<T>().DeserializeFromRecord(record);
        auto castObject = std::dynamic_pointer_cast<T>(object);
        AFL_VERIFY(castObject);

        return *castObject;
    }

    void InitializeTierings(const std::vector<TString>& tierings) {
        std::vector<TString> newTierings;
        for (const TString& tieringId : tierings) {
            if (WatchedTierings.emplace(tieringId).second) {
                newTierings.emplace_back(tieringId);
            }
        }
        RequestPaths(newTierings, {});
    }

    void InitializeTiers(const std::vector<TString>& tiers) {
        std::vector<TString> newTiers;
        for (const TString& tierName : tiers) {
            if (WatchedTiers.emplace(tierName).second) {
                newTiers.emplace_back(tierName);
            }
        }
        RequestPaths({}, newTiers);
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

    void RequestPaths(const std::vector<TString>& tierings, const std::vector<TString>& tiers) {
        TVector<TVector<TString>> paths;
        {
            const TString storagePath = NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath();
            for (const TString& tieringId : tierings) {
                paths.emplace_back(SplitPath(storagePath));
                paths.back().emplace_back(tieringId);
            }
        }
        {
            const TString storagePath = NTiers::TTierConfig::GetBehaviour()->GetStorageTablePath();
            for (const TString& tierName : tiers) {
                paths.emplace_back(SplitPath(storagePath));
                paths.back().emplace_back(tierName);
            }
        }

        auto event =
            BuildSchemeCacheNavigateRequest(std::move(paths), MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnPathFetched(const TPathId& pathId, const TString& objectType) {
        NTiers::ESchemeObject type;
        if (objectType == NTiers::TTierConfig::GetTypeId()) {
            type = NTiers::ESchemeObject::TIER;
        } else if (objectType == NTiers::TTieringRule::GetTypeId()) {
            type = NTiers::ESchemeObject::TIERING;
        } else {
            Y_ABORT_S("object_type=" + objectType);
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId, static_cast<ui64>(type)), IEventHandle::FlagTrackDelivery);
    }

    void OnPathNotFound(const TVector<TString>& path) {
        OnObjectResolutionFailure(path, NTiers::TEvObjectResolutionFailed::EReason::NOT_FOUND);
    }

    void OnLookupError(const TVector<TString>& path) {
        OnObjectResolutionFailure(path, NTiers::TEvObjectResolutionFailed::EReason::LOOKUP_ERROR);
    }

    void OnObjectResolutionFailure(const TVector<TString>& pathComponents, const NTiers::TEvObjectResolutionFailed::EReason reason) {
        const TString path = JoinPath(pathComponents);
        const TString storageDirectory = TString(ExtractParent(path));
        const TString objectId = TString(ExtractBase(path));
        if (IsEqualPaths(storageDirectory, NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath())) {
            WatchedTierings.erase(objectId);
            Send(Owner, new NTiers::TEvObjectResolutionFailed(NTiers::ESchemeObject::TIERING, objectId, reason));
        } else if (IsEqualPaths(storageDirectory, NTiers::TTierConfig::GetBehaviour()->GetStorageTablePath())) {
            WatchedTiers.erase(objectId);
            Send(Owner, new NTiers::TEvObjectResolutionFailed(NTiers::ESchemeObject::TIER, objectId, reason));
        } else {
            AFL_VERIFY(false)("storage_dir", storageDirectory)("object_id", objectId);
        }
    }

    void OnTierFetched(const TString& tierName, NTiers::TTierConfig config) {
        Send(Owner, new NTiers::TEvNotifyTierUpdated(tierName, std::move(config)));
    }

    void OnTieringFetched(const TString& tieringId, NTiers::TTieringRule config) {
        Send(Owner, new NTiers::TEvNotifyTieringUpdated(tieringId, std::move(config)));
    }

    void OnTierDeleted(const TString& tierName) {
        Send(Owner, new NTiers::TEvNotifyObjectDeleted(NTiers::ESchemeObject::TIER, tierName));
    }

    void OnTieringDeleted(const TString& tieringId) {
        Send(Owner, new NTiers::TEvNotifyObjectDeleted(NTiers::ESchemeObject::TIERING, tieringId));
    }

public:
    TTieringFetcher(TActorId owner)
        : Owner(owner) {
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable, Handle);
            hFunc(NTiers::TEvWatchSchemeObjects, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            default:
                break;
        }
    }

    void Bootstrap() {
        Become(&TTieringFetcher::StateMain);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        for (auto entry : result->ResultSet) {
            switch (entry.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    AFL_VERIFY(entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindAbstractObject);
                    OnPathFetched(entry.TableId.PathId, entry.AbstractObjectInfo->Description.GetType());
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
        const auto& describeResult = *ev->Get()->Result;
        const auto& pathDescription = describeResult.GetPathDescription();

        AFL_VERIFY(pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeAbstractObject);
        AFL_VERIFY(pathDescription.HasAbstractObjectDescription());
        const auto& abstractObject = pathDescription.GetAbstractObjectDescription();
        const TString& typeId = abstractObject.GetType();
        const TString& objectId = abstractObject.GetName();

        if (typeId == NTiers::TTierConfig::GetTypeId()) {
            OnTierFetched(objectId, DeserializeObject<NTiers::TTierConfig>(abstractObject.GetProperties()));
        } else if (typeId == NTiers::TTieringRule::GetTypeId()) {
            OnTieringFetched(objectId, DeserializeObject<NTiers::TTieringRule>(abstractObject.GetProperties()));
        } else {
            AFL_VERIFY(false)("type_id", typeId);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto& record = ev->Get();
        const TString objectName = TString(ExtractBase(record->Path));
        switch (record->Key) {
            case NTiers::ESchemeObject::TIER:
                OnTierDeleted(objectName);
                break;
            case NTiers::ESchemeObject::TIERING:
                OnTieringDeleted(objectName);
                break;
            default:
                AFL_VERIFY(false)("key", record->Key);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr& ev) {
        const auto& record = ev->Get();
        AFL_WARN(NKikimrServices::TX_TIERING)("event", "scheme object is unavailable")("path", record->Path);
    }

    void Handle(NTiers::TEvWatchSchemeObjects::TPtr& ev) {
        if (const auto& tierings = ev->Get()->GetTierings(); !tierings.empty()) {
            InitializeTierings(tierings);
        }
        if (const auto& tiers = ev->Get()->GetTiers(); !tiers.empty()) {
            InitializeTiers(tiers);
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(static_cast<ui64>(NTiers::ESchemeObject::TIER)));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(static_cast<ui64>(NTiers::ESchemeObject::TIERING)));
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        AFL_ERROR(NKikimrServices::TX_TIERING)("event", "event undelivered to local service")("reason", ev->Get()->Reason);
    }
};

}   // namespace NKikimr::NColumnShard
