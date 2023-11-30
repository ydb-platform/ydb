#pragma once
#ifndef KIKIMR_DISABLE_S3_OPS
#include "common.h"

#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/list_objects.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/bg_tasks/abstract/activity.h>
#include <ydb/services/metadata/abstract/common.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard::NTiers {

class TEvTierCleared: public TEventLocal<TEvTierCleared, EEvents::EvTierCleared> {
private:
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY(bool, Truncated, false);
public:
    TEvTierCleared(const TString& tierName, const bool truncated)
        : TierName(tierName)
        , Truncated(truncated)
    {

    }
};

class TTierCleaner: public TActorBootstrapped<TTierCleaner> {
private:
    const TString TierName;
    const TActorId OwnerId;
    const ui64 PathId = 0;
    NWrappers::NExternalStorage::IExternalStorageConfig::TPtr StorageConfig;
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr Storage;
    bool Truncated = true;
protected:
    void Handle(NWrappers::NExternalStorage::TEvListObjectsResponse::TPtr& ev);
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectsResponse::TPtr& ev);
    void Handle(NWrappers::NExternalStorage::TEvListObjectsRequest::TPtr& ev);
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectsRequest::TPtr& ev);
public:
    TTierCleaner(const TString& tierName, const TActorId& ownerId, const ui64 pathId,
        NWrappers::IExternalStorageConfig::TPtr storageConfig);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_TIERING_TIER_CLEANER;
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvListObjectsResponse, Handle);
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectsResponse, Handle);
            hFunc(NWrappers::NExternalStorage::TEvListObjectsRequest, Handle);
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectsRequest, Handle);
            default:
                break;
        }
    }

    void Bootstrap();
};
}
#endif
