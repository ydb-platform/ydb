#pragma once
#ifndef KIKIMR_DISABLE_S3_OPS
#include "common.h"
#include "tier_cleaner.h"

#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/list_objects.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/bg_tasks/abstract/activity.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/secret/snapshot.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard::NTiers {
class TConfigsSnapshot;

class TPathCleaner: public TActorBootstrapped<TPathCleaner> {
private:
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(TString, TieringId);
    bool Truncated = false;
    std::shared_ptr<TConfigsSnapshot> Configs;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    std::set<TString> TiersWait;
    NBackgroundTasks::ITaskExecutorController::TPtr Controller;

    NMetadataProvider::ISnapshotParser::TPtr GetTieringSnapshotParser() const;
    NMetadataProvider::ISnapshotParser::TPtr GetSecretsSnapshotParser() const;
protected:
    void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvTierCleared::TPtr& ev);
public:
    TPathCleaner(const TString& tieringId, const ui64 pathId, NBackgroundTasks::ITaskExecutorController::TPtr controller);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_TIERING_PATH_CLEANER;
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
            hFunc(TEvTierCleared, Handle);
            default:
                break;
        }
    }

    void Bootstrap();
};
}
#endif
