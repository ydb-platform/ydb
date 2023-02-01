#pragma once
#include "external_data.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {
namespace NTiers {

NOlap::TCompression ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compression);

class TManager {
private:
    ui64 TabletId = 0;
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY_DEF(TTierConfig, Config);
    YDB_READONLY_DEF(NActors::TActorId, StorageActorId);
public:
    TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TTierConfig& config);

    TManager& Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    bool NeedExport() const {
        return Config.NeedExport();
    }
    bool Stop();
    bool Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
};
}

class TTiersManager {
private:
    class TActor;
    using TManagers = std::unordered_map<TString, NTiers::TManager>;
    ui64 TabletId = 0;
    const TActorId TabletActorId;
    TActor* Actor = nullptr;
    std::unordered_map<ui64, TString> PathIdTiering;
    YDB_READONLY_DEF(TManagers, Managers);

    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    NMetadata::NFetcher::ISnapshot::TPtr Snapshot;
    mutable NMetadata::NFetcher::ISnapshotsFetcher::TPtr ExternalDataManipulation;

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId)
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
    {
    }
    TActorId GetActorId() const;
    THashMap<ui64, NOlap::TTiering> GetTiering() const;
    void TakeConfigs(NMetadata::NFetcher::ISnapshot::TPtr snapshot, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    void EnablePathId(const ui64 pathId, const TString& tieringId) {
        PathIdTiering.emplace(pathId, tieringId);
    }
    void DisablePathId(const ui64 pathId) {
        PathIdTiering.erase(pathId);
    }

    bool IsReady() const {
        return !!Snapshot;
    }

    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop();
    TActorId GetStorageActorId(const TString& tierId);
    const NTiers::TManager& GetManagerVerified(const TString& tierId) const;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetExternalDataManipulation() const;

    TManagers::const_iterator begin() const {
        return Managers.begin();
    }

    TManagers::const_iterator end() const {
        return Managers.end();
    }
};

}
