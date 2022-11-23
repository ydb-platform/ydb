#pragma once
#include "external_data.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>
#include <ydb/services/metadata/service.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {
namespace NTiers {

class TManager {
private:
    ui64 TabletId = 0;
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY_DEF(TTierConfig, Config);
    YDB_READONLY_DEF(NActors::TActorId, StorageActorId);
public:
    TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TTierConfig& config);
    static NOlap::TCompression ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compression);
    NOlap::TStorageTier BuildTierStorage() const;

    TManager& Restart(const TTierConfig& config);
    bool NeedExport() const {
        return Config.NeedExport();
    }
    bool Stop();
    bool Start();
};
}

class TTiersManager {
private:
    class TActor;
    using TManagers = TMap<NTiers::TGlobalTierId, NTiers::TManager>;
    ui64 TabletId = 0;
    const TActorId TabletActorId;
    TActor* Actor = nullptr;
    std::unordered_set<ui64> EnabledPathId;
    YDB_READONLY_DEF(TManagers, Managers);

    NMetadataProvider::ISnapshot::TPtr Snapshot;
    mutable NMetadataProvider::ISnapshotParser::TPtr ExternalDataManipulation;

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId)
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
    {
    }
    TActorId GetActorId() const;
    THashMap<ui64, NOlap::TTiersInfo> GetTiering() const;
    void TakeConfigs(NMetadataProvider::ISnapshot::TPtr snapshot);
    void EnablePathId(const ui64 pathId) {
        EnabledPathId.emplace(pathId);
    }
    void DisablePathId(const ui64 pathId) {
        EnabledPathId.erase(pathId);
    }
    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop();
    TActorId GetStorageActorId(const NTiers::TGlobalTierId& tierId);
    const NTiers::TManager& GetManagerVerified(const NTiers::TGlobalTierId& tierId) const;
    NMetadataProvider::ISnapshotParser::TPtr GetExternalDataManipulation() const;

    TManagers::const_iterator begin() const {
        return Managers.begin();
    }

    TManagers::const_iterator end() const {
        return Managers.end();
    }
};

}
