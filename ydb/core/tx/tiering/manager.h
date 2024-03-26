#pragma once
#include "external_data.h"

#include "abstract/manager.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/accessor/accessor.h>

#include <functional>

namespace NKikimr::NColumnShard {
namespace NTiers {

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TOlapColumn::TSerializer& serializerProto);
NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compressionProto);

class TManager {
private:
    ui64 TabletId = 0;
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY_DEF(TTierConfig, Config);
    YDB_READONLY_DEF(NActors::TActorId, StorageActorId);
    std::optional<NKikimrSchemeOp::TS3Settings> S3Settings;
public:
    const NKikimrSchemeOp::TS3Settings& GetS3Settings() const {
        Y_ABORT_UNLESS(S3Settings);
        return *S3Settings;
    }

    TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TTierConfig& config);

    TManager& Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    bool Stop();
    bool Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);

    TString GetTierName() const {
        return GetConfig().GetTierName();
    }
};
}

class TTiersManager: public ITiersManager {
private:
    class TActor;
    using TManagers = std::map<TString, NTiers::TManager>;
    ui64 TabletId = 0;
    const TActorId TabletActorId;
    std::function<void(const TActorContext& ctx)> ShardCallback;
    TActor* Actor = nullptr;
    std::unordered_map<ui64, TString> PathIdTiering;
    TManagers Managers;

    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    NMetadata::NFetcher::ISnapshot::TPtr Snapshot;
    mutable NMetadata::NFetcher::ISnapshotsFetcher::TPtr ExternalDataManipulation;

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId,
                std::function<void(const TActorContext& ctx)> shardCallback = {})
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , ShardCallback(shardCallback)
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
    TTiersManager& Stop(const bool needStopActor);
    virtual const std::map<TString, NTiers::TManager>& GetManagers() const override {
        AFL_VERIFY(IsReady());
        return Managers;
    }
    virtual const NTiers::TManager* GetManagerOptional(const TString& tierId) const override;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr GetExternalDataManipulation() const;

};

}
