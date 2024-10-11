#pragma once
#include "common.h"

#include "abstract/manager.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/tiering/rule/object.h>
#include <ydb/core/tx/tiering/tier/object.h>
#include <ydb/core/tx/tiering/tier/snapshot.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

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
public:
    using TTieringRules = THashMap<TString, NTiers::TTieringRule>;

private:
    class TActor;
    friend class TActor;
    using TManagers = std::map<TString, NTiers::TManager>;

    ui64 TabletId = 0;
    const TActorId TabletActorId;
    std::function<void(const TActorContext& ctx)> ShardCallback;
    IActor* Actor = nullptr;
    TManagers Managers;

    std::unordered_map<ui64, TString> PathIdTiering;
    YDB_ACCESSOR_DEF(std::shared_ptr<NMetadata::NSecret::TSnapshot>, Secrets);
    YDB_ACCESSOR_DEF(std::shared_ptr<NTiers::TTiersSnapshot>, Tiers);
    YDB_ACCESSOR_DEF(TTieringRules, TieringRules);
    bool HasCompleteData = true;

private:
    bool ValidateDependencies() const;
    void TakeConfigs(std::shared_ptr<NTiers::TTiersSnapshot> tiers, std::optional<TTieringRules> tieringRules, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId,
                std::function<void(const TActorContext& ctx)> shardCallback = {})
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , ShardCallback(shardCallback)
        , Secrets(std::make_shared<NMetadata::NSecret::TSnapshot>(TInstant::Zero()))
        , Tiers(std::make_shared<NTiers::TTiersSnapshot>(TInstant::Zero()))
    {
    }
    TActorId GetActorId() const;
    THashMap<ui64, NOlap::TTiering> GetTiering() const;
    void EnablePathId(const ui64 pathId, const TString& tieringId);
    void DisablePathId(const ui64 pathId);
    void OnConfigsUpdated(bool notifyShard = true);
    bool IsReady() const {
        return HasCompleteData;
    }

    TString DebugString();

    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop(const bool needStopActor);
    virtual const std::map<TString, NTiers::TManager>& GetManagers() const override {
        return Managers;
    }
    virtual const NTiers::TManager* GetManagerOptional(const TString& tierId) const override;
};

}
