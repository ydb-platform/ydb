#pragma once
#include "common.h"

#include "abstract/manager.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/tiering/tier/identifier.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>
#include <ydb/library/accessor/positive_integer.h>
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
    NActors::TActorId TabletActorId;
    TExternalStorageId TierId;
    NActors::TActorId StorageActorId;
    std::optional<NKikimrSchemeOp::TS3Settings> S3Settings;
public:
    const NKikimrSchemeOp::TS3Settings& GetS3Settings() const {
        Y_ABORT_UNLESS(S3Settings);
        return *S3Settings;
    }

    TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TExternalStorageId& tierId);

    bool IsReady() const {
        return !!S3Settings;
    }
    TManager& Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    bool Stop();
    bool Start(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
};
}

class TTiersManager: public ITiersManager {
private:
    friend class TTierRef;
    class TTierRefGuard: public TMoveOnly {
    private:
        NTiers::TExternalStorageId TierId;
        TTiersManager* Owner;

    public:
        const NTiers::TExternalStorageId& GetTierId() const {
            return TierId;
        }

        TTierRefGuard(const NTiers::TExternalStorageId& tierId, TTiersManager& owner);
        ~TTierRefGuard();

        TTierRefGuard(TTierRefGuard&& other)
            : TierId(other.TierId)
            , Owner(other.Owner) {
            other.Owner = nullptr;
        }
        TTierRefGuard& operator=(TTierRefGuard&& other) {
            std::swap(Owner, other.Owner);
            std::swap(TierId, other.TierId);
            return *this;
        }
    };

private:
    class TActor;
    friend class TActor;
    using TManagers = std::map<NTiers::TExternalStorageId, NTiers::TManager>;

    ui64 TabletId = 0;
    const TActorId TabletActorId;
    std::function<void(const TActorContext& ctx)> ShardCallback;
    IActor* Actor = nullptr;
    TManagers Managers;

    using TTierRefCount = THashMap<NTiers::TExternalStorageId, TPositiveControlInteger>;
    using TTierRefsByPathId = THashMap<ui64, std::vector<TTierRefGuard>>;
    YDB_READONLY_DEF(TTierRefCount, TierRefCount);
    YDB_READONLY_DEF(TTierRefsByPathId, UsedTiers);

    using TTierById = THashMap<NTiers::TExternalStorageId, NTiers::TTierConfig>;
    YDB_READONLY_DEF(TTierById, TierConfigs);
    YDB_READONLY_DEF(std::shared_ptr<NMetadata::NSecret::TSnapshot>, Secrets);

private:
    void OnConfigsUpdated(bool notifyShard = true);
    void RegisterTier(const NTiers::TExternalStorageId& name);
    void UnregisterTier(const NTiers::TExternalStorageId& name);

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId, std::function<void(const TActorContext& ctx)> shardCallback = {})
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , ShardCallback(shardCallback)
        , Secrets(std::make_shared<NMetadata::NSecret::TSnapshot>(TInstant::Zero())) {
    }
    TActorId GetActorId() const;
    void EnablePathId(const ui64 pathId, const THashSet<NTiers::TExternalStorageId>& usedTiers);
    void DisablePathId(const ui64 pathId);

    void UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    void UpdateTierConfig(const NTiers::TTierConfig& config, const NTiers::TExternalStorageId& tierId, const bool notifyShard = true);
    bool AreConfigsComplete() const;

    TString DebugString();

    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop(const bool needStopActor);
    virtual const std::map<NTiers::TExternalStorageId, NTiers::TManager>& GetManagers() const override {
        return Managers;
    }
    virtual const NTiers::TManager* GetManagerOptional(const NTiers::TExternalStorageId& tierId) const override;
};

}
