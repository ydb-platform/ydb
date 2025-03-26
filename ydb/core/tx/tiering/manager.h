#pragma once
#include "common.h"

#include "abstract/manager.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/tiering/tier/identifier.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/accessor/validator.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/s3_settings.h>

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
public:
    class TTierGuard : NNonCopyable::TMoveOnly {
    private:
        NTiers::TExternalStorageId StorageId;
        std::optional<NTiers::TTierConfig> Config;
        TTiersManager* Owner;

    public:
        bool HasConfig() const {
            return !!Config;
        }

        const NTiers::TTierConfig& GetConfigVerified() const {
            return *TValidator::CheckNotNull(Config);
        }

        void UpsertConfig(NTiers::TTierConfig config) {
            Config = std::move(config);
        }

        const NTiers::TExternalStorageId& GetStorageId() const {
            return StorageId;
        }

        TTierGuard(const NTiers::TExternalStorageId& storageId, TTiersManager* owner) : StorageId(storageId), Owner(owner) {
            AFL_VERIFY(owner);
            Owner->RegisterTierManager(StorageId, std::nullopt);
        }

        ~TTierGuard() {
            if (Owner) {
                Owner->UnregisterTierManager(StorageId);
            }
        }

        TTierGuard(TTierGuard&& other) : StorageId(other.StorageId), Config(other.Config), Owner(other.Owner) {
            other.Owner = nullptr;
        }
        TTierGuard& operator=(TTierGuard&& other) {
            std::swap(StorageId, other.StorageId);
            std::swap(Config, other.Config);
            std::swap(Owner, other.Owner);
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

    using TTierById = THashMap<NTiers::TExternalStorageId, TTierGuard>;
    YDB_READONLY_DEF(TTierById, Tiers);
    YDB_READONLY_DEF(std::shared_ptr<NMetadata::NSecret::TSnapshot>, Secrets);

private:
    void OnConfigsUpdated(bool notifyShard = true);
    void RegisterTierManager(const NTiers::TExternalStorageId& name, std::optional<NTiers::TTierConfig> config);
    void UnregisterTierManager(const NTiers::TExternalStorageId& name);

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId, std::function<void(const TActorContext& ctx)> shardCallback = {})
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , ShardCallback(shardCallback)
        , Secrets(std::make_shared<NMetadata::NSecret::TSnapshot>(TInstant::Zero())) {
    }
    TActorId GetActorId() const;
    void ActivateTiers(const THashSet<NTiers::TExternalStorageId>& usedTiers);

    void UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    void UpdateTierConfig(std::optional<NTiers::TTierConfig> config, const NTiers::TExternalStorageId& tierId, const bool notifyShard = true);
    ui64 GetAwaitedConfigsCount() const;

    TString DebugString();

    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop(const bool needStopActor);
    virtual const std::map<NTiers::TExternalStorageId, NTiers::TManager>& GetManagers() const override {
        return Managers;
    }
    virtual const NTiers::TManager* GetManagerOptional(const NTiers::TExternalStorageId& tierId) const override;
};

}
