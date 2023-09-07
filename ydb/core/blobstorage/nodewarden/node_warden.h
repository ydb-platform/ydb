#pragma once

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_drivemodel_db.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_factory.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/pdisk_io/sector_map.h>

#include <util/folder/path.h>

namespace NKikimr {
    struct ICacheAccessor {
        virtual ~ICacheAccessor() = default;
        virtual TString Read() = 0;
        virtual void Update(std::function<TString(TString)> processor) = 0;
    };

    struct TNodeWardenConfig : public TThrRefBase {
        NKikimrConfig::TBlobStorageConfig BlobStorageConfig;
        NKikimrConfig::TStaticNameserviceConfig NameserviceConfig;
        TIntrusivePtr<IPDiskServiceFactory> PDiskServiceFactory;
        TIntrusivePtr<TAllVDiskKinds> AllVDiskKinds;
        TIntrusivePtr<NPDisk::TDriveModelDb> AllDriveModels;
        NKikimrBlobStorage::TPDiskConfig PDiskConfigOverlay;
        NKikimrConfig::TFeatureFlags FeatureFlags;
        NKikimrBlobStorage::TIncrHugeConfig IncrHugeConfig;
        THashMap<TString, TIntrusivePtr<NPDisk::TSectorMap>> SectorMaps;
        std::unique_ptr<ICacheAccessor> CacheAccessor;
        TEncryptionKey TenantKey;
        TEncryptionKey StaticKey;
        TVector<TEncryptionKey> PDiskKey;
        bool CachePDisks = false;
        bool CacheVDisks = false;
        bool EnableVDiskCooldownTimeout = false;

        // debugging options
        bool VDiskReplPausedAtStart = false;

        TNodeWardenConfig(const TIntrusivePtr<IPDiskServiceFactory> &pDiskServiceFactory)
            : PDiskServiceFactory(pDiskServiceFactory)
            , AllVDiskKinds(new TAllVDiskKinds)
            , AllDriveModels(new NPDisk::TDriveModelDb)
        {}

        NPDisk::TMainKey CreatePDiskKey() const {
            if (PDiskKey.size() > 0) {
                NPDisk::TMainKey mainKey;
                for (ui32 i = 0; i < PDiskKey.size(); ++i) {
                    const ui8 *key;
                    ui32 keySize;
                    PDiskKey[i].Key.GetKeyBytes(&key, &keySize);
                    mainKey.push_back(*(ui64*)key);
                }
                return mainKey;
            } else {
                return { NPDisk::YdbDefaultPDiskSequence };
            }
        }

        bool IsCacheEnabled() const {
            return static_cast<bool>(CacheAccessor);
        }
    };

    IActor* CreateBSNodeWarden(const TIntrusivePtr<TNodeWardenConfig> &cfg);

    bool ObtainTenantKey(TEncryptionKey *key, const NKikimrProto::TKeyConfig& keyConfig);
    bool ObtainStaticKey(TEncryptionKey *key);
    bool ObtainPDiskKey(TVector<TEncryptionKey> *key, const NKikimrProto::TKeyConfig& keyConfig);

    std::unique_ptr<ICacheAccessor> CreateFileCacheAccessor(const TString& templ, const std::unordered_map<char, TString>& vars);

} // NKikimr
