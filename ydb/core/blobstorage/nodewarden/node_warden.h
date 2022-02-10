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
        NKikimrBlobStorage::TNodeWardenServiceSet ServiceSet;
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
        TEncryptionKey PDiskKey;
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

        NPDisk::TKey CreatePDiskKey() const {
             if (PDiskKey) {
                 const ui8 *key;
                 ui32 keySize;
                 PDiskKey.Key.GetKeyBytes(&key, &keySize);
                 return *(ui64*)key;
             } else {
                 return NPDisk::YdbDefaultPDiskSequence;
             }
        }

        bool IsCacheEnabled() const {
            return static_cast<bool>(CacheAccessor);
        }
    };

    IActor* CreateBSNodeWarden(const TIntrusivePtr<TNodeWardenConfig> &cfg);

    bool ObtainTenantKey(TEncryptionKey *key, const NKikimrProto::TKeyConfig& keyConfig);
    bool ObtainStaticKey(TEncryptionKey *key);
    bool ObtainPDiskKey(TEncryptionKey *key, const NKikimrProto::TKeyConfig& keyConfig);

    std::unique_ptr<ICacheAccessor> CreateFileCacheAccessor(const TFsPath& cacheFilePath);

} // NKikimr
