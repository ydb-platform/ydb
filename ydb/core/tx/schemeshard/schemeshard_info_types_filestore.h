#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/protos/filestore_config.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TFileStoreInfo : public TSimpleRefCount<TFileStoreInfo> {
    using TPtr = TIntrusivePtr<TFileStoreInfo>;

    TShardIdx IndexShardIdx = InvalidShardIdx;
    TTabletId IndexTabletId = InvalidTabletId;

    NKikimrFileStore::TConfig Config;
    ui64 Version = 0;

    THolder<NKikimrFileStore::TConfig> AlterConfig;
    ui64 AlterVersion = 0;

    void PrepareAlter(const NKikimrFileStore::TConfig& alterConfig) {
        Y_ENSURE(!AlterConfig);
        Y_ENSURE(!AlterVersion);

        AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
        AlterConfig->CopyFrom(alterConfig);

        Y_ENSURE(!AlterConfig->GetBlockSize());
        AlterConfig->SetBlockSize(Config.GetBlockSize());

        AlterVersion = Version + 1;
    }

    void ForgetAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        AlterConfig.Reset();
        AlterVersion = 0;
    }

    void FinishAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);

        ForgetAlter();
    }

    TFileStoreSpace GetFileStoreSpace() const {
        auto space = GetFileStoreSpace(Config);

        if (AlterConfig) {
            const auto alterSpace = GetFileStoreSpace(*AlterConfig);
            space.SSD = Max(space.SSD, alterSpace.SSD);
            space.HDD = Max(space.HDD, alterSpace.HDD);
            space.SSDSystem = Max(space.SSDSystem, alterSpace.SSDSystem);
        }

        return space;
    }

    static bool ValidateFileStoreConfigSpaceOverflow(ui64 blockSize, ui64 blockCount, TString& errStr) {
        if (blockSize && blockCount > Max<ui64>() / blockSize) {
            errStr = TStringBuilder()
                << "FileStore size overflows ui64: blocks count " << blockCount
                << " * block size " << blockSize
                << " > " << Max<ui64>();
            return false;
        }

        return true;
    }

private:
    TFileStoreSpace GetFileStoreSpace(const NKikimrFileStore::TConfig& config) const {
        const ui64 blockSize = config.GetBlockSize();
        const ui64 blockCount = config.GetBlocksCount();

        TFileStoreSpace space;
        switch (config.GetStorageMediaKind()) {
            case 1: // STORAGE_MEDIA_SSD
                if (config.GetIsSystem()) {
                    space.SSDSystem += blockCount * blockSize;
                } else {
                    space.SSD += blockCount * blockSize;
                }
                break;
            case 2: // STORAGE_MEDIA_HYBRID
            case 3: // STORAGE_MEDIA_HDD
                space.HDD += blockCount * blockSize;
                break;
        }

        return space;
    }
};

}
}
