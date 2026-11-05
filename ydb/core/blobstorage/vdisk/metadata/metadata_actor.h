#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

namespace NKikimr {

struct TEvCommitVDiskMetadata : public TEventLocal<TEvCommitVDiskMetadata, TEvBlobStorage::EvCommitVDiskMetadata> {
    ui32 MinGroupGeneration = 0;
    explicit TEvCommitVDiskMetadata(ui32 minGroupGeneration = 0) : MinGroupGeneration(minGroupGeneration) {}
};
struct TEvCommitVDiskMetadataDone : public TEventLocal<TEvCommitVDiskMetadataDone, TEvBlobStorage::EvCommitVDiskMetadataDone> {
    ui32 MinGroupGeneration = 0;
    explicit TEvCommitVDiskMetadataDone(ui32 minGroupGeneration = 0) : MinGroupGeneration(minGroupGeneration) {}
};

IActor *CreateMetadataActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint);

} // NKikimr
