#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

namespace NKikimr {

struct TEvCommitVDiskMetadata : public TEventLocal<TEvCommitVDiskMetadata, TEvBlobStorage::EvCommitVDiskMetadata> {};
struct TEvCommitVDiskMetadataDone : public TEventLocal<TEvCommitVDiskMetadataDone, TEvBlobStorage::EvCommitVDiskMetadataDone> {};

IActor *CreateMetadataActor(TIntrusivePtr<TLogContext>& logCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint);

} // NKikimr
