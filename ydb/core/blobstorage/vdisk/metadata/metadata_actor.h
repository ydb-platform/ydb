#pragma once

#include "metadata_context.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

struct TEvCommitVDiskMetadata : public TEventLocal<TEvCommitVDiskMetadata, TEvBlobStorage::EvCommitVDiskMetadata> {};
struct TEvCommitVDiskMetadataDone : public TEventLocal<TEvCommitVDiskMetadataDone, TEvBlobStorage::EvCommitVDiskMetadataDone> {};

IActor *CreateMetadataActor(TIntrusivePtr<TMetadataContext>& metadataCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint);

} // NKikimr
