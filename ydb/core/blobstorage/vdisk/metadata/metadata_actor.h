#pragma once

#include "metadata_context.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

struct TEvCommitMetadata : public TEventLocal<TEvCommitMetadata, TEvBlobStorage::EvCommitMetadata> {};
struct TEvCommitMetadataDone : public TEventLocal<TEvCommitMetadataDone, TEvBlobStorage::EvCommitMetadataDone> {};

IActor *CreateMetadataActor(TIntrusivePtr<TMetadataContext>& metadataCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint);

} // NKikimr
