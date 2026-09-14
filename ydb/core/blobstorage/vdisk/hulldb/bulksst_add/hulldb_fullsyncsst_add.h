#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/defs.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst.h>

namespace NKikimr {

template <class TKey, class TMemRec>
struct TEvAddFullSyncSsts :
    public TEventLocal<TEvAddFullSyncSsts<TKey, TMemRec>, TEvBlobStorage::EvAddFullSyncSsts>
{
    using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
    using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;

    // chunks to commit
    TVector<ui32> CommitChunks;
    // resulting ssts
    TVector<TLevelSegmentPtr> LevelSegments;
    // sst writer actor id
    TActorId SstWriterId;
};

struct TEvAddFullSyncSstsResult :
    public TEventLocal<TEvAddFullSyncSstsResult, TEvBlobStorage::EvAddFullSyncSstsResult>
{};

} // NKikimr
