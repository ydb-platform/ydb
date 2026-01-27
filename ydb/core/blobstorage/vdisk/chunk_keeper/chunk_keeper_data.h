#pragma once

#include <util/generic/set.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

struct TChunkKeeperData {
    TChunkKeeperData(const NKikimrVDiskData::TChunkKeeperEntryPoint& entryPoint);
    void GetOwnedChunks(TSet<ui32>& chunks, const TString& logPrefix);

    std::unordered_map<ui32, ui32> Chunks;                      // ChunkId -> Subsystem
    std::unordered_map<ui32, std::set<ui32>> ChunksBySubsystem; // Subsystem -> [ Chunk1 .. ChunkN ]
};

} // namespace NKikimr
