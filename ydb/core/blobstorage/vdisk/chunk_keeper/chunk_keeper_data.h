#pragma once

#include <util/generic/set.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

struct TChunkRecord {
    ui32 ChunkIdx;
    ui32 Subsystem;
    bool ShredRequested;
};

struct TChunkKeeperData {
    TChunkKeeperData(const NKikimrVDiskData::TChunkKeeperEntryPoint& entryPoint);
    void GetOwnedChunks(TSet<ui32>& chunks, const TString& logPrefix);

    std::unordered_map<ui32, TChunkRecord> Chunks;              // ChunkId -> ChunkRecord
    std::unordered_map<ui32, std::set<ui32>> ChunksBySubsystem; // Subsystem -> [ ChunkIdx1 .. ChunkIdxN ]
};

} // namespace NKikimr
