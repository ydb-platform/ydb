#include "chunk_keeper_data.h"
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {

TChunkKeeperData::TChunkKeeperData(const NKikimrVDiskData::TChunkKeeperEntryPoint& entryPoint) {
    for (const NKikimrVDiskData::TChunkKeeperEntryPoint::TChunk& chunk : entryPoint.GetChunks()) {
        ui32 chunkIdx = chunk.GetChunkIdx();
        ui32 subsystem = chunk.GetSubsystem();
        Y_VERIFY_DEBUG_S(Chunks.find(chunkIdx) == Chunks.end(), "Double ownership detected, ChunkIdx# " <<
                chunkIdx << " Subsystem# " << subsystem << " another Subsystem# " << Chunks[chunkIdx]);

        Chunks[chunkIdx] = subsystem;
        ChunksBySubsystem[subsystem].insert(chunkIdx);
    }
}

void TChunkKeeperData::GetOwnedChunks(TSet<ui32>& chunks, const TString& logPrefix) {
    for (const auto& [chunkIdx, subsystem] : Chunks) {
        auto [_, inserted] = chunks.insert(chunkIdx);
        Y_VERIFY_S(inserted, logPrefix << "Double chunk ownership detected, ChunkIdx# " << chunkIdx);
    }

#ifndef NDEBUG
    // additional consistency check
    ui32 totalAllocatedChunks = 0;
    for (const auto& [subsystem, chunks] : ChunksBySubsystem) {
        totalAllocatedChunks += chunks.size();
        for (const ui32 chunkIdx : chunks) {
            auto it = Chunks.find(chunkIdx);
            Y_VERIFY_S(it != Chunks.end(), logPrefix << "Inconsistent ChunksBySubsystem, ChunkIdx# " << chunkIdx <<
                    " Subsystem# " << subsystem);
            Y_VERIFY_S(it->second == subsystem, logPrefix << "Inconsistent ChunksBySubsystem, ChunkIdx# " << chunkIdx <<
                    " Subsystem# " << subsystem << " another Subsystem# " << it->second);
        }
    }
    Y_VERIFY_S(totalAllocatedChunks == Chunks.size(), logPrefix << "Missing chunks in ChunksBySubsystem");
#endif
}

} // namespace NKikimr
