#pragma once
#include "defs.h"

namespace NKikimr {

    struct TDefragChunk {
        // chunk id to defrag
        ui32 ChunkId = 0;
        // chunk slot size as a hint for fast search
        ui32 SlotSize = 0;

        void Output(IOutputStream &str) const {
            str << ChunkId;
        }

        TDefragChunk(ui32 chunkId, ui32 slotSize)
            : ChunkId(chunkId)
            , SlotSize(slotSize)
        {}
    };

    using TDefragChunks = std::vector<TDefragChunk>;

} // NKikimr

template<>
inline void Out<NKikimr::TDefragChunk>(IOutputStream &str, const NKikimr::TDefragChunk &value) {
    value.Output(str);
}

