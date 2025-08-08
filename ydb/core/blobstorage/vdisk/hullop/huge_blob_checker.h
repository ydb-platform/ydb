#pragma once

#include "defs.h"

namespace NKikimr {

    class THugeBlobLayoutChecker {
        THashMap<TChunkIdx, std::map<ui32, std::tuple<ui32, TLogoBlobID>>> ChunkCoverage;

    public:
        void AddHugeBlob(TLogoBlobID id, TDiskPart location) {
            Y_ABORT_UNLESS(location.ChunkIdx);
            Y_ABORT_UNLESS(location.Size);
            auto& coverage = ChunkCoverage[location.ChunkIdx];
            auto [it, inserted] = coverage.emplace(location.Offset, std::make_tuple(location.Size, id));
            Y_VERIFY_S(inserted, "duplicate HugeBlob entity: id# " << id << " location# " << location.ToString()
                << " existing Offset# " << it->first << " Size# " << std::get<0>(it->second)
                << " BlobId# " << std::get<1>(it->second));
            if (it != coverage.begin()) {
                auto prevIt = std::prev(it);
                const ui32 prevOffset = prevIt->first;
                const auto& [prevSize, prevId] = prevIt->second;
                Y_VERIFY_S(prevOffset + prevSize <= location.Offset, "overlapping HugeBlob entity: id# " << id
                    << " location# " << location.ToString() << " previous Offset# " << prevOffset << " Size# " << prevSize
                    << " BlobId# " << prevId);
            }
            if (std::next(it) != coverage.end()) {
                auto nextIt = std::next(it);
                const ui32 nextOffset = nextIt->first;
                const auto& [nextSize, nextId] = nextIt->second;
                Y_VERIFY_S(location.Offset + location.Size <= nextOffset, "overlapping HugeBlob entity: id# " << id
                    << " location " << location.ToString() << " next Offset# " << nextOffset << " nextSize# " << nextSize
                    << " BlobId# " << nextId);
            }
        }
    };

} // NKikimr
