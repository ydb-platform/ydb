#pragma once

#include "erasure.h"

namespace NKikimr {

// Rope/legacy adapters for the persisted 8+2, 32-byte-column ISA-L format.
bool ErasureSplitBlock82(TErasureType::ECrcMode crcMode, const TRope& whole,
    std::span<TRope> parts, TErasureSplitContext* context, IRcBufAllocator* allocator);
void ErasureRestoreBlock82(TErasureType::ECrcMode crcMode, ui32 fullSize, TRope* whole,
    std::span<TRope> parts, ui32 restoreMask, ui32 offset, bool isFragment);
void ErasureSplitBlock82Legacy(TErasureType::ECrcMode crcMode, TRope& whole, TDataPartSet& parts);
void ErasureRestoreBlock82Legacy(TErasureType::ECrcMode crcMode, TDataPartSet& parts,
    bool restoreParts, bool restoreFullData, bool restoreParityParts);

}
