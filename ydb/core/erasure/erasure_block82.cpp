#include "erasure_block82.h"
#include "erasure_isa.h"

#include <library/cpp/digest/crc32c/crc32c.h>
#include <bit>

namespace NKikimr {
namespace {

constexpr ui32 DataParts = 8;
constexpr ui32 TotalParts = 10;
constexpr ui32 AllParts = (1u << TotalParts) - 1;
constexpr ui32 AllData = (1u << DataParts) - 1;
constexpr TErasureType::EErasureSpecies Species = TErasureType::Erasure8Plus2Block;

void FinishCrc(TRope& part, size_t userSize) {
    ui32 crc = 0;
    auto it = part.Begin();
    for (size_t left = userSize; left;) {
        const size_t n = Min(left, it.ContiguousSize());
        crc = Crc32cExtend(crc, it.ContiguousData(), n);
        it += n;
        left -= n;
    }
    // The suffix is a private allocation, never a view of the input blob.
    Y_ABORT_UNLESS(it.Valid() && it.ContiguousSize() >= sizeof(crc));
    memcpy(const_cast<char*>(it.ContiguousData()), &crc, sizeof(crc));
}

void PrepareSplit(TErasureType::ECrcMode crcMode, const TRope& whole,
        std::span<TRope> parts, IRcBufAllocator* allocator) {
    const TErasureType type(Species);
    const size_t userSize = type.PartUserSize(whole.size());
    const size_t partSize = type.PartSize(crcMode, whole.size());
    auto it = whole.Begin();
    for (ui32 i = 0; i < DataParts; ++i) {
        const size_t used = type.BlockSplitPartUsedSize(whole.size(), i);
        auto end = it + used;
        parts[i] = TRope(it, end);
        it = end;
        if (const size_t extra = partSize - used) {
            auto buffer = allocator->AllocRcBuf(extra, 0, 0);
            memset(buffer.UnsafeGetDataMut(), 0, extra);
            parts[i].Insert(parts[i].End(), std::move(buffer));
        }
    }
    Y_ABORT_UNLESS(it == whole.End());
    for (ui32 i = DataParts; i < TotalParts; ++i) {
        parts[i] = partSize ? TRope(allocator->AllocRcBuf(partSize, 0, 0)) : TRope();
    }
    Y_ABORT_UNLESS(userSize <= partSize);
}

}

bool ErasureSplitBlock82(TErasureType::ECrcMode crcMode, const TRope& whole,
        std::span<TRope> parts, TErasureSplitContext* context, IRcBufAllocator* allocator) {
    Y_ABORT_UNLESS(parts.size() == TotalParts && allocator);
    Y_ABORT_UNLESS(TErasureType::IsCrcModeValid(crcMode));
    const size_t userSize = TErasureType(Species).PartUserSize(whole.size());
    const size_t offset = context ? context->Offset : 0;
    Y_ABORT_UNLESS(offset <= userSize && userSize <= Max<ui32>());
    Y_ABORT_UNLESS(!context || context->MaxSizeAtOnce);
    if (!offset) {
        PrepareSplit(crcMode, whole, parts, allocator);
    }
    const size_t partSize = TErasureType(Species).PartSize(crcMode, whole.size());
    for (const auto& part : parts) {
        Y_ABORT_UNLESS(part.size() == partSize);
    }
    const size_t size = Min(userSize - offset, context ? size_t(context->MaxSizeAtOnce) : userSize);
    size_t remaining = size;
    if (remaining) {
        const auto& backend = GetErasureIsaL();
        std::array<TRope::TConstIterator, DataParts> sources;
        std::array<ui8*, 2> outputs;
        for (ui32 i = 0; i < DataParts; ++i) {
            sources[i] = parts[i].Begin() + offset;
        }
        for (ui32 i = 0; i < 2; ++i) {
            outputs[i] = reinterpret_cast<ui8*>(parts[DataParts + i].GetContiguousSpanMut().data()) + offset;
        }
        while (remaining) {
            size_t n = Min(remaining, size_t(Max<int>()));
            std::array<const ui8*, DataParts> pointers;
            for (ui32 i = 0; i < DataParts; ++i) {
                n = Min(n, sources[i].ContiguousSize());
                pointers[i] = reinterpret_cast<const ui8*>(sources[i].ContiguousData());
            }
            Y_ABORT_UNLESS(n);
            backend.Encode(n, pointers.data(), outputs.data());
            for (auto& source : sources) {
                source += n;
            }
            for (auto& output : outputs) {
                output += n;
            }
            remaining -= n;
        }
    }
    if (context) {
        context->Offset += size;
    }
    const bool done = offset + size == userSize;
    if (done && crcMode == TErasureType::CrcModeWholePart) {
        for (auto& part : parts) {
            FinishCrc(part, userSize);
        }
    }
    return done;
}

void ErasureRestoreBlock82(TErasureType::ECrcMode crcMode, ui32 fullSize, TRope* whole,
        std::span<TRope> parts, ui32 restoreMask, ui32 offset, bool isFragment) {
    Y_ABORT_UNLESS(parts.size() == TotalParts && !(restoreMask & ~AllParts));
    Y_ABORT_UNLESS(TErasureType::IsCrcModeValid(crcMode));
    Y_ABORT_UNLESS(isFragment ? !whole : !offset);
    const TErasureType type(Species);
    const size_t userSize = type.PartUserSize(fullSize);
    const size_t partSize = type.PartSize(crcMode, fullSize);
    Y_ABORT_UNLESS(offset <= userSize);

    ui16 missing = 0;
    size_t length = isFragment ? 0 : userSize;
    for (ui32 i = 0; i < TotalParts; ++i) {
        if (parts[i]) {
            if (isFragment && !length) {
                length = parts[i].size();
            }
            Y_ABORT_UNLESS(parts[i].size() == (isFragment ? length : partSize));
        } else {
            missing |= 1u << i;
        }
    }
    Y_ABORT_UNLESS(length <= userSize - offset);
    // Empty headerless parts have no presence marker and need no GF operation.
    if (!partSize && !isFragment) {
        if (whole) {
            whole->clear();
        }
        return;
    }
    Y_ABORT_UNLESS(std::popcount(missing) <= 2, "block-8-2 requires eight source parts");
    const ui16 needed = missing & (restoreMask | (whole ? AllData : 0));
    std::array<TRope, TotalParts> restored;
    if (needed) {
        const auto& backend = GetErasureIsaL();
        const auto& selection = backend.Sources(missing);
        std::array<TRope::TConstIterator, DataParts> iterators;
        std::array<ui8*, TotalParts> slots{};
        for (ui32 i = 0; i < DataParts; ++i) {
            iterators[i] = parts[selection[i]].Begin();
        }
        for (ui32 i = 0; i < TotalParts; ++i) {
            if (needed >> i & 1) {
                const size_t outputSize = isFragment ? length : partSize;
                restored[i] = TRcBuf::Uninitialized(outputSize);
                slots[i] = reinterpret_cast<ui8*>(restored[i].UnsafeGetContiguousSpanMut().data());
            }
        }
        size_t remaining = length;
        while (remaining) {
            size_t n = Min(remaining, size_t(Max<int>()));
            for (ui32 i = 0; i < DataParts; ++i) {
                n = Min(n, iterators[i].ContiguousSize());
                slots[selection[i]] = reinterpret_cast<ui8*>(const_cast<char*>(iterators[i].ContiguousData()));
            }
            Y_ABORT_UNLESS(n);
            backend.Restore(n, missing, needed, slots.data());
            for (auto& iterator : iterators) {
                iterator += n;
            }
            for (ui32 i = 0; i < TotalParts; ++i) {
                if (needed >> i & 1) {
                    slots[i] += n;
                }
            }
            remaining -= n;
        }
        if (!isFragment && crcMode == TErasureType::CrcModeWholePart) {
            for (ui32 i = 0; i < TotalParts; ++i) {
                if (needed >> i & 1) {
                    FinishCrc(restored[i], userSize);
                }
            }
        }
    }
    if (whole) {
        TRope result;
        for (ui32 i = 0; i < DataParts; ++i) {
            const auto& source = missing >> i & 1 ? restored[i] : parts[i];
            const size_t used = type.BlockSplitPartUsedSize(fullSize, i);
            if (used) {
                result.Insert(result.End(), TRope(source.Begin(), source.Begin() + used));
            }
        }
        Y_ABORT_UNLESS(result.size() == fullSize);
        *whole = std::move(result);
    }
    for (ui32 i = 0; i < TotalParts; ++i) {
        if (missing & restoreMask & (1u << i)) {
            parts[i] = std::move(restored[i]);
        }
    }
}

void ErasureSplitBlock82Legacy(TErasureType::ECrcMode crcMode, TRope& whole, TDataPartSet& partSet) {
    std::array<TRope, TotalParts> parts;
    if (partSet.IsSplitStarted()) {
        Y_ABORT_UNLESS(partSet.Parts.size() == TotalParts && partSet.FullDataSize == whole.size());
        for (ui32 i = 0; i < TotalParts; ++i) {
            // Preserve unique ownership across resumptions: retaining another
            // reference here would copy the entire parity buffer on each write.
            parts[i] = std::move(partSet.Parts[i].OwnedString);
        }
    }
    // CurBlockIdx/WholeBlocks are opaque legacy progress counters. For this
    // codec they count bytes, preserving a bounded 32 KiB per-part quantum.
    auto context = TErasureSplitContext::Init(32 * 1024);
    context.Offset = partSet.CurBlockIdx;
    ErasureSplitBlock82(crcMode, whole, parts, &context, GetDefaultRcBufAllocator());
    partSet.FullDataSize = whole.size();
    partSet.WholeBlocks = TErasureType(Species).PartUserSize(whole.size());
    partSet.CurBlockIdx = context.Offset;
    partSet.PartsMask = AllParts;
    partSet.IsFragment = false;
    partSet.Parts.resize(TotalParts);
    partSet.MemoryConsumed = 0;
    for (ui32 i = 0; i < TotalParts; ++i) {
        auto& fragment = partSet.Parts[i];
        fragment.OwnedString = std::move(parts[i]);
        // The legacy API exposes writable contiguous Bytes. Detach/compact
        // input views once, then retain their allocations on later resumptions.
        fragment.Bytes = fragment.OwnedString.GetContiguousSpanMut().data();
        fragment.Offset = 0;
        fragment.Size = fragment.OwnedString.size();
        fragment.PartSize = fragment.Size;
        partSet.MemoryConsumed += fragment.MemoryConsumed();
    }
}

void ErasureRestoreBlock82Legacy(TErasureType::ECrcMode crcMode, TDataPartSet& partSet,
        bool restoreParts, bool restoreFullData, bool restoreParityParts) {
    Y_ABORT_UNLESS(partSet.Parts.size() == TotalParts && !(partSet.PartsMask & ~AllParts));
    Y_ABORT_UNLESS(!partSet.IsFragment || !restoreFullData);
    Y_ABORT_UNLESS(partSet.FullDataSize <= Max<ui32>());
    const ui32 restoreMask = restoreParityParts ? AllParts : restoreParts ? AllData : 0;
    const size_t partSize = TErasureType(Species).PartSize(crcMode, partSet.FullDataSize);
    std::array<TRope, TotalParts> parts;
    ui64 offset = 0;
    ui64 length = 0;
    bool haveSource = false;
    for (ui32 i = 0; i < TotalParts; ++i) {
        if (partSet.PartsMask >> i & 1) {
            const auto& fragment = partSet.Parts[i];
            Y_ABORT_UNLESS(fragment.PartSize == partSize);
            Y_ABORT_UNLESS(fragment.Offset <= partSize && fragment.Size <= partSize - fragment.Offset);
            Y_ABORT_UNLESS(partSet.IsFragment || (!fragment.Offset && fragment.Size == partSize));
            Y_ABORT_UNLESS(fragment.OwnedString.IsContiguous());
            Y_ABORT_UNLESS(!fragment.Size || fragment.Bytes == fragment.OwnedString.Begin().ContiguousData());
            if (haveSource) {
                Y_ABORT_UNLESS(fragment.Offset == offset && fragment.Size == length);
            } else {
                offset = fragment.Offset;
                length = fragment.Size;
                haveSource = true;
            }
            Y_ABORT_UNLESS(fragment.OwnedString.size() >= fragment.Size);
            parts[i] = TRope(fragment.OwnedString.Begin(), fragment.OwnedString.Begin() + fragment.Size);
        }
    }
    Y_ABORT_UNLESS(offset <= Max<ui32>());
    TRope whole;
    ErasureRestoreBlock82(crcMode, partSet.FullDataSize, restoreFullData ? &whole : nullptr,
        parts, restoreMask, offset, partSet.IsFragment);
    for (ui32 i = 0; i < TotalParts; ++i) {
        if (!(partSet.PartsMask >> i & 1) && (restoreMask >> i & 1)) {
            if (partSet.IsFragment) {
                parts[i].Compact();
                partSet.Parts[i].ReferenceTo(parts[i], offset, length,
                    TErasureType(Species).PartSize(crcMode, partSet.FullDataSize));
            } else {
                partSet.Parts[i].ResetToWhole(parts[i]);
            }
        }
    }
    if (restoreFullData) {
        partSet.FullDataFragment.ResetToWhole(whole);
    }
    partSet.MemoryConsumed = partSet.FullDataFragment.MemoryConsumed();
    for (const auto& part : partSet.Parts) {
        partSet.MemoryConsumed += part.MemoryConsumed();
    }
}

}
