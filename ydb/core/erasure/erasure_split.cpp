#include "erasure.h"

static const char ZeroData[4096] = {0};

namespace NKikimr {

    class TZeroBuffer : public IContiguousChunk {
    public:
        TContiguousSpan GetData() const override {
            return {ZeroData, sizeof(ZeroData)};
        }

        TMutableContiguousSpan GetDataMut() override {
            return {const_cast<char*>(ZeroData), sizeof(ZeroData)};
        }

        TMutableContiguousSpan UnsafeGetDataMut() override {
            return {const_cast<char*>(ZeroData), sizeof(ZeroData)};
        }

        size_t GetOccupiedMemorySize() const override {
            return sizeof(ZeroData);
        }
    };

    void ErasureSplitBlock42Prepare(const TRope& whole, std::span<TRope> parts) {
        const ui32 blockSize = 32;
        const ui32 fullBlockSize = 4 * blockSize;
        const ui32 partSize = ((whole.size() + fullBlockSize - 1) & ~(fullBlockSize - 1)) / 4;
        const ui32 fullBlocks = whole.size() / fullBlockSize;
        ui32 remains = whole.size() % fullBlockSize;

        auto iter = whole.begin();

        for (ui32 part = 0; part < 4; ++part) {
            ui32 partLen = fullBlocks * blockSize;
            if (remains >= blockSize || part == 3) {
                const ui32 len = Min(remains, blockSize);
                partLen += len;
                remains -= len;
            }

            auto nextIter = iter + partLen;
            if (auto& r = parts[part]; !r) {
                r = {iter, nextIter};
                Y_DEBUG_ABORT_UNLESS(r.size() == partLen);
                if (const ui32 padding = partSize - r.size()) {
                    TRcBuf buffer(MakeIntrusive<TZeroBuffer>());
                    r.Insert(r.End(), TRcBuf(TRcBuf::Piece, buffer.data(), padding, buffer));
                }
            }
            iter = nextIter;
        }

        if (!parts[4]) {
            parts[4] = TRcBuf::Uninitialized(partSize);
        }

        if (!parts[5]) {
            parts[5] = TRcBuf::Uninitialized(partSize);
        }
    }

    size_t ErasureSplitBlock42(std::span<TRope> parts, size_t offset, size_t size) {
        const ui32 blockSize = 32;

        Y_DEBUG_ABORT_UNLESS(parts[0].size() == parts[1].size());
        Y_DEBUG_ABORT_UNLESS(parts[1].size() == parts[2].size());
        Y_DEBUG_ABORT_UNLESS(parts[2].size() == parts[3].size());

        auto ptrSpan = parts[4].GetContiguousSpanMut();
        Y_DEBUG_ABORT_UNLESS(ptrSpan.size() == parts[0].size());
        ui64 *ptr = reinterpret_cast<ui64*>(ptrSpan.data() + offset);
        auto diagSpan = parts[5].GetContiguousSpanMut();
        Y_DEBUG_ABORT_UNLESS(diagSpan.size() == parts[0].size());
        ui64 *diag = reinterpret_cast<ui64*>(diagSpan.data() + offset);

        auto iter0 = parts[0].begin() + offset;
        auto iter1 = parts[1].begin() + offset;
        auto iter2 = parts[2].begin() + offset;
        auto iter3 = parts[3].begin() + offset;

        size_t bytesProcessed = 0;

        while (iter0.Valid() && size >= blockSize) {
            const size_t size0 = iter0.ContiguousSize();
            const size_t size1 = iter1.ContiguousSize();
            const size_t size2 = iter2.ContiguousSize();
            const size_t size3 = iter3.ContiguousSize();
            const size_t common = Min(size0, size1, size2, size3, size);
            size_t numBlocks = Max<size_t>(1, common / blockSize);
            const size_t numBytes = numBlocks * blockSize;
            size -= numBytes;
            bytesProcessed += numBytes;

#define FETCH_BLOCK(I) \
            alignas(32) ui64 temp##I[4]; \
            const ui64 *a##I; \
            if (size##I < blockSize) { \
                a##I = temp##I; \
                iter##I.ExtractPlainDataAndAdvance(temp##I, blockSize); \
                Y_DEBUG_ABORT_UNLESS(numBytes == blockSize && numBlocks == 1); \
            } else { \
                a##I = reinterpret_cast<const ui64*>(iter##I.ContiguousData()); \
                iter##I += numBytes; \
            }

            FETCH_BLOCK(0)
            FETCH_BLOCK(1)
            FETCH_BLOCK(2)
            FETCH_BLOCK(3)

            while (numBlocks--) {
                ui64 d0 = 0;
                ui64 d1 = 0;
                ui64 d2 = 0;
                ui64 d3 = 0;
                ui64 s = 0;

                ptr[0] = a0[0] ^ a1[0] ^ a2[0] ^ a3[0];
                d0 ^= a0[0];
                d1 ^= a1[0];
                d2 ^= a2[0];
                d3 ^= a3[0];

                ptr[1] = a0[1] ^ a1[1] ^ a2[1] ^ a3[1];
                d1 ^= a0[1];
                d2 ^= a1[1];
                d3 ^= a2[1];
                s ^= a3[1];

                ptr[2] = a0[2] ^ a1[2] ^ a2[2] ^ a3[2];
                d2 ^= a0[2];
                d3 ^= a1[2];
                s ^= a2[2];
                d0 ^= a3[2];

                ptr[3] = a0[3] ^ a1[3] ^ a2[3] ^ a3[3];
                d3 ^= a0[3];
                s ^= a1[3];
                d0 ^= a2[3];
                d1 ^= a3[3];

                diag[0] = d0 ^ s;
                diag[1] = d1 ^ s;
                diag[2] = d2 ^ s;
                diag[3] = d3 ^ s;

                ptr += 4;
                diag += 4;

                a0 += 4;
                a1 += 4;
                a2 += 4;
                a3 += 4;
            }
        }

        return bytesProcessed;
    }

    bool ErasureSplit(TErasureType::ECrcMode crcMode, TErasureType erasure, const TRope& whole, std::span<TRope> parts,
            TErasureSplitContext *context) {
        Y_ABORT_UNLESS(parts.size() == erasure.TotalPartCount());

        if (erasure.GetErasure() == TErasureType::Erasure4Plus2Block && crcMode == TErasureType::CrcModeNone) {
            ErasureSplitBlock42Prepare(whole, parts);
            if (context) {
                Y_ABORT_UNLESS(context->MaxSizeAtOnce % 32 == 0);
                context->Offset += ErasureSplitBlock42(parts, context->Offset, context->MaxSizeAtOnce);
                return context->Offset == parts[0].size();
            } else {
                ErasureSplitBlock42(parts, 0, Max<size_t>());
                return true;
            }
        }

        TDataPartSet p;
        TRope copy(whole);
        copy.Compact();
        erasure.SplitData(crcMode, copy, p);
        Y_ABORT_UNLESS(p.Parts.size() == parts.size());
        for (ui32 i = 0; i < parts.size(); ++i) {
            parts[i] = std::move(p.Parts[i].OwnedString);
        }

        return true;
    }

} // NKikimr
