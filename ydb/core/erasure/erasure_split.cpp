#include "erasure.h"

namespace NKikimr {

    class TZeroBuffer {
        TRcBuf Buffer;

    public:
        TZeroBuffer()
            : Buffer(TRcBuf::Uninitialized(4096))
        {
            memset(Buffer.GetDataMut(), 0, Buffer.size());
        }

        TRcBuf GetBuffer() const {
            return Buffer;
        }
    } ZeroBuffer;

    void ErasureSplitBlock42(TRope&& whole, std::span<TRope> parts) {
        const ui32 blockSize = 32;
        const ui32 fullBlockSize = 4 * blockSize;
        const ui32 partSize = ((whole.size() + fullBlockSize - 1) & ~(fullBlockSize - 1)) / 4;
        const ui32 fullBlocks = whole.size() / fullBlockSize;
        ui32 remains = whole.size() % fullBlockSize;

        auto iter = whole.begin();

        for (ui32 part = 0; part < 4; ++part) {
            ui32 partLen = fullBlocks * blockSize;
            if (remains >= blockSize || part == 3) {
                partLen += std::min(remains, blockSize);
                remains -= blockSize;
            }
            auto& r = parts[part];
            auto nextIter = iter + partLen;
            r = {iter, nextIter};
            Y_VERIFY_DEBUG(r.size() == partLen);
            if (const ui32 padding = partSize - r.size()) {
                auto buffer = ZeroBuffer.GetBuffer();
                r.Insert(r.End(), TRcBuf(TRcBuf::Piece, buffer.data(), padding, buffer));
            }
            iter = nextIter;
        }

        TRcBuf xorPart = TRcBuf::Uninitialized(partSize);
        ui64 *ptr = reinterpret_cast<ui64*>(xorPart.GetDataMut());
        parts[4] = TRope(std::move(xorPart));

        TRcBuf diagPart = TRcBuf::Uninitialized(partSize);
        ui64 *diag = reinterpret_cast<ui64*>(diagPart.GetDataMut());
        parts[5] = TRope(std::move(diagPart));

        auto iter0 = parts[0].begin();
        auto iter1 = parts[1].begin();
        auto iter2 = parts[2].begin();
        auto iter3 = parts[3].begin();

        while (iter0.Valid()) {
            const size_t size0 = iter0.ContiguousSize();
            const size_t size1 = iter1.ContiguousSize();
            const size_t size2 = iter2.ContiguousSize();
            const size_t size3 = iter3.ContiguousSize();
            const size_t common = Min(size0, size1, size2, size3);
            size_t numBlocks = Max<size_t>(1, common / blockSize);
            const size_t numBytes = numBlocks * blockSize;

#define FETCH_BLOCK(I) \
            alignas(32) ui64 temp##I[4]; \
            const ui64 *a##I; \
            if (size##I < blockSize) { \
                a##I = temp##I; \
                iter##I.ExtractPlainDataAndAdvance(temp##I, blockSize); \
                Y_VERIFY_DEBUG(numBytes == blockSize && numBlocks == 1); \
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
    }

    void ErasureSplit(TErasureType::ECrcMode crcMode, TErasureType erasure, TRope&& whole, std::span<TRope> parts) {
        Y_VERIFY(parts.size() == erasure.TotalPartCount());

        if (erasure.GetErasure() == TErasureType::Erasure4Plus2Block && crcMode == TErasureType::CrcModeNone) {
            return ErasureSplitBlock42(std::move(whole), parts);
        }

        TDataPartSet p;
        erasure.SplitData(crcMode, whole, p);
        Y_VERIFY(p.Parts.size() == parts.size());
        for (ui32 i = 0; i < parts.size(); ++i) {
            parts[i] = std::move(p.Parts[i].OwnedString);
        }
    }

} // NKikimr
