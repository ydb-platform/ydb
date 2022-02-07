#include "float_huffman.h"

#include <util/generic/array_ref.h>
#include <util/generic/bitops.h>
#include <util/generic/cast.h>
#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>
#include <util/system/yassert.h>
#include <util/stream/format.h>

namespace NCodecs::NFloatHuff {
    namespace {
        struct THuffEntry {
            ui32 CodeBase;
            ui16 Prefix;
            int PrefLength;
            int CodeLength;
            int TotalLength;
            ui32 Mask;
            ui64 Offset;

            THuffEntry() = default;

            constexpr THuffEntry(ui32 codeBase, ui16 prefix, int prefLength, int codeLength)
                : CodeBase(codeBase)
                , Prefix(prefix)
                , PrefLength(prefLength)
                , CodeLength(codeLength)
                , TotalLength(prefLength + codeLength)
                , Mask(Mask64(codeLength))
                , Offset(-(ui64(codeBase) << prefLength) + prefix)
            {}

            bool Fit(ui32 code) const {
                return code >= CodeBase && code < CodeBase + (1ULL << CodeLength);
            }
        };

        // NB. There is a typo in the penultimate line (34 instead of 24). It was there from the very
        // first commit and we cannot fix it without breaking all the clients.
        constexpr THuffEntry entries[16] = {
            {0x00000000, 0x01, 1, 0}, // Only +0.0f, 1 bit, prefix                     [1]
            {0x3f800000, 0x0e, 4, 0}, // Only +1.0f, 4 bits, prefix                    [0111]
            {0x3f700000, 0x08, 5, 20}, // [0.9375, 1.0), 25 bits, prefix               [00010]
            {0x3f000000, 0x00, 5, 20}, // [0.5, 0.5625), 25 bits, prefx                [00000]
            {0x3f400000, 0x06, 6, 20}, // [0.75, 0.8125), 26 bits, prefix              [011000]
            {0x3f500000, 0x22, 6, 20}, // [0.8125, 0.875), 26 bits, prefix             [010001]
            {0x3f200000, 0x02, 6, 20}, // [0.625, 0.6875), 26 bits, prefix             [010000]
            {0x3f100000, 0x38, 6, 20}, // [0.5625, 0.625), 26 bits, prefix             [000111]
            {0x3f600000, 0x18, 6, 20}, // [0.875, 0.9375), 26 bits, prefix             [000110]
            {0x3f300000, 0x30, 6, 20}, // [0.6875, 0.75), 26 bits, prefix              [000011]
            {0x3e800000, 0x10, 6, 20}, // [0.25, 0.28125), 26 bits, prefix             [000010]
            {0x3e000000, 0x04, 3, 24}, // [0.125, 0.5), 27 bits, prefix                [001]
            {0x3d000000, 0x0a, 4, 24}, // [0.03125, 0.125), 28 bits, prefix            [0101]
            {0x3c000000, 0x12, 5, 24}, // [0.0078125, 0.03125), 29 bits, prefix        [01001]
            {0x3b000000, 0x26, 6, 34}, // [0.001953125, end of range), 40 bits, prefix [011001]
            {0x00000000, 0x16, 5, 32}, // whole range, 37 bits, prefix                 [01101]
        };

        [[noreturn]] Y_NO_INLINE void ThrowInvalidOffset(size_t size, size_t byteOffset) {
            ythrow yexception() <<
                "Decompression error: requested decoding 8 bytes past end of input buffer of " << size << " bytes size at position " << byteOffset << ". ";
        }

        struct THuffInfo {
            constexpr THuffInfo() {
                for (size_t i = 0; i < 64; ++i) {
                    bool init = false;
                    for (size_t j = 0; j != 16; ++j) {
                        ui16 prefix = i & Mask64(entries[j].PrefLength);
                        if (entries[j].Prefix == prefix) {
                            init = true;
                            DecodeLookup[i] = entries[j];
                            break;
                        }
                    }
                    Y_ASSERT(init);
                }

                for (ui32 i = 0; i < (1 << 12); ++i) {
                    // First two entries (+0.0f and +1.0f) are not present in the lookup, they are handled separately
                    for (int value = 2; value < 16; ++value) {
                        if (entries[value].Fit(i << 20)) {
                            EncodeLookup[i] = value;
                            break;
                        }
                    }
                }
            }

            std::pair<ui64, int> GetCode(ui32 value) const {
                // Zeros are handled separately in the main loop
                Y_ASSERT(value != 0);

                if (value == 0x3f800000) {
                    return {0x0e, 4};
                }

                const auto& entry = entries[EncodeLookup[value >> 20]];

                return {
                    (ui64(value) << entry.PrefLength) + entry.Offset,
                    entry.TotalLength
                };
            }

            THuffEntry DecodeLookup[64];
            ui8 EncodeLookup[1 << 12];
        };

        const THuffInfo huffInfo;
        /// End Of Stream
        const ui32 EOS = ui32(-1);
    }

    TString Encode(TArrayRef<const float> factors) {
        TString result;
        result.resize((factors.size() + 1) * 40 / 8 + 8, 0); // Max code length is 40 bits
        int usedBits = 0;
        ui64 buffer = 0;
        char* writePtr = result.begin();

        auto writeBits = [&](ui64 code, int size) {
            const auto bitsToTransfer = Min(size, 64 - usedBits);
            buffer |= (code << usedBits);
            usedBits += bitsToTransfer;
            if (usedBits == 64) {
                memcpy(writePtr, &buffer, 8);
                usedBits = size - bitsToTransfer;
                if (bitsToTransfer != 64) {
                    buffer = code >> bitsToTransfer;
                } else {
                    buffer = 0;
                }
                writePtr += 8;
            }
        };

        for (size_t i = 0; i != factors.size();) {
            if (BitCast<ui32>(factors[i]) == 0) {
                int zeroCount = 1;
                for (;;) {
                    ++i;
                    if (i == factors.size() || BitCast<ui32>(factors[i]) != 0) {
                        break;
                    }
                    ++zeroCount;
                }
                for (; zeroCount >= 64; zeroCount -= 64) {
                    writeBits(ui64(-1), 64);
                }
                writeBits(Mask64(zeroCount), zeroCount);
            } else {
                const auto [code, codeSize] = huffInfo.GetCode(BitCast<ui32>(factors[i]));
                writeBits(code, codeSize);
                ++i;
            }
        }
        // Write EOS.
        // We use precomputed constants instead of the following:
        //      auto [code, codeSize] = huffInfo.GetCode(EOS);
        //      writeBits(code, codeSize);
        writeBits(211527139302, 40);
        memcpy(writePtr, &buffer, 8);
        result.resize(writePtr - result.begin() + usedBits / 8 + 8);

        return result;
    }

    TDecoder::TDecoder(TStringBuf data)
        : State{
            .Workspace = data.size() < 8 ? ThrowInvalidOffset(data.size(), 0), 0 : ReadUnaligned<ui64>(data.data()),
            .WorkspaceSize = 64,
            .Position = 8,
            .Data = data
        }
    {
        FillDecodeBuffer();
    }

    TVector<float> TDecoder::DecodeAll(size_t sizeHint) {
        TVector<float> result;
        result.reserve(sizeHint);

        while (Begin != End) {
            result.insert(result.end(), Begin, End);
            FillDecodeBuffer();
        }
        return result;
    }

    size_t TDecoder::Decode(TArrayRef<float> dest) {
        size_t count = 0;
        while (count < dest.size()) {
            if (dest.size() - count < size_t(End - Begin)) {
                const auto size = dest.size() - count;
                std::copy(Begin, Begin + size, dest.data() + count);
                Begin += size;
                return dest.size();
            } else {
                std::copy(Begin, End, dest.data() + count);
                count += End - Begin;
                FillDecodeBuffer();
                if (Begin == End) {
                    break;
                }
            }
        }
        return count;
    }

    size_t TDecoder::Skip(size_t count) {
        size_t skippedCount = 0;
        while (skippedCount < count) {
            if (count - skippedCount < size_t(End - Begin)) {
                const auto size = count - skippedCount;
                Begin += size;
                return count;
            } else {
                skippedCount += End - Begin;
                FillDecodeBuffer();
                if (Begin == End) {
                    break;
                }
            }
        }
        return skippedCount;
    }

    bool TDecoder::HasMore() const {
        return Begin != End;
    }

    void TDecoder::FillDecodeBuffer() {
        Begin = DecodeBuffer.data();
        End = DecodeBuffer.data();

        if (HitEos) {
            return;
        }

        // This helps to keep most of the variables in the registers.
        float* end = End;
        TState state = State;

        // It is faster to just zero all the memory here in one go
        // and then avoid inner loop when writing zeros. There we
        // can just increment end pointer.
        std::fill(DecodeBuffer.begin(), DecodeBuffer.end(), 0.0f);

        // Make sure that inside the loop we always have space to put 64 zeros and one other
        // value.
        float* cap = DecodeBuffer.data() + DecodeBuffer.size() - 64 - 1;

        while (end < cap) {
            if (state.Workspace % 2 == 1) {
                // Decode zeros
                // There we can just scan whole state.Workspace for ones because it contains
                // zeros outside of the WorkspaceSize bits.
                const auto negWorkspace = ~state.Workspace;
                const int zeroCount = negWorkspace ? CountTrailingZeroBits(negWorkspace) : 64;
                end += zeroCount;
                state.SkipBits(zeroCount);
                continue;
            }
            if (state.PeekBits(4) == 0x0e) {
                *end++ = 1.0f;
                state.SkipBits(4);
                continue;
            }
            const auto& entry = huffInfo.DecodeLookup[state.PeekBits(6)];
            const auto code = ui32((state.NextBitsUnmasked(entry.TotalLength) >> entry.PrefLength) & entry.Mask) + entry.CodeBase;
            if (Y_UNLIKELY(code == EOS)) {
                HitEos = true;
                break;
            }
            *end++ = BitCast<float>(code);
        }

        End = end;
        State = state;
    }

    ui64 TDecoder::TState::PeekBits(int count) {
        if (WorkspaceSize > count) {
            return Workspace & Mask64(count);
        } else {
            if (Y_UNLIKELY(Position + 8 > Data.size())) {
                ThrowInvalidOffset(Data.size(), Position);
            }
            return (Workspace | (ReadUnaligned<ui64>(Data.data() + Position) << WorkspaceSize)) & Mask64(count);
        }
    }

    ui64 TDecoder::TState::NextBitsUnmasked(int count) {
        if (WorkspaceSize > count) {
            const auto result = Workspace;
            Workspace >>= count;
            WorkspaceSize -= count;
            return result;
        } else {
            if (Y_UNLIKELY(Position + 8 > Data.size())) {
                ThrowInvalidOffset(Data.size(), Position);
            }
            ui64 result = Workspace;
            Workspace = ReadUnaligned<ui64>(Data.data() + Position);
            Position += 8;
            result |= Workspace << WorkspaceSize;
            Workspace >>= count - WorkspaceSize;
            WorkspaceSize += 64 - count;
            return result;
        }
    }

    void TDecoder::TState::SkipBits(int count) {
        if (WorkspaceSize > count) {
            Workspace >>= count;
            WorkspaceSize -= count;
        } else {
            if (Y_UNLIKELY(Position + 8 > Data.size())) {
                ThrowInvalidOffset(Data.size(), Position);
            }
            Workspace = ReadUnaligned<ui64>(Data.data() + Position);
            Position += 8;
            Workspace >>= count - WorkspaceSize;
            WorkspaceSize += 64 - count;
        }
    }

    TVector<float> Decode(TStringBuf data, size_t sizeHint) {
        return TDecoder(data).DecodeAll(sizeHint);
    }
} // namespace NCodecs::NFloatHuff
