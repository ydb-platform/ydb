#include "tuple.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>
#include <vector>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/bitops.h>
#include <util/generic/buffer.h>

#include <arrow/util/bit_util.h>

#include "layout_converter_common.h"
#include "hashes_calc.h"
#include "packing.h"
#include "tuple_bitmatrix.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

namespace {

// Branchless on purpose: zero keys are common enough to make a branch mispredict.
template <typename TFloat, typename TBits>
Y_FORCE_INLINE void CanonicalizeFloatKey(ui8* data) {
    static_assert(sizeof(TFloat) == sizeof(TBits));
    constexpr TBits signMask = TBits(1) << (sizeof(TBits) * 8 - 1);
    constexpr TBits infBits = std::bit_cast<TBits>(std::numeric_limits<TFloat>::infinity());
    constexpr TBits nanBits = std::bit_cast<TBits>(std::numeric_limits<TFloat>::quiet_NaN());

    const TBits bits = ReadUnaligned<TBits>(data);
    const TBits magnitude = bits & ~signMask;
    TBits canonical = magnitude == 0 ? TBits(0) : bits;
    canonical = magnitude > infBits ? nanBits : canonical;
    WriteUnaligned<TBits>(data, canonical);
}

} // namespace


template <bool EqualNulls>
bool TupleKeysEqual(const TTupleLayout *layout,
                           const ui8 *lhsRow, const ui8 *lhsOverflow,
                           const ui8 *rhsRow, const ui8 *rhsOverflow) {
    if (std::memcmp(lhsRow, rhsRow, layout->KeyColumnsFixedEnd)) {
        return false;
    }

    // TODO: better nulls detection??
    const ui8 rem = layout->KeyColumnsNum % 8;
    const ui8 masks[2] = {static_cast<ui8>((1 << rem) - 1), 0xFF};
    for (i32 i = layout->KeyColumnsNum, byteN = 0; i > 0; i -= 8, byteN++) {
        const ui8 lhsBits = ReadUnaligned<ui8>(lhsRow + layout->BitmaskOffset + byteN);
        const ui8 rhsBits = ReadUnaligned<ui8>(rhsRow + layout->BitmaskOffset + byteN);
        const ui8 mask = masks[i >= 8];
        if constexpr (EqualNulls) {
            const ui8 eqMask = layout->GetEqualNullsMaskByte(byteN) & mask;
            const ui8 reqMask = mask & ~eqMask;
            if ((lhsBits & rhsBits & reqMask) != reqMask || (lhsBits & eqMask) != (rhsBits & eqMask)) {
                return false;
            }
        } else if (((lhsBits & mask) != mask) || ((rhsBits & mask) != mask)) {
            return false;
        }
    }

    for (auto colInd = layout->KeyColumnsFixedNum; colInd != layout->KeyColumnsNum; ++colInd) {
        const auto &col = layout->Columns[colInd];
        if constexpr (EqualNulls) {
            if (layout->EqualNullsKeyMask.Get(colInd)) {
                const ui8 bit =
                    (ReadUnaligned<ui8>(lhsRow + layout->BitmaskOffset + colInd / 8) >> (colInd % 8)) & 1u;
                if (bit == 0) {
                    continue; // both sides are NULL; payload of a null variable key is not meaningful
                }
            }
        }

        const auto lhsPrefSize = ReadUnaligned<ui8>(lhsRow + col.Offset);
        const auto rhsPrefSize = ReadUnaligned<ui8>(rhsRow + col.Offset);
        if (lhsPrefSize != rhsPrefSize) {
            return false;
        }

        if (lhsPrefSize < 255) {
            if (std::memcmp(lhsRow + col.Offset + 1,
                            rhsRow + col.Offset + 1, lhsPrefSize)) {
                return false;
            }
        } else {
            const auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
            const auto lhsOverflowOffset = ReadUnaligned<ui32>(
                lhsRow + col.Offset + 1 + 0 * sizeof(ui32));
            const auto lhsOverflowSize = ReadUnaligned<ui32>(
                lhsRow + col.Offset + 1 + 1 * sizeof(ui32));
            const auto rhsOverflowOffset = ReadUnaligned<ui32>(
                rhsRow + col.Offset + 1 + 0 * sizeof(ui32));
            const auto rhsOverflowSize = ReadUnaligned<ui32>(
                rhsRow + col.Offset + 1 + 1 * sizeof(ui32));

            if (lhsOverflowSize != rhsOverflowSize ||
                std::memcmp(lhsRow + col.Offset + 1 + 2 * sizeof(ui32),
                            rhsRow + col.Offset + 1 + 2 * sizeof(ui32),
                            prefixSize) ||
                std::memcmp(lhsOverflow + lhsOverflowOffset,
                            rhsOverflow + rhsOverflowOffset,
                            lhsOverflowSize)) {
                return false;
            }
        }
    }

    return true;
}

template bool TupleKeysEqual<false>(const TTupleLayout *layout,
    const ui8 *lhsRow, const ui8 *lhsOverflow,
    const ui8 *rhsRow, const ui8 *rhsOverflow);
template bool TupleKeysEqual<true>(const TTupleLayout *layout,
    const ui8 *lhsRow, const ui8 *lhsOverflow,
    const ui8 *rhsRow, const ui8 *rhsOverflow);

/// used just for having AN order on tuples
/// cant rely on that comparison in any other way
bool TTupleLayout::KeysLess(const ui8 *lhsRow, const ui8 *lhsOverflow,
                            const ui8 *rhsRow, const ui8 *rhsOverflow) const {
    int cmpRes;

    cmpRes = std::memcmp(lhsRow, rhsRow, KeyColumnsFixedEnd);
    if (cmpRes) {
        return cmpRes < 0;
    }
    cmpRes = std::memcmp(lhsRow + BitmaskOffset, rhsRow + BitmaskOffset,
                         BitmaskSize);
    if (cmpRes) {
        return cmpRes < 0;
    }

    for (auto colInd = KeyColumnsFixedNum; colInd != KeyColumnsNum; ++colInd) {
        const auto &col = Columns[colInd];

        const auto lhsPrefSize = ReadUnaligned<ui8>(lhsRow + col.Offset);
        const auto rhsPrefSize = ReadUnaligned<ui8>(rhsRow + col.Offset);
        if (lhsPrefSize != rhsPrefSize) {
            return lhsPrefSize < rhsPrefSize;
        }

        if (lhsPrefSize < 255) {
            cmpRes = std::memcmp(lhsRow + col.Offset + 1,
                                 rhsRow + col.Offset + 1, lhsPrefSize);
            if (cmpRes) {
                return cmpRes < 0;
            }
        } else {
            const auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
            const auto lhsOverflowOffset =
                ReadUnaligned<ui32>(lhsRow + col.Offset + 1 + 0 * sizeof(ui32));
            const auto lhsOverflowSize =
                ReadUnaligned<ui32>(lhsRow + col.Offset + 1 + 1 * sizeof(ui32));
            const auto rhsOverflowOffset =
                ReadUnaligned<ui32>(rhsRow + col.Offset + 1 + 0 * sizeof(ui32));
            const auto rhsOverflowSize =
                ReadUnaligned<ui32>(rhsRow + col.Offset + 1 + 1 * sizeof(ui32));

            if (lhsOverflowSize != rhsOverflowSize) {
                return lhsOverflowSize < rhsOverflowSize;
            }
            cmpRes = std::memcmp(lhsRow + col.Offset + 1 + 2 * sizeof(ui32),
                                 rhsRow + col.Offset + 1 + 2 * sizeof(ui32),
                                 prefixSize);
            if (cmpRes) {
                return cmpRes < 0;
            }
            cmpRes =
                std::memcmp(lhsOverflow + lhsOverflowOffset,
                            rhsOverflow + rhsOverflowOffset, lhsOverflowSize);
            if (cmpRes) {
                return cmpRes < 0;
            }
        }
    }

    return false;
}

void TTupleLayout::StashFloatOriginals(ui8* rows, ui32 count) const {
    const ui32 rowSize = TotalRowSize;
    for (const auto& orig : FloatOriginals) {
        for (ui32 i = 0; i < count; ++i) {
            ui8* row = rows + i * rowSize;
            std::memcpy(row + orig.PayloadOffset, row + orig.KeyOffset, orig.Size);
        }
    }
}

void TTupleLayout::RestoreFloatOriginalsImpl(ui8** columns, const ui8* rows, ui32 start, ui32 count) const {
    const ui32 rowSize = TotalRowSize;
    for (const auto& orig : FloatOriginals) {
        for (ui32 i = 0; i < count; ++i) {
            std::memcpy(columns[orig.OriginalIndex] + (start + i) * orig.Size,
                        rows + i * rowSize + orig.PayloadOffset, orig.Size);
        }
    }
}

void TTupleLayout::CanonicalizeFloatKeysImpl(ui8* rows, ui32 count) const {
    const ui32 rowSize = TotalRowSize;
    for (const ui32 offset : FloatKeyOffsets) {
        for (ui32 i = 0; i < count; ++i) {
            CanonicalizeFloatKey<float, ui32>(rows + i * rowSize + offset);
        }
    }
    for (const ui32 offset : DoubleKeyOffsets) {
        for (ui32 i = 0; i < count; ++i) {
            CanonicalizeFloatKey<double, ui64>(rows + i * rowSize + offset);
        }
    }
}

void TTupleLayout::ApplyEqualNulls(const std::vector<ui32>& equalNullsInputColumns) {
    EqualNullsKeyMask.Clear();
    HasEqualNullsKeys = false;
    for (ui32 j = 0; j < KeyColumnsNum; ++j) {
        if (std::find(equalNullsInputColumns.begin(), equalNullsInputColumns.end(),
                      KeyColumns[j].OriginalColumnIndex) != equalNullsInputColumns.end()) {
            EqualNullsKeyMask.Set(j);
            HasEqualNullsKeys = true;
        }
    }
}

THolder<TTupleLayout>
TTupleLayout::Create(const std::vector<TColumnDesc> &columns) {
#ifdef USE_X86_SIMD
    if (NX86::HaveAVX2())
        return CreateAVX2TupleLayout(columns);

    if (NX86::HaveSSE42())
        return CreateSSE42TupleLayout(columns);
#endif

    return MakeHolder<TTupleLayoutFallback>(
        columns);
}

TTupleLayoutFallback::TTupleLayoutFallback(
    const std::vector<TColumnDesc> &columns)
    : TTupleLayout(columns) {

    for (ui32 i = 0, idx = 0; i < OrigColumns.size(); ++i) {
        auto &col = OrigColumns[i];

        col.OriginalIndex = idx;
        col.OriginalColumnIndex = i;

        if (col.SizeType == EColumnSizeType::Variable) {
            // we cannot handle (rare) overflow strings unless we have at least
            // space for header; size of inlined strings is limited to 254
            // bytes, limit maximum inline data size
            col.DataSize = std::max<ui32>(1 + 2 * sizeof(ui32),
                                          std::min<ui32>(255, col.DataSize));
            idx += 2; // Variable-size takes two buffers: one for offsets, and
                      // another for payload
        } else {
            idx += 1;
        }

        if (col.Role == EColumnRole::Key) {
            KeyColumns.push_back(col);
        } else {
            PayloadColumns.push_back(col);
        }
    }

    KeyColumnsNum = KeyColumns.size();

    auto ColumnDescLess = [](const TColumnDesc &a, const TColumnDesc &b) {
        if (a.SizeType != b.SizeType) // Fixed first
            return a.SizeType == EColumnSizeType::Fixed;

        if (a.DataSize == b.DataSize)
            // relative order of (otherwise) same key columns must be preserved
            return a.OriginalIndex < b.OriginalIndex;

        return a.DataSize < b.DataSize;
    };

    std::sort(KeyColumns.begin(), KeyColumns.end(), ColumnDescLess);
    std::sort(PayloadColumns.begin(), PayloadColumns.end(), ColumnDescLess);

    KeyColumnsFixedEnd = 0;

    ui32 currOffset = 4; // crc32 hash in the beginning
    KeyColumnsOffset = currOffset;
    KeyColumnsFixedNum = KeyColumnsNum;

    for (ui32 i = 0; i < KeyColumnsNum; ++i) {
        auto &col = KeyColumns[i];

        if (col.SizeType == EColumnSizeType::Variable &&
            KeyColumnsFixedEnd == 0) {
            KeyColumnsFixedEnd = currOffset;
            KeyColumnsFixedNum = i;
        }

        col.ColumnIndex = i;
        col.Offset = currOffset;
        Columns.push_back(col);
        currOffset += col.DataSize;

        if (col.FloatingPoint) {
            Y_ENSURE(col.SizeType == EColumnSizeType::Fixed &&
                         (col.DataSize == sizeof(float) || col.DataSize == sizeof(double)),
                     "floating point key column must be a fixed Float or Double");
            auto& offsets = col.DataSize == sizeof(float) ? FloatKeyOffsets : DoubleKeyOffsets;
            offsets.push_back(col.Offset);
        }
    }
    HasFloatingPointKeys = !FloatKeyOffsets.empty() || !DoubleKeyOffsets.empty();

    KeyColumnsEnd = currOffset;

    if (KeyColumnsFixedEnd == 0) // >= 4 if was ever assigned
        KeyColumnsFixedEnd = KeyColumnsEnd;

    KeyColumnsSize = KeyColumnsEnd - KeyColumnsOffset;

    /// if layout contains varsize keys or null byte of 8 cols is not enough
    if (KeyColumnsFixedNum != KeyColumnsNum || KeyColumnsNum > 8 ||
        !arrow::BitUtil::IsPowerOf2(uint64_t(KeyColumnsSize)) ||
        KeyColumnsSize > (1 << 4)) {
        KeySizeTag_ = 5;
    } else {
        KeySizeTag_ = arrow::BitUtil::CountTrailingZeros(KeyColumnsSize);
    }

    BitmaskOffset = currOffset;

    BitmaskSize = (OrigColumns.size() + 7) / 8;

    currOffset += BitmaskSize;
    BitmaskEnd = currOffset;

    PayloadOffset = currOffset;

    for (ui32 i = 0; i < PayloadColumns.size(); ++i) {
        auto &col = PayloadColumns[i];
        col.ColumnIndex = KeyColumnsNum + i;
        col.Offset = currOffset;
        Columns.push_back(col);
        currOffset += col.DataSize;
    }

    // Original float/double key bytes. Not a real column: the null bitmask and
    // column index space stay sized by the input columns. Unpack writes this
    // slot back over the canonical key.
    for (const auto& key : KeyColumns) {
        if (!key.FloatingPoint) {
            continue;
        }
        FloatOriginals.push_back({key.Offset, currOffset, key.DataSize, key.OriginalIndex});
        currOffset += key.DataSize;
    }

    PayloadEnd = currOffset;
    PayloadSize = PayloadEnd - PayloadOffset;

    TotalRowSize = currOffset;

    for (auto &col : Columns) {
        if (col.SizeType == EColumnSizeType::Variable) {
            VariableColumns.push_back(col);
        } else if (col.DataSize == 0) {
            // singular types (void/null...) no payload bytes to pack/unpack
        } else if (IsPowerOf2(col.DataSize) &&
                   col.DataSize < (1u << FixedPOTColumns_.size())) {
            FixedPOTColumns_[CountTrailingZeroBits(col.DataSize)].push_back(
                col);
        } else {
            FixedNPOTColumns_.push_back(col);
        }
    }
}

// Columns (SoA) format:
//   for fixed size: packed data
//   for variable size: offset (ui32) into next column; size of colum is
//   rowCount + 1
//
// Row (AoS) format:
//   fixed size: packed data
//   variable size:
//     assumes DataSize <= 255 && DataSize >= 1 + 2*4
//     if size of payload is less than col.DataSize:
//       u8 one byte of size (0..254)
//       u8 [size] data
//       u8 [DataSize - 1 - size] padding
//     if size of payload is greater than DataSize:
//       u8 = 255
//       u32 = offset in overflow buffer
//       u32 = size
//       u8 [DataSize - 1 - 2*4] initial bytes of data
// Data is expected to be consistent with isValidBitmask (0 for fixed-size,
// empty for variable-size)
void TTupleLayoutFallback::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const {
    using TTraits = NSimd::TSimdFallbackTraits;

    const ui64 bitmaskTail =
        BitmaskSize * 8 == OrigColumns.size()
            ? 0
            : ~0ull << ((OrigColumns.size() + 8 - BitmaskSize * 8) * 8);
    std::vector<ui64> bitmaskMatrix(BitmaskSize, 0);
    bitmaskMatrix.back() = bitmaskTail;

    if (auto off = (start % 8)) {
        auto bitmaskIdx = start / 8;

        for (ui32 j = Columns.size(); j--;) {
            const ui64 byte =
                isValidBitmask[Columns[j].OriginalIndex]
                    ? isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx]
                    : 0xFF;
            bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
        }

        for (auto &m : bitmaskMatrix) {
            m = transposeBitmatrix(m);
            m >>= off * 8;
        }
    }

    for (; count--; ++start, res += TotalRowSize) {
        auto bitmaskIdx = start / 8;

        if ((start % 8) == 0) {
            std::fill(bitmaskMatrix.begin(), bitmaskMatrix.end(), 0);
            bitmaskMatrix.back() = bitmaskTail;

            for (ui32 j = Columns.size(); j--;) {
                const ui64 byte =
                isValidBitmask[Columns[j].OriginalIndex]
                    ? isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx]
                    : 0xFF;
                bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
            }
            for (auto &m : bitmaskMatrix)
                m = transposeBitmatrix(m);
        }

        for (ui32 j = 0; j < BitmaskSize; ++j) {
            res[BitmaskOffset + j] = ui8(bitmaskMatrix[j]);
            bitmaskMatrix[j] >>= 8;
        }

        for (auto &col : FixedNPOTColumns_) {
            std::memcpy(res + col.Offset,
                        columns[col.OriginalIndex] + start * col.DataSize,
                        col.DataSize);
        }

#define PackPOTColumn(POT)                                                     \
    for (auto &col : FixedPOTColumns_[POT]) {                                  \
        std::memcpy(res + col.Offset,                                          \
                    columns[col.OriginalIndex] + start * (1u << POT),          \
                    1u << POT);                                                \
    }

        PackPOTColumn(0);
        PackPOTColumn(1);
        PackPOTColumn(2);
        PackPOTColumn(3);
        PackPOTColumn(4);
#undef PackPOTColumn

        CanonicalizeFloatKeys(res, 1);
        NormalizeEqualNullsFixedKeys(res);

        ui32 hash = CalculateCRC32<TTraits>(
            res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

        for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
            if (!HashVariableKey(res, i)) {
                continue;
            }
            auto &col = KeyColumns[i];
            auto dataOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * start);
            auto nextOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;
            auto data = columns[col.OriginalIndex + 1] + dataOffset;

            // hash = CalculateCRC32<TTraits>((ui8 *)&size, sizeof(size), hash);
            hash = CalculateCRC32<TTraits>(data, size, hash);
        }

        // isValid bitmap is NOT included into hashed data
        WriteUnaligned<ui32>(res, hash);

        for (auto &col : VariableColumns) {
            auto dataOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * start);
            auto nextOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;
            auto data = columns[col.OriginalIndex + 1] + dataOffset;
            if (size >= col.DataSize) {
                res[col.Offset] = 255;

                auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                auto overflowSize = size - prefixSize;
                auto overflowOffset = overflow.size();

                overflow.resize(overflowOffset + overflowSize);

                WriteUnaligned<ui32>(res + col.Offset + 1 +
                                        0 * sizeof(ui32),
                                    overflowOffset);
                WriteUnaligned<ui32>(
                    res + col.Offset + 1 + 1 * sizeof(ui32), overflowSize);
                std::memcpy(res + col.Offset + 1 + 2 * sizeof(ui32), data,
                            prefixSize);
                std::memcpy(overflow.data() + overflowOffset,
                            data + prefixSize, overflowSize);
            } else {
                Y_DEBUG_ABORT_UNLESS(size < 255);
                res[col.Offset] = size;
                std::memcpy(res + col.Offset + 1, data, size);
                std::memset(res + col.Offset + 1 + size, 0,
                            col.DataSize - (size + 1));
            }
        }
    }
}

void TTupleLayoutFallback::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const {
    const ui8* const rows = res;
    const ui32 rowStart = start;
    const ui32 rowCount = count;
    std::vector<ui64> bitmaskMatrix(BitmaskSize, 0);

    {
        const auto bitmaskIdx = start / 8;
        const auto bitmaskShift = start % 8;
        const auto bitmaskIdxC = (start + count) / 8;
        const auto bitmaskShiftC = (start + count) % 8;

        /// ready first bitmatrix bytes
        for (ui32 j = Columns.size(); j--;) {
            const ui64 byte =
                isValidBitmask[Columns[j].OriginalIndex]
                    ? isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx] &
                          ~(0xFF << bitmaskShift)
                    : 0xFF;
            bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
        }

        /// ready last (which are same as above) bitmatrix bytes if needed
        if (bitmaskIdx == bitmaskIdxC)
            for (ui32 j = Columns.size(); j--;) {
                const ui64 byte = isValidBitmask[Columns[j].OriginalIndex]
                                      ? isValidBitmask[Columns[j].OriginalIndex]
                                                      [bitmaskIdxC] &
                                            (0xFF << bitmaskShiftC)
                                      : 0xFF;
                bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
            }

        for (auto &m : bitmaskMatrix)
            m = transposeBitmatrix(m);
    }

    for (auto ind = 0; ind != start % 8; ++ind) {
        for (ui32 j = 0; j < BitmaskSize; ++j) {
            bitmaskMatrix[j] |=
                ui64(
                    (res - (start % 8 - ind) * TotalRowSize)[BitmaskOffset + j])
                << (ind * 8);
        }
    }

    for (; count--; ++start, res += TotalRowSize) {
        const auto bitmaskIdx = start / 8;
        const auto bitmaskShift = start % 8;

        for (ui32 j = 0; j < BitmaskSize; ++j) {
            bitmaskMatrix[j] |= ui64(res[BitmaskOffset + j])
                                << (bitmaskShift * 8);
        }

        if (bitmaskShift == 7 || count == 0) {
            for (auto &m : bitmaskMatrix)
                m = transposeBitmatrix(m);
            for (ui32 j = Columns.size(); j--;)
                if (isValidBitmask[Columns[j].OriginalIndex])
                    isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx] =
                        ui8(bitmaskMatrix[j / 8] >> ((j % 8) * 8));
            std::fill(bitmaskMatrix.begin(), bitmaskMatrix.end(), 0);

            if (count && count < 8) {
                /// ready last bitmatrix bytes
                for (ui32 j = Columns.size(); j--;) {
                    const ui64 byte =
                        isValidBitmask[Columns[j].OriginalIndex]
                            ? isValidBitmask[Columns[j].OriginalIndex]
                                            [bitmaskIdx + 1] &
                                  (0xFF << count)
                            : 0xFF;
                    bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
                }

                for (auto &m : bitmaskMatrix)
                    m = transposeBitmatrix(m);
            }
        }

        for (auto &col : FixedNPOTColumns_) {
            std::memcpy(columns[col.OriginalIndex] + start * col.DataSize,
                        res + col.Offset, col.DataSize);
        }

#define PackPOTColumn(POT)                                                     \
    for (auto &col : FixedPOTColumns_[POT]) {                                  \
        std::memcpy(columns[col.OriginalIndex] + start * (1u << POT),          \
                    res + col.Offset, 1u << POT);                              \
    }
        PackPOTColumn(0);
        PackPOTColumn(1);
        PackPOTColumn(2);
        PackPOTColumn(3);
        PackPOTColumn(4);
#undef PackPOTColumn

        for (auto &col : VariableColumns) {
            const auto dataOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * start);
            auto *const data = columns[col.OriginalIndex + 1] + dataOffset;

            ui32 size = ReadUnaligned<ui8>(res + col.Offset);

            if (size < 255) { // embedded str
                std::memcpy(data, res + col.Offset + 1, size);
            } else { // overflow buffer used
                const auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                const auto overflowOffset = ReadUnaligned<ui32>(
                    res + col.Offset + 1 + 0 * sizeof(ui32));
                const auto overflowSize = ReadUnaligned<ui32>(
                    res + col.Offset + 1 + 1 * sizeof(ui32));

                std::memcpy(data, res + col.Offset + 1 + 2 * sizeof(ui32),
                            prefixSize);
                std::memcpy(data + prefixSize, overflow.data() + overflowOffset,
                            overflowSize);

                size = prefixSize + overflowSize;
            }

            WriteUnaligned<ui32>(columns[col.OriginalIndex] +
                                     sizeof(ui32) * (start + 1),
                                 dataOffset + size);
        }
    }

    RestoreFloatOriginals(columns, rows, rowStart, rowCount);
}

void TTupleLayoutFallback::BucketPack(
    const ui8 **columns, const ui8 **isValidBitmask,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows, ui32 start,
    ui32 count, ui32 bucketsLogNum) const {
    using TTraits = NSimd::TSimdFallbackTraits;

    if (bucketsLogNum == 0) {
        auto& bres = reses[0];
        const auto size = bres.size();

        bres.resize(size + count * TotalRowSize);
        auto* const res = bres.data() + size;

        Pack(columns, isValidBitmask, res, overflows[0], start, count);
        return;
    }

    std::vector<ui8> resbuf(TotalRowSize);
    ui8 *const res = resbuf.data();

    for (ui32 bucket = 0; bucket < (1u << bucketsLogNum); ++bucket) {
        auto &bres = reses[bucket];
        /// memory reserve heuristic
        bres.reserve(bres.size() + ((count >> bucketsLogNum) + 1) * 9 / 8);
    }

    const ui64 bitmaskTail =
        BitmaskSize * 8 == OrigColumns.size()
            ? 0
            : ~0ull << ((OrigColumns.size() + 8 - BitmaskSize * 8) * 8);
    std::vector<ui64> bitmaskMatrix(BitmaskSize, 0);
    bitmaskMatrix.back() = bitmaskTail;

    if (auto off = (start % 8)) {
        auto bitmaskIdx = start / 8;

        for (ui32 j = Columns.size(); j--;) {
            const ui64 byte =
                isValidBitmask[Columns[j].OriginalIndex]
                    ? isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx]
                    : 0xFF;
            bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
        }

        for (auto &m : bitmaskMatrix) {
            m = transposeBitmatrix(m);
            m >>= off * 8;
        }
    }

    for (; count--; ++start) {
        auto bitmaskIdx = start / 8;

        if ((start % 8) == 0) {
            std::fill(bitmaskMatrix.begin(), bitmaskMatrix.end(), 0);
            bitmaskMatrix.back() = bitmaskTail;

            for (ui32 j = Columns.size(); j--;) {
                const ui64 byte =
                isValidBitmask[Columns[j].OriginalIndex]
                    ? isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx]
                    : 0xFF;
                bitmaskMatrix[j / 8] |= byte << ((j % 8) * 8);
            }
            for (auto &m : bitmaskMatrix)
                m = transposeBitmatrix(m);
        }

        for (ui32 j = 0; j < BitmaskSize; ++j) {
            res[BitmaskOffset + j] = ui8(bitmaskMatrix[j]);
            bitmaskMatrix[j] >>= 8;
        }

        for (auto &col : FixedNPOTColumns_) {
            std::memcpy(res + col.Offset,
                        columns[col.OriginalIndex] + start * col.DataSize,
                        col.DataSize);
        }

#define PackPOTColumn(POT)                                                     \
    for (auto &col : FixedPOTColumns_[POT]) {                                  \
        std::memcpy(res + col.Offset,                                          \
                    columns[col.OriginalIndex] + start * (1u << POT),          \
                    1u << POT);                                                \
    }

        PackPOTColumn(0);
        PackPOTColumn(1);
        PackPOTColumn(2);
        PackPOTColumn(3);
        PackPOTColumn(4);
#undef PackPOTColumn

        CanonicalizeFloatKeys(res, 1);
        NormalizeEqualNullsFixedKeys(res);

        ui32 hash = CalculateCRC32<TTraits>(
            res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

        for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
            if (!HashVariableKey(res, i)) {
                continue;
            }
            auto &col = KeyColumns[i];
            auto dataOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * start);
            auto nextOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;
            auto data = columns[col.OriginalIndex + 1] + dataOffset;

            // hash = CalculateCRC32<TTraits>((ui8 *)&size, sizeof(size), hash);
            hash = CalculateCRC32<TTraits>(data, size, hash);
        }

        // isValid bitmap is NOT included into hashed data
        WriteUnaligned<ui32>(res, hash);

        /// most-significant bits of hash
        const auto bucket = hash >> (sizeof(hash) * 8 - bucketsLogNum);

        auto& overflow = overflows[bucket];

        for (auto &col : VariableColumns) {
            auto dataOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * start);
            auto nextOffset = ReadUnaligned<ui32>(
                columns[col.OriginalIndex] + sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;
            auto data = columns[col.OriginalIndex + 1] + dataOffset;

            if (size >= col.DataSize) {
                res[col.Offset] = 255;

                auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                auto overflowSize = size - prefixSize;
                auto overflowOffset = overflow.size();

                overflow.resize(overflowOffset + overflowSize);

                WriteUnaligned<ui32>(res + col.Offset + 1 +
                                        0 * sizeof(ui32),
                                    overflowOffset);
                WriteUnaligned<ui32>(
                    res + col.Offset + 1 + 1 * sizeof(ui32), overflowSize);
                std::memcpy(res + col.Offset + 1 + 2 * sizeof(ui32), data,
                            prefixSize);
                std::memcpy(overflow.data() + overflowOffset,
                            data + prefixSize, overflowSize);
            } else {
                Y_DEBUG_ABORT_UNLESS(size < 255);
                res[col.Offset] = size;
                std::memcpy(res + col.Offset + 1, data, size);
                std::memset(res + col.Offset + 1 + size, 0,
                            col.DataSize - (size + 1));
            }
        }

        auto &bres = reses[bucket];
        bres.resize_uninitialized(bres.size() + TotalRowSize);
        std::memcpy(bres.data() + bres.size() - TotalRowSize, res, TotalRowSize);
    }
}

void TTupleLayout::CalculateColumnSizes(
    const ui8 *res, ui32 count,
    std::vector<ui64, TMKQLAllocator<ui64>> &bytes) const {

    bytes.resize(Columns.size());

    // handle fixed size columns
    for (const auto& column: OrigColumns) {
        if (column.SizeType == EColumnSizeType::Fixed) {
            bytes[column.OriginalColumnIndex] = column.DataSize * count;
        }
    }

    // handle variable size columns
    for (; count--; res += TotalRowSize) {
        for (const auto& col: VariableColumns) {
            ui32 size = ReadUnaligned<ui8>(res + col.Offset);
            if (size == 255) { // overflow buffer used
                const auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                const auto overflowSize = ReadUnaligned<ui32>(res + col.Offset + 1 + 1 * sizeof(ui32));
                size = prefixSize + overflowSize;
            }
            bytes[col.OriginalColumnIndex] += size;
        }
    }
}

void TTupleLayout::TupleDeepCopy(
    const ui8* inTuple, const ui8* inOverflow,
    ui8* outTuple, ui8* outOverflow, ui64& outOverflowSize) const
{
    std::memcpy(outTuple, inTuple, TotalRowSize);
    for (const auto& col: VariableColumns) {
        ui32 size = ReadUnaligned<ui8>(inTuple + col.Offset);
        if (size == 255) { // overflow buffer used
            auto overflowOffset = ReadUnaligned<ui32>(inTuple + col.Offset + 1 + 0 * sizeof(ui32));
            auto overflowSize   = ReadUnaligned<ui32>(inTuple + col.Offset + 1 + 1 * sizeof(ui32));
            std::memcpy(outOverflow, inOverflow + overflowOffset, overflowSize);
            MKQL_ENSURE(outOverflowSize <= std::numeric_limits<ui32>::max(),
                        "overflow offset exceeds ui32 range");
            WriteUnaligned<ui32>(outTuple + col.Offset + 1 + 0 * sizeof(ui32), outOverflowSize);
            outOverflowSize += overflowSize;
        }
    }
}

void TTupleLayout::TupleDeepCopy(
    const ui8* inTuple, const ui8* inOverflow,
    TTupleData& outTuple, TTupleData& outOverflow) const
{
    auto appendRange = [](TTupleData& to, std::span<const ui8> range) {
        MKQL_ENSURE(to.size() <= std::numeric_limits<ui32>::max(), "tuple buffer exceeds ui32 range");
        ui32 offset = to.size();
        ui32 writeSize = range.size();
        to.resize(offset + writeSize);
        std::memcpy(to.data() + offset, range.data(), writeSize);
    };
    ui32 initSize = outTuple.size();
    appendRange(outTuple, {inTuple, TotalRowSize});
    for (const auto& col: VariableColumns) {
        ui32 size = ReadUnaligned<ui8>(inTuple + col.Offset);
        if (size == 255) { // overflow buffer used
            auto overflowOffset = ReadUnaligned<ui32>(inTuple + col.Offset + 1 + 0 * sizeof(ui32));
            auto overflowSize   = ReadUnaligned<ui32>(inTuple + col.Offset + 1 + 1 * sizeof(ui32));
            MKQL_ENSURE(outOverflow.size() <= std::numeric_limits<ui32>::max(),
                        "overflow offset exceeds ui32 range");
            ui32 outOverflowOffset = outOverflow.size();
            appendRange(outOverflow, {inOverflow + overflowOffset, overflowSize});
            WriteUnaligned<ui32>(outTuple.data() + initSize + col.Offset + 1 + 0 * sizeof(ui32), outOverflowOffset);
        }
    }
}

/// TODO: write unit tests
void TTupleLayout::Concat(
    std::vector<ui8, TMKQLAllocator<ui8>>& dst,
        std::vector<ui8, TMKQLAllocator<ui8>>& dstOverflow,
        ui32 dstCount,
        const ui8 *src, const ui8 *srcOverflow, ui32 srcCount, ui32 srcOverflowSize) const 
{
    if (srcCount == 0) {
        return;
    }
    MKQL_ENSURE(dstOverflow.size() <= std::numeric_limits<ui32>::max(),
                "overflow buffer exceeds ui32 offset range");
    MKQL_ENSURE(dstOverflow.size() + srcOverflowSize <= std::numeric_limits<ui32>::max(),
                "combined overflow would exceed ui32 offset range");
    ui32 dstOverflowOffset = dstOverflow.size();
    if (srcOverflowSize > 0) {
        dstOverflow.resize(dstOverflow.size() + srcOverflowSize);
        std::memcpy(dstOverflow.data() + dstOverflowOffset, srcOverflow, srcOverflowSize);
    }

    constexpr ui32 blockRows = 128;
    dst.resize(static_cast<size_t>(dstCount + srcCount) * TotalRowSize);
    ui8 *dstRow = dst.data() + static_cast<size_t>(dstCount) * TotalRowSize;
    ui32 blockSize;
    
    for (ui32 rowInd = 0; rowInd < srcCount; rowInd += blockRows, 
                                             dstRow += blockSize * TotalRowSize,
                                             src += blockSize * TotalRowSize) {
        blockSize = std::min(srcCount - rowInd, blockRows);
        std::memcpy(dstRow, src, blockSize * TotalRowSize);

        ui8 *res = dstRow;
        for (ui32 blockInd = 0; blockInd < blockSize; blockInd++,
                                                      res += TotalRowSize) {
            for (auto &col : VariableColumns) {
                if (res[col.Offset] == 255) {
                    WriteUnaligned<ui32>(res + col.Offset + 1 + 0 * sizeof(ui32),
                                         dstOverflowOffset);
                    dstOverflowOffset += ReadUnaligned<ui32>(
                        res + col.Offset + 1 + 1 * sizeof(ui32));
                }
            }
        }

    }
}

TPackResult TTupleLayout::Flatten(TArrayRef<TPackResult> tuples) const {
    TPackResult flattened;
    flattened.NTuples = std::accumulate(tuples.begin(), tuples.end(), i64{0},
                                        [](i64 summ, const auto& packRes) { return summ += packRes.NTuples; });

    i64 totalTuplesSize = std::accumulate(tuples.begin(), tuples.end(), i64{0}, [](i64 summ, const auto& packRes) {
        return summ += std::ssize(packRes.PackedTuples);
    });
    flattened.PackedTuples.reserve(totalTuplesSize);

    i64 totalOverflowSize =
        std::accumulate(tuples.begin(), tuples.end(), i64{0},
                        [](i64 summ, const auto& packRes) { return summ += std::ssize(packRes.Overflow); });
    flattened.Overflow.reserve(totalOverflowSize);


    ui32 tupleSize = TotalRowSize;
    for (const TPackResult& tupleBatch : tuples) {
        MKQL_ENSURE(flattened.PackedTuples.size() / tupleSize <= std::numeric_limits<ui32>::max(),
                    "dstCount exceeds ui32 range");
        MKQL_ENSURE(tupleBatch.PackedTuples.size() / tupleSize <= std::numeric_limits<ui32>::max(),
                    "srcCount exceeds ui32 range");
        MKQL_ENSURE(tupleBatch.Overflow.size() <= std::numeric_limits<ui32>::max(),
                    "srcOverflowSize exceeds ui32 range");
        ui32 dstCount = flattened.PackedTuples.size() / tupleSize;
        ui32 srcCount = tupleBatch.PackedTuples.size() / tupleSize;
        ui32 srcOverflowSize = tupleBatch.Overflow.size();
        Concat(flattened.PackedTuples, flattened.Overflow,
                                dstCount, tupleBatch.PackedTuples.data(),
                                tupleBatch.Overflow.data(), srcCount,
                                srcOverflowSize);
    }
    return flattened;

}
ui32 TTupleLayout::GetTupleVarSize(const ui8* inTuple) const {
    ui32 result = 0;
    for (const auto& col: VariableColumns) {
        ui32 size = ReadUnaligned<ui8>(inTuple + col.Offset);
        if (size == 255) { // overflow buffer used
            const auto prefixSize = col.DataSize - 1 - 2 * sizeof(ui32);
            const auto overflowSize = ReadUnaligned<ui32>(inTuple + col.Offset + 1 + 1 * sizeof(ui32));
            size = prefixSize + overflowSize;
        }
        result += size;
    }
    return result;
}

std::string TTupleLayout::Stringify(TSingleTuple tuple) const {
    std::string result;
    result += "{";
    
    const ui8* row = tuple.PackedData;
    const ui8* overflow = tuple.OverflowBegin;
    
    bool first = true;
    for (const auto& col : Columns) {
        if (!first) {
            result += ", ";
        }
        first = false;
        
        result += "[" + std::to_string(col.OriginalColumnIndex) + "]";
        if (col.Role == EColumnRole::Key) {
            result += "K";
        } else {
            result += "P";
        }
        result += ": ";
        
        // Check if value is NULL using bitmask
        ui32 colIdx = col.ColumnIndex;
        ui8 bitmaskByte = ReadUnaligned<ui8>(row + BitmaskOffset + colIdx / 8);
        bool isValid = (bitmaskByte >> (colIdx % 8)) & 1;
        
        if (!isValid) {
            result += "NULL";
            continue;
        }
        
        if (col.SizeType == EColumnSizeType::Variable) {
            // Variable-length field: print as string
            ui8 sizeOrMarker = ReadUnaligned<ui8>(row + col.Offset);
            
            std::string strValue;
            if (sizeOrMarker < 255) {
                // Embedded string
                strValue.assign(reinterpret_cast<const char*>(row + col.Offset + 1), sizeOrMarker);
            } else {
                // Overflow buffer used
                const auto prefixSize = col.DataSize - 1 - 2 * sizeof(ui32);
                const auto overflowOffset = ReadUnaligned<ui32>(row + col.Offset + 1 + 0 * sizeof(ui32));
                const auto overflowSize = ReadUnaligned<ui32>(row + col.Offset + 1 + 1 * sizeof(ui32));
                
                // First copy the prefix
                strValue.assign(reinterpret_cast<const char*>(row + col.Offset + 1 + 2 * sizeof(ui32)), prefixSize);
                // Then append from overflow buffer
                strValue.append(reinterpret_cast<const char*>(overflow + overflowOffset), overflowSize);
            }
            
            result += "\"" + strValue + "\"";
        } else {
            // Fixed-size field: print as integer based on size
            ui64 value = 0;
            switch (col.DataSize) {
                case 1:
                    value = ReadUnaligned<ui8>(row + col.Offset);
                    break;
                case 2:
                    value = ReadUnaligned<ui16>(row + col.Offset);
                    break;
                case 4:
                    value = ReadUnaligned<ui32>(row + col.Offset);
                    break;
                case 8:
                    value = ReadUnaligned<ui64>(row + col.Offset);
                    break;
                default:
                    // For other sizes, print as hex bytes
                    result += "0x";
                    for (ui32 i = 0; i < col.DataSize; ++i) {
                        ui8 byte = ReadUnaligned<ui8>(row + col.Offset + i);
                        static const char hexChars[] = "0123456789abcdef";
                        result += hexChars[(byte >> 4) & 0xF];
                        result += hexChars[byte & 0xF];
                    }
                    continue;
            }
            result += std::to_string(value);
        }
    }
    
    result += "}";
    return result;
}

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
