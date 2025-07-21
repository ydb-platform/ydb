
#include "tuple.h"

#include <algorithm>
#include <queue>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/bitops.h>
#include <util/generic/buffer.h>

#include <arrow/util/bit_util.h>

#include "hashes_calc.h"
#include "packing.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

namespace {

// Transpose 8x8 bit-matrix packed in ui64 integer
Y_FORCE_INLINE ui64 transposeBitmatrix(ui64 x) {
    /// fast path
    if (x == ~0ull) {
        return x;
    }

    /// transpose 1x1 diagonal elements in 2x2 block
    x = ((x &
          0b10101010'01010101'10101010'01010101'10101010'01010101'10101010'01010101ull)) |
        ((x &
          0b01010101'00000000'01010101'00000000'01010101'00000000'01010101'00000000ull) >>
         7) |
        ((x &
          0b00000000'10101010'00000000'10101010'00000000'10101010'00000000'10101010ull)
         << 7);

    /// transpose 2x2 diagonal elements in 4x4 block
    x = ((x &
          0b1100110011001100'0011001100110011'1100110011001100'0011001100110011ull)) |
        ((x &
          0b0011001100110011'0000000000000000'0011001100110011'0000000000000000ull) >>
         14) |
        ((x &
          0b0000000000000000'1100110011001100'0000000000000000'1100110011001100ull)
         << 14);

    /// transpose 4x4 diagonal elements in 8x8 block
    x = ((x &
          0b11110000111100001111000011110000'00001111000011110000111100001111ull)) |
        ((x &
          0b00001111000011110000111100001111'00000000000000000000000000000000ull) >>
         28) |
        ((x &
          0b00000000000000000000000000000000'11110000111100001111000011110000ull)
         << 28);

    return x;
}

void transposeBitmatrix(ui8 dst[], const ui8 *src[], const size_t row_size) {
    ui64 x = 0;
    for (size_t ind = 0; ind != 8; ++ind) {
        x |= ui64(*src[ind]) << (ind * 8);
    }

    x = transposeBitmatrix(x);

    for (size_t ind = 0; ind != 8; ++ind) {
        dst[ind * row_size] = x;
        x >>= 8;
    }
}

void transposeBitmatrix(ui8 *dst[], const ui8 src[], const size_t row_size) {
    ui64 x = 0;
    for (size_t ind = 0; ind != 8; ++ind) {
        x |= ui64(src[ind * row_size]) << (ind * 8);
    }

    x = transposeBitmatrix(x);

    for (size_t ind = 0; ind != 8; ++ind) {
        *dst[ind] = x;
        x >>= 8;
    }
}

} // namespace


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
        const ui8 midx = (i >= 8);
        if (((lhsBits & masks[midx]) != masks[midx]) || (rhsBits & masks[midx]) != masks[midx]) { // if there is at least one null in key cols
            return false;
        }
    }

    for (auto colInd = layout->KeyColumnsFixedNum; colInd != layout->KeyColumnsNum; ++colInd) {
        const auto &col = layout->Columns[colInd];

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

THolder<TTupleLayout>
TTupleLayout::Create(const std::vector<TColumnDesc> &columns) {

    if (NX86::HaveAVX2())
        return MakeHolder<TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>>(
            columns);

    if (NX86::HaveSSE42())
        return MakeHolder<TTupleLayoutSIMD<NSimd::TSimdSSE42Traits>>(
            columns);

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
    }

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

    PayloadEnd = currOffset;
    PayloadSize = PayloadEnd - PayloadOffset;

    TotalRowSize = currOffset;

    for (auto &col : Columns) {
        if (col.SizeType == EColumnSizeType::Variable) {
            VariableColumns.push_back(col);
        } else if (IsPowerOf2(col.DataSize) &&
                   col.DataSize < (1u << FixedPOTColumns_.size())) {
            FixedPOTColumns_[CountTrailingZeroBits(col.DataSize)].push_back(
                col);
        } else {
            FixedNPOTColumns_.push_back(col);
        }
    }
}

template <typename TTraits>
TTupleLayoutSIMD<TTraits>::TTupleLayoutSIMD(
    const std::vector<TColumnDesc> &columns)
    : TTupleLayoutFallback(columns) {
    BlockRows_ = 128 * std::max(1ul, 128ul / TotalRowSize);
    const bool use_simd = true;

    // pack optimization for small tuple
    const size_t max_simd_tuple_size =
        use_simd && TSimd<ui8>::SIZE > TotalRowSize
            ? TSimd<ui8>::SIZE - TotalRowSize
            : 0;

    std::vector<const TColumnDesc *> fallback_cols;
    std::queue<const TColumnDesc *> next_cols;

    size_t fixed_cols_left =
        KeyColumnsFixedNum +
        std::accumulate(PayloadColumns.begin(), PayloadColumns.end(), 0ul,
                        [](size_t prev, const auto &col) {
                            return prev +
                                   (col.SizeType == EColumnSizeType::Fixed);
                        });

    const auto small_tuple_packing = [&](const std::vector<TColumnDesc>
                                             &columns) {
        for (const auto &col : columns) {
            if (col.SizeType != EColumnSizeType::Fixed) {
                break;
            }
            --fixed_cols_left;

            size_t tuple_size = next_cols.empty()
                                    ? 0
                                    : next_cols.back()->DataSize +
                                          next_cols.back()->Offset -
                                          next_cols.front()->Offset;
            const size_t tuple_size_with_col =
                next_cols.empty()
                    ? col.DataSize
                    : col.DataSize + col.Offset - next_cols.front()->Offset;

            const bool col_pushed = tuple_size_with_col <= max_simd_tuple_size;
            if (col_pushed) {
                next_cols.push(&col);
                tuple_size = tuple_size_with_col;
            }

            if (!SIMDSmallTuple_.Cols && next_cols.size() > 1 &&
                tuple_size <= max_simd_tuple_size &&
                (!col_pushed || !fixed_cols_left)) {
                SIMDSmallTupleDesc simd_desc;
                simd_desc.Cols = next_cols.size();
                simd_desc.RowOffset = next_cols.front()->Offset;
                simd_desc.SmallTupleSize = tuple_size;

                const TColumnDesc *col_descs[kSIMDMaxCols];
                ui32 col_max_size = 0;
                for (ui8 col_ind = 0; col_ind != simd_desc.Cols; ++col_ind) {
                    col_descs[col_ind] = next_cols.front();
                    col_max_size =
                        std::max(col_max_size, col_descs[col_ind]->DataSize);
                    next_cols.pop();
                }

                const auto tuples_per_register = std::max(
                    1ul, 1ul + (TSimd<ui8>::SIZE - tuple_size) / TotalRowSize);

                simd_desc.InnerLoopIters = std::min(
                    size_t(kSIMDMaxInnerLoopSize),
                    (TSimd<ui8>::SIZE / col_max_size) / tuples_per_register);

                for (ui8 col_ind = 0; col_ind != simd_desc.Cols; ++col_ind) {
                    const auto &col_desc = col_descs[col_ind];
                    const size_t offset =
                        col_desc->Offset - simd_desc.RowOffset;

                    BlockColsOffsets_.push_back(offset);
                    BlockFixedColsSizes_.push_back(col_desc->DataSize);
                    BlockColumnsOrigInds_.push_back(col_desc->OriginalIndex);
                }

                for (size_t packing_flag = 1; packing_flag != 3;
                     ++packing_flag) {
                    for (ui8 col_ind = 0; col_ind != simd_desc.Cols;
                         ++col_ind) {
                        const auto col_desc = col_descs[col_ind];
                        const size_t offset =
                            col_desc->Offset - simd_desc.RowOffset;

                        for (ui8 ind = 0; ind != simd_desc.InnerLoopIters;
                             ++ind) {
                            auto perm = SIMDPack<TTraits>::BuildTuplePerm(
                                col_desc->DataSize,
                                TotalRowSize - col_desc->DataSize, tuple_size,
                                offset,
                                ind * col_desc->DataSize * tuples_per_register,
                                packing_flag % 2);
                            SIMDPermMasks_.push_back(perm);
                        }
                    }
                }

                SIMDSmallTuple_ = simd_desc;
            }

            if (!col_pushed) {
                next_cols.push(&col);
            }
        }
    };

    const auto transpose_packing = [&](const std::vector<TColumnDesc> &columns,
                                       bool payload) {
        ui32 colsize = 0;

        for (const auto &col : columns) {
            if (col.SizeType != EColumnSizeType::Fixed) {
                break;
            }

            if (colsize != col.DataSize) {
                while (!next_cols.empty()) {
                    fallback_cols.push_back(next_cols.front());
                    next_cols.pop();
                }

                if (use_simd && std::find(SIMDTranspositionsColSizes_.begin(),
                                          SIMDTranspositionsColSizes_.end(),
                                          col.DataSize) !=
                                    SIMDTranspositionsColSizes_.end()) {
                    colsize = col.DataSize;
                }
            }

            if (colsize == col.DataSize) {
                next_cols.push(&col);
                if (next_cols.size() * colsize == TSimd<ui8>::SIZE) {
                    const auto ind =
                        payload * SIMDTranspositionsColSizes_.size() +
                        std::find(SIMDTranspositionsColSizes_.begin(),
                                  SIMDTranspositionsColSizes_.end(), colsize) -
                        SIMDTranspositionsColSizes_.begin();
                    if (SIMDTranspositions_[ind].Cols == 0) {
                        SIMDTranspositions_[ind].RowOffset =
                            next_cols.front()->Offset;
                    }
                    SIMDTranspositions_[ind].Cols += next_cols.size();

                    while (!next_cols.empty()) {
                        const auto col_desc = next_cols.front();
                        BlockColsOffsets_.push_back(col_desc->Offset);
                        BlockFixedColsSizes_.push_back(col_desc->DataSize);
                        BlockColumnsOrigInds_.push_back(
                            col_desc->OriginalIndex);
                        next_cols.pop();
                    }
                }
            } else {
                fallback_cols.push_back(&col);
            }
        }

        while (!next_cols.empty()) {
            fallback_cols.push_back(next_cols.front());
            next_cols.pop();
        }
    };

    if (max_simd_tuple_size) {
        small_tuple_packing(KeyColumns);
        small_tuple_packing(PayloadColumns);

        while (!next_cols.empty()) {
            fallback_cols.push_back(next_cols.front());
            next_cols.pop();
        }
    } else {
        transpose_packing(KeyColumns, 0);
        transpose_packing(PayloadColumns, 1);
    }

    for (const auto col_desc_p : fallback_cols) {
        BlockColsOffsets_.push_back(col_desc_p->Offset);
        BlockFixedColsSizes_.push_back(col_desc_p->DataSize);
        BlockColumnsOrigInds_.push_back(col_desc_p->OriginalIndex);
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

        ui32 hash = CalculateCRC32<TTraits>(
            res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

        for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
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

        ui32 hash = CalculateCRC32<TTraits>(
            res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

        for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
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

#define MULTI_8_I(C, i) C(i, 0) C(i, 1) C(i, 2) C(i, 3)
#define MULTI_8(C, A) C(A, 0) C(A, 1) C(A, 2) C(A, 3)

template <typename TTraits>
void TTupleLayoutSIMD<TTraits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const {
    std::vector<const ui8 *> block_columns;
    for (const auto col_ind : BlockColumnsOrigInds_) {
        block_columns.push_back(columns[col_ind]);
    }

    for (size_t row_ind = 0; row_ind < count; row_ind += BlockRows_) {
        const size_t cur_block_size = std::min(count - row_ind, BlockRows_);
        size_t cols_past = 0;

        if (SIMDSmallTuple_.Cols) {

#define CASE(i, j)                                                             \
    case i *kSIMDMaxCols + j:                                                  \
        SIMDPack<TTraits>::template PackTupleOr<i + 1, j + 1>(                 \
            block_columns.data() + cols_past, res + SIMDSmallTuple_.RowOffset, \
            cur_block_size, BlockFixedColsSizes_.data() + cols_past,           \
            BlockColsOffsets_.data() + cols_past,                              \
            SIMDSmallTuple_.SmallTupleSize, TotalRowSize,                      \
            SIMDPermMasks_.data(), start);                                     \
        break;

            switch ((SIMDSmallTuple_.InnerLoopIters - 1) * kSIMDMaxCols +
                    SIMDSmallTuple_.Cols - 1) {
                MULTI_8(MULTI_8_I, CASE)

            default:
                std::abort();
            }

#undef CASE

            cols_past += SIMDSmallTuple_.Cols;
        }

        for (size_t trnsps_ind = 0; trnsps_ind != SIMDTranspositions_.size();
             ++trnsps_ind) {
            if (SIMDTranspositions_[trnsps_ind].Cols) {
                SIMDPack<TTraits>::PackColSize(
                    block_columns.data() + cols_past,
                    res + SIMDTranspositions_[trnsps_ind].RowOffset,
                    cur_block_size,
                    SIMDTranspositionsColSizes_
                        [trnsps_ind % SIMDTranspositionsColSizes_.size()],
                    SIMDTranspositions_[trnsps_ind].Cols, TotalRowSize, start);
                cols_past += SIMDTranspositions_[trnsps_ind].Cols;
            }
        }

        PackTupleFallbackColImpl(
            block_columns.data() + cols_past, res,
            BlockColsOffsets_.size() - cols_past, cur_block_size,
            BlockFixedColsSizes_.data() + cols_past,
            BlockColsOffsets_.data() + cols_past, TotalRowSize, start);

        for (ui32 cols_ind = 0; cols_ind < Columns.size(); cols_ind += 8) {
            const size_t cols = std::min<size_t>(8ul, Columns.size() - cols_ind);
            const ui8 ones_byte = 0xFF;  // dereferencable + all-ones fast path
            const ui8 *bitmasks[8];

            for (size_t ind = 0; ind != cols; ++ind) {
                const auto &col = Columns[cols_ind + ind];
                bitmasks[ind] = 
                    isValidBitmask[col.OriginalIndex]
                    ? isValidBitmask[col.OriginalIndex] + start / 8
                    : &ones_byte;
            }
            for (size_t ind = cols; ind != 8; ++ind) {
                bitmasks[ind] = &ones_byte;
            }

            const auto advance_masks = [&] {
                for (size_t ind = 0; ind != 8; ++ind) {
                    if (bitmasks[ind] != &ones_byte) {
                        ++bitmasks[ind];
                    }
                }
            };

            const size_t first_full_byte =
                std::min<size_t>((8ul - start) & 7, cur_block_size);
            size_t block_row_ind = 0;

            const auto edge_mask_transpose = [&](const size_t until) {
                for (; block_row_ind < until; ++block_row_ind) {
                    const auto shift = (start + block_row_ind) % 8;

                    const auto new_res = res + block_row_ind * TotalRowSize;
                    const auto res = new_res;

                    res[BitmaskOffset + cols_ind / 8] = 0;
                    for (size_t col_ind = 0; col_ind != 8; ++col_ind) {
                        res[BitmaskOffset + cols_ind / 8] |=
                            ((bitmasks[col_ind][0] >> shift) & 1u) << col_ind;
                    }
                }
            };

            edge_mask_transpose(first_full_byte);
            if (first_full_byte) {
                advance_masks();
            }

            for (; block_row_ind + 7 < cur_block_size; block_row_ind += 8) {
                transposeBitmatrix(res + block_row_ind * TotalRowSize +
                                       BitmaskOffset + cols_ind / 8,
                                   bitmasks, TotalRowSize);
                advance_masks();
            }

            edge_mask_transpose(cur_block_size);
        }

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
             ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

            ui32 hash = CalculateCRC32<TTraits>(
                res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

            for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
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

        start += cur_block_size;
        res += cur_block_size * TotalRowSize;
    }
}

template <typename TTraits>
void TTupleLayoutSIMD<TTraits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const {
    std::vector<ui8 *> block_columns;
    for (const auto col_ind : BlockColumnsOrigInds_) {
        block_columns.push_back(columns[col_ind]);
    }

    for (size_t row_ind = 0; row_ind < count; row_ind += BlockRows_) {
        const size_t cur_block_size = std::min(count - row_ind, BlockRows_);
        size_t cols_past = 0;

        if (SIMDSmallTuple_.Cols) {

#define CASE(i, j)                                                             \
    case i *kSIMDMaxCols + j:                                                  \
        SIMDPack<TTraits>::template UnpackTupleOr<i + 1, j + 1>(               \
            res + SIMDSmallTuple_.RowOffset, block_columns.data() + cols_past, \
            cur_block_size, BlockFixedColsSizes_.data() + cols_past,           \
            BlockColsOffsets_.data() + cols_past,                              \
            SIMDSmallTuple_.SmallTupleSize, TotalRowSize,                      \
            SIMDPermMasks_.data() + (i + 1) * (j + 1), start);                 \
        break;

            switch ((SIMDSmallTuple_.InnerLoopIters - 1) * kSIMDMaxCols +
                    SIMDSmallTuple_.Cols - 1) {
                MULTI_8(MULTI_8_I, CASE)

            default:
                std::abort();
            }

#undef CASE

            cols_past += SIMDSmallTuple_.Cols;
        }

        for (size_t trnsps_ind = 0; trnsps_ind != SIMDTranspositions_.size();
             ++trnsps_ind) {
            if (SIMDTranspositions_[trnsps_ind].Cols) {
                SIMDPack<TTraits>::UnpackColSize(
                    res + SIMDTranspositions_[trnsps_ind].RowOffset,
                    block_columns.data() + cols_past, cur_block_size,
                    SIMDTranspositionsColSizes_
                        [trnsps_ind % SIMDTranspositionsColSizes_.size()],
                    SIMDTranspositions_[trnsps_ind].Cols, TotalRowSize, start);
                cols_past += SIMDTranspositions_[trnsps_ind].Cols;
            }
        }

        UnpackTupleFallbackColImpl(
            res, block_columns.data() + cols_past,
            BlockColsOffsets_.size() - cols_past, cur_block_size,
            BlockFixedColsSizes_.data() + cols_past,
            BlockColsOffsets_.data() + cols_past, TotalRowSize, start);

        for (ui32 cols_ind = 0; cols_ind < Columns.size(); cols_ind += 8) {
            const size_t cols = std::min<size_t>(8ul, Columns.size() - cols_ind);
            ui8 *bitmasks[8];
            ui8 trash_byte;  // dereferencable

            for (size_t ind = 0; ind != cols; ++ind) {
                const auto &col = Columns[cols_ind + ind];
                bitmasks[ind] =
                    isValidBitmask[col.OriginalIndex]
                        ? isValidBitmask[col.OriginalIndex] + start / 8
                        : &trash_byte;
            }
            for (size_t ind = cols; ind != 8; ++ind) {
                bitmasks[ind] = &trash_byte;
            }

            const auto advance_masks = [&] {
                for (size_t ind = 0; ind != 8; ++ind) {
                    if (bitmasks[ind] != &trash_byte) {
                        ++bitmasks[ind];
                    }
                }
            };

            const size_t first_full_byte =
                std::min<size_t>((8ul - start) & 7, cur_block_size);
            size_t block_row_ind = 0;

            const auto edge_mask_transpose = [&](const size_t until) {
                for (size_t col_ind = 0;
                     block_row_ind != until && col_ind != cols; ++col_ind) {
                    auto bitmask =
                        bitmasks[col_ind][0] & ~((0xFF << (block_row_ind & 7)) ^
                                                 (0xFF << (until & 7)));

                    for (size_t row_ind = block_row_ind; row_ind < until;
                         ++row_ind) {
                        const auto shift = (start + row_ind) % 8;

                        const auto new_res = res + row_ind * TotalRowSize;
                        const auto res = new_res;

                        bitmask |=
                            ((res[BitmaskOffset + cols_ind / 8] >> col_ind) &
                             1u)
                            << shift;
                    }

                    bitmasks[col_ind][0] = bitmask;
                }
                block_row_ind = until;
            };

            edge_mask_transpose(first_full_byte);
            if (first_full_byte) {
                advance_masks();
            }

            for (; block_row_ind + 7 < cur_block_size; block_row_ind += 8) {
                transposeBitmatrix(bitmasks,
                                   res + block_row_ind * TotalRowSize +
                                       BitmaskOffset + cols_ind / 8,
                                   TotalRowSize);
                advance_masks();
            }

            edge_mask_transpose(cur_block_size);
        }

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
             ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

            for (auto &col : VariableColumns) {
                const auto dataOffset = ReadUnaligned<ui32>(
                    columns[col.OriginalIndex] + sizeof(ui32) * start);
                auto *const data = columns[col.OriginalIndex + 1] + dataOffset;

                ui32 size = ReadUnaligned<ui8>(res + col.Offset);

                if (size < 255) { // embedded str
                    std::memcpy(data, res + col.Offset + 1, size);
                } else { // overflow buffer used
                    const auto prefixSize =
                        (col.DataSize - 1 - 2 * sizeof(ui32));
                    const auto overflowOffset = ReadUnaligned<ui32>(
                        res + col.Offset + 1 + 0 * sizeof(ui32));
                    const auto overflowSize = ReadUnaligned<ui32>(
                        res + col.Offset + 1 + 1 * sizeof(ui32));

                    std::memcpy(data, res + col.Offset + 1 + 2 * sizeof(ui32),
                                prefixSize);
                    std::memcpy(data + prefixSize,
                                overflow.data() + overflowOffset, overflowSize);

                    size = prefixSize + overflowSize;
                }

                WriteUnaligned<ui32>(columns[col.OriginalIndex] +
                                         sizeof(ui32) * (start + 1),
                                     dataOffset + size);
            }
        }

        start += cur_block_size;
        res += cur_block_size * TotalRowSize;
    }
}

template <typename TTraits>
void TTupleLayoutSIMD<TTraits>::BucketPack(
    const ui8 **columns, const ui8 **isValidBitmask,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows, ui32 start,
    ui32 count, ui32 bucketsLogNum) const {
    if (bucketsLogNum == 0) {
        auto& bres = reses[0];
        const auto size = bres.size();

        bres.resize(size + count * TotalRowSize);
        auto* const res = bres.data() + size;

        Pack(columns, isValidBitmask, res, overflows[0], start, count);
        return;
    }

    std::vector<ui8> resbuf(BlockRows_ * TotalRowSize);
    ui8 *const res = resbuf.data();

    for (ui32 bucket = 0; bucket < (1u << bucketsLogNum); ++bucket) {
        auto &bres = reses[bucket];
        /// memory reserve heuristic
        bres.reserve(bres.size() + ((count >> bucketsLogNum) + 1) * 9 / 8);
    }

    std::vector<const ui8 *> block_columns;
    for (const auto col_ind : BlockColumnsOrigInds_) {
        block_columns.push_back(columns[col_ind]);
    }

    for (size_t row_ind = 0; row_ind < count; row_ind += BlockRows_) {
        const size_t cur_block_size = std::min(count - row_ind, BlockRows_);
        size_t cols_past = 0;

        if (SIMDSmallTuple_.Cols) {

#define CASE(i, j)                                                             \
    case i *kSIMDMaxCols + j:                                                  \
        SIMDPack<TTraits>::template PackTupleOr<i + 1, j + 1>(                 \
            block_columns.data() + cols_past, res + SIMDSmallTuple_.RowOffset, \
            cur_block_size, BlockFixedColsSizes_.data() + cols_past,           \
            BlockColsOffsets_.data() + cols_past,                              \
            SIMDSmallTuple_.SmallTupleSize, TotalRowSize,                      \
            SIMDPermMasks_.data(), start);                                     \
        break;

            switch ((SIMDSmallTuple_.InnerLoopIters - 1) * kSIMDMaxCols +
                    SIMDSmallTuple_.Cols - 1) {
                MULTI_8(MULTI_8_I, CASE)

            default:
                std::abort();
            }

#undef CASE

            cols_past += SIMDSmallTuple_.Cols;
        }

        for (size_t trnsps_ind = 0; trnsps_ind != SIMDTranspositions_.size();
             ++trnsps_ind) {
            if (SIMDTranspositions_[trnsps_ind].Cols) {
                SIMDPack<TTraits>::PackColSize(
                    block_columns.data() + cols_past,
                    res + SIMDTranspositions_[trnsps_ind].RowOffset,
                    cur_block_size,
                    SIMDTranspositionsColSizes_
                        [trnsps_ind % SIMDTranspositionsColSizes_.size()],
                    SIMDTranspositions_[trnsps_ind].Cols, TotalRowSize, start);
                cols_past += SIMDTranspositions_[trnsps_ind].Cols;
            }
        }

        PackTupleFallbackColImpl(
            block_columns.data() + cols_past, res,
            BlockColsOffsets_.size() - cols_past, cur_block_size,
            BlockFixedColsSizes_.data() + cols_past,
            BlockColsOffsets_.data() + cols_past, TotalRowSize, start);

        for (ui32 cols_ind = 0; cols_ind < Columns.size(); cols_ind += 8) {
            const size_t cols = std::min<size_t>(8ul, Columns.size() - cols_ind);
            const ui8 ones_byte = 0xFF;  // dereferencable + all-ones fast path
            const ui8 *bitmasks[8];

            for (size_t ind = 0; ind != cols; ++ind) {
                const auto &col = Columns[cols_ind + ind];
                bitmasks[ind] = 
                    isValidBitmask[col.OriginalIndex]
                    ? isValidBitmask[col.OriginalIndex] + start / 8
                    : &ones_byte;
            }
            for (size_t ind = cols; ind != 8; ++ind) {
                bitmasks[ind] = &ones_byte;
            }

            const auto advance_masks = [&] {
                for (size_t ind = 0; ind != 8; ++ind) {
                    if (bitmasks[ind] != &ones_byte) {
                        ++bitmasks[ind];
                    }
                }
            };

            const size_t first_full_byte =
                std::min<size_t>((8ul - start) & 7, cur_block_size);
            size_t block_row_ind = 0;

            const auto edge_mask_transpose = [&](const size_t until) {
                for (; block_row_ind < until; ++block_row_ind) {
                    const auto shift = (start + block_row_ind) % 8;

                    const auto new_res = res + block_row_ind * TotalRowSize;
                    const auto res = new_res;

                    res[BitmaskOffset + cols_ind / 8] = 0;
                    for (size_t col_ind = 0; col_ind != 8; ++col_ind) {
                        res[BitmaskOffset + cols_ind / 8] |=
                            ((bitmasks[col_ind][0] >> shift) & 1u) << col_ind;
                    }
                }
            };

            edge_mask_transpose(first_full_byte);
            if (first_full_byte) {
                advance_masks();
            }

            for (; block_row_ind + 7 < cur_block_size; block_row_ind += 8) {
                transposeBitmatrix(res + block_row_ind * TotalRowSize +
                                       BitmaskOffset + cols_ind / 8,
                                   bitmasks, TotalRowSize);
                advance_masks();
            }

            edge_mask_transpose(cur_block_size);
        }

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
            ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

            ui32 hash = CalculateCRC32<TTraits>(
                res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset);

            for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
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

        start += cur_block_size;
    }
}

template __attribute__((target("avx2"))) void
TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;
template __attribute__((target("sse4.2"))) void
TTupleLayoutSIMD<NSimd::TSimdSSE42Traits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

template __attribute__((target("avx2"))) void
TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;
template __attribute__((target("sse4.2"))) void
TTupleLayoutSIMD<NSimd::TSimdSSE42Traits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

template __attribute__((target("avx2"))) void
TTupleLayoutSIMD<NSimd::TSimdAVX2Traits>::BucketPack(
    const ui8 **columns, const ui8 **isValidBitmask,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows, ui32 start,
    ui32 count, ui32 bucketsLogNum) const;
template __attribute__((target("sse4.2"))) void
TTupleLayoutSIMD<NSimd::TSimdSSE42Traits>::BucketPack(
    const ui8 **columns, const ui8 **isValidBitmask,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
    TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows, ui32 start,
    ui32 count, ui32 bucketsLogNum) const;

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
            WriteUnaligned<ui32>(outTuple + col.Offset + 1 + 0 * sizeof(ui32), outOverflowSize);
            outOverflowSize += overflowSize;
        }
    }
}

/// TODO: write unit tests
void TTupleLayout::Join(
    std::vector<ui8, TMKQLAllocator<ui8>>& dst,
        std::vector<ui8, TMKQLAllocator<ui8>>& dstOverflow,
        ui32 dstCount,
        const ui8 *src, const ui8 *srcOverflow, ui32 srcCount, ui32 srcOverflowSize) const 
{
    ui32 dstOverflowOffset = dstOverflow.size();
    dstOverflow.resize(dstOverflow.size() + srcOverflowSize);
    std::memcpy(dstOverflow.data() + dstOverflowOffset, srcOverflow, srcOverflowSize);

    constexpr ui32 blockRows = 128;
    dst.resize((dstCount + srcCount) * TotalRowSize);
    ui8 *dstRow = dst.data() + dstCount * TotalRowSize;
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

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr

