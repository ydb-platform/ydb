#include "tuple.h"

#include <algorithm>
#include <queue>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/bitops.h>
#include <util/generic/buffer.h>

#include "hashes_calc.h"
#include "packing.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

namespace {

// Transpose 8x8 bit-matrix packed in ui64 integer
Y_FORCE_INLINE ui64 transposeBitmatrix(ui64 x) {
    if (x == 0xFFFFFFFFFFFFFFFFLL) {
        return x;
    }

    // a b A B aa bb AA BB
    // c d C D cc dd CC DD
    // ->
    // a c A C aa cc AA CC
    // b d B D bb dd BB DD
    // a b A B aa bb AA BB // c d C D cc dd CC DD
    // a c A C aa cc AA CC // b d B D bb dd BB DD
    x = ((x &
          0b10101010'01010101'10101010'01010101'10101010'01010101'10101010'01010101ull)) |
        ((x &
          0b01010101'00000000'01010101'00000000'01010101'00000000'01010101'00000000ull) >>
         7) |
        ((x &
          0b00000000'10101010'00000000'10101010'00000000'10101010'00000000'10101010ull)
         << 7);
    // a1 a2 b1 b2 A1 A2 B1 B2
    // a3 a4 b3 b4 A3 A4 B3 B4
    // c1 c2 d1 d2 C1 C2 D1 D2
    // c3 c4 d3 d4 C3 C4 D3 D4
    // ->
    // a1 a2 c1 c2 A1 A2 C1 C2
    // a3 a4 c3 c4 A3 A4 C3 C4
    // b1 b2 d1 d2 B1 B2 D1 D2
    // b3 b4 d3 d4 B3 B4 D3 D4
    //
    //
    // a1 a2 b1 b2 A1 A2 B1 B2 // a3 a4 b3 b4 A3 A4 B3 B4 // c1 c2 d1 d2 C1 C2
    // D1 D2 // c3 c4 d3 d4 C3 C4 D3 D4
    // ->
    // a1 a2 c1 c2 A1 A2 C1 C2 // a3 a4 c3 c4 A3 A4 C3 C4 // b1 b2 d1 d2 B1 B2
    // D1 D2 // b3 b4 d3 d4 B3 B4 D3 D4
    x = ((x &
          0b1100110011001100'0011001100110011'1100110011001100'0011001100110011ull)) |
        ((x &
          0b0011001100110011'0000000000000000'0011001100110011'0000000000000000ull) >>
         14) |
        ((x &
          0b0000000000000000'1100110011001100'0000000000000000'1100110011001100ull)
         << 14);
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

THolder<TTupleLayout>
TTupleLayout::Create(const std::vector<TColumnDesc> &columns) {

    if (NX86::HaveAVX2())
        return MakeHolder<TTupleLayoutFallback<NSimd::TSimdAVX2Traits>>(
            columns);

    if (NX86::HaveSSE42())
        return MakeHolder<TTupleLayoutFallback<NSimd::TSimdSSE42Traits>>(
            columns);

    return MakeHolder<TTupleLayoutFallback<NSimd::TSimdFallbackTraits>>(
        columns);
}

template <typename TTraits>
TTupleLayoutFallback<TTraits>::TTupleLayoutFallback(
    const std::vector<TColumnDesc> &columns)
    : TTupleLayout(columns) {

    for (ui32 i = 0, idx = 0; i < OrigColumns.size(); ++i) {
        auto &col = OrigColumns[i];

        col.OriginalIndex = idx;

        if (col.SizeType == EColumnSizeType::Variable) {
            // we cannot handle (rare) overflow strings unless we have at least
            // space for header; size of inlined strings is limited to 254
            // bytes, limit maximum inline data size
            col.DataSize = std::max<ui32>(1 + 2 * sizeof(ui32),
                                          std::min<ui32>(255, col.DataSize));
            idx += 2; // Variable-size takes two columns: one for offsets, and
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
            VariableColumns_.push_back(col);
        } else if (IsPowerOf2(col.DataSize) &&
                   col.DataSize < (1u << FixedPOTColumns_.size())) {
            FixedPOTColumns_[CountTrailingZeroBits(col.DataSize)].push_back(
                col);
        } else {
            FixedNPOTColumns_.push_back(col);
        }
    }

    /// TODO: dynamic configuration
    BlockRows_ = 256;
    const bool use_simd = true;

    std::vector<const TColumnDesc *> block_fallback;
    std::queue<const TColumnDesc *> next_cols;

    size_t fixed_cols_left =
        KeyColumnsFixedNum +
        std::accumulate(PayloadColumns.begin(), PayloadColumns.end(), 0ul,
                        [](size_t prev, const auto &col) {
                            return prev +
                                   (col.SizeType == EColumnSizeType::Fixed);
                        });

    size_t prev_tuple_size;
    size_t curr_tuple_size = 0;

    const auto manage_block_packing = [&](const std::vector<TColumnDesc>
                                              &columns) {
        for (size_t col_ind = 0;
             col_ind != columns.size() &&
             columns[col_ind].SizeType == EColumnSizeType::Fixed;) {
            --fixed_cols_left;
            next_cols.push(&columns[col_ind]);
            prev_tuple_size = curr_tuple_size;
            curr_tuple_size = next_cols.back()->Offset +
                              next_cols.back()->DataSize -
                              next_cols.front()->Offset;

            ++col_ind;
            if (curr_tuple_size >= TSimd<ui8>::SIZE ||
                next_cols.size() == kSIMDMaxCols || !fixed_cols_left) {
                const bool oversize = curr_tuple_size > TSimd<ui8>::SIZE;
                const size_t tuple_size =
                    oversize ? prev_tuple_size : curr_tuple_size;
                const size_t tuple_cols = next_cols.size() - oversize;

                if (!use_simd || !tuple_cols ||
                    (Columns.size() != next_cols.size() &&
                     tuple_size < TSimd<ui8>::SIZE * 7 / 8) ||
                    tuple_size > TSimd<ui8>::SIZE ||
                    (!SIMDBlock_.empty() &&
                     TotalRowSize - next_cols.front()->Offset <
                         TSimd<ui8>::SIZE)) {
                    block_fallback.push_back(next_cols.front());
                    next_cols.pop();
                    continue;
                }

                SIMDDesc simd_desc;
                simd_desc.Cols = tuple_cols;
                simd_desc.PermMaskOffset = SIMDPermMasks_.size();
                simd_desc.RowOffset = next_cols.front()->Offset;

                const TColumnDesc *col_descs[kSIMDMaxCols];
                ui32 col_max_size = 0;
                for (ui8 col_ind = 0; col_ind != simd_desc.Cols; ++col_ind) {
                    col_descs[col_ind] = next_cols.front();
                    col_max_size =
                        std::max(col_max_size, col_descs[col_ind]->DataSize);
                    next_cols.pop();
                }

                simd_desc.InnerLoopIters = std::min(
                    size_t(kSIMDMaxInnerLoopSize),
                    (TSimd<ui8>::SIZE / col_max_size) /
                        std::max(size_t(1u), size_t(TSimd<ui8>::SIZE / TotalRowSize)));

                const auto tuples_per_register =
                    std::max(1u, TSimd<ui8>::SIZE / TotalRowSize);

                for (ui8 col_ind = 0; col_ind != simd_desc.Cols; ++col_ind) {
                    const auto &col_desc = col_descs[col_ind];
                    const size_t offset =
                        col_desc->Offset - simd_desc.RowOffset;

                    BlockFixedColsSizes_.push_back(col_desc->DataSize);
                    BlockColsOffsets_.push_back(offset);
                    BlockColumnsOrigInds_.push_back(col_desc->OriginalIndex);
                }

                for (size_t packing_flag = 1; packing_flag != 3;
                     ++packing_flag) {
                    for (ui8 col_ind = 0; col_ind != simd_desc.Cols;
                         ++col_ind) {
                        const auto &col_desc = col_descs[col_ind];
                        const size_t offset =
                            col_desc->Offset - simd_desc.RowOffset;

                        for (ui8 ind = 0; ind != simd_desc.InnerLoopIters;
                             ++ind) {
                            SIMDPermMasks_.push_back(
                                SIMDPack<TTraits>::BuildTuplePerm(
                                    col_desc->DataSize,
                                    TotalRowSize - col_desc->DataSize, offset,
                                    ind * col_desc->DataSize *
                                        tuples_per_register,
                                    packing_flag % 2));
                        }
                    }
                }

                SIMDBlock_.push_back(simd_desc);
            }
        }

        while (!next_cols.empty()) {
            block_fallback.push_back(next_cols.front());
            next_cols.pop();
        }
    };

    manage_block_packing(KeyColumns);
    manage_block_packing(PayloadColumns);

    for (const auto col_desc_p : block_fallback) {
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
template <>
void TTupleLayoutFallback<NSimd::TSimdFallbackTraits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const {
    using TTraits = NSimd::TSimdFallbackTraits;

    std::vector<ui64> bitmaskMatrix(BitmaskSize);

    if (auto off = (start % 8)) {
        auto bitmaskIdx = start / 8;

        for (ui32 j = Columns.size(); j--;)
            bitmaskMatrix[j / 8] |=
                ui64(isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx])
                << ((j % 8) * 8);

        for (auto &m : bitmaskMatrix) {
            m = transposeBitmatrix(m);
            m >>= off * 8;
        }
    }

    for (; count--; ++start, res += TotalRowSize) {
        ui32 hash = 0;
        auto bitmaskIdx = start / 8;

        bool anyOverflow = false;

        for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
            auto &col = KeyColumns[i];
            ui32 dataOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] +
                                                  sizeof(ui32) * start);
            ui32 nextOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] +
                                                  sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;

            if (size >= col.DataSize) {
                anyOverflow = true;
                break;
            }
        }

        if ((start % 8) == 0) {
            std::fill(bitmaskMatrix.begin(), bitmaskMatrix.end(), 0);
            for (ui32 j = Columns.size(); j--;)
                bitmaskMatrix[j / 8] |=
                    ui64(isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx])
                    << ((j % 8) * 8);
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

        for (auto &col : VariableColumns_) {
            auto dataOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] +
                                                  sizeof(ui32) * start);
            auto nextOffset = ReadUnaligned<ui32>(columns[col.OriginalIndex] +
                                                  sizeof(ui32) * (start + 1));
            auto size = nextOffset - dataOffset;
            auto data = columns[col.OriginalIndex + 1] + dataOffset;

            if (size >= col.DataSize) {
                res[col.Offset] = 255;

                ui32 prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                auto overflowSize = size - prefixSize;
                auto overflowOffset = overflow.size();

                overflow.resize(overflowOffset + overflowSize);

                WriteUnaligned<ui32>(res + col.Offset + 1 + 0 * sizeof(ui32),
                                     overflowOffset);
                WriteUnaligned<ui32>(res + col.Offset + 1 + 1 * sizeof(ui32),
                                     overflowSize);
                std::memcpy(res + col.Offset + 1 + 2 * sizeof(ui32), data,
                            prefixSize);
                std::memcpy(overflow.data() + overflowOffset, data + prefixSize,
                            overflowSize);
            } else {
                Y_DEBUG_ABORT_UNLESS(size < 255);
                res[col.Offset] = size;
                std::memcpy(res + col.Offset + 1, data, size);
                std::memset(res + col.Offset + 1 + size, 0,
                            col.DataSize - (size + 1));
            }

            if (anyOverflow && col.Role == EColumnRole::Key) {
                hash =
                    CalculateCRC32<TTraits>((ui8 *)&size, sizeof(ui32), hash);
                hash = CalculateCRC32<TTraits>(data, size, hash);
            }
        }

        // isValid bitmap is NOT included into hashed data
        if (anyOverflow) {
            hash = CalculateCRC32<TTraits>(
                res + KeyColumnsOffset, KeyColumnsFixedEnd - KeyColumnsOffset,
                hash);
        } else {
            hash = CalculateCRC32<TTraits>(res + KeyColumnsOffset,
                                           KeyColumnsEnd - KeyColumnsOffset);
        }
        WriteUnaligned<ui32>(res, hash);
    }
}

template <>
void TTupleLayoutFallback<NSimd::TSimdFallbackTraits>::Unpack(
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
        for (ui32 j = Columns.size(); j--;)
            bitmaskMatrix[j / 8] |=
                (isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx] &
                 ~(0xFF << bitmaskShift))
                << ((j % 8) * 8);

        /// ready last (which are same as above) bitmatrix bytes if needed
        if (bitmaskIdx == bitmaskIdxC)
            for (ui32 j = Columns.size(); j--;)
                bitmaskMatrix[j / 8] |=
                    (isValidBitmask[Columns[j].OriginalIndex][bitmaskIdxC] &
                     (0xFF << bitmaskShiftC))
                    << ((j % 8) * 8);

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
                isValidBitmask[Columns[j].OriginalIndex][bitmaskIdx] =
                    ui8(bitmaskMatrix[j / 8] >> ((j % 8) * 8));
            std::fill(bitmaskMatrix.begin(), bitmaskMatrix.end(), 0);

            if (count && count < 8) {
                /// ready last bitmatrix bytes
                for (ui32 j = Columns.size(); j--;)
                    bitmaskMatrix[j / 8] |=
                        (isValidBitmask[Columns[j].OriginalIndex]
                                       [bitmaskIdx + 1] &
                         (0xFF << count))
                        << ((j % 8) * 8);

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

        for (auto &col : VariableColumns_) {
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

#define MULTI_8_I(C, i)                                                        \
    C(i, 0) C(i, 1) C(i, 2) C(i, 3) C(i, 4) C(i, 5) C(i, 6) C(i, 7)
#define MULTI_8(C, A)                                                          \
    C(A, 0) C(A, 1) C(A, 2) C(A, 3) C(A, 4) C(A, 5) C(A, 6) C(A, 7)

template <typename TTraits>
void TTupleLayoutFallback<TTraits>::Pack(
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

        for (const auto &simd_block : SIMDBlock_) {
#define CASE(i, j)                                                             \
    case i *kSIMDMaxCols + j:                                                  \
        SIMDPack<TTraits>::template PackTupleOrImpl<i + 1, j + 1>(             \
            block_columns.data() + cols_past, res + simd_block.RowOffset,      \
            cur_block_size, BlockFixedColsSizes_.data() + cols_past,           \
            BlockColsOffsets_.data() + cols_past, TotalRowSize,                \
            SIMDPermMasks_.data() + simd_block.PermMaskOffset, start);         \
        break;

            switch ((simd_block.InnerLoopIters - 1) * kSIMDMaxCols +
                    simd_block.Cols - 1) {
                MULTI_8(MULTI_8_I, CASE)

            default:
                std::abort();
            }

#undef CASE

            cols_past += simd_block.Cols;
        }

        PackTupleFallbackColImpl(
            block_columns.data() + cols_past, res,
            BlockColsOffsets_.size() - cols_past, cur_block_size,
            BlockFixedColsSizes_.data() + cols_past,
            BlockColsOffsets_.data() + cols_past, TotalRowSize, start);

        for (ui32 cols_ind = 0; cols_ind < Columns.size(); cols_ind += 8) {
            const ui8 *bitmasks[8];
            const size_t cols = std::min<size_t>(8ul, Columns.size() - cols_ind);
            for (size_t ind = 0; ind != cols; ++ind) {
                const auto &col = Columns[cols_ind + ind];
                bitmasks[ind] = isValidBitmask[col.OriginalIndex] + start / 8;
            }
            const ui8 ones_byte = 0xFF;
            for (size_t ind = cols; ind != 8; ++ind) {
                // dereferencable + all-ones fast path
                bitmasks[ind] = &ones_byte;
            }

            const auto advance_masks = [&] {
                for (size_t ind = 0; ind != cols; ++ind) {
                    ++bitmasks[ind];
                }
            };

            const size_t first_full_byte =
                std::min<size_t>((8ul - start) & 7, cur_block_size);
            size_t block_row_ind = 0;

            const auto simple_mask_transpose = [&](const size_t until) {
                for (; block_row_ind < until; ++block_row_ind) {
                    const auto shift = (start + block_row_ind) % 8;

                    const auto new_res = res + block_row_ind * TotalRowSize;
                    const auto res = new_res;

                    res[BitmaskOffset + cols_ind / 8] = 0;
                    for (size_t col_ind = 0; col_ind != cols; ++col_ind) {
                        res[BitmaskOffset + cols_ind / 8] |=
                            ((bitmasks[col_ind][0] >> shift) & 1u) << col_ind;
                    }
                }
            };

            simple_mask_transpose(first_full_byte);
            if (first_full_byte) {
                advance_masks();
            }

            for (; block_row_ind + 7 < cur_block_size; block_row_ind += 8) {
                transposeBitmatrix(res + block_row_ind * TotalRowSize +
                                       BitmaskOffset + cols_ind / 8,
                                   bitmasks, TotalRowSize);
                advance_masks();
            }

            simple_mask_transpose(cur_block_size);
        }

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
             ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

            ui32 hash = 0;
            bool anyOverflow = false;

            for (ui32 i = KeyColumnsFixedNum; i < KeyColumns.size(); ++i) {
                auto &col = KeyColumns[i];
                auto dataOffset = ReadUnaligned<ui32>(
                    columns[col.OriginalIndex] + sizeof(ui32) * start);
                auto nextOffset = ReadUnaligned<ui32>(
                    columns[col.OriginalIndex] + sizeof(ui32) * (start + 1));
                auto size = nextOffset - dataOffset;

                if (size >= col.DataSize) {
                    anyOverflow = true;
                    break;
                }
            }

            for (auto &col : VariableColumns_) {
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
                if (anyOverflow && col.Role == EColumnRole::Key) {
                    hash = CalculateCRC32<TTraits>((ui8 *)&size, sizeof(ui32),
                                                   hash);
                    hash = CalculateCRC32<TTraits>(data, size, hash);
                }
            }

            // isValid bitmap is NOT included into hashed data
            if (anyOverflow) {
                hash = CalculateCRC32<TTraits>(
                    res + KeyColumnsOffset,
                    KeyColumnsFixedEnd - KeyColumnsOffset, hash);
            } else {
                hash = CalculateCRC32<TTraits>(
                    res + KeyColumnsOffset, KeyColumnsEnd - KeyColumnsOffset);
            }
            WriteUnaligned<ui32>(res, hash);
        }

        start += cur_block_size;
        res += cur_block_size * TotalRowSize;
    }
}

template <typename TTraits>
void TTupleLayoutFallback<TTraits>::Unpack(
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

        for (const auto &simd_block : SIMDBlock_) {
#define CASE(i, j)                                                             \
    case i *kSIMDMaxCols + j:                                                  \
        SIMDPack<TTraits>::template UnpackTupleOrImpl<i + 1, j + 1>(           \
            res + simd_block.RowOffset, block_columns.data() + cols_past,      \
            cur_block_size, BlockFixedColsSizes_.data() + cols_past,           \
            BlockColsOffsets_.data() + cols_past, TotalRowSize,                \
            SIMDPermMasks_.data() + simd_block.PermMaskOffset + i * j, start); \
        break;

            switch ((simd_block.InnerLoopIters - 1) * kSIMDMaxCols +
                    simd_block.Cols - 1) {
                MULTI_8(MULTI_8_I, CASE)

            default:
                std::abort();
            }

#undef CASE

            cols_past += simd_block.Cols;
        }

        UnpackTupleFallbackColImpl(
            res, block_columns.data() + cols_past,
            BlockColsOffsets_.size() - cols_past, cur_block_size,
            BlockFixedColsSizes_.data() + cols_past,
            BlockColsOffsets_.data() + cols_past, TotalRowSize, start);

        for (ui32 cols_ind = 0; cols_ind < Columns.size(); cols_ind += 8) {
            ui8 *bitmasks[8];
            const size_t cols = std::min<size_t>(8ul, Columns.size() - cols_ind);
            for (size_t ind = 0; ind != cols; ++ind) {
                const auto &col = Columns[cols_ind + ind];
                bitmasks[ind] = isValidBitmask[col.OriginalIndex] + start / 8;
            }
            ui8 trash_byte;
            for (size_t ind = cols; ind != 8; ++ind) {
                bitmasks[ind] = &trash_byte; // dereferencable
            }

            const auto advance_masks = [&] {
                for (size_t ind = 0; ind != cols; ++ind) {
                    ++bitmasks[ind];
                }
            };

            const size_t first_full_byte =
                std::min<size_t>((8ul - start) & 7, cur_block_size);
            size_t block_row_ind = 0;

            const auto simple_mask_transpose = [&](const size_t until) {
                for (size_t col_ind = 0;
                     block_row_ind != until && col_ind != cols; ++col_ind) {
                    auto col_bitmask =
                        bitmasks[col_ind][0] & ~((0xFF << (block_row_ind & 7)) ^
                                                 (0xFF << (until & 7)));

                    for (size_t row_ind = block_row_ind; row_ind < until;
                         ++row_ind) {
                        const auto shift = (start + row_ind) % 8;

                        const auto new_res = res + row_ind * TotalRowSize;
                        const auto res = new_res;

                        col_bitmask |=
                            ((res[BitmaskOffset + cols_ind / 8] >> col_ind) &
                             1u)
                            << shift;
                    }

                    bitmasks[col_ind][0] = col_bitmask;
                }
                block_row_ind = until;
            };

            simple_mask_transpose(first_full_byte);
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

            simple_mask_transpose(cur_block_size);
        }

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
             ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

            for (auto &col : VariableColumns_) {
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

template __attribute__((target("avx2"))) void
TTupleLayoutFallback<NSimd::TSimdAVX2Traits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;
template __attribute__((target("sse4.2"))) void
TTupleLayoutFallback<NSimd::TSimdSSE42Traits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

template __attribute__((target("avx2"))) void
TTupleLayoutFallback<NSimd::TSimdAVX2Traits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;
template __attribute__((target("sse4.2"))) void
TTupleLayoutFallback<NSimd::TSimdSSE42Traits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
