#pragma once

#include "tuple.h"
#include "hashes_calc.h"
#include "packing_simd_impl.h"
#include "tuple_bitmatrix.h"

#include <algorithm>
#include <queue>

// Included only by the ISA-specific layout implementations. Keeping the layout
// and packing definitions together allows inlining without LTO.
namespace NKikimr::NMiniKQL::NPackedTuple {

template <typename TTraits> struct TTupleLayoutSIMD : public TTupleLayoutFallback {

    explicit TTupleLayoutSIMD(const std::vector<TColumnDesc> &columns);

    void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
        std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
        ui32 count) const override;

    void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
            const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
            ui32 start, ui32 count) const override;

    void
    BucketPack(const ui8 **columns, const ui8 **isValidBitmask,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows,
                 ui32 start, ui32 count, ui32 bucketsLogNum) const override;

  private:
    using TSimdI8 = typename TTraits::TSimdI8;
    template <class T> using TSimd = typename TTraits::template TSimd8<T>;

    static constexpr ui8 kSIMDMaxCols = 4;
    static constexpr ui8 kSIMDMaxInnerLoopSize = 4;

    size_t BlockRows_; // Estimated rows per cache block
    std::vector<size_t> BlockColsOffsets_;
    std::vector<size_t> BlockFixedColsSizes_;
    std::vector<size_t> BlockColumnsOrigInds_;

    struct SIMDSmallTupleDesc {
        ui8 Cols = 0;
        ui8 InnerLoopIters;
        size_t SmallTupleSize;
        size_t RowOffset;
    };
    SIMDSmallTupleDesc SIMDSmallTuple_;
    std::vector<TSimd<ui8>> SIMDPermMasks_; // small tuple precomputed masks

    struct SIMDTransposeDesc {
        ui8 Cols = 0;
        size_t RowOffset;
    };
    // [1, 2, 4, 8] bytes x [Key, Payload]
    std::array<SIMDTransposeDesc, 8> SIMDTranspositions_;
    static constexpr std::array<size_t, 4> SIMDTranspositionsColSizes_ = {1, 2,
                                                                          4, 8};
};

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

    auto packableFixedPrefix = [](const std::vector<TColumnDesc> &columns) {
        std::vector<TColumnDesc> result;
        for (const auto &col : columns) {
            if (col.SizeType != EColumnSizeType::Fixed) {
                break;
            }
            if (col.DataSize != 0) {
                result.push_back(col);
            }
        }
        return result;
    };
    const auto packableKeyColumns = packableFixedPrefix(KeyColumns);
    const auto packablePayloadColumns = packableFixedPrefix(PayloadColumns);

    size_t fixed_cols_left =
        packableKeyColumns.size() + packablePayloadColumns.size();

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

            // PackTupleOr/UnpackTupleOr are instantiated for at most
            // kSIMDMaxCols columns, so a wider group cannot be dispatched.
            // Stop growing the group there and let the rest of the columns go
            // through the fallback path.
            const bool col_pushed = tuple_size_with_col <= max_simd_tuple_size &&
                                    next_cols.size() < kSIMDMaxCols;
            if (col_pushed) {
                next_cols.push(&col);
                tuple_size = tuple_size_with_col;
            }

            if (!SIMDSmallTuple_.Cols && next_cols.size() > 1 &&
                next_cols.size() <= kSIMDMaxCols &&
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
        small_tuple_packing(packableKeyColumns);
        small_tuple_packing(packablePayloadColumns);

        while (!next_cols.empty()) {
            fallback_cols.push_back(next_cols.front());
            next_cols.pop();
        }
    } else {
        transpose_packing(packableKeyColumns, 0);
        transpose_packing(packablePayloadColumns, 1);
    }

    for (const auto col_desc_p : fallback_cols) {
        BlockColsOffsets_.push_back(col_desc_p->Offset);
        BlockFixedColsSizes_.push_back(col_desc_p->DataSize);
        BlockColumnsOrigInds_.push_back(col_desc_p->OriginalIndex);
    }
}
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
            SIMDPack<TTraits>::PackTupleOr(
                SIMDSmallTuple_.InnerLoopIters, SIMDSmallTuple_.Cols,
                block_columns.data() + cols_past,
                res + SIMDSmallTuple_.RowOffset, cur_block_size,
                BlockFixedColsSizes_.data() + cols_past,
                BlockColsOffsets_.data() + cols_past,
                SIMDSmallTuple_.SmallTupleSize, TotalRowSize,
                SIMDPermMasks_.data(), start);

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

        CanonicalizeFloatKeys(res, cur_block_size);

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
             ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

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
            SIMDPack<TTraits>::UnpackTupleOr(
                SIMDSmallTuple_.InnerLoopIters, SIMDSmallTuple_.Cols,
                res + SIMDSmallTuple_.RowOffset,
                block_columns.data() + cols_past, cur_block_size,
                BlockFixedColsSizes_.data() + cols_past,
                BlockColsOffsets_.data() + cols_past,
                SIMDSmallTuple_.SmallTupleSize, TotalRowSize,
                SIMDPermMasks_.data() + SIMDSmallTuple_.InnerLoopIters *
                    SIMDSmallTuple_.Cols,
                start);

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
                    if (size != 0) {
                        std::memcpy(data, res + col.Offset + 1, size);
                    }
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

        RestoreFloatOriginals(columns, res, start, cur_block_size);

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
            SIMDPack<TTraits>::PackTupleOr(
                SIMDSmallTuple_.InnerLoopIters, SIMDSmallTuple_.Cols,
                block_columns.data() + cols_past,
                res + SIMDSmallTuple_.RowOffset, cur_block_size,
                BlockFixedColsSizes_.data() + cols_past,
                BlockColsOffsets_.data() + cols_past,
                SIMDSmallTuple_.SmallTupleSize, TotalRowSize,
                SIMDPermMasks_.data(), start);

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

        CanonicalizeFloatKeys(res, cur_block_size);

        for (size_t block_row_ind = 0; block_row_ind != cur_block_size;
            ++block_row_ind) {

            const auto new_start = start + block_row_ind;
            const auto start = new_start;

            const auto new_res = res + block_row_ind * TotalRowSize;
            const auto res = new_res;

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

        start += cur_block_size;
    }
}

} // namespace NKikimr::NMiniKQL::NPackedTuple
