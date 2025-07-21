#include <util/system/unaligned_mem.h>
#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

static void
PackTupleFallbackRowImpl(const ui8 *const src_cols[], ui8 *const dst_rows,
                         const size_t cols, const size_t size,
                         const size_t col_sizes[], const size_t offsets[],
                         const size_t padded_size, const size_t start = 0) {
    for (size_t row = 0; row != size; ++row) {
        for (size_t col = 0; col != cols; ++col) {
            switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        *reinterpret_cast<ui##bits *>(dst_rows + row * padded_size +           \
                                      offsets[col]) =                          \
            *reinterpret_cast<const ui##bits *>(src_cols[col] +                \
                                                (start + row) * (bits / 8));   \
        break

                MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

            default:
                memcpy(dst_rows + row * padded_size + offsets[col],
                       src_cols[col] + (start + row) * col_sizes[col],
                       col_sizes[col]);
            }
        }
    }
}

static void
UnpackTupleFallbackRowImpl(const ui8 *const src_rows, ui8 *const dst_cols[],
                           const size_t cols, const size_t size,
                           const size_t col_sizes[], const size_t offsets[],
                           const size_t padded_size, const size_t start = 0) {
    for (size_t row = 0; row != size; ++row) {
        for (size_t col = 0; col != cols; ++col) {
            switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        *reinterpret_cast<ui##bits *>(dst_cols[col] +                          \
                                      (start + row) * (bits / 8)) =            \
            *reinterpret_cast<const ui##bits *>(src_rows + row * padded_size + \
                                                offsets[col]);                 \
        break

                MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

            default:
                memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                       src_rows + row * padded_size + offsets[col],
                       col_sizes[col]);
            }
        }
    }
}

template <class ByteType>
Y_FORCE_INLINE static void
PackTupleFallbackTypedColImpl(const ui8 *const src_col, ui8 *const dst_rows,
                              const size_t size, const size_t padded_size,
                              const size_t start = 0) {
    static constexpr size_t BYTES = sizeof(ByteType);
    for (size_t row = 0; row != size; ++row) {
        WriteUnaligned<ByteType>(
            dst_rows + row * padded_size,
            ReadUnaligned<ByteType>(src_col + (start + row) * BYTES));
    }
}

template <class ByteType>
Y_FORCE_INLINE static void
UnpackTupleFallbackTypedColImpl(const ui8 *const src_rows, ui8 *const dst_col,
                                const size_t size, const size_t padded_size,
                                const size_t start = 0) {
    static constexpr size_t BYTES = sizeof(ByteType);
    for (size_t row = 0; row != size; ++row) {
        WriteUnaligned<ByteType>(
            dst_col + (start + row) * BYTES,
            ReadUnaligned<ByteType>(src_rows + row * padded_size));
    }
}

static void
PackTupleFallbackColImpl(const ui8 *const src_cols[], ui8 *const dst_rows,
                         const size_t cols, const size_t size,
                         const size_t col_sizes[], const size_t offsets[],
                         const size_t padded_size, const size_t start = 0) {
    for (size_t col = 0; col != cols; ++col) {
        switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        PackTupleFallbackTypedColImpl<ui##bits>(                               \
            src_cols[col], dst_rows + offsets[col], size, padded_size, start); \
        break

            MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

        default:
            for (size_t row = 0; row != size; ++row) {
                memcpy(dst_rows + row * padded_size + offsets[col],
                       src_cols[col] + (start + row) * col_sizes[col],
                       col_sizes[col]);
            }
        }
    }
}

static void
UnpackTupleFallbackColImpl(const ui8 *const src_rows, ui8 *const dst_cols[],
                           const size_t cols, const size_t size,
                           const size_t col_sizes[], const size_t offsets[],
                           const size_t padded_size, const size_t start = 0) {
    for (size_t col = 0; col != cols; ++col) {
        switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        UnpackTupleFallbackTypedColImpl<ui##bits>(                             \
            src_rows + offsets[col], dst_cols[col], size, padded_size, start); \
        break

            MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

        default:
            for (size_t row = 0; row != size; ++row) {
                memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                       src_rows + row * padded_size + offsets[col],
                       col_sizes[col]);
            }
        }
    }
}

[[maybe_unused]] static void PackTupleFallbackBlockImpl(
    const ui8 *const src_cols[], ui8 *const dst_rows, const size_t cols,
    const size_t size, const size_t col_sizes[], const size_t offsets[],
    const size_t padded_size, const size_t block_rows, const size_t start = 0) {

    const size_t block_size = size / block_rows;
    for (size_t block = 0; block != block_size; ++block) {
        for (size_t col = 0; col != cols; ++col) {
            switch (col_sizes[col] * 8) {

#define BLOCK_LOOP(...)                                                        \
    for (size_t block_i = 0; block_i != block_rows; ++block_i) {               \
        const size_t row = block_rows * block + block_i;                       \
        __VA_ARGS__                                                            \
    }

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        PackTupleFallbackTypedColImpl<ui##bits>(                               \
            src_cols[col],                                                     \
            dst_rows + block * block_rows * padded_size + offsets[col],        \
            block_rows, padded_size, start + block * block_rows);              \
        break

                MULTY_8x4(CASE);

            default:
                BLOCK_LOOP(
                    memcpy(dst_rows + row * padded_size + offsets[col],
                           src_cols[col] + (start + row) * col_sizes[col],
                           col_sizes[col]);)

#undef CASE
#undef MULTY_8x4
#undef BLOCK_LOOP
            }
        }
    }

    PackTupleFallbackColImpl(
        src_cols, dst_rows + block_size * block_rows * padded_size, cols,
        size - block_size * block_rows, col_sizes, offsets, padded_size,
        start + block_size * block_rows);
}

[[maybe_unused]] static void UnpackTupleFallbackBlockImpl(
    const ui8 *const src_rows, ui8 *const dst_cols[], const size_t cols,
    const size_t size, const size_t col_sizes[], const size_t offsets[],
    const size_t padded_size, const size_t block_rows, const size_t start = 0) {

    const size_t block_size = size / block_rows;
    for (size_t block = 0; block != block_size; ++block) {
        for (size_t col = 0; col != cols; ++col) {
            switch (col_sizes[col] * 8) {

#define BLOCK_LOOP(...)                                                        \
    for (size_t block_i = 0; block_i != block_rows; ++block_i) {               \
        const size_t row = block_rows * block + block_i;                       \
        __VA_ARGS__                                                            \
    }

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        UnpackTupleFallbackTypedColImpl<ui##bits>(                             \
            src_rows + block * block_rows * padded_size + offsets[col],        \
            dst_cols[col], block_rows, padded_size,                            \
            start + block * block_rows);                                       \
        break

                MULTY_8x4(CASE);

            default:
                BLOCK_LOOP(
                    memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                           src_rows + row * padded_size + offsets[col],
                           col_sizes[col]);)

#undef CASE
#undef MULTY_8x4
#undef BLOCK_LOOP
            }
        }
    }

    UnpackTupleFallbackColImpl(src_rows + block_size * block_rows * padded_size,
                               dst_cols, cols, size - block_size * block_rows,
                               col_sizes, offsets, padded_size,
                               start + block_size * block_rows);
}

template <class TTraits> struct SIMDPack {
    template <class T> using TSimd = typename TTraits::template TSimd8<T>;

    /// [8,16,32,64]-bits iters
    static const ui8 BaseIters = 4;

    /// 128-bit lane iters
    static const ui8 LaneIters = [] {
        if constexpr (std::is_same_v<TTraits, NSimd::TSimdAVX2Traits>) {
            return 1;
        }
        return 0;
    }();

    static const ui8 TransposeIters = BaseIters + LaneIters;

    /// bits reversed
    static constexpr ui8 TransposeRevInd[BaseIters][1 << BaseIters] = {
        {
            0x0,
            0x8,
            0x4,
            0xc,
            0x2,
            0xa,
            0x6,
            0xe,
            0x1,
            0x9,
            0x5,
            0xd,
            0x3,
            0xb,
            0x7,
            0xf,
        },
        {
            0x0,
            0x4,
            0x2,
            0x6,
            0x1,
            0x5,
            0x3,
            0x7,
        },
        {
            0x0,
            0x2,
            0x1,
            0x3,
        },
        {
            0x0,
            0x1,
        },
    };

    template <ui8 Cols, ui8 LogIter>
    static void Transpose(TSimd<ui8> (&regs)[2][Cols]) {
        /// iterative transposition, starting from ColSize:
        ///     ui8 -> ui16 -> ui32 -> ui64 -> 128bit-lane
        /// smth like fourier butterfly

#define TRANSPOSE_ITER(iter, bits)                                             \
    if constexpr (LogIter <= iter) {                                           \
        constexpr bool from = iter % 2;                                        \
        constexpr bool to = from ^ 1;                                          \
                                                                               \
        constexpr ui8 log = iter - LogIter;                                    \
        constexpr ui8 exp = 1u << log;                                         \
                                                                               \
        for (ui8 col = 0; col != Cols; ++col) {                                \
            switch ((col & exp) >> log) {                                      \
            case 0: {                                                          \
                regs[to][col] = TSimd<ui8>::UnpackLaneLo##bits(                \
                    regs[from][col & ~exp], regs[from][col | exp]);            \
                break;                                                         \
            }                                                                  \
            case 1: {                                                          \
                regs[to][col] = TSimd<ui8>::UnpackLaneHi##bits(                \
                    regs[from][col & ~exp], regs[from][col | exp]);            \
                break;                                                         \
            }                                                                  \
            default:;                                                          \
            }                                                                  \
        }                                                                      \
    }

        TRANSPOSE_ITER(0, 8);
        TRANSPOSE_ITER(1, 16);
        TRANSPOSE_ITER(2, 32);
        TRANSPOSE_ITER(3, 64);
    
#undef TRANSPOSE_ITER

        if constexpr (LaneIters == 1) {
            constexpr auto iter = BaseIters;

            constexpr bool from = iter % 2;
            constexpr bool to = from ^ 1;

            constexpr ui8 log = iter - LogIter;
            constexpr ui8 exp = 1u << log;

            for (ui8 col = 0; col != Cols; ++col) {
                switch ((col & exp) >> log) {
                case 0: {
                    regs[to][col] =
                        TSimd<ui8>::template PermuteLanes<0 + 2 * 16>(
                            regs[from][col & ~exp], regs[from][col | exp]);
                    break;
                }
                case 1: {
                    regs[to][col] =
                        TSimd<ui8>::template PermuteLanes<1 + 3 * 16>(
                            regs[from][col & ~exp], regs[from][col | exp]);
                    break;
                }
                }
            }
        } else if constexpr (LaneIters) {
            static_assert(!LaneIters, "Not implemented");
        }
    }

    template <ui8 ColSize>
    static void PackColSizeImpl(const ui8 *const src_cols[],
                                ui8 *const dst_rows, const size_t size,
                                const size_t cols_num, const size_t padded_size,
                                const size_t start = 0) {
        static constexpr ui8 Cols = TSimd<ui8>::SIZE / ColSize;
        static constexpr ui8 LogIter = std::countr_zero(ColSize);
        static constexpr std::array<size_t, Cols> ColSizes = [] {
            std::array<size_t, Cols> offsets;
            for (size_t ind = 0; ind != Cols; ++ind) {
                offsets[ind] = ColSize;
            }
            return offsets;
        }();
        static constexpr std::array<size_t, Cols> Offsets = [] {
            std::array<size_t, Cols> offsets;
            for (size_t ind = 0; ind != Cols; ++ind) {
                offsets[ind] = ind * ColSize;
            }
            return offsets;
        }();

        const size_t simd_iters = size / Cols;

        TSimd<ui8> regs[2][Cols];

        for (size_t cols_group = 0; cols_group < cols_num; cols_group += Cols) {
            const ui8 *srcs[Cols];
            std::memcpy(srcs, src_cols + cols_group, sizeof(srcs));
            for (ui8 col = 0; col != Cols; ++col) {
                srcs[col] += ColSize * start;
            }

            auto dst = dst_rows + cols_group * ColSize;
            ui8 *const end = dst + simd_iters * Cols * padded_size;

            while (dst != end) {
                for (ui8 col = 0; col != Cols; ++col) {
                    regs[LogIter % 2][col] = TSimd<ui8>(srcs[col]);
                    srcs[col] += TSimd<ui8>::SIZE;
                }

                Transpose<Cols, LogIter>(regs);

                const bool res = TransposeIters % 2;
                for (ui8 col = 0; col != Cols; ++col) {
                    if constexpr (LaneIters) {
                        const ui8 half_ind = col % (Cols / 2);
                        const ui8 half_shift = col & (Cols / 2);
                        regs[res]
                            [TransposeRevInd[LogIter][half_ind] + half_shift]
                                .Store(dst);
                        dst += padded_size;
                    } else {
                        regs[res][TransposeRevInd[LogIter][col]].Store(dst);
                        dst += padded_size;
                    }
                }
            }

            PackTupleFallbackRowImpl(srcs, dst, Cols, size - simd_iters * Cols,
                                     ColSizes.data(), Offsets.data(),
                                     padded_size);
        }
    }

    template <ui8 ColSize>
    static void
    UnpackColSizeImpl(const ui8 *const src_rows, ui8 *const dst_cols[],
                      size_t size, const size_t cols_num,
                      const size_t padded_size, const size_t start = 0) {
        static constexpr ui8 Cols = TSimd<ui8>::SIZE / ColSize;
        static constexpr ui8 LogIter = std::countr_zero(ColSize);
        static constexpr std::array<size_t, Cols> ColSizes = [] {
            std::array<size_t, Cols> offsets;
            for (size_t ind = 0; ind != Cols; ++ind) {
                offsets[ind] = ColSize;
            }
            return offsets;
        }();
        static constexpr std::array<size_t, Cols> Offsets = [] {
            std::array<size_t, Cols> offsets;
            for (size_t ind = 0; ind != Cols; ++ind) {
                offsets[ind] = ind * ColSize;
            }
            return offsets;
        }();

        const size_t simd_iters = size / Cols;

        TSimd<ui8> regs[2][Cols];

        for (size_t cols_group = 0; cols_group < cols_num; cols_group += Cols) {
            auto src = src_rows + cols_group * ColSize;
            const ui8 *const end = src + simd_iters * Cols * padded_size;

            ui8 *dsts[Cols];
            std::memcpy(dsts, dst_cols + cols_group, sizeof(dsts));
            for (ui8 col = 0; col != Cols; ++col) {
                dsts[col] += ColSize * start;
            }

            while (src != end) {
                for (ui8 iter = 0; iter != Cols; ++iter) {
                    regs[LogIter % 2][iter] = TSimd<ui8>(src);
                    src += padded_size;
                }

                Transpose<Cols, LogIter>(regs);

                const bool res = TransposeIters % 2;
                for (ui8 col = 0; col != Cols; ++col) {
                    if constexpr (LaneIters) {
                        const ui8 half_ind = col % (Cols / 2);
                        const ui8 half_shift = col & (Cols / 2);
                        regs[res]
                            [TransposeRevInd[LogIter][half_ind] + half_shift]
                                .Store(dsts[col]);
                        dsts[col] += TSimd<ui8>::SIZE;
                    } else {
                        regs[res][TransposeRevInd[LogIter][col]].Store(
                            dsts[col]);
                        dsts[col] += TSimd<ui8>::SIZE;
                    }
                }
            }

            UnpackTupleFallbackRowImpl(
                src, dsts, Cols, size - simd_iters * Cols, ColSizes.data(),
                Offsets.data(), padded_size);
        }
    }

    static void PackColSize(const ui8 *const src_cols[], ui8 *const dst_rows,
                            const size_t size, const size_t col_size,
                            const size_t cols_num, const size_t padded_size,
                            const size_t start = 0) {
        switch (col_size) {
        case 1:
            PackColSizeImpl<1>(src_cols, dst_rows, size, cols_num, padded_size,
                               start);
            break;
        case 2:
            PackColSizeImpl<2>(src_cols, dst_rows, size, cols_num, padded_size,
                               start);
            break;
        case 4:
            PackColSizeImpl<4>(src_cols, dst_rows, size, cols_num, padded_size,
                               start);
            break;
        case 8:
            PackColSizeImpl<8>(src_cols, dst_rows, size, cols_num, padded_size,
                               start);
            break;
        default:
            throw std::runtime_error("What? Unexpected pack switch case " +
                                     std::to_string(col_size));
        }
    }

    static void UnpackColSize(const ui8 *const src_rows, ui8 *const dst_cols[],
                              const size_t size, const size_t col_size,
                              const size_t cols_num, const size_t padded_size,
                              const size_t start = 0) {
        switch (col_size) {
        case 1:
            UnpackColSizeImpl<1>(src_rows, dst_cols, size, cols_num,
                                 padded_size, start);
            break;
        case 2:
            UnpackColSizeImpl<2>(src_rows, dst_cols, size, cols_num,
                                 padded_size, start);
            break;
        case 4:
            UnpackColSizeImpl<4>(src_rows, dst_cols, size, cols_num,
                                 padded_size, start);
            break;
        case 8:
            UnpackColSizeImpl<8>(src_rows, dst_cols, size, cols_num,
                                 padded_size, start);
            break;
        default:
            throw std::runtime_error("What? Unexpected unpack switch case " +
                                     std::to_string(col_size));
        }
    }

    static TSimd<ui8> BuildTuplePerm(size_t col_size, size_t col_pad,
                                     size_t tuple_size, ui8 offset, ui8 ind,
                                     bool packing) {
        ui8 perm[TSimd<ui8>::SIZE];
        std::memset(perm, 0x80, TSimd<ui8>::SIZE);

        size_t iters = std::max(1ul, 1 + (TSimd<ui8>::SIZE - tuple_size) /
                                             (col_size + col_pad));
        while (iters--) {
            for (size_t it = col_size; it; --it, ++offset, ++ind) {
                if (packing) {
                    perm[offset] = ind;
                } else {
                    perm[ind] = offset;
                }
            }
            offset += col_pad;
        }

        return TSimd<ui8>{perm};
    }

    template <ui8 TupleSize> static TSimd<ui8> TupleOr(TSimd<ui8> vec[]) {
        return TupleOrImpl<TupleSize>(vec);
    }

    template <ui8 TupleSize> static TSimd<ui8> TupleOrImpl(TSimd<ui8> vec[]) {
        static constexpr ui8 Left = TupleSize / 2;
        static constexpr ui8 Right = TupleSize - Left;

        return TupleOrImpl<Left>(vec) | TupleOrImpl<Right>(vec + Left);
    }

    template <> TSimd<ui8> TupleOrImpl<0>(TSimd<ui8>[]) { std::abort(); }

    template <> TSimd<ui8> TupleOrImpl<1>(TSimd<ui8> vec[]) { return vec[0]; }

    template <> TSimd<ui8> TupleOrImpl<2>(TSimd<ui8> vec[]) {
        return vec[0] | vec[1];
    }

    template <ui8 StoresPerLoad, ui8 Cols>
    static void PackTupleOr(const ui8 *const src_cols[], ui8 *const dst_rows,
                            const size_t size, const size_t col_sizes[],
                            const size_t offsets[], const size_t tuple_size,
                            const size_t padded_size, const TSimd<ui8> perms[],
                            const size_t start = 0) {
        static constexpr size_t kSIMD_Rem = sizeof(TSimd<ui8>) - StoresPerLoad;
        const ui8 tuples_per_store =
            std::max(1ul, 1 + (TSimd<ui8>::SIZE - tuple_size) / padded_size);
        const size_t simd_iters = (size > kSIMD_Rem ? size - kSIMD_Rem : 0) /
                                  (tuples_per_store * StoresPerLoad);

        TSimd<ui8> src_regs[Cols];
        TSimd<ui8> perm_regs[Cols];

        const ui8 *srcs[Cols];
        std::memcpy(srcs, src_cols, sizeof(srcs));
        for (ui8 col = 0; col != Cols; ++col) {
            srcs[col] += col_sizes[col] * start;
        }

        auto dst = dst_rows;
        ui8 *const end = dst_rows + simd_iters * tuples_per_store *
                                        StoresPerLoad * padded_size;
        while (dst != end) {
            for (ui8 col = 0; col != Cols; ++col) {
                src_regs[col] = TSimd<ui8>(srcs[col]);
                srcs[col] += col_sizes[col] * tuples_per_store * StoresPerLoad;
            }

            for (ui8 iter = 0; iter != StoresPerLoad; ++iter) {
                // shuffling each col bytes to the right positions
                // then blending them together with 'or'
                for (ui8 col = 0; col != Cols; ++col) {
                    perm_regs[col] = src_regs[col].Shuffle(
                        perms[col * StoresPerLoad + iter]);
                }

                TupleOr<Cols>(perm_regs).Store(dst);
                dst += padded_size * tuples_per_store;
            }
        }

        PackTupleFallbackRowImpl(srcs, dst, Cols,
                                 size - simd_iters * tuples_per_store *
                                            StoresPerLoad,
                                 col_sizes, offsets, padded_size);
    }

    template <ui8 LoadsPerStore, ui8 Cols>
    static void
    UnpackTupleOr(const ui8 *const src_rows, ui8 *const dst_cols[], size_t size,
                  const size_t col_sizes[], const size_t offsets[],
                  const size_t tuple_size, const size_t padded_size,
                  const TSimd<ui8> perms[], const size_t start = 0) {
        static constexpr size_t kSIMD_Rem = sizeof(TSimd<ui8>) - LoadsPerStore;
        const ui8 tuples_per_load =
            std::max(1ul, 1 + (TSimd<ui8>::SIZE - tuple_size) / padded_size);
        const size_t simd_iters = (size > kSIMD_Rem ? size - kSIMD_Rem : 0) /
                                  (tuples_per_load * LoadsPerStore);

        TSimd<ui8> src_regs[LoadsPerStore];
        TSimd<ui8> perm_regs[LoadsPerStore];

        auto src = src_rows;
        const ui8 *const end = src_rows + simd_iters * tuples_per_load *
                                              LoadsPerStore * padded_size;

        ui8 *dsts[Cols];
        std::memcpy(dsts, dst_cols, sizeof(dsts));
        for (ui8 col = 0; col != Cols; ++col) {
            dsts[col] += col_sizes[col] * start;
        }

        while (src != end) {
            for (ui8 iter = 0; iter != LoadsPerStore; ++iter) {
                src_regs[iter] = TSimd<ui8>(src);
                src += padded_size * tuples_per_load;
            }

            for (ui8 col = 0; col != Cols; ++col) {
                // shuffling each col bytes to the right positions
                // then blending them together with 'or'
                for (ui8 iter = 0; iter != LoadsPerStore; ++iter) {
                    perm_regs[iter] = src_regs[iter].Shuffle(
                        perms[col * LoadsPerStore + iter]);
                }

                TupleOr<LoadsPerStore>(perm_regs).Store(dsts[col]);
                dsts[col] += col_sizes[col] * tuples_per_load * LoadsPerStore;
            }
        }

        UnpackTupleFallbackRowImpl(src, dsts, Cols,
                                   size - simd_iters * tuples_per_load *
                                              LoadsPerStore,
                                   col_sizes, offsets, padded_size);
    }
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
