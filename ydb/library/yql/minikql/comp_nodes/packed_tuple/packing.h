#include <util/system/unaligned_mem.h>
#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

static void
PackTupleFallbackRowImpl(const ui8 *const src_cols[], ui8 *const dst_rows,
                         const size_t cols, const size_t size,
                         const size_t col_sizes[], const size_t offsets[],
                         const size_t tuple_size, const size_t start = 0) {
    for (size_t row = 0; row != size; ++row) {
        for (ui8 col = 0; col != cols; ++col) {
            switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        *reinterpret_cast<ui##bits *>(dst_rows + row * tuple_size +            \
                                      offsets[col]) =                          \
            *reinterpret_cast<const ui##bits *>(src_cols[col] +                \
                                                (start + row) * (bits / 8));   \
        break

                MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

            default:
                memcpy(dst_rows + row * tuple_size + offsets[col],
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
                           const size_t tuple_size, const size_t start = 0) {
    for (size_t row = 0; row != size; ++row) {
        for (ui8 col = 0; col != cols; ++col) {
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
            *reinterpret_cast<const ui##bits *>(src_rows + row * tuple_size +  \
                                                offsets[col]);                 \
        break

                MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

            default:
                memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                       src_rows + row * tuple_size + offsets[col],
                       col_sizes[col]);
            }
        }
    }
}

template <class ByteType>
Y_FORCE_INLINE static void
PackTupleFallbackTypedColImpl(const ui8 *const src_col, ui8 *const dst_rows,
                              const size_t size, const size_t tuple_size,
                              const size_t start = 0) {
    static constexpr size_t BYTES = sizeof(ByteType);
    for (size_t row = 0; row != size; ++row) {
        WriteUnaligned<ByteType>(
            dst_rows + row * tuple_size,
            ReadUnaligned<ByteType>(src_col + (start + row) * BYTES));
    }
}

template <class ByteType>
Y_FORCE_INLINE static void
UnpackTupleFallbackTypedColImpl(const ui8 *const src_rows, ui8 *const dst_col,
                                const size_t size, const size_t tuple_size,
                                const size_t start = 0) {
    static constexpr size_t BYTES = sizeof(ByteType);
    for (size_t row = 0; row != size; ++row) {
        WriteUnaligned<ByteType>(
            dst_col + (start + row) * BYTES,
            ReadUnaligned<ByteType>(src_rows + row * tuple_size));
    }
}

static void
PackTupleFallbackColImpl(const ui8 *const src_cols[], ui8 *const dst_rows,
                         const size_t cols, const size_t size,
                         const size_t col_sizes[], const size_t offsets[],
                         const size_t tuple_size, const size_t start = 0) {
    for (ui8 col = 0; col != cols; ++col) {
        switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        PackTupleFallbackTypedColImpl<ui##bits>(                               \
            src_cols[col], dst_rows + offsets[col], size, tuple_size, start);  \
        break

            MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

        default:
            for (size_t row = 0; row != size; ++row) {
                memcpy(dst_rows + row * tuple_size + offsets[col],
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
                           const size_t tuple_size, const size_t start = 0) {
    for (ui8 col = 0; col != cols; ++col) {
        switch (col_sizes[col] * 8) {

#define MULTY_8x4(...)                                                         \
    __VA_ARGS__(8);                                                            \
    __VA_ARGS__(16);                                                           \
    __VA_ARGS__(32);                                                           \
    __VA_ARGS__(64)

#define CASE(bits)                                                             \
    case bits:                                                                 \
        UnpackTupleFallbackTypedColImpl<ui##bits>(                             \
            src_rows + offsets[col], dst_cols[col], size, tuple_size, start);  \
        break

            MULTY_8x4(CASE);

#undef CASE
#undef MULTY_8x4

        default:
            for (size_t row = 0; row != size; ++row) {
                memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                       src_rows + row * tuple_size + offsets[col],
                       col_sizes[col]);
            }
        }
    }
}

[[maybe_unused]] static void PackTupleFallbackBlockImpl(
    const ui8 *const src_cols[], ui8 *const dst_rows, const size_t cols,
    const size_t size, const size_t col_sizes[], const size_t offsets[],
    const size_t tuple_size, const size_t block_rows, const size_t start = 0) {

    const size_t block_size = size / block_rows;
    for (size_t block = 0; block != block_size; ++block) {
        for (ui8 col = 0; col != cols; ++col) {
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
            dst_rows + block * block_rows * tuple_size + offsets[col],         \
            block_rows, tuple_size, start + block * block_rows);               \
        break

                MULTY_8x4(CASE);

            default:
                BLOCK_LOOP(
                    memcpy(dst_rows + row * tuple_size + offsets[col],
                           src_cols[col] + (start + row) * col_sizes[col],
                           col_sizes[col]);)

#undef CASE
#undef MULTY_8x4
#undef BLOCK_LOOP
            }
        }
    }

    PackTupleFallbackColImpl(
        src_cols, dst_rows + block_size * block_rows * tuple_size, cols,
        size - block_size * block_rows, col_sizes, offsets, tuple_size,
        start + block_size * block_rows);
}

[[maybe_unused]] static void UnpackTupleFallbackBlockImpl(
    const ui8 *const src_rows, ui8 *const dst_cols[], const size_t cols,
    const size_t size, const size_t col_sizes[], const size_t offsets[],
    const size_t tuple_size, const size_t block_rows, const size_t start = 0) {

    const size_t block_size = size / block_rows;
    for (size_t block = 0; block != block_size; ++block) {
        for (ui8 col = 0; col != cols; ++col) {
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
            src_rows + block * block_rows * tuple_size + offsets[col],         \
            dst_cols[col], block_rows, tuple_size,                             \
            start + block * block_rows);                                       \
        break

                MULTY_8x4(CASE);

            default:
                BLOCK_LOOP(
                    memcpy(dst_cols[col] + (start + row) * col_sizes[col],
                           src_rows + row * tuple_size + offsets[col],
                           col_sizes[col]);)

#undef CASE
#undef MULTY_8x4
#undef BLOCK_LOOP
            }
        }
    }

    UnpackTupleFallbackColImpl(src_rows + block_size * block_rows * tuple_size,
                               dst_cols, cols, size - block_size * block_rows,
                               col_sizes, offsets, tuple_size,
                               start + block_size * block_rows);
}

template <class TTraits> struct SIMDPack {
    template <class T> using TSimd = typename TTraits::template TSimd8<T>;

    static TSimd<ui8> BuildTuplePerm(size_t col_size, size_t col_pad,
                                     ui8 offset, ui8 ind, bool packing) {
        ui8 perm[TSimd<ui8>::SIZE];
        std::memset(perm, 0x80, TSimd<ui8>::SIZE);

        size_t iters = std::max(size_t(1u), TSimd<ui8>::SIZE / (col_size + col_pad));
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
    static void
    PackTupleOrImpl(const ui8 *const src_cols[], ui8 *const dst_rows,
                    const size_t size, const size_t col_sizes[],
                    const size_t offsets[], const size_t tuple_size,
                    const TSimd<ui8> perms[], const size_t start = 0) {
        static constexpr size_t kSIMD_Rem = sizeof(TSimd<ui8>) - StoresPerLoad;
        const ui8 tuples_per_store =
            std::max(size_t(1u), TSimd<ui8>::SIZE / tuple_size);
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
                                        StoresPerLoad * tuple_size;
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
                dst += tuple_size * tuples_per_store;
            }
        }

        PackTupleFallbackRowImpl(srcs, dst, Cols,
                                 size - simd_iters * tuples_per_store *
                                            StoresPerLoad,
                                 col_sizes, offsets, tuple_size);
    }

    template <ui8 LoadsPerStore, ui8 Cols>
    static void
    UnpackTupleOrImpl(const ui8 *const src_rows, ui8 *const dst_cols[],
                      size_t size, const size_t col_sizes[],
                      const size_t offsets[], const size_t tuple_size,
                      const TSimd<ui8> perms[], const size_t start = 0) {
        static constexpr size_t kSIMD_Rem = sizeof(TSimd<ui8>) - LoadsPerStore;
        const ui8 tuples_per_load =
            std::max(size_t(1u), TSimd<ui8>::SIZE / tuple_size);
        const size_t simd_iters = (size > kSIMD_Rem ? size - kSIMD_Rem : 0) /
                                  (tuples_per_load * LoadsPerStore);

        TSimd<ui8> src_regs[LoadsPerStore];
        TSimd<ui8> perm_regs[LoadsPerStore];

        auto src = src_rows;
        const ui8 *const end = src_rows + simd_iters * tuples_per_load *
                                              LoadsPerStore * tuple_size;

        ui8 *dsts[Cols];
        std::memcpy(dsts, dst_cols, sizeof(dsts));
        for (ui8 col = 0; col != Cols; ++col) {
            dsts[col] += col_sizes[col] * start;
        }

        while (src != end) {
            for (ui8 iter = 0; iter != LoadsPerStore; ++iter) {
                src_regs[iter] = TSimd<ui8>(src);
                src += tuple_size * tuples_per_load;
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
                                   col_sizes, offsets, tuple_size);
    }
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
