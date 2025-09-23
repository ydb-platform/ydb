#include <util/system/unaligned_mem.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

[[maybe_unused]]
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

[[maybe_unused]]
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


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
