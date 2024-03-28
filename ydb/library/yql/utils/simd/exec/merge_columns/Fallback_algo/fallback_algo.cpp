#include <ydb/library/yql/utils/simd/simd.h>

template <>
THolder<NSimd::Perfomancer::Interface> NSimd::Perfomancer::Create<NSimd::FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return MakeHolder<Algo<FallbackTrait>>();
}

namespace NSimd {

    static constexpr size_t Cols = 4;
    static constexpr size_t Block = 16;

    void FallbackMergeImpl(const i8* const (&src_cols)[Cols],
                           i8* const dst_rows, size_t size,
                           const size_t (&col_sizes)[Cols]) {
        size_t tuple_size = 0;
        size_t offsets[Cols];
        for (size_t col = 0; col != Cols; ++col) {
            offsets[col] = tuple_size;
            tuple_size += col_sizes[col];
        }

        for (size_t row = 0; row != size; ++row) {
            for (size_t col = 0; col != Cols; ++col) {
                memcpy(dst_rows + row * tuple_size + offsets[col],
                       src_cols[col] + row * col_sizes[col],
                       col_sizes[col]);
            }
        }
    }

#define BYTE_M_CASES(case_expr) \
    case 1:                     \
        case_expr(8) break;     \
    case 2:                     \
        case_expr(16) break;    \
    case 3:                     \
    case 4:                     \
        case_expr(32) break;    \
    case 5:                     \
    case 6:                     \
    case 7:                     \
    case 8:                     \
        case_expr(64) break;

    [[maybe_unused]] void FallbackMergeRowImpl(const i8* const (&src_cols)[Cols],
                                               i8* const dst_rows, size_t size,
                                               const size_t (&col_sizes)[Cols]) {
        size_t tuple_size = 0;
        size_t offsets[Cols];
        for (size_t col = 0; col != Cols; ++col) {
            offsets[col] = tuple_size;
            tuple_size += col_sizes[col];
        }

        const size_t no_overflow_size = size < 8 ? 0 : size - 8;
        for (size_t row = 0; row != no_overflow_size; ++row) {
            for (size_t col = 0; col != Cols; ++col) {
                switch (col_sizes[col]) {
#define CASE(bits)                                                             \
    *reinterpret_cast<ui##bits*>(dst_rows + row * tuple_size + offsets[col]) = \
        *reinterpret_cast<const ui##bits*>(src_cols[col] + row * (bits / 8));

                    BYTE_M_CASES(CASE);

#undef CASE

                    default:
                        memcpy(dst_rows + row * tuple_size + offsets[col],
                               src_cols[col] + row * col_sizes[col], col_sizes[col]);
                }
            }
        }

        const i8* srcs[Cols];
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            (..., (srcs[Is] = src_cols[Is] + no_overflow_size * col_sizes[Is]));
        }
        (std::make_index_sequence<Cols>{});
        FallbackMergeImpl(srcs, dst_rows + no_overflow_size * tuple_size, size - no_overflow_size, col_sizes);
    }

#undef BYTE_CASES

#define BYTE_CASES(case_expr) \
    case 1:                   \
        case_expr(8) break;   \
    case 2:                   \
        case_expr(16) break;  \
    case 4:                   \
        case_expr(32) break;  \
    case 8:                   \
        case_expr(64) break;

    void FallbackMergeColImpl(const i8* const (&src_cols)[Cols],
                              i8* const dst_rows, size_t size,
                              const size_t (&col_sizes)[Cols]) {
        size_t tuple_size = 0;
        size_t offsets[Cols];
        for (size_t col = 0; col != Cols; ++col) {
            offsets[col] = tuple_size;
            tuple_size += col_sizes[col];
        }

        for (size_t col = 0; col != Cols; ++col) {
            switch (col_sizes[col]) {
#define CASE(bits)                                                                \
    for (size_t row = 0; row != size; ++row) {                                    \
        *reinterpret_cast<ui##bits*>(dst_rows + row * tuple_size +                \
                                     offsets[col]) =                              \
            *reinterpret_cast<const ui##bits*>(src_cols[col] + row * (bits / 8)); \
    }

                BYTE_CASES(CASE)

#undef CASE

                default:
                    for (size_t row = 0; row != size; ++row) {
                        memcpy(dst_rows + row * tuple_size + offsets[col],
                               src_cols[col] + row * col_sizes[col], col_sizes[col]);
                    }
            }
        }
    }

    void FallbackMergeBlockImpl(const i8* const (&src_cols)[Cols],
                                i8* const dst_rows, size_t size,
                                const size_t (&col_sizes)[Cols]) {
        size_t tuple_size = 0;
        size_t offsets[Cols];
        for (size_t col = 0; col != Cols; ++col) {
            offsets[col] = tuple_size;
            tuple_size += col_sizes[col];
        }

        const size_t block_size = size / Block;
        for (size_t block = 0; block != block_size; ++block) {
            for (size_t col = 0; col != Cols; ++col) {
                switch (col_sizes[col] * 8) {
#define BLOCK_LOOP(...)                                     \
    for (size_t block_i = 0; block_i != Block; ++block_i) { \
        const size_t row = Block * block + block_i;         \
        __VA_ARGS__                                         \
    }

#define CASE(bits)                                                        \
    BLOCK_LOOP(*reinterpret_cast<ui##bits*>(dst_rows + row * tuple_size + \
                                            offsets[col]) =               \
                   *reinterpret_cast<const ui##bits*>(src_cols[col] +     \
                                                      row * (bits / 8));)

                    BYTE_CASES(CASE);

#undef CASE

                    default:
                        BLOCK_LOOP(memcpy(dst_rows + row * tuple_size + offsets[col],
                                          src_cols[col] + row * col_sizes[col],
                                          col_sizes[col]);)
                }
            }
        }

        [&]<size_t... Is>(std::index_sequence<Is...>) {
            FallbackMergeColImpl({src_cols[Is] + block_size * Block * col_sizes[Is]...},
                                 dst_rows + block_size * Block * tuple_size,
                                 size - block_size * Block, col_sizes);
        }
        (std::make_index_sequence<Cols>{});
    }

}
