#include <library/cpp/testing/unittest/registar.h>
#include <type_traits>
#include <memory>
#include <cstddef>
#include <utility>
#include <cstdint>
#include <cstring>

#include "../simd.h"

// Helpers to make pointer to buffer that contains such sequence:
// <From, From + 1, From + 2, ..., From + N>
template <size_t From, size_t... Is>
auto MakeSequenceImpl(std::index_sequence<Is...>)
{
    return std::unique_ptr<i8[]>{new i8[sizeof...(Is)]{(static_cast<i8>(Is + From))...}};
}

template <size_t N, size_t From = 0>
auto MakeSequence()
{
    return MakeSequenceImpl<From>(std::make_index_sequence<N>{});
}

// Class to fill data buffers and result to validate MergeColumns
template <size_t Size1, size_t Size2, size_t Size3, size_t Size4,
          size_t AccumulateSize = Size1 + Size2 + Size3 + Size4>
class ByteFiller {
public:
    explicit ByteFiller(size_t length)
        : m_length(length) {
    }

    /*
    Fill data buffers like this:
    data[0] = <0, 1, ..., Size1 - 1, 0, 1, ..., Size1 - 1, ...>
    data[1] = <Size1, Size + 1, ..., Size1 + Size2 - 1, Size1, Size + 1, ..., Size1 + Size2 - 1, ...>
    ...

    For example, if Size1 == Size2 == Size3 == Size4 == 4:
    data[0] = <0,  1,  2,  3,  0,  1,  2,  3, ...>
    data[1] = <4,  5,  6,  7,  4,  5,  6,  7, ...>
    data[2] = <8,  9,  10, 11, 8,  9,  10, 11, ...>
    data[3] = <12, 13, 14, 15, 12, 13, 14, 15, ...>
    */
    void FillData(i8* data[4]) const {
        for (auto col_i = 0; col_i < 4; ++col_i) {
            i8* column = data[col_i];
            for (size_t i = 0; i < m_length; ++i) {
                std::memcpy(column + m_sizes[col_i] * i, m_elems[col_i].get(), m_sizes[col_i]);
            }
        }
    }

    /*
    Fill result buffer like this:
    data[result] = <0, 1, ..., Size1 + Size2 + Size 3 + Size 4 - 1, 0, 1, ..., Size1 + Size2 + Size 3 + Size 4 - 1, ...>

    For example, if Size1 == Size2 == Size3 == Size4 == 4:
    result = <0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, ...>
    */    
    void FillResult(i8* result) const {
        for (size_t i = 0; i < m_length; ++i) {
            std::memcpy(result + AccumulateSize * i, m_accElem.get(), AccumulateSize);
        }
    }

    // Check byte order in result
    bool CheckResult(const i8* result) const {
        for (size_t i = 0; i < m_length; ++i) {
            for (size_t byte_n = 0; byte_n < AccumulateSize; ++byte_n) {
                if (static_cast<size_t>(result[i * AccumulateSize + byte_n]) != byte_n) {
                    return false;
                }
            }
        }
        return true;
    }

private:
    size_t m_length;
    std::unique_ptr<i8[]> m_elems[4] = {
        MakeSequence<Size1, 0>(),
        MakeSequence<Size2, Size1>(),
        MakeSequence<Size3, Size1 + Size2>(),
        MakeSequence<Size4, Size1 + Size2 + Size3>()
    };
    size_t m_sizes[4] = {
        Size1,
        Size2,
        Size3,
        Size4
    };
    std::unique_ptr<i8[]> m_accElem = MakeSequence<AccumulateSize, 0>();
};

// Tests to check if ByteFiller works correctly
Y_UNIT_TEST_SUITE(CheckFiller) {
    Y_UNIT_TEST(CheckResultTest) {
        i8 expected_result[30] = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        };
        ByteFiller<1, 2, 3, 4> bf{3};

        UNIT_ASSERT_EQUAL(bf.CheckResult(expected_result), true);
    }

    Y_UNIT_TEST(FillResultTest) {
        i8 result[30]{};
        ByteFiller<1, 2, 3, 4> bf{3};
        bf.FillResult(result);

        UNIT_ASSERT_EQUAL(bf.CheckResult(result), true);
    }

    Y_UNIT_TEST(FillDataTest) {
        ByteFiller<1, 2, 3, 4> bf{3};

        i8 result[30]{};
        bf.FillResult(result);

        i8 d1[3]{};
        i8 d2[6]{};
        i8 d3[9]{};
        i8 d4[12]{};
        i8* data[4] = {
            d1, d2, d3, d4
        };

        bf.FillData(data);

        UNIT_ASSERT_EQUAL(!memcmp(d1, (i8[3]){0, 0, 0}, 3), true);
        UNIT_ASSERT_EQUAL(!memcmp(d2, (i8[6]){1, 2, 1, 2, 1, 2}, 6), true);
        UNIT_ASSERT_EQUAL(!memcmp(d3, (i8[9]){3, 4, 5, 3, 4, 5, 3, 4, 5}, 9), true);
        UNIT_ASSERT_EQUAL(!memcmp(d4, (i8[12]){6, 7, 8, 9, 6, 7, 8, 9, 6, 7, 8, 9}, 12), true);
    }
}

// Functions to force choice of trait
template <typename TFactory>
auto ChooseAVX2Trait(TFactory& factory) {
    return factory.template Create<NSimd::AVX2Trait>();
}

template <typename TFactory>
auto ChooseSSE42Trait(TFactory& factory) {
    return factory.template Create<NSimd::SSE42Trait>();
}

template <typename TFactory>
auto ChooseFallbackTrait(TFactory& factory) {
    return factory.template Create<NSimd::FallbackTrait>();
}

// Unit tests macro to check bytes order of elements with same size
#define EQ_SIZE_CHECKER(size, SIMD)                                             \
do {                                                                            \
    size_t sizes[4] = {(size), (size), (size), (size)};                         \
    ByteFiller<(size), (size), (size), (size)> bf{10};                          \
                                                                                \
    i8 expected_result[4 * (size) * 10]{};                                      \
    bf.FillResult(expected_result);                                             \
                                                                                \
    i8 d1[(size) * 10]{};                                                       \
    i8 d2[(size) * 10]{};                                                       \
    i8 d3[(size) * 10]{};                                                       \
    i8 d4[(size) * 10]{};                                                       \
    i8* data[4]{                                                                \
        d1, d2, d3, d4                                                          \
    };                                                                          \
    bf.FillData(data);                                                          \
                                                                                \
    NSimd::Perfomancer perfomancer;                                             \
    auto worker = Choose ## SIMD ## Trait(perfomancer);                         \
    i8 result[4 * (size) * 10]{};                                               \
    worker->MergeColumns(result, data, sizes, 10);                              \
    auto cmp_res = memcmp(expected_result, result, 4 * (size) * 10);            \
    if (cmp_res) {                                                              \
        Cerr << "Expected result:" << Endl;                                     \
        for (auto val: expected_result) {                                       \
            Cerr << static_cast<size_t>(val) << " ";                            \
        }                                                                       \
        Cerr << Endl;                                                           \
        Cerr << "Result returned from MergeColumns:" << Endl;                   \
        for (auto val: result) {                                                \
            Cerr << static_cast<size_t>(val) << " ";                            \
        }                                                                       \
        Cerr << Endl;                                                           \
    }                                                                           \
    UNIT_ASSERT_EQUAL(!cmp_res, true);                                          \
} while(0)

// Tests to check byte order on different arch-s
Y_UNIT_TEST_SUITE(CheckMergeColumnsAVX2) {
    Y_UNIT_TEST(SizeEq1) {
        EQ_SIZE_CHECKER(1, AVX2);
    }

    Y_UNIT_TEST(SizeEq2) {
        EQ_SIZE_CHECKER(2, AVX2);
    }

    Y_UNIT_TEST(SizeEq3) {
        EQ_SIZE_CHECKER(3, AVX2);
    }

    Y_UNIT_TEST(SizeEq4) {
        EQ_SIZE_CHECKER(4, AVX2);
    }

    Y_UNIT_TEST(SizeEq5) {
        EQ_SIZE_CHECKER(5, AVX2);
    }

    Y_UNIT_TEST(SizeEq6) {
        EQ_SIZE_CHECKER(6, AVX2);
    }

    Y_UNIT_TEST(SizeEq7) {
        EQ_SIZE_CHECKER(7, AVX2);
    }

    Y_UNIT_TEST(SizeEq8) {
        EQ_SIZE_CHECKER(8, AVX2);
    }

    // Y_UNIT_TEST(DifferentSizes) {
    //     size_t sizes[4] = {1, 2, 3, 4};
    //     ByteFiller<1, 2, 3, 4> bf{10};

    //     i8 expected_result[(1 + 2 + 3 + 4) * 10]{};
    //     bf.FillResult(expected_result);

    //     i8 d1[1 * 10]{};
    //     i8 d2[2 * 10]{};
    //     i8 d3[3 * 10]{};
    //     i8 d4[4 * 10]{};
    //     i8* data[4]{
    //         d1, d2, d3, d4
    //     };
    //     bf.FillData(data);

    //     NSimd::Perfomancer perfomancer;
    //     auto worker = ChooseAVX2Trait(perfomancer);
    //     i8 result[(1 + 2 + 3 + 4) * 10]{};
    //     worker->MergeColumns(result, data, sizes, 10);
    //     UNIT_ASSERT_EQUAL(!memcmp(expected_result, result, (1 + 2 + 3 + 4) * 10), true);
    // }
}

Y_UNIT_TEST_SUITE(CheckMergeColumnsSSE42) {
    Y_UNIT_TEST(SizeEq1) {
        EQ_SIZE_CHECKER(1, SSE42);
    }

    Y_UNIT_TEST(SizeEq2) {
        EQ_SIZE_CHECKER(2, SSE42);
    }

    Y_UNIT_TEST(SizeEq3) {
        EQ_SIZE_CHECKER(3, SSE42);
    }

    Y_UNIT_TEST(SizeEq4) {
        EQ_SIZE_CHECKER(4, SSE42);
    }

    // Y_UNIT_TEST(DifferentSizes) {
    //     size_t sizes[4] = {1, 2, 3, 4};
    //     ByteFiller<1, 2, 3, 4> bf{10};

    //     i8 expected_result[(1 + 2 + 3 + 4) * 10]{};
    //     bf.FillResult(expected_result);

    //     i8 d1[1 * 10]{};
    //     i8 d2[2 * 10]{};
    //     i8 d3[3 * 10]{};
    //     i8 d4[4 * 10]{};
    //     i8* data[4]{
    //         d1, d2, d3, d4
    //     };
    //     bf.FillData(data);

    //     NSimd::Perfomancer perfomancer;
    //     auto worker = ChooseSSE42Trait(perfomancer);
    //     i8 result[(1 + 2 + 3 + 4) * 10]{};
    //     worker->MergeColumns(result, data, sizes, 10);
    //     UNIT_ASSERT_EQUAL(!memcmp(expected_result, result, (1 + 2 + 3 + 4) * 10), true);
    // }
}

Y_UNIT_TEST_SUITE(CheckMergeColumnsFallback) {
    Y_UNIT_TEST(SizeEq1) {
        EQ_SIZE_CHECKER(1, Fallback);
    }

    Y_UNIT_TEST(SizeEq2) {
        EQ_SIZE_CHECKER(2, Fallback);
    }

    Y_UNIT_TEST(SizeEq3) {
        EQ_SIZE_CHECKER(3, Fallback);
    }

    Y_UNIT_TEST(SizeEq4) {
        EQ_SIZE_CHECKER(4, Fallback);
    }

    Y_UNIT_TEST(SizeEq5) {
        EQ_SIZE_CHECKER(5, Fallback);
    }

    Y_UNIT_TEST(SizeEq6) {
        EQ_SIZE_CHECKER(6, Fallback);
    }

    Y_UNIT_TEST(SizeEq7) {
        EQ_SIZE_CHECKER(7, Fallback);
    }

    Y_UNIT_TEST(SizeEq8) {
        EQ_SIZE_CHECKER(8, Fallback);
    }

    // Y_UNIT_TEST(DifferentSizes) {
    //     size_t sizes[4] = {1, 2, 3, 4};
    //     ByteFiller<1, 2, 3, 4> bf{10};

    //     i8 expected_result[(1 + 2 + 3 + 4) * 10]{};
    //     bf.FillResult(expected_result);

    //     i8 d1[1 * 10]{};
    //     i8 d2[2 * 10]{};
    //     i8 d3[3 * 10]{};
    //     i8 d4[4 * 10]{};
    //     i8* data[4]{
    //         d1, d2, d3, d4
    //     };
    //     bf.FillData(data);

    //     NSimd::Perfomancer perfomancer;
    //     auto worker = ChooseFallbackTrait(perfomancer);
    //     i8 result[(1 + 2 + 3 + 4) * 10]{};
    //     worker->MergeColumns(result, data, sizes, 10);
    //     UNIT_ASSERT_EQUAL(!memcmp(expected_result, result, (1 + 2 + 3 + 4) * 10), true);
    // }
}
