#include <library/cpp/testing/unittest/registar.h>
#include <type_traits>
#include <memory>
#include <cstddef>
#include <utility>
#include <cstdint>
#include <cstring>
#include <numeric>

#include "../simd.h"
#include "filler.h"


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
#define EQ_SIZE_CHECKER_PRIMITIVE(sizes, SIMD)                                  \
do {                                                                            \
    size_t length = 10;                                                         \
    ByteFiller bf(sizes, length);                                               \
                                                                                \
    size_t acc = std::accumulate(sizes.begin(), sizes.end(), 0);                \
    std::vector<i8> expected_result(acc * length);                              \
    bf.FillRows(expected_result.data());                                        \
                                                                                \
    std::vector<std::vector<i8>> ds(sizes.size());                              \
    std::vector<i8*> data(sizes.size());                                        \
    for (size_t i = 0; i < ds.size(); ++i) {                                    \
        ds[i] = std::vector<i8>(sizes[i] * length);                             \
        data[i] = ds[i].data();                                                 \
    }                                                                           \
    bf.FillCols(data.data());                                                   \
                                                                                \
    NSimd::Perfomancer perfomancer;                                             \
    auto worker = Choose ## SIMD ## Trait(perfomancer);                         \
    std::vector<i8> result(acc * length);                                       \
    worker->MergeColumns(result.data(), data.data(), sizes.data(), length);     \
    auto cmp_res = memcmp(expected_result.data(), result.data(), acc * length); \
    if (cmp_res) {                                                              \
        Cerr << "Expected result:" << Endl;                                     \
        for (auto val: expected_result) {                                       \
            Cerr << static_cast<size_t>(val) << " ";                            \
        }                                                                       \
        Cerr << Endl;                                                           \
        Cerr << "Result returned from Pack:" << Endl;                           \
        for (auto val: result) {                                                \
            Cerr << static_cast<size_t>(val) << " ";                            \
        }                                                                       \
        Cerr << Endl;                                                           \
    }                                                                           \
    UNIT_ASSERT_EQUAL(!cmp_res, true);                                          \
} while(0)

// Tests to check byte order on different arch-s
Y_UNIT_TEST_SUITE(CheckTuplePackAVX2) {
    Y_UNIT_TEST(SizeEq1) {
        {
            std::vector<size_t> sizes = {1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq2) {
        {
            std::vector<size_t> sizes = {2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq3) {
        {
            std::vector<size_t> sizes = {3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq4) {
        {
            std::vector<size_t> sizes = {4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq5) {
        {
            std::vector<size_t> sizes = {5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq6) {
        {
            std::vector<size_t> sizes = {6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq7) {
        {
            std::vector<size_t> sizes = {7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(SizeEq8) {
        {
            std::vector<size_t> sizes = {8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
        }
    }

    Y_UNIT_TEST(DifferentSizes) {
        std::vector<size_t> sizes = {1, 2, 3, 4, 5, 6, 7, 8};
        EQ_SIZE_CHECKER_PRIMITIVE(sizes, AVX2);
    }
}

Y_UNIT_TEST_SUITE(CheckTuplePackSSE42) {
    Y_UNIT_TEST(SizeEq1) {
        {
            std::vector<size_t> sizes = {1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
    }

    Y_UNIT_TEST(SizeEq2) {
        {
            std::vector<size_t> sizes = {2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
    }

    Y_UNIT_TEST(SizeEq3) {
        {
            std::vector<size_t> sizes = {3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
    }

    Y_UNIT_TEST(SizeEq4) {
        {
            std::vector<size_t> sizes = {4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
        }
    }

    Y_UNIT_TEST(DifferentSizes) {
        std::vector<size_t> sizes = {1, 2, 3, 4, 5, 6, 7, 8};
        EQ_SIZE_CHECKER_PRIMITIVE(sizes, SSE42);
    }
}

Y_UNIT_TEST_SUITE(CheckTuplePackFallback) {
    Y_UNIT_TEST(SizeEq1) {
        {
            std::vector<size_t> sizes = {1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq2) {
        {
            std::vector<size_t> sizes = {2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq3) {
        {
            std::vector<size_t> sizes = {3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq4) {
        {
            std::vector<size_t> sizes = {4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq5) {
        {
            std::vector<size_t> sizes = {5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq6) {
        {
            std::vector<size_t> sizes = {6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq7) {
        {
            std::vector<size_t> sizes = {7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(SizeEq8) {
        {
            std::vector<size_t> sizes = {8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8};
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8}; // 8
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}; // 15
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
        {
            std::vector<size_t> sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}; // 24
            EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
        }
    }

    Y_UNIT_TEST(DifferentSizes) {
        std::vector<size_t> sizes = {1, 2, 3, 4, 5, 6, 7, 8};
        EQ_SIZE_CHECKER_PRIMITIVE(sizes, Fallback);
    }
}
