#include <library/cpp/testing/unittest/registar.h>
#include <type_traits>
#include <memory>
#include <cstddef>
#include <utility>
#include <cstdint>
#include <cstring>

#include "../algo.h"

template <size_t From, size_t... Is> 
auto MakeSequenceImpl(std::index_sequence<Is...>)
{
    return std::unique_ptr<ui8[]>{new ui8[sizeof...(Is)]{(static_cast<ui8>(Is + From))...}};
}

template <size_t N, size_t From = 0> 
auto MakeSequence()
{
    return MakeSequenceImpl<From>(std::make_index_sequence<N>{});
}

template <size_t Size1, size_t Size2, size_t Size3, size_t Size4,
          size_t AccumulateSize = Size1 + Size2 + Size3 + Size4>
class ByteFiller {
public:
    explicit ByteFiller(size_t length)
        : m_length(length) {
    }

    void FillData(ui8* data[4]) const {
        for (auto col_i = 0; col_i < 4; ++col_i) {
            ui8* column = data[col_i];
            for (size_t i = 0; i < m_length; ++i) {
                std::memcpy(column + m_sizes[col_i] * i, m_elems[col_i].get(), m_sizes[col_i]);
            }
        }
    }

    void FillResult(ui8* result) const {
        for (size_t i = 0; i < m_length; ++i) {
            std::memcpy(result + AccumulateSize * i, m_accElem.get(), AccumulateSize);
        }
    }

    bool CheckResult(const ui8* result) const {
        for (size_t i = 0; i < m_length; ++i) {
            for (size_t byte_n = 0; byte_n < AccumulateSize; ++byte_n) {
                if (result[i * AccumulateSize + byte_n] != byte_n) {
                    return false;
                }
            }
        }
        return true;
    }

private:
    size_t m_length;
    std::unique_ptr<ui8[]> m_elems[4] = {
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
    std::unique_ptr<ui8[]> m_accElem = MakeSequence<AccumulateSize, 0>();
};

Y_UNIT_TEST_SUITE(CheckFiller) {
    Y_UNIT_TEST(CheckResultTest) {
        ui8 expected_result[30] = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        };
        ByteFiller<1, 2, 3, 4> bf{3};

        UNIT_ASSERT_EQUAL(bf.CheckResult(expected_result), true);
    }

    Y_UNIT_TEST(FillResultTest) {
        ui8 result[30]{};
        ByteFiller<1, 2, 3, 4> bf{3};
        bf.FillResult(result);

        UNIT_ASSERT_EQUAL(bf.CheckResult(result), true);
    }

    Y_UNIT_TEST(FillDataTest) {
        ByteFiller<1, 2, 3, 4> bf{3};

        ui8 result[30]{};
        bf.FillResult(result);

        ui8 d1[3]{};
        ui8 d2[6]{};
        ui8 d3[9]{};
        ui8 d4[12]{};
        ui8* data[4] = {
            d1, d2, d3, d4
        };

        bf.FillData(data);

        UNIT_ASSERT_EQUAL(!memcmp(d1, (ui8[3]){0, 0, 0}, 3), true);
        UNIT_ASSERT_EQUAL(!memcmp(d2, (ui8[6]){1, 2, 1, 2, 1, 2}, 6), true);
        UNIT_ASSERT_EQUAL(!memcmp(d3, (ui8[9]){3, 4, 5, 3, 4, 5, 3, 4, 5}, 9), true);
        UNIT_ASSERT_EQUAL(!memcmp(d4, (ui8[12]){6, 7, 8, 9, 6, 7, 8, 9, 6, 7, 8, 9}, 12), true);
    }
}
