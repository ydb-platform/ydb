#include "fp_bits.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/valgrind.h>

namespace NYql {

namespace {
template <typename T>
void CanonizeFpBitsTest() {
    enum EValues {
        Zero,
        MZero,
        One,
        Half,
        Two,
        PMin,
        PMax,
        DNorm,
        PInf,
        NInf,
        QNan,
        SNan,
        INan,
        Max
    };

    T values[EValues::Max];
    T newValues[EValues::Max];
    int valuesClass[EValues::Max];
    values[Zero] = T(0);
    values[MZero] = -values[Zero];
    values[One] = T(1);
    values[Half] = values[One] / 2;
    values[Two] = values[One] * 2;
    values[PMin] = std::numeric_limits<T>::min();
    values[DNorm] = values[PMin] / 2;
    values[PMax] = std::numeric_limits<T>::max();
    values[PInf] = std::numeric_limits<T>::infinity();
    values[NInf] = -std::numeric_limits<T>::infinity();
    values[QNan] = std::numeric_limits<T>::quiet_NaN();
    values[SNan] = std::numeric_limits<T>::signaling_NaN();
    values[INan] = std::numeric_limits<T>::infinity() / std::numeric_limits<T>::infinity();

    for (int v = Zero; v < Max; ++v) {
        int cls;
        if (v <= MZero) {
            cls = FP_ZERO;
        } else if (v < DNorm) {
            cls = FP_NORMAL;
        } else if (v == DNorm) {
            cls = FP_SUBNORMAL;
        } else if (v < QNan) {
            cls = FP_INFINITE;
        } else {
            cls = FP_NAN;
        }

        valuesClass[v] = cls;
    }

    for (int v = Zero; v < Max; ++v) {
        UNIT_ASSERT_VALUES_EQUAL(std::fpclassify(values[v]), valuesClass[v]);
    }

    for (int v = Zero; v < Max; ++v) {
        newValues[v] = values[v];
        CanonizeFpBits<T>(&newValues[v]);
    }

    for (int v = Zero; v < Max; ++v) {
        UNIT_ASSERT_VALUES_EQUAL(std::fpclassify(newValues[v]), valuesClass[v]);
    }

    for (int v = Zero; v < Max; ++v) {
        int originalV = v;
        if (v == MZero) {
            originalV = Zero;
        } else if (v >= QNan) {
            originalV = QNan;
        }

        UNIT_ASSERT(std::memcmp((const void*)&newValues[v], (const void*)&values[originalV], std::min(size_t(10), sizeof(T))) == 0);
    }
}
}

Y_UNIT_TEST_SUITE(TFpBits) {
    Y_UNIT_TEST(CanonizeFloat) {
        CanonizeFpBitsTest<float>();
    }

    Y_UNIT_TEST(CanonizeDouble) {
        CanonizeFpBitsTest<double>();
    }

    Y_UNIT_TEST(CanonizeLongDouble) {
        if (NValgrind::ValgrindIsOn()) {
            return; // TODO KIKIMR-3431
        }
        CanonizeFpBitsTest<long double>();
    }
}

}
