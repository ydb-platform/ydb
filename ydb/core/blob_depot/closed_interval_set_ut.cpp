#include "closed_interval_set.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

using T = TClosedIntervalSet<ui8>;

TString ToString(const T& ivs) {
    TStringStream s("[");
    bool flag = true;
    ivs([&](ui8 first, ui8 last) {
        s << (std::exchange(flag, false) ? "" : " ") << (unsigned)first << "-" << (unsigned)last;
        return true;
    });
    s << "]";
    return s.Str();
}

ui64 Convert(const T& ivs) {
    ui64 res = 0;
    ivs([&](ui8 first, ui8 last) {
        const ui64 mask = (ui64(1) << last + 1) - (ui64(1) << first);
        UNIT_ASSERT_VALUES_EQUAL_C(res & mask, 0, ToString(ivs));
        res |= mask;
        return true;
    });
    return res;
}

T Make(ui64 mask) {
    const ui64 original = mask;
    unsigned pos = 0;
    T res;
    while (mask) {
        const ui64 bit = mask & 1;
        const unsigned n = CountTrailingZeroBits(mask ^ -bit);
        if (bit) {
            res |= {pos, pos + n - 1};
        }
        mask >>= n;
        pos += n;
    }
    UNIT_ASSERT_VALUES_EQUAL(Convert(res), original);
    return res;
}

Y_UNIT_TEST_SUITE(ClosedIntervalSet) {

    Y_UNIT_TEST(Union) {
        for (ui32 i = 0; i < 4096; ++i) {
            for (ui32 begin = 0; begin <= 12; ++begin) {
                for (ui32 end = begin; end <= 12; ++end) {
                    T x = Make(i);
                    x |= {begin, end};
                    UNIT_ASSERT_VALUES_EQUAL(Convert(x), i | ((1 << end + 1) - (1 << begin)));
                }
            }
        }
    }

    Y_UNIT_TEST(Difference) {
        for (ui32 i = 0; i < 1024; ++i) {
            for (ui32 j = 0; j < 1024; ++j) {
                T x = Make(i);
                T y = Make(j);
                ui64 expected = i & ~j;

                ui64 xLeft = 0, xRight = 0;
                x([&](ui8 left, ui8 right) {
                    xLeft |= ui64(1) << left;
                    xRight |= ui64(1) << right;
                    return true;
                });

                y([&](ui8 left, ui8 right) {
                    expected |= i & ~xLeft & ui64(1) << left;
                    expected |= i & ~xRight & ui64(1) << right;
                    return true;
                });

                x -= y;
                UNIT_ASSERT_VALUES_EQUAL(Convert(x), expected);

                std::optional<std::pair<ui8, ui8>> firstRange;
                x([&](ui8 first, ui8 last) {
                    UNIT_ASSERT(!firstRange);
                    firstRange = {first, last};
                    return false;
                });

                const auto interval = Make(i).PartialSubtract(y);
                UNIT_ASSERT_EQUAL(interval, firstRange);
            }
        }
    }

    Y_UNIT_TEST(Contains) {
        for (ui32 i = 0; i < 4096; ++i) {
            T x = Make(i);
            for (ui32 j = 0; j < 12; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(x[j], (i >> j) & 1);
            }
        }
    }

    Y_UNIT_TEST(EnumInRange) {
        for (ui32 i = 0; i < 4096; ++i) {
            T x = Make(i);
            for (ui32 j = 0; j <= 12; ++j) {
                for (ui32 k = j; k <= 12; ++k) {
                    ui8 expectedLeft = j;
                    ui32 res = 0;
                    x.EnumInRange(j, k, false, [&](ui8 left, ui8 right, bool inside) {
                        UNIT_ASSERT_VALUES_EQUAL(left, expectedLeft);
                        UNIT_ASSERT(left <= right);
                        expectedLeft = right;
                        if (inside) {
                            const ui32 mask = (1 << right + 1) - (1 << left);
                            res |= mask;
                        }
                        return true;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(expectedLeft, k);
                    UNIT_ASSERT_VALUES_EQUAL(res, i & ((1 << k + 1) - (1 << j)));
                }
            }
        }
    }

    Y_UNIT_TEST(EnumInRangeReverse) {
        for (ui32 i = 0; i < 4096; ++i) {
            T x = Make(i);
            for (ui32 j = 0; j <= 12; ++j) {
                for (ui32 k = j; k <= 12; ++k) {
                    ui8 expectedRight = k;
                    ui32 res = 0;
                    x.EnumInRange(j, k, true, [&](ui8 left, ui8 right, bool inside) {
                        UNIT_ASSERT_VALUES_EQUAL(right, expectedRight);
                        UNIT_ASSERT(left <= right);
                        expectedRight = left;
                        if (inside) {
                            const ui32 mask = (1 << right + 1) - (1 << left);
                            res |= mask;
                        }
                        return true;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(expectedRight, j);
                    UNIT_ASSERT_VALUES_EQUAL(res, i & ((1 << k + 1) - (1 << j)));
                }
            }
        }
    }

}
