#include "int128_ut_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

#include <array>
#include <type_traits>

#if defined(Y_HAVE_INT128)
bool IsIdentical(const i128 a, const signed __int128 b) {
    const std::array<ui8, 16> arrayA = NInt128Private::GetAsArray(a);
    const std::array<ui8, 16> arrayB = NInt128Private::GetAsArray(b);
    return arrayA == arrayB;
}

Y_UNIT_TEST_SUITE(i128_And_i8_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_i8_Zero) {
        i8 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Minus1) {
        i8 n = -1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Plus1) {
        i8 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Minus42) {
        i8 n = -42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Plus42) {
        i8 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Min) {
        i8 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_Max) {
        i8 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_MinPlus1) {
        i8 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i8_MaxMinus1) {
        i8 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_i16_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_i16_Zero) {
        i16 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Minus1) {
        i16 n = -1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Plus1) {
        i16 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Minus42) {
        i16 n = -42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Plus42) {
        i16 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Min) {
        i16 n = std::numeric_limits<i16>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_Max) {
        i16 n = std::numeric_limits<i16>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_MinPlus1) {
        i16 n = std::numeric_limits<i16>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i16_MaxMinus1) {
        i16 n = std::numeric_limits<i16>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_i32_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_i32_Zero) {
        i32 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Minus1) {
        i32 n = -1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Plus1) {
        i32 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Minus42) {
        i32 n = -42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Plus42) {
        i32 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Min) {
        i32 n = std::numeric_limits<i32>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_Max) {
        i32 n = std::numeric_limits<i32>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_MinPlus1) {
        i32 n = std::numeric_limits<i32>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i32_MaxMinus1) {
        i32 n = std::numeric_limits<i32>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_i64_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_i64_Zero) {
        i64 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Minus1) {
        i64 n = -1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Plus1) {
        i64 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Minus42) {
        i64 n = -42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Plus42) {
        i64 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Min) {
        i64 n = std::numeric_limits<i64>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_Max) {
        i64 n = std::numeric_limits<i64>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_MinPlus1) {
        i64 n = std::numeric_limits<i64>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_i64_MaxMinus1) {
        i64 n = std::numeric_limits<i64>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_signed_int128_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_signed_int128_Zero) {
        signed __int128 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Minus1) {
        signed __int128 n = -1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Plus1) {
        signed __int128 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Minus42) {
        signed __int128 n = -42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Plus42) {
        signed __int128 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Min) {
        signed __int128 n = std::numeric_limits<signed __int128>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_Max) {
        signed __int128 n = std::numeric_limits<signed __int128>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_MinPlus1) {
        signed __int128 n = std::numeric_limits<signed __int128>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_signed_int128_MaxMinus1) {
        signed __int128 n = std::numeric_limits<signed __int128>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_ui8_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_ui8_Zero) {
        ui8 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_Plus1) {
        ui8 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_Plus42) {
        ui8 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_Min) {
        ui8 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_Max) {
        ui8 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_MinPlus1) {
        ui8 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui8_MaxMinus1) {
        ui8 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_ui16_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_ui16_Zero) {
        ui16 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_Plus1) {
        ui16 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_Plus42) {
        ui16 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_Min) {
        ui16 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_Max) {
        ui16 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_MinPlus1) {
        ui16 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui16_MaxMinus1) {
        ui16 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_ui32_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_ui32_Zero) {
        ui32 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_Plus1) {
        ui32 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_Plus42) {
        ui32 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_Min) {
        ui32 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_Max) {
        ui32 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_MinPlus1) {
        ui32 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui32_MaxMinus1) {
        ui32 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_ui64_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_ui64_Zero) {
        ui64 n = 0;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_Plus1) {
        ui64 n = 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_Plus42) {
        ui64 n = 42;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_Min) {
        ui64 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_Max) {
        ui64 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_MinPlus1) {
        ui64 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_ui64_MaxMinus1) {
        ui64 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2{n};
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}

Y_UNIT_TEST_SUITE(i128_And_unsigned_int128_BitwiseIdentity) {
    Y_UNIT_TEST(i128_from_unsigned_int128_Zero) {
        unsigned __int128 n = 0;
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_Plus1) {
        unsigned __int128 n = 1;
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_Plus42) {
        unsigned __int128 n = 42;
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_Min) {
        unsigned __int128 n = std::numeric_limits<i8>::min();
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_Max) {
        unsigned __int128 n = std::numeric_limits<i8>::max();
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_MinPlus1) {
        unsigned __int128 n = std::numeric_limits<i8>::min() + 1;
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }

    Y_UNIT_TEST(i128_from_unsigned_int128_MaxMinus1) {
        unsigned __int128 n = std::numeric_limits<i8>::max() - 1;
        i128 t1{n};
        signed __int128 t2 = static_cast<signed __int128>(n);
        UNIT_ASSERT(IsIdentical(t1, t2));
    }
}
#endif
