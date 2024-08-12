#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NPython;

Y_UNIT_TEST_SUITE(TPyDecimalTest) {
    Y_UNIT_TEST(FromPyZero) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDecimalDataType<12,5>>(
                R"(
from decimal import Decimal
def Test(): return Decimal()
                )",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(!value.GetInt128());
                });
    }

    Y_UNIT_TEST(FromPyPi) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDecimalDataType<28,18>>(
                R"(
from decimal import Decimal
def Test(): return Decimal('3.141592653589793238')
                )",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.GetInt128() == 3141592653589793238LL);
                });
    }

    Y_UNIT_TEST(FromPyTini) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDecimalDataType<35,35>>(
                R"(
from decimal import Decimal
def Test(): return Decimal('-.00000000000000000000000000000000001')
                )",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.GetInt128() == -1);
                });
    }

    Y_UNIT_TEST(FromPyNan) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDecimalDataType<35,34>>(
                R"(
from decimal import Decimal
def Test(): return Decimal('NaN')
                )",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.GetInt128() == NYql::NDecimal::Nan());
                });
    }

    Y_UNIT_TEST(FromPyInf) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TDecimalDataType<35,34>>(
                R"(
from decimal import Decimal
def Test(): return Decimal('-inf')
                )",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.GetInt128() == -NYql::NDecimal::Inf());
                });
    }

    Y_UNIT_TEST(ToPyZero) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDecimalDataType<7,7>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod::Zero();
                },
                "def Test(value): assert value.is_zero()"
        );
    }

    Y_UNIT_TEST(ToPyPi) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDecimalDataType<20,18>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod(NYql::NDecimal::TInt128(3141592653589793238LL));
                },
                "def Test(value): assert str(value) == '3.141592653589793238'"
        );
    }

    Y_UNIT_TEST(ToPyTini) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDecimalDataType<35,35>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod(NYql::NDecimal::TInt128(-1));
                },
                "def Test(value): assert format(value, '.35f') == '-0.00000000000000000000000000000000001'"
        );
    }

    Y_UNIT_TEST(ToPyNan) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDecimalDataType<2,2>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod(NYql::NDecimal::Nan());
                },
                "def Test(value): assert value.is_nan()"
        );
    }

    Y_UNIT_TEST(ToPyInf) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TDecimalDataType<30,0>>(
                [](const TType*, const NUdf::IValueBuilder&) {
                    return NUdf::TUnboxedValuePod(-NYql::NDecimal::Inf());
                },
                "def Test(value): assert value.is_infinite() and value.is_signed()"
        );
    }
}
