#include "py_variant.h"
#include "ut3/py_test_engine.h"
#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyTzDateTest) {
    Y_UNIT_TEST(FromDate) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTzDate>(
                "def Test():\n"
                "    return (2, 'Europe/Moscow')\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT_VALUES_EQUAL(value.Get<ui16>(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(value.GetTimezoneId(), NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
                });
    }

    Y_UNIT_TEST(FromDatetime) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTzDatetime>(
            "def Test():\n"
            "    return (2, 'Europe/Moscow')\n",
            [](const NUdf::TUnboxedValuePod& value) {
            UNIT_ASSERT(value);
            UNIT_ASSERT_VALUES_EQUAL(value.Get<ui32>(), 2);
            UNIT_ASSERT_VALUES_EQUAL(value.GetTimezoneId(), NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
        });
    }

    Y_UNIT_TEST(FromTimestamp) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTzTimestamp>(
            "def Test():\n"
            "    return (2, 'Europe/Moscow')\n",
            [](const NUdf::TUnboxedValuePod& value) {
            UNIT_ASSERT(value);
            UNIT_ASSERT_VALUES_EQUAL(value.Get<ui64>(), 2);
            UNIT_ASSERT_VALUES_EQUAL(value.GetTimezoneId(), NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
        });
    }

    Y_UNIT_TEST(ToDate) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TTzDate>(
                [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
                    auto ret = NUdf::TUnboxedValuePod((ui16)2);
                    ret.SetTimezoneId(NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
                    return ret;
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert value == (2, 'Europe/Moscow')\n");
    }

    Y_UNIT_TEST(ToDatetime) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TTzDatetime>(
            [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
            auto ret = NUdf::TUnboxedValuePod((ui32)2);
            ret.SetTimezoneId(NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
            return ret;
        },
            "def Test(value):\n"
            "    assert isinstance(value, tuple)\n"
            "    assert value == (2, 'Europe/Moscow')\n");
    }

    Y_UNIT_TEST(ToTimestamp) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TTzTimestamp>(
            [](const TType* /*type*/, const NUdf::IValueBuilder& /*vb*/) {
            auto ret = NUdf::TUnboxedValuePod((ui64)2);
            ret.SetTimezoneId(NKikimr::NMiniKQL::GetTimezoneId("Europe/Moscow"));
            return ret;
        },
            "def Test(value):\n"
            "    assert isinstance(value, tuple)\n"
            "    assert value == (2, 'Europe/Moscow')\n");
    }
}
