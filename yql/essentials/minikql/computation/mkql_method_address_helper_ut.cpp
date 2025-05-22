#include <yql/essentials/minikql/computation/mkql_method_address_helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/udf/udf_value.h>

#if SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN
using namespace NYql;
namespace {
// Test class with methods to test method pointers
class TTestClass {
public:
    int MethodWithUnboxedValuePod(NUdf::TUnboxedValuePod val) {
        Y_UNUSED(val);
        CallCount_++;
        return 123;
    }

    NUdf::TUnboxedValuePod& ConstMethodWithUnboxedValuePod(NUdf::TUnboxedValuePod val) const {
        Y_UNUSED(val);
        CallCount_++;
        return UnboxedValuePod_;
    }

    size_t CallCount() const {
        return CallCount_;
    }

private:
    mutable size_t CallCount_ = 0;
    mutable NUdf::TUnboxedValuePod UnboxedValuePod_;
};

NUdf::TUnboxedValuePod FunctionWithUnboxedValuePod(NUdf::TUnboxedValuePod val, NUdf::TUnboxedValuePod& val2, int a, const int& b, int* c, int&& d) {
    Y_UNUSED(val2, a, b, c, d);
    return val;
}
} // namespace

Y_UNIT_TEST_SUITE(TestMethodConvertion) {

Y_UNIT_TEST(TestFreeFunction) {
    __int128_t (*actualMethod)(__int128_t, NUdf::TUnboxedValuePod&, int a, const int& b, int* c, int&& d) = DoGetFreeFunctionPtr<&FunctionWithUnboxedValuePod>();
    Y_UNUSED(actualMethod);
    auto address = GetMethodPtr<&FunctionWithUnboxedValuePod>();
    NUdf::TUnboxedValuePod a;
    UNIT_ASSERT_EQUAL(reinterpret_cast<decltype(actualMethod)>(address)(13, a, 1, 2, nullptr, 3), 13);
}

Y_UNIT_TEST(TestConstMethod) {
    TTestClass testClass;
    NUdf::TUnboxedValuePod& (*actualMethod)(TTestClass*, __int128_t) = DoGetMethodPtr<&TTestClass::ConstMethodWithUnboxedValuePod>();
    Y_UNUSED(actualMethod(&testClass, 123));
    UNIT_ASSERT_EQUAL(testClass.CallCount(), 1);
}

Y_UNIT_TEST(TestNonConstMethod) {
    TTestClass testClass;
    int (*actualMethod)(TTestClass*, __int128_t) = DoGetMethodPtr<&TTestClass::MethodWithUnboxedValuePod>();
    Y_UNUSED(actualMethod);
    actualMethod(&testClass, 123);
    UNIT_ASSERT_EQUAL(testClass.CallCount(), 1);
    auto address = GetMethodPtr<&TTestClass::MethodWithUnboxedValuePod>();
    UNIT_ASSERT(reinterpret_cast<decltype(actualMethod)>(address)(&testClass, 123));
    UNIT_ASSERT_EQUAL(testClass.CallCount(), 2);
}

} // Y_UNIT_TEST_SUITE(TestMethodConvertion)

#endif // SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN
