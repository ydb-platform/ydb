#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>

#define PY_CHECKER(Name, PyType, AsType, Type) \
    struct TPy##Name##Checker { \
        void operator()(PyObject* pyVal, Type expected) { \
            UNIT_ASSERT(Py##PyType##_Check(pyVal)); \
            Type val = Py##PyType##_As##AsType(pyVal); \
            UNIT_ASSERT(val != static_cast<Type>(-1) || !PyErr_Occurred()); \
            UNIT_ASSERT_EQUAL(val, expected); \
        } \
    };

#if PY_MAJOR_VERSION >= 3
PY_CHECKER(Long, Long, Long, long)
#else
PY_CHECKER(Int, Int, Long, long)
#endif

#ifdef HAVE_LONG_LONG
PY_CHECKER(LLong, Long, LongLong, long long)
PY_CHECKER(Ulong, Long, UnsignedLongLong, unsigned long long)
#else
PY_CHECKER(LLong, Long, Long, long)
PY_CHECKER(Ulong, Long, UnsignedLong, unsigned long)
#endif

PY_CHECKER(Float, Float, Double, long)

#undef PY_CHECKER

using namespace NPython;

Y_UNIT_TEST_SUITE(TPyNumberTest) {
    template <typename T, typename TPyChecker>
    void TestCastsInRange(T begin, T end) {
        for (T i = begin; i < end; i++) {
            TPyObjectPtr pyVal = PyCast<T>(i);
            UNIT_ASSERT(pyVal.Get() != nullptr);

            TPyChecker c;
            c(pyVal.Get(), i);

            T cppVal = PyCast<T>(pyVal.Get());
            UNIT_ASSERT_EQUAL(cppVal, i);
        }
    }

    template <typename T, typename TPyChecker, int range = 10>
    void TestSignedCasts() {
        TPythonTestEngine engine;
        TestCastsInRange<T, TPyChecker>(Min<T>(), Min<T>() + range);
        TestCastsInRange<T, TPyChecker>(-range, range);
        TestCastsInRange<T, TPyChecker>(Max<T>() - range, Max<T>());
    }

    template <typename T, typename TPyDownChecker,
              typename TPyUpChecker = TPyDownChecker, int range = 10>
    void TestUnsignedCasts() {
        TPythonTestEngine engine;
        TestCastsInRange<T, TPyDownChecker>(Min<T>(), Min<T>() + range);
        TestCastsInRange<T, TPyUpChecker>(Max<T>() - range, Max<T>());
    }

    Y_UNIT_TEST(Bool) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EQUAL(PyCast<bool>(Py_True), true);
        UNIT_ASSERT_EQUAL(PyCast<bool>(Py_False), false);

        TPyObjectPtr list = PyList_New(0);
        UNIT_ASSERT_EQUAL(PyCast<bool>(list.Get()), false);
        bool res1;
        UNIT_ASSERT(TryPyCast<bool>(list.Get(), res1));
        UNIT_ASSERT_EQUAL(res1, false);

        PyList_Append(list.Get(), Py_None);
        UNIT_ASSERT_EQUAL(PyCast<bool>(list.Get()), true);
        bool res2;
        UNIT_ASSERT(TryPyCast<bool>(list.Get(), res2));
        UNIT_ASSERT_EQUAL(res2, true);
    }

    Y_UNIT_TEST(Float) {
        TestSignedCasts<float, TPyFloatChecker>();
    }

    Y_UNIT_TEST(Double) {
        TestUnsignedCasts<double, TPyFloatChecker>();
    }

    Y_UNIT_TEST(I64) {
        TestSignedCasts<i64, TPyLLongChecker>();
    }

    Y_UNIT_TEST(Ui64) {
        TestUnsignedCasts<ui64, TPyUlongChecker>();
    }

#if PY_MAJOR_VERSION >= 3
    Y_UNIT_TEST(I8) {
        TestSignedCasts<i8, TPyLongChecker>();
    }

    Y_UNIT_TEST(Ui8) {
        TestUnsignedCasts<ui8, TPyLongChecker>();
    }

    Y_UNIT_TEST(I16) {
        TestSignedCasts<i16, TPyLongChecker>();
    }

    Y_UNIT_TEST(Ui16) {
        TestUnsignedCasts<ui16, TPyLongChecker>();
    }

    Y_UNIT_TEST(I32) {
        TestSignedCasts<i32, TPyLongChecker>();
    }

    Y_UNIT_TEST(Ui32) {
        TestUnsignedCasts<ui32, TPyLongChecker>();
    }
    Y_UNIT_TEST(ImplicitIntCasts) {
        TPythonTestEngine engine;
        const ui64 longMask = sizeof(long) == 4 ? Max<ui32>() : Max<ui64>();
        i64 expected = longMask & (static_cast<i64>(Max<ui32>()) + 10);
        TPyObjectPtr pyInt = PyLong_FromLong(expected);

        { // signed
            i64 actual = PyCast<i64>(pyInt.Get());
            UNIT_ASSERT_EQUAL(actual, expected);

            bool isOk = TryPyCast<i64>(pyInt.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, expected);
        }

        { // unsigned
            ui64 actual = PyCast<ui64>(pyInt.Get());
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));

            bool isOk = TryPyCast<ui64>(pyInt.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));
        }

        { // to float
            float f = PyCast<float>(pyInt.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);

            bool isOk = TryPyCast<float>(pyInt.Get(), f);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);
        }

        { // to double
            double d = PyCast<double>(pyInt.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);

            bool isOk = TryPyCast<double>(pyInt.Get(), d);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);
        }

        // expected overflow
        i32 tmp;
        UNIT_ASSERT(!TryPyCast<i32>(pyInt.Get(), tmp));
        ui32 tmpu;
        UNIT_ASSERT(!TryPyCast<ui32>(pyInt.Get(), tmpu));
    }

#else
    Y_UNIT_TEST(I8) {
        TestSignedCasts<i8, TPyIntChecker>();
    }

    Y_UNIT_TEST(Ui8) {
        TestUnsignedCasts<ui8, TPyIntChecker>();
    }

    Y_UNIT_TEST(I16) {
        TestSignedCasts<i16, TPyIntChecker>();
    }

    Y_UNIT_TEST(Ui16) {
        TestUnsignedCasts<ui16, TPyIntChecker>();
    }

    Y_UNIT_TEST(I32) {
        TestSignedCasts<i32, TPyIntChecker>();
    }

    Y_UNIT_TEST(Ui32) {
        if (sizeof(long) == 4) {
            TestUnsignedCasts<ui32, TPyIntChecker, TPyLLongChecker>();
        } else {
            TestUnsignedCasts<ui32, TPyIntChecker>();
        }
    }

    Y_UNIT_TEST(ImplicitIntCasts) {
        TPythonTestEngine engine;
        const ui64 longMask = sizeof(long) == 4 ? Max<ui32>() : Max<ui64>();
        i64 expected = longMask & (static_cast<i64>(Max<ui32>()) + 10);
        TPyObjectPtr pyInt = PyInt_FromLong(expected);

        { // signed
            i64 actual = PyCast<i64>(pyInt.Get());
            UNIT_ASSERT_EQUAL(actual, expected);

            bool isOk = TryPyCast<i64>(pyInt.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, expected);
        }

        { // unsigned
            ui64 actual = PyCast<ui64>(pyInt.Get());
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));

            bool isOk = TryPyCast<ui64>(pyInt.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));
        }

        { // to float
            float f = PyCast<float>(pyInt.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);

            bool isOk = TryPyCast<float>(pyInt.Get(), f);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);
        }

        { // to double
            double d = PyCast<double>(pyInt.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);

            bool isOk = TryPyCast<double>(pyInt.Get(), d);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);
        }

        // expected overflow
        i32 tmp;
        UNIT_ASSERT(!TryPyCast<i32>(pyInt.Get(), tmp));
        ui32 tmpu;
        UNIT_ASSERT(!TryPyCast<ui32>(pyInt.Get(), tmpu));
    }
#endif


    Y_UNIT_TEST(ImplicitLongCasts) {
        TPythonTestEngine engine;
        i64 expected = static_cast<i64>(Max<ui32>()) + 10;
        TPyObjectPtr pyLong;
        #ifdef HAVE_LONG_LONG
            pyLong = PyLong_FromLongLong(expected);
        #else
            pyLong = PyLong_FromLong(expected)
        #endif

        { // signed
            i64 actual = PyCast<i64>(pyLong.Get());
            UNIT_ASSERT_EQUAL(actual, expected);

            bool isOk = TryPyCast<i64>(pyLong.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, expected);
        }

        { // unsigned
            ui64 actual = PyCast<ui64>(pyLong.Get());
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));

            bool isOk = TryPyCast<ui64>(pyLong.Get(), actual);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_EQUAL(actual, static_cast<ui64>(expected));
        }

        { // to float
            float f = PyCast<float>(pyLong.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);

            bool isOk = TryPyCast<float>(pyLong.Get(), f);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);
        }

        { // to double
            double d = PyCast<double>(pyLong.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);

            bool isOk = TryPyCast<double>(pyLong.Get(), d);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);
        }

        // expected overflow
        i8 tmp;
        UNIT_ASSERT(!TryPyCast<i8>(pyLong.Get(), tmp));
    }

    Y_UNIT_TEST(HugeLongOverflow) {
        TPythonTestEngine engine;
        TPyObjectPtr pyLong = PyLong_FromString((char*)"0xfffffffffffffffff", nullptr, 0);
        TPyObjectPtr bitLength = PyObject_CallMethod(pyLong.Get(), (char*)"bit_length", (char*)"()");
        UNIT_ASSERT_EQUAL(PyCast<ui32>(bitLength.Get()), 68); // 68 bits number

        ui64 resUI64;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resUI64));

        i64 resI64;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resI64));

        ui32 resUI32;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resUI32));

        i32 resI32;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resI32));

        ui16 resUI16;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resUI16));

        i16 resI16;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resI16));

        ui8 resUI8;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resUI8));

        i8 resI8;
        UNIT_ASSERT(!TryPyCast(pyLong.Get(), resI8));
    }

    Y_UNIT_TEST(ImplicitFloatCasts) {
        TPythonTestEngine engine;
        double expected = 3.14159;
        TPyObjectPtr pyFloat = PyFloat_FromDouble(expected);

        { // to float
            float f = PyCast<float>(pyFloat.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);

            bool isOk = TryPyCast<float>(pyFloat.Get(), f);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(f, expected, 0.000001);
        }

        { // to double
            double d = PyCast<double>(pyFloat.Get());
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);

            bool isOk = TryPyCast<double>(pyFloat.Get(), d);
            UNIT_ASSERT(isOk);
            UNIT_ASSERT_DOUBLES_EQUAL(d, expected, 0.000001);
        }
    }

}
