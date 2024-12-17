#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NPython;

Y_UNIT_TEST_SUITE(TPyStringTest) {
    template <typename TStringType>
    void TestStringCasts() {
        TStringType testStr1(TStringBuf("test string"));
        TStringBuf strBuf1 = testStr1;
        TPyObjectPtr str1 = PyBytes_FromString(strBuf1.data());
        const auto value = PyCast<TStringType>(str1.Get());

        UNIT_ASSERT_STRINGS_EQUAL(value, testStr1);

        TStringType testStr2(TStringBuf("another test string"));
        TStringBuf strBuf2 = testStr2;
        TPyObjectPtr str2 = PyCast<TStringType>(testStr2);

        Py_ssize_t size = 0U;
        char* buf = nullptr;
        const auto rc = PyBytes_AsStringAndSize(str2.Get(), &buf, &size);
        UNIT_ASSERT(rc >= 0);
        UNIT_ASSERT(buf != nullptr);
        UNIT_ASSERT_EQUAL(static_cast<size_t>(size), strBuf2.size());
        UNIT_ASSERT_STRINGS_EQUAL(buf, testStr2);
    }

    template <typename TStringType>
    void TestBinaryStringCasts() {
        TStringType testStr1(TStringBuf("\xa0\xa1"sv));
        TStringBuf strBuf1 = testStr1;
        TPyObjectPtr str1 = PyBytes_FromString(strBuf1.data());
        const auto value = PyCast<TStringType>(str1.Get());

        UNIT_ASSERT_STRINGS_EQUAL(value, testStr1);

        TStringType testStr2(TStringBuf("\xf0\x90\x28\xbc"sv));
        TStringBuf strBuf2 = testStr2;
        TPyObjectPtr str2 = PyCast<TStringType>(testStr2);

        Py_ssize_t size = 0U;
        char* buf = nullptr;
        const auto rc = PyBytes_AsStringAndSize(str2.Get(), &buf, &size);
        UNIT_ASSERT(rc >= 0);
        UNIT_ASSERT(buf != nullptr);
        UNIT_ASSERT_EQUAL(static_cast<size_t>(size), strBuf2.size());
        UNIT_ASSERT_STRINGS_EQUAL(buf, testStr2);
    }

    template <typename TStringType>
    void TestUtf8StringCasts() {
        const TStringType testStr1(TStringBuf("тестовая строка"));
        TStringBuf strBuf1 = testStr1;
        const TPyObjectPtr str1 = PyUnicode_FromString(strBuf1.data());
        const TPyObjectPtr utf8 = PyUnicode_AsUTF8String(str1.Get());
        const auto value = PyCast<TStringType>(utf8.Get());
        UNIT_ASSERT_STRINGS_EQUAL(value, testStr1);

        const TStringType testStr2(TStringBuf("еще одна тестовая строка"));
        TStringBuf strBuf2 = testStr2;
        const auto str2 = ToPyUnicode<TStringType>(testStr2);

        UNIT_ASSERT(PyUnicode_Check(str2.Get()));

        Py_ssize_t size = 0U;
#if PY_MAJOR_VERSION >= 3
        const auto buf = PyUnicode_AsUTF8AndSize(str2.Get(), &size);
#else
        char* buf = nullptr;
        const TPyObjectPtr pyUtf8Str = PyUnicode_AsUTF8String(str2.Get());
        const auto rc = PyBytes_AsStringAndSize(pyUtf8Str.Get(), &buf, &size);
        UNIT_ASSERT(rc >= 0);
#endif
        UNIT_ASSERT(buf != nullptr);
        UNIT_ASSERT_EQUAL(static_cast<size_t>(size), strBuf2.size());
        UNIT_ASSERT_STRINGS_EQUAL(buf, testStr2);
    }

    Y_UNIT_TEST(Simple) {
        TestStringCasts<TString>();
        TestStringCasts<TStringBuf>();
        TestStringCasts<NUdf::TStringRef>();
    }

    Y_UNIT_TEST(Utf8) {
        TestUtf8StringCasts<TString>();
        TestUtf8StringCasts<TStringBuf>();
        TestUtf8StringCasts<NUdf::TStringRef>();
    }

    Y_UNIT_TEST(Binary) {
        TestBinaryStringCasts<TString>();
        TestBinaryStringCasts<TStringBuf>();
        TestBinaryStringCasts<NUdf::TStringRef>();
    }
}
