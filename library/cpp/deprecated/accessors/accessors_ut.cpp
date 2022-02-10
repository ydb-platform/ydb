#include "accessors.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <array>

class TAccessorsTest: public TTestBase {
    UNIT_TEST_SUITE(TAccessorsTest);
    UNIT_TEST(TestAccessors);
    UNIT_TEST_SUITE_END();

private:
    template <typename T>
    void TestRead(const T& t, const char* comm) {
        const char* beg = (const char*)NAccessors::Begin(t);
        const char* end = (const char*)NAccessors::End(t);
        long sz = NAccessors::Size(t) * sizeof(typename TMemoryTraits<T>::TElementType);

        UNIT_ASSERT_VALUES_EQUAL_C(end - beg, sz, comm);
    }

    template <typename T>
    void TestWrite(const char* comm) {
        typename TMemoryTraits<T>::TElementType val[4] = {'t', 'e', 's', 't'};
        T t;
        NAccessors::Init(t);
        NAccessors::Reserve(t, 6);

        size_t sz = NAccessors::Size(t);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, sz, comm);

        NAccessors::Append(t, 'a');
        sz = NAccessors::Size(t);
        UNIT_ASSERT_VALUES_EQUAL_C(1u, sz, comm);

        NAccessors::Append(t, val, val + 4);
        sz = NAccessors::Size(t);
        UNIT_ASSERT_VALUES_EQUAL_C(5u, sz, comm);

        NAccessors::Clear(t);

        sz = NAccessors::Size(t);
        UNIT_ASSERT_VALUES_EQUAL_C(0u, sz, comm);
    }

    void TestAccessors() {
        TestRead('a', "char");
        TestRead(1, "int");

        int t[4] = {0, 1, 2, 3};

        TestRead(t, "int[4]");

        TStringBuf sbuf = "test";

        TestRead(sbuf, "TStringBuf");

        TUtf16String wtr;
        wtr.resize(10, 1024);

        TestRead(wtr, "TUtf16String");

        TBuffer buf;
        buf.Resize(30);

        TestRead(buf, "TBuffer");

        TVector<ui64> vec(10, 100);

        TestRead(vec, "TVector<ui64>");

        TestWrite<TString>("TString");
        TestWrite<TVector<char>>("TVector<char>");
        TestWrite<TBuffer>("TBuffer");
        TestWrite<TVector<ui64>>("TVector<ui64>");
        TestWrite<TUtf16String>("TUtf16String");

        std::array<TString, 10> sarr;
        NAccessors::Init(sarr);
        NAccessors::Clear(sarr);

        std::array<char, 10> carr;
        NAccessors::Init(carr);
        NAccessors::Clear(carr);
        TestRead(carr, "std::array<char, 10>");
    }
};

UNIT_TEST_SUITE_REGISTRATION(TAccessorsTest)
