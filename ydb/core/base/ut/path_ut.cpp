#include "path.h"
#include <library/cpp/testing/unittest/registar.h>

//#define UT_PERF_TEST

using namespace NKikimr;

namespace {

const TVector<std::pair<TString, TString>> Data = {
    {"", ""},
    {"/", ""},
    {"//", ""},
    {"///", ""},
    {"////", ""},
    {"F", "/F"},
    {"/F", "/F"},
    {"/F/", "/F"},
    {"/F//o", "/F/o"},
    {"/F//o//o", "/F/o/o"},
    {"/F///o//o/", "/F/o/o"},
    {"/Foo", "/Foo"},
    {"//Foo", "/Foo"},
    {"///Foo", "/Foo"},
    {"////Foo", "/Foo"},
    {"////Foo//b", "/Foo/b"},
    {"Foo",  "/Foo"},
    {"Foo/", "/Foo"},
    {"Foo//", "/Foo"},
    {"Foo///", "/Foo"},
    {"Foo/Bar", "/Foo/Bar"},
    {"Foo//Bar", "/Foo/Bar"},
    {"/Foo//Bar", "/Foo/Bar"},
    {"//Foo//Bar", "/Foo/Bar"},
    {"//Foo//Bar/", "/Foo/Bar"},
    {"//Foo//Bar//", "/Foo/Bar"},
    {"///Foo///Bar///", "/Foo/Bar"},
    {"///Foo//////Bar///", "/Foo/Bar"},
    {"///Foo//////Bar///FooBar", "/Foo/Bar/FooBar"},
    {"/Foo/Bar/FooBar", "/Foo/Bar/FooBar"},
    {"/Foo//Barqwertyuiopasdfghjklzxcvbnm/FooBar123456789123456789", "/Foo/Barqwertyuiopasdfghjklzxcvbnm/FooBar123456789123456789"},
};

TString DoCanonizePathFast(const TString& path) {
    return CanonizePath(path);
}

TString DoCanonizePathOld(const TString& path)
{
    if (!path)
        return TString();

    const auto parts = SplitPath(path);
    return CanonizePath(parts);
}

Y_UNIT_TEST_SUITE(Path) {
    Y_UNIT_TEST(CanonizeOld) {
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathOld(Data[i].first);
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
    }

    Y_UNIT_TEST(CanonizeFast) {
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathFast(Data[i].first);
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
    }

    Y_UNIT_TEST(CanonizedStringIsSame1) {
        const TString in = "/Foo/Bar";
        const TString& result = DoCanonizePathFast(in);
        UNIT_ASSERT_VALUES_EQUAL((void*)in.data(), (void*)result.data());
    }

    Y_UNIT_TEST(CanonizedStringIsSame2) {
        const TString in = "/Foo";
        const TString& result = DoCanonizePathFast(in);
        UNIT_ASSERT_VALUES_EQUAL((void*)in.data(), (void*)result.data());
    }

#ifdef UT_PERF_TEST
    Y_UNIT_TEST(CanonizeOldPerf) {
        int count = 10000000;
        i64 x = 0;
        while (count--)
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathOld(Data[i].first);
            x += result.size();
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
        Cerr << x << Endl;
    }

    Y_UNIT_TEST(CanonizeFastPerf) {
        int count = 10000000;
        i64 x = 0;
        while (count--)
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathFast(Data[i].first);
            x += result.size();
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
        Cerr << x << Endl;
    }
#endif

}

}
