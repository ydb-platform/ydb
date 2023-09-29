#include "path.h"
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <locale>

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

    Y_UNIT_TEST(Name_EnglishAlphabet) {
        const TString pathPart = "NameInEnglish";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.end());
    }

    Y_UNIT_TEST(Name_RussianAlphabet) {
        const TString pathPart = "НазваниеНаРусском";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
    }

    class TLocaleGuard {
    public:
        explicit TLocaleGuard(const std::locale& targetLocale)
            : OriginalLocale_(std::locale::global(targetLocale))
        {
        }
        ~TLocaleGuard() {
            std::locale::global(OriginalLocale_);
        }
    
    private:
        const std::locale OriginalLocale_;
    };

    Y_UNIT_TEST(Name_RussianAlphabet_SetLocale_C) {
        TLocaleGuard localeGuard(std::locale("C"));
        const TString pathPart = "НазваниеНаРусском";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
    }

    Y_UNIT_TEST(Name_RussianAlphabet_SetLocale_C_UTF8) {
        try {
            TLocaleGuard localeGuard(std::locale("C.UTF-8"));
            const TString pathPart = "НазваниеНаРусском";
            UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
        } catch (std::runtime_error) {
            // basic utf-8 locale is absent in the system, abort the test
        }
    }

    Y_UNIT_TEST(Name_AllSymbols) {
        const auto isAllowed = [](char symbol) {
            constexpr std::array<char, 3> allowedSymbols = {'-', '_', '.'};
            return std::isalnum(symbol, std::locale::classic())
                || std::find(allowedSymbols.begin(), allowedSymbols.end(), symbol) != allowedSymbols.end();
        };

        for (char symbol = std::numeric_limits<char>::min(); ; ++symbol) {
            const TString pathPart(1, symbol);
            UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), isAllowed(symbol) ? pathPart.end() : pathPart.begin());
            
            if (symbol == std::numeric_limits<char>::max()) {
                break;
            }
        }
    }

    // This ctype facet classifies 'z' letter as not alphabetic.
    // Code is taken from https://en.cppreference.com/w/cpp/locale/ctype_char.
    struct TWeirdCtypeFacet : std::ctype<char>
    {
        static const mask* MakeTable()
        {
            // make a copy of the "C" locale table
            static std::vector<mask> weirdTable(classic_table(), classic_table() + table_size);

            // reclassify 'z'
            weirdTable['z'] &= ~alpha;
            return &weirdTable[0];
        }
    
        TWeirdCtypeFacet(std::size_t refs = 0) : ctype(MakeTable(), false, refs) {}
    };

    Y_UNIT_TEST(Name_WeirdLocale_RegularName) {
        TLocaleGuard localeGuard(std::locale(std::locale::classic(), new TWeirdCtypeFacet));
        const TString regularName = "a";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(regularName), regularName.end());

    }

    Y_UNIT_TEST(Name_WeirdLocale_WeirdName) {
        TLocaleGuard localeGuard(std::locale(std::locale::classic(), new TWeirdCtypeFacet));
        UNIT_ASSERT(!std::isalnum('z', std::locale()));

        const TString weirdName = "z";
        // path part should not be considered to be broken, we should ignore the global locale
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(weirdName), weirdName.end());
    }

    Y_UNIT_TEST(Name_ExtraSymbols) {
        const TString pathPart = "this string contains whitespaces";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), std::find(pathPart.begin(), pathPart.end(), ' '));
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart, " "), pathPart.end());
    }
}

}
