#include "punycode.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/charset/wide.h>

namespace {
        template<typename T1, typename T2>
        inline bool HasSameBuffer(const T1& s1, const T2& s2) {
                return s1.begin() == s2.begin();
        }
}

Y_UNIT_TEST_SUITE(TPunycodeTest) {
    static bool TestRaw(const TString& utf8, const TString& punycode) {
        TUtf16String unicode = UTF8ToWide(utf8);
        TString buf1;
        TUtf16String buf2;
        return HasSameBuffer(WideToPunycode(unicode, buf1), buf1) && buf1 == punycode && HasSameBuffer(PunycodeToWide(punycode, buf2), buf2) && buf2 == unicode && WideToPunycode(unicode) == punycode && PunycodeToWide(punycode) == unicode;
    }

    Y_UNIT_TEST(RawEncodeDecode) {
        UNIT_ASSERT(TestRaw("", ""));
        UNIT_ASSERT(TestRaw(" ", " -"));
        UNIT_ASSERT(TestRaw("-", "--"));
        UNIT_ASSERT(TestRaw("!@#$%", "!@#$%-"));
        UNIT_ASSERT(TestRaw("xn-", "xn--"));
        UNIT_ASSERT(TestRaw("xn--", "xn---"));
        UNIT_ASSERT(TestRaw("abc", "abc-"));
        UNIT_ASSERT(TestRaw("Latin123", "Latin123-"));

        UNIT_ASSERT(TestRaw("München", "Mnchen-3ya"));
        UNIT_ASSERT(TestRaw("bücher", "bcher-kva"));
        UNIT_ASSERT(TestRaw("BüüchEr", "BchEr-kvaa"));

        UNIT_ASSERT(TestRaw("президент", "d1abbgf6aiiy"));
        UNIT_ASSERT(TestRaw("Президент", "r0a6bcbig1bsy"));
        UNIT_ASSERT(TestRaw("ПРЕЗИДЕНТ", "g0abbgf6aiiy"));
        UNIT_ASSERT(TestRaw("рф", "p1ai"));
        UNIT_ASSERT(TestRaw("пример", "e1afmkfd"));

        {
            const wchar16 tmp[] = {0x82, 0x81, 0x80, 0};
            UNIT_ASSERT(PunycodeToWide("abc") == tmp); // "abc" is still valid punycode
        }

        UNIT_ASSERT_EXCEPTION(PunycodeToWide("     "), TPunycodeError);
        UNIT_ASSERT_EXCEPTION(PunycodeToWide("абвгд"), TPunycodeError);
        UNIT_ASSERT_EXCEPTION(PunycodeToWide("-"), TPunycodeError);

        {
            TString longIn;
            for (size_t i = 0; i < 1024; ++i)
                longIn += "Qй";

            TString longOut = "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ-lo11fbabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            UNIT_ASSERT(TestRaw(longIn, longOut));
        }
    }

    static bool TestHostName(const TString& utf8, const TString& punycode, bool canBePunycode = false) {
        TUtf16String unicode = UTF8ToWide(utf8);
        TString buf1;
        TUtf16String buf2;
        //Cerr << "Testing " << utf8 << Endl;
        return HostNameToPunycode(unicode) == punycode && HostNameToPunycode(UTF8ToWide(punycode)) == punycode // repeated encoding should give same result
               && PunycodeToHostName(punycode) == unicode && CanBePunycodeHostName(punycode) == canBePunycode;
    }

    static bool TestForced(const TString& bad) {
        return ForceHostNameToPunycode(UTF8ToWide(bad)) == bad && ForcePunycodeToHostName(bad) == UTF8ToWide(bad);
    }

    Y_UNIT_TEST(HostNameEncodeDecode) {
        UNIT_ASSERT(TestHostName("президент.рф", "xn--d1abbgf6aiiy.xn--p1ai", true));
        UNIT_ASSERT(TestHostName("яндекс.ru", "xn--d1acpjx3f.ru", true));
        UNIT_ASSERT(TestHostName("пример", "xn--e1afmkfd", true));
        UNIT_ASSERT(TestHostName("ascii.test", "ascii.test"));

        UNIT_ASSERT(TestHostName("", ""));
        UNIT_ASSERT(TestHostName(".", "."));
        UNIT_ASSERT(TestHostName("a.", "a.")); // empty root domain is ok
        UNIT_ASSERT(TestHostName("a.b.c.д.e.f", "a.b.c.xn--d1a.e.f", true));
        UNIT_ASSERT(TestHostName("а.б.в.г.д", "xn--80a.xn--90a.xn--b1a.xn--c1a.xn--d1a", true));

        UNIT_ASSERT(TestHostName("-", "-"));
        UNIT_ASSERT(TestHostName("xn--", "xn--", true));
        UNIT_ASSERT(TestHostName("xn--aaa.-", "xn--aaa.-", true));
        UNIT_ASSERT(TestHostName("xn--xn--d1acpjx3f.xn--ru", "xn--xn--d1acpjx3f.xn--ru", true));

        {
            // non-ascii
            TString bad = "президент.рф";
            UNIT_ASSERT_EXCEPTION(PunycodeToHostName("президент.рф"), TPunycodeError);
            UNIT_ASSERT(ForcePunycodeToHostName(bad) == UTF8ToWide(bad));
        }
        {
            // too long domain label
            TString bad(500, 'a');
            UNIT_ASSERT_EXCEPTION(HostNameToPunycode(UTF8ToWide(bad)), TPunycodeError);
            UNIT_ASSERT(TestForced(bad)); // but can decode it
        }
        {
            // already has ACE prefix
            TString bad("xn--яндекс.xn--рф");
            UNIT_ASSERT_EXCEPTION(HostNameToPunycode(UTF8ToWide(bad)), TPunycodeError);
            UNIT_ASSERT(TestForced(bad));
        }
        {
            // empty non-root domain is not allowed (?)
            TString bad(".яндекс.рф");
            UNIT_ASSERT_EXCEPTION(HostNameToPunycode(UTF8ToWide(bad)), TPunycodeError);
            UNIT_ASSERT(TestForced(bad));
        }

        UNIT_ASSERT(CanBePunycodeHostName("xn--"));
        UNIT_ASSERT(CanBePunycodeHostName("yandex.xn--p1ai"));
        UNIT_ASSERT(CanBePunycodeHostName("xn--d1acpjx3f.xn--p1ai"));
        UNIT_ASSERT(CanBePunycodeHostName("a.b.c.d.xn--e"));
        UNIT_ASSERT(CanBePunycodeHostName("xn--a.b.c.xn--d.e"));
        UNIT_ASSERT(!CanBePunycodeHostName("yandex.ru"));       // no xn--
        UNIT_ASSERT(!CanBePunycodeHostName("яндекс.рф"));       // non-ascii
        UNIT_ASSERT(!CanBePunycodeHostName("яндекс.xn--p1ai")); // non-ascii
        UNIT_ASSERT(!CanBePunycodeHostName(""));
        UNIT_ASSERT(!CanBePunycodeHostName("http://xn--a.b")); // scheme prefix is not detected here
    }
}
