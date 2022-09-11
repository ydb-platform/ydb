#include "uri_ut.h"
#include <library/cpp/charset/recyr.hh>
#include <library/cpp/html/entity/htmlentity.h>
#include <util/system/maxlen.h>

namespace NUri {
    namespace {
        TString AsWin1251(const TString& s) {
            return Recode(CODES_UTF8, CODES_WIN, s);
        }
        TString AsKoi8(const TString& s) {
            return Recode(CODES_UTF8, CODES_KOI8, s);
        }
    }

    Y_UNIT_TEST_SUITE(URLTestRU) {
        Y_UNIT_TEST(test_httpURL2) {
            TUri url;
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("g:h"), TState::ParsedBadScheme);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("http:g"), TState::ParsedBadFormat);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("/../g"), TState::ParsedBadPath);
            const char* const UpCaseUrl = "http://www.TEST.Ru:80/InDex.html";
            UNIT_ASSERT_VALUES_EQUAL(url.Parse(UpCaseUrl), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://www.TEST.Ru/InDex.html");
            UNIT_ASSERT_VALUES_EQUAL(url.Parse(UpCaseUrl, TFeature::FeaturesDefault | TFeature::FeatureToLower), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://www.test.ru/InDex.html");
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagScheme), "http:");
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagScheme | TField::FlagHost), "http://www.test.ru");
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagHost), "www.test.ru");
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagHost | TField::FlagPath), "www.test.ru/InDex.html");
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagQuery), "");
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("http://www.TEST.Ru:90/InDex.html"), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagHostPort | TField::FlagPath), "www.TEST.Ru:90/InDex.html");
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("www.ya.ru/index.html"), TState::ParsedOK);
            UNIT_ASSERT(!url.IsValidAbs());
            UNIT_ASSERT(url.IsNull(TField::FlagHost));
            UNIT_ASSERT(!url.IsNull(TField::FlagPath));
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagPath), "www.ya.ru/index.html");

            UNIT_ASSERT_VALUES_EQUAL(url.Parse(AsWin1251("www.TEST.Ru/ФЕУФ\\'\".html?ФЕУФ\\'\"=ФЕУФ+\\'\"%10")), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), AsWin1251("www.TEST.Ru/ФЕУФ\\'\".html?ФЕУФ\\'\"=ФЕУФ+\\'\"%10"));

            UNIT_ASSERT_VALUES_EQUAL(url.Parse(AsWin1251("www.TEST.Ru/ФЕУФ\\'\".html?ФЕУФ\\'\"=ФЕУФ+\\'\"%10"),
                                               TFeature::FeaturesDefault | TFeature::FeatureEncodeExtendedASCII),
                                     TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(),
                                     AsWin1251("www.TEST.Ru/%D4%C5%D3%D4\\'\".html?%D4%C5%D3%D4\\'\"=%D4%C5%D3%D4+\\'\"%10"));

            UNIT_ASSERT_VALUES_EQUAL(url.Parse(AsWin1251("www.TEST.Ru/ФЕУФ\\'\".html?ФЕУФ\\'\"=ФЕУФ+\\'\"%10"),
                                               TFeature::FeaturesDefault | TFeature::FeatureEncodeForSQL),
                                     TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), AsWin1251("www.TEST.Ru/ФЕУФ%5C%27%22.html?ФЕУФ%5C%27%22=ФЕУФ+%5C%27%22%10"));

            UNIT_ASSERT_VALUES_EQUAL(url.Parse("q/%33%26%13%2f%2b%30%20",
                                               TFeature::FeaturesDefault | TFeature::FeatureDecodeStandard),
                                     TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "q/3%26%13/%2B0%20");

            UNIT_ASSERT_VALUES_EQUAL(url.Parse("http://www.prime-tass.ru/news/0/{656F5BAE-6677-4762-9BED-9E3B77E72055}.uif"),
                                     TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("//server/path"), TState::ParsedOK);
            UNIT_ASSERT_VALUES_EQUAL(url.Parse("//server/path", TFeature::FeaturesRobot), TState::ParsedOK);
        }

        const TString links[] = {
            "viewforum.php?f=1&amp;sid=b4568481b67b1d7683bea78634b2e240", "viewforum.php?f=1&sid=b4568481b67b1d7683bea78634b2e240",
            "./viewtopic.php?p=74&amp;sid=6#p74", "./viewtopic.php?p=74&sid=6#p74",
            "viewtopic.php?p=9313&amp;sid=8#9313", "viewtopic.php?p=9313&sid=8#9313",
            "profile.php?mode=viewprofile&u=-1#drafts&amp;sid=a6e5989cee27adb5996bfff044af04ca", "profile.php?mode=viewprofile&u=-1#drafts&sid=a6e5989cee27adb5996bfff044af04ca",

            "images\nil.jpg", "images%0Ail.jpg",
            "http://caedebaturque.termez.su\r\n/?article=218", "http://caedebaturque.termez.su%0D%0A/?article=218",

            AsKoi8("javascript:window.external.AddFavorite(\'http://www.humor.look.ru/\',\'Злобные Деды Морозы!!!\')"), "javascript:window.external.AddFavorite(\'http://www.humor.look.ru/\',\'%FA%CC%CF%C2%CE%D9%C5%20%E4%C5%C4%D9%20%ED%CF%D2%CF%DA%D9!!!\')",
            "search.php?search_author=%CB%FE%E4%EC%E8%EB%E0+%C3%F3%F1%E5%E2%E0&amp;showresults=posts&amp;sid=8", "search.php?search_author=%CB%FE%E4%EC%E8%EB%E0+%C3%F3%F1%E5%E2%E0&showresults=posts&sid=8",
            AsWin1251("/Search/author/?q=Штрибель Х.В."), "/Search/author/?q=%D8%F2%F0%E8%E1%E5%EB%FC%20%D5.%C2.",
            AsWin1251("javascript:ins(\'ГОРШОК\')"), "javascript:ins(\'%C3%CE%D0%D8%CE%CA\')",
            AsWin1251("?l=я"), "?l=%FF",
            AsWin1251("content.php?id=3392&theme=Цена"), "content.php?id=3392&theme=%D6%E5%ED%E0",
            "/a-mp3/stype-1/?search=А", "/a-mp3/stype-1/?search=%D0%90",
            "/a-mp3/stype-1/?search=Б", "/a-mp3/stype-1/?search=%D0%91",
            "/a-mp3/stype-1/?search=В", "/a-mp3/stype-1/?search=%D0%92",
            "/a-mp3/stype-1/?search=Г", "/a-mp3/stype-1/?search=%D0%93",
            "/a-mp3/stype-1/?search=Д", "/a-mp3/stype-1/?search=%D0%94",
            "/a-mp3/stype-1/?search=Е", "/a-mp3/stype-1/?search=%D0%95",
            "/a-mp3/stype-1/?search=Ж", "/a-mp3/stype-1/?search=%D0%96",
            "/a-mp3/stype-1/?search=З", "/a-mp3/stype-1/?search=%D0%97",
            // %98 is not defined in CP1251 so don't put it here explicitly
            "/a-mp3/stype-1/?search=\xD0\x98", "/a-mp3/stype-1/?search=%D0%98",
            "/a-mp3/stype-1/?search=Й", "/a-mp3/stype-1/?search=%D0%99",
            "/a-mp3/stype-1/?search=К", "/a-mp3/stype-1/?search=%D0%9A",
            "/a-mp3/stype-1/?search=Л", "/a-mp3/stype-1/?search=%D0%9B",
            "/a-mp3/stype-1/?search=М", "/a-mp3/stype-1/?search=%D0%9C",
            "/a-mp3/stype-1/?search=Н", "/a-mp3/stype-1/?search=%D0%9D",
            "/a-mp3/stype-1/?search=О", "/a-mp3/stype-1/?search=%D0%9E",
            "/a-mp3/stype-1/?search=П", "/a-mp3/stype-1/?search=%D0%9F",
            "/a-mp3/stype-1/?search=\xD0", "/a-mp3/stype-1/?search=%D0",
            "/a-mp3/stype-1/?search=С", "/a-mp3/stype-1/?search=%D0%A1",
            "/a-mp3/stype-1/?search=Т", "/a-mp3/stype-1/?search=%D0%A2",
            "/a-mp3/stype-1/?search=У", "/a-mp3/stype-1/?search=%D0%A3",
            "/a-mp3/stype-1/?search=Ф", "/a-mp3/stype-1/?search=%D0%A4",
            "/a-mp3/stype-1/?search=Х", "/a-mp3/stype-1/?search=%D0%A5",
            "/a-mp3/stype-1/?search=Ц", "/a-mp3/stype-1/?search=%D0%A6",
            "/a-mp3/stype-1/?search=Ч", "/a-mp3/stype-1/?search=%D0%A7",
            "/a-mp3/stype-1/?search=Ш", "/a-mp3/stype-1/?search=%D0%A8",
            "/a-mp3/stype-1/?search=Щ", "/a-mp3/stype-1/?search=%D0%A9",
            "/a-mp3/stype-1/?search=Ы", "/a-mp3/stype-1/?search=%D0%AB",
            "/a-mp3/stype-1/?search=Э", "/a-mp3/stype-1/?search=%D0%AD",
            "/a-mp3/stype-1/?search=Ю", "/a-mp3/stype-1/?search=%D0%AE",
            "/a-mp3/stype-1/?search=Я", "/a-mp3/stype-1/?search=%D0%AF",

            "javascript:emoticon(\":&#39;(\")", "javascript:emoticon(\":\'(\")",
            "javascript:emoticon(\'&gt;:o\')", "javascript:emoticon(\'>:o\')",
            "javascript:emoticon(\']:-&gt;\')", "javascript:emoticon(\']:->\')",
            "javascript:emoticon(\':-&#33;\')", "javascript:emoticon(\':-!\')",
            "javascript:emoticon(\'@}-&gt;--\')", "javascript:emoticon(\'@}->--\')",
            "http&#58;//www.is-ufa.ru/price2/price_IS.rar", "http://www.is-ufa.ru/price2/price_IS.rar",
            "&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#105;&#110;&#102;&#111;&#64;&#101;&#116;&#101;&#109;&#46;&#100;&#101;", "mailto:info@etem.de",
            "&quot;http://www.fubix.ru&quot;", "\"http://www.fubix.ru\"",
            AsWin1251("mailto:&#107;&#97;&#109;&#112;&#97;&#64;&#117;&#107;&#114;&#46;&#110;&#101;&#116;?subject=Арабский язык"), "mailto:kampa@ukr.net?subject=%C0%F0%E0%E1%F1%EA%E8%E9%20%FF%E7%FB%EA",
            {}};

        Y_UNIT_TEST(testHtLinkDecode) {
            char decodedlink[URL_MAXLEN + 10];
            for (int i = 0; links[i]; i += 2) {
                UNIT_ASSERT(HtLinkDecode(links[i].c_str(), decodedlink, sizeof(decodedlink)));
                UNIT_ASSERT_VALUES_EQUAL(decodedlink, links[i + 1]);
            }
        }

        Y_UNIT_TEST(testRuIDNA) {
            {
#define DEC "\xD7\xE5\xF0\xE5\xEf\xEE\xE2\xE5\xF6.\xF0\xF4" /* "Череповец.рф" in Windows-1251 */
#define ENC "%D7%E5%F0%E5%EF%EE%E2%E5%F6.%F0%F4"
// punycode corresponds to lowercase
#define PNC "xn--b1afab7bff7cb.xn--p1ai"
                TTest test = {
                    "http://" ENC "/" ENC "?" ENC "#" ENC, TParseFlags(TFeature::FeaturesAll | TFeature::FeatureAllowHostIDN, TFeature::FeatureDecodeExtendedASCII), TState::ParsedOK, "http", "", "", DEC, 80, "/" ENC, ENC, ENC, ""};
                TUri url;
                URL_TEST_ENC(url, test, CODES_WIN);
                UNIT_ASSERT_VALUES_EQUAL(url.GetField(TField::FieldHostAscii), PNC);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(), "http://" DEC "/" ENC "?" ENC "#" ENC);
                UNIT_ASSERT_VALUES_EQUAL(url.PrintS(TField::FlagHostAscii), "http://" PNC "/" ENC "?" ENC "#" ENC);
#undef PNC
#undef DEC
#undef ENC
            }
        }

        // Regression test for SEARCH-11283
        Y_UNIT_TEST(RegressionTest11283) {
            TStringBuf url = "http://xn--n1aaa.пидорасы.com/";

            TUri uri;
            TState::EParsed er = uri.Parse(url, NUri::TParseFlags(NUri::TFeature::FeaturesRobot | NUri::TFeature::FeatureNoRelPath));
            UNIT_ASSERT_VALUES_EQUAL(er, TState::ParsedOK);
            TStringBuf host = uri.GetHost();
            // Should be properly null-terminated
            UNIT_ASSERT_VALUES_EQUAL(host.size(), strlen(host.data()));
        }
    }

}
