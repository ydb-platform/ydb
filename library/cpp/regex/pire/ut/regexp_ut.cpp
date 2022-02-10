#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/regex/pire/regexp.h>
#include <library/cpp/regex/pire/pcre2pire.h>

Y_UNIT_TEST_SUITE(TRegExp) {
    using namespace NRegExp;

    Y_UNIT_TEST(False) {
        UNIT_ASSERT(!TMatcher(TFsm::False()).Match("").Final());
        UNIT_ASSERT(!TMatcher(TFsm::False()).Match(TStringBuf{}).Final());
    }

    Y_UNIT_TEST(Surround) {
        UNIT_ASSERT(TMatcher(TFsm("qw", TFsm::TOptions().SetSurround(true))).Match("aqwb").Final());
        UNIT_ASSERT(!TMatcher(TFsm("qw", TFsm::TOptions().SetSurround(false))).Match("aqwb").Final());
    }

    Y_UNIT_TEST(Boundaries) {
        UNIT_ASSERT(!TMatcher(TFsm("qwb$", TFsm::TOptions().SetSurround(true))).Match("aqwb").Final());
        UNIT_ASSERT(!TMatcher(TFsm("^aqw", TFsm::TOptions().SetSurround(true))).Match("aqwb").Final());
        UNIT_ASSERT(TMatcher(TFsm("qwb$", TFsm::TOptions().SetSurround(true))).Match(TStringBuf("aqwb"), true, true).Final());
        UNIT_ASSERT(TMatcher(TFsm("^aqw", TFsm::TOptions().SetSurround(true))).Match(TStringBuf("aqwb"), true, true).Final());
        UNIT_ASSERT(!TMatcher(TFsm("qw$", TFsm::TOptions().SetSurround(true))).Match(TStringBuf("aqwb"), true, true).Final());
        UNIT_ASSERT(!TMatcher(TFsm("^qw", TFsm::TOptions().SetSurround(true))).Match(TStringBuf("aqwb"), true, true).Final());

        UNIT_ASSERT(TMatcher(TFsm("^aqwb$", TFsm::TOptions().SetSurround(true)))
                        .Match(TStringBuf("a"), true, false)
                        .Match(TStringBuf("q"), false, false)
                        .Match(TStringBuf("w"), false, false)
                        .Match(TStringBuf("b"), false, true)
                        .Final());
    }

    Y_UNIT_TEST(Case) {
        UNIT_ASSERT(TMatcher(TFsm("qw", TFsm::TOptions().SetCaseInsensitive(true))).Match("Qw").Final());
        UNIT_ASSERT(!TMatcher(TFsm("qw", TFsm::TOptions().SetCaseInsensitive(false))).Match("Qw").Final());
    }

    Y_UNIT_TEST(UnicodeCase) {
        UNIT_ASSERT(TMatcher(TFsm("\\x{61}\\x{62}", TFsm::TOptions().SetCaseInsensitive(true))).Match("Ab").Final());
        UNIT_ASSERT(!TMatcher(TFsm("\\x{61}\\x{62}", TFsm::TOptions().SetCaseInsensitive(false))).Match("Ab").Final());
    }

    Y_UNIT_TEST(Utf) {
        NRegExp::TFsmBase::TOptions opts;
        opts.Charset = CODES_UTF8;
        opts.Surround = true;
        UNIT_ASSERT(TMatcher(TFsm(".*", opts)).Match("wtf").Final());
        UNIT_ASSERT(TMatcher(TFsm(".*", opts)).Match("чзн").Final());
        UNIT_ASSERT(TMatcher(TFsm("ч.*", opts)).Match("чзн").Final());
        UNIT_ASSERT(!TMatcher(TFsm("чзн", opts)).Match("чзх").Final());
    }

    Y_UNIT_TEST(AndNot) {
        NRegExp::TFsmBase::TOptions opts;
        opts.AndNotSupport = true;
        {
            NRegExp::TFsm fsm(".*&~([0-9]*)", opts);
            UNIT_ASSERT(TMatcher(fsm).Match("a2").Final());
            UNIT_ASSERT(TMatcher(fsm).Match("ab").Final());
            UNIT_ASSERT(TMatcher(fsm).Match("1a").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("12").Final());
        }
        {
            NRegExp::TFsm fsm(".*&~(.*[0-9].*)", opts);
            UNIT_ASSERT(TMatcher(fsm).Match("ab").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("a2").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("1a").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("12").Final());
        }
        {
            NRegExp::TFsm fsm(
                "((([a-z0-9_\\-]+[.])*[a-z0-9_\\-]+)"
                "&~(\\d+[.]\\d+[.]\\d+[.]\\d+))(:\\d+)?",
                TFsm::TOptions().SetCaseInsensitive(true).SetAndNotSupport(true)
            );
            UNIT_ASSERT(TMatcher(fsm).Match("yandex.ru").Final());
            UNIT_ASSERT(TMatcher(fsm).Match("yandex").Final());
            UNIT_ASSERT(TMatcher(fsm).Match("yandex:80").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("127.0.0.1").Final());
            UNIT_ASSERT(!TMatcher(fsm).Match("127.0.0.1:8080").Final());
        }
    }

    Y_UNIT_TEST(Glue) {
        TFsm glued =
            TFsm("qw", TFsm::TOptions().SetCaseInsensitive(true)) |
            TFsm("qw", TFsm::TOptions().SetCaseInsensitive(false)) |
            TFsm("abc", TFsm::TOptions().SetCaseInsensitive(false));
        UNIT_ASSERT(TMatcher(glued).Match("Qw").Final());
        UNIT_ASSERT(TMatcher(glued).Match("Qw").Final());
        UNIT_ASSERT(TMatcher(glued).Match("abc").Final());
        UNIT_ASSERT(!TMatcher(glued).Match("Abc").Final());
    }

    Y_UNIT_TEST(Capture1) {
        TCapturingFsm fsm("here we have user_id=([a-z0-9]+);");

        TSearcher searcher(fsm);
        searcher.Search("in db and here we have user_id=0x0d0a; same as CRLF");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("0x0d0a"));
    }

    Y_UNIT_TEST(Capture2) {
        TCapturingFsm fsm("w([abcdez]+)f");

        TSearcher searcher(fsm);
        searcher.Search("wabcdef");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("abcde"));
    }

    Y_UNIT_TEST(Capture3) {
        TCapturingFsm fsm("http://vk(ontakte[.]ru|[.]com)/id(\\d+)([^0-9]|$)",
                          TFsm::TOptions().SetCapture(2));

        TSearcher searcher(fsm);
        searcher.Search("http://vkontakte.ru/id100500");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("100500"));
    }

    Y_UNIT_TEST(Capture4) {
        TCapturingFsm fsm("Здравствуйте, ((\\s|\\w|[()]|-)+)!",
                          TFsm::TOptions().SetCharset(CODES_UTF8));

        TSearcher searcher(fsm);
        searcher.Search("   Здравствуйте, Уважаемый (-ая)!   ");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("Уважаемый (-ая)"));
    }

    Y_UNIT_TEST(Capture5) {
        TCapturingFsm fsm("away\\.php\\?to=http:([^\"])+\"");
        TSearcher searcher(fsm);
        searcher.Search("\"/away.php?to=http:some.addr\"&id=1");
        UNIT_ASSERT(searcher.Captured());
        //UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("some.addr"));
    }

    Y_UNIT_TEST(Capture6) {
        TCapturingFsm fsm("(/to-match-with)");
        TSearcher searcher(fsm);
        searcher.Search("/some/table/path/to-match-with");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("/to-match-with"));
    }

    Y_UNIT_TEST(Capture7) {
        TCapturingFsm fsm("(pref.*suff)");
        TSearcher searcher(fsm);
        searcher.Search("ala pref bla suff cla");
        UNIT_ASSERT(searcher.Captured());
        //UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("pref bla suff"));
    }

    Y_UNIT_TEST(CaptureXA) {
        TCapturingFsm fsm(".*(xa).*");

        TSearcher searcher(fsm);
        searcher.Search("xa");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("xa"));
    }

    Y_UNIT_TEST(CaptureWrongXX) {
        TCapturingFsm fsm(".*(xx).*");

        TSearcher searcher(fsm);
        searcher.Search("xx");
        UNIT_ASSERT(searcher.Captured());
        // Surprise!
        // TCapturingFsm uses a fast - O(|text|) - but incorrect algorithm.
        // It works more or less for a particular class of regexps to which ".*(xx).*" does not belong.
        // So it returns not the expected "xx" but just the second "x" instead.
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("x"));
    }

    Y_UNIT_TEST(CaptureRight1XX) {
        TCapturingFsm fsm("[^x]+(xx).*");

        TSearcher searcher(fsm);

        searcher.Search("xxx");
        UNIT_ASSERT(!searcher.Captured());
    }

    Y_UNIT_TEST(CaptureRight2XX) {
        TCapturingFsm fsm("[^x]+(xx).*");

        TSearcher searcher(fsm);

        searcher.Search("axx");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("xx"));
    }

    Y_UNIT_TEST(CaptureRight3XX) {
        TCapturingFsm fsm("[^x]+(xx).*");

        TSearcher searcher(fsm);

        searcher.Search("axxb");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("xx"));
    }

    Y_UNIT_TEST(SlowCaptureXX) {
        TSlowCapturingFsm fsm(".*(xx).*");

        TSlowSearcher searcher(fsm);
        searcher.Search("xx");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("xx"));
    }

    Y_UNIT_TEST(SlowCapture) {
        TSlowCapturingFsm fsm("^http://vk(ontakte[.]ru|[.]com)/id(\\d+)([^0-9]|$)",
                              TFsm::TOptions().SetCapture(2));
        TSlowSearcher searcher(fsm);
        searcher.Search("http://vkontakte.ru/id100500");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("100500"));
    }

    Y_UNIT_TEST(SlowCaptureGreedy) {
        TSlowCapturingFsm fsm(".*(pref.*suff)");
        TSlowSearcher searcher(fsm);
        searcher.Search("pref ala bla pref cla suff dla");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("pref cla suff"));
    }

    Y_UNIT_TEST(SlowCaptureNonGreedy) {
        TSlowCapturingFsm fsm(".*?(pref.*suff)");
        TSlowSearcher searcher(fsm);
        searcher.Search("pref ala bla pref cla suff dla");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("pref ala bla pref cla suff"));
    }

    Y_UNIT_TEST(SlowCapture2) {
        TSlowCapturingFsm fsm("Здравствуйте, ((\\s|\\w|[()]|-)+)!",
                              TFsm::TOptions().SetCharset(CODES_UTF8));

        TSlowSearcher searcher(fsm);
        searcher.Search("   Здравствуйте, Уважаемый (-ая)!   ");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("Уважаемый (-ая)"));
    }

    Y_UNIT_TEST(SlowCapture3) {
        TSlowCapturingFsm fsm("here we have user_id=([a-z0-9]+);");
        TSlowSearcher searcher(fsm);
        searcher.Search("in db and here we have user_id=0x0d0a; same as CRLF");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("0x0d0a"));
    }

    Y_UNIT_TEST(SlowCapture4) {
        TSlowCapturingFsm fsm("away\\.php\\?to=http:([^\"]+)\"");
        TSlowSearcher searcher(fsm);
        searcher.Search("\"/away.php?to=http:some.addr\"&id=1");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf("some.addr"));
    }

    Y_UNIT_TEST(CapturedEmptySlow) {
        TSlowCapturingFsm fsm("Comments=(.*)$");
        TSlowSearcher searcher(fsm);
        searcher.Search("And Comments=");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf(""));
    }

    Y_UNIT_TEST(CaptureInOrFirst) {
        TSlowCapturingFsm fsm("(A)|A");
        TSlowSearcher searcher(fsm);
        searcher.Search("A");
        UNIT_ASSERT(searcher.Captured());
    }

    Y_UNIT_TEST(CaptureInOrSecond) {
        TSlowCapturingFsm fsm("A|(A)");
        TSlowSearcher searcher(fsm);
        searcher.Search("A");
        UNIT_ASSERT(!searcher.Captured());
    }

    Y_UNIT_TEST(CaptureOutside) {
        TSlowCapturingFsm fsm("((ID=([0-9]+))?)");
        TSlowSearcher searcher(fsm);
        searcher.Search("ID=");
        UNIT_ASSERT(searcher.Captured());
        UNIT_ASSERT_VALUES_EQUAL(searcher.GetCaptured(), TStringBuf(""));
    }

    Y_UNIT_TEST(CaptureInside) {
        TSlowCapturingFsm fsm("((ID=([0-9]+))?)",
                              TFsm::TOptions().SetCapture(2));
        TSlowSearcher searcher(fsm);
        searcher.Search("ID=");
        UNIT_ASSERT(!searcher.Captured());
    }

    Y_UNIT_TEST(Pcre2PireTest) {
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?:fake)"), "(fake)");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?:fake)??"), "(fake)?");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?:fake)*?fake"), "(fake)*fake");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?P<field>fake)"), "(fake)");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("fake\\#"), "fake#");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?P<field>)fake"), "fake");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?:(?P<field1>)(?P<field2>))"), "");
        UNIT_ASSERT_VALUES_EQUAL(Pcre2Pire("(?:(?:fake))"), "((fake))");
    }
}
