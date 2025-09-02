#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NIniConfig;

Y_UNIT_TEST_SUITE(IniConfigBasicTests) {
    Y_UNIT_TEST(SimpleKeyValue) {
        const TString ini = R"ini(
            key1 = value1
            key2=value2
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["key1"].As<TString>(), "value1");
        UNIT_ASSERT_VALUES_EQUAL(cfg["key2"].As<TString>(), "value2");
    }

    Y_UNIT_TEST(CommentsAndWhitespace) {
        const TString ini = R"ini(
            # This is a comment
            ; This is a semicolon comment
            key = value   # inline comment
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["key"].As<TString>(), "value");
    }

    Y_UNIT_TEST(SimpleSection) {
        const TString ini = R"ini(
            [section]
            name = John
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["section"]["name"].As<TString>(), "John");
    }

    Y_UNIT_TEST(VeryDeepSection) {
        const TString ini = R"ini(
            [a.b.c.d.e]
            nested = ok
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["a"]["b"]["c"]["d"]["e"]["nested"].As<TString>(), "ok");
    }

    Y_UNIT_TEST(SectionOverridesRoot) {
        const TString ini = R"ini(
            name = root

            [sec]
            name = section
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["name"].As<TString>(), "root");
        UNIT_ASSERT_VALUES_EQUAL(cfg["sec"]["name"].As<TString>(), "section");
    }

    Y_UNIT_TEST(NotExistingKey) {
        const TString ini = R"ini(
            key = value
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);

        UNIT_ASSERT(cfg.Has("key"));
        UNIT_ASSERT(!cfg.Has("missing"));

        UNIT_ASSERT_EXCEPTION_CONTAINS(cfg.At("missing"), yexception, "missing key");

        UNIT_ASSERT(cfg["missing"].IsNull());
    }

    Y_UNIT_TEST(IncorrectSectionFormat) {
        const TString ini = R"ini(
            [unclosed
            key = val
        )ini";

        UNIT_ASSERT_EXCEPTION_CONTAINS(TConfig::ReadIni(ini), TConfigParseError, "malformed section");
    }

    Y_UNIT_TEST(LineWithoutEquals) {
        const TString ini = R"ini(
            key=val
            broken_line
        )ini";

        UNIT_ASSERT_EXCEPTION_CONTAINS(TConfig::ReadIni(ini), TConfigParseError, "invalid line");
    }

    Y_UNIT_TEST(QuotedValuesPlaceholder) {
        const TString ini = R"ini(
            path = "/usr/bin"
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_EQUAL(cfg["path"].As<TString>(), "\"/usr/bin\"");
    }

    Y_UNIT_TEST(AsWithDefaults) {
        const TString ini = R"ini(
            key = true
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);

        bool flag = cfg["key"].As<bool>();
        bool fallback = cfg["missing"].As<bool>(false);

        UNIT_ASSERT_EQUAL(flag, true);
        UNIT_ASSERT_EQUAL(fallback, false);
    }

    Y_UNIT_TEST(EmptyContentReturnsEmptyDict) {
        const TString ini = "";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT(cfg.IsA<TDict>());
        UNIT_ASSERT_EQUAL(cfg.Get<TDict>().size(), 0);
    }

    Y_UNIT_TEST(SectionWithDotInName) {
        const TString ini = R"ini(
            [service.local]
            port = 1234
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT(cfg["service"]["local"].Has("port"));
        UNIT_ASSERT_EQUAL(cfg["service"]["local"]["port"].As<int>(), 1234);
    }

    Y_UNIT_TEST(SectionBeforeAndAfterRootKeys) {
        const TString ini = R"ini(
            key1 = root1

            [group]
            key2 = group2

            key3 = root3
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);
        UNIT_ASSERT_VALUES_EQUAL(cfg["key1"].As<TString>(), "root1");
        UNIT_ASSERT_VALUES_EQUAL(cfg["group"]["key2"].As<TString>(), "group2");

        // ini parser considers that key3 is also in a group
        UNIT_ASSERT_VALUES_EQUAL(cfg["group"]["key3"].As<TString>(), "root3");
    }

    Y_UNIT_TEST(TypeChecks_GetAs) {
        const TString ini = R"ini(
            str = value
            [dict]
            key = val
            [arr]
            0 = first
            1 = second
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);

        // str
        UNIT_ASSERT(cfg["str"].IsA<TString>());
        UNIT_ASSERT_VALUES_EQUAL(cfg["str"].Get<TString>(), "value");
        UNIT_ASSERT_VALUES_EQUAL(cfg["str"].As<TString>(), "value");
        UNIT_ASSERT(cfg["str"].IsNumeric() == false);

        // dict
        UNIT_ASSERT(cfg["dict"].IsA<TDict>());
        const auto& dict = cfg["dict"].Get<TDict>();
        UNIT_ASSERT_VALUES_EQUAL(dict.At("key").As<TString>(), "val");

        // arr – manual emulation: arr section with keys "0", "1"
        UNIT_ASSERT(cfg["arr"].IsA<TDict>());
        const auto& arrDict = cfg["arr"].Get<TDict>();

        TVector<TConfig> arr;
        for (size_t i = 0; i < arrDict.size(); ++i) {
            TString k = ToString(i);
            UNIT_ASSERT(arrDict.find(k) != arrDict.end());
            arr.push_back(arrDict.at(k));
        }

        UNIT_ASSERT_EQUAL(arr.size(), 2);
        UNIT_ASSERT_EQUAL(arr[0].As<TString>(), "first");
        UNIT_ASSERT_EQUAL(arr[1].As<TString>(), "second");
    }

    Y_UNIT_TEST(TypeMismatchThrows) {
        const TString ini = R"ini(
            str = hello
            [dict]
            key = val
        )ini";

        TConfig cfg = TConfig::ReadIni(ini);

        UNIT_ASSERT_EXCEPTION_CONTAINS(cfg["str"].Get<TDict>(), TTypeMismatch, "string");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cfg["dict"].Get<TString>(), TTypeMismatch, "dict");
    }

}
