#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <util/string/cast.h>

Y_UNIT_TEST_SUITE(TTranslationSettings) {
Y_UNIT_TEST(InfersCustomSyntax) {
    NSQLTranslation::TTranslationSettings settings;
    NYql::TIssues issues;

    UNIT_ASSERT(NSQLTranslation::ParseTranslationSettings("--!syntax_mock\nSELECT 1;", settings, issues));
    UNIT_ASSERT_VALUES_EQUAL(*settings.Syntax, "mock");
}
} // Y_UNIT_TEST_SUITE(TTranslationSettings)

namespace NSQLTranslation {

Y_UNIT_TEST_SUITE(TTranslationSettingsFlagsTest) {
Y_UNIT_TEST(KnownFlagsAcceptAdditionalArguments) {
    const TVector<std::pair<TString, EYqlSelect>> modes = {
        {"disable", EYqlSelect::Disable},
        {"auto", EYqlSelect::Auto},
        {"force", EYqlSelect::Force},
    };
    for (const auto& [value, expected] : modes) {
        TTranslationSettings settings;
        settings.YqlSelect = expected == EYqlSelect::Force ? EYqlSelect::Disable : EYqlSelect::Force;

        ParseTranslationSettings(TExtendedSqlFlags{{"YqlSelect", {value, "extra"}}}, settings);

        UNIT_ASSERT(settings.YqlSelect == expected);
    }

    TTranslationSettings settings;

    ParseTranslationSettings(TExtendedSqlFlags{{"MaxParseTreeDepth", {"12345", "extra"}}}, settings);

    UNIT_ASSERT_VALUES_EQUAL(settings.MaxParseTreeDepth, size_t(12345));
}

Y_UNIT_TEST(InvalidKnownFlagValuesAreRejected) {
    TTranslationSettings settings;
    const TExtendedSqlFlags invalidYqlSelect = {{"YqlSelect", {"invalid", "extra"}}};
    const TExtendedSqlFlags invalidMaxParseTreeDepth = {{"MaxParseTreeDepth", {"invalid", "extra"}}};

    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseTranslationSettings(invalidYqlSelect, settings), yexception, "Bad YqlSelect args");
    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseTranslationSettings(invalidMaxParseTreeDepth, settings), yexception, "Bad MaxParseTreeDepth args");
}

Y_UNIT_TEST(GroupingLimits) {
    TTranslationSettings settings;
    UNIT_ASSERT_VALUES_EQUAL(settings.GroupByLimit, 64);
    UNIT_ASSERT_VALUES_EQUAL(settings.GroupByCubeLimit, 5);
    for (TStringBuf value : {"0", "2", "4294967295"}) {
        ParseTranslationSettings(TExtendedSqlFlags{{"GroupByLimit", {TString(value)}}, {"GroupByCubeLimit", {TString(value)}}}, settings);
        UNIT_ASSERT_VALUES_EQUAL(settings.GroupByLimit, FromString<ui32>(value));
        UNIT_ASSERT_VALUES_EQUAL(settings.GroupByCubeLimit, FromString<ui32>(value));
    }
    for (TStringBuf flag : {"GroupByLimit", "GroupByCubeLimit"}) {
        for (TStringBuf value : {"-1", "invalid", "4294967296"}) {
            const TExtendedSqlFlags flags{{TString(flag), {TString(value)}}};
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseTranslationSettings(flags, settings), yexception, TString("Bad ") + flag);
        }
    }
}

Y_UNIT_TEST(UnknownValuableFlagIsIgnoredByDefault) {
    TTranslationSettings settings;
    ParseTranslationSettings(TExtendedSqlFlags{{"UnknownFlag", {"some", "args"}}}, settings);
    UNIT_ASSERT(settings.Flags.empty());
}

Y_UNIT_TEST(UnknownValuableFlagIsRejectedInStrictMode) {
    TTranslationSettings settings;
    settings.StrictConfigValidation = true;
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        ParseTranslationSettings(TExtendedSqlFlags{{"UnknownFlag", {"some", "args"}}}, settings),
        yexception, "Unknown SQL flag: UnknownFlag");
}

Y_UNIT_TEST(UnknownSimpleFlagIsIgnoredEvenInStrictMode) {
    TTranslationSettings settings;
    settings.StrictConfigValidation = true;
    ParseTranslationSettings(TExtendedSqlFlags{{"UnknownSimpleFlag", {}}}, settings);
    UNIT_ASSERT(settings.Flags.contains("UnknownSimpleFlag"));
}
} // Y_UNIT_TEST_SUITE(TTranslationSettingsFlagsTest)

} // namespace NSQLTranslation
