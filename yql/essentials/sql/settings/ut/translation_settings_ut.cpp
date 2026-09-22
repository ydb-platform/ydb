#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/public/issue/yql_issue.h>

Y_UNIT_TEST_SUITE(TTranslationSettings) {
Y_UNIT_TEST(InfersCustomSyntax) {
    NSQLTranslation::TTranslationSettings settings;
    NYql::TIssues issues;

    UNIT_ASSERT(NSQLTranslation::ParseTranslationSettings("--!syntax_mock\nSELECT 1;", settings, issues));
    UNIT_ASSERT_VALUES_EQUAL(*settings.Syntax, "mock");
}
} // Y_UNIT_TEST_SUITE(TTranslationSettings)
