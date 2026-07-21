#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/meta/support_links/url_template.h>

static void AssertTemplateParameters(TStringBuf urlTemplate, const TVector<TString>& expectedParameters) {
    const TVector<TString> actualParameters = NMVP::NSupportLinks::ExtractUrlTemplateParameters(urlTemplate);
    UNIT_ASSERT_VALUES_EQUAL(actualParameters.size(), expectedParameters.size());
    for (size_t i = 0; i < expectedParameters.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actualParameters[i], expectedParameters[i]);
    }
}

Y_UNIT_TEST_SUITE(SupportLinksUrlTemplate) {
    Y_UNIT_TEST(ExtractsNamedParametersAndSkipsEscapedBraces) {
        AssertTemplateParameters(
            "https://service.example.net/{{literal}}/{host}?dc={dc}&copy={host}",
            {"host", "dc"}
        );
    }

    Y_UNIT_TEST(ValidationRejectsEmptyPlaceholder) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/{}"),
            yexception,
            "empty placeholders are not supported in url templates"
        );
    }

    Y_UNIT_TEST(ValidationRejectsMissingClosingBrace) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/{host"),
            yexception,
            "missing '}' in url template"
        );
    }

    Y_UNIT_TEST(ValidationRejectsUnmatchedClosingBrace) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/host}"),
            yexception,
            "unmatched '}' in url template"
        );
    }

    Y_UNIT_TEST(ValidationRejectsNumericPlaceholders) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/{0}"),
            yexception,
            "numeric placeholders are not supported in url templates"
        );
    }

    Y_UNIT_TEST(ValidationRejectsInvalidPlaceholderNameCharacters) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/{host-name}"),
            yexception,
            "placeholder names in url templates may contain only letters, digits, and '_'"
        );
    }

    Y_UNIT_TEST(ValidationRejectsFormatSpecifiers) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/{host:>10}"),
            yexception,
            "format specifiers are not supported in url templates"
        );
    }
}
