#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/meta/support_links/url_template.h>

static void AssertTemplateValidationError(TStringBuf urlTemplate, TStringBuf expectedMessagePart) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        NMVP::NSupportLinks::ValidateUrlTemplateSyntax(urlTemplate),
        yexception,
        expectedMessagePart
    );
}

Y_UNIT_TEST_SUITE(SupportLinksUrlTemplate) {
    constexpr TStringBuf InvalidNameError =
        "url template placeholders must use the form {name}, where name matches [A-Za-z_][A-Za-z0-9_]*";

    Y_UNIT_TEST(NoParamsAreOk) {
        UNIT_ASSERT_NO_EXCEPTION(NMVP::NSupportLinks::ValidateUrlTemplateSyntax("https://service.example.net/"));
    }

    Y_UNIT_TEST(RecognizesTemplateExpressionsAndParameters) {
        const TStringBuf urlTemplate = "https://service.example.net/{host}?dc={dc}&copy={host}";
        UNIT_ASSERT(NMVP::NSupportLinks::HasUrlTemplateExpressions(urlTemplate));
        UNIT_ASSERT(NMVP::NSupportLinks::HasUrlTemplateParameter(urlTemplate, "host"));
        UNIT_ASSERT(NMVP::NSupportLinks::HasUrlTemplateParameter(urlTemplate, "dc"));
        UNIT_ASSERT(!NMVP::NSupportLinks::HasUrlTemplateParameter(urlTemplate, "literal"));
    }

    Y_UNIT_TEST(ValidationRejectsMissingClosingBrace) {
        AssertTemplateValidationError(
            "https://service.example.net/{host",
            "missing '}' in url template"
        );
    }

    Y_UNIT_TEST(ValidationRejectsUnmatchedClosingBrace) {
        AssertTemplateValidationError(
            "https://service.example.net/host}",
            "unmatched '}' in url template"
        );
    }

    Y_UNIT_TEST(ValidationRejectsInvalidPlaceholderNameCharacters) {
        AssertTemplateValidationError(
            "https://service.example.net/{host-name}",
            InvalidNameError
        );
    }
}
