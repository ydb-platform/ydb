#include "audit_log_impl.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NAudit {

Y_UNIT_TEST_SUITE(EscapeNonUtf8LogPartsTest) {
    Y_UNIT_TEST(Escape) {
        TAuditLogParts parts = {
            {"name", "value"}, // utf8, OK
            {"", ""}, // utf8, OK
            {"valid-utf-8", "🤺 ברוך הבא"}, // nonusual, but valid utf8
            {"\xc2non\xfeutf-8", "valid - \xfe\xfa\xf5\xc2 - valid"}, // need to be escaped
            {"newline", "\n"},
            {"backslash", "\\"},
        };

        EscapeNonUtf8LogParts(parts);


        TAuditLogParts expectedParts = {
            {"name", "value"},
            {"", ""},
            {"valid-utf-8", "🤺 ברוך הבא"},
            {"C26E6F6EFE7574662D38", "76616C6964202D20FEFAF5C2202D2076616C6964"},
            {"newline", "\n"},
            {"backslash", "\\"},
        };

        UNIT_ASSERT_VALUES_EQUAL(parts.size(), expectedParts.size());
        for (size_t i = 0; i < parts.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(parts[i].first, expectedParts[i].first, i);
            UNIT_ASSERT_VALUES_EQUAL_C(parts[i].second, expectedParts[i].second, i);
        }
    }
}

} // namespace NKikimr::NAudit
