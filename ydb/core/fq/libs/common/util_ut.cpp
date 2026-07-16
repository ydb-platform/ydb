#include "util.h"
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <yql/essentials/public/issue/protos/issue_message.pb.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

Y_UNIT_TEST_SUITE(EscapingBasics) {
    Y_UNIT_TEST(EncloseSecretShouldWork) {
        UNIT_ASSERT_VALUES_EQUAL(EncloseSecret("some_secret1"), "/* 51a91b5d91a99eb7 */some_secret1/* e87c9b191b202354 */");
    }

    Y_UNIT_TEST(HideSecretsShouldWork) {
        UNIT_ASSERT_VALUES_EQUAL(HideSecrets("some text"), "some text");
        UNIT_ASSERT_VALUES_EQUAL(HideSecrets("/* 51a91b5d91a99eb7 */some_secret1/* e87c9b191b202354 */"), "/*SECRET*/");
        UNIT_ASSERT_VALUES_EQUAL(HideSecrets("/* 51a91b5d91a99eb7 */some_secret1/* e87c9b191b202354 */\n/* 51a91b5d91a99eb7 */some_secret2/* e87c9b191b202354 */"), "/*SECRET*/\n/*SECRET*/");
    }

    Y_UNIT_TEST(HideSecretsOverEncloseSecretShouldWork) {
        UNIT_ASSERT_VALUES_EQUAL(HideSecrets(EncloseSecret("some_secret1")), "/*SECRET*/");
    }

    Y_UNIT_TEST(EscapeStringShouldWork) {
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some_secret1", '"'), "some_secret1");
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some_secret1", "}+{", "[*]"), "some_secret1");
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some\"_\"secret1", '"'), "some\\\"_\\\"secret1");
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some\"_\\\"secret1", '"'), "some\\\"_\\\\\\\"secret1");
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some}+{_}+{secret1", "}+{", "[*]"), "some[*]_[*]secret1");
        UNIT_ASSERT_VALUES_EQUAL(EscapeString("some}+{\\}+{secret1", "}+{", "[*]"), "some[*]\\\\[*]secret1");
    }

    Y_UNIT_TEST(EncloseAndEscapeStringShouldWork) {
        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some_secret1", '"'), "\"some_secret1\"");
        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some_secret1\nsome_secret2", "}+{", "[*]"), "}+{some_secret1\nsome_secret2}+{");

        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some\"_\"secret1", '"'), "\"some\\\"_\\\"secret1\"");
        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some\"_\\\"secret1", '"'), "\"some\\\"_\\\\\\\"secret1\"");
        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some_secret1}+{\n}+{some_secret2", "}+{", "[*]"), "}+{some_secret1[*]\n[*]some_secret2}+{");
        UNIT_ASSERT_VALUES_EQUAL(EncloseAndEscapeString("some_secret1}+{\\}+{some_secret2", "}+{", "[*]"), "}+{some_secret1[*]\\\\[*]some_secret2}+{");
    }
}

Y_UNIT_TEST_SUITE(IssuesTextFiltering) {
    Y_UNIT_TEST(ShouldRemoveDatabasePath) {
        TVector<NYql::TIssue> vecIssues;
        auto bottomIssue = MakeIntrusive<NYql::TIssue>("Error /path/to/database/binding_name db.[/path/to/database/binding_name]");
        auto midIssue = MakeIntrusive<NYql::TIssue>("'db.[/path/to/database/baz]' /path/to/database/foo /path/to/database/bar");
        midIssue->AddSubIssue(bottomIssue);
        NYql::TIssue topIssue("While doing smth db.[] /path/to/other/smth");
        topIssue.AddSubIssue(midIssue);
        NYql::TIssues issues({topIssue});

        TVector<NYql::TIssue> vecIssuesExpected;
        auto bottomIssueExpected = MakeIntrusive<NYql::TIssue>("Error binding_name binding_name");
        auto midIssueExpected = MakeIntrusive<NYql::TIssue>("'baz' foo bar");
        midIssueExpected->AddSubIssue(bottomIssueExpected);
        NYql::TIssue topIssueExpected("While doing smth db.[] /path/to/other/smth");
        topIssueExpected.AddSubIssue(midIssueExpected);
        NYql::TIssues issuesExpected({topIssueExpected});

        auto issuesActual = RemoveDatabaseFromIssues(issues, "/path/to/database");
        
        auto iterActual = issuesActual.begin();
        auto iterExpected = issuesExpected.begin();
        while (iterActual != issuesActual.end()) {
            UNIT_ASSERT_VALUES_EQUAL(*iterActual, *iterExpected);
            ++iterActual;
            ++iterExpected;
        }
    }
}

Y_UNIT_TEST_SUITE(IssuesTruncating) {
    NYql::TIssue BuildNestedIssues(ui32 depth) {
        NYql::TIssue issue("Testing nested issue");
        for (ui32 i = 0; i + 1 < depth; ++i) {
            NYql::TIssue nestedIssue(TStringBuilder() << "Nested issue " << i);
            nestedIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            issue = nestedIssue;
        }
        return issue;
    }

    Y_UNIT_TEST(TestTruncateIssues) {
        constexpr ui32 depth = 30;
        NYql::TIssue issue = BuildNestedIssues(depth);

        constexpr ui32 maxLevels = 10;
        constexpr ui32 keepTailLevels = 3;
        const auto truncated = TruncateIssues({issue}, maxLevels, keepTailLevels);
        UNIT_ASSERT_VALUES_EQUAL(truncated.Size(), 1);

        NYql::NIssue::NProto::IssueMessage nestedProto;
        NYql::IssueToMessage(*truncated.begin(), &nestedProto);
        UNIT_ASSERT(CheckNestingDepth(nestedProto, maxLevels + keepTailLevels));
    }

    Y_UNIT_TEST(TestCheckNestingDepth) {
        constexpr ui32 depth = 30;
        NYql::NIssue::NProto::IssueMessage nestedProto;
        NYql::IssueToMessage(BuildNestedIssues(depth), &nestedProto);

        UNIT_ASSERT_C(!CheckNestingDepth(nestedProto, depth - 1), depth - 1);
        UNIT_ASSERT_C(CheckNestingDepth(nestedProto, depth), depth);
        UNIT_ASSERT_C(CheckNestingDepth(nestedProto, depth + 1), depth + 1);
    }

    Y_UNIT_TEST(TestValidateResultSetColumns) {
        constexpr ui32 depth = 30;

        Ydb::Type type;
        type.set_type_id(Ydb::Type::INT32);
        for (ui32 i = 0; i < depth; ++i) {
            Ydb::Type nestedType;
            *nestedType.mutable_optional_type()->mutable_item() = type;
            type = nestedType;
        }

        Ydb::Query::ResultSetMeta meta;
        *meta.add_columns()->mutable_type() = type;

        auto issues = ValidateResultSetColumns(meta.columns(), 2 * depth);
        UNIT_ASSERT_C(!issues.Empty(), 2 * depth);

        issues = ValidateResultSetColumns(meta.columns(), 2 * depth + 1);
        UNIT_ASSERT_C(issues.Empty(), issues.ToOneLineString());
    }
}

} // NFq
