#include "kqp_federated_query_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

#include <yql/essentials/public/issue/protos/issue_message.pb.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(TestFederatedQueryHelpers) {
    NYql::TIssue BuildNestedIssues(ui32 depth) {
        NYql::TIssue issue("Testing nested issue");
        for (ui32 i = 0; i + 1 < depth; ++i) {
            NYql::TIssue nestedIssue(TStringBuilder() << "Nested issue " << i);
            nestedIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            issue = nestedIssue;
        }
        return issue;
    }

    Y_UNIT_TEST(TestCheckNestingDepth) {
        constexpr ui32 depth = 30;
        NYql::NIssue::NProto::IssueMessage nestedProto;
        NYql::IssueToMessage(BuildNestedIssues(depth), &nestedProto);

        UNIT_ASSERT_C(!CheckNestingDepth(nestedProto, depth - 1), depth - 1);
        UNIT_ASSERT_C(CheckNestingDepth(nestedProto, depth), depth);
        UNIT_ASSERT_C(CheckNestingDepth(nestedProto, depth + 1), depth + 1);
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

}  // namespace NKikimr::NKqp
