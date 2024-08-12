#include <ydb/core/fq/libs/metrics/status_code_counters.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(Metrics) {
    Y_UNIT_TEST(EmptyIssuesList) {
        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, NYql::TIssues{}), "GENERIC_ERROR");
    }

    Y_UNIT_TEST(OnlyOneItem) {
        auto issue = NYql::TIssue{"compile error"};
        issue.SetCode(NYql::TIssuesIds::KIKIMR_COMPILE_ERROR, NYql::TSeverityIds::S_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, NYql::TIssues{issue}), "GENERIC_ERROR__KIKIMR_COMPILE_ERROR__E");
    }

    Y_UNIT_TEST(SeveralTopItems) {
        NYql::TIssues issues;
        {
            auto issue = NYql::TIssue{"compile error"};
            issue.SetCode(NYql::TIssuesIds::KIKIMR_COMPILE_ERROR, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
        {
            auto issue = NYql::TIssue{"optimize error"};
            issue.SetCode(NYql::TIssuesIds::DQ_OPTIMIZE_ERROR, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
        {
            auto issue = NYql::TIssue{"default error"};
            issue.SetCode(NYql::TIssuesIds::DEFAULT_ERROR, NYql::TSeverityIds::S_WARNING);
            issues.AddIssue(issue);
        }
       
        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, issues), "GENERIC_ERROR__KIKIMR_COMPILE_ERROR__E__DQ_OPTIMIZE_ERROR__E__DEFAULT_ERROR__W");
    }

    Y_UNIT_TEST(MoreThanFiveItems) {
        NYql::TIssues issues;
        for (int i = 0; i < 10; i++)
        {
            auto issue = NYql::TIssue{"compile error"};
            issue.SetCode(NYql::TIssuesIds::KIKIMR_COMPILE_ERROR, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
       
        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, issues), "GENERIC_ERROR__KIKIMR_COMPILE_ERROR__E__KIKIMR_COMPILE_ERROR__E__KIKIMR_COMPILE_ERROR__E__KIKIMR_COMPILE_ERROR__E__KIKIMR_COMPILE_ERROR__E");
    }

    Y_UNIT_TEST(SeveralSubItems) {
        NYql::TIssues issues;

        TIntrusivePtr<NYql::TIssue> topLevelIssue = MakeIntrusive<NYql::TIssue>("compile error");
        topLevelIssue->SetCode(NYql::TIssuesIds::KIKIMR_COMPILE_ERROR, NYql::TSeverityIds::S_ERROR);

        {
            TIntrusivePtr<NYql::TIssue> issue = MakeIntrusive<NYql::TIssue>("optimize error");
            issue->SetCode(NYql::TIssuesIds::DQ_OPTIMIZE_ERROR, NYql::TSeverityIds::S_ERROR);
            issue->AddSubIssue(topLevelIssue);
            topLevelIssue = issue;
        }

        {
            TIntrusivePtr<NYql::TIssue> issue = MakeIntrusive<NYql::TIssue>("default error");
            issue->SetCode(NYql::TIssuesIds::DEFAULT_ERROR, NYql::TSeverityIds::S_WARNING);
            issue->AddSubIssue(topLevelIssue);
            topLevelIssue = issue;
        }

        issues.AddIssue(*topLevelIssue);
        issues.back().SetCode(NYql::TIssuesIds::DEFAULT_ERROR, NYql::TSeverityIds::S_WARNING);

        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, issues), "GENERIC_ERROR__DEFAULT_ERROR__W__DQ_OPTIMIZE_ERROR__E__KIKIMR_COMPILE_ERROR__E");
    }

    Y_UNIT_TEST(CombineSubItems) {
        NYql::TIssues issues;

        TIntrusivePtr<NYql::TIssue> topLevelIssue = MakeIntrusive<NYql::TIssue>("compile error");
        topLevelIssue->SetCode(NYql::TIssuesIds::KIKIMR_COMPILE_ERROR, NYql::TSeverityIds::S_ERROR);

        {
            TIntrusivePtr<NYql::TIssue> issue = MakeIntrusive<NYql::TIssue>("optimize error");
            issue->SetCode(NYql::TIssuesIds::DQ_OPTIMIZE_ERROR, NYql::TSeverityIds::S_ERROR);
            issue->AddSubIssue(topLevelIssue);
            topLevelIssue = issue;
        }

        {
            TIntrusivePtr<NYql::TIssue> issue = MakeIntrusive<NYql::TIssue>("default error");
            issue->SetCode(NYql::TIssuesIds::DEFAULT_ERROR, NYql::TSeverityIds::S_WARNING);
            issue->AddSubIssue(topLevelIssue);
            topLevelIssue = issue;
        }
        issues.AddIssue(*topLevelIssue);
        issues.back().SetCode(NYql::TIssuesIds::DEFAULT_ERROR, NYql::TSeverityIds::S_WARNING);

        {
            auto issue = NYql::TIssue{"scheme error"};
            issue.SetCode(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
        {
            auto issue = NYql::TIssue{"json parse error"};
            issue.SetCode(NYql::TIssuesIds::JSONPATH_PARSE_ERROR, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
        {
            auto issue = NYql::TIssue{"core exec error"};
            issue.SetCode(NYql::TIssuesIds::CORE_EXEC, NYql::TSeverityIds::S_WARNING);
            issues.AddIssue(issue);
        }

        UNIT_ASSERT_VALUES_EQUAL(NFq::LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_GENERIC_ERROR, issues), "GENERIC_ERROR__DEFAULT_ERROR__W__KIKIMR_SCHEME_ERROR__E__JSONPATH_PARSE_ERROR__E__CORE_EXEC__W__DQ_OPTIMIZE_ERROR__E");
    }
}
