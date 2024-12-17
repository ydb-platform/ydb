#include <library/cpp/testing/unittest/registar.h>

#include "yql_issue_manager.h"

using namespace NYql;

static std::function<TIssuePtr()> CreateScopeIssueFunction(TString name, ui32 column, ui32 row) {
    return [name, column, row]() {
        return new TIssue(TPosition(column, row), name);
    };
}

Y_UNIT_TEST_SUITE(TIssueManagerTest) {
    Y_UNIT_TEST(NoErrorNoLevelTest) {
        TIssueManager issueManager;
        auto completedIssues = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues.Size(), 0);
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 0);
    }

    Y_UNIT_TEST(NoErrorOneLevelTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        auto completedIssues = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues.Size(), 0);
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 0);
    }

    Y_UNIT_TEST(NoErrorTwoLevelsTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("B", 1, 1));
        issueManager.LeaveScope();
        auto completedIssues = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues.Size(), 0);
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 0);
    }

    Y_UNIT_TEST(OneErrorOneLevelTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        auto completedIssues1 = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues1.Size(), 0);
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueOne"));
        auto completedIssues2 = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues2.Size(), 0);
        issueManager.LeaveScope();
        auto completedIssues3 = issueManager.GetCompletedIssues();
        UNIT_ASSERT_VALUES_EQUAL(completedIssues3.Size(), 1);

        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        auto scopeIssue = issues.begin();
        UNIT_ASSERT_VALUES_EQUAL(scopeIssue->GetMessage(), "A");
        auto subIssues = scopeIssue->GetSubIssues();
        UNIT_ASSERT_VALUES_EQUAL(subIssues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetMessage(), "IssueOne");
    }

    Y_UNIT_TEST(OneErrorTwoLevelsTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("B", 1, 1));
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueOne"));
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        auto scopeIssue = issues.begin();
        UNIT_ASSERT_VALUES_EQUAL(scopeIssue->GetMessage(), "A");
        auto subIssues = scopeIssue->GetSubIssues();
        UNIT_ASSERT_VALUES_EQUAL(subIssues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetMessage(), "B");

        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetSubIssues().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetSubIssues()[0]->GetMessage(), "IssueOne");
    }

    Y_UNIT_TEST(MultiErrorsMultiLevelsTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.RaiseIssue(TIssue(TPosition(), "WarningScope1"));
        issueManager.AddScope(CreateScopeIssueFunction("B", 1, 1));
        issueManager.AddScope(CreateScopeIssueFunction("C", 2, 2));
        issueManager.RaiseIssue(TIssue(TPosition(), "ErrorScope3"));
        issueManager.LeaveScope();
        issueManager.RaiseIssue(TIssue(TPosition(), "ErrorScope2"));
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        auto scopeIssue = issues.begin();
        auto subIssues = scopeIssue->GetSubIssues();
        UNIT_ASSERT_VALUES_EQUAL(subIssues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetSubIssues().size(), 0); //WarningScope1
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetMessage(), "WarningScope1");
        UNIT_ASSERT_VALUES_EQUAL(subIssues[1]->GetSubIssues().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[1]->GetSubIssues()[0]->GetSubIssues().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[1]->GetSubIssues()[0]->GetSubIssues()[0]->GetMessage(), "ErrorScope3");
        UNIT_ASSERT_VALUES_EQUAL(subIssues[1]->GetSubIssues()[1]->GetMessage(), "ErrorScope2");
        auto ref = R"___(<main>: Error: A
    <main>: Error: WarningScope1
    <main>:1:1: Error: B
        <main>:2:2: Error: C
            <main>: Error: ErrorScope3
        <main>: Error: ErrorScope2
)___";
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), ref);
    }

    Y_UNIT_TEST(TIssueScopeGuardSimpleTest) {
        TIssueManager issueManager;
        {
            TIssueScopeGuard guard(issueManager, CreateScopeIssueFunction("A", 0, 0));
            issueManager.RaiseIssue(TIssue(TPosition(1,2), "ErrorScope1"));
        }
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        auto scopeIssue = issues.begin();
        UNIT_ASSERT_VALUES_EQUAL(scopeIssue->GetMessage(), "A");
        auto subIssues = scopeIssue->GetSubIssues();
        UNIT_ASSERT_VALUES_EQUAL(subIssues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subIssues[0]->GetMessage(), "ErrorScope1");
    }

    Y_UNIT_TEST(FuseScopesTest) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("B", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("C", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("D", 0, 0));
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueOne"));
        issueManager.LeaveScope();
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueTwo"));
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        auto ref = R"___(<main>: Error: A
    <main>: Error: B, C
        <main>: Error: D
            <main>:2:1: Error: IssueOne
        <main>:2:1: Error: IssueTwo
)___";
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), ref);
    }

    Y_UNIT_TEST(UniqIssues) {
        TIssueManager issueManager;
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("B", 1, 1));
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueOne"));
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueTwo"));
        issueManager.RaiseIssue(TIssue(TPosition(2,3), "IssueOne"));
        issueManager.RaiseWarning(TIssue(TPosition(2,3), "IssueOne").SetCode(1, ESeverity::TSeverityIds_ESeverityId_S_WARNING));
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        issueManager.RaiseIssue(TIssue(TPosition(1,2), "IssueOne"));
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        auto ref = R"___(<main>: Error: A
    <main>:1:1: Error: B
        <main>:2:1: Error: IssueOne
        <main>:2:1: Error: IssueTwo
        <main>:3:2: Error: IssueOne
        <main>:3:2: Warning: IssueOne, code: 1
)___";
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), ref);
    }

    Y_UNIT_TEST(Limits) {
        TIssueManager issueManager;
        issueManager.SetIssueCountLimit(2);
        issueManager.AddScope(CreateScopeIssueFunction("A", 0, 0));
        issueManager.AddScope(CreateScopeIssueFunction("B", 1, 1));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue1"));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue2"));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue3"));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue4").SetCode(1, ESeverity::TSeverityIds_ESeverityId_S_WARNING));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue5").SetCode(1, ESeverity::TSeverityIds_ESeverityId_S_WARNING));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue6").SetCode(1, ESeverity::TSeverityIds_ESeverityId_S_WARNING));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue7").SetCode(2, ESeverity::TSeverityIds_ESeverityId_S_INFO));
        issueManager.RaiseIssue(TIssue(TPosition(1,1), "Issue8").SetCode(2, ESeverity::TSeverityIds_ESeverityId_S_INFO));
        issueManager.LeaveScope();
        issueManager.LeaveScope();
        auto issues = issueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 3);
        auto ref = R"___(<main>: Error: Too many Error issues
<main>: Warning: Too many Warning issues
<main>: Error: A
    <main>:1:1: Error: B
        <main>:1:1: Error: Issue1
        <main>:1:1: Error: Issue2
        <main>:1:1: Warning: Issue4, code: 1
        <main>:1:1: Warning: Issue5, code: 1
        <main>:1:1: Info: Issue7, code: 2
        <main>:1:1: Info: Issue8, code: 2
)___";
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), ref);
    }
}
