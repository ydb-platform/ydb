#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYql;

Y_UNIT_TEST_SUITE(KqpResponse) {
    Y_UNIT_TEST_TWIN(FallbackInfoPreservesSpecificError, Nested) {
        const auto fallback = TIssue("Compilation with the new RBO failed: unsupported query")
            .SetCode(DEFAULT_ERROR, TSeverityIds::S_INFO);

        for (const auto code : {TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION,
                TIssuesIds::KIKIMR_PRECONDITION_FAILED, TIssuesIds::KIKIMR_SCHEME_ERROR}) {
            const auto error = TIssue("The query failed").SetCode(code, TSeverityIds::S_ERROR);
            const auto expected = code == TIssuesIds::KIKIMR_SCHEME_ERROR
                ? Ydb::StatusIds::SCHEME_ERROR : Ydb::StatusIds::PRECONDITION_FAILED;

            TIssues issues;
            if constexpr (Nested) {
                TIssue execution("Execution");
                execution.SetCode(TIssuesIds::CORE_EXEC, TSeverityIds::S_ERROR);
                execution.AddSubIssue(MakeIntrusive<TIssue>(fallback));
                execution.AddSubIssue(MakeIntrusive<TIssue>(error));
                issues.AddIssue(execution);
            } else {
                issues.AddIssue(fallback);
                issues.AddIssue(error);
            }

            const auto diagnostics = issues.ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(GetYdbStatus(issues), expected, diagnostics);
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), diagnostics);
        }
    }

    Y_UNIT_TEST(NonErrorIssuesDoNotOverrideErrors) {
        const auto error = TIssue("Client data writes are disabled")
            .SetCode(TIssuesIds::KIKIMR_PRECONDITION_FAILED, TSeverityIds::S_ERROR);
        for (const auto severity : {TSeverityIds::S_INFO, TSeverityIds::S_WARNING}) {
            // Severity must be checked before a diagnostic's code is mapped to a status.
            const auto diagnostic = TIssue("An unrelated diagnostic")
                .SetCode(TIssuesIds::KIKIMR_ACCESS_DENIED, severity);
            UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{diagnostic, error}), Ydb::StatusIds::PRECONDITION_FAILED);
            UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{error, diagnostic}), Ydb::StatusIds::PRECONDITION_FAILED);
        }
    }

    Y_UNIT_TEST(ErrorPrecedenceIsPreserved) {
        const auto info = TIssue("Compilation with the new RBO failed")
            .SetCode(DEFAULT_ERROR, TSeverityIds::S_INFO);
        const auto precondition = TIssue("Constraint violated")
            .SetCode(TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, TSeverityIds::S_ERROR);
        const auto generic = TIssue("Another error").SetCode(DEFAULT_ERROR, TSeverityIds::S_ERROR);
        const auto fatal = TIssue("Fatal error").SetCode(DEFAULT_ERROR, TSeverityIds::S_FATAL);
        const auto denied = TIssue("Access denied")
            .SetCode(TIssuesIds::KIKIMR_ACCESS_DENIED, TSeverityIds::S_ERROR);

        UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{info, precondition, generic}), Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{info, precondition, fatal}), Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{info, fatal, denied}), Ydb::StatusIds::UNAUTHORIZED);
    }

    Y_UNIT_TEST(NonErrorIssuesDoNotImplySuccess) {
        const auto info = TIssue("An informational diagnostic").SetCode(DEFAULT_ERROR, TSeverityIds::S_INFO);
        const auto warning = TIssue("A warning").SetCode(DEFAULT_ERROR, TSeverityIds::S_WARNING);

        // Success is determined by the operation result, not the absence of error issues.
        UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{}), Ydb::StatusIds::GENERIC_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(GetYdbStatus(TIssues{info, warning}), Ydb::StatusIds::GENERIC_ERROR);
    }
}

} // namespace NKikimr::NKqp
