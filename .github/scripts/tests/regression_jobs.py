"""Regression/nightly job names for test_results queries. Aligned with flaky_tests_history.py."""

REGRESSION_JOB_NAMES = (
    "Nightly-run",
    "Regression-run",
    "Regression-run_Large",
    "Regression-run_Small_and_Medium",
    "Regression-run_compatibility",
    "Regression-whitelist-run",
    "Postcommit_relwithdebinfo",
    "Postcommit_asan",
)


def regression_job_names_sql() -> str:
    """Build SQL IN clause for regression job names."""
    return ", ".join(f"'{j}'" for j in REGRESSION_JOB_NAMES)
