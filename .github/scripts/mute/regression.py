"""Regression/nightly job names and filters for test_results queries."""

# workflow_dispatch runs get run_name with "_manual" suffix (test_ya/action.yml)
# Exclude manual runs from analytics (mute, patterns, flaky history)
EXCLUDE_MANUAL_RUNS_SQL = "AND (pull IS NULL OR pull NOT LIKE '%_manual%')"

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
