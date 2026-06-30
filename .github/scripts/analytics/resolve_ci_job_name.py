#!/usr/bin/env python3
"""Map GitHub workflow + build preset to analytics job_name / github_workflow."""

# Exact legacy workflow names (mute rules, BI queries).
POSTCOMMIT_LEGACY_NAMES = {
    "relwithdebinfo": "Postcommit_relwithdebinfo",
    "release-asan": "Postcommit_asan",
}

# Unified PR-check (#44879): postcommit runs in job postcommit_build_and_test, workflow stays PR-check.
POSTCOMMIT_JOB_NAME = "postcommit_build_and_test"


def resolve_ci_job_name(
    workflow_name: str, build_preset: str, github_job: str = ""
) -> str:
    is_postcommit = (
        workflow_name == "Postcommit"
        or github_job == POSTCOMMIT_JOB_NAME
    )
    if is_postcommit:
        if build_preset in POSTCOMMIT_LEGACY_NAMES:
            return POSTCOMMIT_LEGACY_NAMES[build_preset]
        return f"Postcommit_{build_preset}"
    return workflow_name


if __name__ == "__main__":
    import sys

    if len(sys.argv) not in (3, 4):
        print(
            "usage: resolve_ci_job_name.py <workflow_name> <build_preset> [github_job]",
            file=sys.stderr,
        )
        sys.exit(2)
    github_job = sys.argv[3] if len(sys.argv) == 4 else ""
    print(resolve_ci_job_name(sys.argv[1], sys.argv[2], github_job))
