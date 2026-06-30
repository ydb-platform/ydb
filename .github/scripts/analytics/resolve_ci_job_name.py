#!/usr/bin/env python3
"""Map GitHub workflow + build preset to analytics job_name / github_workflow."""

POSTCOMMIT_LEGACY_NAMES = {
    "relwithdebinfo": "Postcommit_relwithdebinfo",
    "release-asan": "Postcommit_asan",
}


def resolve_ci_job_name(workflow_name: str, build_preset: str) -> str:
    if workflow_name == "Postcommit":
        return POSTCOMMIT_LEGACY_NAMES.get(build_preset, workflow_name)
    return workflow_name


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("usage: resolve_ci_job_name.py <workflow_name> <build_preset>", file=sys.stderr)
        sys.exit(2)
    print(resolve_ci_job_name(sys.argv[1], sys.argv[2]))
