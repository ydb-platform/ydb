#!/usr/bin/env python3
"""Map GitHub workflow + build preset to analytics job_name / github_workflow."""

# Exact legacy workflow names (mute rules, BI queries).
POSTCOMMIT_LEGACY_NAMES = {
    "relwithdebinfo": "Postcommit_relwithdebinfo",
    "release-asan": "Postcommit_asan",
}


def resolve_ci_job_name(workflow_name: str, build_preset: str) -> str:
    if workflow_name == "Postcommit":
        if build_preset in POSTCOMMIT_LEGACY_NAMES:
            return POSTCOMMIT_LEGACY_NAMES[build_preset]
        # New presets: Postcommit_<preset>, same pattern as legacy yaml names.
        return f"Postcommit_{build_preset}"
    return workflow_name


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("usage: resolve_ci_job_name.py <workflow_name> <build_preset>", file=sys.stderr)
        sys.exit(2)
    print(resolve_ci_job_name(sys.argv[1], sys.argv[2]))
