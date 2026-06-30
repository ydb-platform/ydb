#!/usr/bin/env python3
"""Map GitHub workflow + build preset to analytics job_name / github_workflow."""

# Exact legacy workflow names (mute rules, BI queries).
POSTCOMMIT_LEGACY_NAMES = {
    "relwithdebinfo": "Postcommit_relwithdebinfo",
    "release-asan": "Postcommit_asan",
}


def _postcommit_preset_suffix(build_preset: str) -> str:
    if build_preset.startswith("release-"):
        return build_preset.removeprefix("release-")
    return build_preset


def resolve_ci_job_name(
    workflow_name: str, build_preset: str, event_name: str = ""
) -> str:
    is_postcommit = workflow_name == "Postcommit" or (
        workflow_name == "PR-check" and event_name == "push"
    )
    if is_postcommit:
        if build_preset in POSTCOMMIT_LEGACY_NAMES:
            return POSTCOMMIT_LEGACY_NAMES[build_preset]
        return f"Postcommit_{_postcommit_preset_suffix(build_preset)}"
    return workflow_name


if __name__ == "__main__":
    import sys

    if len(sys.argv) not in (3, 4):
        print(
            "usage: resolve_ci_job_name.py <workflow_name> <build_preset> [event_name]",
            file=sys.stderr,
        )
        sys.exit(2)
    event_name = sys.argv[3] if len(sys.argv) == 4 else ""
    print(resolve_ci_job_name(sys.argv[1], sys.argv[2], event_name))
