import json
import yatest.common
import os
import library.python.svn_version as vcs


def get_external_param(name: str, default):
    try:
        return yatest.common.get_param(name, default=default)
    except yatest.common.NoRuntimeFormed:
        return default


def external_param_is_true(name: str) -> bool:
    return get_external_param(name, '').lower() in ['t', 'true', 'yes', '1', 'da']


def get_ci_version() -> str:
    if 'CI_REVISION' in os.environ or 'CI_BRANCH' in os.environ:
        return f'{os.getenv("CI_BRANCH", '').replace(':', '-')}.{os.getenv("CI_REVISION", '')[0:9]}'


def get_self_version() -> str:
    return f'{(vcs.svn_branch() if vcs.svn_branch() else vcs.svn_tag()).split('/')[-1]}.{vcs.commit_id()[0:7]}'


def get_test_tools_git_info() -> dict:
    """Resolved tools version from CI (flow-vars.test_version → main / pr-N / sha)."""
    raw = os.getenv('CI_TEST_GIT_INFO') or ''
    if not raw:
        return {}
    try:
        info = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return info if isinstance(info, dict) else {}


def get_test_tools_version() -> str:
    """Human-readable tools version for Allure / results: branch.sha or fallback to binary VCS."""
    info = get_test_tools_git_info()
    branch = info.get('branch') or ''
    sha = info.get('version') or ''
    requested = os.getenv('CI_TEST_VERSION') or ''
    if branch and sha:
        return f'{branch}.{sha[:7]}'
    if sha:
        return sha[:9]
    if requested:
        return requested
    return get_self_version()
