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
        return f'{os.getenv("CI_BRANCH", '')}.{os.getenv("CI_REVISION", '')[0:9]}'


def get_self_version() -> str:
    return f'{(vcs.svn_branch() if vcs.svn_branch() else vcs.svn_tag()).split('/')[-1]}.{vcs.commit_id()[0:7]}'
