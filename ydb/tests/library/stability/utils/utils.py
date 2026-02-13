from typing import Any, Optional
from library.python import resource
import yatest.common
import stat
import os
import library.python.svn_version as vcs


def get_external_param(name: str, default: Any) -> Any:
    """Get external test parameter with fallback to default.

    Args:
        name: Parameter name
        default: Default value if parameter not found

    Returns:
        Parameter value or default
    """
    try:
        return yatest.common.get_param(name, default=default)
    except yatest.common.NoRuntimeFormed:
        return default


def external_param_is_true(name: str) -> bool:
    """Check if external parameter evaluates to True.

    Args:
        name: Parameter name

    Returns:
        bool: True if parameter value is truthy (t/true/yes/1/da)
    """
    return get_external_param(name, '').lower() in ['t', 'true', 'yes', '1', 'da']


def get_ci_version() -> Optional[str]:
    """Get CI version string from environment variables.

    Returns:
        Optional[str]: Version string in format "branch.commit_hash" or None
    """
    if 'CI_REVISION' in os.environ or 'CI_BRANCH' in os.environ:
        return f'{os.getenv("CI_BRANCH", '').replace(':', '-')}.{os.getenv("CI_REVISION", '')[0:9]}'


def get_self_version() -> str:
    """Get current test tool version from VCS.

    Returns:
        str: Version string in format "branch.commit_hash"
    """
    return f'{(vcs.svn_branch() if vcs.svn_branch() else vcs.svn_tag()).split('/')[-1]}.{vcs.commit_id()[0:7]}'


def unpack_resource(name, target_path):
    res = resource.find(name)
    with open(target_path, "wb") as f:
        f.write(res)

    st = os.stat(target_path)
    os.chmod(target_path, st.st_mode | stat.S_IEXEC)
