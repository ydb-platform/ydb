import socket
import subprocess
import sys

from testsuite.utils import subprocess_helper

BASE_BRANCH = 'develop'
UPSTREAM_REMOTES = ('upstream', 'origin')


def pytest_addoption(parser):
    parser.addoption(
        '--envinfo-no-git',
        action='store_true',
        help='Do not print git-related information in test report header.',
    )


def pytest_report_header(config):
    headers = [
        'args: {}'.format(' '.join(sys.argv)),
        f'hostname: {socket.gethostname()}',
    ]
    if not config.option.envinfo_no_git:
        headers.extend(get_vcs_info())
    return headers


def get_vcs_info() -> list[str]:
    try:
        commit = subprocess_helper.sh('git', 'rev-parse', 'HEAD')
        branch = subprocess_helper.sh(
            'git',
            'rev-parse',
            '--abbrev-ref',
            'HEAD',
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    merge_base = git_merge_base()
    items = [f'branch {branch}']
    if git_is_clean():
        items.append(commit)
    else:
        items.append(f'{commit}*')
    if merge_base:
        items.append(f'base {merge_base}')
    return ['git: ' + ', '.join(items)]


def git_is_clean() -> bool:
    try:
        subprocess_helper.sh(
            'git',
            'diff',
            '--ignore-submodules=dirty',
            '--quiet',
        )
    except subprocess.CalledProcessError:
        return False
    return True


def git_merge_base() -> str | None:
    """Try to guess merge base for current commit."""
    try:
        remotes = set(subprocess_helper.sh('git', 'remote').splitlines())
        for remote in UPSTREAM_REMOTES:
            if remote in remotes:
                return subprocess_helper.sh(
                    'git',
                    'merge-base',
                    f'{remote}/{BASE_BRANCH}',
                    'HEAD',
                )
    except subprocess.CalledProcessError:
        pass
    return None
