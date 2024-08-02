import os

from .npm_constants import NPM_LOCKFILE_FILENAME, NPM_PRE_LOCKFILE_FILENAME


def build_pre_lockfile_path(p):
    return os.path.join(p, NPM_PRE_LOCKFILE_FILENAME)


def build_lockfile_path(p):
    return os.path.join(p, NPM_LOCKFILE_FILENAME)
