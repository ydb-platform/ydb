import os

from .constants import PNPM_PRE_LOCKFILE_FILENAME, PNPM_LOCKFILE_FILENAME, PNPM_WS_FILENAME


def build_pre_lockfile_path(p):
    return os.path.join(p, PNPM_PRE_LOCKFILE_FILENAME)


def build_lockfile_path(p):
    return os.path.join(p, PNPM_LOCKFILE_FILENAME)


def build_ws_config_path(p):
    return os.path.join(p, PNPM_WS_FILENAME)
