import os

from ..base.utils import home_dir
from .constants import (
    PNPM_BUILD_BACKUP_LOCKFILE_FILENAME,
    PNPM_PRE_LOCKFILE_FILENAME,
    PNPM_LOCKFILE_FILENAME,
    PNPM_WS_FILENAME,
    STORE_DIRNAME,
)


def build_pre_lockfile_path(p):
    return os.path.join(p, PNPM_PRE_LOCKFILE_FILENAME)


def build_build_backup_lockfile_path(p):
    return os.path.join(p, PNPM_BUILD_BACKUP_LOCKFILE_FILENAME)


def build_lockfile_path(p):
    return os.path.join(p, PNPM_LOCKFILE_FILENAME)


def build_ws_config_path(p):
    return os.path.join(p, PNPM_WS_FILENAME)


def build_pnpm_store_path() -> str:
    return os.path.join(os.getenv("NOTS_STORE_PATH", os.path.join(home_dir(), ".nots")), STORE_DIRNAME)
