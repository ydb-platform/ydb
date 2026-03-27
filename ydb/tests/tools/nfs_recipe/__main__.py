#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob
import logging
import os
import shutil
import tempfile

from library.python.testing.recipe import declare_recipe, set_env
import yatest.common

MOUNT_PATH_FILE = "nfs_mount_path.txt"


def _find_fake_nfs_lib():
    build_root = yatest.common.build_path()
    candidates = [
        os.path.join(build_root, "ydb/tests/tools/nfs_recipe/fake_nfs/fake_nfs.so"),
        os.path.join(build_root, "ydb/tests/tools/nfs_recipe/fake_nfs/libfake_nfs.so"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path

    search_dir = os.path.join(build_root, "ydb/tests/tools/nfs_recipe/fake_nfs")
    if os.path.isdir(search_dir):
        so_files = glob.glob(os.path.join(search_dir, "*.so"))
        if so_files:
            return so_files[0]

    return None


def start(argv):
    logging.debug("Starting NFS recipe")

    mount_path = tempfile.mkdtemp(prefix="nfs_mount_")
    with open(MOUNT_PATH_FILE, "w") as f:
        f.write(mount_path)

    set_env("NFS_MOUNT_PATH", mount_path)

    fake_nfs_lib = _find_fake_nfs_lib()
    if fake_nfs_lib:
        existing_preload = os.environ.get("LD_PRELOAD", "")
        if existing_preload:
            ld_preload = f"{fake_nfs_lib}:{existing_preload}"
        else:
            ld_preload = fake_nfs_lib
        set_env("LD_PRELOAD", ld_preload)
        logging.debug(f"NFS recipe started, mount_path={mount_path}, LD_PRELOAD={ld_preload}")
    else:
        logging.warning("fake_nfs.so not found, LD_PRELOAD not set. "
                        "Export may work (statfs bypass), but import will fail the NFS check.")
        logging.debug(f"NFS recipe started, mount_path={mount_path}, LD_PRELOAD not set")


def stop(argv):
    logging.debug("Stopping NFS recipe")
    try:
        with open(MOUNT_PATH_FILE, "r") as f:
            mount_path = f.read().strip()
        if os.path.exists(mount_path):
            shutil.rmtree(mount_path, ignore_errors=True)
    except Exception as e:
        logging.warning(f"Failed to cleanup NFS mount path: {e}")


if __name__ == "__main__":
    declare_recipe(start, stop)
