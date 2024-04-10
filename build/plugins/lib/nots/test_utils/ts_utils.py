import os
import errno

from build.plugins.lib.nots.package_manager.base import PackageJson
from build.plugins.lib.nots.package_manager.base.utils import build_pj_path
from build.plugins.lib.nots.typescript import TsConfig, DEFAULT_TS_CONFIG_FILE


def link_test_data(build_root, source_root, test_for_path, dirs, dirs_rename):
    if not len(dirs):
        return

    dirs_rename_map = build_dirs_rename_map(dirs_rename) if dirs_rename else []
    rel_test_for_path = os.path.relpath(test_for_path, build_root)
    rel_dirs = [os.path.join(os.path.relpath(dir, rel_test_for_path), "") for dir in dirs]
    renamed_build_dirs = rel_dirs

    def rename_dir(dir):
        renamed_dir = dir

        for kv in dirs_rename_map:
            rename_from, rename_to = kv
            if not dir.startswith(rename_from):
                continue

            renamed_dir = renamed_dir.replace(rename_from, rename_to, 1)
            break

        return renamed_dir

    if len(dirs_rename_map):
        renamed_build_dirs = map(rename_dir, renamed_build_dirs)

    dirs_map = {renamed_build_dirs[i]: rel_dirs[i] for i in range(len(rel_dirs))}
    symlink_build_dirs = get_top_level_dirs(renamed_build_dirs)

    for build_dir in symlink_build_dirs:
        src_dir = dirs_map[build_dir]
        abs_dir_source_path = os.path.join(source_root, rel_test_for_path, src_dir.rstrip(os.sep))
        abs_dir_build_path = os.path.join(build_root, rel_test_for_path, build_dir.rstrip(os.sep))

        try:
            os.makedirs(os.path.dirname(abs_dir_build_path))
        except OSError:
            pass

        try:
            os.symlink(abs_dir_source_path, abs_dir_build_path)
        except OSError as e:
            if e.errno == errno.EEXIST:
                msg = (
                    "Unable to create symlink to test data from \"{}\" to \"{}\". Directory in bindir is already created. "
                    "Try to save test data in a separate folder".format(abs_dir_source_path, abs_dir_build_path)
                )
                raise ValueError(msg)
            raise


def build_dirs_rename_map(dirs_rename):
    dirs_rename_map = []

    for from_to in filter(None, dirs_rename.split(";")):
        rename_from, rename_to = [
            os.path.join(v.replace("\\", os.sep).replace("/", os.sep), "") for v in from_to.split(":")
        ]
        dirs_rename_map.append([rename_from, rename_to])

    return dirs_rename_map


def get_top_level_dirs(dirs):
    sorted_dirs = sorted(dirs, key=len)
    top_level_dirs = set()

    for idx, dir in enumerate(sorted_dirs):
        if idx == 0:
            top_level_dirs.add(dir)
            continue

        if not any(dir.startswith(tld) for tld in top_level_dirs):
            top_level_dirs.add(dir)

    return top_level_dirs


def create_bin_tsconfig(module_arc_path, source_root, bin_root):
    """
    Creating a tsconfig.json config file inlining the required base files if any
    """
    source_path = os.path.join(source_root, module_arc_path)
    bin_path = os.path.join(bin_root, module_arc_path)

    ts_config = TsConfig.load(os.path.join(source_path, DEFAULT_TS_CONFIG_FILE))
    pj = PackageJson.load(build_pj_path(source_path))
    ts_config.inline_extend(pj.get_dep_paths_by_names())

    bin_ts_config_path = os.path.join(bin_path, DEFAULT_TS_CONFIG_FILE)
    ts_config.write(bin_ts_config_path, indent=4)
    return bin_ts_config_path
