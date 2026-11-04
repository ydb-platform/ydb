import os
import shutil
import stat

from build.plugins.lib.nots.package_manager import PackageJson
from build.plugins.lib.nots.package_manager.utils import build_pj_path
from build.plugins.lib.nots.typescript import DEFAULT_TS_CONFIG_FILE, TsConfig


def copy_dir_contents(src_dir, dest_dir, ignore_list: list[str] = None, skip_links=True):
    """Copy src_dir directory content to dest_dir with write permissions

    Args:
        src_dir (path): Source directory
        dest_dir (path): Destination directory
        ignore_list (list, optional): Top level items to be ignored. Defaults to [].
        skip_links (bool, optional): Ignore top level links. Defaults to True.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for entry in os.listdir(src_dir):
        if entry in (ignore_list or []):
            continue

        src = os.path.join(src_dir, entry)
        dst = os.path.normpath(os.path.join(dest_dir, entry))

        if os.path.islink(src) and skip_links:
            continue

        if os.path.isdir(src):
            copy_dir_contents(src, dst)

        if os.path.isfile(src) and not os.path.exists(dst):
            shutil.copy(src, dst)
            os.chmod(dst, os.stat(dst).st_mode | stat.S_IWRITE)


def create_bin_tsconfig(module_arc_path, source_root, bin_root, ts_config_path=DEFAULT_TS_CONFIG_FILE):
    """
    Creating a tsconfig.json config file inlining the required base files if any
    """
    source_path = os.path.join(source_root, module_arc_path)
    bin_path = os.path.join(bin_root, module_arc_path)

    ts_config = TsConfig.load(os.path.join(source_path, ts_config_path), source_path)
    pj = PackageJson.load(build_pj_path(source_path))
    ts_config.inline_extend(pj.get_dep_paths_by_names())

    bin_ts_config_path = os.path.join(bin_path, ts_config_path)
    ts_config.write(bin_ts_config_path, indent=4)
    return bin_ts_config_path
