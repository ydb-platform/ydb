import hashlib
import os
import shutil
import subprocess
import sys

from .constants import (
    LOCAL_PNPM_INSTALL_MUTEX_FILENAME,
    VIRTUAL_STORE_DIRNAME,
    NODE_MODULES_WORKSPACE_BUNDLE_FILENAME,
    NPM_REGISTRY_URL,
)
from .lockfile import Lockfile
from .utils import (
    build_lockfile_path,
    build_build_backup_lockfile_path,
    build_pre_lockfile_path,
    build_ws_config_path,
    b_rooted,
    build_nm_bundle_path,
    build_nm_path,
    build_pj_path,
    build_pnpm_store_path,
    s_rooted,
)
from .pnpm_workspace import PnpmWorkspace
from .node_modules_bundler import bundle_node_modules
from .timeit import timeit
from .package_json import PackageJson


class PackageManagerError(RuntimeError):
    pass


class PackageManagerCommandError(PackageManagerError):
    def __init__(self, cmd, code, stdout, stderr):
        self.cmd = cmd
        self.code = code
        self.stdout = stdout
        self.stderr = stderr

        msg = "package manager exited with code {} while running {}:\n{}\n{}".format(code, cmd, stdout, stderr)
        super(PackageManagerCommandError, self).__init__(msg)


"""
Creates a decorator that synchronizes access to a function using a mutex file.

The decorator uses file locking (fcntl.LOCK_EX) to ensure only one process can execute the decorated function at a time.
The lock is released (fcntl.LOCK_UN) when the function completes.

Args:
    mutex_filename (str): Path to the file used as a mutex lock.

Returns:
    function: A decorator function that applies the synchronization logic.
"""


def sync_mutex_file(mutex_filename):
    def decorator(function):
        def wrapper(*args, **kwargs):
            import fcntl

            with open(mutex_filename, "w+") as mutex:
                fcntl.lockf(mutex, fcntl.LOCK_EX)
                result = function(*args, **kwargs)
                fcntl.lockf(mutex, fcntl.LOCK_UN)

            return result

        return wrapper

    return decorator


"""
Calculates the MD5 hash of multiple files.

Reads files in chunks of 64KB and updates the MD5 hash incrementally. Files are processed in sorted order to ensure consistent results.

Args:
    files (list): List of file paths to be hashed.

Returns:
    str: Hexadecimal MD5 hash digest of the concatenated file contents.
"""


def hash_files(files):
    BUF_SIZE = 65536  # read in 64kb chunks
    md5 = hashlib.md5()
    for filename in sorted(files):
        with open(filename, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)

    return md5.hexdigest()


"""
Creates a decorator that runs the decorated function only if specified files have changed.

The decorator checks the hash of provided files against a saved hash from previous runs.
If hashes differ (files changed) or no saved hash exists, runs the decorated function
and updates the saved hash. If hashes are the same, skips the function execution.

Args:
    files_to_hash: List of files to track for changes.
    hash_storage_filename: Path to file where hash state is stored.

Returns:
    A decorator function that implements the described behavior.
"""


def hashed_by_files(files_to_hash, paths_to_exist, hash_storage_filename):
    def decorator(function):
        def wrapper(*args, **kwargs):
            all_paths_exist = True
            for p in paths_to_exist:
                if not os.path.exists(p):
                    all_paths_exist = False
                    break

            current_state_hash = hash_files(files_to_hash)
            saved_hash = None
            if all_paths_exist and os.path.exists(hash_storage_filename):
                with open(hash_storage_filename, "r") as f:
                    saved_hash = f.read()

            if saved_hash == current_state_hash:
                return None
            else:
                result = function(*args, **kwargs)
                with open(hash_storage_filename, "w+") as f:
                    f.write(current_state_hash)

            return result

        return wrapper

    return decorator


class PackageManager(object):
    def __init__(
        self,
        build_root,
        build_path,
        sources_path,
        nodejs_bin_path,
        script_path,
        module_path=None,
        sources_root=None,
        inject_peers=False,
        verbose=False,
    ):
        self.module_path = build_path[len(build_root) + 1 :] if module_path is None else module_path
        self.build_path = build_path
        self.sources_path = sources_path
        self.build_root = build_root
        self.sources_root = sources_path[: -len(self.module_path) - 1] if sources_root is None else sources_root
        self.nodejs_bin_path = nodejs_bin_path
        self.script_path = script_path
        self.inject_peers = inject_peers
        self.verbose = verbose

    @classmethod
    def load_package_json(cls, path):
        """
        :param path: path to package.json
        :type path: str
        :rtype: PackageJson
        """
        return PackageJson.load(path)

    @classmethod
    def load_package_json_from_dir(cls, dir_path, empty_if_missing=False):
        """
        :param dir_path: path to directory with package.json
        :type dir_path: str
        :rtype: PackageJson
        """
        pj_path = build_pj_path(dir_path)
        if empty_if_missing and not os.path.exists(pj_path):
            pj = PackageJson(pj_path)
            pj.data = {}
            return pj
        return cls.load_package_json(pj_path)

    def _build_package_json(self):
        """
        :rtype: PackageJson
        """
        pj = self.load_package_json_from_dir(self.sources_path)

        if not os.path.exists(self.build_path):
            os.makedirs(self.build_path, exist_ok=True)

        pj.path = build_pj_path(self.build_path)
        pj.write()

        return pj

    @classmethod
    def load_lockfile(cls, path):
        """
        :param path: path to lockfile
        :type path: str
        :rtype: Lockfile
        """
        return Lockfile.load(path)

    @classmethod
    def load_lockfile_from_dir(cls, dir_path):
        """
        :param dir_path: path to directory with lockfile
        :type dir_path: str
        :rtype: Lockfile
        """
        return cls.load_lockfile(build_lockfile_path(dir_path))

    def get_local_peers_from_package_json(self):
        """
        Returns paths of direct workspace dependencies (source root related).
        :rtype: list of str
        """
        return self.load_package_json_from_dir(self.sources_path).get_workspace_dep_paths(base_path=self.module_path)

    @timeit
    def _exec_command(self, args, cwd: str, include_defaults=True, script_path=None, env={}):
        if not self.nodejs_bin_path:
            raise PackageManagerError("Unable to execute command: nodejs_bin_path is not configured")

        cmd = (
            [self.nodejs_bin_path, script_path or self.script_path]
            + args
            + (self._get_default_options() if include_defaults else [])
        )
        p = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdin=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
            encoding="utf-8",
        )
        stdout, stderr = p.communicate()

        if self.verbose:
            print(f'cd {cwd} && {" ".join(cmd)}', file=sys.stderr)
            print(f'stdout: {stdout}', file=sys.stderr) if stdout else None
            print(f'stderr: {stderr}', file=sys.stderr) if stderr else None

        if p.returncode != 0:
            self._dump_debug_log()

            raise PackageManagerCommandError(cmd, p.returncode, stdout, stderr)

    def _nm_path(self, *parts):
        return os.path.join(build_nm_path(self.build_path), *parts)

    def _tarballs_store_path(self, pkg, store_path):
        return os.path.join(self.module_path, store_path, pkg.tarball_path)

    def _get_default_options(self):
        return ["--registry", NPM_REGISTRY_URL, "--stream", "--reporter", "append-only", "--no-color"]

    def _get_debug_log_path(self):
        return self._nm_path(".pnpm-debug.log")

    def _dump_debug_log(self):
        log_path = self._get_debug_log_path()

        if not log_path:
            return

        try:
            with open(log_path) as f:
                sys.stderr.write("Package manager log {}:\n{}\n".format(log_path, f.read()))
        except Exception:
            sys.stderr.write("Failed to dump package manager log {}.\n".format(log_path))

    @timeit
    def _get_pnpm_store(self):
        return build_pnpm_store_path(self.build_root)

    @timeit
    def _get_file_hash(self, path: str):
        sha256 = hashlib.sha256()

        with open(path, "rb") as f:
            # Read the file in chunks
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    @timeit
    def create_node_modules(
        self,
        yatool_prebuilder_path=None,
        use_legacy_pnpm_virtual_store=False,
        local_cli=False,
        nm_bundle=False,
        original_lf_path=None,
    ):
        """
        Creates node_modules directory according to the lockfile.
        """
        ws = self._prepare_workspace(local_cli)

        self._copy_pnpm_patches()

        # Pure `tier 0` logic - isolated stores in the `build_root` (works in `distbuild` and `CI autocheck`)
        store_dir = self._get_pnpm_store()
        virtual_store_dir = (
            self._nm_path(VIRTUAL_STORE_DIRNAME) if self.inject_peers or use_legacy_pnpm_virtual_store else None
        )
        global_virtual_store_dir = os.path.join(store_dir, "v10", "links")

        self._run_pnpm_install(store_dir, self.build_path, local_cli, virtual_store_dir, self.inject_peers)

        self._run_apply_addons_if_need(yatool_prebuilder_path, virtual_store_dir or global_virtual_store_dir)
        self._restore_original_lockfile(original_lf_path, virtual_store_dir)

        if nm_bundle:
            # TODO: how to bundle node_modules with GVS?
            bundle_node_modules(
                build_root=self.build_root,
                node_modules_path=self._nm_path(),
                peers=ws.get_paths(base_path=self.module_path, ignore_self=True),
                bundle_path=os.path.join(self.build_path, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME),
            )

    """
    Runs pnpm install command with specified parameters in an exclusive and hashed manner.

    This method executes the pnpm install command with various flags and options, ensuring it's run exclusively
    using a mutex file and only if the specified files have changed (using a hash check). The command is executed
    in the given working directory (cwd) with the provided store and virtual store directories.

    Args:
        store_dir (str): Path to the store directory where packages will be stored.
        cwd (str): Working directory where the command will be executed.

    Note:
        Uses file locking via fcntl to ensure exclusive execution.
        The command execution is hashed based on the pnpm-lock.yaml file.
    """

    @timeit
    def _run_pnpm_install(
        self, store_dir: str, cwd: str, local_cli: bool, virtual_store_dir: str | None, inject_peers: bool
    ):
        # Use fcntl to lock a temp file

        def execute_install_cmd():
            install_cmd = [
                "install",
                "--frozen-lockfile",
                "--ignore-pnpmfile",
                "--ignore-scripts",
                "--no-verify-store-integrity",
                "--prefer-offline" if local_cli else "--offline",
                "--config.confirmModulesPurge=false",  # hack for https://st.yandex-team.ru/FBP-1295
                "--config.preferSymlinkedExecutables=true",
                "--package-import-method",
                "hardlink",
                # "--registry" will be set later inside self._exec_command()
                "--store-dir",
                store_dir,
                "--strict-peer-dependencies",
            ]

            if virtual_store_dir:
                install_cmd.extend(["--virtual-store-dir", virtual_store_dir])
            else:
                install_cmd.extend(["--config.enableGlobalVirtualStore=true"])

            if inject_peers:
                install_cmd.extend(
                    ["--config.injectWorkspacePackages=true", "--config.sharedWorkspaceLockfile=false", "--filter", "."]
                )

            self._exec_command(install_cmd, cwd=cwd)

        mutex_file = os.path.join(store_dir, LOCAL_PNPM_INSTALL_MUTEX_FILENAME)
        os.makedirs(os.path.dirname(mutex_file), exist_ok=True)
        execute_hashed_cmd_exclusively = sync_mutex_file(mutex_file)(execute_install_cmd)
        execute_hashed_cmd_exclusively()

    """
    Calculate inputs, outputs and resources for dependency preparation phase.

    Args:
        store_path: Path to the store where tarballs will be stored.
        has_deps: Boolean flag indicating whether the module has dependencies.

    Returns:
        tuple[list[str], list[str], list[str]]: A tuple containing three lists:
            - ins: List of input file paths
            - outs: List of output file paths
            - resources: List of package URIs (when has_deps is True)

    Note:
        Uses @timeit decorator to measure execution time of this method.
    """

    @timeit
    def calc_prepare_deps_inouts_and_resources(
        self, store_path: str, has_deps: bool, local_cli: bool
    ) -> tuple[list[str], list[str], list[str]]:
        ins = [
            s_rooted(build_pj_path(self.module_path)),
            s_rooted(build_lockfile_path(self.module_path)),
        ]
        outs = [
            b_rooted(build_ws_config_path(self.module_path)),
            b_rooted(build_pre_lockfile_path(self.module_path)),
        ]
        resources = []

        if has_deps and not local_cli:
            for pkg in self.extract_packages_meta_from_lockfiles([build_lockfile_path(self.sources_path)]):
                resources.append(pkg.to_uri())
                outs.append(b_rooted(self._tarballs_store_path(pkg, store_path)))

        return ins, outs, resources

    @timeit
    def calc_node_modules_inouts(self, nm_bundle: bool) -> tuple[list[str], list[str]]:
        """
        Returns input and optionally output paths for command that creates `node_modules` bundle.
        It relies on .PEERDIRSELF=TS_PREPARE_DEPS
        Inputs:
            - source package.json
        Outputs:
            - node_modules bundle if `nm_bundle` is True else empty list
        """
        ins = [s_rooted(build_pj_path(self.module_path))]
        outs = []

        if nm_bundle:
            outs.append(b_rooted(build_nm_bundle_path(self.module_path)))

        return ins, outs

    @timeit
    def extract_packages_meta_from_lockfiles(self, lf_paths):
        """
        :type lf_paths: iterable of BaseLockfile
        :rtype: iterable of LockfilePackageMeta
        """
        tarballs = set()
        errors = []

        for lf_path in lf_paths:
            try:
                for pkg in self.load_lockfile(lf_path).get_packages_meta():
                    if pkg.tarball_path not in tarballs:
                        tarballs.add(pkg.tarball_path)
                        yield pkg
            except Exception as e:
                errors.append("{}: {}".format(lf_path, e))

        if errors:
            raise PackageManagerError("Unable to process some lockfiles:\n{}".format("\n".join(errors)))

    @timeit
    def _prepare_workspace(self, local_cli: bool):
        if local_cli:
            shutil.copy(build_pre_lockfile_path(self.build_path), build_lockfile_path(self.build_path))
        else:
            lf = self.load_lockfile(build_pre_lockfile_path(self.build_path))

            lf.update_tarball_resolutions(lambda p: "file:" + os.path.join(self.build_root, p.tarball_url))
            lf.write(build_lockfile_path(self.build_path))

        return PnpmWorkspace.load(build_ws_config_path(self.build_path))

    @timeit
    def build_workspace(self, tarballs_store: str, local_cli: bool):
        """
        :rtype: PnpmWorkspace
        """
        pj = self._build_package_json()

        ws = PnpmWorkspace(build_ws_config_path(self.build_path))
        ws.set_from_package_json(pj)

        dep_paths = ws.get_paths(ignore_self=True)
        self._build_merged_workspace_config(ws, dep_paths)
        self._build_merged_pre_lockfile(tarballs_store, dep_paths, local_cli)

        return ws

    @timeit
    def build_ts_proto_auto_workspace(self, deps_mod: str):
        """
        :rtype: PnpmWorkspace
        """

        ws = PnpmWorkspace(build_ws_config_path(self.build_path))
        ws.packages.add(".")
        ws.write()

        deps_pre_lockfile_path = build_pre_lockfile_path(os.path.join(self.build_root, deps_mod))
        pre_lockfile_path = build_pre_lockfile_path(self.build_path)
        shutil.copyfile(deps_pre_lockfile_path, pre_lockfile_path)

        return ws

    @timeit
    def _build_merged_pre_lockfile(self, tarballs_store, dep_paths, local_cli: bool):
        """
        :type dep_paths: list of str
        :rtype: PnpmLockfile
        """
        lf = self.load_lockfile_from_dir(self.sources_path)
        # Change to the output path for correct path calcs on merging.
        lf.path = build_pre_lockfile_path(self.build_path)
        if not local_cli:
            lf.update_tarball_resolutions(lambda p: self._tarballs_store_path(p, tarballs_store))

        if self.inject_peers is False:
            for dep_path in dep_paths:
                pre_lf_path = build_pre_lockfile_path(dep_path)
                if os.path.isfile(pre_lf_path):
                    lf.merge(self.load_lockfile(pre_lf_path))

        lf.write()

    @timeit
    def _build_merged_workspace_config(self, ws, dep_paths):
        """
        NOTE: This method mutates `ws`.
        :type ws: PnpmWorkspaceConfig
        :type dep_paths: list of str
        """
        for dep_path in dep_paths:
            ws_config_path = build_ws_config_path(dep_path)
            if os.path.isfile(ws_config_path):
                ws.merge(PnpmWorkspace.load(ws_config_path))

        ws.write()

    @timeit
    def _run_apply_addons_if_need(self, yatool_prebuilder_path, virtual_store_dir):
        if not yatool_prebuilder_path:
            return

        self._exec_command(
            [
                "apply-addons",
                "--virtual-store",
                virtual_store_dir,
            ],
            cwd=self.build_path,
            include_defaults=False,
            script_path=os.path.join(yatool_prebuilder_path, "build", "bin", "prebuilder.js"),
        )

    @timeit
    def _restore_original_lockfile(self, original_lf_path: str, virtual_store_dir: str | None):
        original_lf_path = original_lf_path or build_lockfile_path(self.sources_path)
        build_lf_path = build_lockfile_path(self.build_path)
        build_bkp_lf_path = build_build_backup_lockfile_path(self.build_path)

        if virtual_store_dir:
            vs_lf_path = os.path.join(virtual_store_dir, "lock.yaml")
            shutil.copyfile(original_lf_path, vs_lf_path)

        shutil.copyfile(build_lf_path, build_bkp_lf_path)
        shutil.copyfile(original_lf_path, build_lf_path)

    @timeit
    def _copy_pnpm_patches(self):
        pj = self.load_package_json_from_dir(self.build_path)
        patched_dependencies: dict[str, str] = pj.data.get("pnpm", {}).get("patchedDependencies", {})

        for p in patched_dependencies.values():
            patch_source_path = os.path.join(self.sources_path, p)
            patch_build_path = os.path.join(self.build_path, p)
            os.makedirs(os.path.dirname(patch_build_path), exist_ok=True)
            shutil.copyfile(patch_source_path, patch_build_path)
