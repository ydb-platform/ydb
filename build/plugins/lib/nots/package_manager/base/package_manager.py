import os
import subprocess
import sys
from abc import ABCMeta, abstractmethod

from .constants import NPM_REGISTRY_URL
from .package_json import PackageJson
from .utils import build_nm_path, build_pj_path
from .timeit import timeit, is_timeit_enabled


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


class BasePackageManager(object, metaclass=ABCMeta):
    def __init__(
        self,
        build_root,
        build_path,
        sources_path,
        nodejs_bin_path,
        script_path,
        module_path=None,
        sources_root=None,
    ):
        self.module_path = build_path[len(build_root) + 1 :] if module_path is None else module_path
        self.build_path = build_path
        self.sources_path = sources_path
        self.build_root = build_root
        self.sources_root = sources_path[: -len(self.module_path) - 1] if sources_root is None else sources_root
        self.nodejs_bin_path = nodejs_bin_path
        self.script_path = script_path

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
    @abstractmethod
    def load_lockfile(cls, path):
        pass

    @classmethod
    @abstractmethod
    def load_lockfile_from_dir(cls, dir_path):
        pass

    @abstractmethod
    def create_node_modules(self, yatool_prebuilder_path=None, local_cli=False, nm_bundle=False):
        pass

    @abstractmethod
    def extract_packages_meta_from_lockfiles(self, lf_paths):
        pass

    @abstractmethod
    def calc_prepare_deps_inouts_and_resources(
        self, store_path: str, has_deps: bool
    ) -> tuple[list[str], list[str], list[str]]:
        pass

    @abstractmethod
    def calc_node_modules_inouts(self, nm_bundle: bool) -> tuple[list[str], list[str]]:
        pass

    @abstractmethod
    def build_workspace(self, tarballs_store: str):
        pass

    @abstractmethod
    def build_ts_proto_auto_workspace(self, deps_mod: str):
        pass

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
        p = subprocess.Popen(cmd, cwd=cwd, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()

        if is_timeit_enabled():
            print(f'cd {cwd} && {" ".join(cmd)}', file=sys.stderr)
            print(f'stdout: {stdout.decode("utf-8")}', file=sys.stderr)
            print(f'stderr: {stderr.decode("utf-8")}', file=sys.stderr)

        if p.returncode != 0:
            self._dump_debug_log()

            raise PackageManagerCommandError(cmd, p.returncode, stdout.decode("utf-8"), stderr.decode("utf-8"))

    def _nm_path(self, *parts):
        return os.path.join(build_nm_path(self.build_path), *parts)

    def _tarballs_store_path(self, pkg, store_path):
        return os.path.join(self.module_path, store_path, pkg.tarball_path)

    def _get_default_options(self):
        return ["--registry", NPM_REGISTRY_URL]

    def _get_debug_log_path(self):
        return None

    def _dump_debug_log(self):
        log_path = self._get_debug_log_path()

        if not log_path:
            return

        try:
            with open(log_path) as f:
                sys.stderr.write("Package manager log {}:\n{}\n".format(log_path, f.read()))
        except Exception:
            sys.stderr.write("Failed to dump package manager log {}.\n".format(log_path))
