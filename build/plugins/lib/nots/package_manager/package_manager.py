import os

from .lockfile import Lockfile
from .package_json import PackageJson
from .timeit import timeit
from .utils import (
    b_rooted,
    build_lockfile_path,
    build_pj_path,
    build_ws_config_path,
    s_rooted,
)


class PackageManagerError(RuntimeError):
    pass


class PackageManager(object):
    def __init__(
        self,
        build_root,
        build_path,
        sources_path,
        module_path=None,
        sources_root=None,
        inject_peers=False,
    ):
        self.module_path = build_path[len(build_root) + 1 :] if module_path is None else module_path
        self.build_path = build_path
        self.sources_path = sources_path
        self.build_root = build_root
        self.sources_root = sources_path[: -len(self.module_path) - 1] if sources_root is None else sources_root
        self.inject_peers = inject_peers

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

    def _tarballs_store_path(self, pkg, store_path):
        return os.path.join(self.module_path, store_path, pkg.tarball_path)

    @timeit
    def calc_prepare_deps_inouts_and_resources(
        self,
        store_path: str,
        has_deps: bool,
        local_cli: bool,
    ) -> tuple[list[str], list[str], list[str]]:
        ins = [
            s_rooted(build_pj_path(self.module_path)),
        ]
        if has_deps or os.path.exists(build_lockfile_path(self.sources_path)):
            ins.append(s_rooted(build_lockfile_path(self.module_path)))
        outs = [
            b_rooted(build_pj_path(self.module_path)),
            b_rooted(build_ws_config_path(self.module_path)),
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
            from .utils import build_nm_bundle_path

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
