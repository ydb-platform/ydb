import os

from ..base import BasePackageManager, PackageManagerError
from ..base.constants import NODE_MODULES_WORKSPACE_BUNDLE_FILENAME
from ..base.node_modules_bundler import bundle_node_modules
from ..base.utils import b_rooted, build_nm_bundle_path, build_pj_path, s_rooted

from .npm_lockfile import NpmLockfile
from .npm_utils import build_lockfile_path, build_pre_lockfile_path


class NpmPackageManager(BasePackageManager):
    @classmethod
    def load_lockfile(cls, path):
        """
        :param path: path to lockfile
        :type path: str
        :rtype: NpmLockfile
        """
        return NpmLockfile.load(path)

    @classmethod
    def load_lockfile_from_dir(cls, dir_path):
        """
        :param dir_path: path to directory with lockfile
        :type dir_path: str
        :rtype: NpmLockfile
        """
        return cls.load_lockfile(build_lockfile_path(dir_path))

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

    def calc_prepare_deps_inouts(self, store_path: str, has_deps: bool) -> tuple[list[str], list[str]]:
        raise NotImplementedError("NPM does not support contrib/typescript flow.")

    def calc_prepare_deps_inouts_and_resources(
        self, store_path: str, has_deps: bool
    ) -> tuple[list[str], list[str], list[str]]:
        ins = [
            s_rooted(build_pj_path(self.module_path)),
            s_rooted(build_lockfile_path(self.module_path)),
        ]
        outs = [
            b_rooted(build_pre_lockfile_path(self.module_path)),
        ]
        resources = []

        if has_deps:
            for dep_path in self.get_local_peers_from_package_json():
                ins.append(b_rooted(build_pre_lockfile_path(dep_path)))

            for pkg in self.extract_packages_meta_from_lockfiles([build_lockfile_path(self.sources_path)]):
                resources.append(pkg.to_uri())
                outs.append(b_rooted(self._tarballs_store_path(pkg, store_path)))

        return ins, outs, resources

    def calc_node_modules_inouts(self, local_cli=False) -> tuple[list[str], list[str]]:
        """
        Returns input and output paths for command that creates `node_modules` bundle.
        It relies on .PEERDIRSELF=TS_PREPARE_DEPS
        Inputs:
            - source package.json
            - merged pre-lockfiles and workspace configs of TS_PREPARE_DEPS
        Outputs:
            - created node_modules bundle
        """
        ins = [s_rooted(build_pj_path(self.module_path))]
        outs = []

        pj = self.load_package_json_from_dir(self.sources_path)
        if pj.has_dependencies():
            ins.append(b_rooted(build_pre_lockfile_path(self.module_path)))
            if not local_cli:
                outs.append(b_rooted(build_nm_bundle_path(self.module_path)))
            for dep_path in self.get_local_peers_from_package_json():
                ins.append(b_rooted(build_pj_path(dep_path)))

        return ins, outs

    def build_workspace(self, tarballs_store: str):
        self._build_pre_lockfile(tarballs_store)

    def _build_pre_lockfile(self, tarballs_store: str):
        lf = self.load_lockfile_from_dir(self.sources_path)
        # Change to the output path for correct path calcs on merging.
        lf.path = build_pre_lockfile_path(self.build_path)
        lf.update_tarball_resolutions(lambda p: self._tarballs_store_path(p, tarballs_store))

        lf.write()

    def create_node_modules(self, yatool_prebuilder_path=None, local_cli=False, bundle=True):
        """
        Creates node_modules directory according to the lockfile.
        """
        self._prepare_workspace()

        install_cmd = ["clean-install", "--ignore-scripts", "--audit=false"]

        env = os.environ.copy()
        env.update({"NPM_CONFIG_CACHE": os.path.join(self.build_path, ".npm-cache")})

        self._exec_command(install_cmd, env=env)

        if not local_cli and bundle:
            bundle_node_modules(
                build_root=self.build_root,
                node_modules_path=self._nm_path(),
                peers=[],
                bundle_path=os.path.join(self.build_path, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME),
            )

    def _prepare_workspace(self):
        lf = self.load_lockfile(build_pre_lockfile_path(self.build_path))
        lf.update_tarball_resolutions(lambda p: "file:" + os.path.join(self.build_root, p.tarball_url))
        lf.write(build_lockfile_path(self.build_path))
