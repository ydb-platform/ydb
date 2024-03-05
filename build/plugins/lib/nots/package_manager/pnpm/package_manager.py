import os
import shutil

from .lockfile import PnpmLockfile
from .utils import build_lockfile_path, build_pre_lockfile_path, build_ws_config_path
from .workspace import PnpmWorkspace
from ..base import BasePackageManager, PackageManagerError
from ..base.constants import NODE_MODULES_WORKSPACE_BUNDLE_FILENAME
from ..base.node_modules_bundler import bundle_node_modules
from ..base.utils import b_rooted, build_nm_bundle_path, build_pj_path, home_dir, s_rooted


class PnpmPackageManager(BasePackageManager):
    _STORE_NM_PATH = os.path.join(".pnpm", "store")
    _VSTORE_NM_PATH = os.path.join(".pnpm", "virtual-store")
    _STORE_VER = "v3"

    @classmethod
    def load_lockfile(cls, path):
        """
        :param path: path to lockfile
        :type path: str
        :rtype: PnpmLockfile
        """
        return PnpmLockfile.load(path)

    @classmethod
    def load_lockfile_from_dir(cls, dir_path):
        """
        :param dir_path: path to directory with lockfile
        :type dir_path: str
        :rtype: PnpmLockfile
        """
        return cls.load_lockfile(build_lockfile_path(dir_path))

    @staticmethod
    def get_local_pnpm_store():
        return os.path.join(home_dir(), ".cache", "pnpm-store")

    def create_node_modules(self, yatool_prebuilder_path=None, local_cli=False):
        """
        Creates node_modules directory according to the lockfile.
        """
        ws = self._prepare_workspace()

        # Pure `tier 0` logic - isolated stores in the `build_root` (works in `distbuild` and `CI autocheck`)
        store_dir = self._nm_path(self._STORE_NM_PATH)
        virtual_store_dir = self._nm_path(self._VSTORE_NM_PATH)

        # Local mode optimizations (run from the `ya tool nots`)
        if local_cli:
            # Use single CAS for all the projects built locally
            store_dir = self.get_local_pnpm_store()
            # It's a default value of pnpm itself. But it should be defined explicitly for not using values from the lockfiles or from the previous installations.
            virtual_store_dir = self._nm_path('.pnpm')

        install_cmd = [
            "install",
            "--frozen-lockfile",
            "--ignore-pnpmfile",
            "--ignore-scripts",
            "--no-verify-store-integrity",
            "--offline",
            "--config.confirmModulesPurge=false",  # hack for https://st.yandex-team.ru/FBP-1295
            "--package-import-method",
            "hardlink",
            # "--registry" will be set later inside self._exec_command()
            "--store-dir",
            store_dir,
            "--strict-peer-dependencies",
            "--virtual-store-dir",
            virtual_store_dir,
        ]

        self._exec_command(install_cmd)

        self._run_apply_addons_if_need(yatool_prebuilder_path, virtual_store_dir)
        self._replace_internal_lockfile_with_original(virtual_store_dir)

        if not local_cli:
            bundle_node_modules(
                build_root=self.build_root,
                node_modules_path=self._nm_path(),
                peers=ws.get_paths(base_path=self.module_path, ignore_self=True),
                bundle_path=os.path.join(self.build_path, NODE_MODULES_WORKSPACE_BUNDLE_FILENAME),
            )

    # TODO: FBP-1254
    # def calc_prepare_deps_inouts(self, store_path: str, has_deps: bool) -> (list[str], list[str]):
    def calc_prepare_deps_inouts(self, store_path, has_deps):
        ins = [
            s_rooted(build_pj_path(self.module_path)),
            s_rooted(build_lockfile_path(self.module_path)),
        ]
        outs = [
            b_rooted(build_ws_config_path(self.module_path)),
            b_rooted(build_pre_lockfile_path(self.module_path)),
        ]

        if has_deps:
            for dep_path in self.get_local_peers_from_package_json():
                ins.append(b_rooted(build_ws_config_path(dep_path)))
                ins.append(b_rooted(build_pre_lockfile_path(dep_path)))

            for pkg in self.extract_packages_meta_from_lockfiles([build_lockfile_path(self.sources_path)]):
                ins.append(b_rooted(self._contrib_tarball_path(pkg)))
                outs.append(b_rooted(self._tarballs_store_path(pkg, store_path)))

        return ins, outs

    # TODO: FBP-1254
    # def calc_node_modules_inouts(self, local_cli=False) -> (list[str], list[str]):
    def calc_node_modules_inouts(self, local_cli=False):
        """
        Returns input and output paths for command that creates `node_modules` bundle.
        It relies on .PEERDIRSELF=TS_PREPARE_DEPS
        Inputs:
            - source package.json
            - merged lockfiles and workspace configs of TS_PREPARE_DEPS
        Outputs:
            - created node_modules bundle
        """
        ins = [s_rooted(build_pj_path(self.module_path))]
        outs = []

        pj = self.load_package_json_from_dir(self.sources_path)
        if pj.has_dependencies():
            ins.append(b_rooted(build_pre_lockfile_path(self.module_path)))
            ins.append(b_rooted(build_ws_config_path(self.module_path)))
            if not local_cli:
                outs.append(b_rooted(build_nm_bundle_path(self.module_path)))
            for dep_path in self.get_local_peers_from_package_json():
                ins.append(b_rooted(build_pj_path(dep_path)))

        return ins, outs

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

    def _prepare_workspace(self):
        lf = self.load_lockfile(build_pre_lockfile_path(self.build_path))
        lf.update_tarball_resolutions(lambda p: "file:" + os.path.join(self.build_root, p.tarball_url))
        lf.write(build_lockfile_path(self.build_path))

        return PnpmWorkspace.load(build_ws_config_path(self.build_path))

    def build_workspace(self, tarballs_store):
        """
        :rtype: PnpmWorkspace
        """
        pj = self._build_package_json()

        ws = PnpmWorkspace(build_ws_config_path(self.build_path))
        ws.set_from_package_json(pj)

        dep_paths = ws.get_paths(ignore_self=True)
        self._build_merged_workspace_config(ws, dep_paths)
        self._build_merged_pre_lockfile(tarballs_store, dep_paths)

        return ws

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

    def _build_merged_pre_lockfile(self, tarballs_store, dep_paths):
        """
        :type dep_paths: list of str
        :rtype: PnpmLockfile
        """
        lf = self.load_lockfile_from_dir(self.sources_path)
        # Change to the output path for correct path calcs on merging.
        lf.path = build_pre_lockfile_path(self.build_path)
        lf.update_tarball_resolutions(lambda p: self._tarballs_store_path(p, tarballs_store))

        for dep_path in dep_paths:
            pre_lf_path = build_pre_lockfile_path(dep_path)
            if os.path.isfile(pre_lf_path):
                lf.merge(self.load_lockfile(pre_lf_path))

        lf.write()

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

    def _run_apply_addons_if_need(self, yatool_prebuilder_path, virtual_store_dir):
        if not yatool_prebuilder_path:
            return

        self._exec_command(
            [
                "apply-addons",
                "--virtual-store",
                virtual_store_dir,
            ],
            include_defaults=False,
            script_path=os.path.join(yatool_prebuilder_path, "build", "bin", "prebuilder.js"),
        )

    def _replace_internal_lockfile_with_original(self, virtual_store_dir):
        original_lf_path = build_lockfile_path(self.sources_path)
        vs_lf_path = os.path.join(virtual_store_dir, "lock.yaml")

        shutil.copyfile(original_lf_path, vs_lf_path)

    def _get_default_options(self):
        return super(PnpmPackageManager, self)._get_default_options() + [
            "--stream",
            "--reporter",
            "append-only",
            "--no-color",
        ]

    def _get_debug_log_path(self):
        return self._nm_path(".pnpm-debug.log")
