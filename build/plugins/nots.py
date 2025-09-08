import re
import os
from enum import auto, StrEnum
from typing import Any, Literal, TYPE_CHECKING

# noinspection PyUnresolvedReferences
import ymake

import _dart_fields as df
import ytest
from _common import (
    rootrel_arc_src,
    sort_uniq,
    strip_roots,
    to_yesno,
)
from _dart_fields import create_dart_record


if TYPE_CHECKING:
    from lib.nots.erm_json_lite import ErmJsonLite
    from lib.nots.package_manager import PackageManagerType, BasePackageManager
    from lib.nots.semver import Version
    from lib.nots.typescript import TsConfig

# 1 is 60 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
# 0.5 is 120 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
# 0.2 is 300 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
# 0.0 - not to use chunks
ESLINT_FILE_PROCESSING_TIME_DEFAULT = 0.0  # seconds per file

REQUIRED_MISSING = "~~required~~"


class COLORS:
    """
    See https://en.m.wikipedia.org/wiki/ANSI_escape_code#Colors for details
    """

    @staticmethod
    def _wrap_color(color_code: int) -> str:
        return f"\033[0;{color_code}m"

    red = _wrap_color(31)
    green = _wrap_color(32)
    yellow = _wrap_color(33)
    cyan = _wrap_color(36)
    reset = _wrap_color(49)


class TsTestType(StrEnum):
    ESLINT = auto()
    HERMIONE = auto()
    JEST = auto()
    PLAYWRIGHT = auto()
    PLAYWRIGHT_LARGE = auto()
    TSC_TYPECHECK = auto()
    TS_STYLELINT = auto()


class UnitType:
    MessageType = Literal["INFO", "WARN", "ERROR"]
    PluginArgs = str | list[str] | tuple[str]

    def message(self, args: list[MessageType | str]) -> None:
        """
        Print message to the log
        """

    def get(self, var_name: str) -> str | None:
        """
        Get variable value
        """

    def set(self, args: PluginArgs) -> None:
        """
        Set variable value
        """

    def enabled(self, var_name: str) -> None:
        """
        Set variable value to "yes"
        """

    def disabled(self, var_name: str) -> None:
        """
        Set variable value to "no"
        """

    def set_property(self, args: PluginArgs) -> None:
        """
        TODO (set vs set_property?)
        """

    def resolve(self, path: str) -> str:
        """
        Resolve path TODO?
        """

    def resolve_arc_path(self, path: str) -> str:
        """
        Resolve path TODO?
        """

    def path(self) -> str:
        """
        Get the project path
        """

    def ondepends(self, deps: PluginArgs) -> None:
        """
        Run DEPENDS(...)
        """

    def onpeerdir(self, args: str | list[str]) -> None:
        """
        Run PEERDIR(...)
        """


class NotsUnitType(UnitType):
    def on_peerdir_ts_resource(self, *resources: str):
        """
        Ensure dependency installed on the project

        Also check its version (is it supported by erm)
        """

    def on_do_ts_yndexing(self) -> None:
        """
        Turn on code navigation indexing
        """

    def on_from_npm(self, args: UnitType.PluginArgs) -> None:
        """
        TODO remove after removing on_from_pnpm_lockfiles
        """

    def on_setup_install_node_modules_recipe(self) -> None:
        """
        Setup test recipe to install node_modules before running tests
        """

    def on_setup_extract_node_modules_recipe(self, args: UnitType.PluginArgs) -> None:
        """
        Setup test recipe to extract workspace-node_modules.tar before running tests
        """

    def on_setup_extract_output_tars_recipe(self, args: UnitType.PluginArgs) -> None:
        """
        Setup test recipe to extract peer's output before running tests
        """

    def on_ts_proto_auto_prepare_deps_configure(self) -> None:
        """
        Configure prepare deps for TS_PROTO_AUTO
        """


TS_TEST_FIELDS_BASE = (
    df.BinaryPath.normalized,
    df.BuildFolderPath.normalized,
    df.ForkMode.test_fork_mode,
    df.NodejsRootVarName.value,
    df.ScriptRelPath.first_flat,
    df.SourceFolderPath.normalized,
    df.SplitFactor.from_unit,
    df.TestData.from_unit,
    df.TestedProjectName.filename_without_ext,
    df.TestEnv.value,
    df.TestName.value,
    df.TestRecipes.value,
    df.TestTimeout.from_unit,
    df.TsConfigPath.from_unit,
)

TS_TEST_SPECIFIC_FIELDS = {
    TsTestType.ESLINT: (
        df.Size.from_unit,
        df.TestCwd.moddir,
        df.Tag.from_unit,
        df.Requirements.from_unit,
        df.EslintConfigPath.value,
    ),
    TsTestType.HERMIONE: (
        df.Tag.from_unit_fat_external_no_retries,
        df.Requirements.from_unit_with_full_network,
        df.ConfigPath.value,
        df.TsTestDataDirs.value,
        df.TsTestDataDirsRename.value,
        df.TsResources.value,
        df.TsTestForPath.value,
    ),
    TsTestType.JEST: (
        df.Size.from_unit,
        df.Tag.from_unit,
        df.Requirements.from_unit,
        df.ConfigPath.value,
        df.TsTestDataDirs.value,
        df.TsTestDataDirsRename.value,
        df.TsResources.value,
        df.TsTestForPath.value,
        df.DockerImage.value,
    ),
    TsTestType.PLAYWRIGHT: (
        df.Size.from_unit,
        df.Tag.from_unit,
        df.Requirements.from_unit,
        df.ConfigPath.value,
        df.TsTestDataDirs.value,
        df.TsTestDataDirsRename.value,
        df.TsResources.value,
        df.TsTestForPath.value,
        df.DockerImage.value,
    ),
    TsTestType.PLAYWRIGHT_LARGE: (
        df.ConfigPath.value,
        df.Size.from_unit,
        df.Tag.from_unit_fat_external_no_retries,
        df.Requirements.from_unit_with_full_network,
        df.TsResources.value,
        df.TsTestDataDirs.value,
        df.TsTestDataDirsRename.value,
        df.TsTestForPath.value,
    ),
    TsTestType.TSC_TYPECHECK: (
        df.Size.from_unit,
        df.TestCwd.moddir,
        df.Tag.from_unit,
        df.Requirements.from_unit,
    ),
    TsTestType.TS_STYLELINT: (
        df.Size.from_unit,
        df.TsStylelintConfig.value,
        df.TestFiles.stylesheets,
        df.NodeModulesBundleFilename.value,
    ),
}


class PluginLogger(object):
    unit: UnitType = None
    prefix = ""

    def reset(self, unit: NotsUnitType | None, prefix=""):
        self.unit = unit
        self.prefix = prefix

    def get_state(self):
        return self.unit, self.prefix

    def _stringify_messages(self, messages: tuple[Any, ...]):
        parts = []
        for m in messages:
            if m is None:
                parts.append("None")
            else:
                parts.append(m if isinstance(m, str) else repr(m))

        # cyan color (code 36) for messages
        return f"{COLORS.green}{self.prefix}{COLORS.reset}\n{COLORS.cyan}{" ".join(parts)}{COLORS.reset}"

    def info(self, *messages: Any) -> None:
        if self.unit:
            self.unit.message(["INFO", self._stringify_messages(messages)])

    def warn(self, *messages: Any) -> None:
        if self.unit:
            self.unit.message(["WARN", self._stringify_messages(messages)])

    def error(self, *messages: Any) -> None:
        if self.unit:
            self.unit.message(["ERROR", self._stringify_messages(messages)])

    def print_vars(self, *variables: str):
        if self.unit:
            values = ["{}={}".format(v, self.unit.get(v)) for v in variables]
            self.info("\n".join(values))


logger = PluginLogger()


def _parse_list_var(unit: UnitType, var_name: str, sep: str) -> list[str]:
    return [x.strip() for x in unit.get(var_name).removeprefix(f"${var_name}").split(sep) if x.strip()]


def _with_report_configure_error(fn):
    """
    Handle exceptions, report them as ymake configure error

    Also wraps plugin function like `on<macro_name>` to register `unit` in the PluginLogger
    """

    def _wrapper(*args, **kwargs):
        last_state = logger.get_state()
        unit = args[0]
        logger.reset(unit if unit.get("TS_LOG") == "yes" else None, fn.__name__)
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            ymake.report_configure_error(str(exc))
            if unit.get("TS_RAISE") == "yes":
                raise
            else:
                unit.message(["WARN", "Configure error is reported. Add -DTS_RAISE to see actual exception"])
        finally:
            logger.reset(*last_state)

    return _wrapper


def _get_var_name(s: str) -> tuple[bool, str]:
    if not s.startswith("$"):
        return False, ""

    PLAIN_VAR_PATTERN = r"^\$\w+$"
    WRAPPED_VAR_PATTERN = r"^\$\{\w+\}$"
    if re.match(PLAIN_VAR_PATTERN, s):
        return True, s[1:]
    if re.match(WRAPPED_VAR_PATTERN, s):
        return True, s[2:-1]
    return False, ""


def _is_real_file(path: str) -> bool:
    return os.path.isfile(path) and not os.path.islink(path)


def _build_directives(flags: list[str] | tuple[str], paths: list[str]) -> str:
    parts = [p for p in (flags or []) if p]
    parts_str = ";".join(parts)
    expressions = ['${{{parts}:"{path}"}}'.format(parts=parts_str, path=path) for path in paths]

    return " ".join(expressions)


def _build_cmd_input_paths(paths: list[str] | tuple[str], hide=False, disable_include_processor=False):
    hide_part = "hide" if hide else ""
    disable_ip_part = "context=TEXT" if disable_include_processor else ""
    input_part = "input=TEXT" if disable_include_processor else "input"

    return _build_directives([hide_part, disable_ip_part, input_part], paths)


def _build_cmd_output_paths(paths: list[str] | tuple[str], hide=False):
    hide_part = "hide" if hide else ""

    return _build_directives([hide_part, "output"], paths)


def _arc_path(unit: NotsUnitType, path: str) -> str:
    return unit.resolve(unit.resolve_arc_path(path))


def _create_erm_json(unit: NotsUnitType):
    from lib.nots.erm_json_lite import ErmJsonLite

    erm_packages_path = _arc_path(unit, unit.get("ERM_PACKAGES_PATH"))

    return ErmJsonLite.load(erm_packages_path)


def _get_pm_type(unit: NotsUnitType) -> 'PackageManagerType':
    resolved: PackageManagerType | None = unit.get("PM_TYPE")
    if not resolved:
        raise Exception("PM_TYPE is not set yet. Macro _SET_PACKAGE_MANAGER() should be called before.")

    return resolved


def _get_source_path(unit: NotsUnitType) -> str:
    sources_path = unit.get("TS_TEST_FOR_DIR") if unit.get("TS_TEST_FOR") else unit.path()
    return sources_path


def _create_pm(unit: NotsUnitType) -> 'BasePackageManager':
    from lib.nots.package_manager import get_package_manager_type

    sources_path = _get_source_path(unit)
    module_path = unit.get("TS_TEST_FOR_PATH") if unit.get("TS_TEST_FOR") else unit.get("MODDIR")

    # noinspection PyPep8Naming
    PackageManager = get_package_manager_type(_get_pm_type(unit))

    return PackageManager(
        sources_path=unit.resolve(sources_path),
        build_root="$B",
        build_path=unit.path().replace("$S", "$B", 1),
        nodejs_bin_path=None,
        script_path=None,
        module_path=module_path,
    )


@_with_report_configure_error
def on_set_package_manager(unit: NotsUnitType) -> None:
    pm_type = "pnpm"  # projects without any lockfile are processed by pnpm

    source_path = _get_source_path(unit)

    for pm_key, lockfile_name in [("pnpm", "pnpm-lock.yaml"), ("npm", "package-lock.json")]:
        lf_path = os.path.join(source_path, lockfile_name)
        lf_path_resolved = unit.resolve_arc_path(strip_roots(lf_path))

        if lf_path_resolved:
            pm_type = pm_key
            break

    if pm_type == 'npm' and "devtools/dummy_arcadia/typescript/npm" not in source_path:
        ymake.report_configure_error(
            "\n"
            "Project is configured to use npm as a package manager. \n"
            "Only pnpm is supported at the moment.\n"
            "Please follow the instruction to migrate your project:\n"
            "https://docs.yandex-team.ru/frontend-in-arcadia/tutorials/migrate#migrate-to-pnpm"
        )

    unit.on_peerdir_ts_resource(pm_type)
    unit.set(["PM_TYPE", pm_type])
    unit.set(["PM_SCRIPT", f"${pm_type.upper()}_SCRIPT"])


@_with_report_configure_error
def on_set_append_with_directive(unit: NotsUnitType, var_name: str, directive: str, *values: str) -> None:
    wrapped = [f'${{{directive}:"{v}"}}' for v in values]

    __set_append(unit, var_name, " ".join(wrapped))


def _check_nodejs_version(unit: NotsUnitType, major: int) -> None:
    if major < 16:
        raise Exception(
            "Node.js {} is unsupported. Update Node.js please. See https://nda.ya.ru/t/joB9Mivm6h4znu".format(major)
        )

    if major < 20:
        unit.message(
            [
                "WARN",
                "Node.js {} is deprecated. Update Node.js please. See https://nda.ya.ru/t/Yk0qYZe17DeVKP".format(major),
            ]
        )


@_with_report_configure_error
def on_peerdir_ts_resource(unit: NotsUnitType, *resources: str) -> None:
    from lib.nots.package_manager import BasePackageManager

    pj = BasePackageManager.load_package_json_from_dir(unit.resolve(_get_source_path(unit)), empty_if_missing=True)
    erm_json = _create_erm_json(unit)
    dirs = []

    nodejs_version = _select_matching_version(erm_json, "nodejs", pj.get_nodejs_version())

    _check_nodejs_version(unit, nodejs_version.major)
    for tool in resources:
        dir_name = erm_json.canonize_name(tool)
        if erm_json.use_resource_directly(tool):
            # raises the configuration error when the version is unsupported
            _select_matching_version(erm_json, tool, pj.get_dep_specifier(tool), dep_is_required=True)
        elif tool == "nodejs":
            dirs.append(os.path.join("build", "platform", dir_name, str(nodejs_version)))
            _set_resource_vars(unit, erm_json, tool, nodejs_version)
        elif erm_json.is_resource_multiplatform(tool):
            v = _select_matching_version(erm_json, tool, pj.get_dep_specifier(tool))
            sb_resources = [
                sbr for sbr in erm_json.get_sb_resources(tool, v) if sbr.get("nodejs") == nodejs_version.major
            ]
            nodejs_dir = "NODEJS_{}".format(nodejs_version.major)
            if len(sb_resources) > 0:
                dirs.append(os.path.join("build", "external_resources", dir_name, str(v), nodejs_dir))
                _set_resource_vars(unit, erm_json, tool, v, nodejs_version.major)
            else:
                unit.message(["WARN", "Missing {}@{} for {}".format(tool, str(v), nodejs_dir)])
        else:
            v = _select_matching_version(erm_json, tool, pj.get_dep_specifier(tool))
            dirs.append(os.path.join("build", "external_resources", dir_name, str(v)))
            _set_resource_vars(unit, erm_json, tool, v, nodejs_version.major)

    if dirs:
        unit.onpeerdir(dirs)


@_with_report_configure_error
def on_ts_configure(unit: NotsUnitType) -> None:
    from lib.nots.package_manager.base import PackageJson
    from lib.nots.package_manager.base.utils import build_pj_path
    from lib.nots.typescript import TsConfig

    tsconfig_paths = unit.get("TS_CONFIG_PATH").split()
    # for use in CMD as inputs
    __set_append(
        unit, "TS_CONFIG_FILES", _build_cmd_input_paths(tsconfig_paths, hide=True, disable_include_processor=True)
    )

    mod_dir = unit.get("MODDIR")
    cur_dir = unit.get("TS_TEST_FOR_PATH") if unit.get("TS_TEST_FOR") else mod_dir
    pj_path = build_pj_path(_arc_path(unit, cur_dir))
    dep_paths = PackageJson.load(pj_path).get_dep_paths_by_names()

    # reversed for using the first tsconfig as the config for include processor (legacy)
    for tsconfig_path in reversed(tsconfig_paths):
        abs_tsconfig_path = _arc_path(unit, tsconfig_path)
        if not abs_tsconfig_path:
            raise Exception("tsconfig not found: {}".format(tsconfig_path))

        source_dir = _arc_path(unit, _get_source_path(unit))
        tsconfig = TsConfig.load(abs_tsconfig_path, source_dir)
        config_files = tsconfig.inline_extend(dep_paths)
        config_files = [rootrel_arc_src(path, unit) for path in config_files]

        use_tsconfig_outdir = unit.get("TS_CONFIG_USE_OUTDIR") == "yes"
        tsconfig.validate(use_tsconfig_outdir)

        # add tsconfig files from which root tsconfig files were extended
        __set_append(
            unit, "TS_CONFIG_FILES", _build_cmd_input_paths(config_files, hide=True, disable_include_processor=True)
        )

        # region include processor
        unit.set(["TS_CONFIG_ROOT_DIR", tsconfig.compiler_option("rootDir")])  # also for hermione
        if use_tsconfig_outdir:
            unit.set(["TS_CONFIG_OUT_DIR", tsconfig.compiler_option("outDir")])  # also for hermione

        unit.set(["TS_CONFIG_SOURCE_MAP", to_yesno(tsconfig.compiler_option("sourceMap"))])
        unit.set(["TS_CONFIG_DECLARATION", to_yesno(tsconfig.compiler_option("declaration"))])
        unit.set(["TS_CONFIG_DECLARATION_MAP", to_yesno(tsconfig.compiler_option("declarationMap"))])
        unit.set(["TS_CONFIG_PRESERVE_JSX", to_yesno(tsconfig.compiler_option("jsx") == "preserve")])
        # endregion

        _filter_inputs_by_rules_from_tsconfig(unit, tsconfig)

    # Code navigation
    if unit.get("TS_YNDEXING") == "yes":
        unit.on_do_ts_yndexing()

    # Style tests
    _setup_eslint(unit)
    _setup_tsc_typecheck(unit)
    _setup_stylelint(unit)


def _should_setup_build_env(unit: NotsUnitType) -> bool:
    build_env_for = unit.get("TS_BUILD_ENV_FOR")
    if build_env_for is None:
        return True

    target = unit.get("MODDIR")

    return target in build_env_for.split(":")


@_with_report_configure_error
def on_setup_build_env(unit: NotsUnitType) -> None:
    build_env_var = unit.get("TS_BUILD_ENV")
    build_env_defaults_list = _parse_list_var(
        unit, "TS_BUILD_ENV_DEFAULTS_LIST", unit.get("TS_BUILD_ENV_DEFAULTS_LIST_SEP")
    )

    options = []
    names = set()
    if build_env_var and _should_setup_build_env(unit):
        for name in build_env_var.split(","):
            value = unit.get(f"TS_ENV_{name}")
            if value is None:
                ymake.report_configure_error(f"Env var '{name}' is provided in a list, but var value is not provided")
                continue
            double_quote_escaped_value = value.replace('"', '\\"')
            options.append("--env")
            options.append(f'"{name}={double_quote_escaped_value}"')
            names.add(name)

    for env_default in build_env_defaults_list:
        name, value = env_default.split("=", 1)
        if name in names:
            continue
        double_quote_escaped_value = value.replace('"', '\\"')
        options.append("--env")
        options.append(f'"{name}={double_quote_escaped_value}"')

    unit.set(["NOTS_TOOL_BUILD_ENV", " ".join(options)])


def __set_append(unit: NotsUnitType, var_name: str, value: UnitType.PluginArgs, delimiter: str = " ") -> None:
    """
    SET_APPEND() python naive implementation - append value/values to the list of values
    """
    old_str_value = unit.get(var_name)
    old_values = [old_str_value] if old_str_value else []
    new_values = list(value) if isinstance(value, list) or isinstance(value, tuple) else [value]
    unit.set([var_name, delimiter.join(old_values + new_values)])


def __strip_prefix(prefix: str, line: str) -> str:
    if line.startswith(prefix):
        prefix_len = len(prefix)
        return line[prefix_len:]

    return line


def _filter_inputs_by_rules_from_tsconfig(unit: NotsUnitType, tsconfig: 'TsConfig') -> None:
    """
    Reduce file list from the TS_GLOB_FILES variable following tsconfig.json rules
    """
    mod_dir = unit.get("MODDIR")
    target_path = os.path.join("${ARCADIA_ROOT}", mod_dir, "")  # To have "/" in the end

    for from_var, to_var in [("TS_GLOB_FILES", "TS_INPUT_FILES"), ("TS_GLOB_TEST_FILES", "TS_INPUT_TEST_FILES")]:
        all_files = [__strip_prefix(target_path, f) for f in unit.get(from_var).split(" ")]
        filtered_files = tsconfig.filter_files(all_files)
        __set_append(unit, to_var, [os.path.join(target_path, f) for f in filtered_files])


def _is_tests_enabled(unit: NotsUnitType) -> bool:
    return unit.get("CPP_ANALYSIS_MODE") != "yes"


def _setup_eslint(unit: NotsUnitType) -> None:
    if not _is_tests_enabled(unit):
        return

    if unit.get("_NO_LINT_VALUE") == "none":
        return

    test_files = df.TestFiles.ts_lint_srcs(unit, (), {})[df.TestFiles.KEY]
    if not test_files:
        return

    unit.on_peerdir_ts_resource("eslint")
    user_recipes = unit.get("TEST_RECIPES_VALUE")
    unit.on_setup_install_node_modules_recipe()

    test_type = TsTestType.ESLINT

    from lib.nots.package_manager import constants

    peers = _create_pm(unit).get_local_peers_from_package_json()
    deps = df.CustomDependencies.nots_with_recipies(unit, (peers,), {})[df.CustomDependencies.KEY].split()

    if deps:
        unit.ondepends(deps)

    flat_args = (test_type, "MODDIR")

    dart_record = create_dart_record(
        TS_TEST_FIELDS_BASE + TS_TEST_SPECIFIC_FIELDS[test_type],
        unit,
        flat_args,
        {},
    )
    dart_record[df.TestFiles.KEY] = test_files
    dart_record[df.NodeModulesBundleFilename.KEY] = constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME

    extra_deps = df.CustomDependencies.test_depends_only(unit, (), {})[df.CustomDependencies.KEY].split()
    dart_record[df.CustomDependencies.KEY] = " ".join(sort_uniq(deps + extra_deps))

    if unit.get("TS_LOCAL_CLI") != "yes":
        # disable chunks for `ya tool nots`
        dart_record[df.LintFileProcessingTime.KEY] = str(ESLINT_FILE_PROCESSING_TIME_DEFAULT)

    data = ytest.dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])
    unit.set(["TEST_RECIPES_VALUE", user_recipes])


@_with_report_configure_error
def _setup_tsc_typecheck(unit: NotsUnitType) -> None:
    if not _is_tests_enabled(unit):
        return

    if unit.get("_TS_TYPECHECK_VALUE") == "none":
        return

    test_files = df.TestFiles.tsc_typecheck_input_files(unit, (), {})[df.TestFiles.KEY]
    if not test_files:
        return

    tsconfig_paths = unit.get("_TS_TYPECHECK_TSCONFIG").split(" ")
    if len(tsconfig_paths) > 1:
        raise Exception(
            f"You have set several tsconfig files for TS_TYPECHECK: {tsconfig_paths}. "
            f"Only one tsconfig is allowed for `TS_TYPECHECK(<config_filename>)` macro."
        )

    tsconfig_path = tsconfig_paths[0]
    if not tsconfig_path:
        tsconfig_paths = unit.get("TS_CONFIG_PATH").split()
        if len(tsconfig_paths) > 1:
            macros = " or ".join([f"TS_TYPECHECK({p})" for p in tsconfig_paths])
            raise Exception(f"Module uses several tsconfig files, specify which one to use for typecheck: {macros}")

        tsconfig_path = tsconfig_paths[0]

    abs_tsconfig_path = _arc_path(unit, tsconfig_path)
    if not abs_tsconfig_path:
        raise Exception(f"tsconfig for typecheck not found: {tsconfig_path}")

    unit.on_peerdir_ts_resource("typescript")
    user_recipes = unit.get("TEST_RECIPES_VALUE")
    unit.on_setup_install_node_modules_recipe()

    test_type = TsTestType.TSC_TYPECHECK

    from lib.nots.package_manager import constants

    peers = _create_pm(unit).get_local_peers_from_package_json()
    deps = df.CustomDependencies.nots_with_recipies(unit, (peers,), {})[df.CustomDependencies.KEY].split()

    if deps:
        unit.ondepends(deps)

    flat_args = (test_type,)

    dart_record = create_dart_record(
        TS_TEST_FIELDS_BASE + TS_TEST_SPECIFIC_FIELDS[test_type],
        unit,
        flat_args,
        {},
    )
    dart_record[df.TestFiles.KEY] = test_files
    dart_record[df.NodeModulesBundleFilename.KEY] = constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME

    extra_deps = df.CustomDependencies.test_depends_only(unit, (), {})[df.CustomDependencies.KEY].split()
    dart_record[df.CustomDependencies.KEY] = " ".join(sort_uniq(deps + extra_deps))
    dart_record[df.TsConfigPath.KEY] = tsconfig_path

    data = ytest.dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])
    unit.set(["TEST_RECIPES_VALUE", user_recipes])


@_with_report_configure_error
def _setup_stylelint(unit: NotsUnitType) -> None:
    if not _is_tests_enabled(unit):
        return

    if unit.get("_TS_STYLELINT_VALUE") == "no":
        return

    test_files = df.TestFiles.stylesheets(unit, (), {})[df.TestFiles.KEY]
    if not test_files:
        return

    unit.on_peerdir_ts_resource("stylelint")

    from lib.nots.package_manager import constants

    recipes_value = unit.get("TEST_RECIPES_VALUE")
    unit.on_setup_install_node_modules_recipe()

    test_type = TsTestType.TS_STYLELINT

    peers = _create_pm(unit).get_local_peers_from_package_json()

    deps = df.CustomDependencies.nots_with_recipies(unit, (peers,), {})[df.CustomDependencies.KEY].split()
    if deps:
        unit.ondepends(deps)

    flat_args = (test_type,)
    spec_args = dict(nm_bundle=constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME)

    dart_record = create_dart_record(
        TS_TEST_FIELDS_BASE + TS_TEST_SPECIFIC_FIELDS[test_type], unit, flat_args, spec_args
    )

    extra_deps = df.CustomDependencies.test_depends_only(unit, (), {})[df.CustomDependencies.KEY].split()
    dart_record[df.CustomDependencies.KEY] = " ".join(sort_uniq(deps + extra_deps))

    data = ytest.dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])

    unit.set(["TEST_RECIPES_VALUE", recipes_value])


def _set_resource_vars(
    unit: NotsUnitType, erm_json: 'ErmJsonLite', tool: str, version: 'Version', nodejs_major: int = None
) -> None:
    resource_name = erm_json.canonize_name(tool).upper()

    # example: NODEJS_12_18_4 | HERMIONE_7_0_4_NODEJS_18
    version_str = str(version).replace(".", "_")
    yamake_resource_name = "{}_{}".format(resource_name, version_str)

    if erm_json.is_resource_multiplatform(tool):
        yamake_resource_name += "_NODEJS_{}".format(nodejs_major)

    yamake_resource_var = "{}_RESOURCE_GLOBAL".format(yamake_resource_name)

    unit.set(["{}_ROOT".format(resource_name), "${}".format(yamake_resource_var)])
    unit.set(["{}-ROOT-VAR-NAME".format(resource_name), yamake_resource_var])


def _select_matching_version(
    erm_json: 'ErmJsonLite', resource_name: str, range_str: str, dep_is_required=False
) -> 'Version':
    if dep_is_required and range_str is None:
        raise Exception(
            "Please install the '{tool}' package to the project. Run the command:\n"
            "   ya tool nots add -D {tool}".format(tool=resource_name)
        )

    try:
        version = erm_json.select_version_of(resource_name, range_str)
        if version:
            return version

        raise ValueError("There is no allowed version to satisfy this range: '{}'".format(range_str))
    except Exception as error:
        toolchain_versions = erm_json.get_versions_of(erm_json.get_resource(resource_name))

        raise Exception(
            "Requested {} version range '{}' could not be satisfied. \n"
            "Please use a range that would include one of the following: {}. \n"
            "For further details please visit the link: {} \nOriginal error: {} \n".format(
                resource_name,
                range_str,
                ", ".join(map(str, toolchain_versions)),
                "https://docs.yandex-team.ru/frontend-in-arcadia/_generated/toolchain",
                str(error),
            )
        )


@_with_report_configure_error
def on_prepare_deps_configure(unit: NotsUnitType) -> None:
    from lib.nots.package_manager.base.utils import build_pj_path

    pm = _create_pm(unit)

    if not _is_real_file(build_pj_path(pm.sources_path)) and unit.get("TS_PROTO_PREPARE_DEPS") == "yes":
        # if this is a PREPARE_DEPS for TS_PROTO and there is no package.json - this is TS_PROTO_AUTO
        unit.on_ts_proto_auto_prepare_deps_configure()
        return

    pj = pm.load_package_json_from_dir(pm.sources_path)
    has_deps = pj.has_dependencies()
    local_cli = unit.get("TS_LOCAL_CLI") == "yes"
    ins, outs, resources = pm.calc_prepare_deps_inouts_and_resources(unit.get("_TARBALLS_STORE"), has_deps, local_cli)

    if has_deps:
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives(["hide", "input"], sorted(ins)))
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives(["hide", "output"], sorted(outs)))
        unit.set(["_PREPARE_DEPS_RESOURCES", " ".join([f'${{resource:"{uri}"}}' for uri in sorted(resources)])])
        unit.set(["_PREPARE_DEPS_USE_RESOURCES_FLAG", "--resource-root $(RESOURCE_ROOT)"])

    else:
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives(["output"], sorted(outs)))
        unit.set(["_PREPARE_DEPS_CMD", "$_PREPARE_NO_DEPS_CMD"])


@_with_report_configure_error
def on_ts_proto_auto_prepare_deps_configure(unit: NotsUnitType) -> None:
    deps_path = unit.get("_TS_PROTO_AUTO_DEPS")
    unit.onpeerdir([deps_path])

    pm = _create_pm(unit)
    local_cli = unit.get("TS_LOCAL_CLI") == "yes"
    _, outs, _ = pm.calc_prepare_deps_inouts_and_resources(store_path="", has_deps=False, local_cli=local_cli)
    __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives(["hide", "output"], sorted(outs)))
    unit.set(["_PREPARE_DEPS_TS_PROTO_AUTO_FLAG", f"--ts-proto-auto-deps-path {deps_path}"])


def _node_modules_bundle_needed(unit: NotsUnitType, arc_path: str) -> bool:
    if unit.get("_WITH_NODE_MODULES") == "yes":
        return True

    node_modules_for = unit.get("NODE_MODULES_FOR")
    nm_required_for = node_modules_for.split(":") if node_modules_for else []

    return arc_path in nm_required_for


@_with_report_configure_error
def on_node_modules_configure(unit: NotsUnitType) -> None:
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)
    has_deps = pj.has_dependencies()

    if has_deps:
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        nm_bundle_needed = _node_modules_bundle_needed(unit, pm.module_path)
        if nm_bundle_needed:
            unit.set(["_NODE_MODULES_BUNDLE_ARG", "--nm-bundle yes"])

        ins, outs = pm.calc_node_modules_inouts(nm_bundle_needed)

        __set_append(unit, "_NODE_MODULES_INOUTS", _build_directives(["hide", "input"], sorted(ins)))
        if not unit.get("TS_TEST_FOR"):
            __set_append(unit, "_NODE_MODULES_INOUTS", _build_directives(["hide", "output"], sorted(outs)))

        lf = pm.load_lockfile_from_dir(pm.sources_path)

        if hasattr(lf, "validate_importers"):
            lf.validate_importers()

        if pj.get_use_prebuilder():
            unit.on_peerdir_ts_resource("@yatool/prebuilder")
            unit.set(
                [
                    "_YATOOL_PREBUILDER_ARG",
                    "--yatool-prebuilder-path $YATOOL_PREBUILDER_ROOT/node_modules/@yatool/prebuilder",
                ]
            )

            # YATOOL_PREBUILDER_0_7_0_RESOURCE_GLOBAL
            prebuilder_major = unit.get("YATOOL_PREBUILDER-ROOT-VAR-NAME").split("_")[2]
            logger.info(f"Detected prebuilder {COLORS.green}{prebuilder_major}.x.x{COLORS.reset}")

            if prebuilder_major == "0":
                # TODO: FBP-1408
                is_valid, invalid_keys = lf.validate_has_addons_flags()

                if not is_valid:
                    ymake.report_configure_error(
                        "Project is configured to use @yatool/prebuilder. \n"
                        + "Some packages in the pnpm-lock.yaml are misconfigured.\n"
                        + f"Run {COLORS.green}`ya tool nots update-lockfile`{COLORS.reset} to fix lockfile.\n"
                        + "All packages with `requiresBuild:true` have to be marked with `hasAddons:true/false`.\n"
                        + "Misconfigured keys: \n"
                        + "  - "
                        + "\n  - ".join(invalid_keys)
                    )
            else:
                requires_build_packages = lf.get_requires_build_packages()
                is_valid, validation_messages = pj.validate_prebuilds(requires_build_packages)

                if not is_valid:
                    ymake.report_configure_error(
                        "Project is configured to use @yatool/prebuilder. \n"
                        + "Some packages are misconfigured.\n"
                        + f"Run {COLORS.green}`ya tool nots update-lockfile`{COLORS.reset} to fix pnpm-lock.yaml and package.json.\n"
                        + "Validation details: \n"
                        + "\n".join(validation_messages)
                    )


@_with_report_configure_error
def on_ts_test_for_configure(
    unit: NotsUnitType, test_runner: TsTestType, default_config: str, node_modules_filename: str
) -> None:
    if not _is_tests_enabled(unit):
        return

    if unit.enabled('TS_COVERAGE'):
        unit.on_peerdir_ts_resource("nyc")

    for_mod_path = df.TsTestForPath.value(unit, (), {})[df.TsTestForPath.KEY]
    unit.onpeerdir([for_mod_path])

    # user-defined recipes should be in the end
    user_recipes = unit.get("TEST_RECIPES_VALUE").replace("$TEST_RECIPES_VALUE", "").strip()
    unit.set(["TEST_RECIPES_VALUE", ""])
    unit.on_setup_extract_node_modules_recipe([for_mod_path])
    unit.on_setup_extract_output_tars_recipe([for_mod_path])
    __set_append(unit, "TEST_RECIPES_VALUE", user_recipes)

    build_root = "$B" if test_runner in [TsTestType.HERMIONE, TsTestType.PLAYWRIGHT_LARGE] else "$(BUILD_ROOT)"
    unit.set(["TS_TEST_NM", os.path.join(build_root, for_mod_path, node_modules_filename)])

    config_path = unit.get("TS_TEST_CONFIG_PATH")
    if not config_path:
        config_path = os.path.join(for_mod_path, default_config)
        unit.set(["TS_TEST_CONFIG_PATH", config_path])

    test_files = df.TestFiles.ts_test_srcs(unit, (), {})[df.TestFiles.KEY]
    if not test_files:
        ymake.report_configure_error(f"No tests found for {test_runner}")
        return

    from lib.nots.package_manager import constants

    peers = _create_pm(unit).get_local_peers_from_package_json()
    deps = df.CustomDependencies.nots_with_recipies(unit, (peers,), {})[df.CustomDependencies.KEY].split()

    if deps:
        unit.ondepends(deps)

    flat_args = (test_runner, "TS_TEST_FOR_PATH")
    spec_args = {'erm_json': _create_erm_json(unit)}

    dart_record = create_dart_record(
        TS_TEST_FIELDS_BASE + TS_TEST_SPECIFIC_FIELDS[test_runner],
        unit,
        flat_args,
        spec_args,
    )
    dart_record[df.TestFiles.KEY] = test_files
    dart_record[df.NodeModulesBundleFilename.KEY] = constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME

    extra_deps = df.CustomDependencies.test_depends_only(unit, (), {})[df.CustomDependencies.KEY].split()
    dart_record[df.CustomDependencies.KEY] = " ".join(sort_uniq(deps + extra_deps))
    if test_runner in [TsTestType.HERMIONE, TsTestType.PLAYWRIGHT_LARGE]:
        dart_record[df.Size.KEY] = "LARGE"

    data = ytest.dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


# noinspection PyUnusedLocal
@_with_report_configure_error
def on_validate_ts_test_for_args(unit: NotsUnitType, for_mod: str, root: str) -> None:
    if for_mod == "." or for_mod == "./":
        ymake.report_configure_error(f"Tests should be for parent module but got path '{for_mod}'")
        return

    is_arc_root = root == "${ARCADIA_ROOT}"
    is_rel_for_mod = for_mod.startswith(".")

    if is_arc_root and is_rel_for_mod:
        ymake.report_configure_error(
            "You are using a relative path for a module. "
            + "You have to add RELATIVE key, like (RELATIVE {})".format(for_mod)
        )


@_with_report_configure_error
def on_set_ts_test_for_vars(unit: NotsUnitType, for_mod: str) -> None:
    unit.set(["TS_TEST_FOR", "yes"])
    unit.set(["TS_TEST_FOR_DIR", unit.resolve_arc_path(for_mod)])
    unit.set(["TS_TEST_FOR_PATH", rootrel_arc_src(for_mod, unit)])


def __on_ts_files(unit: NotsUnitType, files_in: list[str], files_out: list[str]) -> None:
    for f in files_in:
        if f.startswith(".."):
            ymake.report_configure_error(
                "TS_FILES* macroses are only allowed to use files inside the project directory.\n"
                f"Got path '{f}'.\n"
                "Docs: https://docs.yandex-team.ru/frontend-in-arcadia/references/TS_PACKAGE#ts-files."
            )

    new_items = _build_cmd_input_paths(paths=files_in, hide=True, disable_include_processor=True)
    new_items += " "
    new_items += _build_cmd_output_paths(paths=files_out, hide=True)
    __set_append(unit, "_TS_FILES_INOUTS", new_items)


@_with_report_configure_error
def on_ts_files(unit: NotsUnitType, *files: str) -> None:
    __on_ts_files(unit, files, files)


@_with_report_configure_error
def on_ts_large_files(unit: NotsUnitType, destination: str, *files: list[str]) -> None:
    if destination == REQUIRED_MISSING:
        ymake.report_configure_error(
            "Macro TS_LARGE_FILES() requires to use DESTINATION parameter.\n"
            "   TS_LARGE_FILES(\n"
            "       DESTINATION some_dir\n"
            "       large/file1\n"
            "       large/file2\n"
            "   )\n"
            "Docs: https://docs.yandex-team.ru/frontend-in-arcadia/references/TS_PACKAGE#ts-large-files."
        )
        return

    in_files = [os.path.join('${BINDIR}', f) for f in files]
    out_files = [os.path.join('${BINDIR}', destination, f) for f in files]

    # TODO: FBP-1795
    # ${BINDIR} prefix for input is important to resolve to result of LARGE_FILES and not to SOURCEDIR
    new_items = [f'$COPY_CMD {i} {o}' for (i, o) in zip(in_files, out_files)]
    __set_append(unit, "_TS_PROJECT_SETUP_CMD", new_items, " && ")

    __on_ts_files(unit, in_files, out_files)


@_with_report_configure_error
def on_ts_package_check_files(unit: NotsUnitType) -> None:
    ts_files = unit.get("_TS_FILES_INOUTS")
    if ts_files == "":
        ymake.report_configure_error(
            "\n"
            "In the TS_PACKAGE module, you should define at least one file using the TS_FILES() macro.\n"
            "If you use the TS_FILES_GLOB, check the expression. For example, use `src/**/*` instead of `src/*`.\n"
            "Docs: https://docs.yandex-team.ru/frontend-in-arcadia/references/TS_PACKAGE#ts-files."
        )


@_with_report_configure_error
def on_depends_on_mod(unit: NotsUnitType) -> None:
    if unit.get("_TS_TEST_DEPENDS_ON_BUILD"):
        for_mod_path = unit.get("TS_TEST_FOR_PATH")
        unit.ondepends([for_mod_path])


@_with_report_configure_error
def on_run_javascript_after_build_process_inputs(unit: NotsUnitType, js_script: str) -> None:
    inputs = unit.get("_RUN_JAVASCRIPT_AFTER_BUILD_INPUTS").split(" ")

    def process_input(input: str) -> str:
        is_var, var_name = _get_var_name(input)
        if is_var:
            return f"${{hide;input:{var_name}}}"
        return _build_cmd_input_paths([input], hide=True)

    processed_inputs = [process_input(i) for i in inputs if i]

    js_script = os.path.normpath(js_script)
    if not js_script.startswith("node_modules/"):
        processed_inputs.append(_build_cmd_input_paths([js_script], hide=True))

    unit.set(["_RUN_JAVASCRIPT_AFTER_BUILD_INPUTS", " ".join(processed_inputs)])


@_with_report_configure_error
def on_ts_next_experimental_build_mode(unit: NotsUnitType) -> None:
    from lib.nots.package_manager import BasePackageManager
    from lib.nots.semver import Version

    pj = BasePackageManager.load_package_json_from_dir(unit.resolve(_get_source_path(unit)))
    erm_json = _create_erm_json(unit)
    version = _select_matching_version(erm_json, "next", pj.get_dep_specifier("next"))

    var_name = "TS_NEXT_COMMAND"

    if version >= Version.from_str("14.1.2"):
        # For Next >=v14: build --experimental-build-mode=compile
        # https://github.com/vercel/next.js/commit/47f73cd8ec79d0a6c248139088aa536453b23ae1
        unit.set([var_name, "build --experimental-build-mode=compile"])
    elif version >= Version.from_str("13.5.3"):
        # For Next <v14: experimental-compile
        unit.set([var_name, "experimental-compile"])
    else:
        raise Exception(f"Unsupported Next.js version: {version} for TS_NEXT_EXPERIMENTAL_BUILD_MODE()")
