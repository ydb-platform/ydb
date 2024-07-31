import base64
import six
import os
import ymake
import ytest

from _common import resolve_common_const, get_norm_unit_path, rootrel_arc_src, strip_roots, to_yesno


# 1 is 60 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
# 0.5 is 120 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
# 0.2 is 300 files per chunk for TIMEOUT(60) - default timeout for SIZE(SMALL)
ESLINT_FILE_PROCESSING_TIME_DEFAULT = 0.2  # seconds per file


class PluginLogger(object):
    def __init__(self):
        self.unit = None
        self.prefix = ""

    def reset(self, unit, prefix=""):
        self.unit = unit
        self.prefix = prefix

    def get_state(self):
        return (self.unit, self.prefix)

    def _stringify_messages(self, messages):
        parts = []
        for m in messages:
            if m is None:
                parts.append("None")
            else:
                parts.append(m if isinstance(m, str) else repr(m))

        # cyan color (code 36) for messages
        return "\033[0;32m{}\033[0;49m\n\033[0;36m{}\033[0;49m".format(self.prefix, " ".join(parts))

    def info(self, *messages):
        if self.unit:
            self.unit.message(["INFO", self._stringify_messages(messages)])

    def warn(self, *messages):
        if self.unit:
            self.unit.message(["WARN", self._stringify_messages(messages)])

    def error(self, *messages):
        if self.unit:
            self.unit.message(["ERROR", self._stringify_messages(messages)])

    def print_vars(self, *variables):
        if self.unit:
            values = ["{}={}".format(v, self.unit.get(v)) for v in variables]
            self.info("\n".join(values))


logger = PluginLogger()


def get_values_list(unit, key):
    res = map(str.strip, (unit.get(key) or '').replace('$' + key, '').strip().split())
    return [r for r in res if r and r not in ['""', "''"]]


def format_recipes(data: str | None) -> str:
    if not data:
        return ""

    data = data.replace('"USE_RECIPE_DELIM"', "\n")
    data = data.replace("$TEST_RECIPES_VALUE", "")
    return data


def prepare_recipes(data: str | None) -> bytes:
    formatted = format_recipes(data)
    return base64.b64encode(six.ensure_binary(formatted))


def serialize_list(lst):
    lst = list(filter(None, lst))
    return '\"' + ';'.join(lst) + '\"' if lst else ''


def _with_report_configure_error(fn):
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


def _build_directives(name, flags, paths):
    # type: (str, list[str]|tuple[str], list[str]) -> str

    parts = [p for p in [name] + (flags or []) if p]
    parts_str = ";".join(parts)
    expressions = ['${{{parts}:"{path}"}}'.format(parts=parts_str, path=path) for path in paths]

    return " ".join(expressions)


def _build_cmd_input_paths(paths, hide=False, disable_include_processor=False):
    # type: (list[str]|tuple[str], bool, bool) -> str
    hide_part = "hide" if hide else ""
    disable_ip_part = "context=TEXT" if disable_include_processor else ""

    return _build_directives("input", [hide_part, disable_ip_part], paths)


def _get_pm_type(unit) -> str:
    resolved = unit.get("PM_TYPE")
    if not resolved:
        raise Exception("PM_TYPE is not set yet. Macro _SET_PACKAGE_MANAGER() should be called before.")

    return resolved


def _get_source_path(unit):
    sources_path = unit.get("TS_TEST_FOR_DIR") if unit.get("TS_TEST_FOR") else unit.path()
    return sources_path


def _create_pm(unit):
    from lib.nots.package_manager import get_package_manager_type

    sources_path = _get_source_path(unit)
    module_path = unit.get("TS_TEST_FOR_PATH") if unit.get("TS_TEST_FOR") else unit.get("MODDIR")

    PackageManager = get_package_manager_type(_get_pm_type(unit))

    return PackageManager(
        sources_path=unit.resolve(sources_path),
        build_root="$B",
        build_path=unit.path().replace("$S", "$B", 1),
        contribs_path=unit.get("NPM_CONTRIBS_PATH"),
        nodejs_bin_path=None,
        script_path=None,
        module_path=module_path,
    )


def _create_erm_json(unit):
    from lib.nots.erm_json_lite import ErmJsonLite

    erm_packages_path = unit.get("ERM_PACKAGES_PATH")
    path = unit.resolve(unit.resolve_arc_path(erm_packages_path))

    return ErmJsonLite.load(path)


@_with_report_configure_error
def on_set_package_manager(unit):
    pm_type = "pnpm"  # projects without any lockfile are processed by pnpm

    source_path = _get_source_path(unit)

    for pm_key, lockfile_name in [("pnpm", "pnpm-lock.yaml"), ("npm", "package-lock.json")]:
        lf_path = os.path.join(source_path, lockfile_name)
        lf_path_resolved = unit.resolve_arc_path(strip_roots(lf_path))

        if lf_path_resolved:
            pm_type = pm_key
            break

    unit.on_peerdir_ts_resource(pm_type)
    unit.set(["PM_TYPE", pm_type])
    unit.set(["PM_SCRIPT", f"${pm_type.upper()}_SCRIPT"])


@_with_report_configure_error
def on_set_append_with_directive(unit, var_name, dir, *values):
    wrapped = ['${{{dir}:"{v}"}}'.format(dir=dir, v=v) for v in values]
    __set_append(unit, var_name, " ".join(wrapped))


@_with_report_configure_error
def on_from_npm_lockfiles(unit, *args):
    from lib.nots.package_manager.base import PackageManagerError

    # This is contrib with pnpm-lock.yaml files only
    # Force set to pnpm
    unit.set(["PM_TYPE", "pnpm"])
    pm = _create_pm(unit)
    lf_paths = []

    for lf_path in args:
        abs_lf_path = unit.resolve(unit.resolve_arc_path(lf_path))
        if abs_lf_path:
            lf_paths.append(abs_lf_path)
        elif unit.get("TS_STRICT_FROM_NPM_LOCKFILES") == "yes":
            ymake.report_configure_error("lockfile not found: {}".format(lf_path))

    try:
        for pkg in pm.extract_packages_meta_from_lockfiles(lf_paths):
            unit.on_from_npm([pkg.tarball_url, pkg.sky_id, pkg.integrity, pkg.integrity_algorithm, pkg.tarball_path])
    except PackageManagerError as e:
        logger.warn(str(e))
        pass


def _check_nodejs_version(unit, major):
    if major < 14:
        raise Exception(
            "Node.js {} is unsupported. Update Node.js please. See https://nda.ya.ru/t/joB9Mivm6h4znu".format(major)
        )

    if major < 18:
        unit.message(
            [
                "WARN",
                "Node.js {} is deprecated. Update Node.js please. See https://nda.ya.ru/t/joB9Mivm6h4znu".format(major),
            ]
        )


@_with_report_configure_error
def on_peerdir_ts_resource(unit, *resources):
    from lib.nots.package_manager import BasePackageManager

    pj = BasePackageManager.load_package_json_from_dir(unit.resolve(_get_source_path(unit)))
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
def on_ts_configure(unit):
    # type: (Unit) -> None
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
    pj_path = build_pj_path(unit.resolve(unit.resolve_arc_path(cur_dir)))
    dep_paths = PackageJson.load(pj_path).get_dep_paths_by_names()

    # reversed for using the first tsconfig as the config for include processor (legacy)
    for tsconfig_path in reversed(tsconfig_paths):
        abs_tsconfig_path = unit.resolve(unit.resolve_arc_path(tsconfig_path))
        if not abs_tsconfig_path:
            raise Exception("tsconfig not found: {}".format(tsconfig_path))

        tsconfig = TsConfig.load(abs_tsconfig_path)
        config_files = tsconfig.inline_extend(dep_paths)
        config_files = _resolve_module_files(unit, mod_dir, config_files)

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

    _setup_eslint(unit)
    _setup_tsc_typecheck(unit, tsconfig_paths)

    if unit.get("TS_YNDEXING") == "yes":
        unit.on_do_ts_yndexing()


@_with_report_configure_error
def on_setup_build_env(unit):  # type: (Unit) -> None
    build_env_var = unit.get("TS_BUILD_ENV")  # type: str
    if not build_env_var:
        return

    options = []
    for name in build_env_var.split(","):
        options.append("--env")
        value = unit.get(f"TS_ENV_{name}")
        if value is None:
            ymake.report_configure_error(f"Env var '{name}' is provided in a list, but var value is not provided")
            continue
        double_quote_escaped_value = value.replace('"', '\\"')
        options.append(f'"{name}={double_quote_escaped_value}"')

    unit.set(["NOTS_TOOL_BUILD_ENV", " ".join(options)])


def __set_append(unit, var_name, value):
    # type: (Unit, str, str|list[str]|tuple[str]) -> None
    """
    SET_APPEND() python naive implementation - append value/values to the list of values
    """
    previous_value = unit.get(var_name) or ""
    value_in_str = " ".join(value) if isinstance(value, list) or isinstance(value, tuple) else value
    new_value = previous_value + " " + value_in_str

    unit.set([var_name, new_value])


def __strip_prefix(prefix, line):
    # type: (str, str) -> str

    if line.startswith(prefix):
        prefix_len = len(prefix)
        return line[prefix_len:]

    return line


def _filter_inputs_by_rules_from_tsconfig(unit, tsconfig):
    """
    Reduce file list from the TS_GLOB_FILES variable following tsconfig.json rules
    """
    mod_dir = unit.get("MODDIR")
    target_path = os.path.join("${ARCADIA_ROOT}", mod_dir, "")  # To have "/" in the end

    all_files = [__strip_prefix(target_path, f) for f in unit.get("TS_GLOB_FILES").split(" ")]
    filtered_files = tsconfig.filter_files(all_files)

    __set_append(unit, "TS_INPUT_FILES", [os.path.join(target_path, f) for f in filtered_files])


def _get_ts_test_data_dirs(unit):
    return sorted(
        set([os.path.dirname(rootrel_arc_src(p, unit)) for p in (get_values_list(unit, "_TS_TEST_DATA_VALUE") or [])])
    )


def _resolve_config_path(unit, test_runner, rel_to):
    config_path = unit.get("ESLINT_CONFIG_PATH") if test_runner == "eslint" else unit.get("TS_TEST_CONFIG_PATH")
    arc_config_path = unit.resolve_arc_path(config_path)
    abs_config_path = unit.resolve(arc_config_path)
    if not abs_config_path:
        raise Exception("{} config not found: {}".format(test_runner, config_path))

    unit.onsrcs([arc_config_path])
    abs_rel_to = unit.resolve(unit.resolve_arc_path(unit.get(rel_to)))
    return os.path.relpath(abs_config_path, start=abs_rel_to)


def _is_tests_enabled(unit):
    if unit.get("TIDY") == "yes":
        return False

    return True


def _get_test_runner_handlers():
    return {
        "jest": _add_jest_ts_test,
        "hermione": _add_hermione_ts_test,
        "playwright": _add_playwright_ts_test,
    }


def _add_jest_ts_test(unit, test_runner, test_files, deps, test_record):
    test_record.update(
        {
            "CONFIG-PATH": _resolve_config_path(unit, test_runner, rel_to="TS_TEST_FOR_PATH"),
        }
    )
    _add_test(unit, test_runner, test_files, deps, test_record)


def _add_hermione_ts_test(unit, test_runner, test_files, deps, test_record):
    test_tags = sorted(set(["ya:fat", "ya:external", "ya:noretries"] + get_values_list(unit, "TEST_TAGS_VALUE")))
    test_requirements = sorted(set(["network:full"] + get_values_list(unit, "TEST_REQUIREMENTS_VALUE")))

    test_record.update(
        {
            "SIZE": "LARGE",
            "TAG": serialize_list(test_tags),
            "REQUIREMENTS": serialize_list(test_requirements),
            "CONFIG-PATH": _resolve_config_path(unit, test_runner, rel_to="TS_TEST_FOR_PATH"),
        }
    )

    _add_test(unit, test_runner, test_files, deps, test_record)


def _add_playwright_ts_test(unit, test_runner, test_files, deps, test_record):
    test_record.update(
        {
            "CONFIG-PATH": _resolve_config_path(unit, test_runner, rel_to="TS_TEST_FOR_PATH"),
        }
    )
    _add_test(unit, test_runner, test_files, deps, test_record)


def _setup_eslint(unit):
    if not _is_tests_enabled(unit):
        return

    if unit.get("_NO_LINT_VALUE") == "none":
        return

    lint_files = get_values_list(unit, "_TS_LINT_SRCS_VALUE")
    if not lint_files:
        return

    mod_dir = unit.get("MODDIR")

    unit.on_peerdir_ts_resource("eslint")
    user_recipes = unit.get("TEST_RECIPES_VALUE")
    unit.on_setup_install_node_modules_recipe()

    lint_files = _resolve_module_files(unit, mod_dir, lint_files)
    deps = _create_pm(unit).get_peers_from_package_json()
    test_record = {
        "ESLINT_CONFIG_PATH": _resolve_config_path(unit, "eslint", rel_to="MODDIR"),
        "LINT-FILE-PROCESSING-TIME": str(ESLINT_FILE_PROCESSING_TIME_DEFAULT),
    }

    _add_test(unit, "eslint", lint_files, deps, test_record, mod_dir)
    unit.set(["TEST_RECIPES_VALUE", user_recipes])


def _setup_tsc_typecheck(unit, tsconfig_paths: list[str]):
    if not _is_tests_enabled(unit):
        return

    if unit.get("_TS_TYPECHECK_VALUE") == "none":
        return

    typecheck_files = get_values_list(unit, "TS_INPUT_FILES")
    if not typecheck_files:
        return

    tsconfig_path = tsconfig_paths[0]

    if len(tsconfig_paths) > 1:
        tsconfig_path = unit.get("_TS_TYPECHECK_TSCONFIG")
        if not tsconfig_path:
            macros = " or ".join([f"TS_TYPECHECK({p})" for p in tsconfig_paths])
            raise Exception(f"Module uses several tsconfig files, specify which one to use for typecheck: {macros}")
        abs_tsconfig_path = unit.resolve(unit.resolve_arc_path(tsconfig_path))
        if not abs_tsconfig_path:
            raise Exception(f"tsconfig for typecheck not found: {tsconfig_path}")

    unit.on_peerdir_ts_resource("typescript")
    user_recipes = unit.get("TEST_RECIPES_VALUE")
    unit.on_setup_install_node_modules_recipe()
    unit.on_setup_extract_output_tars_recipe([unit.get("MODDIR")])

    _add_test(
        unit,
        test_type="tsc_typecheck",
        test_files=[resolve_common_const(f) for f in typecheck_files],
        deps=_create_pm(unit).get_peers_from_package_json(),
        test_record={"TS_CONFIG_PATH": tsconfig_path},
        test_cwd=unit.get("MODDIR"),
    )
    unit.set(["TEST_RECIPES_VALUE", user_recipes])


def _resolve_module_files(unit, mod_dir, file_paths):
    mod_dir_with_sep_len = len(mod_dir) + 1
    resolved_files = []

    for path in file_paths:
        resolved = rootrel_arc_src(path, unit)
        if resolved.startswith(mod_dir):
            resolved = resolved[mod_dir_with_sep_len:]
        resolved_files.append(resolved)

    return resolved_files


def _add_test(unit, test_type, test_files, deps=None, test_record=None, test_cwd=None):
    from lib.nots.package_manager import constants

    def sort_uniq(text):
        return sorted(set(text))

    recipes_lines = format_recipes(unit.get("TEST_RECIPES_VALUE")).strip().splitlines()
    if recipes_lines:
        deps = deps or []
        deps.extend([os.path.dirname(r.strip().split(" ")[0]) for r in recipes_lines])

    if deps:
        joined_deps = "\n".join(deps)
        logger.info(f"{test_type} deps: \n{joined_deps}")
        unit.ondepends(deps)

    test_dir = get_norm_unit_path(unit)
    full_test_record = {
        # Key to discover suite (see devtools/ya/test/explore/__init__.py#gen_suite)
        "SCRIPT-REL-PATH": test_type,
        # Test name as shown in PR check, should be unique inside one module
        "TEST-NAME": test_type.lower().replace(".new", ""),
        "TEST-TIMEOUT": unit.get("TEST_TIMEOUT") or "",
        "TEST-ENV": ytest.prepare_env(unit.get("TEST_ENV_VALUE")),
        "TESTED-PROJECT-NAME": os.path.splitext(unit.filename())[0],
        "TEST-RECIPES": prepare_recipes(unit.get("TEST_RECIPES_VALUE")),
        "SOURCE-FOLDER-PATH": test_dir,
        "BUILD-FOLDER-PATH": test_dir,
        "BINARY-PATH": os.path.join(test_dir, unit.filename()),
        "SPLIT-FACTOR": unit.get("TEST_SPLIT_FACTOR") or "",
        "FORK-MODE": unit.get("TEST_FORK_MODE") or "",
        "SIZE": unit.get("TEST_SIZE_NAME") or "",
        "TEST-DATA": serialize_list(get_values_list(unit, "TEST_DATA_VALUE")),
        "TEST-FILES": serialize_list(test_files),
        "TEST-CWD": test_cwd or "",
        "TAG": serialize_list(get_values_list(unit, "TEST_TAGS_VALUE")),
        "REQUIREMENTS": serialize_list(get_values_list(unit, "TEST_REQUIREMENTS_VALUE")),
        "NODEJS-ROOT-VAR-NAME": unit.get("NODEJS-ROOT-VAR-NAME"),
        "NODE-MODULES-BUNDLE-FILENAME": constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME,
        "CUSTOM-DEPENDENCIES": " ".join(sort_uniq((deps or []) + get_values_list(unit, "TEST_DEPENDS_VALUE"))),
    }

    if test_record:
        full_test_record.update(test_record)

    data = ytest.dump_test(unit, full_test_record)
    if data:
        unit.set_property(["DART_DATA", data])


def _set_resource_vars(unit, erm_json, tool, version, nodejs_major=None):
    # type: (any, ErmJsonLite, Version, str|None, int|None) -> None

    resource_name = erm_json.canonize_name(tool).upper()

    # example: NODEJS_12_18_4 | HERMIONE_7_0_4_NODEJS_18
    version_str = str(version).replace(".", "_")
    yamake_resource_name = "{}_{}".format(resource_name, version_str)

    if erm_json.is_resource_multiplatform(tool):
        yamake_resource_name += "_NODEJS_{}".format(nodejs_major)

    yamake_resource_var = "{}_RESOURCE_GLOBAL".format(yamake_resource_name)

    unit.set(["{}_ROOT".format(resource_name), "${}".format(yamake_resource_var)])
    unit.set(["{}-ROOT-VAR-NAME".format(resource_name), yamake_resource_var])


def _select_matching_version(erm_json, resource_name, range_str, dep_is_required=False):
    # type: (ErmJsonLite, str, str, bool) -> Version
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
def on_prepare_deps_configure(unit):
    contrib_path = unit.get("NPM_CONTRIBS_PATH")
    if contrib_path == '-':
        unit.on_prepare_deps_configure_no_contrib()
        return
    unit.onpeerdir(contrib_path)
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)
    has_deps = pj.has_dependencies()
    ins, outs = pm.calc_prepare_deps_inouts(unit.get("_TARBALLS_STORE"), has_deps)

    if has_deps:
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("input", ["hide"], sorted(ins)))
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("output", ["hide"], sorted(outs)))

    else:
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("output", [], sorted(outs)))
        unit.set(["_PREPARE_DEPS_CMD", "$_PREPARE_NO_DEPS_CMD"])


@_with_report_configure_error
def on_prepare_deps_configure_no_contrib(unit):
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)
    has_deps = pj.has_dependencies()
    ins, outs, resources = pm.calc_prepare_deps_inouts_and_resources(unit.get("_TARBALLS_STORE"), has_deps)

    if has_deps:
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("input", ["hide"], sorted(ins)))
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("output", ["hide"], sorted(outs)))
        unit.set(["_PREPARE_DEPS_RESOURCES", " ".join([f'${{resource:"{uri}"}}' for uri in sorted(resources)])])
        unit.set(["_PREPARE_DEPS_USE_RESOURCES_FLAG", "--resource-root $(RESOURCE_ROOT)"])

    else:
        __set_append(unit, "_PREPARE_DEPS_INOUTS", _build_directives("output", [], sorted(outs)))
        unit.set(["_PREPARE_DEPS_CMD", "$_PREPARE_NO_DEPS_CMD"])


@_with_report_configure_error
def on_node_modules_configure(unit):
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)

    if pj.has_dependencies():
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        local_cli = unit.get("TS_LOCAL_CLI") == "yes"
        ins, outs = pm.calc_node_modules_inouts(local_cli)

        __set_append(unit, "_NODE_MODULES_INOUTS", _build_directives("input", ["hide"], sorted(ins)))
        if not unit.get("TS_TEST_FOR"):
            __set_append(unit, "_NODE_MODULES_INOUTS", _build_directives("output", ["hide"], sorted(outs)))

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
            logger.info(f"Detected prebuilder \033[0;32mv{prebuilder_major}.x.x\033[0;49m")

            if prebuilder_major == "0":
                # TODO: FBP-1408
                lf = pm.load_lockfile_from_dir(pm.sources_path)
                is_valid, invalid_keys = lf.validate_has_addons_flags()

                if not is_valid:
                    ymake.report_configure_error(
                        "Project is configured to use @yatool/prebuilder. \n"
                        + "Some packages in the pnpm-lock.yaml are misconfigured.\n"
                        + "Run \033[0;32m`ya tool nots update-lockfile`\033[0;49m to fix lockfile.\n"
                        + "All packages with `requiresBuild:true` have to be marked with `hasAddons:true/false`.\n"
                        + "Misconfigured keys: \n"
                        + "  - "
                        + "\n  - ".join(invalid_keys)
                    )
            else:
                lf = pm.load_lockfile_from_dir(pm.sources_path)
                requires_build_packages = lf.get_requires_build_packages()
                is_valid, validation_messages = pj.validate_prebuilds(requires_build_packages)

                if not is_valid:
                    ymake.report_configure_error(
                        "Project is configured to use @yatool/prebuilder. \n"
                        + "Some packages are misconfigured.\n"
                        + "Run \033[0;32m`ya tool nots update-lockfile`\033[0;49m to fix pnpm-lock.yaml and package.json.\n"
                        + "Validation details: \n"
                        + "\n".join(validation_messages)
                    )


@_with_report_configure_error
def on_ts_test_for_configure(unit, test_runner, default_config, node_modules_filename):
    if not _is_tests_enabled(unit):
        return

    if unit.enabled('TS_COVERAGE'):
        unit.on_peerdir_ts_resource("nyc")

    for_mod_path = unit.get("TS_TEST_FOR_PATH")
    unit.onpeerdir([for_mod_path])
    unit.on_setup_extract_node_modules_recipe([for_mod_path])
    unit.on_setup_extract_output_tars_recipe([for_mod_path])

    root = "$B" if test_runner == "hermione" else "$(BUILD_ROOT)"
    unit.set(["TS_TEST_NM", os.path.join(root, for_mod_path, node_modules_filename)])

    config_path = unit.get("TS_TEST_CONFIG_PATH")
    if not config_path:
        config_path = os.path.join(for_mod_path, default_config)
        unit.set(["TS_TEST_CONFIG_PATH", config_path])

    test_record = _add_ts_resources_to_test_record(
        unit,
        {
            "TS-TEST-FOR-PATH": for_mod_path,
            "TS-TEST-DATA-DIRS": serialize_list(_get_ts_test_data_dirs(unit)),
            "TS-TEST-DATA-DIRS-RENAME": unit.get("_TS_TEST_DATA_DIRS_RENAME_VALUE"),
        },
    )

    test_files = get_values_list(unit, "_TS_TEST_SRCS_VALUE")
    test_files = _resolve_module_files(unit, unit.get("MODDIR"), test_files)
    if not test_files:
        ymake.report_configure_error("No tests found")
        return

    deps = _create_pm(unit).get_peers_from_package_json()
    add_ts_test = _get_test_runner_handlers()[test_runner]
    add_ts_test(unit, test_runner, test_files, deps, test_record)


@_with_report_configure_error
def on_validate_ts_test_for_args(unit, for_mod, root):
    # FBP-1085
    is_arc_root = root == "${ARCADIA_ROOT}"
    is_rel_for_mod = for_mod.startswith(".")

    if is_arc_root and is_rel_for_mod:
        ymake.report_configure_error(
            "You are using a relative path for a module. "
            + "You have to add RELATIVE key, like (RELATIVE {})".format(for_mod)
        )


@_with_report_configure_error
def on_set_ts_test_for_vars(unit, for_mod):
    unit.set(["TS_TEST_FOR", "yes"])
    unit.set(["TS_TEST_FOR_DIR", unit.resolve_arc_path(for_mod)])
    unit.set(["TS_TEST_FOR_PATH", rootrel_arc_src(for_mod, unit)])


def _add_ts_resources_to_test_record(unit, test_record):
    erm_json = _create_erm_json(unit)
    for tool in erm_json.list_npm_packages():
        tool_resource_label = "{}-ROOT-VAR-NAME".format(tool.upper())
        tool_resource_value = unit.get(tool_resource_label)
        if tool_resource_value:
            test_record[tool_resource_label] = tool_resource_value
    return test_record


@_with_report_configure_error
def on_ts_files(unit, *files):
    new_cmds = ['$COPY_CMD ${{input;context=TEXT:"{0}"}} ${{output;noauto:"{0}"}}'.format(f) for f in files]
    all_cmds = unit.get("_TS_FILES_COPY_CMD")
    if all_cmds:
        new_cmds.insert(0, all_cmds)
    unit.set(["_TS_FILES_COPY_CMD", " && ".join(new_cmds)])


@_with_report_configure_error
def on_ts_package_check_files(unit):
    ts_files = unit.get("_TS_FILES_COPY_CMD")
    if ts_files == "":
        ymake.report_configure_error(
            "\n"
            "In the TS_PACKAGE module, you should define at least one file using the TS_FILES() macro.\n"
            "Docs: https://docs.yandex-team.ru/frontend-in-arcadia/references/TS_PACKAGE#ts-files."
        )


@_with_report_configure_error
def on_depends_on_mod(unit):
    if unit.get("_TS_TEST_DEPENDS_ON_BUILD"):
        for_mod_path = unit.get("TS_TEST_FOR_PATH")
        unit.ondepends([for_mod_path])
