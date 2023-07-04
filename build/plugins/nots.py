import fnmatch
import os
import re

import ymake
import ytest
from _common import get_norm_unit_path, rootrel_arc_src, to_yesno

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
        return "\033[0;32m{}\033[0;49m \033[0;36m{}\033[0;49m".format(self.prefix, " ".join(parts))

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
            self.info(values)


logger = PluginLogger()


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


def _canonize_resource_name(name):
    # type: (str) -> str
    return re.sub(r"\W+", "_", name).strip("_").upper()


def _build_cmd_input_paths(paths, hide=False):
    # type: (list[str], bool) -> str
    return " ".join(["${{input{}:\"{}\"}}".format(";hide" if hide else "", p) for p in paths])


def _create_pm(unit):
    from lib.nots.package_manager import manager

    sources_path = unit.path()
    module_path = unit.get("MODDIR")
    if unit.get("TS_TEST_FOR"):
        sources_path = unit.get("TS_TEST_FOR_DIR")
        module_path = unit.get("TS_TEST_FOR_PATH")

    return manager(
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
def on_from_npm_lockfiles(unit, *args):
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
            unit.on_from_npm(
                [pkg.name, pkg.version, pkg.sky_id, pkg.integrity, pkg.integrity_algorithm, pkg.tarball_path]
            )
    except Exception as e:
        if unit.get("TS_RAISE") == "yes":
            raise e
        else:
            unit.message(["WARN", "on_from_npm_lockfiles exception: {}".format(e)])
            pass


@_with_report_configure_error
def on_peerdir_ts_resource(unit, *resources):
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)
    erm_json = _create_erm_json(unit)
    dirs = []

    nodejs_version = _select_matching_version(erm_json, "nodejs", pj.get_nodejs_version())

    for tool in resources:
        if tool == "nodejs":
            dirs.append(os.path.join("build", "platform", tool, str(nodejs_version)))
            _set_resource_vars(unit, erm_json, "nodejs", nodejs_version)
        elif erm_json.is_resource_multiplatform(tool):
            v = _select_matching_version(erm_json, tool, pj.get_dep_specifier(tool))
            sb_resources = [
                sbr for sbr in erm_json.get_sb_resources(tool, v) if sbr.get("nodejs") == nodejs_version.major
            ]
            nodejs_dir = "NODEJS_{}".format(nodejs_version.major)
            if len(sb_resources) > 0:
                dirs.append(os.path.join("build", "external_resources", tool, str(v), nodejs_dir))
                _set_resource_vars(unit, erm_json, tool, v, nodejs_version.major)
            else:
                unit.message(["WARN", "Missing {}@{} for {}".format(tool, str(v), nodejs_dir)])
        else:
            v = _select_matching_version(erm_json, tool, pj.get_dep_specifier(tool))
            dirs.append(os.path.join("build", "external_resources", tool, str(v)))
            _set_resource_vars(unit, erm_json, tool, v, nodejs_version.major)

    unit.onpeerdir(dirs)


@_with_report_configure_error
def on_ts_configure(unit, tsconfig_path):
    from lib.nots.package_manager.base import PackageJson
    from lib.nots.package_manager.base.utils import build_pj_path
    from lib.nots.typescript import TsConfig

    abs_tsconfig_path = unit.resolve(unit.resolve_arc_path(tsconfig_path))
    if not abs_tsconfig_path:
        raise Exception("tsconfig not found: {}".format(tsconfig_path))

    tsconfig = TsConfig.load(abs_tsconfig_path)
    cur_dir = unit.get("TS_TEST_FOR_PATH") if unit.get("TS_TEST_FOR") else unit.get("MODDIR")
    pj_path = build_pj_path(unit.resolve(unit.resolve_arc_path(cur_dir)))
    dep_paths = PackageJson.load(pj_path).get_dep_paths_by_names()
    config_files = tsconfig.inline_extend(dep_paths)

    mod_dir = unit.get("MODDIR")
    config_files = _resolve_module_files(unit, mod_dir, config_files)
    tsconfig.validate()

    unit.set(["TS_CONFIG_FILES", _build_cmd_input_paths(config_files, hide=True)])
    unit.set(["TS_CONFIG_ROOT_DIR", tsconfig.compiler_option("rootDir")])
    unit.set(["TS_CONFIG_OUT_DIR", tsconfig.compiler_option("outDir")])
    unit.set(["TS_CONFIG_SOURCE_MAP", to_yesno(tsconfig.compiler_option("sourceMap"))])
    unit.set(["TS_CONFIG_DECLARATION", to_yesno(tsconfig.compiler_option("declaration"))])
    unit.set(["TS_CONFIG_DECLARATION_MAP", to_yesno(tsconfig.compiler_option("declarationMap"))])
    unit.set(["TS_CONFIG_PRESERVE_JSX", to_yesno(tsconfig.compiler_option("jsx") == "preserve")])

    _setup_eslint(unit)


def _get_ts_test_data_dirs(unit):
    return list(
        set(
            [
                os.path.dirname(rootrel_arc_src(p, unit))
                for p in (ytest.get_values_list(unit, "_TS_TEST_DATA_VALUE") or [])
            ]
        )
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
    }


def _add_jest_ts_test(unit, test_runner, test_files, deps, test_record):
    test_record.update(
        {
            "CONFIG-PATH": _resolve_config_path(unit, test_runner, rel_to="TS_TEST_FOR_PATH"),
        }
    )
    _add_test(unit, test_runner, test_files, deps, test_record)


def _add_hermione_ts_test(unit, test_runner, test_files, deps, test_record):
    unit.on_ts_configure(unit.get("TS_CONFIG_PATH"))
    test_tags = list(set(["ya:fat", "ya:external"] + ytest.get_values_list(unit, "TEST_TAGS_VALUE")))
    test_requirements = list(set(["network:full"] + ytest.get_values_list(unit, "TEST_REQUIREMENTS_VALUE")))

    test_record.update(
        {
            "TS-ROOT-DIR": unit.get("TS_CONFIG_ROOT_DIR"),
            "TS-OUT-DIR": unit.get("TS_CONFIG_OUT_DIR"),
            "SIZE": "LARGE",
            "TAG": ytest.serialize_list(test_tags),
            "REQUIREMENTS": ytest.serialize_list(test_requirements),
            "CONFIG-PATH": _resolve_config_path(unit, test_runner, rel_to="MODDIR"),
        }
    )

    if not len(test_record["TS-TEST-DATA-DIRS"]):
        _add_default_hermione_test_data(unit, test_record)

    _add_test(unit, test_runner, test_files, deps, test_record)


def _add_default_hermione_test_data(unit, test_record):
    mod_dir = unit.get("MODDIR")
    root_dir = test_record["TS-ROOT-DIR"]
    out_dir = test_record["TS-OUT-DIR"]
    test_for_path = test_record["TS-TEST-FOR-PATH"]

    abs_root_dir = os.path.normpath(os.path.join(unit.resolve(unit.path()), root_dir))
    file_paths = _find_file_paths(abs_root_dir, "**/screens/*/*/*.png")
    file_dirs = [os.path.dirname(f) for f in file_paths]

    rename_from, rename_to = [
        os.path.relpath(os.path.normpath(os.path.join(mod_dir, d)), test_for_path) for d in [root_dir, out_dir]
    ]

    test_record.update(
        {
            "TS-TEST-DATA-DIRS": ytest.serialize_list(_resolve_module_files(unit, mod_dir, file_dirs)),
            "TS-TEST-DATA-DIRS-RENAME": "{}:{}".format(rename_from, rename_to),
        }
    )


def _setup_eslint(unit):
    if not _is_tests_enabled(unit):
        return

    if unit.get("_NO_LINT_VALUE") == "none":
        return

    lint_files = ytest.get_values_list(unit, "_TS_LINT_SRCS_VALUE")
    if not lint_files:
        return

    unit.on_peerdir_ts_resource("eslint")

    mod_dir = unit.get("MODDIR")
    lint_files = _resolve_module_files(unit, mod_dir, lint_files)
    deps = _create_pm(unit).get_peers_from_package_json()
    test_record = {
        "ESLINT-ROOT-VAR-NAME": unit.get("ESLINT-ROOT-VAR-NAME"),
        "ESLINT_CONFIG_PATH": _resolve_config_path(unit, "eslint", rel_to="MODDIR"),
        "LINT-FILE-PROCESSING-TIME": str(ESLINT_FILE_PROCESSING_TIME_DEFAULT),
    }

    _add_test(unit, "eslint", lint_files, deps, test_record, mod_dir)


def _resolve_module_files(unit, mod_dir, file_paths):
    resolved_files = []

    for path in file_paths:
        resolved = rootrel_arc_src(path, unit)
        if resolved.startswith(mod_dir):
            mod_dir_with_sep_len = len(mod_dir) + 1
            resolved = resolved[mod_dir_with_sep_len:]
        resolved_files.append(resolved)

    return resolved_files


def _find_file_paths(abs_path, pattern):
    file_paths = []
    _, ext = os.path.splitext(pattern)

    for root, _, filenames in os.walk(abs_path):
        if not any(f.endswith(ext) for f in filenames):
            continue

        abs_file_paths = [os.path.join(root, f) for f in filenames]

        for file_path in fnmatch.filter(abs_file_paths, pattern):
            file_paths.append(file_path)

    return file_paths


def _add_test(unit, test_type, test_files, deps=None, test_record=None, test_cwd=None):
    from lib.nots.package_manager import constants

    def sort_uniq(text):
        return list(sorted(set(text)))

    if deps:
        unit.ondepends(sort_uniq(deps))

    test_dir = get_norm_unit_path(unit)
    full_test_record = {
        "TEST-NAME": test_type.lower(),
        "TEST-TIMEOUT": unit.get("TEST_TIMEOUT") or "",
        "TEST-ENV": ytest.prepare_env(unit.get("TEST_ENV_VALUE")),
        "TESTED-PROJECT-NAME": os.path.splitext(unit.filename())[0],
        "TEST-RECIPES": ytest.prepare_recipes(unit.get("TEST_RECIPES_VALUE")),
        "SCRIPT-REL-PATH": test_type,
        "SOURCE-FOLDER-PATH": test_dir,
        "BUILD-FOLDER-PATH": test_dir,
        "BINARY-PATH": os.path.join(test_dir, unit.filename()),
        "SPLIT-FACTOR": unit.get("TEST_SPLIT_FACTOR") or "",
        "FORK-MODE": unit.get("TEST_FORK_MODE") or "",
        "SIZE": unit.get("TEST_SIZE_NAME") or "",
        "TEST-FILES": ytest.serialize_list(test_files),
        "TEST-CWD": test_cwd or "",
        "TAG": ytest.serialize_list(ytest.get_values_list(unit, "TEST_TAGS_VALUE")),
        "REQUIREMENTS": ytest.serialize_list(ytest.get_values_list(unit, "TEST_REQUIREMENTS_VALUE")),
        "NODEJS-ROOT-VAR-NAME": unit.get("NODEJS-ROOT-VAR-NAME"),
        "NODE-MODULES-BUNDLE-FILENAME": constants.NODE_MODULES_WORKSPACE_BUNDLE_FILENAME,
        "CUSTOM-DEPENDENCIES": " ".join(sort_uniq((deps or []) + ytest.get_values_list(unit, "TEST_DEPENDS_VALUE"))),
    }

    if test_record:
        full_test_record.update(test_record)

    for k, v in full_test_record.items():
        if not isinstance(v, str):
            logger.warn(k, "expected 'str', got:", type(v))

    data = ytest.dump_test(unit, full_test_record)
    if data:
        unit.set_property(["DART_DATA", data])


def _set_resource_vars(unit, erm_json, resource_name, version, nodejs_major=None):
    # type: (any, ErmJsonLite, Version, str|None, int|None) -> None

    # example: hermione -> HERMIONE, super-package -> SUPER_PACKAGE
    canon_resource_name = _canonize_resource_name(resource_name)

    # example: NODEJS_12_18_4 | HERMIONE_7_0_4_NODEJS_18
    version_str = str(version).replace(".", "_")
    yamake_resource_name = "{}_{}".format(canon_resource_name, version_str)

    if erm_json.is_resource_multiplatform(resource_name):
        yamake_resource_name += "_NODEJS_{}".format(nodejs_major)

    yamake_resource_var = "{}_RESOURCE_GLOBAL".format(yamake_resource_name)

    unit.set(["{}_ROOT".format(canon_resource_name), "${}".format(yamake_resource_var)])
    unit.set(["{}-ROOT-VAR-NAME".format(canon_resource_name), yamake_resource_var])


def _select_matching_version(erm_json, resource_name, range_str):
    # type: (ErmJsonLite, str, str) -> Version
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
                map(str, toolchain_versions),
                "https://nda.ya.ru/t/ulU4f5Ru5egzHV",
                str(error),
            )
        )


@_with_report_configure_error
def on_node_modules_configure(unit):
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)

    if pj.has_dependencies():
        unit.onpeerdir(pm.get_local_peers_from_package_json())
        message_level = "ERROR" if unit.get("TS_RAISE") == "yes" else "WARN"
        errors, ins, outs = pm.calc_node_modules_inouts()

        for err in errors:
            unit.message([message_level, "calc_node_modules_inouts exception: {}".format(err)])

        unit.on_set_node_modules_ins_outs(["IN"] + sorted(ins) + ["OUT"] + sorted(outs))
    else:
        # default "noop" command
        unit.set(["_NODE_MODULES_CMD", "$TOUCH_UNIT"])


@_with_report_configure_error
def on_set_node_modules_bundle_as_output(unit):
    pm = _create_pm(unit)
    pj = pm.load_package_json_from_dir(pm.sources_path)
    if pj.has_dependencies():
        unit.set(["NODE_MODULES_BUNDLE_AS_OUTPUT", '${output;hide:"workspace_node_modules.tar"}'])


@_with_report_configure_error
def on_ts_test_for_configure(unit, test_runner, default_config):
    if not _is_tests_enabled(unit):
        return

    if unit.enabled('TS_COVERAGE'):
        unit.on_peerdir_ts_resource("nyc")

    for_mod_path = unit.get("TS_TEST_FOR_PATH")
    unit.onpeerdir([for_mod_path])
    unit.on_setup_extract_node_modules_recipe([for_mod_path])
    unit.on_setup_extract_peer_tars_recipe([for_mod_path])

    unit.set(["TS_TEST_NM", os.path.join(("$B"), for_mod_path, "node_modules.tar")])

    config_path = unit.get("TS_TEST_CONFIG_PATH")
    if not config_path:
        config_path = os.path.join(for_mod_path, default_config)
        unit.set(["TS_TEST_CONFIG_PATH", config_path])

    test_record = _add_ts_resources_to_test_record(
        unit,
        {
            "TS-TEST-FOR-PATH": for_mod_path,
            "TS-TEST-DATA-DIRS": ytest.serialize_list(_get_ts_test_data_dirs(unit)),
            "TS-TEST-DATA-DIRS-RENAME": unit.get("_TS_TEST_DATA_DIRS_RENAME_VALUE"),
        },
    )

    test_files = ytest.get_values_list(unit, "_TS_TEST_SRCS_VALUE")
    test_files = _resolve_module_files(unit, unit.get("MODDIR"), test_files)
    if not test_files:
        ymake.report_configure_error("No tests found")
        return

    deps = _create_pm(unit).get_peers_from_package_json()
    add_ts_test = _get_test_runner_handlers()[test_runner]
    add_ts_test(unit, test_runner, test_files, deps, test_record)


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
