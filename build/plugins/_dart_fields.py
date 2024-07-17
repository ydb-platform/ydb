import base64
import functools
import json
import operator
import os
import re
import shlex
import six
import sys
from functools import reduce

import _common
import lib.test_const as consts
import ymake


CANON_RESULT_FILE_NAME = 'result.json'
CANON_DATA_DIR_NAME = 'canondata'
CANON_OUTPUT_STORAGE = 'canondata_storage'

KTLINT_CURRENT_EDITOR_CONFIG = "arcadia/build/platform/java/ktlint/.editorconfig"
KTLINT_OLD_EDITOR_CONFIG = "arcadia/build/platform/java/ktlint_old/.editorconfig"


class DartValueError(ValueError):
    pass


def create_dart_record(fields, *args):
    return reduce(operator.or_, (value for field in fields if (value := field(*args))), {})


def with_fields(fields):
    def inner(func):
        @functools.wraps(func)
        def innermost(*args, **kwargs):
            func(fields, *args, **kwargs)

        return innermost

    return inner


def serialize_list(lst):
    lst = list(filter(None, lst))
    return '\"' + ';'.join(lst) + '\"' if lst else ''


def deserialize_list(val):
    return list(filter(None, val.replace('"', "").split(";")))


def get_unit_list_variable(unit, name):
    items = unit.get(name)
    if items:
        items = items.split(' ')
        assert items[0] == "${}".format(name), (items, name)
        return items[1:]
    return []


def get_values_list(unit, key):
    res = map(str.strip, (unit.get(key) or '').replace('$' + key, '').strip().split())
    return [r for r in res if r and r not in ['""', "''"]]


def _get_test_tags(unit, spec_args=None):
    if spec_args is None:
        spec_args = {}
    tags = spec_args.get('TAG', []) + get_values_list(unit, 'TEST_TAGS_VALUE')
    tags = set(tags)
    if unit.get('EXPORT_SEM') == 'yes':
        filter_only_tags = sorted(t for t in tags if ':' not in t)
        unit.set(['FILTER_ONLY_TEST_TAGS', ' '.join(filter_only_tags)])
    # DEVTOOLS-7571
    if unit.get('SKIP_TEST_VALUE') and consts.YaTestTags.Fat in tags:
        tags.add(consts.YaTestTags.NotAutocheck)

    return tags


def format_recipes(data: str | None) -> str:
    if not data:
        return ""

    data = data.replace('"USE_RECIPE_DELIM"', "\n")
    data = data.replace("$TEST_RECIPES_VALUE", "")
    return data


def prepare_recipes(data: str | None) -> bytes:
    formatted = format_recipes(data)
    return base64.b64encode(six.ensure_binary(formatted))


def prepare_env(data):
    data = data.replace("$TEST_ENV_VALUE", "")
    return serialize_list(shlex.split(data))


def get_norm_paths(unit, key):
    # return paths without trailing (back)slash
    return [x.rstrip('\\/').replace('${ARCADIA_ROOT}/', '') for x in get_values_list(unit, key)]


def _load_canonical_file(filename, unit_path):
    try:
        with open(filename, 'rb') as results_file:
            return json.load(results_file)
    except Exception as e:
        print("malformed canonical data in {}: {} ({})".format(unit_path, e, filename), file=sys.stderr)
        return {}


def _get_resource_from_uri(uri):
    m = consts.CANON_MDS_RESOURCE_REGEX.match(uri)
    if m:
        key = m.group(1)
        return "{}:{}".format(consts.MDS_SCHEME, key)

    m = consts.CANON_BACKEND_RESOURCE_REGEX.match(uri)
    if m:
        key = m.group(1)
        return "{}:{}".format(consts.MDS_SCHEME, key)

    m = consts.CANON_SBR_RESOURCE_REGEX.match(uri)
    if m:
        # There might be conflict between resources, because all resources in sandbox have 'resource.tar.gz' name
        # That's why we use notation with '=' to specify specific path for resource
        uri = m.group(1)
        res_id = m.group(2)
        return "{}={}".format(uri, '/'.join([CANON_OUTPUT_STORAGE, res_id]))


def _get_external_resources_from_canon_data(data):
    # Method should work with both canonization versions:
    #   result.json: {'uri':X 'checksum':Y}
    #   result.json: {'testname': {'uri':X 'checksum':Y}}
    #   result.json: {'testname': [{'uri':X 'checksum':Y}]}
    # Also there is a bug - if user returns {'uri': 1} from test - machinery will fail
    # That's why we check 'uri' and 'checksum' fields presence
    # (it's still a bug - user can return {'uri':X, 'checksum': Y}, we need to unify canonization format)
    res = set()

    if isinstance(data, dict):
        if 'uri' in data and 'checksum' in data:
            resource = _get_resource_from_uri(data['uri'])
            if resource:
                res.add(resource)
        else:
            for k, v in six.iteritems(data):
                res.update(_get_external_resources_from_canon_data(v))
    elif isinstance(data, list):
        for e in data:
            res.update(_get_external_resources_from_canon_data(e))

    return res


def _get_canonical_data_resources_v2(filename, unit_path):
    return (_get_external_resources_from_canon_data(_load_canonical_file(filename, unit_path)), [filename])


def get_canonical_test_resources(unit):
    unit_path = unit.path()
    if unit.get("CUSTOM_CANONDATA_PATH"):
        path_to_canondata = unit_path.replace("$S", unit.get("CUSTOM_CANONDATA_PATH"))
    else:
        path_to_canondata = unit.resolve(unit_path)
    canon_data_dir = os.path.join(path_to_canondata, CANON_DATA_DIR_NAME, unit.get('CANONIZE_SUB_PATH') or '')
    try:
        _, dirs, files = next(os.walk(canon_data_dir))
    except StopIteration:
        # path doesn't exist
        return [], []
    if CANON_RESULT_FILE_NAME in files:
        return _get_canonical_data_resources_v2(os.path.join(canon_data_dir, CANON_RESULT_FILE_NAME), unit_path)
    return [], []


def java_srcdirs_to_data(unit, var):
    extra_data = []
    for srcdir in (unit.get(var) or '').replace('$' + var, '').split():
        if srcdir == '.':
            srcdir = unit.get('MODDIR')
        if srcdir.startswith('${ARCADIA_ROOT}/') or srcdir.startswith('$ARCADIA_ROOT/'):
            srcdir = srcdir.replace('${ARCADIA_ROOT}/', '$S/')
            srcdir = srcdir.replace('$ARCADIA_ROOT/', '$S/')
        if srcdir.startswith('${CURDIR}') or srcdir.startswith('$CURDIR'):
            srcdir = srcdir.replace('${CURDIR}', os.path.join('$S', unit.get('MODDIR')))
            srcdir = srcdir.replace('$CURDIR', os.path.join('$S', unit.get('MODDIR')))
        srcdir = unit.resolve_arc_path(srcdir)
        if not srcdir.startswith('$'):
            srcdir = os.path.join('$S', unit.get('MODDIR'), srcdir)
        if srcdir.startswith('$S'):
            extra_data.append(srcdir.replace('$S', 'arcadia'))
    return serialize_list(extra_data)


def extract_java_system_properties(unit, args):
    if len(args) % 2:
        return [], 'Wrong use of SYSTEM_PROPERTIES in {}: odd number of arguments'.format(unit.path())

    props = []
    for x, y in zip(args[::2], args[1::2]):
        if x == 'FILE':
            if y.startswith('${BINDIR}') or y.startswith('${ARCADIA_BUILD_ROOT}') or y.startswith('/'):
                return [], 'Wrong use of SYSTEM_PROPERTIES in {}: absolute/build file path {}'.format(unit.path(), y)

            y = _common.rootrel_arc_src(y, unit)
            if not os.path.exists(unit.resolve('$S/' + y)):
                return [], 'Wrong use of SYSTEM_PROPERTIES in {}: can\'t resolve {}'.format(unit.path(), y)

            y = '${ARCADIA_ROOT}/' + y
            props.append({'type': 'file', 'path': y})
        else:
            props.append({'type': 'inline', 'key': x, 'value': y})

    return props, None


def _create_erm_json(unit):
    from lib.nots.erm_json_lite import ErmJsonLite

    erm_packages_path = unit.get("ERM_PACKAGES_PATH")
    path = unit.resolve(unit.resolve_arc_path(erm_packages_path))

    return ErmJsonLite.load(path)


def _resolve_module_files(unit, mod_dir, file_paths):
    mod_dir_with_sep_len = len(mod_dir) + 1
    resolved_files = []

    for path in file_paths:
        resolved = _common.rootrel_arc_src(path, unit)
        if resolved.startswith(mod_dir):
            resolved = resolved[mod_dir_with_sep_len:]
        resolved_files.append(resolved)

    return resolved_files


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


def _resolve_config_path(unit, test_runner, rel_to):
    config_path = unit.get("ESLINT_CONFIG_PATH") if test_runner == "eslint" else unit.get("TS_TEST_CONFIG_PATH")
    arc_config_path = unit.resolve_arc_path(config_path)
    abs_config_path = unit.resolve(arc_config_path)
    if not abs_config_path:
        raise Exception("{} config not found: {}".format(test_runner, config_path))

    unit.onsrcs([arc_config_path])
    abs_rel_to = unit.resolve(unit.resolve_arc_path(unit.get(rel_to)))
    return os.path.relpath(abs_config_path, start=abs_rel_to)


def _get_ts_test_data_dirs(unit):
    return sorted(
        set(
            [
                os.path.dirname(_common.rootrel_arc_src(p, unit))
                for p in (get_values_list(unit, "_TS_TEST_DATA_VALUE") or [])
            ]
        )
    )


class AndroidApkTestActivity:
    KEY = 'ANDROID_APK_TEST_ACTIVITY'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('ANDROID_APK_TEST_ACTIVITY_VALUE')}


class BenchmarkOpts:
    KEY = 'BENCHMARK-OPTS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: serialize_list(get_unit_list_variable(unit, 'BENCHMARK_OPTS_VALUE'))}


class BinaryPath:
    KEY = 'BINARY-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        unit_path = _common.get_norm_unit_path(unit)
        return {cls.KEY: "{}/{}".format(unit_path, unit.filename())}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        unit_path = unit.path()
        binary_path = os.path.join(unit_path, unit.filename())
        if binary_path:
            return {cls.KEY: _common.strip_roots(binary_path)}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        value = _common.strip_roots(os.path.join(unit.path(), unit.filename()).replace(".pkg", ""))
        return {cls.KEY: value}

    # TODO replace with `value`
    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        test_dir = _common.get_norm_unit_path(unit)
        return {cls.KEY: os.path.join(test_dir, unit.filename())}


class Blob:
    KEY = 'BLOB'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_BLOB_DATA')}


class BuildFolderPath:
    KEY = 'BUILD-FOLDER-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: _common.get_norm_unit_path(unit)}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: _common.strip_roots(unit.path())}


class CanonizeSubPath:
    KEY = 'CANONIZE_SUB_PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('CANONIZE_SUB_PATH')}


class Classpath:
    KEY = 'CLASSPATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        ymake_java_test = unit.get('YMAKE_JAVA_TEST') == 'yes'
        if ymake_java_test:
            value = '$B/{}/{}.jar ${{DART_CLASSPATH}}'.format(unit.get('MODDIR'), unit.get('REALPRJNAME'))
            return {cls.KEY: value}


class ConfigPath:
    KEY = 'CONFIG-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_runner, rel_to = flat_args
        return {cls.KEY: _resolve_config_path(unit, test_runner, rel_to=rel_to)}


class CustomDependencies:
    KEY = 'CUSTOM-DEPENDENCIES'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        custom_deps = ' '.join(spec_args.get('DEPENDS', []) + get_values_list(unit, 'TEST_DEPENDS_VALUE'))
        return {cls.KEY: custom_deps}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: " ".join(spec_args.get('DEPENDS', []))}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        custom_deps = get_values_list(unit, 'TEST_DEPENDS_VALUE')
        return {cls.KEY: " ".join(custom_deps)}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        deps = []
        _, linter = flat_args
        deps.append(os.path.dirname(linter))
        deps += spec_args.get('DEPENDS', [])
        return {cls.KEY: " ".join(deps)}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        deps = _create_pm(unit).get_peers_from_package_json()
        recipes_lines = format_recipes(unit.get("TEST_RECIPES_VALUE")).strip().splitlines()
        if recipes_lines:
            deps = deps or []
            deps.extend([os.path.dirname(r.strip().split(" ")[0]) for r in recipes_lines])

        return {cls.KEY: " ".join(deps)}


class EslintConfigPath:
    KEY = 'ESLINT_CONFIG_PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_runner, rel_to = flat_args
        return {cls.KEY: _resolve_config_path(unit, test_runner, rel_to=rel_to)}


class ForkMode:
    KEY = 'FORK-MODE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        fork_mode = []
        if 'FORK_SUBTESTS' in spec_args:
            fork_mode.append('subtests')
        if 'FORK_TESTS' in spec_args:
            fork_mode.append('tests')
        fork_mode = fork_mode or spec_args.get('FORK_MODE', []) or unit.get('TEST_FORK_MODE').split()
        fork_mode = ' '.join(fork_mode) if fork_mode else ''
        return {cls.KEY: fork_mode}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_FORK_MODE')}


class ForkTestFiles:
    KEY = 'FORK-TEST-FILES'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('FORK_TEST_FILES_MODE')}


class FuzzDicts:
    KEY = 'FUZZ-DICTS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = serialize_list(spec_args.get('FUZZ_DICTS', []) + get_unit_list_variable(unit, 'FUZZ_DICTS_VALUE'))
        return {cls.KEY: value}


class FuzzOpts:
    KEY = 'FUZZ-OPTS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = serialize_list(spec_args.get('FUZZ_OPTS', []) + get_unit_list_variable(unit, 'FUZZ_OPTS_VALUE'))
        return {cls.KEY: value}


class Fuzzing:
    KEY = 'FUZZING'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        if unit.get('FUZZING') == 'yes':
            return {cls.KEY: '1'}


class GlobalLibraryPath:
    KEY = 'GLOBAL-LIBRARY-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.global_filename()}


class GoBenchTimeout:
    KEY = 'GO_BENCH_TIMEOUT'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('GO_BENCH_TIMEOUT')}


class IgnoreClasspathClash:
    KEY = 'IGNORE_CLASSPATH_CLASH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = ' '.join(get_values_list(unit, 'JAVA_IGNORE_CLASSPATH_CLASH_VALUE'))
        return {cls.KEY: value}


class JavaClasspathCmdType:
    KEY = 'JAVA_CLASSPATH_CMD_TYPE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        java_cp_arg_type = unit.get('JAVA_CLASSPATH_CMD_TYPE_VALUE') or 'MANIFEST'
        if java_cp_arg_type not in ('MANIFEST', 'COMMAND_FILE', 'LIST'):
            # TODO move error reporting out of field classes
            ymake.report_configure_error(
                '{}: TEST_JAVA_CLASSPATH_CMD_TYPE({}) are invalid. Choose argument from MANIFEST, COMMAND_FILE or LIST)'.format(
                    unit.path(), java_cp_arg_type
                )
            )
            raise DartValueError
        return {cls.KEY: java_cp_arg_type}


class JdkForTests:
    KEY = 'JDK_FOR_TESTS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = 'JDK' + (unit.get('JDK_VERSION') or unit.get('JDK_REAL_VERSION') or '_DEFAULT') + '_FOR_TESTS'
        return {cls.KEY: value}


class JdkLatestVersion:
    KEY = 'JDK_LATEST_VERSION'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('JDK_LATEST_VERSION')}


class JdkResource:
    KEY = 'JDK_RESOURCE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = 'JDK' + (unit.get('JDK_VERSION') or unit.get('JDK_REAL_VERSION') or '_DEFAULT')
        return {cls.KEY: value}


class KtlintBaselineFile:
    KEY = 'KTLINT_BASELINE_FILE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        if unit.get('_USE_KTLINT_OLD') != 'yes':
            baseline_path_relative = unit.get('_KTLINT_BASELINE_FILE')
            if baseline_path_relative:
                return {cls.KEY: baseline_path_relative}


class KtlintBinary:
    KEY = 'KTLINT_BINARY'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = '$(KTLINT_OLD)/run.bat' if unit.get('_USE_KTLINT_OLD') == 'yes' else '$(KTLINT)/run.bat'
        return {cls.KEY: value}


class LintFileProcessingTime:
    KEY = 'LINT-FILE-PROCESSING-TIME'


class ModuleLang:
    KEY = 'MODULE_LANG'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("MODULE_LANG").lower() or consts.ModuleLang.UNKNOWN}


class ModuleType:
    KEY = 'MODULE_TYPE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('MODULE_TYPE')}


class NoCheck:
    KEY = 'NO-CHECK'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        if unit.get('NO_CHECK_IMPORTS_FOR_VALUE') != "None":
            value = serialize_list(get_values_list(unit, 'NO_CHECK_IMPORTS_FOR_VALUE') or ["*"])
            return {cls.KEY: value}


class NodejsRootVarName:
    KEY = 'NODEJS-ROOT-VAR-NAME'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("NODEJS-ROOT-VAR-NAME")}


class NodeModulesBundleFilename:
    KEY = 'NODE-MODULES-BUNDLE-FILENAME'


class PythonPaths:
    KEY = 'PYTHON-PATHS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        python_paths = get_values_list(unit, 'TEST_PYTHON_PATH_VALUE')
        return {cls.KEY: serialize_list(python_paths)}


class Requirements:
    KEY = 'REQUIREMENTS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_requirements = spec_args.get('REQUIREMENTS', []) + get_values_list(unit, 'TEST_REQUIREMENTS_VALUE')
        return {cls.KEY: serialize_list(test_requirements)}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        test_requirements = serialize_list(
            spec_args.get('REQUIREMENTS', []) + get_values_list(unit, 'TEST_REQUIREMENTS_VALUE')
        )
        if unit.get('FUZZING') == 'yes':
            value = serialize_list(filter(None, deserialize_list(test_requirements) + ["cpu:all", "ram:all"]))
            return {cls.KEY: value}
        else:
            return {cls.KEY: test_requirements}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        value = " ".join(spec_args.get('REQUIREMENTS', []))
        return {cls.KEY: value}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        requirements = get_values_list(unit, 'TEST_REQUIREMENTS_VALUE')
        return {cls.KEY: serialize_list(requirements)}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        requirements = sorted(set(["network:full"] + get_values_list(unit, "TEST_REQUIREMENTS_VALUE")))
        return {cls.KEY: serialize_list(requirements)}


class SbrUidExt:
    KEY = 'SBR-UID-EXT'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        uid_ext = unit.get("SBR_UID_EXT").split(" ", 1)[-1]  # strip variable name
        return {cls.KEY: uid_ext}


class ScriptRelPath:
    KEY = 'SCRIPT-REL-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: flat_args[1]}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: flat_args[0]}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        return {cls.KEY: 'py3test.bin' if (unit.get("PYTHON3") == 'yes') else "pytest.bin"}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        return {cls.KEY: 'junit5.test' if unit.get('MODULE_TYPE') == 'JUNIT5' else 'junit.test'}


class Size:
    KEY = 'SIZE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: ''.join(spec_args.get('SIZE', [])) or unit.get('TEST_SIZE_NAME')}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_SIZE_NAME')}


class SkipTest:
    KEY = 'SKIP_TEST'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('SKIP_TEST_VALUE')}


class SourceFolderPath:
    KEY = 'SOURCE-FOLDER-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: _common.get_norm_unit_path(unit)}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        test_dir = _common.get_norm_unit_path(unit)
        test_files = flat_args[1:]
        if test_files:
            test_dir = os.path.dirname(test_files[0]).lstrip("$S/")
        return {cls.KEY: test_dir}


class SplitFactor:
    KEY = 'SPLIT-FACTOR'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = ''.join(spec_args.get('SPLIT_FACTOR', [])) or unit.get('TEST_SPLIT_FACTOR')
        return {cls.KEY: value}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_SPLIT_FACTOR')}


class Tag:
    KEY = 'TAG'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        tags = serialize_list(sorted(_get_test_tags(unit, spec_args)))
        return {cls.KEY: tags}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        tags = serialize_list(get_values_list(unit, "TEST_TAGS_VALUE"))
        return {cls.KEY: tags}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        tags = sorted(set(["ya:fat", "ya:external", "ya:noretries"] + get_values_list(unit, "TEST_TAGS_VALUE")))
        return {cls.KEY: serialize_list(tags)}


class TestClasspath:
    KEY = 'TEST_CLASSPATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_classpath_origins = unit.get('TEST_CLASSPATH_VALUE')
        ymake_java_test = unit.get('YMAKE_JAVA_TEST') == 'yes'
        if test_classpath_origins:
            value = '${TEST_CLASSPATH_MANAGED}'
            return {cls.KEY: value}
        elif ymake_java_test:
            value = '${DART_CLASSPATH}'
            return {cls.KEY: value}


class TestClasspathDeps:
    KEY = 'TEST_CLASSPATH_DEPS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_classpath_origins = unit.get('TEST_CLASSPATH_VALUE')
        ymake_java_test = unit.get('YMAKE_JAVA_TEST') == 'yes'
        if not test_classpath_origins and ymake_java_test:
            return {cls.KEY: '${DART_CLASSPATH_DEPS}'}


class TestClasspathOrigins:
    KEY = 'TEST_CLASSPATH_ORIGINS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_classpath_origins = unit.get('TEST_CLASSPATH_VALUE')
        if test_classpath_origins:
            return {cls.KEY: test_classpath_origins}


class TestCwd:
    KEY = 'TEST-CWD'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_cwd = unit.get('TEST_CWD_VALUE')  # TODO: validate test_cwd value
        return {cls.KEY: test_cwd}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        test_cwd = unit.get('TEST_CWD_VALUE') or ''
        if test_cwd:
            test_cwd = test_cwd.replace("$TEST_CWD_VALUE", "").replace('"MACRO_CALLS_DELIM"', "").strip()
        return {cls.KEY: test_cwd}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("MODDIR")}


class TestData:
    KEY = 'TEST-DATA'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_data = sorted(
            _common.filter_out_by_keyword(
                spec_args.get('DATA', []) + get_norm_paths(unit, 'TEST_DATA_VALUE'), 'AUTOUPDATED'
            )
        )
        return {cls.KEY: serialize_list(test_data)}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        test_data = sorted(
            _common.filter_out_by_keyword(
                spec_args.get('DATA', []) + get_norm_paths(unit, 'TEST_DATA_VALUE'), 'AUTOUPDATED'
            )
        )

        data, _ = get_canonical_test_resources(unit)
        test_data += data
        value = serialize_list(sorted(test_data))
        return {cls.KEY: value}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        if unit.get('_USE_KTLINT_OLD') == 'yes':
            extra_test_data = serialize_list([KTLINT_OLD_EDITOR_CONFIG])
        else:
            data_list = [KTLINT_CURRENT_EDITOR_CONFIG]
            baseline_path_relative = unit.get('_KTLINT_BASELINE_FILE')
            if baseline_path_relative:
                baseline_path = unit.resolve_arc_path(baseline_path_relative).replace('$S', 'arcadia')
                data_list += [baseline_path]
            extra_test_data = serialize_list(data_list)
        return {cls.KEY: extra_test_data}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        ymake_java_test = unit.get('YMAKE_JAVA_TEST') == 'yes'
        if ymake_java_test:
            return {cls.KEY: java_srcdirs_to_data(unit, 'ALL_SRCDIRS')}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        test_data = get_norm_paths(unit, 'TEST_DATA_VALUE')
        data, _ = get_canonical_test_resources(unit)
        test_data += data
        value = serialize_list(sorted(_common.filter_out_by_keyword(test_data, 'AUTOUPDATED')))
        return {cls.KEY: value}

    @classmethod
    def value6(cls, unit, flat_args, spec_args):
        test_data = get_norm_paths(unit, 'TEST_DATA_VALUE')
        test_data.append('arcadia/build/scripts/run_junit.py')
        test_data.append('arcadia/build/scripts/unpacking_jtest_runner.py')

        data, _ = get_canonical_test_resources(unit)
        test_data += data

        props, error_mgs = extract_java_system_properties(unit, get_values_list(unit, 'SYSTEM_PROPERTIES_VALUE'))
        if error_mgs:
            # TODO move error reporting out of field classes
            ymake.report_configure_error(error_mgs)
            raise DartValueError
        for prop in props:
            if prop['type'] == 'file':
                test_data.append(prop['path'].replace('${ARCADIA_ROOT}', 'arcadia'))
        value = serialize_list(sorted(_common.filter_out_by_keyword(test_data, 'AUTOUPDATED')))
        return {cls.KEY: value}

    @classmethod
    def value7(cls, unit, flat_args, spec_args):
        return {cls.KEY: serialize_list(get_values_list(unit, "TEST_DATA_VALUE"))}


class TsConfigPath:
    KEY = 'TS_CONFIG_PATH'


class TsTestDataDirs:
    KEY = 'TS-TEST-DATA-DIRS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = serialize_list(_get_ts_test_data_dirs(unit))
        return {cls.KEY: value}


class TsTestDataDirsRename:
    KEY = 'TS-TEST-DATA-DIRS-RENAME'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("_TS_TEST_DATA_DIRS_RENAME_VALUE")}


class TsTestForPath:
    KEY = 'TS-TEST-FOR-PATH'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("TS_TEST_FOR_PATH")}


class TestedProjectFilename:
    KEY = 'TESTED-PROJECT-FILENAME'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.filename()}


class TestedProjectName:
    KEY = 'TESTED-PROJECT-NAME'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.name()}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        test_dir = _common.get_norm_unit_path(unit)
        return {cls.KEY: os.path.basename(test_dir)}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        test_dir = _common.get_norm_unit_path(unit)
        test_files = flat_args[1:]
        if test_files:
            test_dir = os.path.dirname(test_files[0]).lstrip("$S/")
        return {cls.KEY: os.path.basename(test_dir)}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        binary_path = os.path.join(unit.path(), unit.filename())
        return {cls.KEY: os.path.basename(binary_path)}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        return {cls.KEY: _common.get_norm_unit_path(unit)}

    @classmethod
    def value6(cls, unit, flat_args, spec_args):
        value = os.path.basename(os.path.join(unit.path(), unit.filename()).replace(".pkg", ""))
        return {cls.KEY: value}

    @classmethod
    def value7(cls, unit, flat_args, spec_args):
        return {cls.KEY: os.path.splitext(unit.filename())[0]}


class TestFiles:
    KEY = 'TEST-FILES'
    # TODO remove FILES, see DEVTOOLS-7052
    KEY2 = 'FILES'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        data_re = re.compile(r"sbr:/?/?(\d+)=?.*")
        data = flat_args[1:]
        resources = []
        for f in data:
            matched = re.match(data_re, f)
            if matched:
                resources.append(matched.group(1))
        value = serialize_list(resources)
        return {cls.KEY: value, cls.KEY2: value}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        value = serialize_list(flat_args[1:])
        return {cls.KEY: value, cls.KEY2: value}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        test_files = flat_args[1:]
        check_level = flat_args[1]
        allowed_levels = {
            'base': '/yandex_checks.xml',
            'strict': '/yandex_checks_strict.xml',
            'extended': '/yandex_checks_extended.xml',
            'library': '/yandex_checks_library.xml',
        }
        if check_level not in allowed_levels:
            raise Exception("'{}' is not allowed in LINT(), use one of {}".format(check_level, allowed_levels.keys()))
        test_files[0] = allowed_levels[check_level]
        value = serialize_list(test_files)
        return {cls.KEY: value, cls.KEY2: value}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        value = serialize_list([_common.get_norm_unit_path(unit, unit.filename())])
        return {cls.KEY: value, cls.KEY2: value}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        test_files = get_values_list(unit, 'TEST_SRCS_VALUE')
        return {cls.KEY: serialize_list(test_files)}

    @classmethod
    def value6(cls, unit, flat_args, spec_args):
        test_files = get_values_list(unit, "_TS_TEST_SRCS_VALUE")
        test_files = _resolve_module_files(unit, unit.get("MODDIR"), test_files)
        return {cls.KEY: serialize_list(test_files)}

    @classmethod
    def value7(cls, unit, flat_args, spec_args):
        typecheck_files = get_values_list(unit, "TS_INPUT_FILES")
        test_files = [_common.resolve_common_const(f) for f in typecheck_files]
        return {cls.KEY: serialize_list(test_files)}


class TestEnv:
    KEY = 'TEST-ENV'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: prepare_env(unit.get("TEST_ENV_VALUE"))}


class TestIosDeviceType:
    KEY = 'TEST_IOS_DEVICE_TYPE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_IOS_DEVICE_TYPE_VALUE')}


class TestIosRuntimeType:
    KEY = 'TEST_IOS_RUNTIME_TYPE'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_IOS_RUNTIME_TYPE_VALUE')}


class TestJar:
    KEY = 'TEST_JAR'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_classpath_origins = unit.get('TEST_CLASSPATH_VALUE')
        ymake_java_test = unit.get('YMAKE_JAVA_TEST') == 'yes'
        if not test_classpath_origins and ymake_java_test:
            if unit.get('UNITTEST_DIR'):
                value = '${UNITTEST_MOD}'
            else:
                value = '{}/{}.jar'.format(unit.get('MODDIR'), unit.get('REALPRJNAME'))
            return {cls.KEY: value}


class TestName:
    KEY = 'TEST-NAME'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: flat_args[0]}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        return {cls.KEY: flat_args[0] + '_bench'}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        return {cls.KEY: flat_args[0].lower()}

    @classmethod
    def value4(cls, unit, flat_args, spec_args):
        unit_path = unit.path()
        binary_path = os.path.join(unit_path, unit.filename())
        test_name = os.path.basename(binary_path)
        return {cls.KEY: os.path.splitext(test_name)[0]}

    @classmethod
    def value5(cls, unit, flat_args, spec_args):
        path = _common.get_norm_unit_path(unit)
        value = '-'.join([os.path.basename(os.path.dirname(path)), os.path.basename(path)])
        return {cls.KEY: value}

    @classmethod
    def value6(cls, unit, flat_args, spec_args):
        path = _common.get_norm_unit_path(unit)
        value = '-'.join([os.path.basename(os.path.dirname(path)), os.path.basename(path), 'dependencies']).strip('-')
        return {cls.KEY: value}

    @classmethod
    def value7(cls, unit, flat_args, spec_args):
        test_name = os.path.basename(os.path.join(unit.path(), unit.filename()).replace(".pkg", ""))
        return {cls.KEY: os.path.splitext(test_name)[0]}


class TestPartition:
    KEY = 'TEST_PARTITION'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get("TEST_PARTITION")}


class TestRecipes:
    KEY = 'TEST-RECIPES'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: prepare_recipes(unit.get("TEST_RECIPES_VALUE"))}


class TestRunnerBin:
    KEY = 'TEST-RUNNER-BIN'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        runner_bin = spec_args.get('RUNNER_BIN', [None])[0]
        if runner_bin:
            return {cls.KEY: runner_bin}


class TestTimeout:
    KEY = 'TEST-TIMEOUT'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        test_timeout = ''.join(spec_args.get('TIMEOUT', [])) or unit.get('TEST_TIMEOUT') or ''
        return {cls.KEY: test_timeout}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        timeout = list(filter(None, [unit.get(["TEST_TIMEOUT"])]))
        if timeout:
            timeout = timeout[0]
        else:
            timeout = '0'
        return {cls.KEY: timeout}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('TEST_TIMEOUT')}


class TsResources:
    KEY = "{}-ROOT-VAR-NAME"

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        erm_json = _create_erm_json(unit)
        ret = {}
        for tool in erm_json.list_npm_packages():
            tool_resource_label = cls.KEY.format(tool.upper())
            tool_resource_value = unit.get(tool_resource_label)
            if tool_resource_value:
                ret[tool_resource_label] = tool_resource_value
        return ret


class JvmArgs:
    KEY = 'JVM_ARGS'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = serialize_list(get_values_list(unit, 'JVM_ARGS_VALUE'))
        return {cls.KEY: value}


class StrictClasspathClash:
    KEY = 'STRICT_CLASSPATH_CLASH'


class SystemProperties:
    KEY = 'SYSTEM_PROPERTIES'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        props, error_mgs = extract_java_system_properties(unit, get_values_list(unit, 'SYSTEM_PROPERTIES_VALUE'))
        if error_mgs:
            # TODO move error reporting out of field classes
            ymake.report_configure_error(error_mgs)
            raise DartValueError

        props = base64.b64encode(six.ensure_binary(json.dumps(props)))
        return {cls.KEY: props}


class UnittestDir:
    KEY = 'UNITTEST_DIR'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('UNITTEST_DIR')}


class UseArcadiaPython:
    KEY = 'USE_ARCADIA_PYTHON'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        return {cls.KEY: unit.get('USE_ARCADIA_PYTHON')}


class UseKtlintOld:
    KEY = 'USE_KTLINT_OLD'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        if unit.get('_USE_KTLINT_OLD') == 'yes':
            return {cls.KEY: 'yes'}


class YtSpec:
    KEY = 'YT-SPEC'

    @classmethod
    def value(cls, unit, flat_args, spec_args):
        value = serialize_list(spec_args.get('YT_SPEC', []) + get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))
        return {cls.KEY: value}

    @classmethod
    def value2(cls, unit, flat_args, spec_args):
        yt_spec = get_values_list(unit, 'TEST_YT_SPEC_VALUE')
        if yt_spec:
            return {cls.KEY: serialize_list(yt_spec)}

    @classmethod
    def value3(cls, unit, flat_args, spec_args):
        yt_spec_values = get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE')
        return {cls.KEY: serialize_list(yt_spec_values)}
