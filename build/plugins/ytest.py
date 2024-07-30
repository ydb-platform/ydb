from __future__ import print_function

import base64
import collections
import copy
import json
import os
import re
import six
import subprocess

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


import _common
import _dart_fields as df
import _requirements as reqs
import lib.test_const as consts
import ymake
from _dart_fields import (
    serialize_list,
    get_unit_list_variable,
    deserialize_list,
    prepare_env,
    create_dart_record,
)

BLOCK_SEPARATOR = '============================================================='
SPLIT_FACTOR_MAX_VALUE = 1000
SPLIT_FACTOR_TEST_FILES_MAX_VALUE = 4250
PARTITION_MODS = ('SEQUENTIAL', 'MODULO')
DEFAULT_TIDY_CONFIG = "build/config/tests/clang_tidy/config.yaml"
DEFAULT_TIDY_CONFIG_MAP_PATH = "build/yandex_specific/config/clang_tidy/tidy_default_map.json"
PROJECT_TIDY_CONFIG_MAP_PATH = "build/yandex_specific/config/clang_tidy/tidy_project_map.json"
KTLINT_CURRENT_EDITOR_CONFIG = "arcadia/build/platform/java/ktlint/.editorconfig"
KTLINT_OLD_EDITOR_CONFIG = "arcadia/build/platform/java/ktlint_old/.editorconfig"

YTEST_FIELDS_BASE = (
    df.AndroidApkTestActivity.value,
    df.BinaryPath.value,
    df.BuildFolderPath.value,
    df.CustomDependencies.value,
    df.GlobalLibraryPath.value,
    df.ScriptRelPath.value,
    df.SkipTest.value,
    df.SourceFolderPath.value,
    df.SplitFactor.value,
    df.TestCwd.value,
    df.TestedProjectFilename.value,
    df.TestedProjectName.value,
    df.TestEnv.value,
    df.TestIosDeviceType.value,
    df.TestIosRuntimeType.value,
    df.TestRecipes.value,
)

YTEST_FIELDS_EXTRA = (
    df.Blob.value,
    df.ForkMode.value,
    df.Size.value,
    df.Tag.value,
    df.TestTimeout.value,
    df.YtSpec.value,
)

PY_EXEC_FIELDS_BASE = (
    df.Blob.value,
    df.BuildFolderPath.value2,
    df.CanonizeSubPath.value,
    df.CustomDependencies.value3,
    df.ForkMode.value2,
    df.ForkTestFiles.value,
    df.PythonPaths.value,
    df.Requirements.value4,
    df.Size.value2,
    df.SkipTest.value,
    df.SourceFolderPath.value,
    df.SplitFactor.value2,
    df.Tag.value,
    df.TestCwd.value2,
    df.TestData.value5,
    df.TestEnv.value,
    df.TestFiles.value5,
    df.TestPartition.value,
    df.TestRecipes.value,
    df.TestTimeout.value2,
    df.UseArcadiaPython.value,
)

CHECK_FIELDS_BASE = (
    df.CustomDependencies.value2,
    df.Requirements.value3,
    df.ScriptRelPath.value2,
    df.TestEnv.value,
    df.TestName.value3,
    df.UseArcadiaPython.value,
)

tidy_config_map = None


def ontest_data(unit, *args):
    ymake.report_configure_error("TEST_DATA is removed in favour of DATA")


def is_yt_spec_contain_pool_info(filename):  # XXX switch to yson in ymake + perf test for configure
    pool_re = re.compile(r"""['"]*pool['"]*\s*?=""")
    cypress_root_re = re.compile(r"""['"]*cypress_root['"]*\s*=""")
    with open(filename, 'r') as afile:
        yt_spec = afile.read()
        return pool_re.search(yt_spec) and cypress_root_re.search(yt_spec)


def validate_test(unit, kw):
    def get_list(key):
        return deserialize_list(kw.get(key, ""))

    valid_kw = copy.deepcopy(kw)
    errors = []
    warnings = []

    mandatory_fields = {"SCRIPT-REL-PATH", "SOURCE-FOLDER-PATH", "TEST-NAME"}
    for field in mandatory_fields - valid_kw.keys():
        errors.append(f"Mandatory field {field!r} is not set in DART")

    if valid_kw.get('SCRIPT-REL-PATH') == 'boost.test':
        project_path = valid_kw.get('BUILD-FOLDER-PATH', "")
        if not project_path.startswith(
            ("contrib", "mail", "maps", "tools/idl", "metrika", "devtools", "mds", "yandex_io", "smart_devices")
        ):
            errors.append("BOOSTTEST is not allowed here")

    size_timeout = collections.OrderedDict(sorted(consts.TestSize.DefaultTimeouts.items(), key=lambda t: t[1]))

    size = valid_kw.get('SIZE', consts.TestSize.Small).lower()
    tags = set(get_list("TAG"))
    requirements_orig = get_list("REQUIREMENTS")
    in_autocheck = consts.YaTestTags.NotAutocheck not in tags and consts.YaTestTags.Manual not in tags
    is_fat = consts.YaTestTags.Fat in tags
    is_force_sandbox = consts.YaTestTags.ForceDistbuild not in tags and is_fat
    is_ytexec_run = consts.YaTestTags.YtRunner in tags
    is_fuzzing = valid_kw.get("FUZZING", False)
    is_kvm = 'kvm' in requirements_orig
    requirements = {}
    secret_requirements = ('sb_vault', 'yav')
    list_requirements = secret_requirements
    for req in requirements_orig:
        if req in ('kvm',):
            requirements[req] = str(True)
            continue

        if ":" in req:
            req_name, req_value = req.split(":", 1)
            if req_name in list_requirements:
                requirements[req_name] = ",".join(filter(None, [requirements.get(req_name), req_value]))
            else:
                if req_name in requirements:
                    if req_value in ["0"]:
                        warnings.append(
                            "Requirement [[imp]]{}[[rst]] is dropped [[imp]]{}[[rst]] -> [[imp]]{}[[rst]]".format(
                                req_name, requirements[req_name], req_value
                            )
                        )
                        del requirements[req_name]
                    elif requirements[req_name] != req_value:
                        warnings.append(
                            "Requirement [[imp]]{}[[rst]] is redefined [[imp]]{}[[rst]] -> [[imp]]{}[[rst]]".format(
                                req_name, requirements[req_name], req_value
                            )
                        )
                        requirements[req_name] = req_value
                else:
                    requirements[req_name] = req_value
        else:
            errors.append("Invalid requirement syntax [[imp]]{}[[rst]]: expect <requirement>:<value>".format(req))

    if not errors:
        for req_name, req_value in requirements.items():
            try:
                error_msg = reqs.validate_requirement(
                    req_name,
                    req_value,
                    size,
                    is_force_sandbox,
                    in_autocheck,
                    is_fuzzing,
                    is_kvm,
                    is_ytexec_run,
                    requirements,
                )
            except Exception as e:
                error_msg = str(e)
            if error_msg:
                errors += [error_msg]

    invalid_requirements_for_distbuild = [
        requirement for requirement in requirements.keys() if requirement not in ('ram', 'ram_disk', 'cpu', 'network')
    ]

    sb_tags = []
    # XXX Unfortunately, some users have already started using colons
    # in their tag names. Use skip set to avoid treating their tag as system ones.
    # Remove this check when all such user tags are removed.
    skip_set = ('ynmt_benchmark', 'bert_models', 'zeliboba_map')
    # Verify the prefixes of the system tags to avoid pointless use of the REQUIREMENTS macro parameters in the TAG macro.
    for tag in tags:
        if tag.startswith('sb:'):
            sb_tags.append(tag)
        elif ':' in tag and not tag.startswith('ya:') and tag.split(':')[0] not in skip_set:
            errors.append(
                "Only [[imp]]sb:[[rst]] and [[imp]]ya:[[rst]] prefixes are allowed in system tags: {}".format(tag)
            )

    if is_fat:
        if size != consts.TestSize.Large:
            errors.append("Only LARGE test may have ya:fat tag")

        if in_autocheck and not is_force_sandbox:
            if invalid_requirements_for_distbuild:
                errors.append(
                    "'{}' REQUIREMENTS options can be used only for FAT tests without ya:force_distbuild tag. Remove TAG(ya:force_distbuild) or an option.".format(
                        invalid_requirements_for_distbuild
                    )
                )
            if sb_tags:
                errors.append(
                    "You can set sandbox tags '{}' only for FAT tests without ya:force_distbuild. Remove TAG(ya:force_sandbox) or sandbox tags.".format(
                        sb_tags
                    )
                )
            if consts.YaTestTags.SandboxCoverage in tags:
                errors.append("You can set 'ya:sandbox_coverage' tag only for FAT tests without ya:force_distbuild.")
            if is_ytexec_run:
                errors.append(
                    "Running LARGE tests over YT (ya:yt) on Distbuild (ya:force_distbuild) is forbidden. Consider removing TAG(ya:force_distbuild)."
                )
    else:
        if is_force_sandbox:
            errors.append('ya:force_sandbox can be used with LARGE tests only')
        if consts.YaTestTags.Privileged in tags:
            errors.append("ya:privileged can be used with LARGE tests only")
        if in_autocheck and size == consts.TestSize.Large:
            errors.append("LARGE test must have ya:fat tag")

    if consts.YaTestTags.Privileged in tags and 'container' not in requirements:
        errors.append("Only tests with 'container' requirement can have 'ya:privileged' tag")

    if size not in size_timeout:
        errors.append(
            "Unknown test size: [[imp]]{}[[rst]], choose from [[imp]]{}[[rst]]".format(
                size.upper(), ", ".join([sz.upper() for sz in size_timeout.keys()])
            )
        )
    else:
        try:
            timeout = int(valid_kw.get('TEST-TIMEOUT', size_timeout[size]) or size_timeout[size])
            script_rel_path = valid_kw.get('SCRIPT-REL-PATH')
            if timeout < 0:
                raise Exception("Timeout must be > 0")

            skip_timeout_verification = script_rel_path in ('java.style', 'ktlint')

            if size_timeout[size] < timeout and in_autocheck and not skip_timeout_verification:
                suggested_size = None
                for s, t in size_timeout.items():
                    if timeout <= t:
                        suggested_size = s
                        break

                if suggested_size:
                    suggested_size = ", suggested size: [[imp]]{}[[rst]]".format(suggested_size.upper())
                else:
                    suggested_size = ""
                errors.append(
                    "Max allowed timeout for test size [[imp]]{}[[rst]] is [[imp]]{} sec[[rst]]{}".format(
                        size.upper(), size_timeout[size], suggested_size
                    )
                )
        except Exception as e:
            errors.append("Error when parsing test timeout: [[bad]]{}[[rst]]".format(e))

        requirements_list = []
        for req_name, req_value in six.iteritems(requirements):
            requirements_list.append(req_name + ":" + req_value)
        valid_kw['REQUIREMENTS'] = serialize_list(sorted(requirements_list))

    # Mark test with ya:external tag if it requests any secret from external storages
    # It's not stable and nonreproducible by definition
    for x in secret_requirements:
        if x in requirements:
            tags.add(consts.YaTestTags.External)

    if valid_kw.get("FUZZ-OPTS"):
        for option in get_list("FUZZ-OPTS"):
            if not option.startswith("-"):
                errors.append(
                    "Unrecognized fuzzer option '[[imp]]{}[[rst]]'. All fuzzer options should start with '-'".format(
                        option
                    )
                )
                break
            eqpos = option.find("=")
            if eqpos == -1 or len(option) == eqpos + 1:
                errors.append(
                    "Unrecognized fuzzer option '[[imp]]{}[[rst]]'. All fuzzer options should obtain value specified after '='".format(
                        option
                    )
                )
                break
            if option[eqpos - 1] == " " or option[eqpos + 1] == " ":
                errors.append("Spaces are not allowed: '[[imp]]{}[[rst]]'".format(option))
                break
            if option[:eqpos] in ("-runs", "-dict", "-jobs", "-workers", "-artifact_prefix", "-print_final_stats"):
                errors.append(
                    "You can't use '[[imp]]{}[[rst]]' - it will be automatically calculated or configured during run".format(
                        option
                    )
                )
                break

    if valid_kw.get("YT-SPEC"):
        if not is_ytexec_run:
            errors.append("You can use YT_SPEC macro only tests marked with ya:yt tag")
        else:
            for filename in get_list("YT-SPEC"):
                filename = unit.resolve('$S/' + filename)
                if not os.path.exists(filename):
                    errors.append("File '{}' specified in the YT_SPEC macro doesn't exist".format(filename))
                    continue
                if not is_yt_spec_contain_pool_info(filename):
                    tags.add(consts.YaTestTags.External)
                    tags.add("ya:yt_research_pool")

    partition = valid_kw.get('TEST_PARTITION', 'SEQUENTIAL')
    if partition not in PARTITION_MODS:
        raise ValueError('partition mode should be one of {}, detected: {}'.format(PARTITION_MODS, partition))

    if valid_kw.get('SPLIT-FACTOR'):
        if valid_kw.get('FORK-MODE') == 'none':
            errors.append('SPLIT_FACTOR must be use with FORK_TESTS() or FORK_SUBTESTS() macro')

        value = 1
        try:
            value = int(valid_kw.get('SPLIT-FACTOR'))
            if value <= 0:
                raise ValueError("must be > 0")
            if value > SPLIT_FACTOR_MAX_VALUE:
                raise ValueError("the maximum allowed value is {}".format(SPLIT_FACTOR_MAX_VALUE))
        except ValueError as e:
            errors.append('Incorrect SPLIT_FACTOR value: {}'.format(e))

        if valid_kw.get('FORK-TEST-FILES') and size != consts.TestSize.Large:
            nfiles = count_entries(valid_kw.get('TEST-FILES'))
            if nfiles * value > SPLIT_FACTOR_TEST_FILES_MAX_VALUE:
                errors.append(
                    'Too much chunks generated:{} (limit: {}). Remove FORK_TEST_FILES() macro or reduce SPLIT_FACTOR({}).'.format(
                        nfiles * value, SPLIT_FACTOR_TEST_FILES_MAX_VALUE, value
                    )
                )

    if tags:
        valid_kw['TAG'] = serialize_list(sorted(tags))

    unit_path = _common.get_norm_unit_path(unit)
    if (
        not is_fat
        and consts.YaTestTags.Noretries in tags
        and not is_ytexec_run
        and not unit_path.startswith("devtools/dummy_arcadia/test/noretries")
    ):
        errors.append("Only LARGE tests can have 'ya:noretries' tag")

    if errors:
        return None, warnings, errors

    return valid_kw, warnings, errors


def dump_test(unit, kw):
    kw = {k: v for k, v in kw.items() if v and (not isinstance(v, str | bytes) or v.strip())}
    valid_kw, warnings, errors = validate_test(unit, kw)
    for w in warnings:
        unit.message(['warn', w])
    for e in errors:
        ymake.report_configure_error(e)
    if valid_kw is None:
        return None
    string_handler = StringIO()
    for k, v in six.iteritems(valid_kw):
        print(k + ': ' + six.ensure_str(v), file=string_handler)
    print(BLOCK_SEPARATOR, file=string_handler)
    data = string_handler.getvalue()
    string_handler.close()
    return data


def reference_group_var(varname: str, extensions: list[str] | None = None) -> str:
    if extensions is None:
        return f'"${{join=\\;:{varname}}}"'

    return serialize_list(f'${{ext={ext};join=\\;:{varname}}}' for ext in extensions)


def count_entries(x):
    # see (de)serialize_list
    assert x is None or isinstance(x, str), type(x)
    if not x:
        return 0
    return x.count(";") + 1


def implies(a, b):
    return bool((not a) or b)


def match_coverage_extractor_requirements(unit):
    # we add test if
    return all(
        (
            # tests are requested
            unit.get("TESTS_REQUESTED") == "yes",
            # build implies clang coverage, which supports segment extraction from the binaries
            unit.get("CLANG_COVERAGE") == "yes",
            # contrib was requested
            implies(
                _common.get_norm_unit_path(unit).startswith("contrib/"), unit.get("ENABLE_CONTRIB_COVERAGE") == "yes"
            ),
        )
    )


def get_tidy_config_map(unit, map_path):
    config_map_path = unit.resolve(os.path.join("$S", map_path))
    config_map = {}
    try:
        with open(config_map_path, 'r') as afile:
            config_map = json.load(afile)
    except ValueError:
        ymake.report_configure_error("{} is invalid json".format(map_path))
    except Exception as e:
        ymake.report_configure_error(str(e))
    return config_map


def prepare_config_map(config_map):
    return list(reversed(sorted(config_map.items())))


def get_default_tidy_config(unit):
    unit_path = _common.get_norm_unit_path(unit)
    tidy_default_config_map = prepare_config_map(get_tidy_config_map(unit, DEFAULT_TIDY_CONFIG_MAP_PATH))

    for project_prefix, config_path in tidy_default_config_map:
        if unit_path.startswith(project_prefix):
            return config_path
    return DEFAULT_TIDY_CONFIG


ordered_tidy_map = None


def get_project_tidy_config(unit):
    global ordered_tidy_map
    if ordered_tidy_map is None:
        ordered_tidy_map = prepare_config_map(get_tidy_config_map(unit, PROJECT_TIDY_CONFIG_MAP_PATH))
    unit_path = _common.get_norm_unit_path(unit)

    for project_prefix, config_path in ordered_tidy_map:
        if unit_path.startswith(project_prefix):
            return config_path
    else:
        return get_default_tidy_config(unit)


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.SbrUidExt.value,
        df.TestFiles.value,
    )
)
def check_data(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)
    if not dart_record[df.TestFiles.KEY]:
        return

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.SbrUidExt.value,
        df.TestFiles.value2,
    )
)
def check_resource(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.TestData.value3,
        df.TestFiles.value2,
        df.ModuleLang.value,
        df.KtlintBinary.value,
        df.UseKtlintOld.value,
        df.KtlintBaselineFile.value,
    )
)
def ktlint(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)
    dart_record[df.TestTimeout.KEY] = '120'

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.TestData.value4,
        df.ForkMode.value2,
        df.TestFiles.value3,
        df.JdkLatestVersion.value,
        df.JdkResource.value,
        df.ModuleLang.value,
    )
)
def java_style(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )
    if len(flat_args) < 2:
        raise Exception("Not enough arguments for JAVA_STYLE check")

    # jstyle should use the latest jdk
    unit.onpeerdir([unit.get('JDK_LATEST_PEERDIR')])

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)
    dart_record[df.TestTimeout.KEY] = '240'
    dart_record[df.ScriptRelPath.KEY] = 'java.style'

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value3,
        df.SourceFolderPath.value2,
        df.ForkMode.value2,
        df.TestFiles.value2,
        df.ModuleLang.value,
    )
)
def gofmt(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    CHECK_FIELDS_BASE
    + (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.ForkMode.value2,
        df.TestFiles.value2,
        df.ModuleLang.value,
    )
)
def govet(fields, unit, *args):
    flat_args, spec_args = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


def onadd_check(unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return

    flat_args, *_ = _common.sort_by_keywords(
        {
            "DEPENDS": -1,
            "TIMEOUT": 1,
            "DATA": -1,
            "TAG": -1,
            "REQUIREMENTS": -1,
            "FORK_MODE": 1,
            "SPLIT_FACTOR": 1,
            "FORK_SUBTESTS": 0,
            "FORK_TESTS": 0,
            "SIZE": 1,
        },
        args,
    )
    check_type = flat_args[0]

    if check_type == "check.data" and unit.get('VALIDATE_DATA') != "no":
        check_data(unit, *args)
    elif check_type == "check.resource" and unit.get('VALIDATE_DATA') != "no":
        check_resource(unit, *args)
    elif check_type == "ktlint":
        ktlint(unit, *args)
    elif check_type == "JAVA_STYLE" and (unit.get('YMAKE_JAVA_TEST') != 'yes' or unit.get('ALL_SRCDIRS')):
        java_style(unit, *args)
    elif check_type == "gofmt":
        gofmt(unit, *args)
    elif check_type == "govet":
        govet(unit, *args)


def on_register_no_check_imports(unit):
    s = unit.get('NO_CHECK_IMPORTS_FOR_VALUE')
    if s not in ('', 'None'):
        unit.onresource(['-', 'py/no_check_imports/{}="{}"'.format(_common.pathid(s), s)])


@df.with_fields(
    (
        df.TestedProjectName.value2,
        df.SourceFolderPath.value,
        df.TestEnv.value,
        df.UseArcadiaPython.value,
        df.TestFiles.value4,
        df.ModuleLang.value,
        df.NoCheck.value,
    )
)
def onadd_check_py_imports(fields, unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return
    if unit.get('NO_CHECK_IMPORTS_FOR_VALUE').strip() == "":
        return
    unit.onpeerdir(['library/python/testing/import_test'])

    dart_record = create_dart_record(fields, unit, (), {})
    dart_record[df.TestName.KEY] = 'pyimports'
    dart_record[df.ScriptRelPath.KEY] = 'py.imports'

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    PY_EXEC_FIELDS_BASE
    + (
        df.TestName.value4,
        df.ScriptRelPath.value3,
        df.TestedProjectName.value4,
        df.ModuleLang.value,
        df.BinaryPath.value2,
        df.TestRunnerBin.value,
    )
)
def onadd_pytest_bin(fields, unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return
    flat_args, spec_args = _common.sort_by_keywords({'RUNNER_BIN': 1}, args)
    if flat_args:
        ymake.report_configure_error(
            'Unknown arguments found while processing add_pytest_bin macro: {!r}'.format(flat_args)
        )

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    yt_spec = df.YtSpec.value2(unit, flat_args, spec_args)
    if yt_spec and yt_spec[df.YtSpec.KEY]:
        unit.ondata_files(deserialize_list(yt_spec[df.YtSpec.KEY]))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)
    if yt_spec:
        dart_record |= yt_spec

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    (
        df.SourceFolderPath.value,
        df.TestName.value5,
        df.ScriptRelPath.value4,
        df.TestTimeout.value3,
        df.TestedProjectName.value5,
        df.TestEnv.value,
        df.TestData.value6,
        df.ForkMode.value2,
        df.SplitFactor.value2,
        df.CustomDependencies.value3,
        df.Tag.value,
        df.Size.value2,
        df.Requirements.value2,
        df.TestRecipes.value,
        df.ModuleType.value,
        df.UnittestDir.value,
        df.JvmArgs.value,
        # TODO optimize, SystemProperties is used in TestData
        df.SystemProperties.value,
        df.TestCwd.value,
        df.SkipTest.value,
        df.JavaClasspathCmdType.value,
        df.JdkResource.value,
        df.JdkForTests.value,
        df.ModuleLang.value,
        df.TestClasspath.value,
        df.TestClasspathOrigins.value,
        df.TestClasspathDeps.value,
        df.TestJar.value,
    )
)
def onjava_test(fields, unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return

    assert unit.get('MODULE_TYPE') is not None

    if unit.get('MODULE_TYPE') == 'JTEST_FOR':
        if not unit.get('UNITTEST_DIR'):
            ymake.report_configure_error('skip JTEST_FOR in {}: no args provided'.format(unit.path()))
            return

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    yt_spec = df.YtSpec.value3(unit, (), {})
    unit.ondata_files(deserialize_list(yt_spec[df.YtSpec.KEY]))

    try:
        dart_record = create_dart_record(fields, unit, (), {})
    except df.DartValueError:
        return
    dart_record |= yt_spec

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(['DART_DATA', data])


@df.with_fields(
    (
        df.SourceFolderPath.value,
        df.TestName.value6,
        df.TestedProjectName.value5,
        df.CustomDependencies.value3,
        df.IgnoreClasspathClash.value,
        df.ModuleType.value,
        df.ModuleLang.value,
        df.Classpath.value,
    )
)
def onjava_test_deps(fields, unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return

    assert unit.get('MODULE_TYPE') is not None
    assert len(args) == 1
    mode = args[0]

    dart_record = create_dart_record(fields, unit, (args[0],), {})
    dart_record[df.ScriptRelPath.KEY] = 'java.dependency.test'
    if mode == 'strict':
        dart_record[df.StrictClasspathClash.KEY] = 'yes'

    data = dump_test(unit, dart_record)
    unit.set_property(['DART_DATA', data])


def onsetup_pytest_bin(unit, *args):
    use_arcadia_python = unit.get('USE_ARCADIA_PYTHON') == "yes"
    if use_arcadia_python:
        unit.onresource(['-', 'PY_MAIN={}'.format("library.python.pytest.main:main")])  # XXX
        unit.onadd_pytest_bin(list(args))


def onrun(unit, *args):
    exectest_cmd = unit.get(["EXECTEST_COMMAND_VALUE"]) or ''
    exectest_cmd += "\n" + subprocess.list2cmdline(args)
    unit.set(["EXECTEST_COMMAND_VALUE", exectest_cmd])


@df.with_fields(
    PY_EXEC_FIELDS_BASE
    + (
        df.TestName.value7,
        df.TestedProjectName.value6,
        df.BinaryPath.value3,
    )
)
def onsetup_exectest(fields, unit, *args):
    if unit.get("TIDY") == "yes":
        # graph changed for clang_tidy tests
        return
    command = unit.get(["EXECTEST_COMMAND_VALUE"])
    if command is None:
        ymake.report_configure_error("EXECTEST must have at least one RUN macro")
        return
    command = command.replace("$EXECTEST_COMMAND_VALUE", "")
    if "PYTHON_BIN" in command:
        unit.ondepends('contrib/tools/python')
    unit.set(["TEST_BLOB_DATA", base64.b64encode(six.ensure_binary(command))])
    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    yt_spec = df.YtSpec.value2(unit, (), {})
    if yt_spec and yt_spec[df.YtSpec.KEY]:
        unit.ondata_files(deserialize_list(yt_spec[df.YtSpec.KEY]))

    dart_record = create_dart_record(fields, unit, (), {})
    dart_record[df.ScriptRelPath.KEY] = 'exectest'
    if yt_spec:
        dart_record |= yt_spec

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


def onsetup_run_python(unit):
    if unit.get("USE_ARCADIA_PYTHON") == "yes":
        unit.ondepends('contrib/tools/python')


def on_add_linter_check(unit, *args):
    if unit.get("TIDY") == "yes":
        return
    source_root_from_prefix = '${ARCADIA_ROOT}/'
    source_root_to_prefix = '$S/'
    unlimited = -1

    no_lint_value = _common.get_no_lint_value(unit)
    if no_lint_value in ("none", "none_internal"):
        return

    keywords = {
        "DEPENDS": unlimited,
        "FILES": unlimited,
        "CONFIGS": unlimited,
        "GLOBAL_RESOURCES": unlimited,
        "FILE_PROCESSING_TIME": 1,
        "EXTRA_PARAMS": unlimited,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)
    if len(flat_args) != 2:
        unit.message(['ERROR', '_ADD_LINTER_CHECK params: expected 2 free parameters'])
        return

    configs = []
    for cfg in spec_args.get('CONFIGS', []):
        filename = unit.resolve(source_root_to_prefix + cfg)
        if not os.path.exists(filename):
            unit.message(['ERROR', 'Configuration file {} is not found'.format(filename)])
            return
        configs.append(cfg)
    deps = []

    lint_name, linter = flat_args
    deps.append(os.path.dirname(linter))

    test_files = []
    for path in spec_args.get('FILES', []):
        if path.startswith(source_root_from_prefix):
            test_files.append(path.replace(source_root_from_prefix, source_root_to_prefix, 1))
        elif path.startswith(source_root_to_prefix):
            test_files.append(path)

    if lint_name == 'cpp_style':
        files_dart = reference_group_var("ALL_SRCS", consts.STYLE_CPP_ALL_EXTS)
    else:
        if not test_files:
            unit.message(['WARN', 'No files to lint for {}'.format(lint_name)])
            return
        files_dart = serialize_list(test_files)

    for arg in spec_args.get('EXTRA_PARAMS', []):
        if '=' not in arg:
            unit.message(['WARN', 'Wrong EXTRA_PARAMS value: "{}". Values must have format "name=value".'.format(arg)])
            return

    deps += spec_args.get('DEPENDS', [])

    for dep in deps:
        unit.ondepends(dep)

    for resource in spec_args.get('GLOBAL_RESOURCES', []):
        unit.onpeerdir(resource)

    test_record = {
        'TEST-NAME': lint_name,
        'SCRIPT-REL-PATH': 'custom_lint',
        'TESTED-PROJECT-NAME': unit.name(),
        'SOURCE-FOLDER-PATH': _common.get_norm_unit_path(unit),
        'CUSTOM-DEPENDENCIES': " ".join(deps),
        'TEST-ENV': prepare_env(unit.get("TEST_ENV_VALUE")),
        'USE_ARCADIA_PYTHON': unit.get('USE_ARCADIA_PYTHON') or '',
        # TODO remove FILES, see DEVTOOLS-7052
        'FILES': files_dart,
        'TEST-FILES': files_dart,
        # Linter specific parameters
        # TODO Add configs to DATA. See YMAKE-427
        'LINT-CONFIGS': serialize_list(configs),
        'LINT-NAME': lint_name,
        'LINT-FILE-PROCESSING-TIME': spec_args.get('FILE_PROCESSING_TIME', [''])[0],
        'LINT-EXTRA-PARAMS': serialize_list(spec_args.get('EXTRA_PARAMS', [])),
        'LINTER': linter,
    }

    data = dump_test(unit, test_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + (
        df.TestName.value,
        df.TestPartition.value,
        df.ModuleLang.value,
    )
)
def clang_tidy(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get("TIDY_CONFIG"):
        default_config_path = unit.get("TIDY_CONFIG")
        project_config_path = unit.get("TIDY_CONFIG")
    else:
        default_config_path = get_default_tidy_config(unit)
        project_config_path = get_project_tidy_config(unit)

    unit.set(["DEFAULT_TIDY_CONFIG", default_config_path])
    unit.set(["PROJECT_TIDY_CONFIG", project_config_path])

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
        df.ModuleLang.value,
    )
)
def unittest_py(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
        df.ModuleLang.value,
    )
)
def gunittest(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
        df.ModuleLang.value,
        df.BenchmarkOpts.value,
    )
)
def g_benchmark(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value2,
        df.Requirements.value,
        df.TestPartition.value,
        df.ModuleLang.value,
    )
)
def go_test(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))
    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
    )
)
def boost_test(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))
    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value2,
        df.FuzzDicts.value,
        df.FuzzOpts.value,
        df.Fuzzing.value,
    )
)
def fuzz_test(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    if unit.get('ADD_SRCDIR_TO_TEST_DATA') == "yes":
        unit.ondata_files(_common.get_norm_unit_path(unit))
    unit.ondata_files("fuzzing/{}/corpus.json".format(_common.get_norm_unit_path(unit)))
    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
        df.ModuleLang.value,
        df.BenchmarkOpts.value,
    )
)
def y_benchmark(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
    )
)
def coverage_extractor(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)

    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


@df.with_fields(
    YTEST_FIELDS_BASE
    + YTEST_FIELDS_EXTRA
    + (
        df.TestName.value2,
        df.TestData.value,
        df.Requirements.value,
        df.TestPartition.value,
        df.GoBenchTimeout.value,
        df.ModuleLang.value,
    )
)
def go_bench(fields, unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, spec_args = _common.sort_by_keywords(keywords, args)
    tags = df.Tag.value(unit, flat_args, spec_args)[df.Tag.KEY]

    if "ya:run_go_benchmark" not in tags:
        return

    unit.ondata_files(get_unit_list_variable(unit, 'TEST_YT_SPEC_VALUE'))

    dart_record = create_dart_record(fields, unit, flat_args, spec_args)

    data = dump_test(unit, dart_record)
    if data:
        unit.set_property(["DART_DATA", data])


def onadd_ytest(unit, *args):
    keywords = {
        "DEPENDS": -1,
        "DATA": -1,
        "TIMEOUT": 1,
        "FORK_MODE": 1,
        "SPLIT_FACTOR": 1,
        "FORK_SUBTESTS": 0,
        "FORK_TESTS": 0,
    }
    flat_args, *_ = _common.sort_by_keywords(keywords, args)
    test_type = flat_args[1]

    # TIDY not supported for module
    if unit.get("TIDY_ENABLED") == "yes" and test_type != "clang_tidy":
        return
    # TIDY explicitly disabled for module in ymake.core.conf
    elif test_type == "clang_tidy" and unit.get("TIDY_ENABLED") != "yes":
        return
    # TIDY disabled for module in ya.make
    elif unit.get("TIDY") == "yes" and unit.get("TIDY_ENABLED") != "yes":
        return
    elif test_type == "no.test":
        return
    elif test_type == "clang_tidy" and unit.get("TIDY_ENABLED") == "yes":
        clang_tidy(unit, *args)
    elif test_type == "unittest.py":
        unittest_py(unit, *args)
    elif test_type == "gunittest":
        gunittest(unit, *args)
    elif test_type == "g_benchmark":
        g_benchmark(unit, *args)
    elif test_type == "go.test":
        go_test(unit, *args)
    elif test_type == "boost.test":
        boost_test(unit, *args)
    elif test_type == "fuzz.test":
        fuzz_test(unit, *args)
    elif test_type == "y_benchmark":
        y_benchmark(unit, *args)
    elif test_type == "coverage.extractor" and match_coverage_extractor_requirements(unit):
        coverage_extractor(unit, *args)
    elif test_type == "go.bench":
        go_bench(unit, *args)
