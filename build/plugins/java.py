import _common as common
import ymake
import json
import os
import base64


DELIM = '================================'
CONTRIB_JAVA_PREFIX = 'contrib/java/'


def split_args(s):  # TODO quotes, escapes
    return list(filter(None, s.split()))


def extract_macro_calls(unit, macro_value_name, macro_calls_delim):
    value = unit.get(macro_value_name)  # TODO(dimdim11) replace by get_subst
    if not value:
        return []

    return list(
        filter(
            None,
            map(split_args, value.replace('$' + macro_value_name, '').split(macro_calls_delim)),
        )
    )


def extract_macro_calls2(unit, macro_value_name):
    value = unit.get(macro_value_name)  # TODO(dimdim11) replace by get_subst
    if not value:
        return []

    calls = []
    for call_encoded_args in value.strip().split():
        call_args = json.loads(base64.b64decode(call_encoded_args))
        calls.append(call_args)

    return calls


def onjava_module(unit, *args):
    args_delim = unit.get('ARGS_DELIM')

    if unit.get('YA_IDE_IDEA') != 'yes':
        return

    data = {
        'BUNDLE_NAME': unit.name(),
        'PATH': unit.path(),
        'MODULE_TYPE': unit.get('MODULE_TYPE'),
        'MODULE_ARGS': unit.get('MODULE_ARGS'),
        'MANAGED_PEERS': '${MANAGED_PEERS}',
        'MANAGED_PEERS_CLOSURE': '${MANAGED_PEERS_CLOSURE}',
        'NON_NAMAGEABLE_PEERS': '${NON_NAMAGEABLE_PEERS}',
        'JAVA_SRCS': extract_macro_calls(unit, 'JAVA_SRCS_VALUE', args_delim),
        'JAVAC_FLAGS': extract_macro_calls(unit, 'JAVAC_FLAGS_VALUE', args_delim),
        'ANNOTATION_PROCESSOR': extract_macro_calls(unit, 'ANNOTATION_PROCESSOR_VALUE', args_delim),
        # TODO remove when java test dart is in prod
        'UNITTEST_DIR': unit.get('UNITTEST_DIR'),
        'JVM_ARGS': extract_macro_calls(unit, 'JVM_ARGS_VALUE', args_delim),
        'IDEA_EXCLUDE': extract_macro_calls(unit, 'IDEA_EXCLUDE_DIRS_VALUE', args_delim),
        'IDEA_RESOURCE': extract_macro_calls(unit, 'IDEA_RESOURCE_DIRS_VALUE', args_delim),
        'IDEA_MODULE_NAME': extract_macro_calls(unit, 'IDEA_MODULE_NAME_VALUE', args_delim),
        'TEST_DATA': extract_macro_calls(unit, 'TEST_DATA_VALUE', args_delim),
        'JDK_RESOURCE': 'JDK' + (unit.get('JDK_VERSION') or unit.get('JDK_REAL_VERSION') or '_DEFAULT'),
    }
    if unit.get('ENABLE_PREVIEW_VALUE') == 'yes' and (unit.get('JDK_VERSION') or unit.get('JDK_REAL_VERSION')) in (
        '17',
        '20',
        '21',
        '22',
        '23',
        '24',
    ):
        data['ENABLE_PREVIEW'] = extract_macro_calls(unit, 'ENABLE_PREVIEW_VALUE', args_delim)

    if unit.get('SAVE_JAVAC_GENERATED_SRCS_DIR') and unit.get('SAVE_JAVAC_GENERATED_SRCS_TAR'):
        data['SAVE_JAVAC_GENERATED_SRCS_TAR'] = extract_macro_calls(unit, 'SAVE_JAVAC_GENERATED_SRCS_TAR', args_delim)

    if unit.get('JAVA_ADD_DLLS_VALUE') == 'yes':
        data['ADD_DLLS_FROM_DEPENDS'] = extract_macro_calls(unit, 'JAVA_ADD_DLLS_VALUE', args_delim)

    if unit.get('ERROR_PRONE_VALUE') == 'yes':
        data['ERROR_PRONE'] = extract_macro_calls(unit, 'ERROR_PRONE_VALUE', args_delim)

    if unit.get('WITH_KOTLIN_VALUE') == 'yes':
        if unit.get('KOTLINC_OPTS_VALUE'):
            data['KOTLINC_OPTS'] = extract_macro_calls(unit, 'KOTLINC_OPTS_VALUE', args_delim)

    if unit.get('JAVA_EXTERNAL_DEPENDENCIES_VALUE'):
        valid = []
        for dep in sum(extract_macro_calls(unit, 'JAVA_EXTERNAL_DEPENDENCIES_VALUE', args_delim), []):
            if os.path.normpath(dep).startswith('..'):
                ymake.report_configure_error(
                    '{}: {} - relative paths in JAVA_EXTERNAL_DEPENDENCIES is not allowed'.format(unit.path(), dep)
                )
            elif os.path.isabs(dep):
                ymake.report_configure_error(
                    '{}: {} absolute paths in JAVA_EXTERNAL_DEPENDENCIES is not allowed'.format(unit.path(), dep)
                )
            else:
                valid.append(dep)
        if valid:
            data['EXTERNAL_DEPENDENCIES'] = [valid]

    # IMPORTANT before switching vcs_info.py to python3 the value was always evaluated to $YMAKE_PYTHON but no
    # code in java dart parser extracts its value only checks this key for existance.
    data['EMBED_VCS'] = [['yes']]
    # FORCE_VCS_INFO_UPDATE is responsible for setting special value of VCS_INFO_DISABLE_CACHE__NO_UID__
    macro_val = extract_macro_calls(unit, 'FORCE_VCS_INFO_UPDATE', args_delim)
    macro_str = macro_val[0][0] if macro_val and macro_val[0] and macro_val[0][0] else ''
    if macro_str and macro_str == 'yes':
        data['VCS_INFO_DISABLE_CACHE__NO_UID__'] = macro_val

    for java_srcs_args in data['JAVA_SRCS']:
        external = None

        for i in range(len(java_srcs_args)):
            arg = java_srcs_args[i]

            if arg == 'EXTERNAL':
                if not i + 1 < len(java_srcs_args):
                    continue  # TODO configure error

                ex = java_srcs_args[i + 1]

                if ex in ('EXTERNAL', 'SRCDIR', 'PACKAGE_PREFIX', 'EXCLUDE'):
                    continue  # TODO configure error

                if external is not None:
                    continue  # TODO configure error

                external = ex

        if external:
            unit.onpeerdir(external)

    data = {k: v for k, v in data.items() if v}

    dart = 'JAVA_DART: ' + base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8') + '\n' + DELIM + '\n'
    unit.set_property(['JAVA_DART_DATA', dart])


def on_add_java_style_checks(unit, *args):
    if unit.get('LINT_LEVEL_VALUE') != "none" and common.get_no_lint_value(unit) != 'none':
        unit.onadd_check(['JAVA_STYLE', unit.get('LINT_LEVEL_VALUE')] + list(args))


def on_add_kotlin_style_checks(unit, *args):
    """
    ktlint can be disabled using NO_LINT() and NO_LINT(ktlint)
    """
    if unit.get('WITH_KOTLIN_VALUE') == 'yes':
        if common.get_no_lint_value(unit) == '':
            unit.onadd_check(['ktlint'] + list(args))


def on_add_classpath_clash_check(unit, *args):
    jdeps_val = (unit.get('CHECK_JAVA_DEPS_VALUE') or '').lower()
    if jdeps_val and jdeps_val not in ('yes', 'no', 'strict'):
        ymake.report_configure_error('CHECK_JAVA_DEPS: "yes", "no" or "strict" required')
    if jdeps_val and jdeps_val != 'no':
        unit.onjava_test_deps(jdeps_val)


def on_add_detekt_report_check(unit, *args):
    if unit.get('WITH_KOTLIN_VALUE') == 'yes' and unit.get('WITH_KOTLINC_PLUGIN_DETEKT') == 'yes':
        unit.onadd_check(['detekt.report'] + list(args))


# Ymake java modules related macros


def on_check_java_srcdir(unit, *args):
    args = list(args)
    if 'SKIP_CHECK_SRCDIR' in args:
        return
    for arg in args:
        if '$' not in arg:
            arc_srcdir = os.path.join(unit.get('MODDIR'), arg)
            abs_srcdir = unit.resolve(os.path.join("$S/", arc_srcdir))
            if not os.path.exists(abs_srcdir) or not os.path.isdir(abs_srcdir):
                unit.onsrcdir(os.path.join('${ARCADIA_ROOT}', arc_srcdir))
            return
        srcdir = common.resolve_common_const(unit.resolve_arc_path(arg))
        if srcdir and srcdir.startswith('$S'):
            abs_srcdir = unit.resolve(srcdir)
            if not os.path.exists(abs_srcdir) or not os.path.isdir(abs_srcdir):
                unit.onsrcdir(os.path.join('${ARCADIA_ROOT}', srcdir[3:]))


def on_fill_jar_copy_resources_cmd(unit, *args):
    if len(args) == 4:
        varname, srcdir, base_classes_dir, reslist = tuple(args)
        package = ''
    else:
        varname, srcdir, base_classes_dir, package, reslist = tuple(args)
    dest_dir = os.path.join(base_classes_dir, *package.split('.')) if package else base_classes_dir
    var = unit.get(varname)
    var += ' && $FS_TOOLS copy_files {} {} {}'.format(
        srcdir if srcdir.startswith('"$') else '${CURDIR}/' + srcdir, dest_dir, reslist
    )
    unit.set([varname, var])


def on_fill_jar_gen_srcs(unit, *args):
    varname, jar_type, srcdir, base_classes_dir, java_list, kt_list, res_list = tuple(args[0:7])
    resolved_srcdir = unit.resolve_arc_path(srcdir)
    if not resolved_srcdir.startswith('$') or resolved_srcdir.startswith('$S'):
        return
    if jar_type == 'SRC_JAR' and unit.get('SOURCES_JAR') != 'yes':
        return

    args_delim = unit.get('JAR_BUILD_SCRIPT_FLAGS_DELIM')
    exclude_pos = args.index('EXCLUDE')
    globs = ' '.join(args[7:exclude_pos])
    excludes = ' '.join(args[exclude_pos + 1 :])
    var = unit.get(varname)
    var += f' {args_delim} --append -d {srcdir} -s {java_list} -k {kt_list} -r {res_list} --include-patterns {globs}'
    if jar_type == 'SRC_JAR':
        var += ' --all-resources'
    if len(excludes) > 0:
        var += f' --exclude-patterns {excludes}'
    if unit.get('WITH_KOTLIN_VALUE') == 'yes':
        var += ' --resolve-kotlin'
    unit.set([varname, var])


def on_check_run_java_prog_classpath(unit, *args):
    if len(args) != 1:
        ymake.report_configure_error(
            'multiple CLASSPATH elements in RUN_JAVA_PROGRAM invocation no more supported. Use JAVA_RUNTIME_PEERDIR on the JAVA_PROGRAM module instead'
        )


def extract_words(words, keys):
    kv = {}
    k = None

    for w in words:
        if w in keys:
            k = w
        else:
            if k not in kv:
                kv[k] = []
            kv[k].append(w)

    return kv


def parse_words(words):
    kv = extract_words(words, {'OUT', 'TEMPLATE'})
    if 'TEMPLATE' not in kv:
        kv['TEMPLATE'] = ['template.tmpl']
    ws = []
    for item in ('OUT', 'TEMPLATE'):
        for i, word in list(enumerate(kv[item])):
            if word == 'CUSTOM_PROPERTY':
                ws += kv[item][i:]
                kv[item] = kv[item][:i]
    templates = kv['TEMPLATE']
    outputs = kv['OUT']
    if len(outputs) < len(templates):
        ymake.report_configure_error('To many arguments for TEMPLATE parameter')
        return
    if ws and ws[0] != 'CUSTOM_PROPERTY':
        ymake.report_configure_error('''Can't parse {}'''.format(ws))
    custom_props = []
    for item in ws:
        if item == 'CUSTOM_PROPERTY':
            custom_props.append([])
        else:
            custom_props[-1].append(item)
    props = []
    for p in custom_props:
        if not p:
            ymake.report_configure_error('Empty CUSTOM_PROPERTY')
            continue
        props.append('-B')
        if len(p) > 1:
            props.append(base64.b64encode("{}={}".format(p[0], ' '.join(p[1:])).encode('utf-8')).decode('utf-8'))
        else:
            ymake.report_configure_error('CUSTOM_PROPERTY "{}" value is not specified'.format(p[0]))
    for i, o in enumerate(outputs):
        yield o, templates[min(i, len(templates) - 1)], props


def ongenerate_script(unit, *args):
    for out, tmpl, props in parse_words(list(args)):
        unit.on_add_gen_java_script([out, tmpl] + list(props))


def on_jdk_version_macro_check(unit, *args):
    if len(args) != 1:
        unit.message(["error", "Invalid syntax. Single argument required."])
    jdk_version = args[0]
    available_versions = (
        '11',
        '17',
        '20',
        '21',
        '22',
        '23',
        '24',
    )
    if jdk_version not in available_versions:
        ymake.report_configure_error(
            "Invalid jdk version: {}. {} are available".format(jdk_version, available_versions)
        )
    if int(jdk_version) >= 19 and unit.get('WITH_JDK_VALUE') != 'yes' and unit.get('MODULE_TAG') == 'JAR_RUNNABLE':
        msg = (
            "Missing WITH_JDK() macro for JDK version >= 19"
            # temporary link with additional explanation
            ". For more info see https://clubs.at.yandex-team.ru/arcadia/28543"
        )
        ymake.report_configure_error(msg)


def _maven_coords_for_project(unit, project_dir):
    parts = project_dir.split('/')

    g = '.'.join(parts[2:-2])
    a = parts[-2]
    v = parts[-1]
    c = ''

    pom_path = unit.resolve(os.path.join('$S', project_dir, 'pom.xml'))
    if os.path.exists(pom_path):
        try:
            # TODO(YMAKE-1694): xml is not currenly ready for Python subinterpreters, so we temporarily switch to parser implemented in ymake module
            if hasattr(ymake, 'get_artifact_id_from_pom_xml'):
                with open(pom_path, 'rb') as f:
                    artifact = ymake.get_artifact_id_from_pom_xml(f.read())
                    if artifact is not None:
                        if a != artifact and a.startswith(artifact):
                            c = a[len(artifact) :].lstrip('-_')
                            a = artifact
            else:
                import xml.etree.ElementTree as et

                with open(pom_path, 'rb') as f:
                    root = et.fromstring(f.read())
                for xpath in ('./{http://maven.apache.org/POM/4.0.0}artifactId', './artifactId'):
                    artifact = root.find(xpath)
                    if artifact is not None:
                        artifact = artifact.text
                        if a != artifact and a.startswith(artifact):
                            c = a[len(artifact) :].lstrip('-_')
                            a = artifact
                        break
        except Exception as e:
            raise Exception(f"Can't parse {pom_path}: {str(e)}") from None

    return '{}:{}:{}:{}'.format(g, a, v, c)


def on_setup_maven_export_coords_if_need(unit, *args):
    if not unit.enabled('MAVEN_EXPORT'):
        return

    unit.set(['MAVEN_EXPORT_COORDS_GLOBAL', _maven_coords_for_project(unit, args[0])])


def _get_classpath(unit, dir):
    if dir.startswith(CONTRIB_JAVA_PREFIX):
        return '\\"{}\\"'.format(_maven_coords_for_project(unit, dir).rstrip(':'))
    else:
        return 'project(\\":{}\\")'.format(dir.replace('/', ':'))


def on_setup_project_coords_if_needed(unit, *args):
    if not unit.enabled('EXPORT_GRADLE'):
        return

    project_dir = args[0]
    unit.set(['EXPORT_GRADLE_CLASSPATH', _get_classpath(unit, project_dir)])


def on_java_resource_tar_validate_extract_root(unit, extract_root):
    if extract_root == '<required>':
        ymake.report_configure_error(
            'Macro JAVA_RESOURCE_TAR requires to set EXTRACT_ROOT. '
            'Usage JAVA_RESOURCE_TAR(tar_path EXTRACT_ROOT root_dir)'
        )
