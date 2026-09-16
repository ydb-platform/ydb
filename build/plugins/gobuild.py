import itertools
from hashlib import md5
import os
import posixpath
from _common import resolve_common_const, rootrel_arc_src, tobuilddir
import ymake

runtime_cgo_path = os.path.join('runtime', 'cgo')
runtime_msan_path = os.path.join('runtime', 'msan')
runtime_race_path = os.path.join('runtime', 'race')
arc_project_prefix = 'a.yandex-team.ru/'
import_runtime_cgo_false = {
    'norace': (runtime_cgo_path, runtime_msan_path, runtime_race_path),
    'race': (runtime_cgo_path, runtime_msan_path),
}
import_syscall_false = {
    'norace': (runtime_cgo_path),
    'race': (runtime_cgo_path, runtime_race_path),
}


def get_import_path(unit):
    # std_lib_prefix = unit.get('GO_STD_LIB_PREFIX')
    # unit.get() doesn't evalutate the value of variable, so the line above doesn't really work
    std_lib_prefix = unit.get('GOSTD') + '/'
    arc_project_prefix = unit.get('GO_ARCADIA_PROJECT_PREFIX')
    vendor_prefix = unit.get('GO_CONTRIB_PROJECT_PREFIX')

    module_path = rootrel_arc_src(unit.path(), unit)
    assert len(module_path) > 0

    if go_package_name(unit) == "main":
        return "main"

    import_path = module_path.replace('\\', '/')
    if import_path.startswith(std_lib_prefix):
        import_path = import_path[len(std_lib_prefix) :]
    elif import_path.startswith(vendor_prefix):
        import_path = import_path[len(vendor_prefix) :]
    else:
        import_path = arc_project_prefix + import_path
    assert len(import_path) > 0

    if import_path.endswith("/gotest"):
        return import_path[:-7]

    return import_path


def get_appended_values(unit, key):
    values = []
    raw_value = unit.get(key)
    if raw_value:
        values = [x for x in raw_value.split(' ') if x]
        if len(values) > 0 and values[0] == '$' + key:
            values.pop(0)
    return values


def compare_versions(version1, version2):
    def last_index(version):
        index = version.find('beta')
        return len(version) if index < 0 else index

    v1 = tuple(x.zfill(8) for x in version1[: last_index(version1)].split('.'))
    v2 = tuple(x.zfill(8) for x in version2[: last_index(version2)].split('.'))
    if v1 == v2:
        return 0
    return 1 if v1 < v2 else -1


def go_package_name(unit):
    name = unit.get('_GO_PACKAGE_VALUE')
    if name:
        return name
    if unit.enabled('GO_TEST_MODULE'):
        name = unit.get('GO_PACKAGE_VALUE')
        if name:
            return name
    name = unit.get('GO_TEST_IMPORT_PATH')
    if name:
        return os.path.basename(os.path.normpath(name))
    elif unit.get('MODULE_TYPE') == 'PROGRAM':
        return 'main'
    else:
        return unit.get('REALPRJNAME')


def need_lint(path):
    return not path.startswith('$S/vendor/') and not path.startswith('$S/contrib/')


def resolve_go_path(unit, path):
    # resolve_arc_path leaves $-prefixed paths unchanged, including source
    # variables. Missing paths resolve to an empty string; generated files to $B.
    resolved = resolve_common_const(path.replace('\\', '/'))
    if resolved.startswith('${CURDIR}/'):
        resolved = unit.path() + '/' + resolved[len('${CURDIR}/') :]
    elif resolved.startswith('${BINDIR}/'):
        resolved = tobuilddir(unit.path()) + '/' + resolved[len('${BINDIR}/') :]
    if not resolved.startswith(('$S/', '$B/')):
        resolved = unit.resolve_arc_path([resolved])
    if resolved.startswith(('$S/', '$B/')):
        root = resolved[:3]
        resolved = posixpath.normpath(resolved)
        if resolved.startswith(root):
            return resolved
    return None


@ymake.macro
def _GO_PROCESS_SRCS(unit: ymake.Unit):
    """
    _GO_PROCESS_SRCS() macro processes only 'CGO' files. All remaining *.go files
    and other input files are currently processed by a link command of the
    GO module (GO_LIBRARY, GO_PROGRAM)
    """

    srcs_files = get_appended_values(unit, '_GO_SRCS_VALUE')

    asm_files = []
    c_files = []
    cxx_files = []
    ev_files = []
    fbs_files = []
    go_files = []
    in_files = []
    proto_files = []
    s_files = []
    syso_files = []

    classified_files = {
        '.c': c_files,
        '.cc': cxx_files,
        '.cpp': cxx_files,
        '.cxx': cxx_files,
        '.ev': ev_files,
        '.fbs': fbs_files,
        '.go': go_files,
        '.in': in_files,
        '.proto': proto_files,
        '.s': asm_files,
        '.syso': syso_files,
        '.C': cxx_files,
        '.S': s_files,
    }

    # Classify files specifed in _GO_SRCS() macro by extension and process CGO_EXPORT keyword
    # which can preceed C/C++ files only
    is_cgo_export = False
    for f in srcs_files:
        _, ext = os.path.splitext(f)
        ext_files = classified_files.get(ext)
        if ext_files is not None:
            if is_cgo_export:
                is_cgo_export = False
                if ext in ('.c', '.cc', '.cpp', '.cxx', '.C'):
                    unit.oncopy_file_with_context([f, f, 'OUTPUT_INCLUDES', '${BINDIR}/_cgo_export.h'])
                    f = '${BINDIR}/' + f
                else:
                    ymake.report_configure_error('Unmatched CGO_EXPORT keyword in SRCS() macro')
            ext_files.append(f)
        elif f == 'CGO_EXPORT':
            is_cgo_export = True
        else:
            # FIXME(snermolaev): We can report an unsupported files for _GO_SRCS here
            pass
    if is_cgo_export:
        ymake.report_configure_error('Unmatched CGO_EXPORT keyword in SRCS() macro')

    for f in go_files:
        if f.endswith('_test.go'):
            ymake.report_configure_error('file {} must be listed in GO_TEST_SRCS() or GO_XTEST_SRCS() macros'.format(f))
    go_test_files = get_appended_values(unit, '_GO_TEST_SRCS_VALUE')
    go_xtest_files = get_appended_values(unit, '_GO_XTEST_SRCS_VALUE')
    skipped_tests = get_appended_values(unit, '_ALL_GO_SKIPPED_TEST_FILES')
    if skipped_tests:
        go_unused_test_files = get_appended_values(unit, '_GO_UNUSED_TEST_SRCS_VALUE')
        declared_tests = {resolve_go_path(unit, f) for f in go_test_files + go_xtest_files + go_unused_test_files}
        missing_tests = [f for f in skipped_tests if resolve_go_path(unit, f) not in declared_tests]
        if missing_tests:
            unit.message(
                [
                    'WARN',
                    'ALL_GO_SRCS() skips test files not declared in GO_TEST_SRCS(), GO_XTEST_SRCS() '
                    'or GO_UNUSED_TEST_SRCS() (intentionally unused): '
                    + ', '.join(rootrel_arc_src(f, unit) for f in missing_tests),
                ]
            )
    for f in go_test_files + go_xtest_files:
        if not f.endswith('_test.go'):
            ymake.report_configure_error(
                'file {} should not be listed in GO_TEST_SRCS() or GO_XTEST_SRCS() macros'.format(f)
            )

    is_test_module = unit.enabled('GO_TEST_MODULE')

    unit_path = unit.path()

    # Add gofmt style checks
    add_fmt = True
    if not unit.enabled('_GO_FMT_ADD_CHECK'):
        allow_skip_fmt = unit.get('_GO_FMT_ALLOW_SKIP')
        allow_skip_fmt_list = None
        if allow_skip_fmt:
            allow_skip_fmt_list = unit.get('_GO_FMT_ALLOW_SKIP').split(' ')

        if allow_skip_fmt_list:
            unit_rel_path = rootrel_arc_src(unit_path, unit)
            if unit_rel_path in allow_skip_fmt_list:
                add_fmt = False
            else:
                for item in allow_skip_fmt_list:
                    if item:
                        prefix = item if item[-1] == '/' else item + '/'
                        if unit_rel_path.startswith(prefix):
                            add_fmt = False
                            break
        if add_fmt:
            ymake.report_configure_error('Disabling gofmt is prohibited, please contact devtools')

    if add_fmt:
        resolved_go_files = []
        go_source_files = []
        if not (is_test_module and unit.get('GO_TEST_FOR_DIR')):
            go_source_files = list(go_files)
            # Keep globbed sources in style checks, including CGO originals.
            all_go_files = unit.get('_ALL_GO_FILES')
            if all_go_files:
                go_source_files.extend(all_go_files.split())
        for path in itertools.chain(go_source_files, go_test_files, go_xtest_files):
            if path.endswith('.go'):
                resolved = resolve_go_path(unit, path)
                if resolved and resolved.startswith('$S/') and need_lint(resolved):
                    resolved_go_files.append(resolved)
        if resolved_go_files:
            basedirs = {}
            for f in dict.fromkeys(resolved_go_files):
                basedir = os.path.dirname(f)
                if basedir not in basedirs:
                    basedirs[basedir] = []
                basedirs[basedir].append(f)
            for basedir in basedirs:
                unit.onadd_check(['gofmt'] + basedirs[basedir])

    # ALL_GO_SRCS and SRCS may name the same file differently. Compare resolved
    # source paths, but keep the first spelling and order for build commands.
    # CGO originals must only enter the CGO pipeline, never Go coverage/compile.
    cgo_files = get_appended_values(unit, '_CGO_SRCS_VALUE')
    seen_go_files = {resolve_go_path(unit, f) or f for f in cgo_files}
    unique_go_files = []
    for f in go_files:
        key = resolve_go_path(unit, f) or f
        if key not in seen_go_files:
            seen_go_files.add(key)
            unique_go_files.append(f)
    go_files = unique_go_files

    # Go coverage instrumentation (NOTE! go_files list is modified here)
    if is_test_module and unit.enabled('GO_TEST_COVER'):
        cover_files = []
        generated_go_files = []
        for f in go_files:
            resolved = resolve_go_path(unit, f)
            if resolved and resolved.startswith('$S/'):
                cover_files.append(f)
            else:
                generated_go_files.append(f)
        unit.set(['GO_COVER_MODE', 'set'])  # Enable Go coverage with mode="set"
        unit.on_go_gen_cover([go_package_name(unit), *cover_files])

        # The coverage command reads paths relative to the source root. Generated
        # files must keep their original inputs and generation dependencies, just
        # as files added automatically by generators after this plugin runs.
        go_files = generated_go_files

    # We have cleaned up the list of files from _GO_SRCS_VALUE var and we have to update
    # the value since it is used in module command line
    unit.set(['_GO_SRCS_VALUE', ' '.join(itertools.chain(go_files, asm_files, syso_files))])

    # Add go vet check
    if unit.enabled('_GO_VET_ADD_CHECK') and need_lint(unit_path):
        vet_report_file_name = os.path.join(unit_path, '{}{}'.format(unit.filename(), unit.get('GO_VET_REPORT_EXT')))
        unit.onadd_check(["govet", '$(BUILD_ROOT)/' + tobuilddir(vet_report_file_name)[3:]])

    for f in ev_files:
        ev_proto_file = '{}.proto'.format(f)
        unit.oncopy_file_with_context([f, ev_proto_file])
        proto_files.append(ev_proto_file)

    # Process .proto files
    for f in proto_files:
        unit.on_go_proto_cmd(f)

    # Process .fbs files
    for f in fbs_files:
        unit.on_go_flatc_cmd([f, go_package_name(unit)])

    # Process .in files
    for f in in_files:
        unit.onsrc(f)

    # Generate .symabis for .s files (starting from 1.12 version)
    if len(asm_files) > 0:
        symabis_flags = []
        gostd_version = unit.get('GOSTD_VERSION')
        if compare_versions('1.16', gostd_version) >= 0:
            import_path = get_import_path(unit)
            symabis_flags.extend(['FLAGS', '-p', import_path])
        unit.on_go_compile_symabis(asm_files + symabis_flags)

    # Process cgo files
    cgo_cflags = []
    if len(c_files) + len(cxx_files) + len(s_files) + len(cgo_files) > 0:
        if is_test_module:
            go_test_for_dir = unit.get('GO_TEST_FOR_DIR')
            if go_test_for_dir and go_test_for_dir.startswith('$S/'):
                unit.onaddincl(['FOR', 'c', go_test_for_dir[3:]])
        unit.onaddincl(['FOR', 'c', unit.get('MODDIR')])
        cgo_cflags = get_appended_values(unit, 'CGO_CFLAGS_VALUE')

    for f in itertools.chain(c_files, cxx_files, s_files):
        unit.onsrc([f] + cgo_cflags)

    if len(cgo_files) > 0:
        if not unit.enabled('CGO_ENABLED'):
            ymake.report_configure_error('trying to build with CGO (CGO_SRCS is non-empty) when CGO is disabled')
        import_path = get_import_path(unit)
        if import_path != runtime_cgo_path:
            go_std_root = unit.get('GOSTD')
            unit.onpeerdir(os.path.join(go_std_root, runtime_cgo_path))
        race_mode = 'race' if unit.enabled('RACE') else 'norace'
        import_runtime_cgo = 'false' if import_path in import_runtime_cgo_false[race_mode] else 'true'
        import_syscall = 'false' if import_path in import_syscall_false[race_mode] else 'true'
        args = (
            [import_path]
            + cgo_files
            + ['FLAGS', '-import_runtime_cgo=' + import_runtime_cgo, '-import_syscall=' + import_syscall]
        )
        unit.on_go_compile_cgo1(args)
        cgo2_cflags = get_appended_values(unit, 'CGO2_CFLAGS_VALUE')
        for f in cgo_files:
            if f.endswith('.go'):
                unit.onsrc([f[:-2] + 'cgo2.c'] + cgo_cflags + cgo2_cflags)
            else:
                ymake.report_configure_error('file {} should not be listed in CGO_SRCS() macros'.format(f))
        args = [go_package_name(unit)] + cgo_files
        if len(c_files) > 0:
            args += ['C_FILES'] + c_files
        if len(s_files) > 0:
            args += ['S_FILES'] + s_files
        if len(syso_files) > 0:
            args += ['OBJ_FILES'] + syso_files
        unit.on_go_compile_cgo2(args)


@ymake.macro
def _GO_RESOURCE(unit: ymake.Unit, *args: str):
    args = list(args)
    files = args[::2]
    keys = args[1::2]
    suffix_md5 = md5('@'.join(args).encode()).hexdigest()
    resource_go = os.path.join("resource.{}.res.go".format(suffix_md5))

    unit.onpeerdir(["library/go/core/resource"])

    if len(files) != len(keys):
        ymake.report_configure_error("last file {} is missing resource key".format(files[-1]))

    for i, (key, filename) in enumerate(zip(keys, files)):
        if not key:
            ymake.report_configure_error("file key must be non empty")
            return

        if filename == "-" and "=" not in key:
            ymake.report_configure_error("key \"{}\" must contain = sign".format(key))
            return

        # quote key, to avoid automatic substitution of filename by absolute
        # path in RUN_PROGRAM
        args[2 * i + 1] = "notafile" + args[2 * i + 1]

    files = [file for file in files if file != "-"]
    unit.onrun_program(
        ["library/go/core/resource/cc", "-package", go_package_name(unit), "-o", resource_go]
        + list(args)
        + ["IN"]
        + files
        + ["OUT", resource_go]
    )
