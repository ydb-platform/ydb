import collections
import json
import os
import six
from hashlib import md5

import ymake
from _common import stripext, rootrel_arc_src, listid, pathid, lazy, get_no_lint_value


YA_IDE_VENV_VAR = 'YA_IDE_VENV'
PY_NAMESPACE_PREFIX = 'py/namespace'
BUILTIN_PROTO = 'builtin_proto'
DEFAULT_FLAKE8_FILE_PROCESSING_TIME = "1.5"  # in seconds
DEFAULT_BLACK_FILE_PROCESSING_TIME = "1.5"  # in seconds


def _split_macro_call(macro_call, data, item_size, chunk_size=1024):
    index = 0
    length = len(data)
    offset = item_size * chunk_size
    while index + 1 < length:
        macro_call(data[index : index + offset])
        index += offset


def is_arc_src(src, unit):
    return (
        src.startswith('${ARCADIA_ROOT}/')
        or src.startswith('${CURDIR}/')
        or unit.resolve_arc_path(src).startswith('$S/')
    )


def is_extended_source_search_enabled(path, unit):
    if not is_arc_src(path, unit):
        return False
    if unit.get('NO_EXTENDED_SOURCE_SEARCH') == 'yes':
        return False
    # contrib is unfriendly to extended source search
    if unit.resolve_arc_path(path).startswith('$S/contrib/'):
        return False
    return True


def to_build_root(path, unit):
    if is_arc_src(path, unit):
        return '${ARCADIA_BUILD_ROOT}/' + rootrel_arc_src(path, unit)
    return path


def uniq_suffix(path, unit):
    upath = unit.path()
    if '/' not in path:
        return ''
    return '.{}'.format(pathid(upath)[:4])


def pb2_arg(suf, path, mod, unit):
    return '{path}__int{py_ver}__{suf}={mod}{modsuf}'.format(
        path=stripext(to_build_root(path, unit)), suf=suf, mod=mod, modsuf=stripext(suf), py_ver=unit.get('_PYTHON_VER')
    )


def proto_arg(path, mod, unit):
    return '{}.proto={}'.format(stripext(to_build_root(path, unit)), mod)


def pb_cc_arg(suf, path, unit):
    return '{}{suf}'.format(stripext(to_build_root(path, unit)), suf=suf)


def ev_cc_arg(path, unit):
    return '{}.ev.pb.cc'.format(stripext(to_build_root(path, unit)))


def ev_arg(path, mod, unit):
    return '{}__int{}___ev_pb2.py={}_ev_pb2'.format(stripext(to_build_root(path, unit)), unit.get('_PYTHON_VER'), mod)


def mangle(name):
    if '.' not in name:
        return name
    return ''.join('{}{}'.format(len(s), s) for s in name.split('.'))


def parse_pyx_includes(filename, path, source_root, seen=None):
    def normpath(*args):
        return os.path.normpath(os.path.join(*args))

    abs_path = normpath(source_root, filename)
    seen = seen or set()
    if abs_path in seen:
        return
    seen.add(abs_path)

    if not os.path.exists(abs_path):
        # File might be missing, because it might be generated
        return

    with open(abs_path, 'rb') as f:
        # Don't parse cimports and etc - irrelevant for cython, it's linker work
        includes = [six.ensure_str(x) for x in ymake.parse_cython_includes(f.read())]

    abs_dirname = os.path.dirname(abs_path)
    # All includes are relative to the file which include
    path_dirname = os.path.dirname(path)
    file_dirname = os.path.dirname(filename)

    for incfile in includes:
        abs_path = normpath(abs_dirname, incfile)
        if os.path.exists(abs_path):
            incname, incpath = normpath(file_dirname, incfile), normpath(path_dirname, incfile)
            yield (incname, incpath)
            # search for includes in the included files
            for e in parse_pyx_includes(incname, incpath, source_root, seen):
                yield e
        else:
            # There might be arcadia root or cython relative include.
            # Don't treat such file as missing, because there must be PEERDIR on py_library
            # which contains it.
            for path in [
                source_root,
                source_root + "/contrib/tools/cython/Cython/Includes",
            ]:
                if os.path.exists(normpath(path, incfile)):
                    break
            else:
                ymake.report_configure_error("'{}' includes missing file: {} ({})".format(path, incfile, abs_path))


def has_pyx(args):
    return any(arg.endswith('.pyx') for arg in args)


def get_srcdir(path, unit):
    return rootrel_arc_src(path, unit)[: -len(path)].rstrip('/')


@lazy
def get_ruff_configs(unit):
    rel_config_path = rootrel_arc_src(unit.get('RUFF_CONFIG_PATHS_FILE'), unit)
    arc_config_path = unit.resolve_arc_path(rel_config_path)
    abs_config_path = unit.resolve(arc_config_path)
    with open(abs_config_path, 'r') as fd:
        return list(json.load(fd).values())


def add_python_lint_checks(unit, py_ver, files):
    @lazy
    def get_resolved_files():
        resolved_files = []
        for path in files:
            resolved = unit.resolve_arc_path([path])
            if resolved.startswith('$S'):  # path was resolved as source file.
                resolved_files.append(resolved)
        return resolved_files

    upath = unit.path()[3:]

    no_lint_value = get_no_lint_value(unit)
    if no_lint_value == "none":
        no_lint_allowed_paths = (
            "contrib/",
            "devtools/",
            "junk/",
            # temporary allowed, TODO: remove
            "taxi/uservices/",
            "travel/",
            "market/report/lite/",  # MARKETOUT-38662, deadline: 2021-08-12
            "passport/backend/oauth/",  # PASSP-35982
            "testenv/",  # CI-3229
            "yt/yt/",  # YT-20053
            "yt/python/",  # YT-20053
        )

        if not upath.startswith(no_lint_allowed_paths):
            ymake.report_configure_error("NO_LINT() is allowed only in " + ", ".join(no_lint_allowed_paths))

    if files and no_lint_value not in ("none", "none_internal"):
        resolved_files = get_resolved_files()
        if resolved_files:
            flake8_cfg = 'build/config/tests/flake8/flake8.conf'
            migrations_cfg = 'build/rules/flake8/migrations.yaml'
            resource = "build/external_resources/flake8_py{}".format(py_ver)
            lint_name = "py2_flake8" if py_ver == 2 else "flake8"
            params = [lint_name, "tools/flake8_linter/flake8_linter"]
            params += ["FILES"] + resolved_files
            params += ["GLOBAL_RESOURCES", resource]
            params += [
                "FILE_PROCESSING_TIME",
                unit.get("FLAKE8_FILE_PROCESSING_TIME") or DEFAULT_FLAKE8_FILE_PROCESSING_TIME,
            ]

            extra_params = []
            if unit.get("DISABLE_FLAKE8_MIGRATIONS") == "yes":
                extra_params.append("DISABLE_FLAKE8_MIGRATIONS=yes")
                config_files = [flake8_cfg, '']
            else:
                config_files = [flake8_cfg, migrations_cfg]
            params += ["CONFIGS"] + config_files

            if extra_params:
                params += ["EXTRA_PARAMS"] + extra_params
            unit.on_add_linter_check(params)

    # ruff related stuff
    if unit.get('STYLE_RUFF_VALUE') == 'yes':
        if no_lint_value in ("none", "none_internal"):
            ymake.report_configure_error(
                'NO_LINT() and STYLE_RUFF() can\'t be enabled both at the same time',
            )

        resolved_files = get_resolved_files()
        if resolved_files:
            resource = "build/external_resources/ruff"
            params = ["ruff", "tools/ruff_linter/bin/ruff_linter"]
            params += ["FILES"] + resolved_files
            params += ["GLOBAL_RESOURCES", resource]
            configs = [
                rootrel_arc_src(unit.get('RUFF_CONFIG_PATHS_FILE'), unit),
                'build/config/tests/ruff/ruff.toml',
            ] + get_ruff_configs(unit)
            params += ['CONFIGS'] + configs
            unit.on_add_linter_check(params)

    if files and unit.get('STYLE_PYTHON_VALUE') == 'yes' and is_py3(unit):
        resolved_files = get_resolved_files()
        if resolved_files:
            black_cfg = unit.get('STYLE_PYTHON_PYPROJECT_VALUE') or 'build/config/tests/py_style/config.toml'
            params = ['black', 'tools/black_linter/black_linter']
            params += ['FILES'] + resolved_files
            params += ['CONFIGS', black_cfg]
            params += [
                "FILE_PROCESSING_TIME",
                unit.get("BLACK_FILE_PROCESSING_TIME") or DEFAULT_BLACK_FILE_PROCESSING_TIME,
            ]
            unit.on_add_linter_check(params)


def is_py3(unit):
    return unit.get("PYTHON3") == "yes"


def on_py_program(unit, *args):
    py_program(unit, is_py3(unit))


def py_program(unit, py3):
    """
    Documentation: https://wiki.yandex-team.ru/devtools/commandsandvars/py_srcs/#modulpyprogramimakrospymain
    """
    if py3:
        peers = ['library/python/runtime_py3/main']
        if unit.get('PYTHON_SQLITE3') != 'no':
            peers.append('contrib/tools/python3/Modules/_sqlite')
    else:
        peers = ['library/python/runtime/main']
        if unit.get('PYTHON_SQLITE3') != 'no':
            peers.append('contrib/tools/python/src/Modules/_sqlite')
    unit.onpeerdir(peers)
    if unit.get('MODULE_TYPE') == 'PROGRAM':  # can not check DLL
        unit.onadd_check_py_imports()


def onpy_srcs(unit, *args):
    """
    @usage PY_SRCS({| CYTHONIZE_PY} {| CYTHON_C} { | TOP_LEVEL | NAMESPACE ns} Files...)

    PY_SRCS() - is rule to build extended versions of Python interpreters and containing all application code in its executable file.
    It can be used to collect only the executables but not shared libraries, and, in particular, not to collect the modules that are imported using import directive.
    The main disadvantage is the lack of IDE support; There is also no readline yet.
    The application can be collect from any of the sources from which the C library, and with the help of PY_SRCS .py , .pyx,.proto and .swg files.
    At the same time extensions for Python on C language generating from .pyx and .swg, will be registered in Python's as built-in modules, and sources on .py are stored as static data:
    when the interpreter starts, the initialization code will add a custom loader of these modules to sys.meta_path.
    You can compile .py files as Cython sources with CYTHONIZE_PY directive (Use carefully, as build can get too slow). However, with it you won't have profiling info by default.
    To enable it, add "# cython: profile=True" line to the beginning of every cythonized source.
    By default .pyx files are collected as C++-extensions. To collect them as C (similar to BUILDWITH_CYTHON_C, but with the ability to specify namespace), you must specify the Directive CYTHON_C.
    Building with pyx automatically registers modules, you do not need to call PY_REGISTER for them
    __init__.py never required, but if present (and specified in PY_SRCS), it will be imported when you import package modules with __init__.py Oh.

    Example of library declaration with PY_SRCS():
    PY2_LIBRARY(mymodule)
    PY_SRCS(a.py sub/dir/b.py e.proto sub/dir/f.proto c.pyx sub/dir/d.pyx g.swg sub/dir/h.swg)
    END()

    PY_REGISTER honors Python2 and Python3 differences and adjusts itself to Python version of a current module
    Documentation: https://wiki.yandex-team.ru/arcadia/python/pysrcs/#modulipylibrarypy3libraryimakrospysrcs
    """
    # Each file arg must either be a path, or "${...}/buildpath=modname", where
    # "${...}/buildpath" part will be used as a file source in a future macro,
    # and "modname" will be used as a module name.

    upath = unit.path()[3:]
    py3 = is_py3(unit)
    py_main_only = unit.get('PROCESS_PY_MAIN_ONLY')
    with_py = not unit.get('PYBUILD_NO_PY')
    with_pyc = not unit.get('PYBUILD_NO_PYC')
    in_proto_library = unit.get('PY_PROTO') or unit.get('PY3_PROTO')
    venv = unit.get(YA_IDE_VENV_VAR)
    need_gazetteer_peerdir = False
    trim = 0

    if (
        not upath.startswith('contrib/tools/python')
        and not upath.startswith('library/python/runtime')
        and unit.get('NO_PYTHON_INCLS') != 'yes'
    ):
        unit.onpeerdir(['contrib/libs/python'])

    unit_needs_main = unit.get('MODULE_TYPE') in ('PROGRAM', 'DLL')
    if unit_needs_main:
        py_program(unit, py3)

    py_namespace_value = unit.get('PY_NAMESPACE_VALUE')
    if py_namespace_value == ".":
        ns = ""
    else:
        ns = (unit.get('PY_NAMESPACE_VALUE') or upath.replace('/', '.')) + '.'

    cython_coverage = unit.get('CYTHON_COVERAGE') == 'yes'
    cythonize_py = False
    optimize_proto = unit.get('OPTIMIZE_PY_PROTOS_FLAG') == 'yes'

    cython_directives = []
    if cython_coverage:
        cython_directives += ['-X', 'linetrace=True']

    pyxs_c = []
    pyxs_c_h = []
    pyxs_c_api_h = []
    pyxs_cpp = []
    pyxs_cpp_h = []
    pyxs = pyxs_cpp
    swigs_c = []
    swigs_cpp = []
    swigs = swigs_cpp
    pys = []
    pyis = []
    protos = []
    evs = []
    fbss = []
    py_namespaces = {}

    dump_dir = unit.get('PYTHON_BUILD_DUMP_DIR')
    dump_output = None
    if dump_dir:
        import threading

        pid = os.getpid()
        tid = threading.current_thread().ident
        dump_name = '{}-{}.dump'.format(pid, tid)
        dump_output = open(os.path.join(dump_dir, dump_name), 'a')

    args = iter(args)
    for arg in args:
        # Namespace directives.
        if arg == 'TOP_LEVEL':
            ns = ''
        elif arg == 'NAMESPACE':
            ns = next(args) + '.'
        # Cython directives.
        elif arg == 'CYTHON_C':
            pyxs = pyxs_c
        elif arg == 'CYTHON_C_H':
            pyxs = pyxs_c_h
        elif arg == 'CYTHON_C_API_H':
            pyxs = pyxs_c_api_h
        elif arg == 'CYTHON_CPP':
            pyxs = pyxs_cpp
        elif arg == 'CYTHON_CPP_H':
            pyxs = pyxs_cpp_h
        elif arg == 'CYTHON_DIRECTIVE':
            cython_directives += ['-X', next(args)]
        elif arg == 'CYTHONIZE_PY':
            cythonize_py = True
        # SWIG.
        elif arg == 'SWIG_C':
            swigs = swigs_c
        elif arg == 'SWIG_CPP':
            swigs = swigs_cpp
        # Unsupported but legal PROTO_LIBRARY arguments.
        elif arg == 'GLOBAL' or not in_proto_library and arg.endswith('.gztproto'):
            pass
        elif arg == '_MR':
            # GLOB support: convert arcadia-root-relative paths to module-relative
            # srcs are assumed to start with ${ARCADIA_ROOT}
            trim = len(unit.path()) + 14
        # Sources.
        else:
            main_mod = arg == 'MAIN'
            if main_mod:
                arg = next(args)

            if '=' in arg:
                main_py = False
                path, mod = arg.split('=', 1)
            else:
                if trim:
                    arg = arg[trim:]
                if arg.endswith('.gztproto'):
                    need_gazetteer_peerdir = True
                    path = '{}.proto'.format(arg[:-9])
                else:
                    path = arg
                main_py = path == '__main__.py' or path.endswith('/__main__.py')
                if not py3 and unit_needs_main and main_py:
                    mod = '__main__'
                else:
                    if arg.startswith('../'):
                        ymake.report_configure_error('PY_SRCS item starts with "../": {!r}'.format(arg))
                    if arg.startswith('/'):
                        ymake.report_configure_error('PY_SRCS item starts with "/": {!r}'.format(arg))
                        continue
                    mod_name = stripext(arg).replace('/', '.')
                    if py3 and path.endswith('.py') and is_extended_source_search_enabled(path, unit):
                        # Dig out real path from the file path. Unit.path is not enough because of SRCDIR and ADDINCL
                        root_rel_path = rootrel_arc_src(path, unit)
                        mod_root_path = root_rel_path[: -(len(path) + 1)]
                        py_namespaces.setdefault(mod_root_path, set()).add(ns if ns else '.')
                    mod = ns + mod_name
                    if in_proto_library:
                        mod = mod.replace('-', '_')

            if main_mod:
                py_main(unit, mod + ":main")
            elif py3 and unit_needs_main and main_py:
                py_main(unit, mod)

            if py_main_only:
                continue

            if py3 and mod == '__main__':
                ymake.report_configure_error('TOP_LEVEL __main__.py is not allowed in PY3_PROGRAM')

            pathmod = (path, mod)

            if dump_output is not None:
                dump_output.write(
                    '{path}\t{module}\t{py3}\n'.format(
                        path=rootrel_arc_src(path, unit), module=mod, py3=1 if py3 else 0
                    )
                )

            if path.endswith('.py'):
                if cythonize_py:
                    pyxs.append(pathmod)
                else:
                    pys.append(pathmod)
            elif path.endswith('.pyx'):
                pyxs.append(pathmod)
            elif path.endswith('.proto'):
                protos.append(pathmod)
            elif path.endswith('.ev'):
                evs.append(pathmod)
            elif path.endswith('.swg'):
                swigs.append(pathmod)
            elif path.endswith('.pyi'):
                pyis.append(pathmod)
            elif path.endswith('.fbs'):
                fbss.append(pathmod)
            else:
                ymake.report_configure_error('in PY_SRCS: unrecognized arg {!r}'.format(path))

    if dump_output is not None:
        dump_output.close()

    if pyxs:
        py_files2res = set()
        cpp_files2res = set()
        # Include map stores files which were included in the processing pyx file,
        # to be able to find source code of the included file inside generated file
        # for currently processing pyx file.
        include_map = collections.defaultdict(set)

        if cython_coverage:

            def process_pyx(filename, path, out_suffix, with_ext):
                # skip generated files
                if not is_arc_src(path, unit):
                    return
                # source file
                py_files2res.add((filename, path))
                # generated
                if with_ext is None:
                    cpp_files2res.add(
                        (
                            os.path.splitext(filename)[0] + out_suffix,
                            os.path.splitext(path)[0] + out_suffix,
                        )
                    )
                else:
                    cpp_files2res.add((filename + with_ext + out_suffix, path + with_ext + out_suffix))
                # used includes
                for entry in parse_pyx_includes(filename, path, unit.resolve('$S')):
                    py_files2res.add(entry)
                    include_arc_rel = entry[0]
                    include_map[filename].add(include_arc_rel)

        else:

            def process_pyx(filename, path, out_suffix, with_ext):
                pass

        obj_suff = unit.get('OBJ_SUF')
        assert obj_suff is not None
        for pyxs, cython, out_suffix, with_ext in [
            (pyxs_c, unit.on_buildwith_cython_c_dep, ".c", obj_suff),
            (pyxs_c_h, unit.on_buildwith_cython_c_h, ".c", None),
            (pyxs_c_api_h, unit.on_buildwith_cython_c_api_h, ".c", None),
            (pyxs_cpp, unit.on_buildwith_cython_cpp_dep, ".cpp", obj_suff),
            (pyxs_cpp_h, unit.on_buildwith_cython_cpp_h, ".cpp", None),
        ]:
            for path, mod in pyxs:
                filename = rootrel_arc_src(path, unit)
                cython_args = [path]

                dep = path
                if path.endswith('.py'):
                    pxd = '/'.join(mod.split('.')) + '.pxd'
                    if unit.resolve_arc_path(pxd):
                        dep = pxd
                cython_args.append(dep)

                cython_args += [
                    '--module-name',
                    mod,
                    '--init-suffix',
                    mangle(mod),
                    '--source-root',
                    '${ARCADIA_ROOT}',
                    # set arcadia root relative __file__ for generated modules
                    '-X',
                    'set_initial_path={}'.format(filename),
                ] + cython_directives

                cython(cython_args)
                py_register(unit, mod, py3)
                process_pyx(filename, path, out_suffix, with_ext)

            if cythonize_py:
                # Lint checks are not added for cythonized files by default, so we must add it here
                # as we are doing for regular pys.
                _23 = 3 if py3 else 2
                add_python_lint_checks(
                    unit,
                    _23,
                    [path for path, mod in pyxs if path.endswith(".py")]
                    + unit.get(['_PY_EXTRA_LINT_FILES_VALUE']).split(),
                )

        if py_files2res:
            # Compile original and generated sources into target for proper cython coverage calculation
            for files2res in (py_files2res, cpp_files2res):
                unit.onresource_files([x for name, path in files2res for x in ('DEST', name, path)])

        if include_map:
            data = []
            prefix = 'resfs/cython/include'
            for line in sorted(
                '{}/{}={}'.format(prefix, filename, ':'.join(sorted(files)))
                for filename, files in six.iteritems(include_map)
            ):
                data += ['-', line]
            unit.onresource(data)

    for swigs, on_swig_python in [
        (swigs_c, unit.on_swig_python_c),
        (swigs_cpp, unit.on_swig_python_cpp),
    ]:
        for path, mod in swigs:
            # Make output prefix basename match swig module name.
            prefix = path[: path.rfind('/') + 1] + mod.rsplit('.', 1)[-1]
            swg_py = '{}/{}/{}.py'.format('${ARCADIA_BUILD_ROOT}', upath, prefix)
            on_swig_python([path, prefix])
            onpy_register(unit, mod + '_swg')
            onpy_srcs(unit, swg_py + '=' + mod)

    if pys:
        pys_seen = set()
        pys_dups = {m for _, m in pys if (m in pys_seen or pys_seen.add(m))}
        if pys_dups:
            ymake.report_configure_error('Duplicate(s) is found in the PY_SRCS macro: {}'.format(pys_dups))

        res = []

        if py3:
            mod_list_md5 = md5()
            for path, mod in pys:
                mod_list_md5.update(six.ensure_binary(mod))
                if not (venv and is_extended_source_search_enabled(path, unit)):
                    dest = 'py/' + mod.replace('.', '/') + '.py'
                    if with_py:
                        res += ['DEST', dest, path]
                    if with_pyc:
                        root_rel_path = rootrel_arc_src(path, unit)
                        dst = path + uniq_suffix(path, unit)
                        unit.on_py3_compile_bytecode([root_rel_path + '-', path, dst])
                        res += ['DEST', dest + '.yapyc3', dst + '.yapyc3']

            if py_namespaces:
                # Note: Add md5 to key to prevent key collision if two or more PY_SRCS() used in the same ya.make
                ns_res = []
                for path, ns in sorted(py_namespaces.items()):
                    key = '{}/{}/{}'.format(PY_NAMESPACE_PREFIX, mod_list_md5.hexdigest(), path)
                    namespaces = ':'.join(sorted(ns))
                    ns_res += ['-', '{}="{}"'.format(key, namespaces)]
                unit.onresource(ns_res)

            _split_macro_call(unit.onresource_files, res, (3 if with_py else 0) + (3 if with_pyc else 0))
            add_python_lint_checks(
                unit, 3, [path for path, mod in pys] + unit.get(['_PY_EXTRA_LINT_FILES_VALUE']).split()
            )
        else:
            for path, mod in pys:
                root_rel_path = rootrel_arc_src(path, unit)
                if with_py:
                    key = '/py_modules/' + mod
                    res += [
                        path,
                        key,
                        '-',
                        'resfs/src/{}={}'.format(key, root_rel_path),
                    ]
                if with_pyc:
                    src = unit.resolve_arc_path(path) or path
                    dst = path + uniq_suffix(path, unit)
                    unit.on_py_compile_bytecode([root_rel_path + '-', src, dst])
                    res += [dst + '.yapyc', '/py_code/' + mod]

            _split_macro_call(unit.onresource, res, (4 if with_py else 0) + (2 if with_pyc else 0))
            add_python_lint_checks(
                unit, 2, [path for path, mod in pys] + unit.get(['_PY_EXTRA_LINT_FILES_VALUE']).split()
            )

    if pyis:
        pyis_seen = set()
        pyis_dups = {m for _, m in pyis if (m in pyis_seen or pyis_seen.add(m))}
        if pyis_dups:
            pyis_dups = ', '.join(name for name in sorted(pyis_dups))
            ymake.report_configure_error('Duplicate(s) is found in the PY_SRCS macro: {}'.format(pyis_dups))

        res = []
        for path, mod in pyis:
            dest = 'py/' + mod.replace('.', '/') + '.pyi'
            res += ['DEST', dest, path]

        unit.onresource_files(res)

    use_vanilla_protoc = unit.get('USE_VANILLA_PROTOC') == 'yes'
    if use_vanilla_protoc:
        cpp_runtime_path = 'contrib/libs/protobuf_std'
        py_runtime_path = 'contrib/python/protobuf_std'
        builtin_proto_path = cpp_runtime_path + '/' + BUILTIN_PROTO
    else:
        cpp_runtime_path = 'contrib/libs/protobuf'
        py_runtime_path = 'contrib/python/protobuf'
        builtin_proto_path = cpp_runtime_path + '/' + BUILTIN_PROTO

    if protos:
        if not upath.startswith(py_runtime_path) and not upath.startswith(builtin_proto_path):
            if 'protobuf_old' not in upath:
                unit.onpeerdir(py_runtime_path)

        unit.onpeerdir(unit.get("PY_PROTO_DEPS").split())

        proto_paths = [path for path, mod in protos]
        unit.on_generate_py_protos_internal(proto_paths)
        unit.onpy_srcs(
            [
                pb2_arg(py_suf, path, mod, unit)
                for path, mod in protos
                for py_suf in unit.get("PY_PROTO_SUFFIXES").split()
            ]
        )

        if optimize_proto and need_gazetteer_peerdir:
            unit.onpeerdir(['kernel/gazetteer/proto'])

    if evs:
        unit.onpeerdir([cpp_runtime_path])
        unit.on_generate_py_evs_internal([path for path, mod in evs])
        unit.onpy_srcs([ev_arg(path, mod, unit) for path, mod in evs])

    if fbss:
        unit.onpeerdir(unit.get('_PY_FBS_DEPS').split())
        pysrc_base_name = listid(fbss)
        if py3:
            unit.onfbs_to_pysrc([pysrc_base_name] + [path for path, _ in fbss])
            unit.onsrcs(['GLOBAL', '{}.py3.fbs.pysrc'.format(pysrc_base_name)])
        else:
            unit.onfbs_to_py2src([pysrc_base_name] + [path for path, _ in fbss])
            unit.onsrcs(['GLOBAL', '{}.py2.fbs.pysrc'.format(pysrc_base_name)])


def _check_test_srcs(*args):
    used = set(args) & {"NAMESPACE", "TOP_LEVEL", "__main__.py"}
    if used:
        param = list(used)[0]
        ymake.report_configure_error(
            'in TEST_SRCS: you cannot use {} here - it would broke testing machinery'.format(param)
        )


def ontest_srcs(unit, *args):
    _check_test_srcs(*args)
    if unit.get('PY3TEST_BIN' if is_py3(unit) else 'PYTEST_BIN') != 'no':
        unit.onpy_srcs(["NAMESPACE", "__tests__"] + list(args))


def onpy_doctests(unit, *args):
    """
    @usage PY_DOCTESTS(Packages...)

    Add to the test doctests for specified Python packages
    The packages should be part of a test (listed as sources of the test or its PEERDIRs).
    """
    if unit.get('PY3TEST_BIN' if is_py3(unit) else 'PYTEST_BIN') != 'no':
        unit.onresource(['-', 'PY_DOCTEST_PACKAGES="{}"'.format(' '.join(args))])


def py_register(unit, func, py3):
    if py3:
        unit.on_py3_register([func])
    else:
        unit.on_py_register([func])


def onpy_register(unit, *args):
    """
    @usage: PY_REGISTER([package.]module_name)

    Python knows about which built-ins can be imported, due to their registration in the Assembly or at the start of the interpreter.
    All modules from the sources listed in PY_SRCS() are registered automatically.
    To register the modules from the sources in the SRCS(), you need to use PY_REGISTER().

    PY_REGISTER(module_name) initializes module globally via call to initmodule_name()
    PY_REGISTER(package.module_name) initializes module in the specified package
    It renames its init function with CFLAGS(-Dinitmodule_name=init7package11module_name)
    or CFLAGS(-DPyInit_module_name=PyInit_7package11module_name)

    Documentation: https://wiki.yandex-team.ru/arcadia/python/pysrcs/#makrospyregister
    """

    py3 = is_py3(unit)

    for name in args:
        assert '=' not in name, name
        py_register(unit, name, py3)
        if '.' in name:
            shortname = name.rsplit('.', 1)[1]
            if py3:
                unit.oncflags(['-DPyInit_{}=PyInit_{}'.format(shortname, mangle(name))])
            else:
                unit.oncflags(['-Dinit{}=init{}'.format(shortname, mangle(name))])


def py_main(unit, arg):
    if unit.get('IGNORE_PY_MAIN'):
        return
    unit_needs_main = unit.get('MODULE_TYPE') in ('PROGRAM', 'DLL')
    if unit_needs_main:
        py_program(unit, is_py3(unit))
    unit.onresource(['-', 'PY_MAIN={}'.format(arg)])


def onpy_main(unit, arg):
    """
    @usage: PY_MAIN(package.module[:func])

    Specifies the module or function from which to start executing a python program

    Documentation: https://wiki.yandex-team.ru/arcadia/python/pysrcs/#modulipyprogrampy3programimakrospymain
    """

    arg = arg.replace('/', '.')

    if ':' not in arg:
        arg += ':main'

    py_main(unit, arg)


def onpy_constructor(unit, arg):
    """
    @usage: PY_CONSTRUCTOR(package.module[:func])

    Specifies the module or function which will be started before python's main()
    init() is expected in the target module if no function is specified
    Can be considered as __attribute__((constructor)) for python
    """
    if ':' not in arg:
        arg = arg + '=init'
    else:
        arg[arg.index(':')] = '='
    unit.onresource(['-', 'py/constructors/{}'.format(arg)])


def onpy_enums_serialization(unit, *args):
    ns = ''
    args = iter(args)
    for arg in args:
        # Namespace directives.
        if arg == 'NAMESPACE':
            ns = next(args)
        else:
            unit.on_py_enum_serialization_to_json(arg)
            unit.on_py_enum_serialization_to_py(arg)
            filename = arg.rsplit('.', 1)[0] + '.py'
            if len(ns) != 0:
                onpy_srcs(unit, 'NAMESPACE', ns, filename)
            else:
                onpy_srcs(unit, filename)


def oncpp_enums_serialization(unit, *args):
    args = iter(args)
    for arg in args:
        # Namespace directives.
        if arg == 'NAMESPACE':
            next(args)
        else:
            unit.ongenerate_enum_serialization_with_header(arg)
