import io
import json
import optparse
import os
import sys
import subprocess
import time
import zipfile
import platform

# This script changes test run classpath by unpacking tests.jar -> tests-dir. The goal
# is to launch tests with the same classpath as maven does.

PY3 = sys.version_info[0] == 3


def parse_args():
    parser = optparse.OptionParser()
    parser.disable_interspersed_args()
    parser.add_option('--trace-file')
    parser.add_option('--jar-binary')
    parser.add_option('--tests-jar-path')
    parser.add_option('--classpath-option-type', choices=('manifest', 'command_file', 'list'), default='manifest')
    return parser.parse_args()


# temporary, for jdk8/jdk9+ compatibility
def fix_cmd(cmd):
    if not cmd:
        return cmd
    java = cmd[0]
    if not java.endswith('java') and not java.endswith('java.exe'):
        return cmd
    p = None
    # FIXME: Remove after py3 migration
    if PY3:
        p = subprocess.Popen([java, '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    else:
        p = subprocess.Popen([java, '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    out, err = out.strip(), err.strip()
    if (out or '').strip().startswith('java version "1.8') or (err or '').strip().startswith('java version "1.8'):
        res = []
        i = 0
        while i < len(cmd):
            for option in ('--add-exports', '--add-modules'):
                if cmd[i] == option:
                    i += 1
                    break
                elif cmd[i].startswith(option + '='):
                    break
            else:
                res.append(cmd[i])
            i += 1
        return res
    return cmd


def dump_event(etype, data, filename):
    event = {
        'timestamp': time.time(),
        'value': data,
        'name': etype,
    }

    with io.open(filename, 'a', encoding='utf8') as afile:
        if PY3:
            afile.write(json.dumps(event) + '\n')
        else:
            afile.write(unicode(json.dumps(event) + '\n'))


def dump_chunk_event(data, filename):
    return dump_event('chunk-event', data, filename)


def extract_jars(dest, archive):
    if os.path.exists(dest):
        return
    os.makedirs(dest)
    with zipfile.ZipFile(archive) as zf:
        zf.extractall(dest)


def make_bfg_from_cp(class_path, out):
    if os.path.exists(out):
        return
    class_path = ' '.join(map(lambda path: ('file:/' + path.lstrip('/')) if os.path.isabs(path) else path, class_path))
    with zipfile.ZipFile(out, 'w') as zf:
        lines = []
        while class_path:
            lines.append(class_path[:60])
            class_path = class_path[60:]
        if lines:
            zf.writestr('META-INF/MANIFEST.MF', 'Manifest-Version: 1.0\nClass-Path: \n ' + '\n '.join(lines) + ' \n\n')


def make_command_file_from_cp(class_path, out):
    if os.path.exists(out):
        return
    with open(out, 'w') as cp_file:
        cp_file.write(os.pathsep.join(class_path))


def jacoco_includes(jars):
    """
    Build the jacoco agent `includes` option from the .covcls class lists lying next to the classpath jars.
    A class list is produced only for modules selected by JAVA_COVERAGE_SOURCE_FILTER at configure time
    (see _FILTER_JAVA_COVERAGE in build/plugins/coverage.py), so instrumenting just their packages makes
    the agent as selective as clang coverage: other modules, contrib and the JDK are left untouched.
    Packages rather than classes keep the option short; a class of another module sharing the package is
    instrumented in vain and dropped by the report, which filters by the same lists exactly.
    Return an empty option when no jar carries a class list: the filter matched no module on the
    classpath, so there is nothing to instrument.
    """
    patterns = set()
    for jar in jars:
        class_list = os.path.splitext(jar)[0] + '.covcls'
        if not os.path.exists(class_list):
            continue
        with io.open(class_list, encoding='utf8') as afile:
            for line in afile:
                name = line.strip()[: -len('.class')]
                if '/' in name:
                    patterns.add(os.path.dirname(name).replace('/', '.') + '.*')
                elif name:
                    # default package: there is no prefix to match
                    patterns.add(name)
    return ':'.join(sorted(patterns))


def add_jacoco_includes(args, jars):
    for i, arg in enumerate(args):
        # ya leaves the option empty when JAVA_COVERAGE_SOURCE_FILTER is set
        if arg.startswith('-javaagent:') and arg.endswith(',includes='):
            args[i] = arg + jacoco_includes(jars)


def main():
    s = time.time()
    opts, args = parse_args()

    # unpack tests jar
    try:
        build_root = args[args.index('--build-root') + 1]
        dest = os.path.join(build_root, 'test-classes')
    except Exception:
        build_root = ''
        dest = os.path.abspath('test-classes')

    extract_jars(dest, opts.tests_jar_path)

    metrics = {
        'suite_jtest_extract_jars_(seconds)': time.time() - s,
    }

    s = time.time()
    # fix java classpath
    cp_idx = args.index('-classpath')
    if args[cp_idx + 1].startswith('@'):
        real_name = args[cp_idx + 1][1:]
        mf = os.path.join(os.path.dirname(real_name), 'fixed.bfg.jar')
        with open(real_name) as origin:
            class_path = [os.path.join(build_root, i.strip()) for i in origin]
        add_jacoco_includes(args, class_path + [opts.tests_jar_path])
        if opts.tests_jar_path in class_path:
            class_path.remove(opts.tests_jar_path)
        if opts.classpath_option_type == 'manifest':
            make_bfg_from_cp(class_path, mf)
            mf = os.pathsep.join([dest, mf])
        elif opts.classpath_option_type == 'command_file':
            mf = os.path.splitext(mf)[0] + '.txt'
            make_command_file_from_cp([dest] + class_path, mf)
            mf = "@" + mf
        elif opts.classpath_option_type == 'list':
            mf = os.pathsep.join([dest] + class_path)
        else:
            raise Exception("Unexpected classpath option type: " + opts.classpath_option_type)
        args = fix_cmd(args[: cp_idx + 1]) + [mf] + args[cp_idx + 2 :]
    else:
        add_jacoco_includes(args, args[cp_idx + 1].split(os.pathsep))
        args[cp_idx + 1] = args[cp_idx + 1].replace(opts.tests_jar_path, dest)
        args = fix_cmd(args[:cp_idx]) + args[cp_idx:]

    metrics['suite_jtest_fix_classpath_(seconds)'] = time.time() - s

    if opts.trace_file:
        dump_chunk_event({'metrics': metrics}, opts.trace_file)

    # run java cmd
    if platform.system() == 'Windows':
        sys.exit(subprocess.Popen(args).wait())
    else:
        os.execv(args[0], args)


if __name__ == '__main__':
    main()
