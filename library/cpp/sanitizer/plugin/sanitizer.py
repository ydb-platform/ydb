import collections
import os
import sys
import json


def get_leaks_suppressions(cmd):
    supp, newcmd = [], []
    for arg in cmd:
        if arg.endswith(".supp"):
            supp.append(arg)
        else:
            newcmd.append(arg)
    return supp, newcmd


def fix_sanitize_flag(cmd, clang_ver):
    """
    Remove -fsanitize=address flag if sanitazers are linked explicitly for linux target.
    """
    for flag in cmd:
        if flag.startswith('--target') and 'linux' not in flag.lower():
            # use toolchained sanitize libraries
            return cmd
    CLANG_RT = 'contrib/libs/clang' + clang_ver + '-rt/lib/'
    sanitize_flags = {
        '-fsanitize=address': CLANG_RT + 'asan',
        '-fsanitize=memory': CLANG_RT + 'msan',
        '-fsanitize=leak': CLANG_RT + 'lsan',
        '-fsanitize=undefined': CLANG_RT + 'ubsan',
        '-fsanitize=thread': CLANG_RT + 'tsan',
    }

    used_sanitize_libs = []
    aux = []
    for flag in cmd:
        if flag.startswith('-fsanitize-coverage='):
            # do not link sanitizer libraries from clang
            aux.append('-fno-sanitize-link-runtime')
        if flag in sanitize_flags and any(s.startswith(sanitize_flags[flag]) for s in cmd):
            # exclude '-fsanitize=' if appropriate library is linked explicitly
            continue
        if any(flag.startswith(lib) for lib in sanitize_flags.values()):
            used_sanitize_libs.append(flag)
            continue
        aux.append(flag)

    # move sanitize libraries out of the repeatedly searched group of archives
    flags = []
    for flag in aux:
        if flag == '-Wl,--start-group':
            flags += ['-Wl,--whole-archive'] + used_sanitize_libs + ['-Wl,--no-whole-archive']
        flags.append(flag)

    return flags


def gen_default_suppressions(inputs, output, source_root):
    supp_map = collections.defaultdict(set)
    for filename in inputs:
        sanitizer = os.path.basename(filename).split('.', 1)[0]
        with open(os.path.join(source_root, filename)) as src:
            for line in src:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                supp_map[sanitizer].add(line)

    with open(output, "wt") as dst:
        for supp_type, supps in supp_map.items():
            dst.write('extern "C" const char *__%s_default_suppressions() {\n' % supp_type)
            dst.write('    return "{}";\n'.format('\\n'.join(sorted(supps))))
            dst.write('}\n')


def sanitize(cmd):
    clang_ver = cmd[cmd.index('--clang-ver') + 1]
    source_root = cmd[cmd.index('--source-root') + 1]
    cmd = fix_sanitize_flag(cmd, clang_ver)
    supp, cmd = get_leaks_suppressions(cmd)
    if supp:
        src_file = "default_suppressions.cpp"
        gen_default_suppressions(supp, src_file, source_root)
        cmd += [src_file]
    return cmd


if __name__ == '__main__':
    cmd = sys.argv[1:]

    if 'link_exe.py' in str(cmd):
        cmd = sanitize(cmd)
    else:
        cmd = [s for s in cmd if not s.endswith('.supp')]

    sys.stdout.write(json.dumps(cmd))
