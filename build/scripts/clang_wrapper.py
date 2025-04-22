import subprocess
import sys


def fix(s):
    if s == '/Z7' or s == '/Od' or s == '/Ob0' or s == '/D_DEBUG':
        return None

    # disable sanitizers for generated code
    if s.startswith('-fsanitize') or s == '-Dmemory_sanitizer_enabled' or s.startswith('-fsanitize-blacklist'):
        return None

    # strip gcc toolchain flags (appear when crosscompiling)
    if s.startswith('-fabi-version'):
        return None

    # remove arguments unknown to clang-cl
    if s == '-fcase-insensitive-paths':  # or s == '-fno-lto':  # DEVTOOLSSUPPORT-3966
        return None

    # Paths under .ya/tools/v3/.../msvc/include are divided with '\'
    return s.replace('\\', '/')


def fix_path(p):
    try:
        i = p.rfind('/bin/clang')
        p = p[:i] + '/bin/clang-cl'
    except ValueError:
        pass
    return p


def drop_stl(s):
    if s.startswith('-I') and 'contrib/libs/cxxsupp/libcxx/include' in s:
        return None

    return s


if __name__ == '__main__':
    is_on_win = sys.argv[1] == 'yes'
    path = sys.argv[2]
    args = list(filter(None, [fix(s) for s in sys.argv[3:]]))
    if is_on_win:
        path = fix_path(path)
        try:
            i = args.index('-emit-llvm')
            args[i : i + 1] = ['-Xclang', '-emit-llvm']
        except ValueError:
            pass
        args.append('-fms-compatibility-version=19')

    for i in range(len(args) - 1):
        if args[i] == '-target' and args[i + 1] == 'bpf':
            # bpf should not be able to include stl headers
            args = list(filter(None, [drop_stl(s) for s in args]))
            break

    cmd = [path] + args

    rc = subprocess.call(cmd, shell=False, stderr=sys.stderr, stdout=sys.stdout)
    sys.exit(rc)
