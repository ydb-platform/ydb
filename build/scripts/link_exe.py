import itertools
import os
import os.path
import sys
import json
import subprocess
import optparse
import shlex
import textwrap

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf
import thinlto_cache

from process_whole_archive_option import ProcessWholeArchiveOption


def remove_excessive_flags(cmd):
    flags = []
    for flag in cmd:
        if not flag.endswith('.ios.interface') and not flag.endswith('.pkg.fake'):
            flags.append(flag)
    return flags


def remove_libs(cmd, libs):
    excluded_flags = ['-l{}'.format(lib) for lib in libs]

    flags = []

    for flag in cmd:
        if flag in excluded_flags:
            continue

        flags.append(flag)

    return flags


def with_lld_threads(cmd, arch, threads):
    """Apply runtime parallelism only to ELF LLD, preserving the graph command."""
    if threads is None or arch not in ('LINUX', 'ANDROID', 'FREEBSD'):
        return cmd

    # Inspect response files too: the driver can select a different linker there.
    args = []
    for arg in cmd:
        if pcf.is_cmdfile_arg(arg):
            content = '\n'.join(pcf.read_from_command_file(pcf.cmdfile_path(arg)))
            args.extend(shlex.split(content))
        else:
            args.append(arg)
    if any(arg.startswith('@') for arg in args):
        return cmd

    linker_path = None
    linker = None
    for index, arg in enumerate(args):
        if arg.startswith('--ld-path='):
            linker_path = arg[len('--ld-path=') :]
        elif arg == '--ld-path' and index + 1 < len(args):
            linker_path = args[index + 1]
        elif arg.startswith('-fuse-ld='):
            linker = arg[len('-fuse-ld=') :]
        elif arg == '-fuse-ld' and index + 1 < len(args):
            linker = args[index + 1]

    if linker_path is not None:
        if os.path.basename(linker_path) != 'ld.lld':
            return cmd
    elif linker != 'lld':
        return cmd

    threads = int(threads)
    if threads <= 0:
        raise ValueError('_YA_LLD_THREADS requires a positive integer')
    return cmd + ['-Wl,--threads={}'.format(threads)]


def parse_args(args):
    parser = optparse.OptionParser()
    parser.disable_interspersed_args()
    parser.add_option('--custom-step')
    parser.add_option('--python')
    parser.add_option('--source-root')
    parser.add_option('--build-root')
    parser.add_option('--clang-ver')
    parser.add_option('--dynamic-cuda', action='store_true')
    parser.add_option('--objcopy-exe')
    parser.add_option('--arch')
    parser.add_option('--linker-output')
    parser.add_option('--whole-archive-peers', action='append')
    parser.add_option('--whole-archive-libs', action='append')
    parser.add_option('--exclude-libs', action='append')
    thinlto_cache.add_options(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = sys.argv[1:]
    plugins = []

    if '--start-plugins' in args:
        ib = args.index('--start-plugins')
        ie = args.index('--end-plugins')
        plugins = list(sorted(args[ib + 1 : ie]))
        args = args[:ib] + args[ie + 1 :]

    for p in plugins:
        res = subprocess.check_output([sys.executable, p, sys.argv[0]] + args).decode().strip()

        if res:
            args = json.loads(res)[1:]

    # inputs meant for the plugins only (see LD_PLUGIN), never for the linker
    if '--start-plugin-inputs' in args:
        ib = args.index('--start-plugin-inputs')
        ie = args.index('--end-plugin-inputs')
        args = args[:ib] + args[ie + 1 :]

    opts, args = parse_args(args)
    args = pcf.skip_markers(args)

    cmd = args
    cmd = remove_excessive_flags(cmd)

    if opts.exclude_libs:
        cmd = remove_libs(cmd, opts.exclude_libs)

    cmd = ProcessWholeArchiveOption(opts.arch, opts.whole_archive_peers, opts.whole_archive_libs).construct_cmd(cmd)

    if opts.custom_step:
        assert opts.python
        subprocess.check_call([opts.python] + [opts.custom_step] + args)

    if opts.linker_output:
        stdout = open(opts.linker_output, 'w')
    else:
        stdout = sys.stdout

    thinlto_cache.preprocess(opts, cmd)
    cmd = with_lld_threads(cmd, opts.arch, os.environ.get('_YA_LLD_THREADS'))
    # Temporary diagnostic for the ydbd linker comparison PRs.
    expanded = pcf.get_args(cmd)
    output = expanded[expanded.index('-o') + 1] if '-o' in expanded else ''
    if opts.arch == 'LINUX' and ('/' + output).endswith('/ydb/apps/ydbd/ydbd'):
        import time

        started = time.time()
        clock_started = time.monotonic()
        with open(output + '.linker.log', 'w') as linker_log:
            proc = subprocess.Popen(cmd, stderr=linker_log, stdout=linker_log)
            _, status, usage = os.wait4(proc.pid, 0)
        rc = os.waitstatus_to_exitcode(status)
        proc.returncode = rc
        metrics = {
            'started': started,
            'wall_seconds': time.monotonic() - clock_started,
            'user_seconds': usage.ru_utime,
            'system_seconds': usage.ru_stime,
            'max_rss_kib': usage.ru_maxrss,
            'read_bytes': usage.ru_inblock * 512,
            'write_bytes': usage.ru_oublock * 512,
            'minor_faults': usage.ru_minflt,
            'major_faults': usage.ru_majflt,
            'voluntary_context_switches': usage.ru_nvcsw,
            'involuntary_context_switches': usage.ru_nivcsw,
            'exit_code': rc,
            'command': expanded,
        }
        with open(output + '.link-stats.json', 'w') as report:
            json.dump(metrics, report, indent=2)
        with open(output + '.linker.log') as linker_log:
            sys.stderr.write(linker_log.read())
    else:
        rc = subprocess.call(cmd, shell=False, stderr=sys.stderr, stdout=stdout)
    thinlto_cache.postprocess(opts)

    sys.exit(rc)
