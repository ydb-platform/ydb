#!/usr/bin/env python3
import os
from pathlib import Path
import re
import datetime
import json
import argparse
import signal
import sys

try:
    from time import clock_gettime_ns, CLOCK_MONOTONIC

    def time_ns():

        return clock_gettime_ns(CLOCK_MONOTONIC)

except Exception:
    from time import time_ns


RE_DIGITS = re.compile(r'([0-9]+)')


def run(argv, out, err, timeout=30*60, hard_timeout=5):

    oldmask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGCHLD})
    try:
        start_time = time_ns()
        pid = os.posix_spawn(argv[0], argv, os.environ, setsigmask=oldmask, file_actions=(
            ([(os.POSIX_SPAWN_OPEN, 1, out, os.O_WRONLY | os.O_CREAT, 0o666)] if out else []) +
            ([(os.POSIX_SPAWN_OPEN, 2, err, os.O_WRONLY | os.O_CREAT, 0o666)] if err else [])
            ))
        assert pid > 0
        try:
            procio = open('/proc/{}/io'.format(pid))
        except Exception:
            pass
        if timeout is not None:
            siginfo = signal.sigtimedwait({signal.SIGCHLD}, timeout)
            if siginfo is None:
                os.kill(pid, signal.SIGTERM)
                siginfo = signal.sigtimedwait({signal.SIGCHLD}, hard_timeout)
                if siginfo is None:
                    os.kill(pid, signal.SIGKILL)
        iostat = {}
        try:
            for line in procio:
                (key, value) = line.strip().split(': ')
                value = int(value)
                iostat[key] = value
        except Exception as ex:
            iostat = None
            print(ex, sys.stderr)
        finally:
            if procio:
                procio.close()
        (pid, status, rusage) = os.wait4(pid, 0)
        elapsed = time_ns()
        elapsed -= start_time
        exitcode = os.waitstatus_to_exitcode(status)
        return exitcode, rusage, elapsed, iostat
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, oldmask)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query-dir', type=str, default='q/scalar')
    parser.add_argument('--bindings', type=str, default='bindings.json')
    parser.add_argument('--result-dir', type=Path, default="result-{:%Y%m%dT%H%M%S}".format(datetime.datetime.now()))
    parser.add_argument('--timeout', type=int, default=30*60)
    parser.add_argument('--perf', action='store_true')
    parser.add_argument('--arc-path', type=str, default='{}/arcadia'.format(os.environ['HOME']))
    parser.add_argument('--include-q', default=[], action='append')
    parser.add_argument('--exclude-q', default=[], action='append')
    parser.add_argument('--query-filter', action="append", default=[])
            
    args, argv = parser.parse_known_intermixed_args()
    qdir = args.query_dir
    bindings = args.bindings
    outdir = args.result_dir
    assert len(argv)
    querydir = Path(qdir)
    with open(outdir / "summary.tsv", "w") as outf, \
         open(outdir / "summary.json", "w") as outj:
        print(' '.join(argv + ['-p', qdir, '--bindings-file', bindings]), file=outf)
        print(json.dumps({
            'cmdline': argv,
            'query_dir': qdir,
            'bindings_file': bindings,
            'version': 100
        }), file=outj)
        for query in sorted(querydir.glob('**/*.sql'), key=lambda x: tuple(map(lambda y: int(y) if re.match(RE_DIGITS, y) else y, re.split(RE_DIGITS, str(x))))):
            q = str(query.stem)
            # q<num>.sql
            num = q[1:-4]
            if args.query_filter != [] and num not in args.query_filter:
                continue
            print(f"{q}", end="", flush=True)
            name = str(outdir / q)
            if len(args.include_q):
                include = False
                for r in args.include_q:
                    if re.search(r, name):
                        include = True
                        break
                if not include:
                    continue
            if len(args.exclude_q):
                include = True
                for r in args.exclude_q:
                    if re.search(r, name):
                        include = False
                        break
                if not include:
                    continue
            print(q, end='\t', file=outf)
            outname = name + '-result.yson'
            print(".", end="", flush=True)
            exitcode, rusage, elapsed, iostat = run(
                argv + [
                    '--result-file', outname,
                    '--bindings-file', bindings,
                    '--plan-file', name + '-plan.yson',
                    '--err-file', name + '-err.txt',
                    '--expr-file', name + '-expr.txt',
                    '--stat', name + '-stat.yson',
                    '-p', str(query)
                ],
                name + '-stdout.txt',
                name + '-stderr.txt',
                timeout=args.timeout)
            print(rusage.ru_utime, end='\t', file=outf)
            print(rusage.ru_stime, end='\t', file=outf)
            print(rusage.ru_maxrss, end='\t', file=outf)
            print(exitcode, end='\t', file=outf)
            print(elapsed, end='\t', file=outf)
            print(rusage.ru_minflt, end='\t', file=outf)
            print(rusage.ru_majflt, end='\t', file=outf)
            print(rusage.ru_inblock, end='\t', file=outf)
            print(rusage.ru_oublock, end='\t', file=outf)
            print(rusage.ru_nvcsw, end='\t', file=outf)
            print(rusage.ru_nivcsw, end='\t', file=outf)
            print(iostat['rchar'] if iostat and 'rchar' in iostat else -1, end='\t', file=outf)
            print(iostat['wchar'] if iostat and 'wchar' in iostat else -1, end='\t', file=outf)
            # resource.struct_rusage(ru_utime=7.919329, ru_stime=5.22704,
            #   ru_maxrss=639600, ru_ixrss=0, ru_idrss=0, ru_isrss=0,
            #   ru_minflt=135127, ru_majflt=0, ru_nswap=0, ru_inblock=0,
            #   ru_oublock=48, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0,
            #   ru_nvcsw=57452, ru_nivcsw=273
            # )
            print(file=outf)
            outf.flush()
            print(json.dumps({
                'q': q, 'exitcode': exitcode,
                'elapsed': elapsed,
                'io': iostat,
                'rusage': {
                    'utime': rusage.ru_utime,
                    'stime': rusage.ru_stime,
                    'maxrss': rusage.ru_maxrss,
                    'minflt': rusage.ru_minflt,
                    'majflt': rusage.ru_majflt,
                    'inblock': rusage.ru_inblock,
                    'oublock': rusage.ru_oublock,
                    'nvcsw': rusage.ru_nvcsw,
                    'nivcsw': rusage.ru_nivcsw,
                    'nswap': rusage.ru_nswap,
                }
            }), file=outj)
            outj.flush()
            print(".", end="", flush=True)
            if args.perf:
                exitcode, rusage, elapsed, iostat = run(
                    ['/usr/bin/perf', 'record', '-F250', '-g', '--call-graph', 'dwarf', '-o', '{}/perf.data'.format(outdir), '--'] +
                    argv + [
                        '--result-file', '/dev/null',
                        '--bindings-file', bindings,
                        '--plan-file', '/dev/null',
                        '--err-file', '/dev/null',
                        '--expr-file', '/dev/null',
                        '-p', str(query)
                    ],
                    name + '-stdout-perf.txt',
                    name + '-stderr-perf.txt',
                    timeout=args.timeout)
                os.system('''
                /usr/bin/perf script -i {2}/perf.data --header |
                {0}/contrib/tools/flame-graph/stackcollapse-perf.pl |
                {0}/contrib/tools/flame-graph/flamegraph.pl > {1}.svg
                '''.format(args.arc_path, name, outdir))
            print(".", flush=True)


if __name__ == "__main__":
    main()
