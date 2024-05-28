#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import re
import datetime

try:
    from time import clock_gettime_ns, CLOCK_MONOTONIC

    def time_ns():

        return clock_gettime_ns(CLOCK_MONOTONIC)

except Exception:
    from time import time_ns


RE_DIGITS = re.compile(r'([0-9]+)')


def run(argv, out, err):

    start_time = time_ns()
    pid = os.posix_spawn(argv[0], argv, {}, file_actions=(
        (os.POSIX_SPAWN_OPEN, 1, out, os.O_WRONLY | os.O_CREAT, 0o666),
        (os.POSIX_SPAWN_OPEN, 2, err, os.O_WRONLY | os.O_CREAT, 0o666),
        ))
    (pid, status, rusage) = os.wait4(pid, 0)
    elapsed = time_ns()
    elapsed -= start_time
    exitcode = os.waitstatus_to_exitcode(status)
    return exitcode, rusage, elapsed


def main():

    qdir = sys.argv[1] or 'q/scalar'
    bindings = sys.argv[2] or 'bindings.json'
    outdir = sys.argv[3] or "result-{:%Y%m%dT%H%M%S}".format(datetime.datetime.now())
    argv = sys.argv[4:]
    assert len(argv)
    querydir = Path(qdir)
    os.makedirs(outdir + '/' + qdir, exist_ok=True)
    with open(outdir + '/' + qdir + "/summary.tsv", "w") as outf:
        print(' '.join(argv + ['-p', qdir, '--bindings-file', bindings]), file=outf)
        for query in sorted(querydir.glob('**/*.sql'), key=lambda x: tuple(map(lambda y: int(y) if re.match(RE_DIGITS, y) else y, re.split(RE_DIGITS, str(x))))):
            q = str(query)
            print(q, end='\t', file=outf)
            name = outdir + '/' + q
            outname = name + '-result.yson'
            exitcode, rusage, elapsed = run(
                argv + [
                    '--result-file', outname,
                    '--bindings-file', bindings,
                    '--plan-file', name + '-plan.yson',
                    '--err-file', name + '-err.txt',
                    '--expr-file', name + '-expr.txt',
                    '-p', q
                ],
                name + '-stdout.txt',
                name + '-stderr.txt')
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
            # resource.struct_rusage(ru_utime=7.919329, ru_stime=5.22704,
            #   ru_maxrss=639600, ru_ixrss=0, ru_idrss=0, ru_isrss=0,
            #   ru_minflt=135127, ru_majflt=0, ru_nswap=0, ru_inblock=0,
            #   ru_oublock=48, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0,
            #   ru_nvcsw=57452, ru_nivcsw=273
            # )
            print(file=outf)
            outf.flush()


if __name__ == "__main__":
    main()
