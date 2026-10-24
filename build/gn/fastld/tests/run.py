#!/usr/bin/env python3
"""Loader semantics micro-suite: native glibc ld.so vs fastld.

Builds each case with the gn toolchain flags (clang 20, lld, ya sysroot,
-nodefaultlibs, -z now, RUNPATH $ORIGIN, build libcxx.so for C++), runs every
program natively and, with --fastld, as `fastld <exe> args`, and compares
exit code, stdout and stderr.

    run.py [OUT_DIR] [--fastld PATH] [-k SUBSTR] [--no-build] [-j N] [-v]
"""
import argparse
import difflib
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..', '..'))
TOOLS = '/home/ilezhankin/.ya/tools/v4/13899442575/bin'
SYSROOT = '/home/ilezhankin/.ya/tools/v4/243881345'
LIBCXX_SO = os.path.expanduser('~/b/gn/libcxx.so')
SCRATCH = '/place/vartmp/claude-25341/-home-ilezhankin-g-ydb-gn/68912edb-cd26-452f-b8f7-b5c5efe0a8f7/scratchpad'
DEFAULT_OUT = SCRATCH + '/fastld_tests_out'

CFLAGS = ['--sysroot=' + SYSROOT, '-D_GNU_SOURCE', '-fPIC', '-pthread', '-g', '-Wall', '-Werror',
          '-Wno-unused-function']
CXXFLAGS = ['-nostdinc++', '-I' + REPO + '/contrib/libs/cxxsupp/libcxx/include',
            '-I' + REPO + '/contrib/libs/cxxsupp/libcxxrt/include', '-DLIBCXXRT', '-std=c++20',
            '-fvisibility-inlines-hidden']
LDFLAGS = ['-fuse-ld=' + TOOLS + '/ld.lld', '-nodefaultlibs', '-Wl,-z,now', '-Wl,-z,relro',
           '-Wl,--hash-style=gnu', '-Wl,--no-undefined', '-Wl,-rpath,$ORIGIN']
BSYM = '-Wl,-Bsymbolic-non-weak-functions'  # the real build links every .so with it
SYSLIBS = ['-lc', '-lm', '-ldl', '-lpthread', '-lrt']


class Lib:
    def __init__(self, name, srcs, deps=(), cflags=(), ldflags=(), bsym=True, subdir=''):
        self.name, self.srcs, self.deps = name, list(srcs), list(deps)
        self.cflags, self.ldflags, self.bsym, self.subdir = list(cflags), list(ldflags), bsym, subdir


class Exe:
    def __init__(self, name, srcs, deps=(), cflags=()):
        self.name, self.srcs, self.deps, self.cflags = name, list(srcs), list(deps), list(cflags)


class Run:
    def __init__(self, exe, args=(), rc=0, stderr_re=None):
        # stderr_re: stderr must match (search) in both runs; otherwise stderr must be equal.
        self.exe, self.args, self.rc, self.stderr_re = exe, list(args), rc, stderr_re


class Case:
    def __init__(self, name, lang, libs, exes, runs, opt='-O2', cflags=(), post=None):
        self.name, self.lang, self.libs, self.exes, self.runs = name, lang, libs, exes, runs
        self.opt, self.cflags, self.post = opt, list(cflags), post


def install_undef_v2(out):
    shutil.copy2(os.path.join(out, 'v2', 'libu_lib.so'), os.path.join(out, 'libu_lib.so'))


def init_lib(n, deps, extra_ld=()):
    return Lib('i_' + n, ['lib.c'], deps, ['-DNAME="%s"' % n, '-DSYM=reg_' + n] +
               (['-DWITH_DT_INIT'] if extra_ld else []), extra_ld)


CASES = [
    # BFS scope order, first definition wins (weak or strong), exe data interposes, dedup,
    # host libc at its BFS position, undefined weak -> 0.
    Case('scope', 'c', [
        Lib('s_d', ['s_d.c'], bsym=False),
        Lib('s_c', ['s_c.c'], ['s_d'], bsym=False),
        Lib('s_b', ['s_b.c'], ['s_d'], bsym=False),
        Lib('s_a', ['s_a.c'], ['s_c'], bsym=False),
    ], [Exe('exe', ['exe.c'], ['s_a', 's_b'])], [Run('exe')], opt='-O0'),
    # Versioned references into host libc/libpthread; IFUNC; unversioned defs satisfy versioned refs.
    Case('versions', 'c', [
        Lib('v_def', ['v_def.c']),
        Lib('v_lib', ['v_lib.c']),
    ], [Exe('exe', ['exe.c'], ['v_def', 'v_lib'])], [Run('exe')]),
    # TLS: exe local-exec, exe -> .so TLS (initial-exec, TPOFF64), .so GD/LD, cross-.so,
    # initial-exec TLS in a .so (STATIC_TLS), thread_local dtors, std::thread / pthread_create, dlopen'ed TLS, concurrency.
    Case('tls', 'c++', [
        Lib('tls_a', ['tls_a.cpp']),
        Lib('tls_b', ['tls_b.cpp'], ['tls_a']),
        Lib('tls_plugin', ['tls_plugin.cpp']),
        Lib('tls_ie', ['tls_ie.cpp']),
    ], [Exe('exe', ['exe.cpp'], ['tls_a', 'tls_b', 'tls_ie'])], [Run('exe')]),
    # Dynamic TLS of a .so read by its pthread key destructor at the thread exit, in several rounds.
    Case('tlsexit', 'c', [Lib('te_lib', ['lib.c'])],
         [Exe('exe', ['exe.c'], ['te_lib'])], [Run('exe')]),
    # Init/fini order, ctor args, preinit_array, DT_INIT/DT_FINI, atexit from ctors and from main.
    Case('init', 'c', [
        init_lib('c', [], ['-Wl,-init=dt_init', '-Wl,-fini=dt_fini']),
        init_lib('b', ['i_c']),
        init_lib('a', ['i_b', 'i_c']),
        init_lib('d', ['i_c']),
    ], [Exe('exe', ['exe.c'], ['i_a', 'i_d'])], [Run('exe', ['arg1', 'arg2'])]),
    # malloc family interposed by the exe (-rdynamic) and, separately, by the first NEEDED .so.
    Case('malloc', 'c', [
        Lib('m_alloc', ['alloc.c']),
        Lib('m_lib', ['m_lib.c']),
    ], [
        Exe('exe', ['exe.c', 'alloc.c'], ['m_lib']),
        Exe('exe_so', ['exe.c'], ['m_alloc', 'm_lib']),
    ], [Run('exe'), Run('exe_so')]),
    # dlsym RTLD_DEFAULT/RTLD_NEXT/handles, dlopen (loaded, plugin, from a lib, GLOBAL), dladdr,
    # dl_iterate_phdr, dlclose.
    Case('dl', 'c', [
        Lib('dl_a', ['dl_a.c']),
        Lib('dl_b', ['dl_b.c']),
        Lib('dl_c', ['dl_c.c']),
        Lib('dl_plugin', ['dl_plugin.c'], ['dl_c', 'dl_a']),
        Lib('dl_plugin2', ['dl_plugin2.c']),
    ], [Exe('exe', ['exe.c'], ['dl_a', 'dl_b'])], [Run('exe')],
        # RTLD_NEXT keys on the return address: a tail call to dlsym would make it the caller's caller.
        cflags=['-fno-optimize-sibling-calls']),
    # C++ exceptions across objects (incl. thrown inside libcxx.so and during static init),
    # vague-linkage type_info uniqueness, dynamic_cast.
    Case('exc', 'c++', [
        Lib('ex_base', ['ex_base.cpp']),
        Lib('ex_thrower', ['ex_thrower.cpp'], ['ex_base']),
        Lib('ex_catcher', ['ex_catcher.cpp'], ['ex_thrower', 'ex_base']),
    ], [Exe('exe', ['exe.cpp'], ['ex_catcher', 'ex_thrower', 'ex_base'])], [Run('exe')]),
    # Same symbols in several libs, with and without -Bsymbolic-non-weak-functions; protected.
    Case('interpose', 'c', [
        Lib('ip_a', ['ip_a.c'], bsym=False),
        Lib('ip_b', ['ip_b.c'], cflags=['-DPFX=b', '-DVAL="B"'], bsym=False),
        Lib('ip_bs', ['ip_b.c'], cflags=['-DPFX=bs', '-DVAL="BS"'], bsym=True),
    ], [Exe('exe', ['exe.c'], ['ip_a', 'ip_b', 'ip_bs'])], [Run('exe')], opt='-O0'),
    # Strong reference to a symbol missing at run time (BIND_NOW): error exit; weak one: NULL.
    Case('undef', 'c', [
        Lib('u_lib', ['u_lib.c'], cflags=['-DV1']),
        Lib('u_lib', ['u_lib.c'], subdir='v2'),
        Lib('u_mid', ['u_mid.c'], ['u_lib']),
        Lib('u_weak', ['u_weak.c'], ['u_lib']),
    ], [
        Exe('exe_strong', ['exe_strong.c'], ['u_lib']),
        Exe('exe_mid', ['exe_mid.c'], ['u_mid', 'u_lib']),
        Exe('exe_weak', ['exe_weak.c'], ['u_weak', 'u_lib']),
    ], [
        Run('exe_strong', rc=127, stderr_re=r'symbol lookup error: .*undefined symbol: gone'),
        Run('exe_mid', rc=127, stderr_re=r'symbol lookup error: .*libu_mid\.so: undefined symbol: gone'),
        Run('exe_weak'),
    ], post=install_undef_v2),
    # argv/envp/auxv//proc/self as seen by the program, exit code and atexit.
    Case('process', 'c', [], [Exe('exe', ['exe.c'])], [Run('exe', ['x', 'y'], rc=3)]),
]


def sh(cmd, cwd):
    p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    if p.returncode:
        raise RuntimeError('build failed in %s:\n%s\n%s' % (cwd, ' '.join(cmd), p.stdout))


def build_case(case, out_root):
    src = os.path.join(HERE, case.name)
    out = os.path.join(out_root, case.name)
    os.makedirs(out, exist_ok=True)
    cxx = case.lang == 'c++'
    cc = [TOOLS + ('/clang++' if cxx else '/clang')] + CFLAGS + (CXXFLAGS if cxx else []) + [case.opt] + case.cflags
    tail = []
    if cxx:
        link = os.path.join(out, 'libcxx.so')
        if os.path.lexists(link) and not os.path.exists(link):
            os.unlink(link)
        if not os.path.lexists(link):
            os.symlink(LIBCXX_SO, link)
        tail.append(link)
    tail += SYSLIBS

    def deps(names):
        return [os.path.join(out, 'lib%s.so' % d) for d in names]

    for lib in case.libs:
        d = os.path.join(out, lib.subdir)
        os.makedirs(d, exist_ok=True)
        so = 'lib%s.so' % lib.name
        sh(cc + lib.cflags + [os.path.join(src, s) for s in lib.srcs] + ['-shared'] + LDFLAGS +
           ([BSYM] if lib.bsym else []) + lib.ldflags + ['-Wl,-soname=' + so, '-o', os.path.join(d, so)] +
           deps(lib.deps) + tail, out)
    for exe in case.exes:
        sh(cc + exe.cflags + [os.path.join(src, s) for s in exe.srcs] + ['-pie', '-rdynamic'] + LDFLAGS +
           [BSYM, '-o', os.path.join(out, exe.name)] + deps(exe.deps) + tail, out)
    if case.post:
        case.post(out)


def run_one(cmd, cwd):
    env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'HOME': os.environ.get('HOME', '/'), 'FASTLD_TEST_ENV': '1'}
    try:
        p = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
        return p.returncode, p.stdout.decode(errors='replace'), p.stderr.decode(errors='replace')
    except subprocess.TimeoutExpired:
        return 'timeout', '', ''


def check_stderr(r, ref, run):
    if run.stderr_re:
        return re.search(run.stderr_re, r[2]) is not None
    return r[2] == ref[2]


def run_case(case, out_root, fastld):
    out = os.path.join(out_root, case.name)
    results = []
    for run in case.runs:
        exe = os.path.join(out, run.exe)
        n1, n2 = run_one([exe] + run.args, out), run_one([exe] + run.args, out)
        problems = []
        if n1[0] != run.rc:
            problems.append('rc=%s' % (n1[0],))
        if not check_stderr(n1, ('', '', ''), run):
            problems.append('stderr')
        if n1[1] != n2[1] or n1[0] != n2[0]:
            problems.append('nondeterministic')
        native = 'ok' if not problems else 'FAIL(%s)' % ','.join(problems)
        f = None
        fl = '-'
        if fastld:
            f = run_one([fastld, exe] + run.args, out)
            diffs = []
            if f[0] != n1[0]:
                diffs.append('rc %s!=%s' % (f[0], n1[0]))
            if f[1] != n1[1]:
                diffs.append('stdout')
            if not check_stderr(f, n1, run):
                diffs.append('stderr')
            fl = 'ok' if not diffs else 'DIFF(%s)' % ', '.join(diffs)
        name = '%s/%s' % (case.name, run.exe)
        for tag, r in (('native', n1), ('fastld', f)):
            if r is not None:
                with open(os.path.join(out, run.exe + '.' + tag + '.txt'), 'w') as fh:
                    fh.write('rc: %s\n--- stdout\n%s--- stderr\n%s' % r)
        results.append((name, native, fl, n1, f))
    return results


def show(lines, limit=60):
    for l in lines[:limit]:
        print('    ' + l.rstrip('\n'))
    if len(lines) > limit:
        print('    ... (%d more)' % (len(lines) - limit))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('out', nargs='?', default=DEFAULT_OUT)
    ap.add_argument('--fastld')
    ap.add_argument('-k', help='only cases whose name contains this')
    ap.add_argument('--no-build', action='store_true')
    ap.add_argument('-j', type=int, default=8)
    ap.add_argument('-v', action='store_true', help='print outputs of passing runs too')
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    fastld = os.path.abspath(a.fastld) if a.fastld else None
    cases = [c for c in CASES if not a.k or a.k in c.name]
    jobs = max(1, min(a.j, 8))
    with ThreadPoolExecutor(jobs) as pool:
        if not a.no_build:
            for c, fut in [(c, pool.submit(build_case, c, out)) for c in cases]:
                fut.result()
        results = [r for fut in [pool.submit(run_case, c, out, fastld) for c in cases] for r in fut.result()]
    print('%-22s %-24s %s' % ('case', 'native', 'fastld'))
    bad = False
    for name, native, fl, n, f in results:
        print('%-22s %-24s %s' % (name, native, fl))
        bad |= native != 'ok' or fl not in ('ok', '-')
    for name, native, fl, n, f in results:
        if native != 'ok' or a.v:
            print('\n== %s native: rc=%s' % (name, n[0]))
            show(n[1].splitlines(True))
            if n[2]:
                print('  stderr:')
                show(n[2].splitlines(True), 20)
        if f is not None and fl != 'ok':
            print('\n== %s fastld: rc=%s (native rc=%s)' % (name, f[0], n[0]))
            show(list(difflib.unified_diff(n[1].splitlines(True), f[1].splitlines(True), 'native', 'fastld', n=1)))
            if f[2] != n[2]:
                print('  fastld stderr:')
                show(f[2].splitlines(True), 20)
    print('\noutputs: %s/<case>/<exe>.{native,fastld}.txt' % out)
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
