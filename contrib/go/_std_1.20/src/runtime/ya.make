GO_LIBRARY()

SRCS(
    alg.go
    arena.go
    asan0.go
    asm.s
    asm_ppc64x.h
    atomic_pointer.go
    cgo.go
    cgocall.go
    cgocallback.go
    cgocheck.go
    chan.go
    checkptr.go
    compiler.go
    complex.go
    covercounter.go
    covermeta.go
    cpuflags.go
    cpuprof.go
    debug.go
    debugcall.go
    debuglog.go
    debuglog_off.go
    env_posix.go
    error.go
    exithook.go
    extern.go
    fastlog2.go
    fastlog2table.go
    float.go
    funcdata.h
    go_tls.h
    hash64.go
    heapdump.go
    histogram.go
    iface.go
    lfstack.go
    lfstack_64bit.go
    lockrank.go
    lockrank_off.go
    malloc.go
    map.go
    map_fast32.go
    map_fast64.go
    map_faststr.go
    mbarrier.go
    mbitmap.go
    mcache.go
    mcentral.go
    mcheckmark.go
    mem.go
    metrics.go
    mfinal.go
    mfixalloc.go
    mgc.go
    mgclimit.go
    mgcmark.go
    mgcpacer.go
    mgcscavenge.go
    mgcstack.go
    mgcsweep.go
    mgcwork.go
    mheap.go
    mpagealloc.go
    mpagealloc_64bit.go
    mpagecache.go
    mpallocbits.go
    mprof.go
    mranges.go
    msan0.go
    msize.go
    mspanset.go
    mstats.go
    mwbbuf.go
    netpoll.go
    os_nonopenbsd.go
    pagetrace_off.go
    panic.go
    plugin.go
    preempt.go
    print.go
    proc.go
    profbuf.go
    proflabel.go
    rdebug.go
    runtime.go
    runtime1.go
    runtime2.go
    runtime_boring.go
    rwmutex.go
    select.go
    sema.go
    sigqueue.go
    sizeclasses.go
    slice.go
    softfloat64.go
    stack.go
    stkframe.go
    string.go
    stubs.go
    symtab.go
    sys_nonppc64x.go
    textflag.h
    time.go
    time_nofake.go
    trace.go
    traceback.go
    type.go
    typekind.go
    unsafe.go
    utf8.go
    write_err.go
)

IF (ARCH_ARM64)
    SRCS(
        asm_arm64.s
        atomic_arm64.s
        cpuflags_arm64.go
        duff_arm64.s
        memclr_arm64.s
        memmove_arm64.s
        preempt_arm64.s
        stubs_arm64.go
        sys_arm64.go
        tls_arm64.h
        tls_arm64.s
        tls_stub.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        asm_amd64.h
        asm_amd64.s
        cpuflags_amd64.go
        cputicks.go
        duff_amd64.s
        memclr_amd64.s
        memmove_amd64.s
        preempt_amd64.s
        stubs_amd64.go
        sys_x86.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        create_file_unix.go
        lock_sema.go
        mem_darwin.go
        nbpipe_pipe.go
        netpoll_kqueue.go
        os_darwin.go
        os_unix_nonlinux.go
        preempt_nonwindows.go
        relax_stub.go
        retry.go
        security_issetugid.go
        security_unix.go
        signal_darwin.go
        signal_unix.go
        stubs_nonlinux.go
        sys_darwin.go
        sys_libc.go
        timestub.go
        vdso_in_none.go
    )

    IF (ARCH_ARM64)
        SRCS(
            defs_darwin_arm64.go
            os_darwin_arm64.go
            rt0_darwin_arm64.s
            signal_arm64.go
            signal_darwin_arm64.go
            sys_darwin_arm64.go
            sys_darwin_arm64.s
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            defs_darwin_amd64.go
            rt0_darwin_amd64.s
            signal_amd64.go
            signal_darwin_amd64.go
            sys_darwin_amd64.s
            tls_stub.go
        )
    ENDIF()
ENDIF()

IF (OS_LINUX)
    SRCS(
        cgo_mmap.go
        cgo_sigaction.go
        create_file_unix.go
        lock_futex.go
        mem_linux.go
        nbpipe_pipe2.go
        netpoll_epoll.go
        os_linux.go
        os_linux_generic.go
        preempt_nonwindows.go
        relax_stub.go
        retry.go
        security_linux.go
        security_unix.go
        signal_unix.go
        sigqueue_note.go
        sigtab_linux_generic.go
        stubs2.go
        stubs3.go
        stubs_linux.go
        vdso_elf64.go
        vdso_linux.go
    )

    IF (ARCH_ARM64)
        SRCS(
            defs_linux_arm64.go
            os_linux_arm64.go
            rt0_linux_arm64.s
            signal_arm64.go
            signal_linux_arm64.go
            sys_linux_arm64.s
            timestub.go
            timestub2.go
            vdso_linux_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            defs_linux_amd64.go
            os_linux_noauxv.go
            os_linux_x86.go
            rt0_linux_amd64.s
            signal_amd64.go
            signal_linux_amd64.go
            sys_linux_amd64.s
            time_linux_amd64.s
            timeasm.go
            tls_stub.go
            vdso_linux_amd64.go
        )
    ENDIF()
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        auxv_none.go
        create_file_nounix.go
        defs_windows.go
        lock_sema.go
        mem_windows.go
        netpoll_windows.go
        os_windows.go
        security_nonunix.go
        signal_windows.go
        sigqueue_note.go
        stubs3.go
        stubs_nonlinux.go
        syscall_windows.go
        time_windows.h
        timeasm.go
        vdso_in_none.go
        zcallback_windows.go
    )

    IF (ARCH_ARM64)
        SRCS(
            defs_windows_arm64.go
            os_windows_arm64.go
            rt0_windows_arm64.s
            sys_windows_arm64.s
            time_windows_arm64.s
            zcallback_windows_arm64.s
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            defs_windows_amd64.go
            rt0_windows_amd64.s
            sys_windows_amd64.s
            time_windows_amd64.s
            tls_windows_amd64.go
            zcallback_windows.s
        )
    ENDIF()
ENDIF()

IF (CGO_ENABLED OR OS_DARWIN)
    IF (RACE)
        SRCS(
            race.go
        )

        IF (ARCH_ARM64)
            SRCS(
                race_arm64.s
            )
        ENDIF()

        IF (ARCH_X86_64)
            SRCS(
                race_amd64.s
            )
        ENDIF()
    ELSE()
        SRCS(
            race0.go
        )
    ENDIF()
ELSE()
    SRCS(
        race0.go
    )
ENDIF()

END()


IF (CGO_ENABLED)
    RECURSE(
        cgo
    )
ENDIF()

RECURSE(
    coverage
    debug
    internal
    metrics
    pprof
    race
    trace
)
