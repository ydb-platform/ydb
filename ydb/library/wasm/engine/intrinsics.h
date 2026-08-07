#pragma once

#include "wavm_private_imports.h"

#include <util/system/types.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

WAVM_DECLARE_INTRINSIC_MODULE(empty);
WAVM_DECLARE_INTRINSIC_MODULE(standard);

////////////////////////////////////////////////////////////////////////////////

#define FOREACH_WEB_ASSEMBLY_SYSCALL(macro) \
    macro(__subtf3, void, i64, i64, i64, i64, i64); \
    macro(__addtf3, void, i64, i64, i64, i64, i64); \
    macro(__multf3, void, i64, i64, i64, i64, i64); \
    macro(__divtf3, void, i64, i64, i64, i64, i64); \
    macro(__getf2, i32, i64, i64, i64, i64); \
    macro(__fixtfdi, i64, i64, i64); \
    macro(__floatditf, void, i64, i64); \
    macro(__trunctfsf2, float, i64, i64); \
    macro(__extendsftf2, void, i64, float); \
    macro(__trunctfdf2, double, i64, i64); \
    macro(__extenddftf2, void, i64, double); \
    macro(__muldc3, void, i64, double, double, double, double); \
    macro(__mulsc3, void, i64, float, float, float, float); \
    macro(__unordtf2, i32, i64, i64, i64, i64); \
    macro(__multc3, void, i64, i64, i64, i64, i64, i64, i64, i64, i64); \
    macro(__floatsitf, void, i64, i32); \
    macro(__floatunsitf, void, i64, i32); \
    macro(__netf2, i32, i64, i64, i64, i64); \
    macro(__multi3, void, i64, i64, i64, i64, i64); \
    macro(__lttf2, i32, i64, i64, i64, i64); \
    macro(__gttf2, i32, i64, i64, i64, i64); \
    macro(__fixtfsi, i32, i64, i64); \
    macro(__eqtf2, i32, i64, i64, i64, i64); \
    macro(__letf2, i32, i64, i64, i64, i64); \
    macro(fork, i32); \
    macro(__syscall_getpriority, i32, i32, i32); \
    macro(__syscall_getresgid32, i32, i64, i64, i64); \
    macro(__syscall_getresuid32, i32, i64, i64, i64); \
    macro(__syscall_prlimit64, i32, i32, i32, i64, i64); \
    macro(__syscall_ugetrlimit, i32, i32, i64); \
    macro(__syscall_getrusage, i32, i32, i64); \
    macro(__syscall_setdomainname, i32, i64, i64); \
    macro(__syscall_setpriority, i32, i32, i32, i32); \
    macro(__syscall_setrlimit, i32, i32, i64); \
    macro(__syscall_uname, i32, i64); \
    macro(__syscall_madvise, i32, i64, i64, i32); \
    macro(__syscall_mincore, i32, i64, i64, i64); \
    macro(__syscall_mlock, i32, i64, i64); \
    macro(__syscall_mlockall, i32, i32); \
    macro(__syscall_mprotect, i32, i64, i64, i32); \
    macro(__syscall_mremap, i32, i64, i64, i64, i32, i64); \
    macro(__syscall_munlock, i32, i64, i64); \
    macro(__syscall_munlockall, i32); \
    macro(__syscall_recvmmsg, i32, i32, i64, i64, i32, i64); \
    macro(execve, i32, i64, i64, i64); \
    macro(__syscall_wait4, i32, i32, i64, i32, i32); \
    macro(getpwnam_r, i32, i64, i64, i64, i64, i64); \
    macro(getpwuid_r, i32, i32, i64, i64, i64, i64); \
    macro(__syscall_pselect6, i32, i32, i64, i64, i64, i64, i64); \
    macro(__syscall_umask, i32, i32); \
    macro(__syscall_acct, i32, i64); \
    macro(__syscall_getegid32, i32); \
    macro(__syscall_geteuid32, i32); \
    macro(__syscall_getgid32, i32); \
    macro(__syscall_getgroups32, i32, i32, i64); \
    macro(__syscall_getpgid, i32, i32); \
    macro(__syscall_getppid, i32); \
    macro(__syscall_getsid, i32, i32); \
    macro(__syscall_getuid32, i32); \
    macro(__syscall_linkat, i32, i32, i64, i32, i64, i32); \
    macro(__syscall_pause, i32); \
    macro(__syscall_pipe2, i32, i64, i32); \
    macro(__syscall_setpgid, i32, i32, i32); \
    macro(__syscall_setsid, i32); \
    macro(__syscall_sync, i32); \
    macro(accept, i32, i32, i64, i64); \
    macro(args_get, i32, i64, i64); \
    macro(bind, i32, i32, i64, i32); \
    macro(clock_res_get, i32, i32, i64); \
    macro(clock_time_get, i32, i32, i64, i64); \
    macro(connect, i32, i32, i64, i32); \
    macro(__cxa_thread_atexit_impl, i32, i64, i64, i64); \
    macro(_dlopen_js, i64, i64); \
    macro(_dlsym_catchup_js, i64, i64, i32); \
    macro(_dlsym_js, i64, i64, i64, i64); \
    macro(emscripten_date_now, double); \
    macro(_emscripten_dlopen_js, void, i64, i64, i64, i64); \
    macro(_emscripten_dlsync_threads_async, void, i64, i64, i64); \
    macro(_emscripten_dlsync_threads, void); \
    macro(_emscripten_get_now_is_monotonic, i32); \
    macro(emscripten_get_now_res, double); \
    macro(emscripten_promise_create, i64); \
    macro(emscripten_promise_destroy, void, i64); \
    macro(emscripten_promise_resolve, void, i64, i32, i64); \
    macro(emscripten_proxy_callback, i32, i64, i64, i64, i64, i64, i64); \
    macro(emscripten_stack_get_base, i64); \
    macro(emscripten_stack_get_current, i64); \
    macro(emscripten_stack_get_end, i64); \
    macro(fd_close, i32, i32); \
    macro(fd_fdstat_get, i32, i32, i64); \
    macro(fd_pread, i32, i32, i64, i64, i64, i64); \
    macro(fd_pwrite, i32, i32, i64, i64, i64, i64); \
    macro(fd_read, i32, i32, i64, i64, i64); \
    macro(fd_seek, i32, i32, i64, i32, i64); \
    macro(fd_sync, i32, i32); \
    macro(fd_write, i32, i32, i64, i64, i64); \
    macro(freeaddrinfo, void, i64); \
    macro(getaddrinfo, i32, i64, i64, i64, i64); \
    macro(getdtablesize, i32); \
    macro(getgrouplist, i32, i64, i32, i64, i64); \
    macro(getnameinfo, i32, i64, i32, i64, i32, i64, i32, i32); \
    macro(getpeername, i32, i32, i64, i64); \
    macro(getsockname, i32, i32, i64, i64); \
    macro(getsockopt, i32, i32, i32, i32, i64, i64); \
    macro(_gmtime_js, void, i64, i64); \
    macro(_localtime_js, void, i64, i64); \
    macro(__main_argc_argv, i32, i32, i64); \
    macro(_msync_js, i32, i64, i64, i32, i32, i32, i64); \
    macro(proc_exit, void, i32); \
    macro(pthread_setschedparam, i32, i64, i32, i64); \
    macro(recvfrom, i64, i32, i64, i64, i32, i64, i64); \
    macro(recv, i64, i32, i64, i64, i32); \
    macro(send, i64, i32, i64, i64, i32); \
    macro(sendmsg, i64, i32, i64, i32); \
    macro(sendto, i64, i32, i64, i64, i32, i64, i32); \
    macro(setsockopt, i32, i32, i32, i32, i64, i32); \
    macro(shmat, i64, i32, i64, i32); \
    macro(shmctl, i32, i32, i32, i64); \
    macro(shmdt, i32, i64); \
    macro(shmget, i32, i32, i64, i32); \
    macro(shutdown, i32, i32, i32); \
    macro(socket, i32, i32, i32, i32); \
    macro(socketpair, i32, i32, i32, i32, i64); \
    macro(__syscall_chdir, i32, i64); \
    macro(__syscall_chmod, i32, i64, i32); \
    macro(__syscall_dup3, i32, i32, i32, i32); \
    macro(__syscall_dup, i32, i32); \
    macro(__syscall_faccessat, i32, i32, i64, i32, i32); \
    macro(__syscall_fadvise64, i32, i32, i64, i64, i32); \
    macro(__syscall_fallocate, i32, i32, i32, i64, i64); \
    macro(__syscall_fchdir, i32, i32); \
    macro(__syscall_fchmodat, i32, i32, i64, i32, i64); \
    macro(__syscall_fchmod, i32, i32, i32); \
    macro(__syscall_fchown32, i32, i32, i32, i32); \
    macro(__syscall_fchownat, i32, i32, i64, i32, i32, i32); \
    macro(__syscall_fdatasync, i32, i32); \
    macro(__syscall_fstatfs64, i32, i32, i64, i64); \
    macro(__syscall_ftruncate64, i32, i32, i64); \
    macro(__syscall_getcwd, i32, i64, i64); \
    macro(__syscall_getdents64, i32, i32, i64, i64); \
    macro(__syscall_lstat64, i32, i64, i64); \
    macro(__syscall_mkdirat, i32, i32, i64, i32); \
    macro(__syscall_mknodat, i32, i32, i64, i32, i32); \
    macro(__syscall_newfstatat, i32, i32, i64, i64, i32); \
    macro(__syscall__newselect, i32, i32, i64, i64, i64, i64); \
    macro(__syscall_pipe, i32, i64); \
    macro(__syscall_poll, i32, i64, i32, i32); \
    macro(__syscall_readlinkat, i32, i32, i64, i64, i64); \
    macro(__syscall_renameat, i32, i32, i64, i32, i64); \
    macro(__syscall_rmdir, i32, i64); \
    macro(__syscall_stat64, i32, i64, i64); \
    macro(__syscall_statfs64, i32, i64, i64, i64); \
    macro(__syscall_symlinkat, i32, i64, i32, i64); \
    macro(__syscall_symlink, i32, i64, i64); \
    macro(__syscall_truncate64, i32, i64, i64); \
    macro(__syscall_unlinkat, i32, i32, i64, i32); \
    macro(__syscall_utimensat, i32, i32, i64, i64, i32); \
    macro(__syscall_accept4, i32, i32, i64, i64, i32, i32, i32); \
    macro(_tzset_js, void, i64, i64, i64); \
    macro(_Z11GetExecPathv, i64); \
    macro(_ZN5NVdso8FunctionEPKcS1_, i64, i64, i64); \
    macro(_ZN8NMemInfo10GetMemInfoEi, void, i64, i32); \
    macro(_ZN9NResource4FindE15TBasicStringBufIcNSt4__y111char_traitsIcEEE, void, i64, i64); \
    macro(pthread_kill, i32, i64, i32); \
    macro(reallocarray, i64, i64, i64, i64); \
    macro(strptime, i64, i64, i64, i64); \
    macro(_emscripten_log_formatted, void, i32, i64); \
    macro(_emscripten_lookup_name, i32, i64); \
    macro(random_get, i32, i64, i64); \
    macro(__syscall_fchmodat2, i32, i32, i64, i32, i32); \
    macro(_emscripten_system, i32, i64); \
    macro(__syscall_bind, i32, i32, i64, i64, i32, i32, i32); \
    macro(__syscall_connect, i32, i32, i64, i64, i32, i32, i32); \
    macro(__syscall_getpeername, i32, i32, i64, i64, i32, i32, i32); \
    macro(__syscall_getsockname, i32, i32, i64, i64, i32, i32, i32); \
    macro(__syscall_getsockopt, i32, i32, i32, i32, i64, i64, i32); \
    macro(__syscall_listen, i32, i32, i32, i32, i32, i32, i32); \
    macro(__syscall_recvfrom, i32, i32, i64, i64, i32, i64, i64); \
    macro(__syscall_recvmsg, i32, i32, i64, i32, i32, i32, i32); \
    macro(__syscall_sendmsg, i32, i32, i64, i32, i64, i64, i32); \
    macro(__syscall_sendto, i32, i32, i64, i64, i32, i64, i64); \
    macro(__syscall_socket, i32, i32, i32, i32, i32, i32, i32);


////////////////////////////////////////////////////////////////////////////////

#define DECLARE_WEB_ASSEMBLY_SYSCALL(name, result, ...) \
    result name(__VA_ARGS__);

    DECLARE_WEB_ASSEMBLY_SYSCALL(emscripten_notify_memory_growth, void, i64);
    DECLARE_WEB_ASSEMBLY_SYSCALL(logAPIs, i32);
    DECLARE_WEB_ASSEMBLY_SYSCALL(emscripten_dbg_backtrace, void, i64);
    FOREACH_WEB_ASSEMBLY_SYSCALL(DECLARE_WEB_ASSEMBLY_SYSCALL); // NOLINT

#undef DECLARE_WEB_ASSEMBLY_SYSCALL

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
