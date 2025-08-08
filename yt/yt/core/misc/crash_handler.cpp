#include "crash_handler.h"

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/codicil.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/library/undumpable/undumpable.h>

#include <library/cpp/yt/system/exit.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/misc/tls.h>

#ifdef _unix_
#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>
#else
#include <library/cpp/yt/backtrace/cursors/dummy/dummy_cursor.h>
#endif

#include <util/system/defaults.h>

#include <signal.h>
#include <time.h>

#include <yt/yt/build/config.h>

#ifdef HAVE_UNISTD_H
#   include <unistd.h>
#endif
#ifdef HAVE_UCONTEXT_H
#ifdef _linux_
#   include <ucontext.h>
#endif
#endif
#ifdef HAVE_SYS_UCONTEXT_H
#   include <sys/ucontext.h>
#endif
#ifdef _win_
#   include <io.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WriteToStderr(const char* buffer, int length)
{
    // Ignore errors.
#ifdef _win_
    HandleEintr(::write, 2, buffer, length);
#else
    HandleEintr(write, 2, buffer, length);
#endif
}

void WriteToStderr(TStringBuf buffer)
{
    WriteToStderr(buffer.begin(), buffer.length());
}

void WriteToStderr(const char* buffer)
{
    WriteToStderr(buffer, ::strlen(buffer));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_NO_INLINE TStackTrace GetStackTrace(TStackTraceBuffer* buffer)
{
#ifdef _unix_
    NBacktrace::TLibunwindCursor cursor;
#else
    NBacktrace::TDummyCursor cursor;
#endif
    return NBacktrace::GetBacktrace(
        &cursor,
        TMutableRange(*buffer),
        /*framesToSkip*/ 2);
}

using NYT::WriteToStderr;

#ifdef _unix_

// See http://pubs.opengroup.org/onlinepubs/009695399/functions/xsh_chap02_04.html
// for a list of async signal safe functions.

//! Returns the program counter from a signal context, NULL if unknown.
void* GetPC(void* uc)
{
    // TODO(sandello): Merge with code from Bind() internals.
#if (defined(HAVE_UCONTEXT_H) || defined(HAVE_SYS_UCONTEXT_H)) && defined(PC_FROM_UCONTEXT) && defined(_linux_)
    if (uc) {
        const auto* context = reinterpret_cast<ucontext_t*>(uc);
        return reinterpret_cast<void*>(context->PC_FROM_UCONTEXT);
    }
#else
    Y_UNUSED(uc);
#endif
    return nullptr;
}

using TFormatter = TRawFormatter<1024>;

void WriteToStderr(const TBaseFormatter& formatter)
{
    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

//! Dumps time information.
/*!
 *  We do not dump human-readable time information with localtime()
 *  as it is not guaranteed to be async signal safe.
 */
void DumpTimeInfo()
{
    auto timeSinceEpoch = ::time(nullptr);

    TFormatter formatter;
    formatter.AppendString("*** Aborted at ");
    formatter.AppendNumber(timeSinceEpoch);
    formatter.AppendString(" (Unix time); Try \"date -d @");
    formatter.AppendNumber(timeSinceEpoch, 10);
    formatter.AppendString("\" if you are using GNU date\n");
    WriteToStderr(formatter);
}

//! Dumps codicils.
void DumpCodicils()
{
    auto builders = GetCodicilBuilders();
    if (!builders.empty()) {
        WriteToStderr("*** Begin codicils\n");
        TCodicilFormatter formatter;
        for (const auto& builder : builders) {
            formatter.Reset();
            builder(&formatter);
            WriteToStderr(formatter);
            if (formatter.GetBytesRemaining() == 0) {
                WriteToStderr(" (truncated)");
            }
            WriteToStderr("\n");
        }
        WriteToStderr("*** End codicils\n");
    }
}

// We will install the failure signal handler for signals SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS
// We could use strsignal() to get signal names, but we do not use it to avoid
// introducing yet another #ifdef complication.
const char* GetSignalName(int signo)
{
#define XX(name, message) case name: return #name " (" message ")";

    switch (signo) {
        XX(SIGILL, "Illegal instruction")
        XX(SIGFPE, "Floating-point exception")
        XX(SIGSEGV, "Segmentation violation")
        XX(SIGBUS, "BUS error")
        XX(SIGABRT, "Abort")
        XX(SIGTRAP, "Trace trap")
        XX(SIGCHLD, "Child status has changed")
#if 0
        XX(SIGPOLL, "Pollable event occurred")
#endif
        default: return nullptr;
    }

#undef XX
}

#ifdef _unix_

const char* GetSignalCodeName(int signo, int code)
{
#define XX(name, message) case name: return #name " (" message ")";

    switch (signo) {
        case SIGILL: switch (code) {
            XX(ILL_ILLOPC, "Illegal opcode.")
            XX(ILL_ILLOPN, "Illegal operand.")
            XX(ILL_ILLADR, "Illegal addressing mode.")
            XX(ILL_ILLTRP, "Illegal trap.")
            XX(ILL_PRVOPC, "Privileged opcode.")
            XX(ILL_PRVREG, "Privileged register.")
            XX(ILL_COPROC, "Coprocessor error.")
            XX(ILL_BADSTK, "Internal stack error.")
            default: return nullptr;
        }
        case SIGFPE: switch (code) {
            XX(FPE_INTDIV, "Integer divide by zero.")
            XX(FPE_INTOVF, "Integer overflow.")
            XX(FPE_FLTDIV, "Floating point divide by zero.")
            XX(FPE_FLTOVF, "Floating point overflow.")
            XX(FPE_FLTUND, "Floating point underflow.")
            XX(FPE_FLTRES, "Floating point inexact result.")
            XX(FPE_FLTINV, "Floating point invalid operation.")
            XX(FPE_FLTSUB, "Subscript out of range.")
            default: return nullptr;
        }
        case SIGSEGV: switch (code) {
            XX(SEGV_MAPERR, "Address not mapped to object.")
            XX(SEGV_ACCERR, "Invalid permissions for mapped object.")
            default: return nullptr;
        }
        case SIGBUS: switch (code) {
            XX(BUS_ADRALN, "Invalid address alignment.")
            XX(BUS_ADRERR, "Non-existent physical address.")
            XX(BUS_OBJERR, "Object specific hardware error.")
#if 0
            XX(BUS_MCEERR_AR, "Hardware memory error: action required.")
            XX(BUS_MCEERR_AO, "Hardware memory error: action optional.")
#endif
            default: return nullptr;
        }

        case SIGTRAP: switch (code) {
            XX(TRAP_BRKPT, "Process breakpoint.")
            XX(TRAP_TRACE, "Process trace trap.")
            default: return nullptr;
        }

        case SIGCHLD: switch (code) {
            XX(CLD_EXITED, "Child has exited." )
            XX(CLD_KILLED, "Child was killed.")
            XX(CLD_DUMPED, "Child terminated abnormally.")
            XX(CLD_TRAPPED, "Traced child has trapped.")
            XX(CLD_STOPPED, "Child has stopped.")
            XX(CLD_CONTINUED, "Stopped child has continued.")
            default: return nullptr;
        }
#if 0
        case SIGPOLL: switch (code) {
            XX(POLL_IN, "Data input available.")
            XX(POLL_OUT, "Output buffers available.")
            XX(POLL_MSG, "Input message available.")
            XX(POLL_ERR, "I/O error.")
            XX(POLL_PRI, "High priority input available.")
            XX(POLL_HUP, "Device disconnected.")
            default: return nullptr;
        }
#endif
        default: return nullptr;
    }

#undef XX
}

#endif

#ifdef _x86_64_

// From include/asm/traps.h

[[maybe_unused]]
const char* FindTrapName(int trapno)
{
#define XX(name, value, message) case value: return #name " (" message ")";

    switch (trapno) {
        XX(X86_TRAP_DE,          0, "Divide-by-zero")
        XX(X86_TRAP_DB,          1, "Debug")
        XX(X86_TRAP_NMI,         2, "Non-maskable Interrupt")
        XX(X86_TRAP_BP,          3, "Breakpoint")
        XX(X86_TRAP_OF,          4, "Overflow")
        XX(X86_TRAP_BR,          5, "Bound Range Exceeded")
        XX(X86_TRAP_UD,          6, "Invalid Opcode")
        XX(X86_TRAP_NM,          7, "Device Not Available")
        XX(X86_TRAP_DF,          8, "Double Fault")
        XX(X86_TRAP_OLD_MF,      9, "Coprocessor Segment Overrun")
        XX(X86_TRAP_TS,         10, "Invalid TSS")
        XX(X86_TRAP_NP,         11, "Segment Not Present")
        XX(X86_TRAP_SS,         12, "Stack Segment Fault")
        XX(X86_TRAP_GP,         13, "General Protection Fault")
        XX(X86_TRAP_PF,         14, "Page Fault")
        XX(X86_TRAP_SPURIOUS,   15, "Spurious Interrupt")
        XX(X86_TRAP_MF,         16, "x87 Floating-Point Exception")
        XX(X86_TRAP_AC,         17, "Alignment Check")
        XX(X86_TRAP_MC,         18, "Machine Check")
        XX(X86_TRAP_XF,         19, "SIMD Floating-Point Exception")
        XX(X86_TRAP_IRET,       32, "IRET Exception")
        default: return nullptr;
    }

#undef XX
}

[[maybe_unused]]
void FormatErrorCodeName(TBaseFormatter* formatter, int codeno)
{
    /*
     * Page fault error code bits:
     *
     *   bit 0 ==    0: no page found   1: protection fault
     *   bit 1 ==    0: read access     1: write access
     *   bit 2 ==    0: kernel-mode access  1: user-mode access
     *   bit 3 ==               1: use of reserved bit detected
     *   bit 4 ==               1: fault was an instruction fetch
     *   bit 5 ==               1: protection keys block access
     */
    enum x86_pf_error_code
    {
        X86_PF_PROT  =   1 << 0,
        X86_PF_WRITE =   1 << 1,
        X86_PF_USER  =   1 << 2,
        X86_PF_RSVD  =   1 << 3,
        X86_PF_INSTR =   1 << 4,
        X86_PF_PK    =   1 << 5,
    };

    formatter->AppendString(codeno & X86_PF_PROT ? "protection fault" : "no page found");
    formatter->AppendString(codeno & X86_PF_WRITE ? " write" : " read");
    formatter->AppendString(codeno & X86_PF_USER ? " user-mode" : " kernel-mode");
    formatter->AppendString( " access");

    if (codeno & X86_PF_RSVD) {
        formatter->AppendString(", use of reserved bit detected");
    }

    if (codeno & X86_PF_INSTR) {
        formatter->AppendString(", fault was an instruction fetch");
    }

    if (codeno & X86_PF_PK) {
        formatter->AppendString(", protection keys block access");
    }
}

#endif // _x86_64_

//! Dumps information about the signal.
void DumpSignalInfo(siginfo_t* si)
{
    TFormatter formatter;

    if (const char* name = GetSignalName(si->si_signo)) {
        formatter.AppendString(name);
    } else {
        // Use the signal number if the name is unknown. The signal name
        // should be known, but just in case.
        formatter.AppendString("Signal ");
        formatter.AppendNumber(si->si_signo);
    }

    formatter.AppendString(" (@0x");
    formatter.AppendNumber(reinterpret_cast<uintptr_t>(si->si_addr), /*radix*/ 16);
    formatter.AppendString(")");
    formatter.AppendString(" received by PID ");
    formatter.AppendNumber(getpid());

    formatter.AppendString(" (FID 0x");
    formatter.AppendNumber(NConcurrency::GetCurrentFiberId(), /*radix*/ 16);
    formatter.AppendString(" TID 0x");
    // We assume pthread_t is an integral number or a pointer, rather
    // than a complex struct. In some environments, pthread_self()
    // returns an uint64 but in some other environments pthread_self()
    // returns a pointer. Hence we use C-style cast here, rather than
    // reinterpret/static_cast, to support both types of environments.
    formatter.AppendNumber(reinterpret_cast<uintptr_t>(pthread_self()), /*radix*/ 16);
    formatter.AppendString(") ");
    // Only linux has the PID of the signal sender in si_pid.
#ifdef _unix_
    formatter.AppendString("from PID ");
    formatter.AppendNumber(si->si_pid);
    formatter.AppendString(" ");
    formatter.AppendString("code ");

    if (const char* codeMessage = GetSignalCodeName(si->si_signo, si->si_code)) {
        formatter.AppendString(codeMessage);
    } else {
        formatter.AppendNumber(si->si_code);
    }
#endif
    formatter.AppendChar('\n');

    WriteToStderr(formatter);
}

void DumpSigcontext(void* uc)
{
#if (defined(HAVE_UCONTEXT_H) || defined(HAVE_SYS_UCONTEXT_H)) && defined(PC_FROM_UCONTEXT) && defined(_linux_) && defined(_x86_64_)
    ucontext_t* context = reinterpret_cast<ucontext_t*>(uc);

    TFormatter formatter;

    formatter.AppendString("ERR    ");
    FormatErrorCodeName(&formatter, context->uc_mcontext.gregs[REG_ERR]);
    formatter.AppendChar('\n');

    formatter.AppendString("TRAPNO ");
    if (const char* trapName = FindTrapName(context->uc_mcontext.gregs[REG_TRAPNO])) {
        formatter.AppendString(trapName);
    } else {
        formatter.AppendString("0x");
        formatter.AppendNumber(context->uc_mcontext.gregs[REG_TRAPNO], 16);
    }
    formatter.AppendChar('\n');

    auto formatRegister = [&, horizontalPos = 0] (TStringBuf name, int reg) mutable {
        if (horizontalPos > 1) {
            formatter.AppendChar('\n');
            horizontalPos = 0;
        } else if (horizontalPos > 0) {
            formatter.AppendChar(' ', 4);
        }
        formatter.AppendString(name);
        formatter.AppendChar(' ', 7 - name.length());
        formatter.AppendString("0x");
        formatter.AppendNumber(context->uc_mcontext.gregs[reg], /*radix*/ 16, /*width*/ 16, /*ch*/ '0');
        ++horizontalPos;
    };
    formatRegister("RAX", REG_RAX);
    formatRegister("RBX", REG_RBX);
    formatRegister("RCX", REG_RCX);
    formatRegister("RDX", REG_RDX);
    formatRegister("RSI", REG_RSI);
    formatRegister("RDI", REG_RDI);
    formatRegister("RBP", REG_RBP);
    formatRegister("RSP", REG_RSP);
    formatRegister("R8", REG_R8);
    formatRegister("R9", REG_R9);
    formatRegister("R10", REG_R10);
    formatRegister("R11", REG_R11);
    formatRegister("R12", REG_R12);
    formatRegister("R13", REG_R13);
    formatRegister("R14", REG_R14);
    formatRegister("R15", REG_R15);
    formatRegister("RIP", REG_RIP);
    formatRegister("EFL", REG_EFL);
    formatRegister("CR2", REG_CR2);
    formatRegister("CSGSFS", REG_CSGSFS);
    formatter.AppendChar('\n');

    WriteToStderr(formatter);
#else
    Y_UNUSED(uc);
#endif
}

void CrashTimeoutHandler(int /*signal*/)
{
    WriteToStderr("*** Crash signal handler timed out\n");

    YT_BUILTIN_TRAP();
}

void DumpUndumpableBlocksInfo()
{
    auto cutInfo = CutUndumpableRegionsFromCoredump();

    {
        TFormatter formatter;
        formatter.AppendString("*** Marked memory regions of total size ");
        formatter.AppendNumber(cutInfo.MarkedSize / 1_MB);
        formatter.AppendString(" MB as undumpable\n");
        WriteToStderr(formatter);
    }

    for (const auto& record : cutInfo.FailedToMarkMemory) {
        if (record.ErrorCode == 0) {
            break;
        }

        TFormatter formatter;
        formatter.AppendString("*** Failed to mark ");
        formatter.AppendNumber(record.Size / 1_MB);
        formatter.AppendString(" MB with error code ");
        formatter.AppendNumber(record.ErrorCode);
        formatter.AppendString("\n");
        WriteToStderr(formatter);
    }
}

#endif

Y_WEAK void MaybeThrowSafeAssertionException(TStringBuf /*message*/)
{
    // A default implementation has no means of safety.
    // Actual implementation lives in yt/yt/library/safe_assert.
}

void AssertTrapImpl(
    TStringBuf trapType,
    TStringBuf expr,
    TStringBuf description,
    TStringBuf file,
    int line,
    TStringBuf function)
{
    TRawFormatter<1024> formatter;
    formatter.AppendString("*** ");
    formatter.AppendString(trapType);
    formatter.AppendString("(");
    formatter.AppendString(expr);
    if (!description.empty()) {
        if (!expr.empty()) {
            formatter.AppendString(", ");
        }
        formatter.AppendChar('"');
        FormatString(&formatter, description, "Q");
        formatter.AppendChar('"');
    }
    formatter.AppendString(") at ");
    formatter.AppendString(file);
    formatter.AppendString(":");
    formatter.AppendNumber(line);
    if (function) {
        formatter.AppendString(" in ");
        formatter.AppendString(function);
        formatter.AppendString("\n");
    }

    MaybeThrowSafeAssertionException(formatter.GetBuffer());

    WriteToStderr(formatter.GetBuffer());

    // This (hopefully) invokes CrashSignalHandler.
    YT_BUILTIN_TRAP();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

// Dumps signal, stack frame information and codicils.
void CrashSignalHandler(int /*signal*/, siginfo_t* si, void* uc)
{
    // All code here _MUST_ be async signal safe unless specified otherwise.

    // Actually, it is not okay to hang.
    ::signal(SIGALRM, NDetail::CrashTimeoutHandler);
    ::alarm(60);

    NDetail::DumpTimeInfo();

    NDetail::DumpCodicils();

    NDetail::DumpSignalInfo(si);

    NDetail::DumpSigcontext(uc);

    // The easiest way to choose proper overload...
    DumpStackTrace([] (TStringBuf str) { WriteToStderr(str); }, NDetail::GetPC(uc));

    NDetail::DumpUndumpableBlocksInfo();

    WriteToStderr("*** Waiting for logger to shut down\n");

    NLogging::TLogManager::Get()->Shutdown();

    WriteToStderr("*** Terminating\n");
}

#else

void CrashSignalHandler(int /*signal*/)
{ }

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
