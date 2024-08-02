#include "crash_handler.h"
#include "signal_registry.h"

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/library/undumpable/undumpable.h>

#include <library/cpp/yt/system/exit.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/system/handle_eintr.h>

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
        MakeMutableRange(*buffer),
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
    formatter.AppendString("\" if you are using GNU date ***\n");
    WriteToStderr(formatter);
}

using TCodicilStack = std::vector<TString>;

NConcurrency::TFlsSlot<TCodicilStack>& CodicilStackSlot()
{
    static NConcurrency::TFlsSlot<TCodicilStack> Slot;
    return Slot;
}

//! Dump codicils.
void DumpCodicils()
{
    // NB: Avoid constructing FLS slot to avoid allocations; these may lead to deadlocks if the
    // program crashes during an allocation itself.
    if (CodicilStackSlot().IsInitialized() && !CodicilStackSlot()->empty()) {
        WriteToStderr("*** Begin codicils ***\n");
        for (const auto& data : *CodicilStackSlot()) {
            TFormatter formatter;
            formatter.AppendString(data.c_str());
            formatter.AppendString("\n");
            WriteToStderr(formatter);
        }
        WriteToStderr("*** End codicils ***\n");
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
const char* GetTrapName(int trapno)
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
    enum x86_pf_error_code {
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

    formatter.AppendString("*** ");
    if (const char* name = GetSignalName(si->si_signo)) {
        formatter.AppendString(name);
    } else {
        // Use the signal number if the name is unknown. The signal name
        // should be known, but just in case.
        formatter.AppendString("Signal ");
        formatter.AppendNumber(si->si_signo);
    }

    formatter.AppendString(" (@0x");
    formatter.AppendNumber(reinterpret_cast<uintptr_t>(si->si_addr), 16);
    formatter.AppendString(")");
    formatter.AppendString(" received by PID ");
    formatter.AppendNumber(getpid());

    formatter.AppendString(" (FID 0x");
    formatter.AppendNumber(NConcurrency::GetCurrentFiberId(), 16);
    formatter.AppendString(" TID 0x");
    // We assume pthread_t is an integral number or a pointer, rather
    // than a complex struct. In some environments, pthread_self()
    // returns an uint64 but in some other environments pthread_self()
    // returns a pointer. Hence we use C-style cast here, rather than
    // reinterpret/static_cast, to support both types of environments.
    formatter.AppendNumber((uintptr_t)pthread_self(), 16);
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

    formatter.AppendString(" ");
#endif
    formatter.AppendString("***\n");

    WriteToStderr(formatter);
}

void DumpSigcontext(void* uc)
{
#if (defined(HAVE_UCONTEXT_H) || defined(HAVE_SYS_UCONTEXT_H)) && defined(PC_FROM_UCONTEXT) && defined(_linux_) && defined(_x86_64_)
    ucontext_t* context = reinterpret_cast<ucontext_t*>(uc);

    TFormatter formatter;

    formatter.AppendString("\nERR ");
    FormatErrorCodeName(&formatter, context->uc_mcontext.gregs[REG_ERR]);

    formatter.AppendString("\nTRAPNO ");
    if (const char* trapName = GetTrapName(context->uc_mcontext.gregs[REG_TRAPNO])) {
        formatter.AppendString(trapName);
    } else {
        formatter.AppendString("0x");
        formatter.AppendNumber(context->uc_mcontext.gregs[REG_TRAPNO], 16);
    }

    formatter.AppendString("\nR8 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R8], 16);
    formatter.AppendString("\nR9 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R9], 16);
    formatter.AppendString("\nR10 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R10], 16);
    formatter.AppendString("\nR11 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R11], 16);
    formatter.AppendString("\nR12 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R12], 16);
    formatter.AppendString("\nR13 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R13], 16);
    formatter.AppendString("\nR14 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R14], 16);
    formatter.AppendString("\nR15 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_R15], 16);
    formatter.AppendString("\nRDI 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RDI], 16);
    formatter.AppendString("\nRSI 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RSI], 16);
    formatter.AppendString("\nRBP 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RBP], 16);
    formatter.AppendString("\nRBX 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RBX], 16);
    formatter.AppendString("\nRDX 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RDX], 16);
    formatter.AppendString("\nRAX 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RAX], 16);
    formatter.AppendString("\nRCX 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RCX], 16);
    formatter.AppendString("\nRSP 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RSP], 16);
    formatter.AppendString("\nRIP 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_RIP], 16);
    formatter.AppendString("\nEFL 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_EFL], 16);
    formatter.AppendString("\nCSGSFS 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_CSGSFS], 16);
    formatter.AppendString("\nOLDMASK 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_OLDMASK], 16);
    formatter.AppendString("\nCR2 0x");
    formatter.AppendNumber(context->uc_mcontext.gregs[REG_CR2], 16);
    formatter.AppendChar('\n');

    WriteToStderr(formatter);
#else
    Y_UNUSED(uc);
#endif
}

void CrashTimeoutHandler(int /*signal*/)
{
    WriteToStderr("*** Process hung during crash ***\n");
    AbortProcess(ToUnderlying(EProcessExitCode::GenericError));
}

void DumpUndumpableBlocksInfo()
{
    auto cutInfo = CutUndumpableRegionsFromCoredump();

    {
        TFormatter formatter;
        formatter.AppendString("*** Marked memory regions of total size ");
        formatter.AppendNumber(cutInfo.MarkedSize / 1_MB);
        formatter.AppendString(" MB as undumpable ***\n");
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
        formatter.AppendString(" ***\n");
        WriteToStderr(formatter);
    }
}

#endif

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

// Dumps signal, stack frame information and codicils.
void CrashSignalHandler(int /*signal*/, siginfo_t* si, void* uc)
{
    // All code here _MUST_ be async signal safe unless specified otherwise.

    // When did the crash happen?
    NDetail::DumpTimeInfo();

    // Dump codicils.
    NDetail::DumpCodicils();

    // Where did the crash happen?
    {
        std::array<const void*, 1> frames{NDetail::GetPC(uc)};
        NBacktrace::SymbolizeBacktrace(
            MakeRange(frames),
            [] (TStringBuf info) {
                info.SkipPrefix(" 1. ");
                WriteToStderr(info);
            });
    }

    NDetail::DumpSignalInfo(si);

    NDetail::DumpSigcontext(uc);

    // The easiest way to choose proper overload...
    DumpStackTrace([] (TStringBuf str) { WriteToStderr(str); }, NDetail::GetPC(uc));

    NDetail::DumpUndumpableBlocksInfo();

    WriteToStderr("*** Waiting for logger to shut down ***\n");

    // Actually, it is not okay to hang.
    ::signal(SIGALRM, NDetail::CrashTimeoutHandler);
    ::alarm(5);

    NLogging::TLogManager::Get()->Shutdown();

    WriteToStderr("*** Terminating ***\n");
}

#else

void CrashSignalHandler(int /*signal*/)
{ }

#endif

////////////////////////////////////////////////////////////////////////////////

void PushCodicil(const TString& data)
{
#ifdef _unix_
    NDetail::CodicilStackSlot()->push_back(data);
#else
    Y_UNUSED(data);
#endif
}

void PopCodicil()
{
#ifdef _unix_
    YT_VERIFY(!NDetail::CodicilStackSlot()->empty());
    NDetail::CodicilStackSlot()->pop_back();
#endif
}

std::vector<TString> GetCodicils()
{
#ifdef _unix_
    return *NDetail::CodicilStackSlot();
#else
    return {};
#endif
}

TCodicilGuard::TCodicilGuard()
    : Active_(false)
{ }

TCodicilGuard::TCodicilGuard(const TString& data)
    : Active_(true)
{
    PushCodicil(data);
}

TCodicilGuard::~TCodicilGuard()
{
    Release();
}

TCodicilGuard::TCodicilGuard(TCodicilGuard&& other)
    : Active_(other.Active_)
{
    other.Active_ = false;
}

TCodicilGuard& TCodicilGuard::operator=(TCodicilGuard&& other)
{
    if (this != &other) {
        Release();
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

void TCodicilGuard::Release()
{
    if (Active_) {
        PopCodicil();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
